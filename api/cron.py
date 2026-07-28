import os
import json
import sqlite3
import hashlib
import hmac
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

# ============================================================
# LAZY IMPORTS (avoid import errors on cold start)
# ============================================================
def _import_requests():
    import requests
    return requests

def _import_feedparser():
    import feedparser
    return feedparser

# ============================================================
# CONFIGURATION
# ============================================================
ALL_TICKERS = ["AMZN", "NVDA", "VDY", "BTC", "ETH", "SOL", "XRP"]

# Group tickers by asset class for better formatting
ASSET_CLASSES = {
    "📈 <b>US Stocks</b>": ["AMZN", "NVDA"],
    "🇨🇦 <b>Canadian ETFs</b>": ["VDY"],
    "₿ <b>Crypto</b>": ["BTC", "ETH", "SOL", "XRP"],
}

NEWS_FEEDS = {
    "stocks": [
        "https://seekingalpha.com/feed.xml",
        "https://feeds.finance.yahoo.com/rss/2.0/headline",
        "https://www.marketwatch.com/rss/topstories",
        "https://feeds.bloomberg.com/markets/news.rss",
        "https://www.reuters.com/business/finance/rss",
    ],
    "crypto": [
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "https://theblock.co/rss.xml",
        "https://cointelegraph.com/rss",
        "https://decrypt.co/feed",
    ],
    "general": [
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^IXIC",
    ],
    # NEW: Macro/macro-economic feeds for broader context
    "macro": [
        "https://www.reuters.com/markets/us/rss",           # US markets/macro
        "https://www.reuters.com/business/economy/rss",    # Economics/Fed/rates
        "https://www.reuters.com/markets/commodities/rss", # Oil/commodities
        "https://www.reuters.com/technology/artificial-intelligence/rss", # AI sector
        "https://www.bloomberg.com/feed/podcast/etf-report.xml", # ETF/sector flows
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=CL=F",  # Crude oil
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GC=F",  # Gold
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^TNX",  # 10Y Treasury yield
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=DX-Y.NYB", # Dollar index
    ]
}

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
CRON_SECRET = os.getenv("CRON_SECRET")
NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY")

ALERT_THRESHOLD = 7
DEDUPE_HOURS = 24

# ============================================================
# DATABASE
# ============================================================
DB_PATH = Path("/tmp/portfolio_news.db")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_items (
            hash TEXT PRIMARY KEY,
            title TEXT, url TEXT, ticker TEXT, score INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Drop old schema and create new one
    conn.execute("DROP TABLE IF EXISTS analytics_cache")
    conn.execute("""
        CREATE TABLE analytics_cache (
            ticker TEXT PRIMARY KEY,
            signal_short TEXT, confidence_short INTEGER, reasoning_short TEXT,
            signal_long TEXT, confidence_long INTEGER, reasoning_long TEXT,
            price REAL, target_price_short REAL, target_price_long REAL,
            price_change_1d REAL, price_change_7d REAL, price_change_30d REAL,
            analyst_rating TEXT, analyst_target REAL, analyst_count INTEGER,
            news_summary TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn

def is_duplicate(conn, item_hash: str) -> bool:
    return conn.execute("SELECT 1 FROM seen_items WHERE hash = ?", (item_hash,)).fetchone() is not None

def mark_seen(conn, item_hash: str, title: str, url: str, ticker: str, score: int):
    conn.execute("INSERT OR IGNORE INTO seen_items VALUES (?, ?, ?, ?, ?, ?)",
                 (item_hash, title, url, ticker, score, datetime.now()))
    conn.commit()

def cleanup_old(conn, hours: int = DEDUPE_HOURS):
    cutoff = datetime.now() - timedelta(hours=hours)
    conn.execute("DELETE FROM seen_items WHERE created_at < ?", (cutoff,))
    conn.commit()

def get_cached_analytics(conn, ticker: str) -> Optional[Dict]:
    row = conn.execute("""SELECT signal_short, confidence_short, reasoning_short,
                          signal_long, confidence_long, reasoning_long,
                          price, target_price_short, target_price_long,
                          price_change_1d, price_change_7d, price_change_30d,
                          analyst_rating, analyst_target, analyst_count,
                          news_summary
                          FROM analytics_cache WHERE ticker = ?""", (ticker,)).fetchone()
    if row:
        return {
            "signal_short": row[0], "confidence_short": row[1], "reasoning_short": row[2],
            "signal_long": row[3], "confidence_long": row[4], "reasoning_long": row[5],
            "price": row[6], "target_price_short": row[7], "target_price_long": row[8],
            "price_change_1d": row[9], "price_change_7d": row[10], "price_change_30d": row[11],
            "analyst_rating": row[12], "analyst_target": row[13], "analyst_count": row[14],
            "news_summary": row[15]
        }
    return None

def cache_analytics(conn, ticker: str, data: Dict):
    conn.execute("""INSERT OR REPLACE INTO analytics_cache VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                 (ticker, data["signal_short"], data["confidence_short"], data["reasoning_short"],
                  data["signal_long"], data["confidence_long"], data["reasoning_long"],
                  data["price"], data["target_price_short"], data["target_price_long"],
                  data["price_change_1d"], data["price_change_7d"], data["price_change_30d"],
                  data["analyst_rating"], data["analyst_target"], data["analyst_count"],
                  data["news_summary"], datetime.now()))
    conn.commit()

# ============================================================
# MACRO NEWS EXTRACTION & ANALYSIS
# ============================================================
MACRO_KEYWORDS = {
    "oil": ["oil", "crude", "brent", "wti", "opec", "energy sector", "energy stocks", "xle", "energy etf"],
    "rates": ["fed", "federal reserve", "interest rate", "rate cut", "rate hike", "powell", "fomc", "treasury yield", "10-year", "bond yield"],
    "usd": ["dollar", "usd", "dxy", "greenback", "currency", "forex"],
    "inflation": ["inflation", "cpi", "pce", "core inflation", "prices rose", "prices fell"],
    "ai": ["artificial intelligence", "ai chip", "ai infrastructure", "data center", "gpu", "nvidia", "semiconductor", "chip"],
    "crypto_reg": ["sec", "crypto regulation", "etf approval", "bitcoin etf", "ethereum etf", "cryptocurrency regulation", "binance", "coinbase"],
    "recession": ["recession", "gdp", "unemployment", "jobless", "economic growth", "soft landing", "hard landing"],
    "china": ["china", "chinese economy", "pboc", "yuan", "hong kong"],
    "earnings": ["earnings", "quarterly results", "guidance", "outlook", "revenue beat", "revenue miss"],
}

def extract_macro_themes(articles: List[Dict]) -> Dict[str, List[Dict]]:
    """Extract macro themes from all fetched articles."""
    themes = {theme: [] for theme in MACRO_KEYWORDS}
    
    for article in articles:
        text = f"{article['title']} {article['summary']}".lower()
        for theme, keywords in MACRO_KEYWORDS.items():
            if any(kw in text for kw in keywords):
                themes[theme].append(article)
                break  # Assign to first matching theme to avoid duplicates
    
    # Only return themes with content
    return {k: v for k, v in themes.items() if v}

def analyze_macro_impact(macro_themes: Dict[str, List[Dict]], holdings: Dict) -> List[str]:
    """Analyze macro themes and their impact on portfolio holdings."""
    impacts = []
    
    # Oil → Energy sector, but also impacts inflation/rates
    if macro_themes.get("oil"):
        oil_articles = macro_themes["oil"][:3]
        oil_items = []
        for a in oil_articles:
            title = a['title'][:80]
            oil_items.append(f"• {title}...")
        impacts_str = "\n".join(oil_items)
        impacts.append(f"🛢️ <b>OIL & ENERGY</b> — Impacts: Energy sector (XLE), inflation → rates, CAD (oil-linked)\n{impacts_str}")
    
    # Rates → Growth stocks, crypto, bonds
    if macro_themes.get("rates"):
        rate_articles = macro_themes["rates"][:3]
        rate_items = []
        for a in rate_articles:
            title = a['title'][:80]
            rate_items.append(f"• {title}...")
        impacts_str = "\n".join(rate_items)
        impacts.append(f"📊 <b>INTEREST RATES / FED</b> — Impacts: Growth stocks (NVDA, AMZN), Crypto (risk-off), Bonds, USD\n{impacts_str}")
    
    # USD → Crypto, commodities, international
    if macro_themes.get("usd"):
        usd_articles = macro_themes["usd"][:3]
        usd_items = []
        for a in usd_articles:
            title = a['title'][:80]
            usd_items.append(f"• {title}...")
        impacts_str = "\n".join(usd_items)
        impacts.append(f"💵 <b>US DOLLAR (DXY)</b> — Impacts: Crypto (inverse correlation), Commodities, Int'l earnings (AMZN)\n{impacts_str}")
    
    # AI → NVDA, AMZN (AWS), semis
    if macro_themes.get("ai"):
        ai_articles = macro_themes["ai"][:3]
        ai_items = []
        for a in ai_articles:
            title = a['title'][:80]
            ai_items.append(f"• {title}...")
        impacts_str = "\n".join(ai_items)
        impacts.append(f"🤖 <b>AI / SEMICONDUCTORS</b> — Direct: NVDA | Indirect: AMZN (AWS), semis supply chain\n{impacts_str}")
    
    # Crypto regulation → BTC, ETH, SOL, XRP
    if macro_themes.get("crypto_reg"):
        reg_articles = macro_themes["crypto_reg"][:3]
        reg_items = []
        for a in reg_articles:
            title = a['title'][:80]
            reg_items.append(f"• {title}...")
        impacts_str = "\n".join(reg_items)
        impacts.append(f"⚖️ <b>CRYPTO REGULATION</b> — Direct: BTC, ETH, SOL, XRP | ETF flows, institutional adoption\n{impacts_str}")
    
    # Inflation → Rates, commodities, value vs growth
    if macro_themes.get("inflation"):
        inf_articles = macro_themes["inflation"][:2]
        inf_items = []
        for a in inf_articles:
            title = a['title'][:80]
            inf_items.append(f"• {title}...")
        impacts_str = "\n".join(inf_items)
        impacts.append(f"📈 <b>INFLATION DATA</b> — Impacts: Fed policy → rates, Value (VDY) vs Growth (NVDA/AMZN), Commodities\n{impacts_str}")
    
    # China → Commodities, global growth, tech supply chain
    if macro_themes.get("china"):
        cn_articles = macro_themes["china"][:2]
        cn_items = []
        for a in cn_articles:
            title = a['title'][:80]
            cn_items.append(f"• {title}...")
        impacts_str = "\n".join(cn_items)
        impacts.append(f"🇨🇳 <b>CHINA / EM</b> — Impacts: Commodities, Global growth, Tech supply chain (NVDA, AMZN)\n{impacts_str}")
    
    return impacts

# ============================================================
# NEWS & SCORING
# ============================================================
def fetch_feed(url: str) -> List[Dict]:
    try:
        fp = _import_feedparser()
        req = _import_requests()
        resp = req.get(url, timeout=10, headers={"User-Agent": "PortfolioNewsBot/1.0"})
        feed = fp.parse(resp.content)
        items = []
        for entry in feed.entries[:20]:
            items.append({
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "summary": entry.get("summary", entry.get("description", "")),
                "published": entry.get("published", ""),
            })
        return items
    except Exception as e:
        print(f"Feed error {url}: {e}")
        return []

def score_relevance(title: str, summary: str, ticker: str) -> int:
    text = f"{title} {summary}".lower()
    t_l = ticker.lower()
    if t_l in text: base = 8
    elif ticker in ["BTC","ETH","SOL","XRP"] and any(c in text for c in ["bitcoin","ethereum","solana","ripple","xrp"]): base = 7
    elif ticker in ["AMZN","NVDA","VDY"] and any(c in text for c in ["amazon","nvidia","vanguard","dividend"]): base = 7
    else: base = 2
    keywords = ["earnings","guidance","revenue","profit","loss","beat","miss","acquisition","merger","buyback","dividend","split","ipo",
                "sec","regulation","lawsuit","investigation","fine","partnership","contract","deal","launch","product",
                "upgrade","downgrade","target price","price target","whale","etf","institutional","adoption","integration"]
    return min(10, base + sum(1 for k in keywords if k in text))

def extract_tickers(text: str) -> List[str]:
    tl = text.lower()
    found = []
    for t in ALL_TICKERS:
        t_l = t.lower()
        if t_l in tl: found.append(t)
        elif t=="BTC" and "bitcoin" in tl: found.append(t)
        elif t=="ETH" and "ethereum" in tl: found.append(t)
        elif t=="SOL" and "solana" in tl: found.append(t)
        elif t=="XRP" and "ripple" in tl: found.append(t)
        elif t=="AMZN" and "amazon" in tl: found.append(t)
        elif t=="NVDA" and "nvidia" in tl: found.append(t)
        elif t=="VDY" and ("vanguard" in tl or "dividend" in tl): found.append(t)
    return list(set(found))

# ============================================================
# NVIDIA NEMOTRON LLM FOR NEWS SUMMARIZATION
# ============================================================
def summarize_news_with_llm(ticker: str, articles: List[Dict]) -> str:
    """Use NVIDIA Nemotron to summarize news articles for a ticker."""
    if not NVIDIA_API_KEY or not articles:
        return "No recent news available."
    
    try:
        req = _import_requests()
        articles_text = "\n\n".join([f"Title: {a['title']}\nSummary: {a['summary'][:500]}" for a in articles[:5]])
        
        prompt = f"""Summarize the key news for {ticker} from these articles in 3-4 concise bullet points. Focus on: price catalysts, earnings, guidance, products, partnerships, regulation, macro factors. Be specific with numbers/dates where mentioned.

Articles:
{articles_text}

Summary for {ticker}:"""
        
        resp = req.post(
            "https://integrate.api.nvidia.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {NVIDIA_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "nvidia/nemotron-3-ultra",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 300,
                "temperature": 0.3
            },
            timeout=30
        )
        
        if resp.status_code == 200:
            return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"LLM summary error {ticker}: {e}")
    
    # Fallback: simple extractive summary
    summaries = []
    for a in articles[:3]:
        s = a['summary'][:200].strip()
        if s: summaries.append(f"• {s}...")
    return "\n".join(summaries) if summaries else "No recent news."

# ============================================================
# PRICE & ANALYTICS
# ============================================================
def fetch_price_yahoo(ticker: str) -> Optional[float]:
    try:
        req = _import_requests()
        # Handle Canadian ETFs
        if ticker == "VDY":
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}.TO"
        elif ticker in ["BTC","ETH","SOL","XRP"]:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}-USD"
        else:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        resp = req.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        return float(resp.json()["chart"]["result"][0]["meta"]["regularMarketPrice"])
    except Exception as e:
        print(f"Price error {ticker}: {e}")
        return None

def fetch_price_history(ticker: str, days: int = 90) -> List[float]:
    try:
        req = _import_requests()
        if ticker == "VDY":
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}.TO"
        elif ticker in ["BTC","ETH","SOL","XRP"]:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}-USD"
        else:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        url += f"?period1={int(time.time())-days*86400}&period2={int(time.time())}&interval=1d"
        data = req.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"}).json()
        return [c for c in data["chart"]["result"][0]["indicators"]["quote"][0]["close"] if c is not None]
    except Exception as e:
        print(f"History error {ticker}: {e}")
        return []

def fetch_analyst_data(ticker: str) -> Dict:
    """Fetch analyst ratings and price targets from Yahoo Finance."""
    try:
        req = _import_requests()
        if ticker == "VDY":
            url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}.TO?modules=recommendationTrend,financialData"
        elif ticker in ["BTC","ETH","SOL","XRP"]:
            url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}-USD?modules=recommendationTrend,financialData"
        else:
            url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=recommendationTrend,financialData"
        resp = req.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        data = resp.json().get("quoteSummary", {}).get("result", [{}])[0]
        
        # Analyst recommendation trend
        rec = data.get("recommendationTrend", {}).get("trend", [{}])[0]
        buy = rec.get("buy", 0)
        hold = rec.get("hold", 0)
        sell = rec.get("sell", 0)
        strong_buy = rec.get("strongBuy", 0)
        strong_sell = rec.get("strongSell", 0)
        analyst_count = buy + hold + sell + strong_buy + strong_sell
        
        # Determine consensus
        total_bullish = strong_buy + buy
        total_bearish = strong_sell + sell
        if total_bullish > total_bearish + hold:
            consensus = "BUY"
        elif total_bearish > total_bullish + hold:
            consensus = "SELL"
        else:
            consensus = "HOLD"
        
        # Price target
        fin_data = data.get("financialData", {})
        target = fin_data.get("targetMeanPrice", {}).get("raw")
        current = fin_data.get("currentPrice", {}).get("raw")
        
        return {
            "analyst_rating": consensus,
            "analyst_target": target,
            "analyst_count": analyst_count,
            "analyst_breakdown": {"strongBuy": strong_buy, "buy": buy, "hold": hold, "sell": sell, "strongSell": strong_sell},
            "current_price": current
        }
    except Exception as e:
        print(f"Analyst data error {ticker}: {e}")
        return {"analyst_rating": "N/A", "analyst_target": None, "analyst_count": 0, "analyst_breakdown": {}, "current_price": None}

def compute_rsi(prices: List[float], period: int = 14) -> float:
    if len(prices) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        d = prices[i] - prices[i-1]
        gains.append(d if d > 0 else 0)
        losses.append(-d if d < 0 else 0)
    ag = sum(gains[-period:]) / period
    al = sum(losses[-period:]) / period
    return 100 if al == 0 else 100 - 100/(1 + ag/al)

def generate_analytics(ticker: str, price: float, news_items: List[Dict]) -> Dict:
    hist = fetch_price_history(ticker, 90)
    analyst = fetch_analyst_data(ticker)
    
    if len(hist) < 20:
        return default_analytics(ticker, price, analyst, "Insufficient price history")
    
    # Price changes
    price_change_1d = ((price - hist[-1]) / hist[-1] * 100) if len(hist) >= 2 else 0
    price_change_7d = ((price - hist[-7]) / hist[-7] * 100) if len(hist) >= 8 else 0
    price_change_30d = ((price - hist[-30]) / hist[-30] * 100) if len(hist) >= 31 else 0
    
    # Technical indicators
    rsi = compute_rsi(hist)
    sma20 = sum(hist[-20:]) / 20
    sma50 = sum(hist[-50:]) / 50 if len(hist) >= 50 else sma20
    v20 = (price - sma20) / sma20 * 100
    v50 = (price - sma50) / sma50 * 100
    
    # ==================== SHORT-TERM SIGNAL (1-4 weeks) ====================
    # Weighted toward momentum, RSI, recent news sentiment
    score_short, reasons_short = 50, []
    
    # RSI (short-term weight: 20%)
    if rsi > 75: score_short -= 12; reasons_short.append(f"RSI overbought ({rsi:.0f})")
    elif rsi > 65: score_short -= 5; reasons_short.append(f"RSI elevated ({rsi:.0f})")
    elif rsi < 25: score_short += 12; reasons_short.append(f"RSI oversold ({rsi:.0f})")
    elif rsi < 35: score_short += 5; reasons_short.append(f"RSI low ({rsi:.0f})")
    else: reasons_short.append(f"RSI neutral ({rsi:.0f})")
    
    # Short-term trend (10d momentum, weight: 20%)
    m10 = (price - hist[-10]) / hist[-10] * 100 if len(hist) >= 10 else 0
    if m10 > 8: score_short += 10; reasons_short.append(f"Strong 10d momentum (+{m10:.1f}%)")
    elif m10 > 3: score_short += 5; reasons_short.append(f"Positive 10d ({m10:.1f}%)")
    elif m10 < -8: score_short -= 10; reasons_short.append(f"Weak 10d ({m10:.1f}%)")
    elif m10 < -3: score_short -= 5; reasons_short.append(f"Negative 10d ({m10:.1f}%)")
    
    # Price vs SMA20 (weight: 15%)
    if v20 > 5: score_short += 8; reasons_short.append(f"Above SMA20 ({v20:.1f}%)")
    elif v20 < -5: score_short -= 8; reasons_short.append(f"Below SMA20 ({v20:.1f}%)")
    
    # Recent news sentiment (weight: 25%) - high score news = positive catalyst
    high_score_news = sum(1 for n in news_items if n.get('score', 0) >= 8)
    if high_score_news >= 3: score_short += 10; reasons_short.append(f"High-impact news ({high_score_news} items)")
    elif high_score_news >= 1: score_short += 5; reasons_short.append(f"Positive catalysts ({high_score_news} items)")
    
    # Analyst consensus (short-term weight: 20%)
    if analyst["analyst_rating"] == "BUY": score_short += 8; reasons_short.append("Analyst consensus: BUY")
    elif analyst["analyst_rating"] == "SELL": score_short -= 8; reasons_short.append("Analyst consensus: SELL")
    else: reasons_short.append("Analyst consensus: HOLD")
    
    signal_short = "BUY" if score_short >= 62 else "SELL" if score_short <= 38 else "HOLD"
    conf_short = min(95, max(40, abs(score_short - 50) + 40))
    target_short = price * (1.10 if signal_short == "BUY" else 0.90 if signal_short == "SELL" else 1.0)
    
    # ==================== LONG-TERM SIGNAL (3-12 months) ====================
    # Weighted toward fundamentals, trend, analyst targets, macro
    score_long, reasons_long = 50, []
    
    # Long-term trend (30d/60d, weight: 25%)
    m30 = (price - hist[-30]) / hist[-30] * 100 if len(hist) >= 30 else 0
    m60 = (price - hist[-60]) / hist[-60] * 100 if len(hist) >= 60 else 0
    if m30 > 15: score_long += 10; reasons_long.append(f"Strong 30d trend (+{m30:.1f}%)")
    elif m30 > 5: score_long += 5; reasons_long.append(f"Positive 30d ({m30:.1f}%)")
    elif m30 < -15: score_long -= 10; reasons_long.append(f"Weak 30d ({m30:.1f}%)")
    elif m30 < -5: score_long -= 5; reasons_long.append(f"Negative 30d ({m30:.1f}%)")
    
    if m60 > 25: score_long += 8; reasons_long.append(f"Strong 60d trend (+{m60:.1f}%)")
    elif m60 < -25: score_long -= 8; reasons_long.append(f"Weak 60d ({m60:.1f}%)")
    
    # Price vs SMA50 (weight: 20%)
    if v50 > 15: score_long += 10; reasons_long.append(f"Well above SMA50 ({v50:.1f}%)")
    elif v50 > 5: score_long += 5; reasons_long.append(f"Above SMA50 ({v50:.1f}%)")
    elif v50 < -15: score_long -= 10; reasons_long.append(f"Well below SMA50 ({v50:.1f}%)")
    elif v50 < -5: score_long -= 5; reasons_long.append(f"Below SMA50 ({v50:.1f}%)")
    
    # Analyst price target upside (weight: 25%)
    if analyst["analyst_target"] and analyst["current_price"]:
        upside = (analyst["analyst_target"] - analyst["current_price"]) / analyst["current_price"] * 100
        if upside > 20: score_long += 12; reasons_long.append(f"Analyst target +{upside:.0f}% upside")
        elif upside > 10: score_long += 6; reasons_long.append(f"Analyst target +{upside:.0f}% upside")
        elif upside < -10: score_long -= 8; reasons_long.append(f"Analyst target {upside:.0f}% downside")
        elif upside < 0: score_long -= 4; reasons_long.append(f"Analyst target {upside:.0f}% downside")
    
    # Analyst consensus (weight: 15%)
    if analyst["analyst_rating"] == "BUY": score_long += 8; reasons_long.append("Analyst consensus: BUY")
    elif analyst["analyst_rating"] == "SELL": score_long -= 8; reasons_long.append("Analyst consensus: SELL")
    else: reasons_long.append("Analyst consensus: HOLD")
    
    # Sector/theme tailwinds (simplified, weight: 15%)
    if ticker in ["NVDA"]: score_long += 8; reasons_long.append("AI infrastructure tailwind")
    elif ticker in ["BTC", "ETH"]: score_long += 5; reasons_long.append("Crypto adoption cycle")
    elif ticker == "AMZN": score_long += 5; reasons_long.append("Cloud/AI growth driver")
    elif ticker == "VDY": reasons_long.append("Dividend defensive play")
    
    signal_long = "BUY" if score_long >= 62 else "SELL" if score_long <= 38 else "HOLD"
    conf_long = min(95, max(40, abs(score_long - 50) + 40))
    target_long = price * (1.20 if signal_long == "BUY" else 0.80 if signal_long == "SELL" else 1.0)
    
    # News summary via LLM
    news_summary = summarize_news_with_llm(ticker, news_items)
    
    return {
        "signal_short": signal_short, "confidence_short": conf_short, "reasoning_short": "; ".join(reasons_short),
        "signal_long": signal_long, "confidence_long": conf_long, "reasoning_long": "; ".join(reasons_long),
        "price": round(price, 2), "target_price_short": round(target_short, 2), "target_price_long": round(target_long, 2),
        "price_change_1d": round(price_change_1d, 2), "price_change_7d": round(price_change_7d, 2), "price_change_30d": round(price_change_30d, 2),
        "analyst_rating": analyst["analyst_rating"], "analyst_target": analyst["analyst_target"], "analyst_count": analyst["analyst_count"],
        "news_summary": news_summary
    }

def default_analytics(ticker: str, price: float, analyst: Dict, reason: str) -> Dict:
    return {
        "signal_short": "HOLD", "confidence_short": 30, "reasoning_short": reason,
        "signal_long": "HOLD", "confidence_long": 30, "reasoning_long": reason,
        "price": round(price, 2) if price else 0, "target_price_short": round(price, 2) if price else 0, "target_price_long": round(price, 2) if price else 0,
        "price_change_1d": 0, "price_change_7d": 0, "price_change_30d": 0,
        "analyst_rating": analyst.get("analyst_rating", "N/A"), "analyst_target": analyst.get("analyst_target"), "analyst_count": analyst.get("analyst_count", 0),
        "news_summary": "No data available."
    }

# ============================================================
# TELEGRAM FORMATTING
# ============================================================
def send_telegram(msg: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return False
    try:
        req = _import_requests()
        resp = req.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                        json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
        return resp.status_code == 200
    except Exception as e:
        print(f"Telegram error: {e}")
        return False


def format_price_change(pct: float) -> str:
    """Format price change with visual indicator."""
    if pct > 0:
        return f"🟢 +{pct:.2f}%"
    elif pct < 0:
        return f"🔴 {pct:.2f}%"
    return "⚪ 0.00%"


def format_digest(news: Dict[str, List[Dict]], analytics: Dict[str, Dict], macro_analysis: List[str] = None) -> str:
    today = datetime.now().strftime("%B %d, %Y")
    lines = [
        f"📊 <b>Portfolio Digest — {today}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        ""
    ]

    # ── QUICK LEGEND ───
    lines.append("📖 <b>How to Read This Digest</b>")
    lines.append("  • <b>Signal</b> = BUY (consider adding), HOLD (stay put), SELL (consider reducing)")
    lines.append("  • <b>Confidence</b> = How certain the model is (40% = low, 95% = high)")
    lines.append("  • <b>Target Price</b> = Where the model thinks the price could go")
    lines.append("  • <b>Short Term</b> = 1 to 4 weeks outlook  |  <b>Long Term</b> = 3 to 12 months outlook")
    lines.append("  • <b>Price Changes</b> = 🟢 Green = up  |  🔴 Red = down  |  ⚪ Gray = flat")
    lines.append("  • <b>Analyst Consensus</b> = What Wall Street professionals think on average")
    lines.append("")

    # ── SIGNALS & TARGETS grouped by asset class ───
    lines.append("📈 <b>SIGNALS & TARGETS BY HOLDING</b>")
    lines.append("")

    for class_label, tickers in ASSET_CLASSES.items():
        lines.append(f"{class_label}")
        lines.append("─" * 20)
        
        for t in tickers:
            a = analytics.get(t)
            if not a:
                lines.append(f"  {t}: ⏳ No data available")
                lines.append("")
                continue
            
            emoji_s = {"BUY": "🟢", "HOLD": "🟡", "SELL": "🔴"}.get(a["signal_short"], "⚪")
            emoji_l = {"BUY": "🟢", "HOLD": "🟡", "SELL": "🔴"}.get(a["signal_long"], "⚪")
            
            p = a["price"]
            ts = a["target_price_short"]
            tl = a["target_price_long"]
            pct_s = ((ts - p) / p * 100) if p else 0
            pct_l = ((tl - p) / p * 100) if p else 0
            
            # Price changes with visual indicators
            ch1 = format_price_change(a["price_change_1d"])
            ch7 = format_price_change(a["price_change_7d"])
            ch30 = format_price_change(a["price_change_30d"])
            
            # Confidence stars
            conf_s_stars = "★" * (a["confidence_short"] // 20) + "☆" * (5 - a["confidence_short"] // 20)
            conf_l_stars = "★" * (a["confidence_long"] // 20) + "☆" * (5 - a["confidence_long"] // 20)
            
            # Signal explanation in plain English
            short_explanation = {
                "BUY": "Consider adding to your position — the model sees upside in the next 1-4 weeks",
                "HOLD": "Stay with your current position — no strong reason to buy or sell right now",
                "SELL": "Consider reducing your position — the model sees downside risk in the next 1-4 weeks"
            }.get(a["signal_short"], "No clear signal")
            
            long_explanation = {
                "BUY": "Consider adding for the long run — the model sees multi-month upside",
                "HOLD": "Keep holding — the long-term thesis is intact but no urgent action needed",
                "SELL": "Consider reducing long-term exposure — structural headwinds expected"
            }.get(a["signal_long"], "No clear signal")
            
            # Current price and recent performance
            lines.append(f"  {emoji_s} <b>{t}</b> — Current Price: ${p:.2f}")
            lines.append(f"     Recent Performance: 1-Day {ch1}  |  7-Day {ch7}  |  30-Day {ch30}")
            lines.append("")
            
            # Short-term signal
            lines.append(f"     ┌ <b>SHORT TERM (1-4 Weeks)</b>: {a['signal_short']}  {conf_s_stars}  Confidence: {a['confidence_short']}%")
            lines.append(f"     │   What this means: {short_explanation}")
            lines.append(f"     │   Target Price: ${ts:.2f}  ({pct_s:+.1f}% from current)")
            lines.append(f"     │   Key Reasons: {a['reasoning_short']}")
            lines.append(f"     └ ")
            
            # Long-term signal
            lines.append(f"     ┌ <b>LONG TERM (3-12 Months)</b>: {a['signal_long']}  {conf_l_stars}  Confidence: {a['confidence_long']}%")
            lines.append(f"     │   What this means: {long_explanation}")
            lines.append(f"     │   Target Price: ${tl:.2f}  ({pct_l:+.1f}% from current)")
            lines.append(f"     │   Key Reasons: {a['reasoning_long']}")
            lines.append(f"     └ ")
            
            # Analyst opinions section
            if a['analyst_target'] and a['analyst_target'] > 0:
                analyst_upside = ((a['analyst_target'] - p) / p * 100) if p else 0
                lines.append(f"     📊 <b>Wall Street Analyst Consensus</b>")
                lines.append(f"        • Average Rating: {a['analyst_rating']}  (from {a['analyst_count']} analysts)")
                lines.append(f"        • Average Price Target: ${a['analyst_target']:.2f}  ({analyst_upside:+.1f}% vs current ${p:.2f})")
                if analyst_upside > 15:
                    lines.append(f"        • Interpretation: Analysts are <b>bullish</b> — significant upside expected")
                elif analyst_upside > 5:
                    lines.append(f"        • Interpretation: Analysts are <b>moderately positive</b> — modest upside expected")
                elif analyst_upside > -5:
                    lines.append(f"        • Interpretation: Analysts are <b>neutral</b> — fairly valued near current price")
                elif analyst_upside > -15:
                    lines.append(f"        • Interpretation: Analysts are <b>cautious</b> — some downside risk seen")
                else:
                    lines.append(f"        • Interpretation: Analysts are <b>bearish</b> — significant downside expected")
            else:
                lines.append(f"     📊 <b>Wall Street Analyst Consensus</b>: {a['analyst_rating']} ({a['analyst_count']} analysts) — No price target available")
            lines.append("")

    # ── MACRO ANALYSIS ───
    if macro_analysis:
        lines.append("🌍 <b>MACRO DRIVERS & HOW THEY AFFECT YOUR HOLDINGS</b>")
        lines.append("─" * 20)
        lines.append("<i>These are big-picture economic and market forces that move multiple holdings at once.</i>")
        lines.append("")
        for item in macro_analysis:
            lines.append(f"  {item}")
        lines.append("")

    # ── NEWS SUMMARIES grouped by asset class ───
    lines.append("📰 <b>RECENT NEWS SUMMARIES (Last 24-48 Hours)</b>")
    lines.append("─" * 20)
    lines.append("<i>Only material, high-relevance news is shown. Each item is scored for relevance to your specific holding.</i>")
    lines.append("")
    
    any_news = False
    for class_label, tickers in ASSET_CLASSES.items():
        class_has_news = False
        class_lines = []
        
        for t in tickers:
            items = news.get(t, [])
            if not items:
                continue
            
            if not class_has_news:
                class_lines.append(f"  {class_label}")
                class_has_news = True
            
            any_news = True
            class_lines.append(f"    <b>{t}</b> — {len(items)} relevant article{'s' if len(items) > 1 else ''} found")
            
            # Show actual news headlines/URLs
            for item in items[:3]:  # Limit to top 3 per ticker
                title = item.get('title', 'No title')
                url = item.get('url', '')
                if url:
                    class_lines.append(f"      • <a href=\"{url}\">{title}</a>")
                else:
                    class_lines.append(f"      • {title}")
            class_lines.append("")
        
        if class_has_news:
            lines.extend(class_lines)
    
    if not any_news:
        lines.append("  No material ticker-specific news in recent feeds.")
    
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"<i>Generated {datetime.now().strftime('%H:%M ET')} | Next run: Weekday 8:00 AM ET</i>")
    lines.append("<i>Disclaimer: This is automated analysis, not financial advice. Do your own research.</i>")
    
    return "\n".join(lines)

# ============================================================
# VERCEL HANDLER
# ============================================================
def handler(request):
    headers = request.get("headers", {}) if isinstance(request, dict) else {}
    auth = headers.get("authorization") or headers.get("Authorization", "")
    if CRON_SECRET and (not auth.startswith("Bearer ") or not hmac.compare_digest(auth[7:], CRON_SECRET)):
        return {"statusCode": 401, "body": json.dumps({"error": "Unauthorized"})}
    
    conn = init_db()
    cleanup_old(conn)
    
    # Fetch all news (including macro feeds)
    all_items = []
    for feeds in NEWS_FEEDS.values():
        for url in feeds:
            feed_items = fetch_feed(url)
            all_items.extend(feed_items)
    
    # Extract macro themes from ALL articles (not just ticker-specific)
    macro_themes = extract_macro_themes(all_items)
    macro_analysis = analyze_macro_impact(macro_themes, {t: {"ticker": t} for t in ALL_TICKERS})
    
    # Score & filter per ticker
    news_by_ticker = {t: [] for t in ALL_TICKERS}
    for item in all_items:
        mentioned = extract_tickers(f"{item['title']} {item['summary']}")
        for t in mentioned:
            score = score_relevance(item['title'], item['summary'], t)
            if score >= ALERT_THRESHOLD:
                h = hashlib.md5(f"{t}{item['link']}".encode()).hexdigest()
                if not is_duplicate(conn, h):
                    mark_seen(conn, h, item['title'], item['link'], t, score)
                    news_by_ticker[t].append({"title": item['title'], "url": item['link'], "score": score, "summary": item['summary']})
    
    # Analytics per ticker
    analytics = {}
    for t in ALL_TICKERS:
        cached = get_cached_analytics(conn, t)
        if cached:
            analytics[t] = cached
            continue
        price = fetch_price_yahoo(t)
        if price:
            items = news_by_ticker.get(t, [])
            res = generate_analytics(t, price, items)
            cache_analytics(conn, t, res)
            analytics[t] = res
        else:
            analyst = fetch_analyst_data(t)
            analytics[t] = default_analytics(t, 0, analyst, "Price fetch failed")
    
    # Send digest with macro analysis
    msg = format_digest(news_by_ticker, analytics, macro_analysis)
    ok = send_telegram(msg)
    conn.close()
    
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": json.dumps({"success": ok, "news": sum(len(v) for v in news_by_ticker.values()), "analytics": len(analytics)})}

# Alias for Vercel detection
app = handler
application = handler