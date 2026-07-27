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
    ]
}

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
CRON_SECRET = os.getenv("CRON_SECRET")

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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analytics_cache (
            ticker TEXT PRIMARY KEY,
            signal TEXT, confidence INTEGER, reasoning TEXT,
            price REAL, target_price REAL,
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
    row = conn.execute("SELECT signal, confidence, reasoning, price, target_price FROM analytics_cache WHERE ticker = ?", (ticker,)).fetchone()
    if row:
        return {"signal": row[0], "confidence": row[1], "reasoning": row[2], "price": row[3], "target_price": row[4]}
    return None

def cache_analytics(conn, ticker: str, signal: str, confidence: int, reasoning: str, price: float, target: float):
    conn.execute("""INSERT OR REPLACE INTO analytics_cache VALUES (?, ?, ?, ?, ?, ?, ?)""",
                 (ticker, signal, confidence, reasoning, price, target, datetime.now()))
    conn.commit()

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
# ANALYTICS
# ============================================================
def fetch_price_yahoo(ticker: str) -> Optional[float]:
    try:
        req = _import_requests()
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        if ticker in ["BTC","ETH","SOL","XRP"]: url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}-USD"
        resp = req.get(url, timeout=10)
        return float(resp.json()["chart"]["result"][0]["meta"]["regularMarketPrice"])
    except Exception as e:
        print(f"Price error {ticker}: {e}")
        return None

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

def fetch_price_history(ticker: str, days: int = 30) -> List[float]:
    try:
        req = _import_requests()
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        if ticker in ["BTC","ETH","SOL","XRP"]: url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}-USD"
        url += f"?period1={int(time.time())-days*86400}&period2={int(time.time())}&interval=1d"
        data = req.get(url, timeout=10).json()
        return [c for c in data["chart"]["result"][0]["indicators"]["quote"][0]["close"] if c is not None]
    except Exception as e:
        print(f"History error {ticker}: {e}")
        return []

def generate_analytics(ticker: str, price: float) -> Dict:
    hist = fetch_price_history(ticker, 60)
    if len(hist) < 20:
        return {"signal":"HOLD","confidence":40,"reasoning":"Insufficient data","price":price,"target_price":price}
    rsi = compute_rsi(hist)
    sma20 = sum(hist[-20:])/20
    sma50 = sum(hist[-50:])/50 if len(hist)>=50 else sma20
    v20 = (price-sma20)/sma20*100
    v50 = (price-sma50)/sma50*100
    m10 = (price-hist[-10])/hist[-10]*100 if len(hist)>=10 else 0
    m30 = (price-hist[-30])/hist[-30]*100 if len(hist)>=30 else 0
    score, reasons = 50, []
    if rsi > 70: score -= 15; reasons.append(f"RSI overbought ({rsi:.0f})")
    elif rsi < 30: score += 15; reasons.append(f"RSI oversold ({rsi:.0f})")
    else: reasons.append(f"RSI neutral ({rsi:.0f})")
    if v20 > 5: score += 10; reasons.append(f"Above SMA20 ({v20:.1f}%)")
    elif v20 < -5: score -= 10; reasons.append(f"Below SMA20 ({v20:.1f}%)")
    if v50 > 10: score += 10; reasons.append(f"Strong above SMA50 ({v50:.1f}%)")
    elif v50 < -10: score -= 10; reasons.append(f"Below SMA50 ({v50:.1f}%)")
    if m10 > 5: score += 5; reasons.append(f"10d +{m10:.1f}%")
    elif m10 < -5: score -= 5; reasons.append(f"10d {m10:.1f}%")
    if m30 > 15: score += 5; reasons.append(f"30d +{m30:.1f}%")
    elif m30 < -15: score -= 5; reasons.append(f"30d {m30:.1f}%")
    signal = "BUY" if score>=65 else "SELL" if score<=35 else "HOLD"
    target = price * (1.15 if signal=="BUY" else 0.85 if signal=="SELL" else 1)
    return {"signal":signal,"confidence":min(95,max(40,abs(score-50)+40)),"reasoning":"; ".join(reasons),"price":round(price,2),"target_price":round(target,2)}

# ============================================================
# TELEGRAM
# ============================================================
def send_telegram(msg: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return False
    try:
        req = _import_requests()
        resp = req.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                        json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML","disable_web_page_preview":True}, timeout=10)
        return resp.status_code == 200
    except Exception as e:
        print(f"Telegram error: {e}")
        return False

def format_digest(news: Dict[str,List[Dict]], analytics: Dict[str,Dict]) -> str:
    today = datetime.now().strftime("%B %d, %Y")
    lines = [f"📊 <b>Portfolio Digest — {today}</b>", "", "📈 <b>Signals & Targets</b>"]
    for t in ALL_TICKERS:
        a = analytics.get(t)
        if not a: lines.append(f"  {t}: ⏳ No data"); continue
        emoji = {"BUY":"🟢","HOLD":"🟡","SELL":"🔴"}.get(a["signal"],"⚪")
        pct = ((a["target_price"]-a["price"])/a["price"]*100) if a["price"] else 0
        lines.append(f"  {emoji} <b>{t}</b>: {a['signal']} ({a['confidence']}%) | ${a['price']:.2f} → ${a['target_price']:.2f} ({pct:+.1f}%)")
        lines.append(f"     <i>{a['reasoning']}</i>")
    lines += ["", "📰 <b>Relevant News (24h)</b>"]
    any_news = False
    for t in ALL_TICKERS:
        items = news.get(t, [])
        if not items: continue
        any_news = True
        lines.append(f"  <b>{t}</b>:")
        for it in items[:3]: lines.append(f"    • <a href='{it['url']}'>{it['title']}</a> (score: {it['score']}/10)")
    if not any_news: lines.append("  No material news in last 24h")
    return "\n".join(lines)

# ============================================================
# VERCEL HANDLER (must be at module level, named 'handler')
# ============================================================
def handler(request):
    # Vercel passes request as dict with: headers, query, body, method, etc.
    headers = request.get("headers", {}) if isinstance(request, dict) else {}
    auth = headers.get("authorization") or headers.get("Authorization", "")
    if CRON_SECRET and (not auth.startswith("Bearer ") or not hmac.compare_digest(auth[7:], CRON_SECRET)):
        return {"statusCode": 401, "body": json.dumps({"error": "Unauthorized"})}
    
    conn = init_db()
    cleanup_old(conn)
    
    # Fetch news
    all_items = []
    for feeds in NEWS_FEEDS.values():
        for url in feeds:
            for item in fetch_feed(url):
                item["category"] = feeds[0]  # rough category
            all_items.extend(feed_items := fetch_feed(url))
            all_items.extend(feed_items)
    
    # Score & filter
    news_by_ticker = {t: [] for t in ALL_TICKERS}
    for item in all_items:
        mentioned = extract_tickers(f"{item['title']} {item['summary']}")
        for t in mentioned:
            score = score_relevance(item['title'], item['summary'], t)
            if score >= ALERT_THRESHOLD:
                h = hashlib.md5(f"{t}{item['link']}".encode()).hexdigest()
                if not is_duplicate(conn, h):
                    mark_seen(conn, h, item['title'], item['link'], t, score)
                    news_by_ticker[t].append({"title": item['title'], "url": item['link'], "score": score})
    
    # Analytics
    analytics = {}
    for t in ALL_TICKERS:
        cached = get_cached_analytics(conn, t)
        if cached:
            analytics[t] = cached
            continue
        price = fetch_price_yahoo(t)
        if price:
            res = generate_analytics(t, price)
            cache_analytics(conn, t, res["signal"], res["confidence"], res["reasoning"], res["price"], res["target_price"])
            analytics[t] = res
        else:
            analytics[t] = {"signal":"HOLD","confidence":30,"reasoning":"Price fetch failed","price":0,"target_price":0}
    
    # Send
    msg = format_digest(news_by_ticker, analytics)
    ok = send_telegram(msg)
    conn.close()
    
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": json.dumps({"success": ok, "news": sum(len(v) for v in news_by_ticker.values()), "analytics": len(analytics)})}

# Alias for Vercel detection
app = handler
application = handler