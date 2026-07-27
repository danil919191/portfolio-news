import os
import json
import sqlite3
import hashlib
import hmac
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
import requests
import feedparser
from bs4 import BeautifulSoup

# ============================================================
# CONFIGURATION
# ============================================================
HOLDINGS = {
    "stocks": ["AMZN", "NVDA", "VDY"],
    "crypto": ["BTC", "ETH", "SOL", "XRP"]
}

ALL_TICKERS = HOLDINGS["stocks"] + HOLDINGS["crypto"]

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
        "https://www.theblock.co/rss",
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
# DATABASE (SQLite - ephemeral on Vercel, but works per-run)
# ============================================================
DB_PATH = Path("/tmp/portfolio_news.db")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_items (
            hash TEXT PRIMARY KEY,
            title TEXT,
            url TEXT,
            ticker TEXT,
            score INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analytics_cache (
            ticker TEXT PRIMARY KEY,
            signal TEXT,
            confidence INTEGER,
            reasoning TEXT,
            price REAL,
            target_price REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn

def is_duplicate(conn, item_hash: str) -> bool:
    cur = conn.execute("SELECT 1 FROM seen_items WHERE hash = ?", (item_hash,))
    return cur.fetchone() is not None

def mark_seen(conn, item_hash: str, title: str, url: str, ticker: str, score: int):
    conn.execute(
        "INSERT OR IGNORE INTO seen_items (hash, title, url, ticker, score) VALUES (?, ?, ?, ?, ?)",
        (item_hash, title, url, ticker, score)
    )
    conn.commit()

def cleanup_old(conn, hours: int = DEDUPE_HOURS):
    cutoff = datetime.now() - timedelta(hours=hours)
    conn.execute("DELETE FROM seen_items WHERE created_at < ?", (cutoff,))
    conn.commit()

def get_cached_analytics(conn, ticker: str) -> Optional[Dict]:
    cur = conn.execute(
        "SELECT signal, confidence, reasoning, price, target_price FROM analytics_cache WHERE ticker = ?",
        (ticker,)
    )
    row = cur.fetchone()
    if row:
        return {
            "signal": row[0], "confidence": row[1], "reasoning": row[2],
            "price": row[3], "target_price": row[4]
        }
    return None

def cache_analytics(conn, ticker: str, signal: str, confidence: int, reasoning: str, price: float, target: float):
    conn.execute("""
        INSERT OR REPLACE INTO analytics_cache (ticker, signal, confidence, reasoning, price, target_price, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (ticker, signal, confidence, reasoning, price, target, datetime.now()))
    conn.commit()

# ============================================================
# NEWS FETCHING & SCORING
# ============================================================
def fetch_feed(url: str) -> List[Dict]:
    try:
        response = requests.get(url, timeout=10, headers={"User-Agent": "PortfolioNewsBot/1.0"})
        feed = feedparser.parse(response.content)
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
        print(f"Error fetching {url}: {e}")
        return []

def score_relevance(title: str, summary: str, ticker: str) -> int:
    text = f"{title} {summary}".lower()
    ticker_lower = ticker.lower()
    
    if ticker_lower in text:
        base = 8
    elif ticker in ["BTC", "ETH", "SOL", "XRP"] and any(c in text for c in ["bitcoin", "ethereum", "solana", "ripple", "xrp"]):
        base = 7
    elif ticker in ["AMZN", "NVDA", "VDY"] and any(c in text for c in ["amazon", "nvidia", "vanguard", "dividend"]):
        base = 7
    else:
        base = 2
    
    material_keywords = [
        "earnings", "guidance", "revenue", "profit", "loss", "beat", "miss",
        "acquisition", "merger", "buyback", "dividend", "split", "ipo",
        "sec", "regulation", "lawsuit", "investigation", "fine",
        "partnership", "contract", "deal", "launch", "product",
        "upgrade", "downgrade", "target price", "price target",
        "whale", "etf", "institutional", "adoption", "integration"
    ]
    boost = sum(1 for kw in material_keywords if kw in text)
    return min(10, base + boost)

def extract_tickers(text: str) -> List[str]:
    text_lower = text.lower()
    found = []
    for t in ALL_TICKERS:
        t_lower = t.lower()
        if t_lower in text_lower:
            found.append(t)
        elif t == "BTC" and "bitcoin" in text_lower:
            found.append(t)
        elif t == "ETH" and "ethereum" in text_lower:
            found.append(t)
        elif t == "SOL" and "solana" in text_lower:
            found.append(t)
        elif t == "XRP" and "ripple" in text_lower:
            found.append(t)
        elif t == "AMZN" and "amazon" in text_lower:
            found.append(t)
        elif t == "NVDA" and "nvidia" in text_lower:
            found.append(t)
        elif t == "VDY" and ("vanguard" in text_lower or "dividend" in text_lower):
            found.append(t)
    return list(set(found))

# ============================================================
# ANALYTICS
# ============================================================
def fetch_price_yahoo(ticker: str) -> Optional[float]:
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        if ticker in ["BTC", "ETH", "SOL", "XRP"]:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}-USD"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
        return float(price)
    except Exception as e:
        print(f"Price fetch failed for {ticker}: {e}")
        return None

def compute_rsi(prices: List[float], period: int = 14) -> float:
    if len(prices) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        if diff > 0:
            gains.append(diff)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(-diff)
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def fetch_price_history(ticker: str, days: int = 30) -> List[float]:
    try:
        interval = "1d"
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        if ticker in ["BTC", "ETH", "SOL", "XRP"]:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}-USD"
        url += f"?period1={int(time.time()) - days*86400}&period2={int(time.time())}&interval={interval}"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        return [c for c in closes if c is not None]
    except Exception as e:
        print(f"History fetch failed for {ticker}: {e}")
        return []

def generate_analytics(ticker: str, current_price: float) -> Dict:
    history = fetch_price_history(ticker, 60)
    if len(history) < 20:
        return {
            "signal": "HOLD", "confidence": 40,
            "reasoning": "Insufficient data for analysis",
            "price": current_price, "target_price": current_price
        }
    
    rsi = compute_rsi(history)
    sma_20 = sum(history[-20:]) / 20
    sma_50 = sum(history[-50:]) / 50 if len(history) >= 50 else sma_20
    price_vs_sma20 = (current_price - sma_20) / sma_20 * 100
    price_vs_sma50 = (current_price - sma_50) / sma_50 * 100
    
    momentum_10 = (current_price - history[-10]) / history[-10] * 100 if len(history) >= 10 else 0
    momentum_30 = (current_price - history[-30]) / history[-30] * 100 if len(history) >= 30 else 0
    
    score = 50
    reasons = []
    
    if rsi > 70:
        score -= 15
        reasons.append(f"RSI overbought ({rsi:.0f})")
    elif rsi < 30:
        score += 15
        reasons.append(f"RSI oversold ({rsi:.0f})")
    else:
        reasons.append(f"RSI neutral ({rsi:.0f})")
    
    if price_vs_sma20 > 5:
        score += 10
        reasons.append(f"Above SMA20 ({price_vs_sma20:.1f}%)")
    elif price_vs_sma20 < -5:
        score -= 10
        reasons.append(f"Below SMA20 ({price_vs_sma20:.1f}%)")
    
    if price_vs_sma50 > 10:
        score += 10
        reasons.append(f"Strong above SMA50 ({price_vs_sma50:.1f}%)")
    elif price_vs_sma50 < -10:
        score -= 10
        reasons.append(f"Below SMA50 ({price_vs_sma50:.1f}%)")
    
    if momentum_10 > 5:
        score += 5
        reasons.append(f"10-day momentum +{momentum_10:.1f}%")
    elif momentum_10 < -5:
        score -= 5
        reasons.append(f"10-day momentum {momentum_10:.1f}%")
    
    if momentum_30 > 15:
        score += 5
        reasons.append(f"30-day momentum +{momentum_30:.1f}%")
    elif momentum_30 < -15:
        score -= 5
        reasons.append(f"30-day momentum {momentum_30:.1f}%")
    
    if score >= 65:
        signal = "BUY"
    elif score <= 35:
        signal = "SELL"
    else:
        signal = "HOLD"
    
    if signal == "BUY":
        target = current_price * 1.15
    elif signal == "SELL":
        target = current_price * 0.85
    else:
        target = current_price
    
    return {
        "signal": signal,
        "confidence": min(95, max(40, abs(score - 50) + 40)),
        "reasoning": "; ".join(reasons),
        "price": round(current_price, 2),
        "target_price": round(target, 2)
    }

# ============================================================
# TELEGRAM DELIVERY
# ============================================================
def send_telegram(message: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram credentials not configured")
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        resp = requests.post(url, json=payload, timeout=10)
        return resp.status_code == 200
    except Exception as e:
        print(f"Telegram send failed: {e}")
        return False

def format_digest(news_by_ticker: Dict[str, List[Dict]], analytics: Dict[str, Dict]) -> str:
    today = datetime.now().strftime("%B %d, %Y")
    lines = [f"📊 <b>Portfolio Digest — {today}</b>", ""]
    
    lines.append("📈 <b>Signals & Targets</b>")
    for ticker in ALL_TICKERS:
        a = analytics.get(ticker)
        if not a:
            lines.append(f"  {ticker}: ⏳ No data")
            continue
        emoji = {"BUY": "🟢", "HOLD": "🟡", "SELL": "🔴"}.get(a["signal"], "⚪")
        change_pct = ((a["target_price"] - a["price"]) / a["price"] * 100) if a["price"] else 0
        lines.append(
            f"  {emoji} <b>{ticker}</b>: {a['signal']} ({a['confidence']}%) | "
            f"${a['price']:.2f} → ${a['target_price']:.2f} ({change_pct:+.1f}%)"
        )
        lines.append(f"     <i>{a['reasoning']}</i>")
    lines.append("")
    
    lines.append("📰 <b>Relevant News (24h)</b>")
    any_news = False
    for ticker in ALL_TICKERS:
        items = news_by_ticker.get(ticker, [])
        if not items:
            continue
        any_news = True
        lines.append(f"  <b>{ticker}</b>:")
        for item in items[:3]:
            lines.append(f"    • <a href='{item['url']}'>{item['title']}</a> (score: {item['score']}/10)")
    if not any_news:
        lines.append("  No material news in last 24h")
    
    return "\n".join(lines)

# ============================================================
# VERCEL HANDLER (Entry point)
# ============================================================
def handler(request):
    """Vercel serverless function entry point"""
    # Verify cron secret if set
    if CRON_SECRET:
        auth_header = request.headers.get("authorization", "") or request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return {
                "statusCode": 401,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Unauthorized"})
            }
        if not hmac.compare_digest(auth_header[7:], CRON_SECRET):
            return {
                "statusCode": 401,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Invalid secret"})
            }
    
    conn = init_db()
    cleanup_old(conn)
    
    # 1. Fetch all news
    all_items = []
    for category, feeds in NEWS_FEEDS.items():
        for feed_url in feeds:
            items = fetch_feed(feed_url)
            for item in items:
                item["category"] = category
            all_items.extend(items)
    
    print(f"Fetched {len(all_items)} total news items")
    
    # 2. Score and filter for our tickers
    news_by_ticker = {t: [] for t in ALL_TICKERS}
    for item in all_items:
        text = f"{item['title']} {item['summary']}"
        mentioned = extract_tickers(text)
        for ticker in mentioned:
            score = score_relevance(item['title'], item['summary'], ticker)
            if score >= ALERT_THRESHOLD:
                item_hash = hashlib.md5(f"{ticker}{item['link']}".encode()).hexdigest()
                if not is_duplicate(conn, item_hash):
                    mark_seen(conn, item_hash, item['title'], item['link'], ticker, score)
                    news_by_ticker[ticker].append({
                        "title": item['title'],
                        "url": item['link'],
                        "score": score
                    })
    
    # 3. Generate analytics for each holding
    analytics = {}
    for ticker in ALL_TICKERS:
        cached = get_cached_analytics(conn, ticker)
        if cached:
            # Check if cache is fresh (4 hours)
            analytics[ticker] = cached
            continue
        
        price = fetch_price_yahoo(ticker)
        if price:
            result = generate_analytics(ticker, price)
            cache_analytics(conn, ticker, result["signal"], result["confidence"], 
                          result["reasoning"], result["price"], result["target_price"])
            analytics[ticker] = result
        else:
            analytics[ticker] = {
                "signal": "HOLD", "confidence": 30,
                "reasoning": "Price fetch failed", "price": 0, "target_price": 0
            }
    
    # 4. Format and send digest
    message = format_digest(news_by_ticker, analytics)
    success = send_telegram(message)
    
    conn.close()
    
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "success": success,
            "news_items": sum(len(v) for v in news_by_ticker.values()),
            "analytics_count": len(analytics),
            "timestamp": datetime.now().isoformat()
        })
    }