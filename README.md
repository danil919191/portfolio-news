# Portfolio News & Analytics Bot

Daily 8 AM ET digest to Telegram with:
- **News**: Material news for AMZN, BTC, ETH, NVDA, SOL, VDY, XRP (score ≥ 7/10)
- **Analytics**: BUY/HOLD/SELL signal + price target + confidence for each holding

---

## Quick Deploy (5 min)

### 1. Supabase (Database)
1. Go to [supabase.com](https://supabase.com) → New Project → `portfolio-news`
2. **Settings → API** → Copy **Project URL** and **anon public** key
3. **SQL Editor** → Paste `supabase_schema.sql` → Run

### 2. Telegram Bot
1. Message `@BotFather` → `/newbot` → Name: `My Portfolio Bot` → Username: `yourname_portfolio_bot`
2. Copy **HTTP API token** (looks like `123456:ABC-DEF...`)
3. Your chat ID: `5188494297` (already known)

### 3. GitHub → Vercel
1. Create new repo on GitHub (e.g., `danil/portfolio-news`)
2. Push this code:
   ```bash
   git init
   git add .
   git commit -m "Initial commit"
   git remote add origin https://github.com/danil/portfolio-news.git
   git push -u origin main
   ```
3. Go to [vercel.com](https://vercel.com) → Import GitHub repo
4. **Environment Variables** (add all 4):
   ```
   TELEGRAM_BOT_TOKEN=your_bot_token
   TELEGRAM_CHAT_ID=5188494297
   SUPABASE_URL=https://xxx.supabase.co
   SUPABASE_ANON_KEY=eyJ...
   ```
5. Deploy → Vercel auto-detects `vercel.json` cron (runs Mon-Fri 8 AM ET)

### 4. Test It
- Vercel Dashboard → Functions → `api/cron` → **Invoke** → Check Telegram

---

## Local Development Tools → Logs

---

## Your Holdings
| Asset | Type |
|-------|------|
| AMZN | Stock |
| NVDA | Stock |
| VDY | Stock (Vanguard Canadian Dividend ETF) |
| BTC | Crypto |
| ETH | Crypto |
| SOL | Crypto |
| XRP | Crypto |

---

## Customization
- **Cadence**: Edit `vercel.json` cron schedule (cron syntax, UTC)
- **Threshold**: Change `ALERT_THRESHOLD` in `api/cron.py` (default 7/10)
- **Quiet hours**: Already handled (cron runs 8 AM ET weekdays only)
- **More tickers**: Add to `ALL_TICKERS` and `TICKER_META` in `api/cron.py`

---

## Files
```
├── api/cron.py           # Main handler (Vercel entry point)
├── vercel.json           # Vercel config + cron schedule
├── requirements.txt      # Python deps
├── supabase_schema.sql   # Database schema
├── .github/workflows/    # GitHub Actions backup cron
└── .env.example          # Env var template
```

---

## Monitoring
- **Vercel Logs**: Dashboard → Functions → `api/cron` → Logs
- **Supabase**: Table Editor → `cron_logs` for run history
- **Telegram**: Daily message at 8 AM ET