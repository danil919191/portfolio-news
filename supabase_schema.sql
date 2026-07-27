-- Run this in Supabase SQL Editor (Dashboard → SQL Editor → New Query)

-- Table for deduplication (news items already seen)
CREATE TABLE IF NOT EXISTS seen_items (
    id BIGSERIAL PRIMARY KEY,
    item_hash TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    ticker TEXT NOT NULL,
    score INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for fast dedupe lookups
CREATE INDEX IF NOT EXISTS idx_seen_items_hash ON seen_items(item_hash);
CREATE INDEX IF NOT EXISTS idx_seen_items_ticker ON seen_items(ticker);
CREATE INDEX IF NOT EXISTS idx_seen_items_created ON seen_items(created_at DESC);

-- Table for cached analytics (signals, targets)
CREATE TABLE IF NOT EXISTS analytics_cache (
    ticker TEXT PRIMARY KEY,
    signal TEXT NOT NULL CHECK (signal IN ('BUY', 'HOLD', 'SELL')),
    confidence INTEGER NOT NULL CHECK (confidence BETWEEN 0 AND 100),
    reasoning TEXT NOT NULL,
    price DECIMAL(12, 4) NOT NULL,
    target_price DECIMAL(12, 4) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table for cron run logs
CREATE TABLE IF NOT EXISTS cron_logs (
    id BIGSERIAL PRIMARY KEY,
    run_at TIMESTAMPTZ DEFAULT NOW(),
    success BOOLEAN NOT NULL,
    news_count INTEGER DEFAULT 0,
    analytics_count INTEGER DEFAULT 0,
    error_message TEXT,
    duration_ms INTEGER
);

-- Enable Row Level Security (optional, for future)
ALTER TABLE seen_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE analytics_cache ENABLE ROW LEVEL SECURITY;
ALTER TABLE cron_logs ENABLE ROW LEVEL SECURITY;

-- Allow anon/service_role to do everything (adjust for production)
CREATE POLICY "Allow all for service role" ON seen_items
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Allow all for service role" ON analytics_cache
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Allow all for service role" ON cron_logs
    FOR ALL USING (auth.role() = 'service_role');