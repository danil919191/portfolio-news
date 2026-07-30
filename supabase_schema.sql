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
    signal_short TEXT NOT NULL CHECK (signal_short IN ('BUY', 'HOLD', 'SELL')),
    confidence_short INTEGER NOT NULL CHECK (confidence_short BETWEEN 0 AND 100),
    reasoning_short TEXT NOT NULL,
    signal_long TEXT NOT NULL CHECK (signal_long IN ('BUY', 'HOLD', 'SELL')),
    confidence_long INTEGER NOT NULL CHECK (confidence_long BETWEEN 0 AND 100),
    reasoning_long TEXT NOT NULL,
    price DECIMAL(12, 4) NOT NULL,
    target_price_short DECIMAL(12, 4) NOT NULL,
    target_price_long DECIMAL(12, 4) NOT NULL,
    price_change_1d DECIMAL(8, 4) DEFAULT 0,
    price_change_7d DECIMAL(8, 4) DEFAULT 0,
    price_change_30d DECIMAL(8, 4) DEFAULT 0,
    analyst_rating TEXT,
    analyst_target DECIMAL(12, 4),
    analyst_count INTEGER DEFAULT 0,
    model_confidence_short INTEGER DEFAULT 50,
    model_confidence_long INTEGER DEFAULT 50,
    news_summary TEXT,
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