-- Persist the higher-timeframe trend captured when each RSI signal is detected.
-- Legacy signals are UNKNOWN because historical 4h context is not backfilled.
ALTER TABLE signals
    ADD COLUMN higher_timeframe TEXT NOT NULL DEFAULT '240'
        CHECK (higher_timeframe = '240'),
    ADD COLUMN higher_market_regime TEXT NOT NULL DEFAULT 'UNKNOWN'
        CHECK (higher_market_regime IN ('UNKNOWN', 'SIDEWAYS', 'UPTREND', 'DOWNTREND', 'TRANSITIONAL'));
