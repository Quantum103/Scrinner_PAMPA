-- Persist the market classification and indicator snapshot captured with each RSI signal.
-- NULL indicator values represent metrics that were not ready at the signal candle.
ALTER TABLE signals
    ADD COLUMN market_regime TEXT NOT NULL DEFAULT 'UNKNOWN'
        CHECK (market_regime IN ('UNKNOWN', 'SIDEWAYS', 'UPTREND', 'DOWNTREND', 'TRANSITIONAL')),
    ADD COLUMN market_adx NUMERIC(12, 8)
        CHECK (market_adx IS NULL OR market_adx BETWEEN 0 AND 100),
    ADD COLUMN market_chop NUMERIC(12, 8)
        CHECK (market_chop IS NULL OR market_chop BETWEEN 0 AND 100),
    ADD COLUMN market_ema NUMERIC(30, 12)
        CHECK (market_ema IS NULL OR market_ema > 0);
