DROP TABLE IF EXISTS scanner_profile_states;
DROP INDEX IF EXISTS signals_profile_idx;
ALTER TABLE signals DROP CONSTRAINT IF EXISTS signals_profile_market_event_unique;
ALTER TABLE signals ADD CONSTRAINT signals_unique_market_event
    UNIQUE (symbol, timeframe, candle_start, direction);
ALTER TABLE signals DROP COLUMN IF EXISTS ema_length, DROP COLUMN IF EXISTS rsi_length;
