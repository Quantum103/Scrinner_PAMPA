DROP TABLE IF EXISTS scanner_profile_states;
DROP INDEX IF EXISTS signals_profile_idx;
ALTER TABLE signals DROP CONSTRAINT IF EXISTS signals_profile_market_event_unique;
-- Rollback collapses profile-specific copies to one historical market event.
-- Deliveries to removed duplicates cascade with the deleted signal rows.
DELETE FROM signals older
USING signals newer
WHERE older.symbol = newer.symbol
  AND older.timeframe = newer.timeframe
  AND older.candle_start = newer.candle_start
  AND older.direction = newer.direction
  AND older.id > newer.id;

ALTER TABLE signals ADD CONSTRAINT signals_unique_market_event
    UNIQUE (symbol, timeframe, candle_start, direction);
ALTER TABLE signals DROP COLUMN IF EXISTS ema_length, DROP COLUMN IF EXISTS rsi_length;
