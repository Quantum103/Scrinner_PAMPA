ALTER TABLE signals
    DROP COLUMN IF EXISTS market_ema,
    DROP COLUMN IF EXISTS market_chop,
    DROP COLUMN IF EXISTS market_adx,
    DROP COLUMN IF EXISTS market_regime;
