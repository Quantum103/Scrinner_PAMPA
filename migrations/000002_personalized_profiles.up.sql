ALTER TABLE signals
    ADD COLUMN rsi_length INTEGER NOT NULL DEFAULT 14 CHECK (rsi_length BETWEEN 2 AND 500),
    ADD COLUMN ema_length INTEGER NOT NULL DEFAULT 9 CHECK (ema_length BETWEEN 1 AND 500);
ALTER TABLE signals DROP CONSTRAINT signals_unique_market_event;
ALTER TABLE signals ADD CONSTRAINT signals_profile_market_event_unique
    UNIQUE (symbol, timeframe, candle_start, direction, rsi_length, ema_length);
CREATE INDEX signals_profile_idx ON signals (rsi_length, ema_length, timeframe, detected_at DESC);
CREATE TABLE scanner_profile_states (
    symbol TEXT NOT NULL REFERENCES instruments(symbol) ON DELETE RESTRICT,
    timeframe TEXT NOT NULL CHECK (timeframe IN ('5', '15', '60')),
    rsi_length INTEGER NOT NULL CHECK (rsi_length BETWEEN 2 AND 500),
    ema_length INTEGER NOT NULL CHECK (ema_length BETWEEN 1 AND 500),
    last_processed_candle_start TIMESTAMPTZ NOT NULL,
    last_close NUMERIC(30, 12) NOT NULL DEFAULT 0,
    last_rsi NUMERIC(12, 8),
    last_ema_rsi NUMERIC(12, 8),
    last_cross_direction TEXT CHECK (last_cross_direction IS NULL OR last_cross_direction IN ('LONG', 'SHORT')),
    last_cross_at TIMESTAMPTZ,
    initialized BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, timeframe, rsi_length, ema_length),
    CHECK (rsi_length + ema_length <= 1000)
);
CREATE INDEX scanner_profile_states_updated_idx ON scanner_profile_states (updated_at DESC);
