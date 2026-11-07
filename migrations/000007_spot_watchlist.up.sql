-- Separate Spot market catalogue and persistent low-liquidity Watchlist.
-- The existing instruments table remains dedicated to Linear Perpetual markets.
CREATE TABLE spot_instruments (
    symbol TEXT PRIMARY KEY,
    base_coin TEXT NOT NULL,
    quote_coin TEXT NOT NULL CHECK (quote_coin = 'USDT'),
    status TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    turnover_24h NUMERIC(30, 8) NOT NULL DEFAULT 0 CHECK (turnover_24h >= 0),
    last_price NUMERIC(30, 12) NOT NULL CHECK (last_price > 0),
    ticker_updated_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX spot_instruments_active_turnover_idx
    ON spot_instruments (turnover_24h ASC, symbol)
    WHERE is_active = TRUE;

CREATE TABLE spot_watchlist (
    symbol TEXT PRIMARY KEY
        REFERENCES spot_instruments(symbol) ON DELETE CASCADE,
    state TEXT NOT NULL
        CHECK (state IN ('watching', 'over_limit_hold', 'excluded')),
    over_limit_since TIMESTAMPTZ,
    excluded_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT spot_watchlist_state_timestamps_chk CHECK (
        (state = 'watching' AND over_limit_since IS NULL AND excluded_at IS NULL)
        OR (state = 'over_limit_hold' AND over_limit_since IS NOT NULL AND excluded_at IS NULL)
        OR (state = 'excluded' AND over_limit_since IS NOT NULL AND excluded_at IS NOT NULL)
    )
);

CREATE INDEX spot_watchlist_active_idx
    ON spot_watchlist (state, updated_at DESC)
    WHERE state IN ('watching', 'over_limit_hold');
