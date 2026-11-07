package storage

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	SpotWatchlistWatching      = "watching"
	SpotWatchlistOverLimitHold = "over_limit_hold"
	SpotWatchlistExcluded     = "excluded"
)

type SpotInstrument struct {
	Symbol          string
	BaseCoin        string
	QuoteCoin       string
	Status          string
	Turnover24h     string
	LastPrice       string
	TickerUpdatedAt time.Time
}

type SpotWatchlistEntry struct {
	Symbol          string
	BaseCoin        string
	QuoteCoin       string
	Turnover24h     string
	LastPrice       string
	State           string
	OverLimitSince  *time.Time
	TickerUpdatedAt time.Time
}

// SyncSpotCatalog atomically replaces the active Spot market snapshot and
// reconciles the durable Watchlist. A symbol that crosses the turnover limit
// is retained for the full hold period; if it remains above the limit at the
// deadline it is excluded. Falling back to or below the limit restores it.
func (r *Repository) SyncSpotCatalog(
	ctx context.Context,
	instruments []SpotInstrument,
	turnoverLimit float64,
	overLimitHold time.Duration,
	now time.Time,
) error {
	if len(instruments) == 0 {
		return fmt.Errorf("%w: Spot instrument snapshot must not be empty", ErrInvalidInput)
	}
	if math.IsNaN(turnoverLimit) || math.IsInf(turnoverLimit, 0) || turnoverLimit <= 0 || overLimitHold <= 0 || now.IsZero() {
		return fmt.Errorf("%w: invalid Spot Watchlist policy", ErrInvalidInput)
	}
	seen := make(map[string]struct{}, len(instruments))
	for _, item := range instruments {
		if err := validateSpotInstrument(item); err != nil {
			return err
		}
		if _, exists := seen[item.Symbol]; exists {
			return fmt.Errorf("%w: duplicate Spot instrument %s", ErrInvalidInput, item.Symbol)
		}
		seen[item.Symbol] = struct{}{}
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin Spot catalogue transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if _, err := tx.Exec(ctx, "UPDATE spot_instruments SET is_active = FALSE, updated_at = $1 WHERE is_active = TRUE", now.UTC()); err != nil {
		return fmt.Errorf("deactivate stale Spot instruments: %w", err)
	}

	const upsertSpotInstrumentSQL = `INSERT INTO spot_instruments (
		symbol, base_coin, quote_coin, status, is_active,
		turnover_24h, last_price, ticker_updated_at, created_at, updated_at
	)
	VALUES ($1, $2, $3, $4, TRUE, $5, $6, $7, $7, $7)
	ON CONFLICT (symbol) DO UPDATE SET
		base_coin = EXCLUDED.base_coin,
		quote_coin = EXCLUDED.quote_coin,
		status = EXCLUDED.status,
		is_active = TRUE,
		turnover_24h = EXCLUDED.turnover_24h,
		last_price = EXCLUDED.last_price,
		ticker_updated_at = EXCLUDED.ticker_updated_at,
		updated_at = EXCLUDED.updated_at`
	batch := &pgx.Batch{}
	for _, item := range instruments {
		batch.Queue(upsertSpotInstrumentSQL, item.Symbol, item.BaseCoin, item.QuoteCoin,
			item.Status, item.Turnover24h, item.LastPrice, item.TickerUpdatedAt.UTC())
	}
	results := tx.SendBatch(ctx, batch)
	for _, item := range instruments {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("upsert Spot instrument %s: %w", item.Symbol, err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("finish Spot instrument batch: %w", err)
	}

	turnoverLimitText := strconv.FormatFloat(turnoverLimit, 'f', -1, 64)
	_, err = tx.Exec(ctx, `INSERT INTO spot_watchlist (
		symbol, state, over_limit_since, excluded_at, updated_at
	)
	SELECT
		symbol,
		CASE WHEN turnover_24h <= $1::numeric THEN 'watching' ELSE 'over_limit_hold' END,
		CASE WHEN turnover_24h <= $1::numeric THEN NULL ELSE $2 END,
		NULL,
		$2
	FROM spot_instruments
	WHERE is_active = TRUE
	ON CONFLICT (symbol) DO UPDATE SET
		state = CASE
			WHEN EXCLUDED.state = 'watching' THEN 'watching'
			WHEN spot_watchlist.state = 'excluded' THEN 'excluded'
			WHEN spot_watchlist.over_limit_since IS NOT NULL
			 AND spot_watchlist.over_limit_since + ($3 * interval '1 second') <= $2 THEN 'excluded'
			ELSE 'over_limit_hold'
		END,
		over_limit_since = CASE
			WHEN EXCLUDED.state = 'watching' THEN NULL
			WHEN spot_watchlist.over_limit_since IS NULL THEN $2
			ELSE spot_watchlist.over_limit_since
		END,
		excluded_at = CASE
			WHEN EXCLUDED.state = 'watching' THEN NULL
			WHEN spot_watchlist.state = 'excluded' THEN spot_watchlist.excluded_at
			WHEN spot_watchlist.over_limit_since IS NOT NULL
			 AND spot_watchlist.over_limit_since + ($3 * interval '1 second') <= $2 THEN $2
			ELSE NULL
		END,
		updated_at = $2`, turnoverLimitText, now.UTC(), overLimitHold.Seconds())
	if err != nil {
		return fmt.Errorf("reconcile Spot Watchlist: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit Spot catalogue transaction: %w", err)
	}
	return nil
}

// ListSpotWatchlist returns only active Spot pairs currently monitored.
// Symbols excluded after the 24-hour high-turnover hold remain persisted in
// spot_watchlist and can automatically re-enter if turnover falls to the limit.
func (r *Repository) ListSpotWatchlist(ctx context.Context) ([]SpotWatchlistEntry, error) {
	rows, err := r.pool.Query(ctx, `SELECT i.symbol, i.base_coin, i.quote_coin, i.turnover_24h::text,
		i.last_price::text, w.state, w.over_limit_since, i.ticker_updated_at
		FROM spot_watchlist w
		JOIN spot_instruments i ON i.symbol = w.symbol
		WHERE i.is_active = TRUE
		  AND w.state IN ('watching', 'over_limit_hold')
		ORDER BY i.turnover_24h ASC, i.symbol`)
	if err != nil {
		return nil, fmt.Errorf("list Spot Watchlist: %w", err)
	}
	defer rows.Close()

	result := make([]SpotWatchlistEntry, 0)
	for rows.Next() {
		var item SpotWatchlistEntry
		if err := rows.Scan(
			&item.Symbol, &item.BaseCoin, &item.QuoteCoin, &item.Turnover24h,
			&item.LastPrice, &item.State, &item.OverLimitSince, &item.TickerUpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan Spot Watchlist entry: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Spot Watchlist: %w", err)
	}
	return result, nil
}

func validateSpotInstrument(item SpotInstrument) error {
	symbol := strings.TrimSpace(item.Symbol)
	baseCoin := strings.TrimSpace(item.BaseCoin)
	if symbol == "" || symbol != strings.ToUpper(symbol) || baseCoin == "" || baseCoin != strings.ToUpper(baseCoin) ||
		item.QuoteCoin != "USDT" || item.Status != "Trading" || item.TickerUpdatedAt.IsZero() {
		return fmt.Errorf("%w: invalid Spot instrument %q", ErrInvalidInput, item.Symbol)
	}
	for _, value := range symbol {
		if !(value >= 'A' && value <= 'Z') && !(value >= '0' && value <= '9') {
			return fmt.Errorf("%w: invalid Spot symbol %q", ErrInvalidInput, item.Symbol)
		}
	}
	for _, value := range baseCoin {
		if !(value >= 'A' && value <= 'Z') && !(value >= '0' && value <= '9') {
			return fmt.Errorf("%w: invalid Spot base coin %q", ErrInvalidInput, item.BaseCoin)
		}
	}
	lastPrice, err := strconv.ParseFloat(item.LastPrice, 64)
	if err != nil || math.IsNaN(lastPrice) || math.IsInf(lastPrice, 0) || lastPrice <= 0 {
		return fmt.Errorf("%w: invalid Spot last price for %s", ErrInvalidInput, item.Symbol)
	}
	turnover, err := strconv.ParseFloat(item.Turnover24h, 64)
	if err != nil || math.IsNaN(turnover) || math.IsInf(turnover, 0) || turnover < 0 {
		return fmt.Errorf("%w: invalid Spot 24h turnover for %s", ErrInvalidInput, item.Symbol)
	}
	return nil
}
