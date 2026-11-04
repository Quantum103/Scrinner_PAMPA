package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ScannerProfile identifies one indicator configuration.
type ScannerProfile struct {
	RSILength int
	EMALength int
}

// ScannerWorkload contains one profile/timeframe and the union of symbols
// selected by active users who share that profile.
type ScannerWorkload struct {
	RSILength int
	EMALength int
	Timeframe string
	Symbols   []string
}

// ScannerProfileState is an independent cursor for one symbol, timeframe and profile.
type ScannerProfileState struct {
	Symbol                   string
	Timeframe                string
	RSILength                int
	EMALength                int
	LastProcessedCandleStart time.Time
	LastClose                float64
	LastRSI                  float64
	LastEMARSI               float64
	LastCrossDirection       string
	LastCrossAt              *time.Time
	Initialized              bool
	UpdatedAt                time.Time
}

// ListScannerWorkloads applies each active user's universe settings and groups
// the required market pairs by their unique RSI/EMA profile.
func (r *Repository) ListScannerWorkloads(ctx context.Context) ([]ScannerWorkload, error) {
	rows, err := r.pool.Query(ctx, `
		WITH ranked_instruments AS (
			SELECT symbol, row_number() OVER (ORDER BY turnover_24h DESC, symbol) AS market_rank
			FROM instruments
			WHERE is_active = TRUE
		)
		SELECT DISTINCT settings.rsi_length, settings.ema_length, tf.timeframe, instrument.symbol
		FROM telegram_users user_account
		JOIN user_settings settings ON settings.user_id = user_account.id
		JOIN user_timeframes tf ON tf.user_id = user_account.id
		JOIN instruments instrument ON instrument.is_active = TRUE
		LEFT JOIN ranked_instruments ranked ON ranked.symbol = instrument.symbol
		WHERE user_account.is_active = TRUE
		  AND (
			settings.scan_all_instruments = TRUE
			OR ranked.market_rank <= settings.instrument_limit
			OR EXISTS (
				SELECT 1 FROM user_instrument_preferences included
				WHERE included.user_id = user_account.id
				  AND included.symbol = instrument.symbol
				  AND included.preference = 'include'
			)
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM user_instrument_preferences excluded
			WHERE excluded.user_id = user_account.id
			  AND excluded.symbol = instrument.symbol
			  AND excluded.preference = 'exclude'
		  )
		ORDER BY settings.rsi_length, settings.ema_length, tf.timeframe, instrument.symbol
	`)
	if err != nil {
		return nil, fmt.Errorf("list personalized scanner workloads: %w", err)
	}
	defer rows.Close()

	type workloadKey struct {
		rsiLength int
		emaLength int
		timeframe string
	}
	indexes := make(map[workloadKey]int)
	workloads := make([]ScannerWorkload, 0)
	for rows.Next() {
		var rsiLength, emaLength int
		var timeframe, symbol string
		if err := rows.Scan(&rsiLength, &emaLength, &timeframe, &symbol); err != nil {
			return nil, fmt.Errorf("scan personalized scanner workload: %w", err)
		}
		key := workloadKey{rsiLength: rsiLength, emaLength: emaLength, timeframe: timeframe}
		position, exists := indexes[key]
		if !exists {
			position = len(workloads)
			indexes[key] = position
			workloads = append(workloads, ScannerWorkload{
				RSILength: rsiLength,
				EMALength: emaLength,
				Timeframe: timeframe,
			})
		}
		workloads[position].Symbols = append(workloads[position].Symbols, symbol)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate personalized scanner workloads: %w", err)
	}
	return workloads, nil
}

// ListScannerProfilesForPair returns the distinct profiles currently interested
// in a symbol/timeframe, accounting for ranking and manual user preferences.
func (r *Repository) ListScannerProfilesForPair(ctx context.Context, symbol, timeframe string) ([]ScannerProfile, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if err := validateScannerProfileIdentity(symbol, timeframe, 14, 9); err != nil {
		return nil, err
	}
	rows, err := r.pool.Query(ctx, `
		WITH ranked_instruments AS (
			SELECT symbol, row_number() OVER (ORDER BY turnover_24h DESC, symbol) AS market_rank
			FROM instruments
			WHERE is_active = TRUE
		)
		SELECT DISTINCT settings.rsi_length, settings.ema_length
		FROM telegram_users user_account
		JOIN user_settings settings ON settings.user_id = user_account.id
		JOIN user_timeframes tf ON tf.user_id = user_account.id AND tf.timeframe = $2
		JOIN instruments instrument ON instrument.symbol = $1 AND instrument.is_active = TRUE
		LEFT JOIN ranked_instruments ranked ON ranked.symbol = instrument.symbol
		WHERE user_account.is_active = TRUE
		  AND (
			settings.scan_all_instruments = TRUE
			OR ranked.market_rank <= settings.instrument_limit
			OR EXISTS (
				SELECT 1 FROM user_instrument_preferences included
				WHERE included.user_id = user_account.id
				  AND included.symbol = instrument.symbol
				  AND included.preference = 'include'
			)
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM user_instrument_preferences excluded
			WHERE excluded.user_id = user_account.id
			  AND excluded.symbol = instrument.symbol
			  AND excluded.preference = 'exclude'
		  )
		ORDER BY settings.rsi_length, settings.ema_length
	`, symbol, timeframe)
	if err != nil {
		return nil, fmt.Errorf("list scanner profiles for %s %s: %w", symbol, timeframe, err)
	}
	defer rows.Close()

	profiles := make([]ScannerProfile, 0)
	for rows.Next() {
		var profile ScannerProfile
		if err := rows.Scan(&profile.RSILength, &profile.EMALength); err != nil {
			return nil, fmt.Errorf("scan scanner profile for %s %s: %w", symbol, timeframe, err)
		}
		profiles = append(profiles, profile)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scanner profiles for %s %s: %w", symbol, timeframe, err)
	}
	return profiles, nil
}

func validateScannerProfileIdentity(symbol, timeframe string, rsiLength, emaLength int) error {
	if strings.TrimSpace(symbol) == "" {
		return fmt.Errorf("%w: scanner profile symbol is required", ErrInvalidInput)
	}
	if timeframe != "5" && timeframe != "15" && timeframe != "60" {
		return fmt.Errorf("%w: unsupported scanner profile timeframe %q", ErrInvalidInput, timeframe)
	}
	if rsiLength < 2 || rsiLength > 500 || emaLength < 1 || emaLength > 500 ||
		rsiLength+emaLength > 1000 {
		return fmt.Errorf("%w: scanner profile periods must fit into the 1000-candle history limit", ErrInvalidInput)
	}
	return nil
}

func validateScannerProfileState(state ScannerProfileState) error {
	if err := validateScannerProfileIdentity(state.Symbol, state.Timeframe, state.RSILength, state.EMALength); err != nil {
		return err
	}
	if state.LastProcessedCandleStart.IsZero() {
		return fmt.Errorf("%w: scanner profile candle timestamp is required", ErrInvalidInput)
	}
	if state.LastClose <= 0 || math.IsNaN(state.LastClose) || math.IsInf(state.LastClose, 0) {
		return fmt.Errorf("%w: scanner profile close must be finite and positive", ErrInvalidInput)
	}
	for name, value := range map[string]float64{"RSI": state.LastRSI, "EMA RSI": state.LastEMARSI} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 100 {
			return fmt.Errorf("%w: scanner profile %s must be finite and between 0 and 100", ErrInvalidInput, name)
		}
	}
	if state.LastCrossDirection != "" && state.LastCrossDirection != "LONG" &&
		state.LastCrossDirection != "SHORT" {
		return fmt.Errorf("%w: invalid scanner profile last cross direction", ErrInvalidInput)
	}
	return nil
}

func (r *Repository) GetScannerProfileState(
	ctx context.Context, symbol, timeframe string, rsiLength, emaLength int,
) (ScannerProfileState, error) {
	if err := validateScannerProfileIdentity(symbol, timeframe, rsiLength, emaLength); err != nil {
		return ScannerProfileState{}, err
	}
	return scanScannerProfileState(r.pool.QueryRow(ctx, scannerProfileStateSelectSQL,
		strings.ToUpper(strings.TrimSpace(symbol)), timeframe, rsiLength, emaLength))
}

const scannerProfileStateSelectSQL = `
	SELECT symbol, timeframe, rsi_length, ema_length, last_processed_candle_start,
	       last_close::float8, COALESCE(last_rsi::float8, 0), COALESCE(last_ema_rsi::float8, 0),
	       COALESCE(last_cross_direction, ''), last_cross_at, initialized, updated_at
	FROM scanner_profile_states
	WHERE symbol = $1 AND timeframe = $2 AND rsi_length = $3 AND ema_length = $4
`

func scanScannerProfileState(row pgx.Row) (ScannerProfileState, error) {
	var state ScannerProfileState
	err := row.Scan(
		&state.Symbol, &state.Timeframe, &state.RSILength, &state.EMALength,
		&state.LastProcessedCandleStart, &state.LastClose, &state.LastRSI, &state.LastEMARSI,
		&state.LastCrossDirection, &state.LastCrossAt, &state.Initialized, &state.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ScannerProfileState{}, ErrNotFound
	}
	if err != nil {
		return ScannerProfileState{}, fmt.Errorf("scan scanner profile state: %w", err)
	}
	return state, nil
}

// InitializeScannerProfileState records the first fully calculated candle without
// overwriting a cursor that was persisted by an earlier process.
func (r *Repository) InitializeScannerProfileState(
	ctx context.Context, baseline ScannerProfileState,
) (ScannerProfileState, bool, error) {
	if err := validateScannerProfileState(baseline); err != nil {
		return ScannerProfileState{}, false, err
	}
	tag, err := r.pool.Exec(ctx, `
		INSERT INTO scanner_profile_states (
			symbol, timeframe, rsi_length, ema_length, last_processed_candle_start,
			last_close, last_rsi, last_ema_rsi, last_cross_direction,
			last_cross_at, initialized
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), $10, $11)
		ON CONFLICT (symbol, timeframe, rsi_length, ema_length) DO NOTHING
	`, strings.ToUpper(strings.TrimSpace(baseline.Symbol)), baseline.Timeframe,
		baseline.RSILength, baseline.EMALength, baseline.LastProcessedCandleStart,
		baseline.LastClose, baseline.LastRSI, baseline.LastEMARSI,
		baseline.LastCrossDirection, nullableTime(baseline.LastCrossAt), baseline.Initialized)
	if err != nil {
		return ScannerProfileState{}, false, fmt.Errorf("initialize scanner profile state: %w", err)
	}
	state, err := r.GetScannerProfileState(ctx, baseline.Symbol, baseline.Timeframe, baseline.RSILength, baseline.EMALength)
	if err != nil {
		return ScannerProfileState{}, false, err
	}
	return state, tag.RowsAffected() == 1, nil
}

// CommitScannerProfileCandle advances one profile cursor and atomically persists
// the profile-specific signal and its eligible user deliveries.
func (r *Repository) CommitScannerProfileCandle(
	ctx context.Context, state ScannerProfileState, signal *Signal,
) (int64, bool, int64, error) {
	if err := validateScannerProfileState(state); err != nil {
		return 0, false, 0, err
	}
	if signal != nil {
		rsiLength, emaLength := signalPeriods(*signal)
		if err := validateSignal(*signal); err != nil {
			return 0, false, 0, err
		}
		if !strings.EqualFold(signal.Symbol, state.Symbol) ||
			signal.Timeframe != state.Timeframe ||
			!signal.CandleStart.Equal(state.LastProcessedCandleStart) ||
			rsiLength != state.RSILength || emaLength != state.EMALength {
			return 0, false, 0, fmt.Errorf("%w: signal does not match the committed profile candle", ErrInvalidInput)
		}
	} else {
		advanced, err := advanceScannerProfileState(ctx, r.pool, state)
		if err != nil {
			return 0, false, 0, fmt.Errorf("advance profile cursor without signal: %w", err)
		}
		if !advanced {
			return 0, false, 0, ErrStaleCandle
		}
		return 0, false, 0, nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, false, 0, fmt.Errorf("begin profile candle commit: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	advanced, err := advanceScannerProfileState(ctx, tx, state)
	if err != nil {
		return 0, false, 0, fmt.Errorf("advance scanner profile cursor: %w", err)
	}
	if !advanced {
		return 0, false, 0, ErrStaleCandle
	}

	var signalID int64
	var inserted bool
	var queued int64
	if signal != nil {
		signalID, inserted, err = insertSignal(ctx, tx, *signal)
		if err != nil {
			return 0, false, 0, fmt.Errorf("insert personalized scanner signal: %w", err)
		}
		if inserted {
			queued, err = enqueueSignalForEligibleUsers(ctx, tx, signalID)
			if err != nil {
				return 0, false, 0, fmt.Errorf("enqueue personalized scanner signal: %w", err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, false, 0, fmt.Errorf("commit scanner profile candle: %w", err)
	}
	return signalID, inserted, queued, nil
}

type scannerProfileStateExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func advanceScannerProfileState(ctx context.Context, executor scannerProfileStateExecer, state ScannerProfileState) (bool, error) {
	tag, err := executor.Exec(ctx, `
		INSERT INTO scanner_profile_states (
			symbol, timeframe, rsi_length, ema_length, last_processed_candle_start,
			last_close, last_rsi, last_ema_rsi, last_cross_direction,
			last_cross_at, initialized
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''), $10, $11)
		ON CONFLICT (symbol, timeframe, rsi_length, ema_length) DO UPDATE SET
			last_processed_candle_start = EXCLUDED.last_processed_candle_start,
			last_close = EXCLUDED.last_close,
			last_rsi = EXCLUDED.last_rsi,
			last_ema_rsi = EXCLUDED.last_ema_rsi,
			last_cross_direction = EXCLUDED.last_cross_direction,
			last_cross_at = EXCLUDED.last_cross_at,
			initialized = EXCLUDED.initialized,
			updated_at = now()
		WHERE scanner_profile_states.last_processed_candle_start < EXCLUDED.last_processed_candle_start
	`, strings.ToUpper(strings.TrimSpace(state.Symbol)), state.Timeframe,
		state.RSILength, state.EMALength, state.LastProcessedCandleStart,
		state.LastClose, state.LastRSI, state.LastEMARSI, state.LastCrossDirection,
		nullableTime(state.LastCrossAt), state.Initialized)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// HasActiveScannerUsers differentiates an empty active-user universe from an
// excluded market pair. With no active users the legacy global scanner may run.
func (r *Repository) HasActiveScannerUsers(ctx context.Context) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM telegram_users
			WHERE is_active = TRUE
		)
	`).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check active Telegram scanner users: %w", err)
	}
	return exists, nil
}
