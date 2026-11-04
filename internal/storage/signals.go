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

func (r *Repository) SaveSignal(ctx context.Context, signal Signal) (int64, bool, error) {
	if err := validateSignal(signal); err != nil {
		return 0, false, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("begin save signal: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	id, inserted, err := insertSignal(ctx, tx, signal)
	if err != nil {
		return 0, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, false, fmt.Errorf("commit signal: %w", err)
	}
	return id, inserted, nil
}

func insertSignal(ctx context.Context, tx pgx.Tx, signal Signal) (int64, bool, error) {
	rsiLength, emaLength := signalPeriods(signal)
	arguments := []any{
		strings.ToUpper(signal.Symbol), signal.Timeframe, signal.Direction,
		signal.CandleStart, signal.CandleClose, signal.ClosePrice, signal.RSI,
		signal.EMARSI, rsiLength, emaLength, nullableDetectedAt(signal.DetectedAt),
	}
	var id int64
	err := tx.QueryRow(ctx, `
		INSERT INTO signals (
			symbol, timeframe, direction, candle_start, candle_close,
			close_price, rsi, ema_rsi, rsi_length, ema_length, detected_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, COALESCE($11, now()))
		ON CONFLICT (symbol, timeframe, candle_start, direction, rsi_length, ema_length) DO NOTHING
		RETURNING id
	`, arguments...).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, fmt.Errorf("insert signal %s %s %s: %w", signal.Symbol, signal.Timeframe, signal.Direction, err)
	}
	err = tx.QueryRow(ctx, `
		SELECT id FROM signals
		WHERE symbol = $1 AND timeframe = $2 AND candle_start = $3 AND direction = $4
		  AND rsi_length = $5 AND ema_length = $6
	`, arguments[0], arguments[1], arguments[3], arguments[2], arguments[8], arguments[9]).Scan(&id)
	if err != nil {
		return 0, false, fmt.Errorf("load saved signal ID: %w", err)
	}
	return id, false, nil
}

func signalPeriods(signal Signal) (int, int) {
	rsiLength, emaLength := signal.RSILength, signal.EMALength
	if rsiLength == 0 {
		rsiLength = 14
	}
	if emaLength == 0 {
		emaLength = 9
	}
	return rsiLength, emaLength
}

func nullableDetectedAt(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func validateSignal(signal Signal) error {
	if strings.TrimSpace(signal.Symbol) == "" {
		return fmt.Errorf("%w: signal symbol is required", ErrInvalidInput)
	}
	if signal.Timeframe != "5" && signal.Timeframe != "15" && signal.Timeframe != "60" {
		return fmt.Errorf("%w: unsupported signal timeframe %q", ErrInvalidInput, signal.Timeframe)
	}
	if signal.Direction != "LONG" && signal.Direction != "SHORT" {
		return fmt.Errorf("%w: signal direction must be LONG or SHORT", ErrInvalidInput)
	}
	if signal.CandleStart.IsZero() || !signal.CandleClose.After(signal.CandleStart) {
		return fmt.Errorf("%w: signal candle interval is invalid", ErrInvalidInput)
	}
	if signal.ClosePrice <= 0 || math.IsNaN(signal.ClosePrice) || math.IsInf(signal.ClosePrice, 0) {
		return fmt.Errorf("%w: signal close price must be finite and positive", ErrInvalidInput)
	}
	if signal.RSI < 0 || signal.RSI > 100 || math.IsNaN(signal.RSI) || math.IsInf(signal.RSI, 0) {
		return fmt.Errorf("%w: signal RSI must be between 0 and 100", ErrInvalidInput)
	}
	if signal.EMARSI < 0 || signal.EMARSI > 100 || math.IsNaN(signal.EMARSI) || math.IsInf(signal.EMARSI, 0) {
		return fmt.Errorf("%w: signal EMA RSI must be between 0 and 100", ErrInvalidInput)
	}
	rsiLength, emaLength := signalPeriods(signal)
	if rsiLength < 2 || rsiLength > 500 || emaLength < 1 || emaLength > 500 || rsiLength+emaLength > 1000 {
		return fmt.Errorf("%w: signal RSI/EMA periods must fit into the 1000-candle history limit", ErrInvalidInput)
	}
	return nil
}

// InitializeScannerState records the first fully calculated baseline only if no cursor exists.
// On a restart, the previously persisted cursor is returned without being overwritten.
func (r *Repository) InitializeScannerState(ctx context.Context, baseline ScannerState) (ScannerState, bool, error) {
	if err := validateScannerState(baseline); err != nil {
		return ScannerState{}, false, err
	}
	tag, err := r.pool.Exec(ctx, `
		INSERT INTO scanner_states (
			symbol, timeframe, last_processed_candle_start, last_close,
			last_rsi, last_ema_rsi, last_cross_direction, last_cross_at, initialized
		)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9)
		ON CONFLICT (symbol, timeframe) DO NOTHING
	`, strings.ToUpper(baseline.Symbol), baseline.Timeframe, baseline.LastProcessedCandleStart,
		baseline.LastClose, baseline.LastRSI, baseline.LastEMARSI, baseline.LastCrossDirection,
		nullableTime(baseline.LastCrossAt), baseline.Initialized)
	if err != nil {
		return ScannerState{}, false, fmt.Errorf("initialize scanner state: %w", err)
	}
	state, err := r.GetScannerState(ctx, baseline.Symbol, baseline.Timeframe)
	if err != nil {
		return ScannerState{}, false, err
	}
	return state, tag.RowsAffected() == 1, nil
}

func (r *Repository) GetScannerState(ctx context.Context, symbol, timeframe string) (ScannerState, error) {
	return scanScannerState(r.pool.QueryRow(ctx, scannerStateSelectSQL, strings.ToUpper(symbol), timeframe))
}

const scannerStateSelectSQL = `
	SELECT symbol, timeframe, last_processed_candle_start, last_close,
	       COALESCE(last_rsi::float8, 0), COALESCE(last_ema_rsi::float8, 0),
	       COALESCE(last_cross_direction, ''), last_cross_at, initialized, updated_at
	FROM scanner_states
	WHERE symbol = $1 AND timeframe = $2
`

func scanScannerState(row pgx.Row) (ScannerState, error) {
	var state ScannerState
	err := row.Scan(
		&state.Symbol, &state.Timeframe, &state.LastProcessedCandleStart, &state.LastClose,
		&state.LastRSI, &state.LastEMARSI, &state.LastCrossDirection, &state.LastCrossAt,
		&state.Initialized, &state.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ScannerState{}, ErrNotFound
	}
	if err != nil {
		return ScannerState{}, fmt.Errorf("scan scanner state: %w", err)
	}
	return state, nil
}

func validateScannerState(state ScannerState) error {
	if strings.TrimSpace(state.Symbol) == "" {
		return fmt.Errorf("%w: scanner state symbol is required", ErrInvalidInput)
	}
	if state.Timeframe != "5" && state.Timeframe != "15" && state.Timeframe != "30" && state.Timeframe != "60" {
		return fmt.Errorf("%w: unsupported scanner state timeframe %q", ErrInvalidInput, state.Timeframe)
	}
	if state.LastProcessedCandleStart.IsZero() {
		return fmt.Errorf("%w: scanner state candle timestamp is required", ErrInvalidInput)
	}
	if state.LastCrossDirection != "" && state.LastCrossDirection != "LONG" && state.LastCrossDirection != "SHORT" {
		return fmt.Errorf("%w: invalid last cross direction", ErrInvalidInput)
	}
	for name, value := range map[string]float64{"close": state.LastClose, "RSI": state.LastRSI, "EMA RSI": state.LastEMARSI} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("%w: scanner state %s must be finite", ErrInvalidInput, name)
		}
	}
	if state.LastRSI < 0 || state.LastRSI > 100 || state.LastEMARSI < 0 || state.LastEMARSI > 100 {
		return fmt.Errorf("%w: scanner state RSI values must be between 0 and 100", ErrInvalidInput)
	}
	return nil
}

type scannerStateExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func advanceScannerState(ctx context.Context, executor scannerStateExecer, state ScannerState) (bool, error) {
	tag, err := executor.Exec(ctx, `
		INSERT INTO scanner_states (
			symbol, timeframe, last_processed_candle_start, last_close,
			last_rsi, last_ema_rsi, last_cross_direction, last_cross_at, initialized
		)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9)
		ON CONFLICT (symbol, timeframe) DO UPDATE SET
			last_processed_candle_start = EXCLUDED.last_processed_candle_start,
			last_close = EXCLUDED.last_close,
			last_rsi = EXCLUDED.last_rsi,
			last_ema_rsi = EXCLUDED.last_ema_rsi,
			last_cross_direction = EXCLUDED.last_cross_direction,
			last_cross_at = EXCLUDED.last_cross_at,
			initialized = EXCLUDED.initialized,
			updated_at = now()
		WHERE scanner_states.last_processed_candle_start < EXCLUDED.last_processed_candle_start
	`, strings.ToUpper(state.Symbol), state.Timeframe, state.LastProcessedCandleStart,
		state.LastClose, state.LastRSI, state.LastEMARSI, state.LastCrossDirection,
		nullableTime(state.LastCrossAt), state.Initialized)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// CommitClosedCandle advances the scanner cursor and saves an optional signal in one transaction.
func (r *Repository) CommitClosedCandle(ctx context.Context, state ScannerState, signal *Signal) (int64, bool, error) {
	signalID, inserted, _, err := r.commitScannedCandle(ctx, state, signal, false)
	return signalID, inserted, err
}

// CommitScannedCandle atomically advances the cursor, inserts a new signal and
// creates all eligible user deliveries. A failure cannot leave a signal saved
// without its initial delivery records.
func (r *Repository) CommitScannedCandle(ctx context.Context, state ScannerState, signal *Signal) (int64, bool, int64, error) {
	return r.commitScannedCandle(ctx, state, signal, true)
}

func (r *Repository) commitScannedCandle(ctx context.Context, state ScannerState, signal *Signal, enqueue bool) (int64, bool, int64, error) {
	if err := validateScannerState(state); err != nil {
		return 0, false, 0, err
	}
	if signal != nil {
		if err := validateSignal(*signal); err != nil {
			return 0, false, 0, err
		}
		if !strings.EqualFold(signal.Symbol, state.Symbol) ||
			signal.Timeframe != state.Timeframe ||
			!signal.CandleStart.Equal(state.LastProcessedCandleStart) {
			return 0, false, 0, fmt.Errorf("%w: signal must belong to the candle being committed", ErrInvalidInput)
		}
	} else {
		advanced, err := advanceScannerState(ctx, r.pool, state)
		if err != nil {
			return 0, false, 0, fmt.Errorf("advance scanner cursor without signal: %w", err)
		}
		if !advanced {
			return 0, false, 0, ErrStaleCandle
		}
		return 0, false, 0, nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, false, 0, fmt.Errorf("begin closed candle commit: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	advanced, err := advanceScannerState(ctx, tx, state)
	if err != nil {
		return 0, false, 0, fmt.Errorf("advance scanner cursor: %w", err)
	}
	if !advanced {
		return 0, false, 0, ErrStaleCandle
	}

	var signalID int64
	signalInserted := false
	var deliveriesEnqueued int64
	if signal != nil {
		signalID, signalInserted, err = insertSignal(ctx, tx, *signal)
		if err != nil {
			return 0, false, 0, fmt.Errorf("insert signal during candle commit: %w", err)
		}
		if enqueue && signalInserted {
			deliveriesEnqueued, err = enqueueSignalForEligibleUsers(ctx, tx, signalID)
			if err != nil {
				return 0, false, 0, fmt.Errorf("enqueue signal deliveries during candle commit: %w", err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, false, 0, fmt.Errorf("commit closed candle: %w", err)
	}
	return signalID, signalInserted, deliveriesEnqueued, nil
}

// EnqueueSignalForEligibleUsers creates deliveries for users whose saved filters accept this event.
func (r *Repository) EnqueueSignalForEligibleUsers(ctx context.Context, signalID int64) (int64, error) {
	return enqueueSignalForEligibleUsers(ctx, r.pool, signalID)
}

type signalDeliveryExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func enqueueSignalForEligibleUsers(ctx context.Context, executor signalDeliveryExecer, signalID int64) (int64, error) {
	if signalID <= 0 {
		return 0, fmt.Errorf("%w: signal ID must be positive", ErrInvalidInput)
	}
	tag, err := executor.Exec(ctx, `
		WITH selected_signal AS (
			SELECT id, symbol, timeframe, direction, rsi_length, ema_length
			FROM signals
			WHERE id = $1
		),
		ranked_instruments AS (
			SELECT symbol, row_number() OVER (ORDER BY turnover_24h DESC, symbol) AS market_rank
			FROM instruments
			WHERE is_active = TRUE
		)
		INSERT INTO signal_deliveries (user_id, signal_id)
		SELECT u.id, s.id
		FROM selected_signal s
		JOIN telegram_users u ON u.is_active = TRUE
		JOIN user_settings settings ON settings.user_id = u.id
		JOIN user_timeframes tf ON tf.user_id = u.id AND tf.timeframe = s.timeframe
		WHERE settings.rsi_length = s.rsi_length
		  AND settings.ema_length = s.ema_length
		  AND (
			(s.direction = 'LONG' AND settings.notify_long = TRUE)
			OR (s.direction = 'SHORT' AND settings.notify_short = TRUE)
		)
		AND NOT EXISTS (
			SELECT 1
			FROM user_instrument_preferences excluded
			WHERE excluded.user_id = u.id
			  AND excluded.symbol = s.symbol
			  AND excluded.preference = 'exclude'
		)
		AND (
			settings.scan_all_instruments = TRUE
			OR EXISTS (
				SELECT 1
				FROM user_instrument_preferences included
				WHERE included.user_id = u.id
				  AND included.symbol = s.symbol
				  AND included.preference = 'include'
			)
			OR EXISTS (
				SELECT 1
				FROM ranked_instruments ranked
				WHERE ranked.symbol = s.symbol
				  AND ranked.market_rank <= settings.instrument_limit
			)
		)
		ON CONFLICT (user_id, signal_id) DO NOTHING
	`, signalID)
	if err != nil {
		return 0, fmt.Errorf("enqueue signal for eligible users: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (r *Repository) CreateSignalDeliveries(ctx context.Context, signalID int64, userIDs []int64) error {
	if signalID <= 0 {
		return fmt.Errorf("%w: signal ID must be positive", ErrInvalidInput)
	}
	if len(userIDs) == 0 {
		return nil
	}
	unique := make(map[int64]struct{}, len(userIDs))
	ids := make([]int64, 0, len(userIDs))
	for _, id := range userIDs {
		if id <= 0 {
			return fmt.Errorf("%w: user IDs must be positive", ErrInvalidInput)
		}
		if _, exists := unique[id]; exists {
			continue
		}
		unique[id] = struct{}{}
		ids = append(ids, id)
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO signal_deliveries (user_id, signal_id)
		SELECT user_id, $1 FROM unnest($2::bigint[]) AS selected(user_id)
		ON CONFLICT (user_id, signal_id) DO NOTHING
	`, signalID, ids)
	if err != nil {
		return fmt.Errorf("create signal deliveries: %w", err)
	}
	return nil
}

// ClaimPendingDeliveries leases work atomically across concurrent notification workers.
func (r *Repository) ClaimPendingDeliveries(ctx context.Context, limit int, lease time.Duration) ([]Delivery, error) {
	return r.claimPendingDeliveries(ctx, limit, lease, nil)
}

// ClaimPendingDeliveriesForUsers restricts queue claims to the configured
// Telegram allowlist. Filtering happens before LIMIT so unrelated rows cannot
// starve deliveries for authorized users.
func (r *Repository) ClaimPendingDeliveriesForUsers(ctx context.Context, limit int, lease time.Duration, telegramIDs []int64) ([]Delivery, error) {
	if len(telegramIDs) == 0 {
		return nil, fmt.Errorf("%w: Telegram delivery allowlist must not be empty", ErrInvalidInput)
	}
	seen := make(map[int64]struct{}, len(telegramIDs))
	unique := make([]int64, 0, len(telegramIDs))
	for _, id := range telegramIDs {
		if id <= 0 {
			return nil, fmt.Errorf("%w: Telegram delivery allowlist contains an invalid ID", ErrInvalidInput)
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	return r.claimPendingDeliveries(ctx, limit, lease, unique)
}

func (r *Repository) claimPendingDeliveries(ctx context.Context, limit int, lease time.Duration, telegramIDs []int64) ([]Delivery, error) {
	if limit < 1 || limit > 1000 || lease <= 0 {
		return nil, fmt.Errorf("%w: delivery limit or lease is invalid", ErrInvalidInput)
	}
	rows, err := r.pool.Query(ctx, `
		WITH candidates AS (
			SELECT d.user_id, d.signal_id
			FROM signal_deliveries d
			JOIN telegram_users eligible_user ON eligible_user.id = d.user_id
			WHERE (
				d.status = 'pending'
				OR (d.status = 'failed' AND COALESCE(d.next_attempt_at, now()) <= now())
				OR (d.status = 'sending' AND d.lease_until <= now())
			)
			  AND eligible_user.is_active = TRUE
			  AND ($3::bigint[] IS NULL OR eligible_user.telegram_id = ANY($3))
			ORDER BY d.created_at, d.user_id
			LIMIT $1
			FOR UPDATE OF d SKIP LOCKED
		)
		UPDATE signal_deliveries d
		SET status = 'sending',
		    attempt_count = d.attempt_count + 1,
		    next_attempt_at = NULL,
		    last_attempt_at = now(),
		    lease_until = now() + ($2 * interval '1 second'),
		    updated_at = now()
		FROM candidates c
		JOIN telegram_users u ON u.id = c.user_id
		JOIN signals s ON s.id = c.signal_id
		WHERE d.user_id = c.user_id AND d.signal_id = c.signal_id
		  AND u.is_active = TRUE
		RETURNING d.user_id, u.telegram_id, d.signal_id, d.status, d.attempt_count,
		          s.symbol, s.timeframe, s.direction, s.candle_start, s.candle_close,
		          s.close_price::float8, s.rsi::float8, s.ema_rsi::float8, s.rsi_length, s.ema_length, s.detected_at
	`, limit, lease.Seconds(), telegramIDs)
	if err != nil {
		return nil, fmt.Errorf("claim pending signal deliveries: %w", err)
	}
	defer rows.Close()

	result := make([]Delivery, 0)
	for rows.Next() {
		var item Delivery
		if err := rows.Scan(
			&item.UserID, &item.TelegramID, &item.SignalID, &item.Status, &item.AttemptCount,
			&item.Symbol, &item.Timeframe, &item.Direction, &item.CandleStart, &item.CandleClose,
			&item.ClosePrice, &item.RSI, &item.EMARSI, &item.RSILength, &item.EMALength, &item.DetectedAt,
		); err != nil {
			return nil, fmt.Errorf("scan claimed signal delivery: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed signal deliveries: %w", err)
	}
	return result, nil
}

func (r *Repository) MarkDeliverySent(ctx context.Context, userID, signalID int64) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE signal_deliveries
		SET status = 'sent', sent_at = now(), lease_until = NULL, last_error = '', updated_at = now()
		WHERE user_id = $1 AND signal_id = $2 AND status = 'sending'
	`, userID, signalID)
	if err != nil {
		return fmt.Errorf("mark signal delivery sent: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repository) MarkDeliveryFailed(ctx context.Context, userID, signalID int64, nextAttempt time.Time, deliveryErr error) error {
	if deliveryErr == nil {
		return fmt.Errorf("%w: delivery error is required", ErrInvalidInput)
	}
	message := deliveryErr.Error()
	if len(message) > 4000 {
		message = message[:4000]
	}
	tag, err := r.pool.Exec(ctx, `
		UPDATE signal_deliveries
		SET status = 'failed', next_attempt_at = $3, lease_until = NULL,
		    last_error = $4, updated_at = now()
		WHERE user_id = $1 AND signal_id = $2 AND status = 'sending'
	`, userID, signalID, nextAttempt, message)
	if err != nil {
		return fmt.Errorf("mark signal delivery failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repository) ListUserSignals(ctx context.Context, userID int64, filters SignalFilters) ([]Signal, error) {
	if userID <= 0 {
		return nil, fmt.Errorf("%w: user ID must be positive", ErrInvalidInput)
	}
	if filters.Limit <= 0 {
		filters.Limit = 20
	}
	if filters.Limit > 100 {
		filters.Limit = 100
	}
	if filters.Offset < 0 {
		return nil, fmt.Errorf("%w: offset must not be negative", ErrInvalidInput)
	}
	if filters.Direction != "" && filters.Direction != "LONG" && filters.Direction != "SHORT" {
		return nil, fmt.Errorf("%w: invalid signal direction", ErrInvalidInput)
	}
	if filters.Timeframe != "" && filters.Timeframe != "5" && filters.Timeframe != "15" && filters.Timeframe != "30" && filters.Timeframe != "60" {
		return nil, fmt.Errorf("%w: invalid signal timeframe", ErrInvalidInput)
	}

	rows, err := r.pool.Query(ctx, `
		SELECT s.id, s.symbol, s.timeframe, s.direction, s.candle_start, s.candle_close,
		       s.close_price::float8, s.rsi::float8, s.ema_rsi::float8, s.rsi_length, s.ema_length, s.detected_at
		FROM signal_deliveries d
		JOIN signals s ON s.id = d.signal_id
		WHERE d.user_id = $1
		  AND ($2 = '' OR s.symbol = $2)
		  AND ($3 = '' OR s.direction = $3)
		  AND ($4 = '' OR s.timeframe = $4)
		  AND ($5::timestamptz IS NULL OR s.detected_at >= $5)
		ORDER BY s.detected_at DESC, s.id DESC
		LIMIT $6 OFFSET $7
	`, userID, strings.ToUpper(strings.TrimSpace(filters.Symbol)), filters.Direction,
		filters.Timeframe, filters.Since, filters.Limit, filters.Offset)
	if err != nil {
		return nil, fmt.Errorf("list user signal history: %w", err)
	}
	defer rows.Close()

	result := make([]Signal, 0)
	for rows.Next() {
		var item Signal
		if err := rows.Scan(
			&item.ID, &item.Symbol, &item.Timeframe, &item.Direction, &item.CandleStart,
			&item.CandleClose, &item.ClosePrice, &item.RSI, &item.EMARSI, &item.RSILength, &item.EMALength, &item.DetectedAt,
		); err != nil {
			return nil, fmt.Errorf("scan user signal history: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate user signal history: %w", err)
	}
	return result, nil
}

func (r *Repository) CountUserSignalsSince(ctx context.Context, userID int64, since time.Time) (int64, error) {
	var count int64
	err := r.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM signal_deliveries d
		JOIN signals s ON s.id = d.signal_id
		WHERE d.user_id = $1 AND s.detected_at >= $2
	`, userID, since).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count user signals: %w", err)
	}
	return count, nil
}
