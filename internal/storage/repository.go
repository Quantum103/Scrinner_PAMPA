package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound     = errors.New("storage: record not found")
	ErrStaleCandle  = errors.New("storage: candle is not newer than the stored cursor")
	ErrInvalidInput = errors.New("storage: invalid input")
)

type Repository struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) (*Repository, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: PostgreSQL pool is nil", ErrInvalidInput)
	}
	return &Repository{pool: pool}, nil
}

func (r *Repository) EnsureTelegramUser(ctx context.Context, user TelegramUser) (TelegramUser, error) {
	if user.TelegramID <= 0 {
		return TelegramUser{}, fmt.Errorf("%w: Telegram ID must be positive", ErrInvalidInput)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return TelegramUser{}, fmt.Errorf("begin ensure Telegram user: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var saved TelegramUser
	err = tx.QueryRow(ctx, `
		INSERT INTO telegram_users (
			telegram_id, username, first_name, last_name, language_code, last_seen_at
		)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (telegram_id) DO UPDATE SET
			username = CASE WHEN EXCLUDED.username = '' THEN telegram_users.username ELSE EXCLUDED.username END,
			first_name = CASE WHEN EXCLUDED.first_name = '' THEN telegram_users.first_name ELSE EXCLUDED.first_name END,
			last_name = CASE WHEN EXCLUDED.last_name = '' THEN telegram_users.last_name ELSE EXCLUDED.last_name END,
			language_code = CASE WHEN EXCLUDED.language_code = '' THEN telegram_users.language_code ELSE EXCLUDED.language_code END,
			last_seen_at = now(),
			updated_at = now()
		RETURNING id, telegram_id, username, first_name, last_name, language_code,
		          is_active, created_at, updated_at, last_seen_at
	`, user.TelegramID, user.Username, user.FirstName, user.LastName, user.LanguageCode).Scan(
		&saved.ID, &saved.TelegramID, &saved.Username, &saved.FirstName, &saved.LastName,
		&saved.LanguageCode, &saved.IsActive, &saved.CreatedAt, &saved.UpdatedAt, &saved.LastSeenAt,
	)
	if err != nil {
		return TelegramUser{}, fmt.Errorf("upsert Telegram user: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO user_settings (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING
	`, saved.ID); err != nil {
		return TelegramUser{}, fmt.Errorf("create default user settings: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO user_timeframes (user_id, timeframe)
		VALUES ($1, '5'), ($1, '15'), ($1, '60')
		ON CONFLICT (user_id, timeframe) DO NOTHING
	`, saved.ID); err != nil {
		return TelegramUser{}, fmt.Errorf("create default user timeframes: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return TelegramUser{}, fmt.Errorf("commit Telegram user: %w", err)
	}
	return saved, nil
}

func (r *Repository) GetUserByTelegramID(ctx context.Context, telegramID int64) (TelegramUser, error) {
	var user TelegramUser
	err := r.pool.QueryRow(ctx, `
		SELECT id, telegram_id, username, first_name, last_name, language_code,
		       is_active, created_at, updated_at, last_seen_at
		FROM telegram_users WHERE telegram_id = $1
	`, telegramID).Scan(
		&user.ID, &user.TelegramID, &user.Username, &user.FirstName, &user.LastName,
		&user.LanguageCode, &user.IsActive, &user.CreatedAt, &user.UpdatedAt, &user.LastSeenAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return TelegramUser{}, ErrNotFound
	}
	if err != nil {
		return TelegramUser{}, fmt.Errorf("get Telegram user: %w", err)
	}
	return user, nil
}

func DefaultUserSettings(userID int64) UserSettings {
	return UserSettings{
		UserID:             userID,
		RSILength:          14,
		EMALength:          9,
		InstrumentLimit:    100,
		ScanAllInstruments: false,
		NotifyLong:         true,
		NotifyShort:        true,
		NotifyConfluence:   true,
		Timeframes:         []string{"5", "15", "60"},
	}
}

func (r *Repository) GetOrCreateUserSettings(ctx context.Context, userID int64) (UserSettings, error) {
	if userID <= 0 {
		return UserSettings{}, fmt.Errorf("%w: user ID must be positive", ErrInvalidInput)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return UserSettings{}, fmt.Errorf("begin get user settings: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if _, err := tx.Exec(ctx, `
		INSERT INTO user_settings (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING
	`, userID); err != nil {
		return UserSettings{}, fmt.Errorf("ensure user settings: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO user_timeframes (user_id, timeframe)
		VALUES ($1, '5'), ($1, '15'), ($1, '60')
		ON CONFLICT (user_id, timeframe) DO NOTHING
	`, userID); err != nil {
		return UserSettings{}, fmt.Errorf("ensure user timeframes: %w", err)
	}

	settings, err := scanSettings(ctx, tx, userID)
	if err != nil {
		return UserSettings{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return UserSettings{}, fmt.Errorf("commit get user settings: %w", err)
	}
	return settings, nil
}

func (r *Repository) UpdateUserSettings(ctx context.Context, settings UserSettings) error {
	if err := validateSettings(settings); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin update user settings: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	tag, err := tx.Exec(ctx, `
		UPDATE user_settings
		SET rsi_length = $2,
		    ema_length = $3,
		    instrument_limit = $4,
		    scan_all_instruments = $5,
		    notify_long = $6,
		    notify_short = $7,
		    notify_confluence = $8,
		    updated_at = now()
		WHERE user_id = $1
	`, settings.UserID, settings.RSILength, settings.EMALength, settings.InstrumentLimit,
		settings.ScanAllInstruments, settings.NotifyLong, settings.NotifyShort, settings.NotifyConfluence)
	if err != nil {
		return fmt.Errorf("update user settings: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	if _, err := tx.Exec(ctx, `DELETE FROM user_timeframes WHERE user_id = $1`, settings.UserID); err != nil {
		return fmt.Errorf("replace user timeframes: %w", err)
	}
	for _, timeframe := range settings.Timeframes {
		if _, err := tx.Exec(ctx, `
			INSERT INTO user_timeframes (user_id, timeframe) VALUES ($1, $2)
		`, settings.UserID, timeframe); err != nil {
			return fmt.Errorf("save user timeframe %q: %w", timeframe, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit user settings: %w", err)
	}
	return nil
}

func scanSettings(ctx context.Context, tx pgx.Tx, userID int64) (UserSettings, error) {
	var settings UserSettings
	err := tx.QueryRow(ctx, `
		SELECT user_id, rsi_length, ema_length, instrument_limit, scan_all_instruments,
		       notify_long, notify_short, notify_confluence
		FROM user_settings WHERE user_id = $1
	`, userID).Scan(
		&settings.UserID, &settings.RSILength, &settings.EMALength, &settings.InstrumentLimit,
		&settings.ScanAllInstruments, &settings.NotifyLong, &settings.NotifyShort, &settings.NotifyConfluence,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return UserSettings{}, ErrNotFound
	}
	if err != nil {
		return UserSettings{}, fmt.Errorf("read user settings: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT timeframe FROM user_timeframes
		WHERE user_id = $1
		ORDER BY CASE timeframe WHEN '5' THEN 1 WHEN '15' THEN 2 WHEN '60' THEN 3 END
	`, userID)
	if err != nil {
		return UserSettings{}, fmt.Errorf("read user timeframes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var timeframe string
		if err := rows.Scan(&timeframe); err != nil {
			return UserSettings{}, fmt.Errorf("scan user timeframe: %w", err)
		}
		settings.Timeframes = append(settings.Timeframes, timeframe)
	}
	if err := rows.Err(); err != nil {
		return UserSettings{}, fmt.Errorf("iterate user timeframes: %w", err)
	}
	return settings, nil
}

func validateSettings(settings UserSettings) error {
	if settings.UserID <= 0 {
		return fmt.Errorf("%w: user ID must be positive", ErrInvalidInput)
	}
	if settings.RSILength < 2 || settings.RSILength > 500 {
		return fmt.Errorf("%w: RSI length must be between 2 and 500", ErrInvalidInput)
	}
	if settings.EMALength < 1 || settings.EMALength > 500 {
		return fmt.Errorf("%w: EMA length must be between 1 and 500", ErrInvalidInput)
	}
	if settings.InstrumentLimit < 1 || settings.InstrumentLimit > 10000 {
		return fmt.Errorf("%w: instrument limit must be between 1 and 10000", ErrInvalidInput)
	}
	if len(settings.Timeframes) == 0 {
		return fmt.Errorf("%w: at least one timeframe must be enabled", ErrInvalidInput)
	}
	seen := make(map[string]struct{}, len(settings.Timeframes))
	for _, timeframe := range settings.Timeframes {
		if timeframe != "5" && timeframe != "15" && timeframe != "60" {
			return fmt.Errorf("%w: unsupported timeframe %q", ErrInvalidInput, timeframe)
		}
		if _, ok := seen[timeframe]; ok {
			return fmt.Errorf("%w: duplicate timeframe %q", ErrInvalidInput, timeframe)
		}
		seen[timeframe] = struct{}{}
	}
	return nil
}

func (r *Repository) UpsertInstrument(ctx context.Context, instrument Instrument) error {
	if err := validateInstrument(instrument); err != nil {
		return err
	}
	if _, err := r.pool.Exec(ctx, instrumentUpsertSQL,
		instrument.Symbol, instrument.BaseCoin, instrument.QuoteCoin, instrument.SettleCoin,
		instrument.ContractType, instrument.Status, instrument.Status == "Trading",
		instrument.Turnover24h, instrument.LastPrice, nullableTime(instrument.TickerUpdatedAt)); err != nil {
		return fmt.Errorf("upsert instrument %s: %w", instrument.Symbol, err)
	}
	return nil
}

// SyncInstrumentCatalog deactivates the previous catalogue and atomically upserts the latest REST snapshot.
func (r *Repository) SyncInstrumentCatalog(ctx context.Context, instruments []Instrument) error {
	for _, instrument := range instruments {
		if err := validateInstrument(instrument); err != nil {
			return err
		}
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin instrument catalogue sync: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if _, err := tx.Exec(ctx, `UPDATE instruments SET is_active = FALSE, updated_at = now()`); err != nil {
		return fmt.Errorf("deactivate previous instrument catalogue: %w", err)
	}
	for _, instrument := range instruments {
		if _, err := tx.Exec(ctx, instrumentUpsertSQL,
			instrument.Symbol, instrument.BaseCoin, instrument.QuoteCoin, instrument.SettleCoin,
			instrument.ContractType, instrument.Status, instrument.Status == "Trading",
			instrument.Turnover24h, instrument.LastPrice, nullableTime(instrument.TickerUpdatedAt)); err != nil {
			return fmt.Errorf("upsert instrument %s: %w", instrument.Symbol, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit instrument catalogue sync: %w", err)
	}
	return nil
}

const instrumentUpsertSQL = `
	INSERT INTO instruments (
		symbol, base_coin, quote_coin, settle_coin, contract_type, status, is_active,
		turnover_24h, last_price, instrument_updated_at, ticker_updated_at
	)
	VALUES ($1, $2, $3, $4, $5, $6, $7, COALESCE(NULLIF($8, '')::numeric, 0), NULLIF($9, '')::numeric, now(), $10)
	ON CONFLICT (symbol) DO UPDATE SET
		base_coin = EXCLUDED.base_coin,
		quote_coin = EXCLUDED.quote_coin,
		settle_coin = EXCLUDED.settle_coin,
		contract_type = EXCLUDED.contract_type,
		status = EXCLUDED.status,
		is_active = EXCLUDED.is_active,
		turnover_24h = EXCLUDED.turnover_24h,
		last_price = COALESCE(EXCLUDED.last_price, instruments.last_price),
		instrument_updated_at = now(),
		ticker_updated_at = COALESCE(EXCLUDED.ticker_updated_at, instruments.ticker_updated_at),
		updated_at = now()
`

func validateInstrument(instrument Instrument) error {
	if strings.TrimSpace(instrument.Symbol) == "" || strings.TrimSpace(instrument.BaseCoin) == "" {
		return fmt.Errorf("%w: instrument symbol and base coin are required", ErrInvalidInput)
	}
	if instrument.QuoteCoin != "USDT" || instrument.SettleCoin != "USDT" || instrument.ContractType != "LinearPerpetual" {
		return fmt.Errorf("%w: instrument %s is not a USDT LinearPerpetual", ErrInvalidInput, instrument.Symbol)
	}
	if strings.TrimSpace(instrument.Status) == "" {
		return fmt.Errorf("%w: instrument %s has empty status", ErrInvalidInput, instrument.Symbol)
	}
	if instrument.Turnover24h != "" {
		turnover, err := parsePositiveNumber(instrument.Turnover24h)
		if err != nil || turnover < 0 {
			return fmt.Errorf("%w: invalid turnover for %s", ErrInvalidInput, instrument.Symbol)
		}
	}
	if instrument.LastPrice != "" {
		value, err := parsePositiveNumber(instrument.LastPrice)
		if err != nil || value <= 0 {
			return fmt.Errorf("%w: invalid last price for %s", ErrInvalidInput, instrument.Symbol)
		}
	}
	return nil
}

func parsePositiveNumber(value string) (float64, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("invalid number")
	}
	return parsed, nil
}

func nullableTime(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}
	return *value
}

func (r *Repository) ListActiveInstruments(ctx context.Context, limit int) ([]Instrument, error) {
	if limit < 1 || limit > 10000 {
		return nil, fmt.Errorf("%w: instrument limit must be between 1 and 10000", ErrInvalidInput)
	}
	rows, err := r.pool.Query(ctx, `
		SELECT symbol, base_coin, quote_coin, settle_coin, contract_type, status, is_active,
		       turnover_24h::text, COALESCE(last_price::text, ''),
		       instrument_updated_at, ticker_updated_at
		FROM instruments
		WHERE is_active = TRUE
		ORDER BY turnover_24h DESC, symbol
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list active instruments: %w", err)
	}
	defer rows.Close()

	result := make([]Instrument, 0)
	for rows.Next() {
		var item Instrument
		if err := rows.Scan(
			&item.Symbol, &item.BaseCoin, &item.QuoteCoin, &item.SettleCoin,
			&item.ContractType, &item.Status, &item.IsActive, &item.Turnover24h,
			&item.LastPrice, &item.InstrumentUpdatedAt, &item.TickerUpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan active instrument: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active instruments: %w", err)
	}
	return result, nil
}

func (r *Repository) SetInstrumentPreference(ctx context.Context, userID int64, symbol, preference string) error {
	if userID <= 0 || strings.TrimSpace(symbol) == "" {
		return fmt.Errorf("%w: user ID and symbol are required", ErrInvalidInput)
	}
	if preference != "include" && preference != "exclude" {
		return fmt.Errorf("%w: preference must be include or exclude", ErrInvalidInput)
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO user_instrument_preferences (user_id, symbol, preference)
		VALUES ($1, $2, $3)
		ON CONFLICT (user_id, symbol) DO UPDATE SET
			preference = EXCLUDED.preference,
			updated_at = now()
	`, userID, strings.ToUpper(strings.TrimSpace(symbol)), preference)
	if err != nil {
		return fmt.Errorf("set instrument preference: %w", err)
	}
	return nil
}

func (r *Repository) ClearInstrumentPreference(ctx context.Context, userID int64, symbol string) error {
	tag, err := r.pool.Exec(ctx, `
		DELETE FROM user_instrument_preferences WHERE user_id = $1 AND symbol = $2
	`, userID, strings.ToUpper(strings.TrimSpace(symbol)))
	if err != nil {
		return fmt.Errorf("clear instrument preference: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repository) ListInstrumentPreferences(ctx context.Context, userID int64) ([]InstrumentPreference, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT symbol, preference, updated_at
		FROM user_instrument_preferences
		WHERE user_id = $1
		ORDER BY preference, symbol
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("list instrument preferences: %w", err)
	}
	defer rows.Close()

	result := make([]InstrumentPreference, 0)
	for rows.Next() {
		var item InstrumentPreference
		if err := rows.Scan(&item.Symbol, &item.Preference, &item.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan instrument preference: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate instrument preferences: %w", err)
	}
	return result, nil
}
