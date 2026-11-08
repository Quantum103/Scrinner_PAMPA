package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound     = errors.New("storage: record not found")
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
	var saved TelegramUser
	err := r.pool.QueryRow(ctx, `
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
	return saved, nil
}

func (r *Repository) ActivateTelegramUser(ctx context.Context, telegramID int64) error {
	if telegramID <= 0 {
		return fmt.Errorf("%w: Telegram ID must be positive", ErrInvalidInput)
	}
	tag, err := r.pool.Exec(ctx, `
		UPDATE telegram_users
		SET is_active = TRUE, updated_at = now(), last_seen_at = now()
		WHERE telegram_id = $1
	`, telegramID)
	if err != nil {
		return fmt.Errorf("activate Telegram user: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
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
