package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// SubscribeSpotAlerts enables Spot momentum notifications for an active Telegram user.
// Calling it more than once is safe.
func (r *Repository) SubscribeSpotAlerts(ctx context.Context, userID int64) error {
	if userID <= 0 {
		return fmt.Errorf("%w: user ID must be positive", ErrInvalidInput)
	}
	tag, err := r.pool.Exec(ctx, `
		INSERT INTO spot_alert_subscriptions (user_id)
		SELECT id FROM telegram_users WHERE id = $1 AND is_active = TRUE
		ON CONFLICT (user_id) DO NOTHING
	`, userID)
	if err != nil {
		return fmt.Errorf("subscribe to Spot alerts: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var active bool
		err := r.pool.QueryRow(ctx, `
			SELECT is_active FROM telegram_users WHERE id = $1
		`, userID).Scan(&active)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && !active) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("verify Telegram user for Spot subscription: %w", err)
		}
	}
	return nil
}

// UnsubscribeSpotAlerts disables Spot alerts and cancels deliveries that have not
// completed.
func (r *Repository) UnsubscribeSpotAlerts(ctx context.Context, userID int64) error {
	if userID <= 0 {
		return fmt.Errorf("%w: user ID must be positive", ErrInvalidInput)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin Spot unsubscription: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if _, err := tx.Exec(ctx, `
		DELETE FROM spot_alert_subscriptions WHERE user_id = $1
	`, userID); err != nil {
		return fmt.Errorf("remove Spot subscription: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE spot_alert_deliveries
		SET status = 'cancelled', lease_until = NULL, next_attempt_at = NULL,
		    updated_at = now()
		WHERE user_id = $1 AND status IN ('pending', 'failed', 'sending')
	`, userID); err != nil {
		return fmt.Errorf("cancel pending Spot deliveries: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit Spot unsubscription: %w", err)
	}
	return nil
}

// SaveSpotMomentumAlert stores a detected market event and enqueues it for every
// currently subscribed, active Telegram user in one transaction.
func (r *Repository) SaveSpotMomentumAlert(ctx context.Context, alert SpotMomentumAlert) (int64, bool, int64, error) {
	if err := validateSpotMomentumAlert(alert); err != nil {
		return 0, false, 0, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, false, 0, fmt.Errorf("begin Spot alert save: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var alertID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO spot_momentum_alerts (
			symbol, detected_at, reference_at, current_price, reference_price,
			change_percent, window_seconds
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (symbol, detected_at, reference_at) DO NOTHING
		RETURNING id
	`, strings.ToUpper(strings.TrimSpace(alert.Symbol)), alert.DetectedAt, alert.ReferenceAt,
		alert.Price, alert.ReferencePrice, alert.ChangePercent, alert.WindowSeconds).Scan(&alertID)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `
			SELECT id FROM spot_momentum_alerts
			WHERE symbol = $1 AND detected_at = $2 AND reference_at = $3
		`, strings.ToUpper(strings.TrimSpace(alert.Symbol)), alert.DetectedAt, alert.ReferenceAt).Scan(&alertID)
		if err != nil {
			return 0, false, 0, fmt.Errorf("load existing Spot alert: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, false, 0, fmt.Errorf("commit duplicate Spot alert lookup: %w", err)
		}
		return alertID, false, 0, nil
	}
	if err != nil {
		return 0, false, 0, fmt.Errorf("insert Spot momentum alert: %w", err)
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO spot_alert_deliveries (user_id, alert_id)
		SELECT subscription.user_id, $1
		FROM spot_alert_subscriptions subscription
		JOIN telegram_users user_account ON user_account.id = subscription.user_id
		WHERE user_account.is_active = TRUE
		ON CONFLICT (user_id, alert_id) DO NOTHING
	`, alertID)
	if err != nil {
		return 0, false, 0, fmt.Errorf("enqueue Spot alert deliveries: %w", err)
	}
	queued := tag.RowsAffected()
	if err := tx.Commit(ctx); err != nil {
		return 0, false, 0, fmt.Errorf("commit Spot momentum alert: %w", err)
	}
	return alertID, true, queued, nil
}

func validateSpotMomentumAlert(alert SpotMomentumAlert) error {
	symbol := strings.ToUpper(strings.TrimSpace(alert.Symbol))
	if len(symbol) < 3 || len(symbol) > 30 {
		return fmt.Errorf("%w: Spot alert symbol length is invalid", ErrInvalidInput)
	}
	for _, char := range symbol {
		if !(char >= 'A' && char <= 'Z') && !(char >= '0' && char <= '9') {
			return fmt.Errorf("%w: Spot alert symbol contains invalid characters", ErrInvalidInput)
		}
	}
	if alert.DetectedAt.IsZero() || alert.ReferenceAt.IsZero() || !alert.DetectedAt.After(alert.ReferenceAt) {
		return fmt.Errorf("%w: Spot alert timestamps are invalid", ErrInvalidInput)
	}
	for name, value := range map[string]float64{
		"current price": alert.Price, "reference price": alert.ReferencePrice,
		"change percent": alert.ChangePercent,
	} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("%w: Spot alert %s must be finite", ErrInvalidInput, name)
		}
	}
	if alert.Price <= 0 || alert.ReferencePrice <= 0 || alert.ChangePercent <= 0 {
		return fmt.Errorf("%w: Spot alert prices and change must be positive", ErrInvalidInput)
	}
	if alert.WindowSeconds < 1 || alert.WindowSeconds > 86400 {
		return fmt.Errorf("%w: Spot alert window must be between 1 second and 24 hours", ErrInvalidInput)
	}
	return nil
}

// ClaimPendingSpotAlertDeliveries leases queued Spot alerts without allowing
// concurrent workers to claim the same user/event pair.
func (r *Repository) ClaimPendingSpotAlertDeliveries(ctx context.Context, limit int, lease time.Duration) ([]SpotAlertDelivery, error) {
	if limit < 1 || limit > 1000 || lease <= 0 {
		return nil, fmt.Errorf("%w: Spot delivery limit or lease is invalid", ErrInvalidInput)
	}
	rows, err := r.pool.Query(ctx, `
		WITH candidates AS (
			SELECT delivery.user_id, delivery.alert_id
			FROM spot_alert_deliveries delivery
			JOIN spot_alert_subscriptions subscription ON subscription.user_id = delivery.user_id
			JOIN telegram_users eligible_user ON eligible_user.id = delivery.user_id
			WHERE (
				delivery.status = 'pending'
				OR (delivery.status = 'failed' AND COALESCE(delivery.next_attempt_at, now()) <= now())
				OR (delivery.status = 'sending' AND delivery.lease_until <= now())
			)
			  AND eligible_user.is_active = TRUE
			ORDER BY delivery.created_at, delivery.user_id
			LIMIT $1
			FOR UPDATE OF delivery SKIP LOCKED
		)
		UPDATE spot_alert_deliveries delivery
		SET status = 'sending',
		    attempt_count = delivery.attempt_count + 1,
		    next_attempt_at = NULL,
		    last_attempt_at = now(),
		    lease_until = now() + ($2 * interval '1 second'),
		    updated_at = now()
		FROM candidates candidate
		JOIN telegram_users user_account ON user_account.id = candidate.user_id
		JOIN spot_momentum_alerts alert ON alert.id = candidate.alert_id
		WHERE delivery.user_id = candidate.user_id
		  AND delivery.alert_id = candidate.alert_id
		RETURNING delivery.user_id, user_account.telegram_id, delivery.alert_id,
		          delivery.attempt_count, alert.symbol, alert.detected_at, alert.reference_at,
		          alert.current_price::float8, alert.reference_price::float8,
		          alert.change_percent::float8, alert.window_seconds
	`, limit, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim pending Spot alert deliveries: %w", err)
	}
	defer rows.Close()

	result := make([]SpotAlertDelivery, 0)
	for rows.Next() {
		var item SpotAlertDelivery
		if err := rows.Scan(
			&item.UserID, &item.TelegramID, &item.AlertID, &item.AttemptCount,
			&item.Symbol, &item.DetectedAt, &item.ReferenceAt, &item.Price,
			&item.ReferencePrice, &item.ChangePercent, &item.WindowSeconds,
		); err != nil {
			return nil, fmt.Errorf("scan claimed Spot alert delivery: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed Spot alert deliveries: %w", err)
	}
	return result, nil
}

func (r *Repository) MarkSpotAlertDeliverySent(ctx context.Context, userID, alertID int64) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE spot_alert_deliveries
		SET status = 'sent', sent_at = now(), lease_until = NULL,
		    next_attempt_at = NULL, last_error = '', updated_at = now()
		WHERE user_id = $1 AND alert_id = $2 AND status = 'sending'
	`, userID, alertID)
	if err != nil {
		return fmt.Errorf("mark Spot alert delivery sent: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repository) MarkSpotAlertDeliveryFailed(ctx context.Context, userID, alertID int64, nextAttempt time.Time, deliveryErr error) error {
	if deliveryErr == nil {
		return fmt.Errorf("%w: Spot delivery error is required", ErrInvalidInput)
	}
	message := deliveryErr.Error()
	if len(message) > 4000 {
		message = message[:4000]
	}
	tag, err := r.pool.Exec(ctx, `
		UPDATE spot_alert_deliveries
		SET status = 'failed', next_attempt_at = $3, lease_until = NULL,
		    last_error = $4, updated_at = now()
		WHERE user_id = $1 AND alert_id = $2 AND status = 'sending'
	`, userID, alertID, nextAttempt, message)
	if err != nil {
		return fmt.Errorf("mark Spot alert delivery failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
