package storage

import (
	"context"
	"fmt"
)

func (r *Repository) ListUserSpotAlertHistory(ctx context.Context, userID int64, limit, offset int) ([]SpotAlertHistoryItem, error) {
	if userID <= 0 || limit < 1 || limit > 50 || offset < 0 || offset > 10000 {
		return nil, fmt.Errorf("%w: invalid Spot history filters", ErrInvalidInput)
	}
	rows, err := r.pool.Query(ctx, `
		SELECT alert.id, alert.symbol, alert.detected_at, alert.reference_at,
		       alert.current_price::float8, alert.reference_price::float8,
		       alert.change_percent::float8, alert.window_seconds
		FROM spot_alert_deliveries delivery
		JOIN spot_momentum_alerts alert ON alert.id = delivery.alert_id
		WHERE delivery.user_id = $1 AND delivery.status = 'sent'
		ORDER BY alert.detected_at DESC, alert.id DESC
		LIMIT $2 OFFSET $3
	`, userID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list user's Spot alert history: %w", err)
	}
	defer rows.Close()

	result := make([]SpotAlertHistoryItem, 0)
	for rows.Next() {
		var item SpotAlertHistoryItem
		if err := rows.Scan(
			&item.ID, &item.Symbol, &item.DetectedAt, &item.ReferenceAt,
			&item.Price, &item.ReferencePrice, &item.ChangePercent, &item.WindowSeconds,
		); err != nil {
			return nil, fmt.Errorf("scan Spot alert history item: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Spot alert history: %w", err)
	}
	return result, nil
}

func (r *Repository) ClearUserSpotAlertHistory(ctx context.Context, userID int64) (int64, error) {
	if userID <= 0 {
		return 0, fmt.Errorf("%w: user ID must be positive", ErrInvalidInput)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin Spot history cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `DELETE FROM spot_alert_deliveries WHERE user_id = $1`, userID)
	if err != nil {
		return 0, fmt.Errorf("delete user's Spot history: %w", err)
	}
	deleted := tag.RowsAffected()
	if _, err := tx.Exec(ctx, `
		DELETE FROM spot_momentum_alerts alert
		WHERE NOT EXISTS (
			SELECT 1 FROM spot_alert_deliveries delivery WHERE delivery.alert_id = alert.id
		)
	`); err != nil {
		return 0, fmt.Errorf("delete orphaned Spot alert events: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit Spot history cleanup: %w", err)
	}
	return deleted, nil
}
