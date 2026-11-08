package storage

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"
)

const (
	MinSpotAlertThresholdPercent = 0.01
	MaxSpotAlertThresholdPercent = 1000.0
	MinSpotAlertWindowSeconds    = 60
	MaxSpotAlertWindowSeconds    = 3600
	MaxSpotAlertRulesPerUser     = 20
	DefaultSpotAlertThreshold    = 5.0
	DefaultSpotAlertWindow       = 15 * 60
)

var ErrLastSpotAlertRule = errors.New("at least one Spot alert rule must remain")

func ValidateSpotAlertRule(thresholdPercent float64, windowSeconds int) error {
	if math.IsNaN(thresholdPercent) || math.IsInf(thresholdPercent, 0) ||
		thresholdPercent < MinSpotAlertThresholdPercent ||
		thresholdPercent > MaxSpotAlertThresholdPercent {
		return fmt.Errorf("%w: threshold must be between %.2f and %.0f percent",
			ErrInvalidInput, MinSpotAlertThresholdPercent, MaxSpotAlertThresholdPercent)
	}
	if math.Abs(thresholdPercent*10000-math.Round(thresholdPercent*10000)) > 1e-7 {
		return fmt.Errorf("%w: threshold supports up to four decimal places", ErrInvalidInput)
	}
	if windowSeconds < MinSpotAlertWindowSeconds ||
		windowSeconds > MaxSpotAlertWindowSeconds ||
		windowSeconds%60 != 0 {
		return fmt.Errorf("%w: timeframe must be between 1 and 60 whole minutes",
			ErrInvalidInput)
	}
	return nil
}

// AddSpotAlertRule adds a personal rule. Duplicate requests are idempotent;
// each user can keep no more than MaxSpotAlertRulesPerUser rules.
func (r *Repository) AddSpotAlertRule(ctx context.Context, userID int64, thresholdPercent float64, windowSeconds int) (SpotAlertRule, bool, error) {
	if userID <= 0 {
		return SpotAlertRule{}, false, fmt.Errorf("%w: user ID must be positive", ErrInvalidInput)
	}
	if err := ValidateSpotAlertRule(thresholdPercent, windowSeconds); err != nil {
		return SpotAlertRule{}, false, err
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return SpotAlertRule{}, false, fmt.Errorf("begin Spot rule creation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var lockedUserID int64
	if err := tx.QueryRow(ctx, `SELECT id FROM telegram_users WHERE id = $1 FOR UPDATE`, userID).Scan(&lockedUserID); errors.Is(err, pgx.ErrNoRows) {
		return SpotAlertRule{}, false, ErrNotFound
	} else if err != nil {
		return SpotAlertRule{}, false, fmt.Errorf("lock Telegram user for Spot rule creation: %w", err)
	}

	var existing SpotAlertRule
	err = tx.QueryRow(ctx, `
		SELECT id, user_id, threshold_percent::float8, window_seconds
		FROM spot_alert_rules
		WHERE user_id = $1 AND threshold_percent = $2::numeric AND window_seconds = $3
	`, userID, thresholdPercent, windowSeconds).Scan(
		&existing.ID, &existing.UserID, &existing.ThresholdPercent, &existing.WindowSeconds,
	)
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return SpotAlertRule{}, false, fmt.Errorf("commit duplicate Spot rule lookup: %w", err)
		}
		return existing, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return SpotAlertRule{}, false, fmt.Errorf("find existing Spot rule: %w", err)
	}

	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM spot_alert_rules WHERE user_id = $1`, userID).Scan(&count); err != nil {
		return SpotAlertRule{}, false, fmt.Errorf("count user's Spot rules: %w", err)
	}
	if count >= MaxSpotAlertRulesPerUser {
		return SpotAlertRule{}, false, fmt.Errorf("%w: no more than %d Spot rules are allowed",
			ErrInvalidInput, MaxSpotAlertRulesPerUser)
	}

	var saved SpotAlertRule
	err = tx.QueryRow(ctx, `
		INSERT INTO spot_alert_rules (user_id, threshold_percent, window_seconds)
		VALUES ($1, $2, $3)
		RETURNING id, user_id, threshold_percent::float8, window_seconds
	`, userID, thresholdPercent, windowSeconds).Scan(
		&saved.ID, &saved.UserID, &saved.ThresholdPercent, &saved.WindowSeconds,
	)
	if err != nil {
		return SpotAlertRule{}, false, fmt.Errorf("insert Spot alert rule: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return SpotAlertRule{}, false, fmt.Errorf("commit Spot rule creation: %w", err)
	}
	return saved, true, nil
}

func (r *Repository) ListSpotAlertRules(ctx context.Context, userID int64) ([]SpotAlertRule, error) {
	if userID <= 0 {
		return nil, fmt.Errorf("%w: user ID must be positive", ErrInvalidInput)
	}
	rows, err := r.pool.Query(ctx, `
		SELECT id, user_id, threshold_percent::float8, window_seconds
		FROM spot_alert_rules WHERE user_id = $1
		ORDER BY window_seconds, threshold_percent, id
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("list user's Spot alert rules: %w", err)
	}
	defer rows.Close()

	rules := make([]SpotAlertRule, 0)
	for rows.Next() {
		var rule SpotAlertRule
		if err := rows.Scan(&rule.ID, &rule.UserID, &rule.ThresholdPercent, &rule.WindowSeconds); err != nil {
			return nil, fmt.Errorf("scan user's Spot alert rule: %w", err)
		}
		rules = append(rules, rule)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate user's Spot alert rules: %w", err)
	}
	return rules, nil
}

// ListActiveSpotAlertRules returns rules that should currently be monitored.
// The tracker de-duplicates equivalent rules shared by several users.
func (r *Repository) ListActiveSpotAlertRules(ctx context.Context) ([]SpotAlertRule, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT DISTINCT ON (rule.window_seconds, rule.threshold_percent)
		       rule.id, rule.user_id, rule.threshold_percent::float8, rule.window_seconds
		FROM spot_alert_rules rule
		JOIN spot_alert_subscriptions subscription ON subscription.user_id = rule.user_id
		JOIN telegram_users user_account ON user_account.id = rule.user_id
		WHERE user_account.is_active = TRUE
		ORDER BY rule.window_seconds, rule.threshold_percent, rule.id
	`)
	if err != nil {
		return nil, fmt.Errorf("list active Spot alert rules: %w", err)
	}
	defer rows.Close()

	rules := make([]SpotAlertRule, 0)
	for rows.Next() {
		var rule SpotAlertRule
		if err := rows.Scan(&rule.ID, &rule.UserID, &rule.ThresholdPercent, &rule.WindowSeconds); err != nil {
			return nil, fmt.Errorf("scan active Spot alert rule: %w", err)
		}
		rules = append(rules, rule)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active Spot alert rules: %w", err)
	}
	return rules, nil
}

// DeleteSpotAlertRule removes one of the user's rules, but never leaves the
// account without any signal rule.
func (r *Repository) DeleteSpotAlertRule(ctx context.Context, userID, ruleID int64) error {
	if userID <= 0 || ruleID <= 0 {
		return fmt.Errorf("%w: user and rule IDs must be positive", ErrInvalidInput)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin Spot rule deletion: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var lockedUserID int64
	if err := tx.QueryRow(ctx, `SELECT id FROM telegram_users WHERE id = $1 FOR UPDATE`, userID).Scan(&lockedUserID); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return fmt.Errorf("lock Telegram user for Spot rule deletion: %w", err)
	}

	var target SpotAlertRule
	if err := tx.QueryRow(ctx, `
		SELECT threshold_percent::float8, window_seconds
		FROM spot_alert_rules WHERE id = $1 AND user_id = $2
	`, ruleID, userID).Scan(&target.ThresholdPercent, &target.WindowSeconds); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return fmt.Errorf("load Spot rule before deletion: %w", err)
	}
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM spot_alert_rules WHERE user_id = $1`, userID).Scan(&count); err != nil {
		return fmt.Errorf("count user's Spot rules before deletion: %w", err)
	}
	if count <= 1 {
		return ErrLastSpotAlertRule
	}
	if _, err := tx.Exec(ctx, `
		UPDATE spot_alert_deliveries delivery
		SET status = 'cancelled', lease_until = NULL, next_attempt_at = NULL,
		    updated_at = now()
		FROM spot_momentum_alerts alert
		WHERE delivery.alert_id = alert.id AND delivery.user_id = $1
		  AND alert.threshold_percent = $2::numeric
		  AND alert.window_seconds = $3
		  AND delivery.status IN ('pending', 'failed', 'sending')
	`, userID, target.ThresholdPercent, target.WindowSeconds); err != nil {
		return fmt.Errorf("cancel pending deliveries for removed Spot rule: %w", err)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM spot_alert_rules WHERE id = $1 AND user_id = $2`, ruleID, userID)
	if err != nil {
		return fmt.Errorf("delete Spot alert rule: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit Spot rule deletion: %w", err)
	}
	return nil
}
