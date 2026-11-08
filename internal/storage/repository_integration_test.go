package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testRepository(t *testing.T) (*Repository, *pgxpool.Pool) {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_TEST_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_TEST_URL is not set; PostgreSQL integration test skipped")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create test PostgreSQL pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("connect to test PostgreSQL: %v", err)
	}
	if err := migrations.Up(ctx, pool, filepath.Join("..", "..", "migrations")); err != nil {
		pool.Close()
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE TABLE telegram_users, spot_instruments, spot_momentum_alerts RESTART IDENTITY CASCADE"); err != nil {
		pool.Close()
		t.Fatalf("reset integration test database: %v", err)
	}
	repository, err := New(pool)
	if err != nil {
		pool.Close()
		t.Fatalf("create repository: %v", err)
	}
	t.Cleanup(pool.Close)
	return repository, pool
}

func uniqueSuffix() string {
	return fmt.Sprintf("%X", time.Now().UnixNano())
}

func createTestUser(t *testing.T, repository *Repository) TelegramUser {
	t.Helper()
	user, err := repository.EnsureTelegramUser(context.Background(), TelegramUser{
		TelegramID: time.Now().UnixNano(),
		Username: "spot_test",
		FirstName: "Test",
	})
	if err != nil {
		t.Fatalf("ensure Telegram user: %v", err)
	}
	return user
}

func TestEnsureTelegramUserAndActivate(t *testing.T) {
	repository, pool := testRepository(t)
	ctx := context.Background()
	user := createTestUser(t, repository)
	loaded, err := repository.GetUserByTelegramID(ctx, user.TelegramID)
	if err != nil || loaded.ID != user.ID {
		t.Fatalf("load Telegram user: %+v err=%v", loaded, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE telegram_users SET is_active = FALSE WHERE id = $1", user.ID); err != nil {
		t.Fatal(err)
	}
	if err := repository.ActivateTelegramUser(ctx, user.TelegramID); err != nil {
		t.Fatal(err)
	}
	loaded, err = repository.GetUserByTelegramID(ctx, user.TelegramID)
	if err != nil || !loaded.IsActive {
		t.Fatalf("user was not activated: %+v err=%v", loaded, err)
	}
}

func TestRSISchemaIsRemovedAndSpotSchemaRemains(t *testing.T) {
	_, pool := testRepository(t)
	ctx := context.Background()
	for _, table := range []string{
		"signals", "signal_deliveries", "scanner_states", "scanner_profile_states",
		"user_settings", "user_timeframes", "instruments",
	} {
		var exists bool
		if err := pool.QueryRow(ctx, "SELECT to_regclass('public.' || $1) IS NOT NULL", table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Errorf("RSI table %q still exists after migration 000009", table)
		}
	}
	for _, table := range []string{
		"spot_instruments", "spot_watchlist", "spot_momentum_alerts",
		"spot_alert_subscriptions", "spot_alert_deliveries", "spot_alert_rules", "telegram_users",
	} {
		var exists bool
		if err := pool.QueryRow(ctx, "SELECT to_regclass('public.' || $1) IS NOT NULL", table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Errorf("Spot table %q is missing after migration 000009", table)
		}
	}
}
