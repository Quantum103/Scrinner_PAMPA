package storage

import (
	"context"
	"errors"
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
	if _, err := pool.Exec(ctx, `TRUNCATE TABLE telegram_users, instruments RESTART IDENTITY CASCADE`); err != nil {
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
		Username:   "scanner_test",
		FirstName:  "Test",
	})
	if err != nil {
		t.Fatalf("ensure Telegram user: %v", err)
	}
	return user
}

func createTestInstrument(t *testing.T, repository *Repository) string {
	t.Helper()
	symbol := "TST" + uniqueSuffix() + "USDT"
	err := repository.UpsertInstrument(context.Background(), Instrument{
		Symbol:       symbol,
		BaseCoin:     "TST",
		QuoteCoin:    "USDT",
		SettleCoin:   "USDT",
		ContractType: "LinearPerpetual",
		Status:       "Trading",
		Turnover24h:  "125000.12345678",
		LastPrice:    "1.234567890123",
	})
	if err != nil {
		t.Fatalf("upsert test instrument: %v", err)
	}
	return symbol
}

func TestEnsureTelegramUserAndPersistSettings(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	user := createTestUser(t, repository)

	settings, err := repository.GetOrCreateUserSettings(ctx, user.ID)
	if err != nil {
		t.Fatalf("get default settings: %v", err)
	}
	if settings.RSILength != 14 || settings.EMALength != 9 || settings.InstrumentLimit != 100 {
		t.Fatalf("unexpected defaults: %+v", settings)
	}
	if len(settings.Timeframes) != 3 || settings.Timeframes[0] != "5" || settings.Timeframes[1] != "15" || settings.Timeframes[2] != "60" {
		t.Fatalf("unexpected default timeframes: %#v", settings.Timeframes)
	}

	settings.RSILength = 21
	settings.EMALength = 7
	settings.InstrumentLimit = 250
	settings.ScanAllInstruments = true
	settings.NotifyLong = false
	settings.NotifyShort = true
	settings.NotifyConfluence = false
	settings.Timeframes = []string{"15", "60"}
	if err := repository.UpdateUserSettings(ctx, settings); err != nil {
		t.Fatalf("update settings: %v", err)
	}

	reloaded, err := repository.GetOrCreateUserSettings(ctx, user.ID)
	if err != nil {
		t.Fatalf("reload settings: %v", err)
	}
	if reloaded.RSILength != 21 || reloaded.EMALength != 7 || reloaded.InstrumentLimit != 250 ||
		!reloaded.ScanAllInstruments || reloaded.NotifyLong || !reloaded.NotifyShort || reloaded.NotifyConfluence {
		t.Fatalf("settings were not persisted: %+v", reloaded)
	}
	if len(reloaded.Timeframes) != 2 || reloaded.Timeframes[0] != "15" || reloaded.Timeframes[1] != "60" {
		t.Fatalf("timeframes were not persisted: %#v", reloaded.Timeframes)
	}

	otherUser := createTestUser(t, repository)
	otherSettings, err := repository.GetOrCreateUserSettings(ctx, otherUser.ID)
	if err != nil {
		t.Fatalf("get second user's settings: %v", err)
	}
	if otherSettings.RSILength != 14 || otherSettings.EMALength != 9 || len(otherSettings.Timeframes) != 3 {
		t.Fatalf("user settings are not isolated: %+v", otherSettings)
	}
}

func TestInstrumentCatalogAndUserPreferences(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	user := createTestUser(t, repository)
	symbol := createTestInstrument(t, repository)

	if err := repository.SetInstrumentPreference(ctx, user.ID, symbol, "include"); err != nil {
		t.Fatalf("set include preference: %v", err)
	}
	if err := repository.SetInstrumentPreference(ctx, user.ID, symbol, "exclude"); err != nil {
		t.Fatalf("update preference: %v", err)
	}
	preferences, err := repository.ListInstrumentPreferences(ctx, user.ID)
	if err != nil {
		t.Fatalf("list preferences: %v", err)
	}
	if len(preferences) != 1 || preferences[0].Symbol != symbol || preferences[0].Preference != "exclude" {
		t.Fatalf("unexpected instrument preferences: %+v", preferences)
	}
	if err := repository.ClearInstrumentPreference(ctx, user.ID, symbol); err != nil {
		t.Fatalf("clear preference: %v", err)
	}
	if err := repository.ClearInstrumentPreference(ctx, user.ID, symbol); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second clear error = %v, want ErrNotFound", err)
	}

	snapshot := []Instrument{{
		Symbol:       symbol,
		BaseCoin:     "TST",
		QuoteCoin:    "USDT",
		SettleCoin:   "USDT",
		ContractType: "LinearPerpetual",
		Status:       "Trading",
		Turnover24h:  "500",
		LastPrice:    "2.5",
	}}
	if err := repository.SyncInstrumentCatalog(ctx, snapshot); err != nil {
		t.Fatalf("sync instrument catalogue: %v", err)
	}
	active, err := repository.ListActiveInstruments(ctx, 10)
	if err != nil {
		t.Fatalf("list active instruments: %v", err)
	}
	found := false
	for _, instrument := range active {
		if instrument.Symbol == symbol {
			found = true
			if instrument.Turnover24h != "500" {
				t.Fatalf("turnover = %s, want 500", instrument.Turnover24h)
			}
		}
	}
	if !found {
		t.Fatalf("instrument %s not found in active catalogue", symbol)
	}
}

func TestSignalDeduplicationDeliveryRetryAndUserHistory(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	user := createTestUser(t, repository)
	otherUser := createTestUser(t, repository)
	symbol := createTestInstrument(t, repository)
	start := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute)
	signal := Signal{
		Symbol:      symbol,
		Timeframe:   "5",
		Direction:   "LONG",
		CandleStart: start,
		CandleClose: start.Add(5 * time.Minute),
		ClosePrice:  12.345,
		RSI:         56.82,
		EMARSI:      54.31,
		DetectedAt:  time.Now().UTC(),
	}

	signalID, inserted, err := repository.SaveSignal(ctx, signal)
	if err != nil {
		t.Fatalf("save signal: %v", err)
	}
	if !inserted || signalID <= 0 {
		t.Fatalf("first signal save: id=%d inserted=%t", signalID, inserted)
	}
	duplicateID, inserted, err := repository.SaveSignal(ctx, signal)
	if err != nil {
		t.Fatalf("save duplicate signal: %v", err)
	}
	if inserted || duplicateID != signalID {
		t.Fatalf("duplicate signal was not deduplicated: id=%d inserted=%t", duplicateID, inserted)
	}

	if err := repository.CreateSignalDeliveries(ctx, signalID, []int64{user.ID, user.ID}); err != nil {
		t.Fatalf("create signal deliveries: %v", err)
	}
	if err := repository.CreateSignalDeliveries(ctx, signalID, []int64{user.ID}); err != nil {
		t.Fatalf("create duplicate signal delivery: %v", err)
	}

	claimed, err := repository.ClaimPendingDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("claim pending deliveries: %v", err)
	}
	if len(claimed) != 1 || claimed[0].TelegramID != user.TelegramID || claimed[0].AttemptCount != 1 {
		t.Fatalf("unexpected claimed deliveries: %+v", claimed)
	}

	if err := repository.MarkDeliveryFailed(ctx, user.ID, signalID, time.Now().Add(-time.Second), errors.New("temporary Telegram error")); err != nil {
		t.Fatalf("mark delivery failed: %v", err)
	}
	retried, err := repository.ClaimPendingDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("claim retry: %v", err)
	}
	if len(retried) != 1 || retried[0].AttemptCount != 2 {
		t.Fatalf("retry attempt was not incremented: %+v", retried)
	}
	if err := repository.MarkDeliverySent(ctx, user.ID, signalID); err != nil {
		t.Fatalf("mark delivery sent: %v", err)
	}
	remaining, err := repository.ClaimPendingDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("check sent delivery: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("sent delivery was claimed again: %+v", remaining)
	}

	history, err := repository.ListUserSignals(ctx, user.ID, SignalFilters{Limit: 20})
	if err != nil {
		t.Fatalf("list user's signal history: %v", err)
	}
	if len(history) != 1 || history[0].ID != signalID {
		t.Fatalf("unexpected user's signal history: %+v", history)
	}
	otherHistory, err := repository.ListUserSignals(ctx, otherUser.ID, SignalFilters{Limit: 20})
	if err != nil {
		t.Fatalf("list another user's signal history: %v", err)
	}
	if len(otherHistory) != 0 {
		t.Fatalf("signal history leaked between users: %+v", otherHistory)
	}
	count, err := repository.CountUserSignalsSince(ctx, user.ID, start.Add(-time.Hour))
	if err != nil {
		t.Fatalf("count user's signals: %v", err)
	}
	if count != 1 {
		t.Fatalf("signal count = %d, want 1", count)
	}
}

func TestScannerStateInitializationAndAtomicCandleCommit(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	symbol := createTestInstrument(t, repository)
	start := time.Now().UTC().Truncate(time.Minute).Add(-10 * time.Minute)

	baseline := ScannerState{
		Symbol:                   symbol,
		Timeframe:                "5",
		LastProcessedCandleStart: start,
		LastClose:                10,
		LastRSI:                  48,
		LastEMARSI:               49,
		Initialized:              true,
	}
	state, created, err := repository.InitializeScannerState(ctx, baseline)
	if err != nil {
		t.Fatalf("initialize scanner state: %v", err)
	}
	if !created || !state.LastProcessedCandleStart.Equal(start) {
		t.Fatalf("initial cursor was not created: created=%t state=%+v", created, state)
	}
	_, created, err = repository.InitializeScannerState(ctx, baseline)
	if err != nil {
		t.Fatalf("initialize existing scanner state: %v", err)
	}
	if created {
		t.Fatal("reinitialization overwrote or recreated an existing cursor")
	}

	nextStart := start.Add(5 * time.Minute)
	crossAt := nextStart.Add(5 * time.Minute)
	signal := &Signal{
		Symbol:      symbol,
		Timeframe:   "5",
		Direction:   "LONG",
		CandleStart: nextStart,
		CandleClose: crossAt,
		ClosePrice:  11,
		RSI:         52,
		EMARSI:      50,
	}
	next := ScannerState{
		Symbol:                   symbol,
		Timeframe:                "5",
		LastProcessedCandleStart: nextStart,
		LastClose:                11,
		LastRSI:                  52,
		LastEMARSI:               50,
		LastCrossDirection:       "LONG",
		LastCrossAt:              &crossAt,
		Initialized:              true,
	}
	committedSignalID, signalInserted, err := repository.CommitClosedCandle(ctx, next, signal)
	if err != nil {
		t.Fatalf("commit closed candle with signal: %v", err)
	}
	if !signalInserted || committedSignalID <= 0 {
		t.Fatalf("signal was not inserted together with the candle cursor: id=%d inserted=%t", committedSignalID, signalInserted)
	}
	if _, _, err := repository.CommitClosedCandle(ctx, next, signal); !errors.Is(err, ErrStaleCandle) {
		t.Fatalf("duplicate candle commit error = %v, want ErrStaleCandle", err)
	}

	saved, err := repository.GetScannerState(ctx, symbol, "5")
	if err != nil {
		t.Fatalf("get scanner state: %v", err)
	}
	if !saved.LastProcessedCandleStart.Equal(nextStart) || saved.LastCrossDirection != "LONG" {
		t.Fatalf("scanner cursor was not persisted: %+v", saved)
	}
	history, err := repository.ListUserSignals(ctx, 0, SignalFilters{Limit: 10})
	if err == nil || history != nil {
		t.Fatal("history query with invalid user ID should fail")
	}
}

func TestEnqueueSignalForEligibleUsersAppliesUserFilters(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	eligible := createTestUser(t, repository)
	longDisabled := createTestUser(t, repository)
	timeframeDisabled := createTestUser(t, repository)
	excluded := createTestUser(t, repository)

	settings, err := repository.GetOrCreateUserSettings(ctx, longDisabled.ID)
	if err != nil {
		t.Fatal(err)
	}
	settings.NotifyLong = false
	if err := repository.UpdateUserSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}

	settings, err = repository.GetOrCreateUserSettings(ctx, timeframeDisabled.ID)
	if err != nil {
		t.Fatal(err)
	}
	settings.Timeframes = []string{"15"}
	if err := repository.UpdateUserSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}

	symbol := createTestInstrument(t, repository)
	if err := repository.SetInstrumentPreference(ctx, excluded.ID, symbol, "exclude"); err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute)
	signalID, inserted, err := repository.SaveSignal(ctx, Signal{
		Symbol:      symbol,
		Timeframe:   "5",
		Direction:   "LONG",
		CandleStart: start,
		CandleClose: start.Add(5 * time.Minute),
		ClosePrice:  3,
		RSI:         55,
		EMARSI:      52,
	})
	if err != nil || !inserted {
		t.Fatalf("save signal: id=%d inserted=%t err=%v", signalID, inserted, err)
	}

	enqueued, err := repository.EnqueueSignalForEligibleUsers(ctx, signalID)
	if err != nil {
		t.Fatalf("enqueue eligible deliveries: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("enqueued deliveries = %d, want 1", enqueued)
	}
	claimed, err := repository.ClaimPendingDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].TelegramID != eligible.TelegramID {
		t.Fatalf("signal was delivered to users outside saved filters: %+v", claimed)
	}
}

