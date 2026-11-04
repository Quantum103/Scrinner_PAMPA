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
			if instrument.Turnover24h != "500.00000000" {
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

	nextSignal := signal
	nextSignal.CandleStart = signal.CandleStart.Add(5 * time.Minute)
	nextSignal.CandleClose = signal.CandleClose.Add(5 * time.Minute)
	nextSignal.DetectedAt = time.Now().UTC()
	nextSignalID, nextInserted, err := repository.SaveSignal(ctx, nextSignal)
	if err != nil || !nextInserted {
		t.Fatalf("save allowlist test signal: id=%d inserted=%t err=%v", nextSignalID, nextInserted, err)
	}
	if err := repository.CreateSignalDeliveries(ctx, nextSignalID, []int64{user.ID, otherUser.ID}); err != nil {
		t.Fatalf("create allowlist test deliveries: %v", err)
	}
	authorized, err := repository.ClaimPendingDeliveriesForUsers(ctx, 10, time.Minute, []int64{otherUser.TelegramID})
	if err != nil {
		t.Fatalf("claim deliveries for allowlisted user: %v", err)
	}
	if len(authorized) != 1 || authorized[0].TelegramID != otherUser.TelegramID {
		t.Fatalf("allowlist did not restrict claimed deliveries: %+v", authorized)
	}
	if _, err := repository.ClaimPendingDeliveriesForUsers(ctx, 10, time.Minute, nil); err == nil {
		t.Fatal("empty Telegram delivery allowlist must be rejected")
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

func TestCommitScannedCandleAtomicallyQueuesEligibleDelivery(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	user := createTestUser(t, repository)
	symbol := createTestInstrument(t, repository)
	start := time.Now().UTC().Truncate(5 * time.Minute).Add(-10 * time.Minute)
	closeAt := start.Add(5 * time.Minute)

	state := ScannerState{
		Symbol: symbol, Timeframe: "5", LastProcessedCandleStart: start,
		LastClose: 101, LastRSI: 55, LastEMARSI: 52,
		LastCrossDirection: "LONG", LastCrossAt: &closeAt, Initialized: true,
	}
	signal := &Signal{
		Symbol: symbol, Timeframe: "5", Direction: "LONG",
		CandleStart: start, CandleClose: closeAt, ClosePrice: 101,
		RSI: 55, EMARSI: 52,
	}
	signalID, inserted, queued, err := repository.CommitScannedCandle(ctx, state, signal)
	if err != nil {
		t.Fatalf("commit scanned candle: %v", err)
	}
	if signalID <= 0 || !inserted || queued != 1 {
		t.Fatalf("signal commit = id:%d inserted:%t queued:%d, want id, true, 1", signalID, inserted, queued)
	}

	claimed, err := repository.ClaimPendingDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("claim queued delivery: %v", err)
	}
	if len(claimed) != 1 || claimed[0].UserID != user.ID || claimed[0].SignalID != signalID {
		t.Fatalf("unexpected claimed deliveries: %+v", claimed)
	}
	if _, _, _, err := repository.CommitScannedCandle(ctx, state, signal); !errors.Is(err, ErrStaleCandle) {
		t.Fatalf("replayed candle error = %v, want ErrStaleCandle", err)
	}
}

func TestScannerProfilesHaveIndependentCursorsAndDeliveries(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	defaultUser := createTestUser(t, repository)
	customUser := createTestUser(t, repository)
	customSettings, err := repository.GetOrCreateUserSettings(ctx, customUser.ID)
	if err != nil {
		t.Fatal(err)
	}
	customSettings.RSILength = 10
	customSettings.EMALength = 5
	if err := repository.UpdateUserSettings(ctx, customSettings); err != nil {
		t.Fatal(err)
	}

	symbol := createTestInstrument(t, repository)
	profiles, err := repository.ListScannerProfilesForPair(ctx, symbol, "5")
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 2 {
		t.Fatalf("profiles for selected pair = %+v, want default and custom", profiles)
	}
	workloads, err := repository.ListScannerWorkloads(ctx)
	if err != nil {
		t.Fatal(err)
	}
	foundDefault, foundCustom := false, false
	for _, workload := range workloads {
		if workload.Timeframe != "5" || len(workload.Symbols) != 1 || workload.Symbols[0] != symbol {
			continue
		}
		foundDefault = foundDefault || (workload.RSILength == 14 && workload.EMALength == 9)
		foundCustom = foundCustom || (workload.RSILength == 10 && workload.EMALength == 5)
	}
	if !foundDefault || !foundCustom {
		t.Fatalf("workloads omit one of the user's profiles: %+v", workloads)
	}

	start := time.Now().UTC().Truncate(5 * time.Minute).Add(-10 * time.Minute)
	closeAt := start.Add(5 * time.Minute)
	makeState := func(rsiLength, emaLength int) ScannerProfileState {
		return ScannerProfileState{
			Symbol: symbol, Timeframe: "5", RSILength: rsiLength, EMALength: emaLength,
			LastProcessedCandleStart: start, LastClose: 101, LastRSI: 55, LastEMARSI: 52,
			LastCrossDirection: "LONG", LastCrossAt: &closeAt, Initialized: true,
		}
	}
	makeSignal := func(rsiLength, emaLength int) *Signal {
		return &Signal{
			Symbol: symbol, Timeframe: "5", Direction: "LONG",
			CandleStart: start, CandleClose: closeAt, ClosePrice: 101,
			RSI: 55, EMARSI: 52, RSILength: rsiLength, EMALength: emaLength,
		}
	}
	defaultID, inserted, queued, err := repository.CommitScannerProfileCandle(ctx, makeState(14, 9), makeSignal(14, 9))
	if err != nil || !inserted || queued != 1 {
		t.Fatalf("default profile commit: id=%d inserted=%t queued=%d err=%v", defaultID, inserted, queued, err)
	}
	customID, inserted, queued, err := repository.CommitScannerProfileCandle(ctx, makeState(10, 5), makeSignal(10, 5))
	if err != nil || !inserted || queued != 1 || customID == defaultID {
		t.Fatalf("custom profile commit: id=%d inserted=%t queued=%d err=%v", customID, inserted, queued, err)
	}
	if _, err := repository.GetScannerProfileState(ctx, symbol, "5", 14, 9); err != nil {
		t.Fatalf("load default profile cursor: %v", err)
	}
	if _, err := repository.GetScannerProfileState(ctx, symbol, "5", 10, 5); err != nil {
		t.Fatalf("load custom profile cursor: %v", err)
	}

	claimed, err := repository.ClaimPendingDeliveries(ctx, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d deliveries, want two profile-specific deliveries: %+v", len(claimed), claimed)
	}
	delivered := make(map[int64]Delivery, len(claimed))
	for _, delivery := range claimed {
		delivered[delivery.UserID] = delivery
	}
	if got := delivered[defaultUser.ID]; got.SignalID != defaultID || got.RSILength != 14 || got.EMALength != 9 {
		t.Fatalf("default user received wrong profile signal: %+v", got)
	}
	if got := delivered[customUser.ID]; got.SignalID != customID || got.RSILength != 10 || got.EMALength != 5 {
		t.Fatalf("custom user received wrong profile signal: %+v", got)
	}
}

func TestCommitScannerCandleWithoutSignalAdvancesCursorAndRejectsReplay(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	symbol := createTestInstrument(t, repository)
	start := time.Now().UTC().Truncate(5 * time.Minute)

	legacy := ScannerState{
		Symbol: symbol, Timeframe: "5", LastProcessedCandleStart: start,
		LastClose: 10, LastRSI: 51, LastEMARSI: 50, Initialized: true,
	}
	id, inserted, queued, err := repository.CommitScannedCandle(ctx, legacy, nil)
	if err != nil || id != 0 || inserted || queued != 0 {
		t.Fatalf("legacy no-signal commit: id=%d inserted=%t queued=%d err=%v", id, inserted, queued, err)
	}
	if _, _, _, err := repository.CommitScannedCandle(ctx, legacy, nil); !errors.Is(err, ErrStaleCandle) {
		t.Fatalf("legacy replay error = %v, want ErrStaleCandle", err)
	}

	profile := ScannerProfileState{
		Symbol: symbol, Timeframe: "5", RSILength: 14, EMALength: 9,
		LastProcessedCandleStart: start, LastClose: 10, LastRSI: 51, LastEMARSI: 50,
		Initialized: true,
	}
	id, inserted, queued, err = repository.CommitScannerProfileCandle(ctx, profile, nil)
	if err != nil || id != 0 || inserted || queued != 0 {
		t.Fatalf("profile no-signal commit: id=%d inserted=%t queued=%d err=%v", id, inserted, queued, err)
	}
	if _, _, _, err := repository.CommitScannerProfileCandle(ctx, profile, nil); !errors.Is(err, ErrStaleCandle) {
		t.Fatalf("profile replay error = %v, want ErrStaleCandle", err)
	}
	if _, err := repository.GetScannerState(ctx, symbol, "5"); err != nil {
		t.Fatalf("legacy no-signal cursor was not saved: %v", err)
	}
	if _, err := repository.GetScannerProfileState(ctx, symbol, "5", 14, 9); err != nil {
		t.Fatalf("profile no-signal cursor was not saved: %v", err)
	}
}

func Test30MinuteSignalsAndScannerStatesPersist(t *testing.T) {
	repository, _ := testRepository(t)
	ctx := context.Background()
	symbol := createTestInstrument(t, repository)
	start := time.Now().UTC().Truncate(30 * time.Minute)

	scannerState := ScannerState{
		Symbol: symbol, Timeframe: "30", LastProcessedCandleStart: start,
		LastClose: 100, LastRSI: 55, LastEMARSI: 50, Initialized: true,
	}
	if _, inserted, err := repository.InitializeScannerState(ctx, scannerState); err != nil || !inserted {
		t.Fatalf("initialize 30m scanner state: inserted=%v error=%v", inserted, err)
	}

	profileState := ScannerProfileState{
		Symbol: symbol, Timeframe: "30", RSILength: 14, EMALength: 9,
		LastProcessedCandleStart: start, LastClose: 100,
		LastRSI: 55, LastEMARSI: 50, Initialized: true,
	}
	if _, inserted, err := repository.InitializeScannerProfileState(ctx, profileState); err != nil || !inserted {
		t.Fatalf("initialize 30m profile state: inserted=%v error=%v", inserted, err)
	}

	signal := Signal{
		Symbol: symbol, Timeframe: "30", Direction: "LONG",
		CandleStart: start, CandleClose: start.Add(30 * time.Minute),
		ClosePrice: 100, RSI: 55, EMARSI: 50, RSILength: 14, EMALength: 9,
	}
	if _, inserted, err := repository.SaveSignal(ctx, signal); err != nil || !inserted {
		t.Fatalf("save 30m signal: inserted=%v error=%v", inserted, err)
	}
}
