package scanner

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
)

type fakeSource struct {
	serverTime time.Time
	candles    map[string][]bybit.Candle
	err        error
	calls      int
}

func (f *fakeSource) GetServerTime(context.Context) (time.Time, error) {
	return f.serverTime, nil
}

func (f *fakeSource) GetKlinesAt(_ context.Context, symbol, timeframe string, limit int, serverTime time.Time) ([]bybit.Candle, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if limit < 1 || !serverTime.Equal(f.serverTime) {
		return nil, errors.New("invalid kline request")
	}
	return append([]bybit.Candle(nil), f.candles[pairKey(symbol, timeframe)]...), nil
}

type fakeRepository struct {
	instruments []storage.Instrument
	states      map[string]storage.ScannerState
	signals     []storage.Signal
	deliveries  int64
	commits     int
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{states: make(map[string]storage.ScannerState)}
}

func (f *fakeRepository) ListActiveInstruments(_ context.Context, limit int) ([]storage.Instrument, error) {
	if limit < 1 {
		return nil, errors.New("invalid instrument limit")
	}
	result := append([]storage.Instrument(nil), f.instruments...)
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (f *fakeRepository) GetScannerState(_ context.Context, symbol, timeframe string) (storage.ScannerState, error) {
	state, ok := f.states[pairKey(symbol, timeframe)]
	if !ok {
		return storage.ScannerState{}, storage.ErrNotFound
	}
	return state, nil
}

func (f *fakeRepository) InitializeScannerState(_ context.Context, baseline storage.ScannerState) (storage.ScannerState, bool, error) {
	key := pairKey(baseline.Symbol, baseline.Timeframe)
	if state, exists := f.states[key]; exists {
		return state, false, nil
	}
	f.states[key] = baseline
	return baseline, true, nil
}

func (f *fakeRepository) CommitScannedCandle(_ context.Context, state storage.ScannerState, signal *storage.Signal) (int64, bool, int64, error) {
	key := pairKey(state.Symbol, state.Timeframe)
	if saved, exists := f.states[key]; exists && !state.LastProcessedCandleStart.After(saved.LastProcessedCandleStart) {
		return 0, false, 0, storage.ErrStaleCandle
	}
	f.states[key] = state
	f.commits++
	if signal == nil {
		return 0, false, 0, nil
	}
	f.signals = append(f.signals, *signal)
	f.deliveries++
	return int64(len(f.signals)), true, 1, nil
}

func pairKey(symbol, timeframe string) string {
	return symbol + "/" + timeframe
}

func makeCandles(symbol, timeframe string, closes []float64, start time.Time, serverTime time.Time) []bybit.Candle {
	interval, _ := timeframeDuration(timeframe)
	result := make([]bybit.Candle, len(closes))
	for index, closePrice := range closes {
		candleStart := start.Add(time.Duration(index) * interval)
		result[index] = bybit.Candle{
			Symbol: symbol, Timeframe: timeframe, Start: candleStart,
			Close: candleStart.Add(interval), Open: closePrice, High: closePrice,
			Low: closePrice, ClosePrice: closePrice, Volume: 1, Turnover: closePrice,
		}
	}
	return result
}

func newScannerForTest(t *testing.T, closes []float64) (*Scanner, *fakeSource, *fakeRepository, time.Time) {
	t.Helper()
	start := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	serverTime := start.Add(time.Duration(len(closes)) * 5 * time.Minute)
	source := &fakeSource{
		serverTime: serverTime,
		candles: map[string][]bybit.Candle{
			pairKey("BTCUSDT", "5"): makeCandles("BTCUSDT", "5", closes, start, serverTime),
		},
	}
	repository := newFakeRepository()
	repository.instruments = []storage.Instrument{{Symbol: "BTCUSDT"}}
	service, err := New(source, repository, Settings{
		Timeframes: []string{"5"}, RSILength: 3, EMALength: 2,
		InstrumentLimit: 10, HistoryCandles: 20,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	service.now = func() time.Time { return serverTime }
	return service, source, repository, start
}

func TestScanInitializesBaselineWithoutHistoricalSignals(t *testing.T) {
	closes := []float64{100, 101, 102, 103, 102, 101, 100, 99, 98, 99, 100, 101}
	service, _, repository, _ := newScannerForTest(t, closes)

	report, err := service.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce() error = %v", err)
	}
	if report.Instruments != 1 || report.PairsScanned != 1 || report.CandlesProcessed != 0 {
		t.Fatalf("unexpected report: %+v", report)
	}
	state, exists := repository.states[pairKey("BTCUSDT", "5")]
	expectedStart := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC).Add(time.Duration(len(closes)-1) * 5 * time.Minute)
	if !exists || !state.Initialized || !state.LastProcessedCandleStart.Equal(expectedStart) {
		t.Fatalf("baseline cursor = %+v, want latest candle at %s", state, expectedStart)
	}
	if len(repository.signals) != 0 {
		t.Fatalf("baseline generated historical signals: %+v", repository.signals)
	}
}

func TestScanDetectsLongCrossoverOnNewClosedCandle(t *testing.T) {
	prefix := []float64{100, 101, 102, 103, 102, 101, 100, 99, 98}
	full := []float64{100, 101, 102, 103, 102, 101, 100, 99, 98, 99, 100, 101}
	service, source, repository, start := newScannerForTest(t, prefix)

	if _, err := service.ScanOnce(context.Background()); err != nil {
		t.Fatalf("baseline scan: %v", err)
	}
	source.candles[pairKey("BTCUSDT", "5")] = makeCandles("BTCUSDT", "5", full, start, source.serverTime.Add(15*time.Minute))
	source.serverTime = source.serverTime.Add(15 * time.Minute)
	service.now = func() time.Time { return source.serverTime }

	report, err := service.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("cross scan: %v", err)
	}
	if report.SignalsInserted != 1 || report.DeliveriesQueued != 1 || len(repository.signals) != 1 {
		t.Fatalf("expected exactly one LONG signal: report=%+v signals=%+v", report, repository.signals)
	}
	signal := repository.signals[0]
	if signal.Direction != "LONG" || !signal.CandleStart.Equal(start.Add(9*5*time.Minute)) {
		t.Fatalf("unexpected crossover signal: %+v", signal)
	}

	replay, err := service.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("replay scan: %v", err)
	}
	if replay.SignalsInserted != 0 || len(repository.signals) != 1 {
		t.Fatalf("replay duplicated a signal: report=%+v signals=%+v", replay, repository.signals)
	}
}

func TestScanDetectsBothDirectionsAcrossNewCandles(t *testing.T) {
	prefix := []float64{100, 99, 98, 99, 100, 101}
	full := []float64{100, 99, 98, 99, 100, 101, 100, 99, 98, 97, 98, 99, 100, 101}
	service, source, repository, start := newScannerForTest(t, prefix)
	if _, err := service.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	source.candles[pairKey("BTCUSDT", "5")] = makeCandles("BTCUSDT", "5", full, start, source.serverTime.Add(40*time.Minute))
	source.serverTime = source.serverTime.Add(40 * time.Minute)
	service.now = func() time.Time { return source.serverTime }

	report, err := service.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("ScanOnce() error = %v", err)
	}
	if report.SignalsInserted != 2 || len(repository.signals) != 2 {
		t.Fatalf("expected SHORT and LONG signals: report=%+v signals=%+v", report, repository.signals)
	}
	if repository.signals[0].Direction != "SHORT" || repository.signals[1].Direction != "LONG" {
		t.Fatalf("unexpected signal directions: %+v", repository.signals)
	}
	if !repository.signals[0].CandleStart.Equal(start.Add(6*5*time.Minute)) ||
		!repository.signals[1].CandleStart.Equal(start.Add(10*5*time.Minute)) {
		t.Fatalf("signals attached to wrong candles: %+v", repository.signals)
	}
}

func TestScanDoesNotTreatCandleGapAsCrossover(t *testing.T) {
	prefix := []float64{100, 99, 98, 97, 98, 97}
	service, source, repository, start := newScannerForTest(t, prefix)
	if _, err := service.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	candles := makeCandles("BTCUSDT", "5", []float64{100, 99, 98, 97, 98, 97, 96}, start, source.serverTime.Add(15*time.Minute))
	candles[6].Start = candles[6].Start.Add(5 * time.Minute)
	candles[6].Close = candles[6].Start.Add(5 * time.Minute)
	source.candles[pairKey("BTCUSDT", "5")] = candles
	source.serverTime = source.serverTime.Add(20 * time.Minute)

	report, err := service.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("gap scan: %v", err)
	}
	if report.SignalsInserted != 0 || len(repository.signals) != 0 {
		t.Fatalf("candle gap generated a false crossover: report=%+v signals=%+v", report, repository.signals)
	}
}

func TestScanContinuesOtherPairsAfterOneSourceError(t *testing.T) {
	service, source, repository, _ := newScannerForTest(t, []float64{100, 101, 102, 103, 102, 101})
	repository.instruments = append(repository.instruments, storage.Instrument{Symbol: "ETHUSDT"})
	source.candles[pairKey("ETHUSDT", "5")] = nil
	source.err = errors.New("Bybit unavailable")

	report, err := service.ScanOnce(context.Background())
	if err == nil {
		t.Fatal("ScanOnce() expected source errors")
	}
	if report.PairsScanned != 0 || source.calls != 2 {
		t.Fatalf("unexpected partial scan report: %+v calls=%d", report, source.calls)
	}
}

func TestNewRejectsInvalidSettings(t *testing.T) {
	source := &fakeSource{}
	repository := newFakeRepository()
	tests := []struct {
		name     string
		settings Settings
	}{
		{name: "unsupported timeframe", settings: Settings{Timeframes: []string{"30"}, RSILength: 14, EMALength: 9, InstrumentLimit: 10, HistoryCandles: 300}},
		{name: "duplicate timeframe", settings: Settings{Timeframes: []string{"5", "5"}, RSILength: 14, EMALength: 9, InstrumentLimit: 10, HistoryCandles: 300}},
		{name: "too many candles", settings: Settings{Timeframes: []string{"5"}, RSILength: 14, EMALength: 9, InstrumentLimit: 10, HistoryCandles: 1001}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(source, repository, test.settings); err == nil {
				t.Fatal("New() expected invalid settings error")
			}
		})
	}
}

func TestNormalizeCandlesRejectsWrongInstrumentAndBadInterval(t *testing.T) {
	serverTime := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	candle := bybit.Candle{
		Symbol: "ETHUSDT", Timeframe: "5", Start: serverTime.Add(-5 * time.Minute),
		Close: serverTime, ClosePrice: 10,
	}
	if _, err := normalizeCandles([]bybit.Candle{candle}, "BTCUSDT", "5", serverTime); err == nil {
		t.Fatal("mismatched symbol must fail")
	}
	candle.Symbol = "BTCUSDT"
	candle.Close = serverTime.Add(-time.Second)
	if _, err := normalizeCandles([]bybit.Candle{candle}, "BTCUSDT", "5", serverTime); err == nil {
		t.Fatal("incorrect candle interval must fail")
	}
}

func TestPairKeyIsStable(t *testing.T) {
	if got := pairKey("BTCUSDT", "5"); got != "BTCUSDT/5" {
		t.Fatalf("pair key = %s", got)
	}
}

func ExampleScanner_ScanOnce() {
	fmt.Println("RSI crosses above EMA RSI => LONG")
	fmt.Println("RSI crosses below EMA RSI => SHORT")
	// Output:
	// RSI crosses above EMA RSI => LONG
	// RSI crosses below EMA RSI => SHORT
}

func TestProcessClosedWebSocketCandleUsesCachedHistoryAndDeduplicates(t *testing.T) {
	prefix := []float64{100, 101, 102, 103, 102, 101, 100, 99, 98}
	full := []float64{100, 101, 102, 103, 102, 101, 100, 99, 98, 99}
	service, source, repository, start := newScannerForTest(t, prefix)
	if _, err := service.ScanOnce(context.Background()); err != nil {
		t.Fatalf("initial scan: %v", err)
	}

	candle := makeCandles("BTCUSDT", "5", full, start, source.serverTime)[len(full)-1]
	report, err := service.ProcessClosedCandle(context.Background(), candle)
	if err != nil {
		t.Fatalf("ProcessClosedCandle() error = %v", err)
	}
	if report.SignalsInserted != 1 || len(repository.signals) != 1 ||
		repository.signals[0].Direction != "LONG" {
		t.Fatalf("expected one LONG signal from WebSocket candle: report=%+v signals=%+v", report, repository.signals)
	}
	if source.calls != 1 {
		t.Fatalf("contiguous WebSocket candle unexpectedly fetched REST history: calls=%d", source.calls)
	}

	replay, err := service.ProcessClosedCandle(context.Background(), candle)
	if err != nil {
		t.Fatalf("duplicate ProcessClosedCandle() error = %v", err)
	}
	if replay.SignalsInserted != 0 || len(repository.signals) != 1 {
		t.Fatalf("duplicate WebSocket event generated another signal: report=%+v signals=%+v", replay, repository.signals)
	}
}

func TestProcessClosedWebSocketCandleBackfillsMissedCandles(t *testing.T) {
	prefix := []float64{100, 99, 98, 99, 100, 101}
	full := []float64{100, 99, 98, 99, 100, 101, 100, 99, 98, 97, 98, 99, 100, 101}
	service, source, repository, start := newScannerForTest(t, prefix)
	if _, err := service.ScanOnce(context.Background()); err != nil {
		t.Fatalf("initial scan: %v", err)
	}

	source.candles[pairKey("BTCUSDT", "5")] = makeCandles("BTCUSDT", "5", full, start, source.serverTime.Add(40*time.Minute))
	source.serverTime = source.serverTime.Add(40 * time.Minute)
	last := makeCandles("BTCUSDT", "5", full, start, source.serverTime)[len(full)-1]

	report, err := service.ProcessClosedCandle(context.Background(), last)
	if err != nil {
		t.Fatalf("ProcessClosedCandle() gap recovery error = %v", err)
	}
	if source.calls != 2 {
		t.Fatalf("gap recovery should make one REST request, calls=%d", source.calls)
	}
	if report.SignalsInserted != 2 || len(repository.signals) != 2 {
		t.Fatalf("REST backfill did not recover both missed crossovers: report=%+v signals=%+v", report, repository.signals)
	}
	if repository.signals[0].Direction != "SHORT" || repository.signals[1].Direction != "LONG" {
		t.Fatalf("unexpected recovered signals: %+v", repository.signals)
	}
}

type profileFakeRepository struct {
	*fakeRepository
	workloads          []storage.ScannerWorkload
	states             map[string]storage.ScannerProfileState
	profilePairLookups int
	activeUserChecks   int
	hasUsers           bool
}

func newProfileFakeRepository(workloads []storage.ScannerWorkload) *profileFakeRepository {
	return &profileFakeRepository{
		fakeRepository: newFakeRepository(),
		workloads:      workloads,
		states:         make(map[string]storage.ScannerProfileState),
	}
}

func profileStateKey(symbol, timeframe string, rsiLength, emaLength int) string {
	return fmt.Sprintf("%s/%s/%d/%d", symbol, timeframe, rsiLength, emaLength)
}

func (f *profileFakeRepository) ListScannerWorkloads(context.Context) ([]storage.ScannerWorkload, error) {
	return append([]storage.ScannerWorkload(nil), f.workloads...), nil
}

func (f *profileFakeRepository) ListScannerProfilesForPair(_ context.Context, symbol, timeframe string) ([]storage.ScannerProfile, error) {
	f.profilePairLookups++
	profiles := make([]storage.ScannerProfile, 0)
	for _, workload := range f.workloads {
		if workload.Timeframe != timeframe {
			continue
		}
		for _, selected := range workload.Symbols {
			if selected == symbol {
				profiles = append(profiles, storage.ScannerProfile{RSILength: workload.RSILength, EMALength: workload.EMALength})
				break
			}
		}
	}
	return profiles, nil
}

func (f *profileFakeRepository) HasActiveScannerUsers(context.Context) (bool, error) {
	f.activeUserChecks++
	return f.hasUsers || len(f.workloads) > 0, nil
}

func (f *profileFakeRepository) GetScannerProfileState(_ context.Context, symbol, timeframe string, rsiLength, emaLength int) (storage.ScannerProfileState, error) {
	state, ok := f.states[profileStateKey(symbol, timeframe, rsiLength, emaLength)]
	if !ok {
		return storage.ScannerProfileState{}, storage.ErrNotFound
	}
	return state, nil
}

func (f *profileFakeRepository) InitializeScannerProfileState(_ context.Context, baseline storage.ScannerProfileState) (storage.ScannerProfileState, bool, error) {
	key := profileStateKey(baseline.Symbol, baseline.Timeframe, baseline.RSILength, baseline.EMALength)
	if current, exists := f.states[key]; exists {
		return current, false, nil
	}
	f.states[key] = baseline
	return baseline, true, nil
}

func (f *profileFakeRepository) CommitScannerProfileCandle(_ context.Context, state storage.ScannerProfileState, signal *storage.Signal) (int64, bool, int64, error) {
	key := profileStateKey(state.Symbol, state.Timeframe, state.RSILength, state.EMALength)
	if current, exists := f.states[key]; exists && !state.LastProcessedCandleStart.After(current.LastProcessedCandleStart) {
		return 0, false, 0, storage.ErrStaleCandle
	}
	f.states[key] = state
	f.commits++
	if signal == nil {
		return 0, false, 0, nil
	}
	f.signals = append(f.signals, *signal)
	f.deliveries++
	return int64(len(f.signals)), true, 1, nil
}

func TestPersonalizedScannerSharesMarketFetchButKeepsProfileCursors(t *testing.T) {
	closes := []float64{100, 101, 102, 103, 102, 101, 100, 99, 98, 99, 100, 101}
	start := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	serverTime := start.Add(time.Duration(len(closes)) * 5 * time.Minute)
	source := &fakeSource{
		serverTime: serverTime,
		candles: map[string][]bybit.Candle{
			pairKey("BTCUSDT", "5"): makeCandles("BTCUSDT", "5", closes, start, serverTime),
		},
	}
	repository := newProfileFakeRepository([]storage.ScannerWorkload{
		{RSILength: 3, EMALength: 2, Timeframe: "5", Symbols: []string{"BTCUSDT"}},
		{RSILength: 5, EMALength: 2, Timeframe: "5", Symbols: []string{"BTCUSDT"}},
	})
	repository.instruments = []storage.Instrument{{Symbol: "BTCUSDT"}}
	service, err := New(source, repository, Settings{
		Timeframes: []string{"5"}, RSILength: 3, EMALength: 2,
		InstrumentLimit: 10, HistoryCandles: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := service.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("personalized ScanOnce() error = %v", err)
	}
	if report.Instruments != 1 || report.PairsScanned != 1 || source.calls != 1 {
		t.Fatalf("expected one shared market fetch for two profiles: report=%+v calls=%d", report, source.calls)
	}
	if len(repository.states) != 2 {
		t.Fatalf("stored profile cursors = %d, want 2: %+v", len(repository.states), repository.states)
	}
	for _, profile := range []storage.ScannerProfile{{RSILength: 3, EMALength: 2}, {RSILength: 5, EMALength: 2}} {
		state, ok := repository.states[profileStateKey("BTCUSDT", "5", profile.RSILength, profile.EMALength)]
		if !ok || !state.Initialized || !state.LastProcessedCandleStart.Equal(start.Add(time.Duration(len(closes)-1)*5*time.Minute)) {
			t.Fatalf("profile cursor missing or incorrect for %+v: %+v", profile, state)
		}
	}
	if len(repository.signals) != 0 {
		t.Fatalf("initial personalized scan must establish baselines, not replay history: %+v", repository.signals)
	}
}

func TestPersonalizedScannerOnlyEmitsCrossForMatchingProfile(t *testing.T) {
	closes := []float64{
		100, 102, 100, 97, 99, 98, 101, 99, 101, 105,
		103, 105, 108, 104, 108, 112, 110, 108, 104, 105,
	}
	start := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	prefix := closes[:len(closes)-1]
	serverTime := start.Add(time.Duration(len(prefix)) * 5 * time.Minute)
	source := &fakeSource{
		serverTime: serverTime,
		candles: map[string][]bybit.Candle{
			pairKey("BTCUSDT", "5"): makeCandles("BTCUSDT", "5", prefix, start, serverTime),
		},
	}
	repository := newProfileFakeRepository([]storage.ScannerWorkload{
		{RSILength: 3, EMALength: 2, Timeframe: "5", Symbols: []string{"BTCUSDT"}},
		{RSILength: 8, EMALength: 3, Timeframe: "5", Symbols: []string{"BTCUSDT"}},
	})
	service, err := New(source, repository, Settings{
		Timeframes: []string{"5"}, RSILength: 3, EMALength: 2,
		InstrumentLimit: 10, HistoryCandles: 20,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	service.now = func() time.Time { return serverTime }

	baseline, err := service.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("initial personalized scan: %v", err)
	}
	if baseline.SignalsInserted != 0 || len(repository.signals) != 0 {
		t.Fatalf("initial scan must only establish profile baselines: report=%+v signals=%+v", baseline, repository.signals)
	}

	serverTime = start.Add(time.Duration(len(closes)) * 5 * time.Minute)
	source.serverTime = serverTime
	source.candles[pairKey("BTCUSDT", "5")] = makeCandles("BTCUSDT", "5", closes, start, serverTime)

	report, err := service.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("personalized crossover scan: %v", err)
	}
	if report.SignalsInserted != 1 || report.DeliveriesQueued != 1 || len(repository.signals) != 1 {
		t.Fatalf("expected exactly one profile-specific signal: report=%+v signals=%+v", report, repository.signals)
	}

	signal := repository.signals[0]
	if signal.Direction != "LONG" || signal.RSILength != 3 || signal.EMALength != 2 ||
		!signal.CandleStart.Equal(start.Add(time.Duration(len(closes)-1)*5*time.Minute)) {
		t.Fatalf("signal belongs to the wrong profile or candle: %+v", signal)
	}
	otherProfile := repository.states[profileStateKey("BTCUSDT", "5", 8, 3)]
	if !otherProfile.LastProcessedCandleStart.Equal(signal.CandleStart) {
		t.Fatalf("non-crossing profile cursor did not advance independently: %+v", otherProfile)
	}
}

func TestPersonalizedWebSocketCandleIsProfileAwareAndDeduplicated(t *testing.T) {
	closes := []float64{
		100, 102, 100, 97, 99, 98, 101, 99, 101, 105,
		103, 105, 108, 104, 108, 112, 110, 108, 104, 105,
	}
	start := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	prefix := closes[:len(closes)-1]
	serverTime := start.Add(time.Duration(len(prefix)) * 5 * time.Minute)
	source := &fakeSource{
		serverTime: serverTime,
		candles: map[string][]bybit.Candle{
			pairKey("BTCUSDT", "5"): makeCandles("BTCUSDT", "5", prefix, start, serverTime),
		},
	}
	repository := newProfileFakeRepository([]storage.ScannerWorkload{
		{RSILength: 3, EMALength: 2, Timeframe: "5", Symbols: []string{"BTCUSDT"}},
	})
	service, err := New(source, repository, Settings{
		Timeframes: []string{"5"}, RSILength: 3, EMALength: 2,
		InstrumentLimit: 10, HistoryCandles: 20,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := service.ScanOnce(context.Background()); err != nil {
		t.Fatalf("initial personalized baseline: %v", err)
	}
	serverTime = start.Add(time.Duration(len(closes)) * 5 * time.Minute)
	source.serverTime = serverTime
	source.candles[pairKey("BTCUSDT", "5")] = makeCandles("BTCUSDT", "5", closes, start, serverTime)
	lastCandle := source.candles[pairKey("BTCUSDT", "5")][len(closes)-1]

	report, err := service.ProcessClosedCandle(context.Background(), lastCandle)
	if err != nil {
		t.Fatalf("personalized WebSocket candle: %v", err)
	}
	if report.SignalsInserted != 1 || len(repository.signals) != 1 {
		t.Fatalf("expected one personalized WebSocket signal: report=%+v signals=%+v", report, repository.signals)
	}
	if repository.signals[0].RSILength != 3 || repository.signals[0].EMALength != 2 ||
		repository.signals[0].Direction != "LONG" {
		t.Fatalf("unexpected personalized WebSocket signal: %+v", repository.signals[0])
	}

	replay, err := service.ProcessClosedCandle(context.Background(), lastCandle)
	if err != nil {
		t.Fatalf("replayed personalized WebSocket candle: %v", err)
	}
	if replay.SignalsInserted != 0 || len(repository.signals) != 1 {
		t.Fatalf("replayed personalized WebSocket candle duplicated signal: report=%+v signals=%+v", replay, repository.signals)
	}
	if source.calls != 2 {
		t.Fatalf("expected one initial REST fetch and one WebSocket gap backfill, calls=%d", source.calls)
	}
	if repository.profilePairLookups != 0 || repository.activeUserChecks != 0 {
		t.Fatalf("WebSocket hot path queried profile routing in PostgreSQL: pair_lookups=%d active_user_checks=%d",
			repository.profilePairLookups, repository.activeUserChecks)
	}
}

func TestPersonalizedScannerRefreshesProfileRoutingCache(t *testing.T) {
	closes := []float64{100, 101, 102, 103, 102, 101, 100, 99, 98, 99, 100, 101}
	start := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	serverTime := start.Add(time.Duration(len(closes)) * 5 * time.Minute)
	source := &fakeSource{
		serverTime: serverTime,
		candles: map[string][]bybit.Candle{
			pairKey("BTCUSDT", "5"): makeCandles("BTCUSDT", "5", closes, start, serverTime),
		},
	}
	repository := newProfileFakeRepository([]storage.ScannerWorkload{
		{RSILength: 3, EMALength: 2, Timeframe: "5", Symbols: []string{"BTCUSDT"}},
	})
	service, err := New(source, repository, Settings{
		Timeframes: []string{"5"}, RSILength: 3, EMALength: 2,
		InstrumentLimit: 10, HistoryCandles: 20,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, err := service.ScanOnce(context.Background()); err != nil {
		t.Fatalf("first profile scan: %v", err)
	}
	key := historyKey("BTCUSDT", "5")
	if got := service.profilePairs[key]; len(got) != 1 || got[0] != (storage.ScannerProfile{RSILength: 3, EMALength: 2}) {
		t.Fatalf("initial cached profile routing = %+v", got)
	}

	updatedWorkloads := []storage.ScannerWorkload{
		{RSILength: 5, EMALength: 2, Timeframe: "5", Symbols: []string{"BTCUSDT"}},
	}
	if err := service.RefreshProfileRouting(updatedWorkloads, true); err != nil {
		t.Fatalf("minute-level profile routing refresh: %v", err)
	}
	if got := service.profilePairs[key]; len(got) != 1 || got[0] != (storage.ScannerProfile{RSILength: 5, EMALength: 2}) {
		t.Fatalf("profile routing cache was not refreshed: %+v", got)
	}
	if source.calls != 1 {
		t.Fatalf("profile routing refresh unexpectedly performed REST reconciliation: calls=%d", source.calls)
	}
	if service.hasActiveProfileUsers != true {
		t.Fatal("active profile user state was not retained in routing cache")
	}
	if repository.profilePairLookups != 0 {
		t.Fatalf("profile cache refresh unexpectedly used per-pair lookups: %d", repository.profilePairLookups)
	}
}

func TestPersonalizedWebSocketFailsClosedBeforeProfileCacheInitialization(t *testing.T) {
	start := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	serverTime := start.Add(5 * time.Minute)
	source := &fakeSource{serverTime: serverTime}
	repository := newProfileFakeRepository([]storage.ScannerWorkload{
		{RSILength: 3, EMALength: 2, Timeframe: "5", Symbols: []string{"BTCUSDT"}},
	})
	service, err := New(source, repository, Settings{
		Timeframes: []string{"5"}, RSILength: 3, EMALength: 2,
		InstrumentLimit: 10, HistoryCandles: 20,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	candle := makeCandles("BTCUSDT", "5", []float64{100}, start, serverTime)[0]
	if _, err := service.ProcessClosedCandle(context.Background(), candle); err == nil {
		t.Fatal("ProcessClosedCandle() must fail closed before personalized routing is initialized")
	}
	if len(repository.fakeRepository.states) != 0 || len(repository.signals) != 0 {
		t.Fatalf("uninitialized profile cache fell back to legacy processing: states=%+v signals=%+v",
			repository.fakeRepository.states, repository.signals)
	}
}
