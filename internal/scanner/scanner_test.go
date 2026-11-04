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
