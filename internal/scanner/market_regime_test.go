package scanner

import (
	"context"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"

	"github.com/Quantum103/Scrinner_PAMPA/internal/indicators"
	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
)

func TestMinimumMarketHistoryIncludesEMAAndConfirmationWarmup(t *testing.T) {
	if got := minimumMarketHistory(20); got != 52 {
		t.Fatalf("minimumMarketHistory(20) = %d, want 52", got)
	}
	if got := minimumMarketHistory(300); got != 300 {
		t.Fatalf("minimumMarketHistory(300) = %d, want 300", got)
	}
	if got := minimumMarketHistory(1000); got != 1000 {
		t.Fatalf("minimumMarketHistory(1000) = %d, want 1000", got)
	}
}

func TestMarketSnapshotRequiresTwoOfThreeSidewaysCandles(t *testing.T) {
	points := []indicators.MarketPoint{
		{ADX: 18, ADXReady: true, CHOP: 65, CHOPReady: true, EMA: 100, EMAReady: true},
		{ADX: 19, ADXReady: true, CHOP: 63, CHOPReady: true, EMA: 100.2, EMAReady: true},
		{ADX: 24, ADXReady: true, CHOP: 55, CHOPReady: true, EMA: 100.4, EMAReady: true},
	}

	snapshot := marketSnapshotAt(points, 2)
	if snapshot.regime != marketRegimeSideways {
		t.Fatalf("regime = %s, want %s", snapshot.regime, marketRegimeSideways)
	}
	if snapshot.adx != 24 || !snapshot.adxReady ||
		snapshot.chop != 55 || !snapshot.chopReady ||
		snapshot.ema != 100.4 || !snapshot.emaReady {
		t.Fatalf("snapshot did not retain latest-candle indicator values: %+v", snapshot)
	}

	points[1].CHOP = 61.8 // The threshold is strict: equality is not a sideways candle.
	snapshot = marketSnapshotAt(points, 2)
	if snapshot.regime != marketRegimeTransitional {
		t.Fatalf("one qualifying sideways candle classified as %s, want TRANSITIONAL", snapshot.regime)
	}
}

func TestMarketSnapshotUsesConfiguredThresholds(t *testing.T) {
	points := []indicators.MarketPoint{
		{ADX: 21, ADXReady: true, CHOP: 58, CHOPReady: true, EMA: 100, EMAReady: true},
		{ADX: 22, ADXReady: true, CHOP: 60, CHOPReady: true, EMA: 101, EMAReady: true},
		{ADX: 26, ADXReady: true, CHOP: 50, CHOPReady: true, EMA: 102, EMAReady: true},
	}
	if got := marketSnapshotAt(points, 2).regime; got != marketRegimeTransitional {
		t.Fatalf("default thresholds classified market as %s, want %s", got, marketRegimeTransitional)
	}
	custom := marketRegimeThresholds{
		adxSidewaysMax:  23,
		chopSidewaysMin: 55,
		adxTrendMin:     30,
	}
	if got := marketSnapshotAtWithThresholds(points, 2, custom).regime; got != marketRegimeSideways {
		t.Fatalf("custom thresholds classified market as %s, want %s", got, marketRegimeSideways)
	}
}

func TestMarketSnapshotClassifiesUptrendAndDowntrend(t *testing.T) {
	tests := []struct {
		name   string
		points []indicators.MarketPoint
		want   string
	}{
		{
			name: "uptrend",
			points: []indicators.MarketPoint{
				{ADX: 28, ADXReady: true, EMA: 100, EMAReady: true},
				{ADX: 30, ADXReady: true, EMA: 101, EMAReady: true},
				{ADX: 32, ADXReady: true, EMA: 102, EMAReady: true},
				{ADX: 31, ADXReady: true, EMA: 103, EMAReady: true},
			},
			want: marketRegimeUptrend,
		},
		{
			name: "downtrend",
			points: []indicators.MarketPoint{
				{ADX: 28, ADXReady: true, EMA: 104, EMAReady: true},
				{ADX: 30, ADXReady: true, EMA: 103, EMAReady: true},
				{ADX: 32, ADXReady: true, EMA: 102, EMAReady: true},
				{ADX: 31, ADXReady: true, EMA: 101, EMAReady: true},
			},
			want: marketRegimeDowntrend,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := marketSnapshotAt(test.points, len(test.points)-1)
			if snapshot.regime != test.want {
				t.Fatalf("regime = %s, want %s", snapshot.regime, test.want)
			}
		})
	}
}

func TestMarketSnapshotReturnsTransitionalWhenConfirmationIsInsufficient(t *testing.T) {
	points := []indicators.MarketPoint{
		{ADX: 29, ADXReady: true, EMA: 100, EMAReady: true},
		{ADX: 29, ADXReady: true, EMA: 101, EMAReady: true},
		{ADX: 17, ADXReady: true, CHOP: 60, CHOPReady: true, EMA: 100, EMAReady: true},
	}
	snapshot := marketSnapshotAt(points, 2)
	if snapshot.regime != marketRegimeTransitional {
		t.Fatalf("regime = %s, want %s", snapshot.regime, marketRegimeTransitional)
	}
}

func TestMarketSnapshotIsUnknownUntilIndicatorsAreReady(t *testing.T) {
	points := []indicators.MarketPoint{
		{},
		{},
		{},
	}
	if got := marketSnapshotAt(points, 2).regime; got != marketRegimeUnknown {
		t.Fatalf("regime = %s, want %s", got, marketRegimeUnknown)
	}
}

func TestApplyMarketContextCopiesSnapshotIntoSignal(t *testing.T) {
	points := []indicators.MarketPoint{
		{ADX: 18, ADXReady: true, CHOP: 65, CHOPReady: true, EMA: 100, EMAReady: true},
		{ADX: 19, ADXReady: true, CHOP: 66, CHOPReady: true, EMA: 100.5, EMAReady: true},
		{ADX: 21, ADXReady: true, CHOP: 58, CHOPReady: true, EMA: 101, EMAReady: true},
	}
	signal := &storage.Signal{Symbol: "BTCUSDT", Direction: "LONG"}
	applyMarketContext(signal, points, 2)

	if signal.MarketRegime != marketRegimeSideways ||
		signal.MarketADX != 21 || !signal.MarketADXReady ||
		signal.MarketCHOP != 58 || !signal.MarketCHOPReady ||
		signal.MarketEMA != 101 || !signal.MarketEMAReady {
		t.Fatalf("signal market context = %+v", signal)
	}
}

func regimeCrossoverCloses() []float64 {
	const count = 53
	closes := make([]float64, count)
	for index := range closes {
		closes[index] = 100 + float64(index)
	}

	// Keep the long-term market trend intact, pull RSI below its EMA RSI,
	// then create a LONG cross on the first candle after the 52-candle warm-up.
	closes[count-3] = closes[count-4] - 2
	closes[count-2] = closes[count-3] - 1.5
	closes[count-1] = closes[count-2] + 3
	return closes
}

func assertSignalHasUptrendContext(t *testing.T, signal storage.Signal) {
	t.Helper()
	if signal.Direction != "LONG" {
		t.Fatalf("signal direction = %s, want LONG", signal.Direction)
	}
	if signal.MarketRegime != marketRegimeUptrend {
		t.Fatalf("market regime = %s, want %s (signal=%+v)", signal.MarketRegime, marketRegimeUptrend, signal)
	}
	if !signal.MarketADXReady || signal.MarketADX <= marketADXTrendMin {
		t.Fatalf("ADX snapshot is not ready or too weak: %+v", signal)
	}
	if !signal.MarketCHOPReady {
		t.Fatalf("CHOP snapshot should be ready: %+v", signal)
	}
	if !signal.MarketEMAReady || signal.MarketEMA <= 0 {
		t.Fatalf("EMA snapshot is not ready: %+v", signal)
	}
}

func TestLegacyScannerAttachesMarketContextToRSISignal(t *testing.T) {
	closes := regimeCrossoverCloses()
	prefix := closes[:len(closes)-1]
	service, source, repository, start := newScannerForTest(t, prefix)

	if _, err := service.ScanOnce(context.Background()); err != nil {
		t.Fatalf("initial baseline scan: %v", err)
	}

	serverTime := start.Add(time.Duration(len(closes)) * 5 * time.Minute)
	source.serverTime = serverTime
	source.candles[pairKey("BTCUSDT", "5")] = makeCandles("BTCUSDT", "5", closes, start, serverTime)
	service.now = func() time.Time { return serverTime }

	report, err := service.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("scan new closed candle: %v", err)
	}
	if report.SignalsInserted != 1 || len(repository.signals) != 1 {
		t.Fatalf("expected one signal: report=%+v signals=%+v", report, repository.signals)
	}
	assertSignalHasUptrendContext(t, repository.signals[0])
}

func TestPersonalizedScannerAttachesSharedMarketContextToRSISignal(t *testing.T) {
	closes := regimeCrossoverCloses()
	prefix := closes[:len(closes)-1]
	start := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
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
	service.now = func() time.Time { return serverTime }

	if _, err := service.ScanOnce(context.Background()); err != nil {
		t.Fatalf("initial personalized baseline scan: %v", err)
	}

	serverTime = start.Add(time.Duration(len(closes)) * 5 * time.Minute)
	source.serverTime = serverTime
	source.candles[pairKey("BTCUSDT", "5")] = makeCandles("BTCUSDT", "5", closes, start, serverTime)

	report, err := service.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("personalized scan new closed candle: %v", err)
	}
	if report.SignalsInserted != 1 || len(repository.signals) != 1 {
		t.Fatalf("expected one personalized signal: report=%+v signals=%+v", report, repository.signals)
	}
	assertSignalHasUptrendContext(t, repository.signals[0])
}

func makeSidewaysCrossoverCandles(length int, start time.Time) []bybit.Candle {
	closes := make([]float64, length)
	for index := range closes {
		closes[index] = 100.2
		if index%2 == 1 {
			closes[index] = 99.8
		}
	}

	candles := makeCandles("BTCUSDT", "5", closes, start, start.Add(time.Duration(length)*5*time.Minute))
	for index := range candles {
		candles[index].High = closes[index] + 0.5
		candles[index].Low = closes[index] - 0.5
	}
	return candles
}

func TestMarketRegimeAllowsSignalsOnlyWhenNotConfirmedSideways(t *testing.T) {
	tests := []struct {
		regime string
		want   bool
	}{
		{regime: marketRegimeSideways, want: false},
		{regime: "sideways", want: false},
		{regime: marketRegimeUptrend, want: true},
		{regime: marketRegimeDowntrend, want: true},
		{regime: marketRegimeTransitional, want: true},
		{regime: marketRegimeUnknown, want: true},
		{regime: "", want: true},
	}
	for _, test := range tests {
		if got := marketRegimeAllowsSignal(test.regime); got != test.want {
			t.Errorf("marketRegimeAllowsSignal(%q) = %t, want %t", test.regime, got, test.want)
		}
	}
}

func TestLegacyScannerFiltersSidewaysCrossAndAdvancesCursor(t *testing.T) {
	start := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	prefix := makeSidewaysCrossoverCandles(52, start)
	serverTime := start.Add(52 * 5 * time.Minute)
	service, source, repository, _ := newScannerForTest(t, make([]float64, 52))
	source.serverTime = serverTime
	source.candles[pairKey("BTCUSDT", "5")] = prefix

	if _, err := service.ScanOnce(context.Background()); err != nil {
		t.Fatalf("baseline scan: %v", err)
	}

	full := makeSidewaysCrossoverCandles(53, start)
	serverTime = start.Add(53 * 5 * time.Minute)
	source.serverTime = serverTime
	source.candles[pairKey("BTCUSDT", "5")] = full
	service.now = func() time.Time { return serverTime }

	report, err := service.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("sideways crossover scan: %v", err)
	}
	if report.SignalsInserted != 0 || report.SidewaysSignalsFiltered != 1 ||
		report.DeliveriesQueued != 0 || len(repository.signals) != 0 {
		t.Fatalf("sideways RSI cross was not reported as filtered: report=%+v signals=%+v", report, repository.signals)
	}
	if report.CandlesProcessed != 1 {
		t.Fatalf("processed candles = %d, want 1", report.CandlesProcessed)
	}
	state := repository.states[pairKey("BTCUSDT", "5")]
	if !state.LastProcessedCandleStart.Equal(full[len(full)-1].Start) {
		t.Fatalf("cursor did not advance past filtered sideways candle: %+v", state)
	}
	if state.LastCrossDirection != "LONG" || state.LastCrossAt == nil {
		t.Fatalf("filtered crossover was not recorded in scanner cursor: %+v", state)
	}

	replay, err := service.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("replay filtered candle: %v", err)
	}
	if replay.CandlesProcessed != 0 || replay.SignalsInserted != 0 ||
		replay.SidewaysSignalsFiltered != 0 || len(repository.signals) != 0 {
		t.Fatalf("filtered candle was reprocessed: report=%+v signals=%+v", replay, repository.signals)
	}
}

func TestPersonalizedScannerFiltersSidewaysCrossAndAdvancesCursor(t *testing.T) {
	start := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	prefix := makeSidewaysCrossoverCandles(52, start)
	serverTime := start.Add(52 * 5 * time.Minute)
	source := &fakeSource{
		serverTime: serverTime,
		candles:    map[string][]bybit.Candle{pairKey("BTCUSDT", "5"): prefix},
	}
	repository := newProfileFakeRepository([]storage.ScannerWorkload{
		{RSILength: 3, EMALength: 2, Timeframe: "5", Symbols: []string{"BTCUSDT"}},
	})
	service, err := New(source, repository, Settings{
		Timeframes: []string{"5"}, RSILength: 3, EMALength: 2,
		InstrumentLimit: 10, HistoryCandles: 20,
	})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	service.now = func() time.Time { return serverTime }

	if _, err := service.ScanOnce(context.Background()); err != nil {
		t.Fatalf("baseline personalized scan: %v", err)
	}

	full := makeSidewaysCrossoverCandles(53, start)
	serverTime = start.Add(53 * 5 * time.Minute)
	source.serverTime = serverTime
	source.candles[pairKey("BTCUSDT", "5")] = full

	report, err := service.ScanOnce(context.Background())
	if err != nil {
		t.Fatalf("personalized sideways crossover scan: %v", err)
	}
	if report.SignalsInserted != 0 || report.SidewaysSignalsFiltered != 1 ||
		report.DeliveriesQueued != 0 || len(repository.signals) != 0 {
		t.Fatalf("personalized sideways RSI cross was not reported as filtered: report=%+v signals=%+v", report, repository.signals)
	}
	state := repository.states[profileStateKey("BTCUSDT", "5", 3, 2)]
	if !state.LastProcessedCandleStart.Equal(full[len(full)-1].Start) {
		t.Fatalf("personalized cursor did not advance past filtered sideways candle: %+v", state)
	}
	if state.LastCrossDirection != "LONG" || state.LastCrossAt == nil {
		t.Fatalf("personalized filtered crossover was not recorded in cursor: %+v", state)
	}
}

func TestPersonalizedWebSocketFiltersSidewaysCrossAndReportsIt(t *testing.T) {
	start := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	prefix := makeSidewaysCrossoverCandles(52, start)
	serverTime := prefix[len(prefix)-1].Close
	source := &fakeSource{
		serverTime: serverTime,
		candles:    map[string][]bybit.Candle{pairKey("BTCUSDT", "5"): prefix},
	}
	repository := newProfileFakeRepository([]storage.ScannerWorkload{
		{RSILength: 3, EMALength: 2, Timeframe: "5", Symbols: []string{"BTCUSDT"}},
	})
	service, err := New(source, repository, Settings{
		Timeframes: []string{"5"}, RSILength: 3, EMALength: 2,
		InstrumentLimit: 10, HistoryCandles: 20,
	})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	service.now = func() time.Time { return serverTime }

	if _, err := service.ScanOnce(context.Background()); err != nil {
		t.Fatalf("baseline personalized scan: %v", err)
	}

	full := makeSidewaysCrossoverCandles(53, start)
	lastCandle := full[len(full)-1]
	source.serverTime = lastCandle.Close
	source.candles[pairKey("BTCUSDT", "5")] = full

	report, err := service.ProcessClosedCandle(context.Background(), lastCandle)
	if err != nil {
		t.Fatalf("personalized WebSocket sideways crossover: %v", err)
	}
	if report.SidewaysSignalsFiltered != 1 || report.SignalsInserted != 0 ||
		report.DeliveriesQueued != 0 || len(repository.signals) != 0 {
		t.Fatalf("personalized WebSocket sideways cross was not reported as filtered: report=%+v signals=%+v",
			report, repository.signals)
	}
	state := repository.states[profileStateKey("BTCUSDT", "5", 3, 2)]
	if !state.LastProcessedCandleStart.Equal(lastCandle.Start) {
		t.Fatalf("personalized WebSocket cursor did not advance: %+v", state)
	}
	if state.LastCrossDirection != "LONG" || state.LastCrossAt == nil {
		t.Fatalf("personalized WebSocket filtered cross was not recorded in cursor: %+v", state)
	}

	replay, err := service.ProcessClosedCandle(context.Background(), lastCandle)
	if err != nil {
		t.Fatalf("replay personalized WebSocket sideways crossover: %v", err)
	}
	if replay.SidewaysSignalsFiltered != 0 || replay.SignalsInserted != 0 {
		t.Fatalf("personalized WebSocket replay counted the filtered cross again: %+v", replay)
	}
}
