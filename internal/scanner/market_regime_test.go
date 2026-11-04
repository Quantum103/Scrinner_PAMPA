package scanner

import (
	"context"
	"math"
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
	const count = 78
	closes := make([]float64, count)
	for index := range closes {
		closes[index] = 100 + 0.5*float64(index) + 0.8*math.Sin(float64(index)*0.6)
	}

	// A short pullback followed by a strong recovery creates a final LONG
	// RSI/EMA-RSI cross while the broader market remains in an established uptrend.
	closes[count-6] = closes[count-7] - 2
	closes[count-5] = closes[count-6] - 2
	closes[count-4] = closes[count-5] - 1.5
	closes[count-3] = closes[count-4] + 0.4
	closes[count-2] = closes[count-3] + 2
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
	prefix := closes[:len(closes)-3]
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
