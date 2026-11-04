package scanner

import (
	"testing"

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
