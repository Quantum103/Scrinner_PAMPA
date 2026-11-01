package analysis

import (
	"math"
	"testing"
	"time"
)

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.RVOLInterval = 10 * time.Second
	cfg.RVOLLookbackIntervals = 2
	cfg.FlowWindow = 10 * time.Second
	cfg.CVDWindow = 20 * time.Second
	cfg.MomentumWindows = []time.Duration{5 * time.Second}
	cfg.VolumeAccelerationWindow = 5 * time.Second
	cfg.OrderBookLevels = 2
	return cfg
}

func fixedNow() time.Time {
	return time.Unix(1_700_000_005, 0).UTC()
}

func newTestAnalyzer(t *testing.T, cfg Config, startedAt, now time.Time) *Analyzer {
	t.Helper()
	analyzer, err := New(cfg, 5*time.Second)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	analyzer.StartSession("BTCUSDT", startedAt, false)
	return analyzer
}

func addTrade(t *testing.T, analyzer *Analyzer, at time.Time, side string, quantity, price float64) {
	t.Helper()
	if err := analyzer.AddTrade("BTCUSDT", Trade{
		Time: at, ReceivedAt: at, Side: side, Quantity: quantity, Price: price,
	}); err != nil {
		t.Fatalf("AddTrade(%s, %s) error = %v", side, at, err)
	}
}

func freshBook(now time.Time) Book {
	return Book{
		Valid: true, Fresh: true, ReceivedAt: now,
		BestBid: 100, BestAsk: 100.02,
		Bids: []Level{{Price: 100, Size: 2}, {Price: 99.99, Size: 3}},
		Asks: []Level{{Price: 100.02, Size: 4}, {Price: 100.03, Size: 5}},
	}
}

func seedIntervals(t *testing.T, analyzer *Analyzer, now time.Time) {
	t.Helper()
	// Два завершённых RVOL-интервала: по 10 единиц объёма каждый.
	addTrade(t, analyzer, now.Add(-20*time.Second), "Buy", 10, 100)
	addTrade(t, analyzer, now.Add(-10*time.Second), "Sell", 10, 100)
	// Цена для расчёта momentum на границе окна.
	addTrade(t, analyzer, now.Add(-5*time.Second), "Buy", 1, 100)
}

func TestBuyerDominanceAndPositiveDelta(t *testing.T) {
	cfg := testConfig()
	now := fixedNow()
	analyzer := newTestAnalyzer(t, cfg, now.Add(-time.Minute), now)
	seedIntervals(t, analyzer, now)
	addTrade(t, analyzer, now.Add(-4*time.Second), "Buy", 10, 100.01)
	addTrade(t, analyzer, now.Add(-2*time.Second), "Sell", 2, 100.01)
	addTrade(t, analyzer, now.Add(-time.Second), "Buy", 5, 100.01)

	got := analyzer.Snapshot("BTCUSDT", now, true, freshBook(now))
	if got.TakerBuyVolume == nil || got.TakerSellVolume == nil || got.Delta == nil {
		t.Fatalf("taker metrics are missing: %+v", got)
	}
	if *got.TakerBuySharePercent <= 50 || *got.TakerSellSharePercent >= 50 {
		t.Fatalf("buyer dominance was not detected: buy=%v sell=%v", *got.TakerBuySharePercent, *got.TakerSellSharePercent)
	}
	if *got.Delta <= 0 {
		t.Fatalf("delta = %v, want positive", *got.Delta)
	}
}

func TestSellerDominance(t *testing.T) {
	cfg := testConfig()
	now := fixedNow()
	analyzer := newTestAnalyzer(t, cfg, now.Add(-time.Minute), now)
	seedIntervals(t, analyzer, now)
	addTrade(t, analyzer, now.Add(-4*time.Second), "Sell", 12, 99.99)
	addTrade(t, analyzer, now.Add(-2*time.Second), "Buy", 2, 99.99)
	addTrade(t, analyzer, now.Add(-time.Second), "Sell", 4, 99.98)

	got := analyzer.Snapshot("BTCUSDT", now, true, freshBook(now))
	if got.TakerBuySharePercent == nil || *got.TakerBuySharePercent >= 50 {
		t.Fatalf("seller dominance was not detected: %+v", got)
	}
}

func TestNegativeDelta(t *testing.T) {
	cfg := testConfig()
	now := fixedNow()
	analyzer := newTestAnalyzer(t, cfg, now.Add(-time.Minute), now)
	seedIntervals(t, analyzer, now)
	addTrade(t, analyzer, now.Add(-4*time.Second), "Buy", 1, 100)
	addTrade(t, analyzer, now.Add(-2*time.Second), "Sell", 8, 99.99)
	addTrade(t, analyzer, now.Add(-time.Second), "Sell", 3, 99.98)

	got := analyzer.Snapshot("BTCUSDT", now, true, freshBook(now))
	if got.Delta == nil || *got.Delta >= 0 {
		t.Fatalf("delta = %v, want negative", got.Delta)
	}
}

func TestCVDGrowth(t *testing.T) {
	cfg := testConfig()
	now := fixedNow()
	analyzer := newTestAnalyzer(t, cfg, now.Add(-time.Minute), now)
	seedIntervals(t, analyzer, now)
	addTrade(t, analyzer, now.Add(-4*time.Second), "Buy", 7, 100.01)
	addTrade(t, analyzer, now.Add(-2*time.Second), "Buy", 5, 100.02)
	addTrade(t, analyzer, now.Add(-time.Second), "Sell", 1, 100.02)

	got := analyzer.Snapshot("BTCUSDT", now, true, freshBook(now))
	if got.CVD == nil || *got.CVD <= 0 {
		t.Fatalf("CVD = %v, want positive cumulative delta", got.CVD)
	}
}

func TestRVOLSpikeUsesCompletedIntervalsOnly(t *testing.T) {
	cfg := testConfig()
	now := fixedNow()
	analyzer := newTestAnalyzer(t, cfg, now.Add(-time.Minute), now)
	// Два завершённых интервала: 10 + 10. Текущий интервал: 40.
	addTrade(t, analyzer, now.Add(-24*time.Second), "Buy", 10, 100)
	addTrade(t, analyzer, now.Add(-14*time.Second), "Buy", 10, 100)
	addTrade(t, analyzer, now.Add(-4*time.Second), "Buy", 39, 100.01)
	addTrade(t, analyzer, now.Add(-time.Second), "Sell", 1, 100.01)

	got := analyzer.Snapshot("BTCUSDT", now, true, freshBook(now))
	if got.RVOL == nil || math.Abs(*got.RVOL-4) > 0.000001 {
		t.Fatalf("RVOL = %v, want 4.0", got.RVOL)
	}
	if got.RVOLAverageVolume == nil || *got.RVOLAverageVolume != 10 {
		t.Fatalf("RVOL baseline = %v, want 10", got.RVOLAverageVolume)
	}
}

func TestBuyAbsorption(t *testing.T) {
	cfg := testConfig()
	cfg.AbsorptionMinBuyShare = 0.7
	cfg.AbsorptionBuyVolumeMultiplier = 2
	cfg.AbsorptionMaxPriceChangePct = 0.05
	now := fixedNow()
	analyzer := newTestAnalyzer(t, cfg, now.Add(-time.Minute), now)
	// В предыдущем flow-окне покупательский объём равен 5.
	addTrade(t, analyzer, now.Add(-20*time.Second), "Buy", 5, 100)
	addTrade(t, analyzer, now.Add(-10*time.Second), "Buy", 5, 100)
	addTrade(t, analyzer, now.Add(-5*time.Second), "Buy", 1, 100)
	// В текущем окне покупки резко выросли, но цена почти не сдвинулась.
	addTrade(t, analyzer, now.Add(-4*time.Second), "Buy", 12, 100.01)
	addTrade(t, analyzer, now.Add(-2*time.Second), "Buy", 8, 100.01)
	addTrade(t, analyzer, now.Add(-time.Second), "Sell", 1, 100.01)

	got := analyzer.Snapshot("BTCUSDT", now, true, freshBook(now))
	if got.Absorption == nil || !*got.Absorption {
		t.Fatalf("absorption = %v, want true", got.Absorption)
	}
}

func TestInsufficientDataDoesNotInventMetrics(t *testing.T) {
	cfg := testConfig()
	now := fixedNow()
	analyzer := newTestAnalyzer(t, cfg, now.Add(0), now)
	addTrade(t, analyzer, now.Add(-time.Second), "Buy", 3, 100)

	got := analyzer.Snapshot("BTCUSDT", now, true, freshBook(now))
	if got.Status != StatusInsufficientData {
		t.Fatalf("status = %q, want %q", got.Status, StatusInsufficientData)
	}
	if got.RVOL != nil || got.TakerBuyVolume != nil || got.CVD != nil || got.VolumeAccelerationPercent != nil {
		t.Fatalf("insufficient data produced metrics: %+v", got)
	}
}

func TestStaleAndIncompleteStatuses(t *testing.T) {
	cfg := testConfig()
	now := fixedNow()
	analyzer := newTestAnalyzer(t, cfg, now.Add(-time.Minute), now)
	addTrade(t, analyzer, now.Add(-time.Second), "Buy", 1, 100)

	stale := analyzer.Snapshot("BTCUSDT", now.Add(10*time.Second), true, freshBook(now))
	if stale.Status != StatusStale {
		t.Fatalf("stale status = %q, want %q", stale.Status, StatusStale)
	}

	analyzer.MarkGap("BTCUSDT", now, "тестовый разрыв")
	incomplete := analyzer.Snapshot("BTCUSDT", now, true, freshBook(now))
	if incomplete.Status != StatusIncomplete {
		t.Fatalf("incomplete status = %q, want %q", incomplete.Status, StatusIncomplete)
	}
}

func TestOrderBookSpreadAndLiquidity(t *testing.T) {
	cfg := testConfig()
	now := fixedNow()
	analyzer := newTestAnalyzer(t, cfg, now.Add(-time.Minute), now)
	seedIntervals(t, analyzer, now)
	addTrade(t, analyzer, now.Add(-4*time.Second), "Buy", 1, 100)
	addTrade(t, analyzer, now.Add(-time.Second), "Buy", 1, 100)

	got := analyzer.Snapshot("BTCUSDT", now, true, freshBook(now))
	if got.SpreadBPS == nil || *got.SpreadBPS <= 0 {
		t.Fatalf("spread = %v, want positive", got.SpreadBPS)
	}
	if got.BidLiquidityQuote == nil || math.Abs(*got.BidLiquidityQuote-499.97) > 0.000001 {
		t.Fatalf("bid liquidity = %v, want 499.97", got.BidLiquidityQuote)
	}
	if got.AskLiquidityQuote == nil || *got.AskLiquidityQuote <= 0 {
		t.Fatalf("ask liquidity = %v, want positive", got.AskLiquidityQuote)
	}
}

func TestMomentumAndVolumeAcceleration(t *testing.T) {
	cfg := testConfig()
	now := fixedNow()
	analyzer := newTestAnalyzer(t, cfg, now.Add(-time.Minute), now)
	addTrade(t, analyzer, now.Add(-8*time.Second), "Buy", 5, 100)
	addTrade(t, analyzer, now.Add(-6*time.Second), "Sell", 5, 100)
	addTrade(t, analyzer, now.Add(-5*time.Second), "Buy", 1, 100)
	addTrade(t, analyzer, now.Add(-4*time.Second), "Buy", 15, 100.5)
	addTrade(t, analyzer, now.Add(-time.Second), "Buy", 15, 101)

	got := analyzer.Snapshot("BTCUSDT", now, true, freshBook(now))
	momentum, ok := got.MomentumPercent["5s"]
	if !ok || math.Abs(momentum-1) > 0.000001 {
		t.Fatalf("5s momentum = %v, want 1%%", got.MomentumPercent)
	}
	if got.VolumeAccelerationPercent == nil || math.Abs(*got.VolumeAccelerationPercent-210) > 0.000001 {
		t.Fatalf("volume acceleration = %v, want 210%%", got.VolumeAccelerationPercent)
	}
}


func TestFlowTradeCountAndLargestTradeConcentration(t *testing.T) {
	cfg := testConfig()
	now := fixedNow()
	analyzer := newTestAnalyzer(t, cfg, now.Add(-time.Minute), now)
	seedIntervals(t, analyzer, now)
	addTrade(t, analyzer, now.Add(-4*time.Second), "Buy", 10, 100.01)
	addTrade(t, analyzer, now.Add(-2*time.Second), "Sell", 2, 100.01)
	addTrade(t, analyzer, now.Add(-time.Second), "Buy", 5, 100.01)

	got := analyzer.Snapshot("BTCUSDT", now, true, freshBook(now))
	if got.FlowTradeCount != 4 {
		t.Fatalf("flow trade count = %d, want 4", got.FlowTradeCount)
	}
	if got.LargestTradeSharePercent == nil {
		t.Fatal("largest trade concentration is missing")
	}
	want := 10.0 / 18.0 * 100
	if math.Abs(*got.LargestTradeSharePercent-want) > 0.000001 {
		t.Fatalf("largest trade share = %.6f, want %.6f", *got.LargestTradeSharePercent, want)
	}
	if got.LastPrice == nil || *got.LastPrice != 100.01 {
		t.Fatalf("last price = %v, want 100.01", got.LastPrice)
	}
	if got.BidBookLevels != 2 || got.AskBookLevels != 2 {
		t.Fatalf("book levels = bid:%d ask:%d, want 2/2", got.BidBookLevels, got.AskBookLevels)
	}
}
