package strategy

import (
	"testing"
	"time"

	marketanalysis "github.com/Quantum103/Scrinner_PAMPA/internal/analysis"
)

func testStrategyConfig() Config {
	cfg := DefaultConfig()
	cfg.ObservationInterval = time.Second
	cfg.MaxObservationGap = 3 * time.Second
	cfg.SignalCooldown = 10 * time.Second
	return cfg
}

func readyMarket(now time.Time, cvd float64) marketanalysis.Snapshot {
	return marketanalysis.Snapshot{
		Symbol: "TESTUSDT", Status: marketanalysis.StatusReady,
		UpdatedAt:                now,
		RVOL:                     floatPtr(3.4),
		TakerBuyVolume:           floatPtr(90),
		TakerBuySharePercent:     floatPtr(72),
		Delta:                    floatPtr(44),
		CVD:                      floatPtr(cvd),
		LastPrice:                floatPtr(100.25),
		MomentumPercent:          map[string]float64{"5m0s": 0.7},
		SpreadBPS:                floatPtr(6),
		BidLiquidityQuote:        floatPtr(12000),
		AskLiquidityQuote:        floatPtr(14000),
		Absorption:               boolPtr(false),
		FlowTradeCount:           12,
		LargestTradeSharePercent: floatPtr(18),
	}
}

func floatPtr(value float64) *float64 { return &value }
func boolPtr(value bool) *bool        { return &value }

func newEngine(t *testing.T, cfg Config) *Engine {
	t.Helper()
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return engine
}

func eval(engine *Engine, at time.Time, market marketanalysis.Snapshot, usable bool, bidLevels, askLevels int) (Decision, *Signal) {
	return engine.Evaluate("TESTUSDT", market, usable, bidLevels, askLevels, at)
}

func TestQualifiedImpulseProducesLongAfterSequentialConfirmations(t *testing.T) {
	engine := newEngine(t, testStrategyConfig())
	start := time.Unix(1_800_000_000, 0).UTC()

	for i := 0; i < 2; i++ {
		at := start.Add(time.Duration(i) * time.Second)
		decision, signal := eval(engine, at, readyMarket(at, 100+float64(i)*10), true, 10, 10)
		if signal != nil {
			t.Fatalf("signal emitted after only %d observations", i+1)
		}
		if decision.State != DecisionConfirming {
			t.Fatalf("state after observation %d = %q, want confirming", i+1, decision.State)
		}
	}

	at := start.Add(2 * time.Second)
	decision, signal := eval(engine, at, readyMarket(at, 120), true, 10, 10)
	if signal == nil {
		t.Fatalf("third valid observation did not produce a signal: %+v", decision)
	}
	if signal.Direction != "LONG" || signal.Symbol != "TESTUSDT" || signal.Price != 100.25 {
		t.Fatalf("unexpected signal: %+v", signal)
	}
	if signal.RVOL != 3.4 || signal.TakerBuySharePercent != 72 || signal.Delta != 44 || signal.CVD != 120 || signal.SpreadBPS != 6 {
		t.Fatalf("signal does not preserve entry metrics: %+v", signal)
	}
	if signal.Time.IsZero() || signal.Reason == "" || len(decision.History) != 1 {
		t.Fatalf("signal timestamp/reason/history missing: %+v", decision)
	}
}

func TestSingleLargeTradeDoesNotTrigger(t *testing.T) {
	engine := newEngine(t, testStrategyConfig())
	start := time.Unix(1_800_000_000, 0).UTC()
	for i := 0; i < 5; i++ {
		at := start.Add(time.Duration(i) * time.Second)
		market := readyMarket(at, 100+float64(i)*10)
		market.LargestTradeSharePercent = floatPtr(61)
		decision, signal := eval(engine, at, market, true, 10, 10)
		if signal != nil || decision.ConfirmationCount != 0 {
			t.Fatalf("single large trade must block confirmation: %+v signal=%+v", decision, signal)
		}
	}
}

func TestBriefBuyerSpikeDoesNotSurviveConsecutiveConfirmation(t *testing.T) {
	engine := newEngine(t, testStrategyConfig())
	start := time.Unix(1_800_000_000, 0).UTC()
	sequence := []struct {
		buyShare float64
		cvd      float64
	}{
		{72, 100},
		{48, 110}, // Одно короткое наблюдение прерывает подтверждение.
		{72, 120},
		{72, 130},
	}
	for i, item := range sequence {
		at := start.Add(time.Duration(i) * time.Second)
		market := readyMarket(at, item.cvd)
		market.TakerBuySharePercent = floatPtr(item.buyShare)
		decision, signal := eval(engine, at, market, true, 10, 10)
		if signal != nil {
			t.Fatalf("brief spike unexpectedly produced signal: %+v", signal)
		}
		if i == 3 && decision.ConfirmationCount != 2 {
			t.Fatalf("confirmation count = %d, want 2 after interrupted sequence", decision.ConfirmationCount)
		}
	}
}

func TestAbsorptionBlocksLong(t *testing.T) {
	engine := newEngine(t, testStrategyConfig())
	start := time.Unix(1_800_000_000, 0).UTC()
	for i := 0; i < 5; i++ {
		at := start.Add(time.Duration(i) * time.Second)
		market := readyMarket(at, 100+float64(i)*10)
		market.Absorption = boolPtr(true)
		decision, signal := eval(engine, at, market, true, 10, 10)
		if signal != nil || decision.ConfirmationCount != 0 {
			t.Fatalf("absorption should block LONG: %+v signal=%+v", decision, signal)
		}
		if i == 0 && decision.Reason != "обнаружено поглощение покупок" {
			t.Fatalf("reason = %q, want absorption", decision.Reason)
		}
	}
}

func TestStaleOrIncompleteMarketDataBlocksSignal(t *testing.T) {
	engine := newEngine(t, testStrategyConfig())
	start := time.Unix(1_800_000_000, 0).UTC()
	for i := 0; i < 5; i++ {
		at := start.Add(time.Duration(i) * time.Second)
		market := readyMarket(at, 100+float64(i)*10)
		market.Status = marketanalysis.StatusStale
		market.Reason = "тест устаревших данных"
		decision, signal := eval(engine, at, market, false, 10, 10)
		if signal != nil || decision.ConfirmationCount != 0 {
			t.Fatalf("stale data must block signal: %+v signal=%+v", decision, signal)
		}
	}
}

func TestPriceAlreadyMovedBeyondLimitBlocksLong(t *testing.T) {
	engine := newEngine(t, testStrategyConfig())
	start := time.Unix(1_800_000_000, 0).UTC()
	for i := 0; i < 4; i++ {
		at := start.Add(time.Duration(i) * time.Second)
		market := readyMarket(at, 100+float64(i)*10)
		market.MomentumPercent["5m0s"] = 1.8
		decision, signal := eval(engine, at, market, true, 10, 10)
		if signal != nil || decision.ConfirmationCount != 0 {
			t.Fatalf("overextended price must block signal: %+v signal=%+v", decision, signal)
		}
	}
}

func TestInsufficientLiquiditySpreadOrBookDepthBlocksLong(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*marketanalysis.Snapshot)
		bids   int
		asks   int
	}{
		{name: "bid liquidity", mutate: func(m *marketanalysis.Snapshot) { m.BidLiquidityQuote = floatPtr(100) }, bids: 10, asks: 10},
		{name: "ask liquidity", mutate: func(m *marketanalysis.Snapshot) { m.AskLiquidityQuote = floatPtr(100) }, bids: 10, asks: 10},
		{name: "wide spread", mutate: func(m *marketanalysis.Snapshot) { m.SpreadBPS = floatPtr(30) }, bids: 10, asks: 10},
		{name: "shallow book", mutate: func(m *marketanalysis.Snapshot) {}, bids: 2, asks: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine := newEngine(t, testStrategyConfig())
			start := time.Unix(1_800_000_000, 0).UTC()
			for i := 0; i < 4; i++ {
				at := start.Add(time.Duration(i) * time.Second)
				market := readyMarket(at, 100+float64(i)*10)
				test.mutate(&market)
				decision, signal := eval(engine, at, market, true, test.bids, test.asks)
				if signal != nil || decision.ConfirmationCount != 0 {
					t.Fatalf("insufficient market quality should block signal: %+v signal=%+v", decision, signal)
				}
			}
		})
	}
}

func TestSignalIsLatchedWhileConditionsRemainContinuouslyTrue(t *testing.T) {
	engine := newEngine(t, testStrategyConfig())
	start := time.Unix(1_800_000_000, 0).UTC()
	var signalCount int
	for i := 0; i < 20; i++ {
		at := start.Add(time.Duration(i) * time.Second)
		_, signal := eval(engine, at, readyMarket(at, 100+float64(i)*10), true, 10, 10)
		if signal != nil {
			signalCount++
		}
	}
	if signalCount != 1 {
		t.Fatalf("signal count = %d, want exactly one while conditions remain true", signalCount)
	}
}

func TestCooldownAndRearmPreventDuplicateSignals(t *testing.T) {
	cfg := testStrategyConfig()
	engine := newEngine(t, cfg)
	start := time.Unix(1_800_000_000, 0).UTC()
	var signals int
	for i := 0; i <= 14; i++ {
		at := start.Add(time.Duration(i) * time.Second)
		market := readyMarket(at, 100+float64(i)*10)
		if i == 3 || i == 4 {
			market.TakerBuySharePercent = floatPtr(40) // Сбрасываем защёлку двумя неуспешными наблюдениями.
		}
		_, signal := eval(engine, at, market, true, 10, 10)
		if signal != nil {
			signals++
		}
	}
	if signals != 2 {
		t.Fatalf("number of signals = %d, want exactly 2 (initial + after cooldown/rearm)", signals)
	}
	decision := engine.Snapshot("TESTUSDT")
	if len(decision.History) != 2 {
		t.Fatalf("signal history = %d, want 2", len(decision.History))
	}
}

func TestNonIncreasingCVDResetsConfirmation(t *testing.T) {
	engine := newEngine(t, testStrategyConfig())
	start := time.Unix(1_800_000_000, 0).UTC()
	for i := 0; i < 5; i++ {
		at := start.Add(time.Duration(i) * time.Second)
		decision, signal := eval(engine, at, readyMarket(at, 100), true, 10, 10)
		if signal != nil || decision.ConfirmationCount > 1 {
			t.Fatalf("flat CVD must not accumulate confirmation: %+v signal=%+v", decision, signal)
		}
	}
}

func TestObservationGapRestartsConfirmation(t *testing.T) {
	engine := newEngine(t, testStrategyConfig())
	start := time.Unix(1_800_000_000, 0).UTC()
	for _, offset := range []time.Duration{0, time.Second} {
		at := start.Add(offset)
		_, signal := eval(engine, at, readyMarket(at, 100+float64(offset/time.Second)*10), true, 10, 10)
		if signal != nil {
			t.Fatalf("signal emitted before gap: %+v", signal)
		}
	}
	for index, offset := range []time.Duration{10 * time.Second, 11 * time.Second, 12 * time.Second} {
		at := start.Add(offset)
		decision, signal := eval(engine, at, readyMarket(at, 200+float64(index)*10), true, 10, 10)
		if index < 2 && signal != nil {
			t.Fatalf("signal emitted with only %d post-gap observations: %+v", index+1, signal)
		}
		if index == 0 && decision.ConfirmationCount != 1 {
			t.Fatalf("confirmation after gap = %d, want 1", decision.ConfirmationCount)
		}
		if index == 2 && signal == nil {
			t.Fatalf("signal was not emitted after three consecutive post-gap observations: %+v", decision)
		}
	}
}

func TestEngineRequiresCVDAndRejectsMissingMetrics(t *testing.T) {
	engine := newEngine(t, testStrategyConfig())
	at := time.Unix(1_800_000_000, 0).UTC()
	market := readyMarket(at, 100)
	market.CVD = nil
	decision, signal := eval(engine, at, market, true, 10, 10)
	if signal != nil || decision.State != DecisionWaiting {
		t.Fatalf("missing CVD must block signal without panic: %+v signal=%+v", decision, signal)
	}
}
