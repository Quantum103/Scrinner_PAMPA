package momentum

import (
	"math"
	"testing"
	"time"
)

func newTestTracker(t *testing.T) *Tracker {
	t.Helper()
	tracker, err := NewTracker(15*time.Minute, 5, 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return tracker
}

func TestTrackerSignalsImmediatelyAboveFivePercent(t *testing.T) {
	tracker := newTestTracker(t)
	start := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	if err := tracker.Seed("abcusdt", []PricePoint{
		{At: start, Price: 100},
		{At: start.Add(15 * time.Minute), Price: 104.9},
	}); err != nil {
		t.Fatal(err)
	}

	signal, err := tracker.Add("ABCUSDT", PricePoint{
		At: start.Add(15*time.Minute + time.Second), Price: 105.01,
	})
	if err != nil {
		t.Fatal(err)
	}
	if signal == nil || signal.ChangePercent <= 5 || signal.Price != 105.01 || signal.ReferencePrice != 100 {
		t.Fatalf("expected an immediate >5%% signal, got %+v", signal)
	}
	if !signal.At.Equal(start.Add(15*time.Minute + time.Second)) {
		t.Fatalf("signal timestamp = %s", signal.At)
	}
}

func TestTrackerRequiresStrictlyMoreThanThreshold(t *testing.T) {
	tracker := newTestTracker(t)
	start := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	if err := tracker.Seed("ABCUSDT", []PricePoint{{At: start, Price: 100}}); err != nil {
		t.Fatal(err)
	}

	signal, err := tracker.Add("ABCUSDT", PricePoint{
		At: start.Add(15 * time.Minute), Price: 105,
	})
	if err != nil {
		t.Fatal(err)
	}
	if signal != nil {
		t.Fatalf("exactly +5%% must not signal: %+v", signal)
	}

	signal, err = tracker.Add("ABCUSDT", PricePoint{
		At: start.Add(15*time.Minute + time.Second), Price: 105.0001,
	})
	if err != nil {
		t.Fatal(err)
	}
	if signal == nil || signal.ChangePercent <= 5 {
		t.Fatalf("strictly above +5%% should signal: %+v", signal)
	}
}

func TestTrackerDoesNotRepeatUntilRearmed(t *testing.T) {
	tracker := newTestTracker(t)
	start := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	if err := tracker.Seed("ABCUSDT", []PricePoint{
		{At: start, Price: 100},
		{At: start.Add(15 * time.Minute), Price: 106},
	}); err != nil {
		t.Fatal(err)
	}

	first, err := tracker.Add("ABCUSDT", PricePoint{
		At: start.Add(15*time.Minute + time.Second), Price: 107,
	})
	if err != nil || first == nil {
		t.Fatalf("first signal=%+v error=%v", first, err)
	}
	second, err := tracker.Add("ABCUSDT", PricePoint{
		At: start.Add(15*time.Minute + 2*time.Second), Price: 108,
	})
	if err != nil || second != nil {
		t.Fatalf("duplicate signal=%+v error=%v", second, err)
	}

	// Move the rolling reference forward until the return drops to <=5%.
	rearmed, err := tracker.Add("ABCUSDT", PricePoint{
		At: start.Add(30*time.Minute + time.Second), Price: 105,
	})
	if err != nil || rearmed != nil {
		t.Fatalf("re-arm tick should not signal, signal=%+v error=%v", rearmed, err)
	}
	next, err := tracker.Add("ABCUSDT", PricePoint{
		At: start.Add(30*time.Minute + 3*time.Second), Price: 114,
	})
	if err != nil || next == nil || next.ChangePercent <= 5 {
		t.Fatalf("expected a new signal after re-arm, signal=%+v error=%v", next, err)
	}
}

func TestTrackerRejectsStaleReference(t *testing.T) {
	tracker := newTestTracker(t)
	start := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	if err := tracker.Seed("ABCUSDT", []PricePoint{{At: start, Price: 100}}); err != nil {
		t.Fatal(err)
	}
	signal, err := tracker.Add("ABCUSDT", PricePoint{
		At: start.Add(20 * time.Minute), Price: 120,
	})
	if err != nil {
		t.Fatal(err)
	}
	if signal != nil {
		t.Fatalf("stale reference must not create a signal: %+v", signal)
	}
}

func TestTrackerKeepsSymbolsAndCoalescesSameSecond(t *testing.T) {
	tracker := newTestTracker(t)
	start := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		if _, err := tracker.Add("AAAUSDT", PricePoint{
			At:    start.Add(time.Duration(i) * 100 * time.Millisecond),
			Price: 1 + float64(i)/100,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(tracker.symbols["AAAUSDT"].points); got != 1 {
		t.Fatalf("same-second points = %d, want 1", got)
	}
	if _, err := tracker.Add("BBBUSDT", PricePoint{At: start, Price: 2}); err != nil {
		t.Fatal(err)
	}
	tracker.KeepSymbols([]string{"AAAUSDT"})
	if !tracker.HasSymbol("AAAUSDT") || tracker.HasSymbol("BBBUSDT") {
		t.Fatal("KeepSymbols did not retain only requested symbols")
	}
}

func TestTrackerRejectsInvalidInput(t *testing.T) {
	if _, err := NewTracker(0, 5, time.Minute); err == nil {
		t.Fatal("zero window must fail")
	}
	if _, err := NewTracker(time.Minute, math.NaN(), time.Minute); err == nil {
		t.Fatal("NaN threshold must fail")
	}
	tracker := newTestTracker(t)
	if _, err := tracker.Add("bad-symbol", PricePoint{At: time.Now(), Price: 1}); err == nil {
		t.Fatal("invalid symbol must fail")
	}
	if _, err := tracker.Add("ABCUSDT", PricePoint{At: time.Now(), Price: 0}); err == nil {
		t.Fatal("zero price must fail")
	}
}

func TestTrackerSeedMergesHistoryWithLivePoint(t *testing.T) {
	tracker := newTestTracker(t)
	start := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	if _, err := tracker.Add("ABCUSDT", PricePoint{
		At: start.Add(16 * time.Minute), Price: 106,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Seed("ABCUSDT", []PricePoint{
		{At: start, Price: 100},
		{At: start.Add(time.Minute), Price: 101},
	}); err != nil {
		t.Fatal(err)
	}
	if !tracker.HasSymbol("ABCUSDT") || !tracker.HasHistory("ABCUSDT") {
		t.Fatal("seeded symbol/history disappeared")
	}
	signal, err := tracker.Add("ABCUSDT", PricePoint{
		At: start.Add(16*time.Minute + time.Second), Price: 107,
	})
	if err != nil {
		t.Fatal(err)
	}
	if signal == nil || signal.ReferencePrice != 101 {
		t.Fatalf("seed/live merge did not preserve historical baseline: %+v", signal)
	}
}

func TestTrackerCanRearmAfterDownstreamPersistenceFailure(t *testing.T) {
	tracker := newTestTracker(t)
	start := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	if err := tracker.Seed("ABCUSDT", []PricePoint{{At: start, Price: 100}}); err != nil {
		t.Fatal(err)
	}
	first, err := tracker.Add("ABCUSDT", PricePoint{At: start.Add(15*time.Minute + time.Second), Price: 106})
	if err != nil || first == nil {
		t.Fatalf("first crossing: signal=%+v err=%v", first, err)
	}
	duplicate, err := tracker.Add("ABCUSDT", PricePoint{At: start.Add(15*time.Minute + 2*time.Second), Price: 107})
	if err != nil || duplicate != nil {
		t.Fatalf("tracker should disarm after crossing: signal=%+v err=%v", duplicate, err)
	}

	tracker.Rearm("ABCUSDT")
	retried, err := tracker.Add("ABCUSDT", PricePoint{At: start.Add(15*time.Minute + 3*time.Second), Price: 108})
	if err != nil || retried == nil {
		t.Fatalf("crossing should be retryable after persistence failure: signal=%+v err=%v", retried, err)
	}
}

func TestTrackerSupportsMultipleIndependentRules(t *testing.T) {
	tracker, err := NewRuleTracker(DefaultMaxReferenceAge)
	if err != nil {
		t.Fatal(err)
	}
	rules := []Rule{
		{Window: time.Minute, ThresholdPercent: 1},
		{Window: 5 * time.Minute, ThresholdPercent: 3},
		{Window: 10 * time.Minute, ThresholdPercent: 10},
	}
	if err := tracker.SetRules(rules); err != nil {
		t.Fatal(err)
	}
	if got := tracker.RuleCount(); got != 3 {
		t.Fatalf("active rule count = %d, want 3", got)
	}

	start := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	if err := tracker.Seed("ABCUSDT", []PricePoint{
		{At: start.Add(-10 * time.Minute), Price: 100},
		{At: start.Add(-5 * time.Minute), Price: 100},
		{At: start.Add(-time.Minute), Price: 100},
	}); err != nil {
		t.Fatal(err)
	}

	signals, err := tracker.AddAll("ABCUSDT", PricePoint{At: start, Price: 112})
	if err != nil {
		t.Fatal(err)
	}
	if len(signals) != 3 {
		t.Fatalf("signals = %+v, want all three rules to trigger", signals)
	}
	want := map[Rule]bool{
		{Window: time.Minute, ThresholdPercent: 1}:       false,
		{Window: 5 * time.Minute, ThresholdPercent: 3}:    false,
		{Window: 10 * time.Minute, ThresholdPercent: 10}:  false,
	}
	for _, signal := range signals {
		rule := Rule{Window: signal.Window, ThresholdPercent: signal.ThresholdPercent}
		if _, ok := want[rule]; !ok {
			t.Errorf("unexpected triggered rule: %+v", signal)
		}
		delete(want, rule)
	}
	if len(want) != 0 {
		t.Fatalf("some configured rules did not trigger: %+v", want)
	}

	// All three rules re-arm on a pullback, but they still have independent
	// state and can trigger again on the next qualifying movement.
	if _, err := tracker.AddAll("ABCUSDT", PricePoint{
		At: start.Add(time.Minute), Price: 103,
	}); err != nil {
		t.Fatal(err)
	}
	signals, err = tracker.AddAll("ABCUSDT", PricePoint{
		At: start.Add(time.Minute + time.Second), Price: 115,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(signals) != 3 {
		t.Fatalf("signals after re-arm = %+v, want three independently re-armed rules", signals)
	}
}

func TestTrackerUpdatesRulesWithoutLosingSeededHistory(t *testing.T) {
	tracker, err := NewRuleTracker(DefaultMaxReferenceAge)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	if err := tracker.Seed("ABCUSDT", []PricePoint{
		{At: start.Add(-60 * time.Minute), Price: 100},
		{At: start.Add(-time.Minute), Price: 100},
	}); err != nil {
		t.Fatal(err)
	}
	if err := tracker.SetRules([]Rule{{Window: time.Minute, ThresholdPercent: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := tracker.SetRules([]Rule{{Window: 60 * time.Minute, ThresholdPercent: 5}}); err != nil {
		t.Fatal(err)
	}
	signals, err := tracker.AddAll("ABCUSDT", PricePoint{At: start, Price: 106})
	if err != nil {
		t.Fatal(err)
	}
	if len(signals) != 1 || signals[0].Window != 60*time.Minute || signals[0].ReferencePrice != 100 {
		t.Fatalf("new longer rule did not use retained history: %+v", signals)
	}
}

func TestTrackerRejectsUnsupportedRule(t *testing.T) {
	tracker, err := NewRuleTracker(DefaultMaxReferenceAge)
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range []Rule{
		{Window: 59 * time.Second, ThresholdPercent: 1},
		{Window: 61 * time.Minute, ThresholdPercent: 1},
		{Window: time.Minute, ThresholdPercent: 0},
		{Window: time.Minute, ThresholdPercent: 1001},
		{Window: time.Minute, ThresholdPercent: math.NaN()},
	} {
		if err := tracker.SetRules([]Rule{rule}); err == nil {
			t.Errorf("unsupported rule should fail: %+v", rule)
		}
	}
}
