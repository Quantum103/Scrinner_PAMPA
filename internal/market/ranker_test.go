package market

import (
	"fmt"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
)

func TestRankerBuilds25CandidatesPlus10DistinctAccelerationLeaders(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 0, 20, 0, time.UTC)
	store := NewCandleStore(90 * time.Second)
	items := make([]bybit.MarketData, 0, 41)

	for i := 0; i < 30; i++ {
		symbol := fmt.Sprintf("CAND%02dUSDT", i)
		turnovers := make([]float64, 20)
		for j := 0; j < 15; j++ {
			turnovers[j] = 10
		}
		for j := 15; j < 20; j++ {
			turnovers[j] = float64(100 + i)
		}
		addRankerFixture(t, store, symbol, turnovers, flatPrices(20, 100), now)
		items = append(items, bybit.MarketData{Instrument: bybit.Instrument{Symbol: symbol}})
	}

	for i := 0; i < 10; i++ {
		symbol := fmt.Sprintf("ACCEL%02dUSDT", i)
		turnovers := make([]float64, 20)
		for j := range turnovers {
			turnovers[j] = 0.1
		}
		prices := acceleratingPrices(i)
		addRankerFixture(t, store, symbol, turnovers, prices, now)
		items = append(items, bybit.MarketData{Instrument: bybit.Instrument{Symbol: symbol}})
	}

	// Этот инструмент ещё не прогрел непрерывную историю и не должен попадать в рейтинг.
	store.Activate("WARMUSDT")
	items = append(items, bybit.MarketData{Instrument: bybit.Instrument{Symbol: "WARMUSDT"}})

	ranker, err := NewRanker(store, RankerConfig{UpdateInterval: time.Second, AccelerationLeaders: 10})
	if err != nil {
		t.Fatalf("NewRanker(): %v", err)
	}
	snapshot := ranker.Build(MonitorSnapshot{UpdatedAt: now, Items: items}, now)

	if snapshot.Status != RankingStatusReady {
		t.Fatalf("status = %q, want %q (%s)", snapshot.Status, RankingStatusReady, snapshot.Reason)
	}
	if snapshot.MarketSymbols != 41 || snapshot.ReadySymbols != 40 {
		t.Fatalf("market/ready symbols = %d/%d, want 41/40", snapshot.MarketSymbols, snapshot.ReadySymbols)
	}
	if len(snapshot.Candidates) != 25 {
		t.Fatalf("candidates = %d, want 25", len(snapshot.Candidates))
	}
	if len(snapshot.Accelerators) != 10 {
		t.Fatalf("accelerators = %d, want 10", len(snapshot.Accelerators))
	}
	if snapshot.Candidates[0].Symbol != "CAND29USDT" {
		t.Fatalf("first candidate = %s, want CAND29USDT", snapshot.Candidates[0].Symbol)
	}
	if snapshot.Candidates[0].Rank != 1 {
		t.Fatalf("first candidate rank = %d, want 1", snapshot.Candidates[0].Rank)
	}
	if snapshot.Candidates[0].Turnover5mUSDT != 645 {
		t.Fatalf("TOP candidate turnover = %.4f, want 645", snapshot.Candidates[0].Turnover5mUSDT)
	}
	if !snapshot.Candidates[0].TurnoverMultipleAvailable ||
		snapshot.Candidates[0].AveragePrevious5mTurnoverUSDT != 50 ||
		snapshot.Candidates[0].TurnoverMultiple != 12.9 {
		t.Fatalf("incorrect accumulation baseline/multiple: %+v", snapshot.Candidates[0])
	}
	if snapshot.Accelerators[0].Symbol != "ACCEL09USDT" {
		t.Fatalf("first acceleration leader = %s, want ACCEL09USDT", snapshot.Accelerators[0].Symbol)
	}
	seen := make(map[string]bool, 35)
	for _, item := range snapshot.Candidates {
		if seen[item.Symbol] {
			t.Fatalf("duplicate candidate %s", item.Symbol)
		}
		seen[item.Symbol] = true
	}
	for _, item := range snapshot.Accelerators {
		if seen[item.Symbol] {
			t.Fatalf("candidate and acceleration groups overlap at %s", item.Symbol)
		}
		seen[item.Symbol] = true
	}
	if len(seen) != 35 {
		t.Fatalf("distinct ranked symbols = %d, want 35", len(seen))
	}
}

func TestRankerRejectsStaleMarketAndNonPositiveAcceleration(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 0, 20, 0, time.UTC)
	store := NewCandleStore(90 * time.Second)
	turnovers := make([]float64, 20)
	prices := flatPrices(20, 100)
	for i := range turnovers {
		turnovers[i] = 1
	}
	addRankerFixture(t, store, "FLATUSDT", turnovers, prices, now)
	ranker, err := NewRanker(store, RankerConfig{UpdateInterval: time.Second, AccelerationLeaders: 5})
	if err != nil {
		t.Fatalf("NewRanker(): %v", err)
	}

	stale := ranker.Build(MonitorSnapshot{
		UpdatedAt: now.Add(-rankingMarketMaxAge - time.Second),
		Items: []bybit.MarketData{{Instrument: bybit.Instrument{Symbol: "FLATUSDT"}}},
	}, now)
	if stale.Status != RankingStatusStale || len(stale.Candidates) != 0 || len(stale.Accelerators) != 0 {
		t.Fatalf("stale snapshot must not produce rankings: %+v", stale)
	}

	fresh := ranker.Build(MonitorSnapshot{
		UpdatedAt: now,
		Items: []bybit.MarketData{{Instrument: bybit.Instrument{Symbol: "FLATUSDT"}}},
	}, now)
	if len(fresh.Accelerators) != 0 {
		t.Fatalf("flat price must not be treated as acceleration leader: %+v", fresh.Accelerators)
	}
	if len(fresh.Candidates) != 1 {
		t.Fatalf("positive turnover should produce one candidate, got %d", len(fresh.Candidates))
	}
}

func TestNewRankerValidatesAccelerationLeaderLimit(t *testing.T) {
	store := NewCandleStore(time.Minute)
	for _, count := range []int{0, 4, 11} {
		if _, err := NewRanker(store, RankerConfig{UpdateInterval: time.Second, AccelerationLeaders: count}); err == nil {
			t.Errorf("NewRanker accepted acceleration leader count %d", count)
		}
	}
}

func TestMonitorStoresDefensiveRankingSnapshot(t *testing.T) {
	monitor := NewMonitor()
	input := RankingSnapshot{
		Status: RankingStatusReady,
		Candidates: []RankedSymbol{{Rank: 1, Symbol: "AAAUSDT", Turnover5mUSDT: 100}},
		Accelerators: []RankedSymbol{{Rank: 1, Symbol: "BBBUSDT", PriceAccelerationPP: 1}},
	}
	monitor.UpdateRanking(input)
	input.Candidates[0].Symbol = "MUTATED"
	read := monitor.Ranking()
	read.Accelerators[0].Symbol = "ALSO_MUTATED"
	stored := monitor.Ranking()
	if stored.Candidates[0].Symbol != "AAAUSDT" || stored.Accelerators[0].Symbol != "BBBUSDT" {
		t.Fatalf("ranking snapshot was not copied defensively: %+v", stored)
	}
}

func addRankerFixture(t *testing.T, store *CandleStore, symbol string, turnovers, prices []float64, now time.Time) {
	t.Helper()
	if len(turnovers) != 20 || len(prices) != 20 {
		t.Fatal("ranker fixture must contain exactly 20 completed candles")
	}
	currentMinute := now.UTC().Truncate(time.Minute)
	store.Activate(symbol)
	history := make([]bybit.Kline, 20)
	for i := range history {
		start := currentMinute.Add(time.Duration(i-20) * time.Minute)
		closePrice := prices[i]
		history[i] = bybit.Kline{
			StartTime: start,
			Open: closePrice,
			High: closePrice,
			Low: closePrice,
			Close: closePrice,
			Volume: turnovers[i] / closePrice,
			Turnover: turnovers[i],
		}
	}
	if err := store.LoadHistory(symbol, history, now); err != nil {
		t.Fatalf("LoadHistory(%s): %v", symbol, err)
	}
	currentPrice := prices[len(prices)-1]
	if err := store.ApplyUpdate(symbol, Candle{
		StartTime: currentMinute,
		Open: currentPrice,
		High: currentPrice,
		Low: currentPrice,
		Close: currentPrice,
	}, now); err != nil {
		t.Fatalf("ApplyUpdate(%s): %v", symbol, err)
	}
}

func flatPrices(count int, price float64) []float64 {
	result := make([]float64, count)
	for i := range result {
		result[i] = price
	}
	return result
}

func acceleratingPrices(index int) []float64 {
	result := flatPrices(20, 100)
	result[14] = 100.05
	result[15] = 100.10
	result[16] = 100.15
	result[17] = 100.40
	result[18] = 100.80
	result[19] = 101.20 + float64(index)*0.10
	return result
}
