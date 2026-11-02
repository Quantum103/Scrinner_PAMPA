package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	marketanalysis "github.com/Quantum103/Scrinner_PAMPA/internal/analysis"
)

func newTestStore(t *testing.T, depth, tradeCapacity int) *Store {
	t.Helper()
	store, err := NewStore(depth, 5*time.Second, tradeCapacity, 100)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	store.StartSession([]string{"BTCUSDT"})
	return store
}

func TestOrderBookSnapshotAndDelta(t *testing.T) {
	store := newTestStore(t, 50, 10)
	now := time.Now()

	snapshot := `{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1793510400000,"data":{"s":"BTCUSDT","b":[["100.00","2"],["99","3"],["98","1"]],"a":[["101","4"],["102","5"]],"u":10,"seq":100}}`
	if err := store.ApplyOrderBook(withFreshTimestamp(snapshot, now), now); err != nil {
		t.Fatalf("snapshot rejected: %v", err)
	}

	delta := `{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1793510400001,"data":{"s":"BTCUSDT","b":[["100","0"],["97","6"],["99","7"]],"a":[["101","0"],["103","8"]],"u":11,"seq":101}}`
	if err := store.ApplyOrderBook(withFreshTimestamp(delta, now.Add(time.Millisecond)), now.Add(time.Millisecond)); err != nil {
		t.Fatalf("delta rejected: %v", err)
	}

	state, ok := store.Snapshot("BTCUSDT", now.Add(2*time.Millisecond))
	if !ok || !state.OrderBook.Valid {
		t.Fatal("orderbook is not valid after snapshot + delta")
	}
	if state.OrderBook.BestBid != "99" || state.OrderBook.BestAsk != "102" {
		t.Fatalf("best prices = %s / %s, want 99 / 102", state.OrderBook.BestBid, state.OrderBook.BestAsk)
	}
	if len(state.OrderBook.Bids) != 3 || state.OrderBook.Bids[0].Size != "7" {
		t.Fatalf("bid levels were not updated/deleted: %+v", state.OrderBook.Bids)
	}
	if len(state.OrderBook.Asks) != 2 || state.OrderBook.Asks[1].Price != "103" {
		t.Fatalf("ask levels were not updated/inserted: %+v", state.OrderBook.Asks)
	}
}

func TestNewSnapshotCompletelyReplacesLocalBook(t *testing.T) {
	store := newTestStore(t, 50, 10)
	now := time.Now()
	first := `{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100","1"],["99","1"]],"a":[["101","1"],["102","1"]],"u":10,"seq":10}}`
	second := `{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":2,"data":{"s":"BTCUSDT","b":[["90","4"]],"a":[["110","5"]],"u":1,"seq":1}}`
	if err := store.ApplyOrderBook(withFreshTimestamp(first, now), now); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyOrderBook(withFreshTimestamp(second, now.Add(time.Millisecond)), now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Snapshot("BTCUSDT", now.Add(2*time.Millisecond))
	if len(state.OrderBook.Bids) != 1 || len(state.OrderBook.Asks) != 1 || state.OrderBook.BestBid != "90" || state.OrderBook.BestAsk != "110" {
		t.Fatalf("snapshot did not replace previous levels: %+v", state.OrderBook)
	}
}

func TestDeltaWithoutSnapshotInvalidatesBook(t *testing.T) {
	store, err := NewStore(50, time.Second, 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	store.StartSession([]string{"BTCUSDT"})
	delta := `{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1,"data":{"s":"BTCUSDT","b":[["100","1"]],"a":[],"u":5,"seq":5}}`
	if err := store.ApplyOrderBook(withFreshTimestamp(delta, time.Now()), time.Now()); !errors.Is(err, ErrSnapshotRequired) {
		t.Fatalf("delta without snapshot error = %v, want ErrSnapshotRequired", err)
	}
	state, _ := store.Snapshot("BTCUSDT", time.Now())
	if state.OrderBook.Valid || state.UsableForSignal {
		t.Fatal("book became usable without snapshot")
	}
}

func TestSnapshotWithCrossedPricesIsRejected(t *testing.T) {
	store := newTestStore(t, 50, 10)
	now := time.Now()
	payload := `{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["102","1"]],"a":[["101","1"]],"u":10,"seq":10}}`
	if err := store.ApplyOrderBook(withFreshTimestamp(payload, now), now); !errors.Is(err, ErrInvalidOrderbook) {
		t.Fatalf("crossed snapshot error = %v, want ErrInvalidOrderbook", err)
	}
	trades := `{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"BTCUSDT","S":"Buy","v":"1","p":"100","i":"crossed-book-trade"}]}`
	if err := store.ApplyTrades(withFreshTimestamp(trades, now), now); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Snapshot("BTCUSDT", now)
	if state.OrderBook.Valid {
		t.Fatal("crossed book marked as valid")
	}
	if state.Analysis.Status != marketanalysis.StatusIncomplete {
		t.Fatalf("analysis status = %q, want incomplete for an invalid book", state.Analysis.Status)
	}
}

func TestLowerSequenceInvalidatesBook(t *testing.T) {
	store := newTestStore(t, 50, 10)
	now := time.Now()
	snapshot := `{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","1"]],"u":10,"seq":100}}`
	if err := store.ApplyOrderBook(withFreshTimestamp(snapshot, now), now); err != nil {
		t.Fatal(err)
	}
	delta := `{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":2,"data":{"s":"BTCUSDT","b":[],"a":[["102","1"]],"u":11,"seq":99}}`
	if err := store.ApplyOrderBook(withFreshTimestamp(delta, now.Add(time.Millisecond)), now.Add(time.Millisecond)); !errors.Is(err, ErrInvalidOrderbook) {
		t.Fatalf("decreasing sequence error = %v, want ErrInvalidOrderbook", err)
	}
	state, _ := store.Snapshot("BTCUSDT", now.Add(time.Millisecond))
	if state.OrderBook.Valid {
		t.Fatal("book with decreasing sequence marked as valid")
	}
}

func TestTradeParsingDeduplicationAndOverflowAreVisible(t *testing.T) {
	store := newTestStore(t, 50, 2)
	now := time.Now()
	book := `{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","1"]],"u":10,"seq":10}}`
	if err := store.ApplyOrderBook(withFreshTimestamp(book, now), now); err != nil {
		t.Fatal(err)
	}

	trades := `{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[` +
		`{"T":1,"s":"BTCUSDT","S":"Buy","v":"0.1","p":"100.1","i":"trade-1"},` +
		`{"T":2,"s":"BTCUSDT","S":"Sell","v":"0.2","p":"100.2","i":"trade-2"},` +
		`{"T":3,"s":"BTCUSDT","S":"Buy","v":"0.3","p":"100.3","i":"trade-3"}]}`
	if err := store.ApplyTrades(withFreshTimestamp(trades, now), now); err != nil {
		t.Fatal(err)
	}
	duplicate := `{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":4,"data":[{"T":4,"s":"BTCUSDT","S":"Buy","v":"0.1","p":"100.1","i":"trade-3"}]}`
	if err := store.ApplyTrades(withFreshTimestamp(duplicate, now.Add(time.Millisecond)), now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	state, _ := store.Snapshot("BTCUSDT", now.Add(2*time.Millisecond))
	if state.DuplicateTrades != 1 || state.DroppedTrades != 1 || !state.TradeGap || state.UsableForSignal {
		t.Fatalf("trade data loss/duplicates not exposed: %+v", state)
	}
	queued := store.DrainTrades("BTCUSDT", 10)
	if len(queued) != 2 || queued[0].TradeID != "trade-2" || queued[1].TradeID != "trade-3" {
		t.Fatalf("trade queue = %+v", queued)
	}
	store.AcknowledgeTradeGap("BTCUSDT")
	state, _ = store.Snapshot("BTCUSDT", now.Add(2*time.Millisecond))
	if !state.UsableForSignal {
		t.Fatal("fresh book/trades should be usable after the consumer acknowledges rebuilt trade history")
	}
}

func TestStaleTradeIsDroppedWithoutRejectingFreshTradesInBatch(t *testing.T) {
	store := newTestStore(t, 50, 10)
	now := time.Now()
	payload := fmt.Sprintf(`{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":%d,"data":[
		{"T":%d,"s":"BTCUSDT","S":"Buy","v":"0.1","p":"100.1","i":"stale-trade"},
		{"T":%d,"s":"BTCUSDT","S":"Buy","v":"0.2","p":"100.2","i":"fresh-trade"}
	]}`,
		now.UnixMilli(),
		now.Add(-6*time.Second).UnixMilli(),
		now.Add(-100*time.Millisecond).UnixMilli(),
	)

	err := store.ApplyTrades([]byte(payload), now)
	if !errors.Is(err, ErrTradeDataGap) {
		t.Fatalf("ApplyTrades() error = %v, want ErrTradeDataGap", err)
	}

	state, ok := store.Snapshot("BTCUSDT", now)
	if !ok {
		t.Fatal("symbol snapshot is missing")
	}
	if !state.TradeGap {
		t.Fatal("stale trade did not mark the history as incomplete")
	}
	if state.LastTrade == nil || state.LastTrade.TradeID != "fresh-trade" {
		t.Fatalf("fresh trade was not retained after stale trade: %+v", state.LastTrade)
	}
	queued := store.DrainTrades("BTCUSDT", 10)
	if len(queued) != 1 || queued[0].TradeID != "fresh-trade" {
		t.Fatalf("trade queue contains stale or missing trades: %+v", queued)
	}
}

func TestTradeGapClearsOnlyAfterAnalysisHistoryIsReady(t *testing.T) {
	cfg := marketanalysis.DefaultConfig()
	cfg.RVOLInterval = time.Second
	cfg.RVOLLookbackIntervals = 1
	cfg.FlowWindow = time.Second
	cfg.CVDWindow = 2 * time.Second
	cfg.MomentumWindows = []time.Duration{time.Second}
	cfg.VolumeAccelerationWindow = time.Second

	store, err := NewStoreWithAnalysis(50, 5*time.Second, 100, 100, cfg)
	if err != nil {
		t.Fatalf("NewStoreWithAnalysis() error = %v", err)
	}

	now := time.Unix(1_700_000_005, 500_000_000).UTC()
	evaluationAt := now.Add(400 * time.Millisecond)
	store.startSessionAt([]string{"BTCUSDT"}, now.Add(-10*time.Second))
	store.analyzer.MarkGap("BTCUSDT", now.Add(-5*time.Second), "тестовый разрыв")
	store.mu.Lock()
	store.states["BTCUSDT"].tradeGap = true
	store.mu.Unlock()

	book := `{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100","2"],["99.9","3"]],"a":[["100.02","4"],["100.03","5"]],"u":10,"seq":10}}`
	if err := store.ApplyOrderBook(withFreshTimestamp(book, evaluationAt), evaluationAt); err != nil {
		t.Fatalf("ApplyOrderBook() error = %v", err)
	}

	trades := fmt.Sprintf(`{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":%d,"data":[
		{"T":%d,"s":"BTCUSDT","S":"Buy","v":"10","p":"100","i":"gap-1"},
		{"T":%d,"s":"BTCUSDT","S":"Buy","v":"10","p":"100.01","i":"gap-2"},
		{"T":%d,"s":"BTCUSDT","S":"Buy","v":"1","p":"100.01","i":"gap-3"},
		{"T":%d,"s":"BTCUSDT","S":"Buy","v":"20","p":"100.02","i":"gap-4"},
		{"T":%d,"s":"BTCUSDT","S":"Sell","v":"1","p":"100.02","i":"gap-5"}
	]}`,
		evaluationAt.UnixMilli(),
		now.Add(-1400*time.Millisecond).UnixMilli(),
		now.Add(-900*time.Millisecond).UnixMilli(),
		now.Add(-600*time.Millisecond).UnixMilli(),
		now.Add(100*time.Millisecond).UnixMilli(),
		now.Add(300*time.Millisecond).UnixMilli(),
	)
	if err := store.ApplyTrades([]byte(trades), evaluationAt); err != nil {
		t.Fatalf("ApplyTrades() error = %v", err)
	}

	state, ok := store.Snapshot("BTCUSDT", evaluationAt)
	if !ok {
		t.Fatal("symbol snapshot is missing")
	}
	if state.Analysis.Status != marketanalysis.StatusReady {
		t.Fatalf("analysis status = %q, want ready: %+v", state.Analysis.Status, state.Analysis)
	}
	if state.TradeGap {
		t.Fatal("trade gap stayed active after the complete analysis history was rebuilt")
	}
}

func TestReconnectKeepsSignalDisabledUntilTradeGapAcknowledged(t *testing.T) {
	store := newTestStore(t, 50, 10)
	now := time.Now()
	book := `{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","1"]],"u":10,"seq":10}}`
	trades := `{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"BTCUSDT","S":"Buy","v":"0.1","p":"100.1","i":"trade-1"}]}`
	if err := store.ApplyOrderBook(withFreshTimestamp(book, now), now); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyTrades(withFreshTimestamp(trades, now), now); err != nil {
		t.Fatal(err)
	}
	store.MarkDisconnected([]string{"BTCUSDT"})
	store.StartSession([]string{"BTCUSDT"})
	if err := store.ApplyOrderBook(withFreshTimestamp(book, now.Add(time.Millisecond)), now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyTrades(withFreshTimestamp(trades, now.Add(time.Millisecond)), now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Snapshot("BTCUSDT", now.Add(2*time.Millisecond))
	if state.UsableForSignal || !state.TradeGap {
		t.Fatalf("reconnected symbol was incorrectly enabled: %+v", state)
	}
}

func TestStaleOrderbookCannotBeUsed(t *testing.T) {
	store := newTestStore(t, 50, 10)
	now := time.Now()
	book := `{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","1"]],"u":10,"seq":10}}`
	trades := `{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"BTCUSDT","S":"Buy","v":"0.1","p":"100.1","i":"trade-1"}]}`
	if err := store.ApplyOrderBook(withFreshTimestamp(book, now), now); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyTrades(withFreshTimestamp(trades, now), now); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Snapshot("BTCUSDT", now.Add(6*time.Second))
	if state.UsableForSignal || state.OrderBook.Fresh {
		t.Fatal("stale data marked as fresh or usable")
	}
}

func withFreshTimestamp(payload string, now time.Time) []byte {
	var value map[string]any
	if err := json.Unmarshal([]byte(payload), &value); err != nil {
		panic(err)
	}
	value["ts"] = now.UnixMilli()
	if data, ok := value["data"].([]any); ok {
		for _, item := range data {
			trade := item.(map[string]any)
			trade["T"] = now.UnixMilli()
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}

func TestSubscriptionBatchesStayWithinConfiguredLimits(t *testing.T) {
	symbols := make([]string, 26)
	for i := range symbols {
		symbols[i] = fmt.Sprintf("COIN%02dUSDT", i)
	}
	batches := buildSubscriptionBatches(symbols, 50, 10)
	for _, batch := range batches {
		if len(batch.Arguments) > 10 {
			t.Fatalf("subscription batch has %d arguments", len(batch.Arguments))
		}
	}
	shards := splitSymbols(symbols, 25)
	if len(shards) != 2 || len(shards[0]) != 25 || len(shards[1]) != 1 {
		t.Fatalf("unexpected sharding: sizes %d and %d", len(shards[0]), len(shards[1]))
	}
}

func TestDeltaAfterQuietBookPeriodKeepsSnapshotValid(t *testing.T) {
	store := newTestStore(t, 50, 10)
	now := time.Now()
	snapshot := `{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","1"]],"u":10,"seq":10}}`
	if err := store.ApplyOrderBook(withFreshTimestamp(snapshot, now), now); err != nil {
		t.Fatal(err)
	}
	delta := `{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1,"data":{"s":"BTCUSDT","b":[["100","2"]],"a":[],"u":11,"seq":11}}`
	later := now.Add(6 * time.Second)
	if err := store.ApplyOrderBook(withFreshTimestamp(delta, later), later); err != nil {
		t.Fatalf("delta after a quiet period was rejected: %v", err)
	}
	state, ok := store.Snapshot("BTCUSDT", later)
	if !ok || !state.OrderBook.Valid || !state.OrderBook.Fresh || state.OrderBook.BestBid != "100" {
		t.Fatalf("orderbook was not kept valid after quiet period: %+v", state.OrderBook)
	}
	if len(state.OrderBook.Bids) == 0 || state.OrderBook.Bids[0].Size != "2" {
		t.Fatalf("delta was not applied after quiet period: %+v", state.OrderBook.Bids)
	}
}

func TestTradeAfterLongSilenceRequiresWindowRebuild(t *testing.T) {
	store := newTestStore(t, 50, 10)
	now := time.Now()
	book := `{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","1"]],"u":10,"seq":10}}`
	if err := store.ApplyOrderBook(withFreshTimestamp(book, now), now); err != nil {
		t.Fatal(err)
	}
	first := `{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"BTCUSDT","S":"Buy","v":"0.1","p":"100.1","i":"trade-1"}]}`
	if err := store.ApplyTrades(withFreshTimestamp(first, now), now); err != nil {
		t.Fatal(err)
	}
	later := now.Add(6 * time.Second)
	second := `{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"BTCUSDT","S":"Buy","v":"0.1","p":"100.2","i":"trade-2"}]}`
	if err := store.ApplyTrades(withFreshTimestamp(second, later), later); err != nil {
		t.Fatal(err)
	}
	state, _ := store.Snapshot("BTCUSDT", later)
	if !state.TradeGap || state.UsableForSignal {
		t.Fatalf("сделка после длительного молчания не отметила разрыв: %+v", state)
	}
}

func TestStoreUpdatesSingleAnalyzerFromTradeEvents(t *testing.T) {
	cfg := marketanalysis.DefaultConfig()
	cfg.RVOLInterval = time.Second
	cfg.RVOLLookbackIntervals = 1
	cfg.FlowWindow = time.Second
	cfg.CVDWindow = 2 * time.Second
	cfg.MomentumWindows = []time.Duration{time.Second}
	cfg.VolumeAccelerationWindow = time.Second
	cfg.AbsorptionBuyVolumeMultiplier = 1.5
	cfg.AbsorptionMaxPriceChangePct = 0.1
	store, err := NewStoreWithAnalysis(50, 5*time.Second, 100, 100, cfg)
	if err != nil {
		t.Fatalf("NewStoreWithAnalysis() error = %v", err)
	}

	now := time.Now().UTC()
	store.startSessionAt([]string{"BTCUSDT"}, now.Add(-10*time.Second))
	book := fmt.Sprintf(`{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":%d,"data":{"s":"BTCUSDT","b":[["100","2"],["99.9","3"]],"a":[["100.02","4"],["100.03","5"]],"u":10,"seq":10}}`, now.UnixMilli())
	if err := store.ApplyOrderBook([]byte(book), now); err != nil {
		t.Fatalf("ApplyOrderBook() error = %v", err)
	}

	trades := fmt.Sprintf(`{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":%d,"data":[`+
		`{"T":%d,"s":"BTCUSDT","S":"Buy","v":"5","p":"100","i":"analysis-1"},`+
		`{"T":%d,"s":"BTCUSDT","S":"Buy","v":"10","p":"100.01","i":"analysis-2"},`+
		`{"T":%d,"s":"BTCUSDT","S":"Sell","v":"2","p":"100.01","i":"analysis-3"},`+
		`{"T":%d,"s":"BTCUSDT","S":"Buy","v":"1","p":"100.01","i":"analysis-4"}]}`, now.UnixMilli(),
		now.Add(-1500*time.Millisecond).UnixMilli(),
		now.Add(-500*time.Millisecond).UnixMilli(),
		now.Add(-300*time.Millisecond).UnixMilli(),
		now.Add(-100*time.Millisecond).UnixMilli())
	if err := store.ApplyTrades([]byte(trades), now); err != nil {
		t.Fatalf("ApplyTrades() error = %v", err)
	}

	state, ok := store.Snapshot("BTCUSDT", now)
	if !ok {
		t.Fatal("Snapshot() returned no symbol")
	}
	if state.Analysis.TakerBuyVolume == nil || state.Analysis.TakerSellVolume == nil || state.Analysis.Delta == nil {
		t.Fatalf("analyzer did not receive trade events: %+v", state.Analysis)
	}
	if *state.Analysis.TakerBuyVolume != 11 || *state.Analysis.TakerSellVolume != 2 || *state.Analysis.Delta != 9 {
		t.Fatalf("unexpected aggregated taker metrics: buy=%0.10f sell=%0.10f delta=%0.10f status=%s", *state.Analysis.TakerBuyVolume, *state.Analysis.TakerSellVolume, *state.Analysis.Delta, state.Analysis.Status)
	}
	if state.Analysis.SpreadBPS == nil || state.Analysis.BidLiquidityQuote == nil || state.Analysis.AskLiquidityQuote == nil {
		t.Fatalf("analyzer did not consume orderbook snapshot: %+v", state.Analysis)
	}
}
