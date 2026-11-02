package stream

import (
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	marketpaper "github.com/Quantum103/Scrinner_PAMPA/internal/paper"
	"github.com/Quantum103/Scrinner_PAMPA/internal/strategy"
)

func TestEntryPauseKeepsPaperPositionManagerActive(t *testing.T) {
	store := newTestStore(t, 50, 10)
	now := time.Now().UTC()
	store.paper = func() *marketpaper.Engine {
		cfg := marketpaper.DefaultConfig()
		engine, err := marketpaper.New(cfg)
		if err != nil {
			t.Fatalf("create paper engine: %v", err)
		}
		return engine
	}()

	store.UpdateMarketData([]bybit.MarketData{{
		Instrument: bybit.Instrument{
			Symbol: "BTCUSDT", MinOrderQty: "0.001", QtyStep: "0.001",
			MinNotionalValue: "5", TickSize: "0.01", MaxMktOrderQty: "100",
			FundingInterval: 60,
		},
		Ticker: bybit.Ticker{
			MarkPrice: "100", FundingRate: "0.0001", NextFundingTime: now.Add(time.Hour),
		},
	}}, now)
	entryBook := marketpaper.Book{
		Valid: true, Fresh: true, ReceivedAt: now,
		Bids: []marketpaper.Level{{Price: 99, Quantity: 100}},
		Asks: []marketpaper.Level{{Price: 100, Quantity: 100}},
	}
	signal := strategy.Signal{
		Symbol: "BTCUSDT", Direction: "LONG", Price: 100, Time: now,
		Reason: "test signal",
	}
	entry := store.paper.OpenLong(signal, entryBook, now)
	if entry.Type != "position_opened" && entry.Type != "position_partially_opened" {
		t.Fatalf("open test position: %+v", entry)
	}
	before := store.PaperSnapshot(now)
	initialStop := before.OpenPositions[0].StopPrice

	store.SetEntryPaused(true)
	if !store.EntryPaused() {
		t.Fatal("entry pause was not enabled")
	}
	// Менеджер позиции вызывается до проверки паузы поиска новых сигналов.
	rallyAt := now.Add(time.Second)
	payload := `{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["105","100"]],"a":[["106","100"]],"u":1,"seq":1}}`
	if err := store.ApplyOrderBook(withFreshTimestamp(payload, rallyAt), rallyAt); err != nil {
		t.Fatalf("apply fresh book while paused: %v", err)
	}
	after := store.PaperSnapshot(rallyAt)
	if len(after.OpenPositions) != 1 {
		t.Fatalf("open position was not preserved: %+v", after.OpenPositions)
	}
	if after.OpenPositions[0].StopPrice <= initialStop {
		t.Fatalf("Trailing Stop did not move while entries paused: before %.8f after %.8f",
			initialStop, after.OpenPositions[0].StopPrice)
	}
}

func TestManualAndDailyPauseAreIndependent(t *testing.T) {
	store := newTestStore(t, 50, 10)
	store.SetEntryPaused(true)
	store.SetDailyLossReached(true)
	store.SetEntryPaused(false)
	if !store.EntryPaused() {
		t.Fatal("manual resume must not clear the daily loss lock")
	}
	store.SetDailyLossReached(false)
	if store.EntryPaused() {
		t.Fatal("entry lock remained after both pause reasons were cleared")
	}
}
