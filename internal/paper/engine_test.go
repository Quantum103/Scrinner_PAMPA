package paper

import (
	"math"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	"github.com/Quantum103/Scrinner_PAMPA/internal/strategy"
)

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.InitialBalanceUSDT = 10
	cfg.MaxBookAge = 5 * time.Second
	return cfg
}

func testEngine(t *testing.T, cfg Config, now time.Time) *Engine {
	t.Helper()
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	engine.UpdateMarketData([]bybit.MarketData{{
		Instrument: bybit.Instrument{
			Symbol: "TESTUSDT", MinOrderQty: "0.001", QtyStep: "0.001",
			MinNotionalValue: "5", TickSize: "0.01", MaxMktOrderQty: "1000",
			FundingInterval: 60,
		},
		Ticker: bybit.Ticker{
			MarkPrice: "100", FundingRate: "0.0001", NextFundingTime: now.Add(time.Minute),
		},
	}}, now)
	return engine
}

func testBook(now time.Time) Book {
	return Book{
		Valid: true, Fresh: true, ReceivedAt: now,
		Asks: []Level{{Price: 100, Quantity: 0.05}, {Price: 100.1, Quantity: 100}},
		Bids: []Level{{Price: 99.9, Quantity: 100}, {Price: 99.8, Quantity: 100}},
	}
}

func testSignal(now time.Time) strategy.Signal {
	return strategy.Signal{
		Symbol: "TESTUSDT", Direction: "LONG", Price: 100,
		Time: now, Reason: "unit test",
	}
}

func openTestPosition(t *testing.T, engine *Engine, now time.Time) Event {
	t.Helper()
	event := engine.OpenLong(testSignal(now), testBook(now), now)
	if event.Type != "position_opened" && event.Type != "position_partially_opened" {
		t.Fatalf("OpenLong() event = %+v, want opened", event)
	}
	return event
}

func TestCalculatePositionSizeRiskAndMargin(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	event := openTestPosition(t, engine, now)

	if event.Quantity <= 0 || event.Notional <= 0 {
		t.Fatalf("invalid fill size: %+v", event)
	}
	if event.Leverage != 2 {
		t.Fatalf("leverage = %v, want 2", event.Leverage)
	}
	if event.Margin > 5+1e-8 {
		t.Fatalf("reserved margin = %.8f, above 50%% of 10 USDT", event.Margin)
	}
	if event.RiskPercent > 3 {
		t.Fatalf("calculated risk = %.4f%%, exceeds 3%%", event.RiskPercent)
	}
	if event.RiskPercent < 2.3 || event.RiskPercent > 2.7 {
		t.Fatalf("risk sizing = %.4f%%, want close to the 2.5%% target while remaining below the 3%% hard limit", event.RiskPercent)
	}

	account := engine.Snapshot(now)
	if len(account.OpenPositions) != 1 {
		t.Fatalf("open positions = %d, want 1", len(account.OpenPositions))
	}
	position := account.OpenPositions[0]
	if position.Direction != "LONG" || position.EntryPrice <= 100 || position.StopPrice >= position.EntryPrice {
		t.Fatalf("unexpected position: %+v", position)
	}
	if position.EntryFee <= 0 || position.InitialMargin <= 0 {
		t.Fatalf("position should include fee and margin: %+v", position)
	}
	if account.ReservedMargin <= 0 || account.FreeMargin >= account.Equity {
		t.Fatalf("margin snapshot is incorrect: %+v", account)
	}
}

func TestMarketBuyUsesAskDepthVWAPAndSlippage(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	event := openTestPosition(t, engine, now)

	// Вход должен пройти по нескольким Ask-уровням и включить 5 bps проскальзывания.
	if event.Price <= 100.05 {
		t.Fatalf("entry VWAP=%f, want price above best Ask due to depth and slippage", event.Price)
	}
	if event.Type != "position_opened" || math.Abs(event.Quantity-event.RequestedQuantity) > 1e-8 {
		t.Fatalf("the available Ask depth should fully execute this test order: %+v", event)
	}
}

func TestEntryCommissionIsChargedAndExitCommissionReducesPnL(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	entry := openTestPosition(t, engine, now)
	afterEntry := engine.Snapshot(now)
	if math.Abs(afterEntry.Balance-(10-entry.Fee)) > 1e-8 {
		t.Fatalf("balance after entry = %.8f, want %.8f", afterEntry.Balance, 10-entry.Fee)
	}

	closeBook := testBook(now.Add(time.Second))
	closeBook.Bids = []Level{{Price: 101, Quantity: 100}}
	closeBook.Asks = []Level{{Price: 102, Quantity: 100}}
	closeEvent := engine.CloseLong("TESTUSDT", closeBook, now.Add(time.Second), "test close")
	if closeEvent.Type != "position_closed" {
		t.Fatalf("CloseLong() = %+v, want position_closed", closeEvent)
	}
	if closeEvent.Fee <= 0 || closeEvent.GrossPnL <= closeEvent.NetPnL {
		t.Fatalf("exit commission not deducted: %+v", closeEvent)
	}
	account := engine.Snapshot(now.Add(time.Second))
	if len(account.OpenPositions) != 0 {
		t.Fatalf("positions after full close = %d, want 0", len(account.OpenPositions))
	}
	if account.TotalFees <= entry.Fee || account.RealizedPnL <= 0 || account.Balance <= 10 {
		t.Fatalf("commission or realized PnL accounting is wrong: %+v", account)
	}
	if len(account.ClosedTrades) != 1 {
		t.Fatalf("closed trade history = %d, want 1", len(account.ClosedTrades))
	}
	closed := account.ClosedTrades[0]
	if closed.ClosedAt.IsZero() || closed.CloseReason == "" || closed.ExitFee <= 0 {
		t.Fatalf("closed trade details are incomplete: %+v", closed)
	}
	if math.Abs(closed.RealizedPnL-account.RealizedPnL) > 1e-8 ||
		math.Abs(closeEvent.NetPnL-account.RealizedPnL) > 1e-8 {
		t.Fatalf("closed trade net PnL omits entry/exit costs: trade=%f event=%f account=%f", closed.RealizedPnL, closeEvent.NetPnL, account.RealizedPnL)
	}
}

func TestPnLAndUnrealizedPnLUseExecutableBid(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	openTestPosition(t, engine, now)

	moved := testBook(now.Add(time.Second))
	moved.Bids = []Level{{Price: 101, Quantity: 100}}
	moved.Asks = []Level{{Price: 101.1, Quantity: 100}}
	engine.UpdateBook("TESTUSDT", moved, now.Add(time.Second))
	account := engine.Snapshot(now.Add(time.Second))
	if account.UnrealizedPnL <= 0 {
		t.Fatalf("unrealized PnL = %.8f, want positive", account.UnrealizedPnL)
	}
	if account.Equity <= account.Balance {
		t.Fatalf("equity %.8f should include positive unrealized PnL above balance %.8f", account.Equity, account.Balance)
	}
}

func TestFundingIsAppliedWhenPositionCrossesFundingTime(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	openTestPosition(t, engine, now)

	fundingAt := now.Add(time.Minute)
	markAt := fundingAt.Add(-time.Second)
	engine.UpdateMarketData([]bybit.MarketData{{
		Instrument: bybit.Instrument{
			Symbol: "TESTUSDT", MinOrderQty: "0.001", QtyStep: "0.001",
			MinNotionalValue: "5", TickSize: "0.01", MaxMktOrderQty: "1000",
			FundingInterval: 60,
		},
		Ticker: bybit.Ticker{MarkPrice: "101.25", FundingRate: "0.0001", NextFundingTime: fundingAt},
	}}, markAt)
	book := testBook(fundingAt)
	book.Bids = []Level{{Price: 100, Quantity: 100}}
	book.Asks = []Level{{Price: 100.1, Quantity: 100}}
	engine.UpdateBook("TESTUSDT", book, fundingAt)

	events := engine.DrainEvents()
	found := false
	for _, event := range events {
		if event.Type == "funding_applied" {
			found = true
			if event.Funding >= 0 {
				t.Fatalf("positive funding rate should be a LONG cash outflow, got %f", event.Funding)
			}
			if math.Abs(event.Price-101.25) > 1e-9 {
				t.Fatalf("funding used price %.8f, want public Mark Price 101.25 rather than best Bid", event.Price)
			}
			if math.Abs(event.Funding+101.25*event.Quantity*0.0001) > 1e-9 {
				t.Fatalf("funding amount %.10f does not use Mark Price × quantity × rate", event.Funding)
			}
		}
	}
	if !found {
		t.Fatalf("funding event not found: %+v", events)
	}
	account := engine.Snapshot(fundingAt)
	if account.NetFunding >= 0 || account.RealizedPnL >= 0 {
		t.Fatalf("funding payment not reflected in account: %+v", account)
	}
}

func TestNegativeFundingRateCreditsLongPosition(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	engine.UpdateMarketData([]bybit.MarketData{{
		Instrument: bybit.Instrument{
			Symbol: "TESTUSDT", MinOrderQty: "0.001", QtyStep: "0.001",
			MinNotionalValue: "5", TickSize: "0.01", MaxMktOrderQty: "1000",
			FundingInterval: 60,
		},
		Ticker: bybit.Ticker{MarkPrice: "100", FundingRate: "-0.0002", NextFundingTime: now.Add(time.Minute)},
	}}, now)
	openTestPosition(t, engine, now)

	fundingAt := now.Add(time.Minute)
	markAt := fundingAt.Add(-time.Second)
	engine.UpdateMarketData([]bybit.MarketData{{
		Instrument: bybit.Instrument{
			Symbol: "TESTUSDT", MinOrderQty: "0.001", QtyStep: "0.001",
			MinNotionalValue: "5", TickSize: "0.01", MaxMktOrderQty: "1000",
			FundingInterval: 60,
		},
		Ticker: bybit.Ticker{MarkPrice: "100", FundingRate: "-0.0002", NextFundingTime: fundingAt},
	}}, markAt)
	book := testBook(fundingAt)
	book.Bids = []Level{{Price: 100, Quantity: 100}}
	book.Asks = []Level{{Price: 100.1, Quantity: 100}}
	engine.UpdateBook("TESTUSDT", book, fundingAt)
	account := engine.Snapshot(fundingAt)
	if account.NetFunding <= 0 {
		t.Fatalf("negative funding rate should credit LONG, net funding = %f", account.NetFunding)
	}
}

func TestTrailingStopClosesPositionUsingBidLiquidity(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	openTestPosition(t, engine, now)

	rally := testBook(now.Add(time.Second))
	rally.Bids = []Level{{Price: 105, Quantity: 100}}
	rally.Asks = []Level{{Price: 105.1, Quantity: 100}}
	engine.UpdateBook("TESTUSDT", rally, now.Add(time.Second))
	account := engine.Snapshot(now.Add(time.Second))
	if len(account.OpenPositions) != 1 {
		t.Fatal("position unexpectedly closed during rally")
	}
	stop := account.OpenPositions[0].StopPrice
	if stop <= 100 {
		t.Fatalf("trailing stop did not move up: %f", stop)
	}

	drop := testBook(now.Add(2 * time.Second))
	drop.Bids = []Level{{Price: stop - 0.1, Quantity: 100}}
	drop.Asks = []Level{{Price: stop + 0.1, Quantity: 100}}
	engine.UpdateBook("TESTUSDT", drop, now.Add(2*time.Second))
	account = engine.Snapshot(now.Add(2 * time.Second))
	if len(account.OpenPositions) != 0 {
		t.Fatalf("position was not closed at stop: %+v", account.OpenPositions)
	}
}

func TestPartialExitRemainsOpenAndReservesOnlyRemainingMargin(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	openTestPosition(t, engine, now)

	rally := testBook(now.Add(time.Second))
	rally.Bids = []Level{{Price: 105, Quantity: 100}}
	engine.UpdateBook("TESTUSDT", rally, now.Add(time.Second))
	position := engine.Snapshot(now.Add(time.Second)).OpenPositions[0]
	drop := testBook(now.Add(2 * time.Second))
	drop.Bids = []Level{{Price: position.StopPrice - 0.1, Quantity: position.RemainingQuantity / 2}}
	drop.Asks = []Level{{Price: position.StopPrice + 0.1, Quantity: 100}}
	engine.UpdateBook("TESTUSDT", drop, now.Add(2*time.Second))

	account := engine.Snapshot(now.Add(2 * time.Second))
	if len(account.OpenPositions) != 1 {
		t.Fatalf("partial exit should keep residual position open: %+v", account.OpenPositions)
	}
	if account.OpenPositions[0].RemainingQuantity >= position.RemainingQuantity {
		t.Fatalf("remaining quantity did not decrease: before=%f after=%f", position.RemainingQuantity, account.OpenPositions[0].RemainingQuantity)
	}
	if account.ReservedMargin >= position.ReservedMargin {
		t.Fatalf("reserved margin was not reduced after partial exit: before=%f after=%f", position.ReservedMargin, account.ReservedMargin)
	}
}

func TestRejectsEntryWhenActualDepthMakesRiskExceedThreePercent(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	book := testBook(now)
	// Из-за тонкого первого уровня почти весь объём исполнился бы существенно дороже best Ask.
	book.Asks = []Level{{Price: 100, Quantity: 0.001}, {Price: 300, Quantity: 100}}
	event := engine.OpenLong(testSignal(now), book, now)
	if event.Type != "entry_rejected" {
		t.Fatalf("expected risk rejection, got %+v", event)
	}
	if len(engine.Snapshot(now).OpenPositions) != 0 {
		t.Fatal("position opened although depth-adjusted risk exceeded limit")
	}
}

func TestRejectsOrderBelowMinimumContractQuantityOrNotional(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	cfg := testConfig()
	engine := testEngine(t, cfg, now)
	engine.UpdateMarketData([]bybit.MarketData{{
		Instrument: bybit.Instrument{
			Symbol: "TESTUSDT", MinOrderQty: "1", QtyStep: "1",
			MinNotionalValue: "1000", TickSize: "0.01", MaxMktOrderQty: "1000",
			FundingInterval: 60,
		},
		Ticker: bybit.Ticker{MarkPrice: "100", FundingRate: "0.0001", NextFundingTime: now.Add(time.Minute)},
	}}, now)

	event := engine.OpenLong(testSignal(now), testBook(now), now)
	if event.Type != "entry_rejected" {
		t.Fatalf("expected contract minimum rejection, got %+v", event)
	}
}

func TestRejectsStaleBookOrMissingFundingData(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	stale := testBook(now.Add(-10 * time.Second))
	event := engine.OpenLong(testSignal(now), stale, now)
	if event.Type != "entry_rejected" {
		t.Fatalf("stale book should reject: %+v", event)
	}

	engine.UpdateMarketData([]bybit.MarketData{{
		Instrument: bybit.Instrument{
			Symbol: "TESTUSDT", MinOrderQty: "0.001", QtyStep: "0.001",
			MinNotionalValue: "5", TickSize: "0.01", MaxMktOrderQty: "1000",
			FundingInterval: 60,
		},
		Ticker: bybit.Ticker{FundingRate: "", NextFundingTime: now.Add(time.Minute)},
	}}, now)
	event = engine.OpenLong(testSignal(now), testBook(now), now)
	if event.Type != "entry_rejected" {
		t.Fatalf("missing funding should reject new entry: %+v", event)
	}
}

func TestOnlyOneOpenLongAndNoAveraging(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	openTestPosition(t, engine, now)

	second := testSignal(now.Add(time.Second))
	second.Time = now.Add(time.Second)
	event := engine.OpenLong(second, testBook(now.Add(time.Second)), now.Add(time.Second))
	if event.Type != "entry_rejected" {
		t.Fatalf("second position should be rejected: %+v", event)
	}
	if len(engine.Snapshot(now.Add(time.Second)).OpenPositions) != 1 {
		t.Fatal("second LONG was opened or position was averaged")
	}
}

func TestRejectsNonLongDirection(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	signal := testSignal(now)
	signal.Direction = "SHORT"
	event := engine.OpenLong(signal, testBook(now), now)
	if event.Type != "entry_rejected" {
		t.Fatalf("SHORT should be rejected: %+v", event)
	}
}

func TestPartialEntryIsReportedAndRemainsWithinRiskAndContractRules(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	book := testBook(now)
	book.Asks = []Level{{Price: 100, Quantity: 0.05}, {Price: 100.1, Quantity: 0.02}}
	event := engine.OpenLong(testSignal(now), book, now)
	if event.Type != "position_partially_opened" {
		t.Fatalf("partial entry not reported: %+v", event)
	}
	if event.Quantity > event.RequestedQuantity+1e-9 || event.RiskPercent > 3 {
		t.Fatalf("partial position violates risk or quantity bounds: %+v", event)
	}
}

func TestConfigRejectsUnsafeLimits(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxLeverage = 3
	if _, err := New(cfg); err == nil {
		t.Fatal("leverage above 2x must be rejected")
	}
	cfg = DefaultConfig()
	cfg.MaxMarginPercent = 60
	if _, err := New(cfg); err == nil {
		t.Fatal("margin allocation above 50% must be rejected")
	}
	cfg = DefaultConfig()
	cfg.MaxOpenPositions = 2
	if _, err := New(cfg); err == nil {
		t.Fatal("more than one open position must be rejected")
	}
}
func TestFundingIsSkippedWhenPublicMarkPriceIsStale(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	openTestPosition(t, engine, now)

	fundingAt := now.Add(time.Minute)
	book := testBook(fundingAt)
	book.Bids = []Level{{Price: 100, Quantity: 100}}
	book.Asks = []Level{{Price: 100.1, Quantity: 100}}
	engine.UpdateBook("TESTUSDT", book, fundingAt)

	if account := engine.Snapshot(fundingAt); account.NetFunding != 0 {
		t.Fatalf("funding should not use stale Mark Price, got %f", account.NetFunding)
	}
	foundSkipped := false
	for _, event := range engine.DrainEvents() {
		if event.Type == "funding_skipped" {
			foundSkipped = true
		}
	}
	if !foundSkipped {
		t.Fatal("expected funding_skipped event for stale Mark Price")
	}
}

func TestEntryRejectsFundingTimestampThatAlreadyPassed(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	engine := testEngine(t, testConfig(), now)
	engine.UpdateMarketData([]bybit.MarketData{{
		Instrument: bybit.Instrument{
			Symbol: "TESTUSDT", MinOrderQty: "0.001", QtyStep: "0.001",
			MinNotionalValue: "5", TickSize: "0.01", MaxMktOrderQty: "1000",
			FundingInterval: 60,
		},
		Ticker: bybit.Ticker{MarkPrice: "100", FundingRate: "0.0001", NextFundingTime: now.Add(-time.Second)},
	}}, now)
	event := engine.OpenLong(testSignal(now), testBook(now), now)
	if event.Type != "entry_rejected" {
		t.Fatalf("expired funding timestamp must reject new entry: %+v", event)
	}
}

