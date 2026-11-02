package paper

import (
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
)

func openPersistentTestEngine(t *testing.T, path string, now time.Time) (*Engine, *SQLiteRepository) {
	t.Helper()
	repository, err := OpenSQLiteRepository(path, 5*time.Second)
	if err != nil {
		t.Fatalf("OpenSQLiteRepository() error = %v", err)
	}
	engine, err := NewWithRepository(testConfig(), repository)
	if err != nil {
		repository.Close()
		t.Fatalf("NewWithRepository() error = %v", err)
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
	return engine, repository
}

func TestSQLiteRestoresAccountAndOpenPositionAfterRestart(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	path := filepath.Join(t.TempDir(), "volume-trader.db")
	engine, repository := openPersistentTestEngine(t, path, now)
	openTestPosition(t, engine, now)

	rallyAt := now.Add(time.Second)
	rally := flowBook(rallyAt, 105)
	engine.UpdateBook("TESTUSDT", rally, rallyAt)
	before := engine.Snapshot(rallyAt)
	if len(before.OpenPositions) != 1 || before.OpenPositions[0].MFEUSDT <= 0 {
		t.Fatalf("position metrics were not updated before restart: %+v", before.OpenPositions)
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("close SQLite before restart: %v", err)
	}

	reopenedRepository, err := OpenSQLiteRepository(path, 5*time.Second)
	if err != nil {
		t.Fatalf("reopen SQLite: %v", err)
	}
	defer reopenedRepository.Close()
	restored, err := NewWithRepository(testConfig(), reopenedRepository)
	if err != nil {
		t.Fatalf("restore engine: %v", err)
	}

	after := restored.Snapshot(rallyAt)
	if math.Abs(after.Balance-before.Balance) > 1e-9 {
		t.Fatalf("restored balance = %.10f, want %.10f", after.Balance, before.Balance)
	}
	if len(after.OpenPositions) != 1 {
		t.Fatalf("restored open positions = %d, want 1", len(after.OpenPositions))
	}
	position := after.OpenPositions[0]
	if position.TradeID == "" || position.SignalID == "" || position.EntryReason == "" ||
		position.EntrySignal.Symbol != "TESTUSDT" || position.StopPrice != before.OpenPositions[0].StopPrice {
		t.Fatalf("restored position metadata is incomplete: %+v", position)
	}
	if position.MFEUSDT <= 0 || position.HighWaterPrice != 105 || position.RemainingQuantity != before.OpenPositions[0].RemainingQuantity {
		t.Fatalf("MFE/high-water/quantity were not restored: %+v", position)
	}

	// Продолжение сопровождения на новом свежем стакане после восстановления.
	later := rallyAt.Add(time.Second)
	pullback := flowBook(later, 104)
	restored.UpdateBook("TESTUSDT", pullback, later)
	afterUpdate := restored.Snapshot(later)
	if len(afterUpdate.OpenPositions) != 1 || afterUpdate.OpenPositions[0].StopPrice != position.StopPrice {
		t.Fatalf("restored position was not managed by fresh market data: %+v", afterUpdate.OpenPositions)
	}
	if err := reopenedRepository.IntegrityCheck(t.Context()); err != nil {
		t.Fatalf("integrity_check after restart: %v", err)
	}
}

func TestSQLiteCloseIsSavedConsistentlyAndRestored(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	path := filepath.Join(t.TempDir(), "volume-trader.db")
	engine, repository := openPersistentTestEngine(t, path, now)
	entry := openTestPosition(t, engine, now)
	closeAt := now.Add(time.Second)
	book := flowBook(closeAt, 102)
	closeEvent := engine.CloseLong("TESTUSDT", book, closeAt, "persistent test close")
	if closeEvent.Type != "position_closed" {
		t.Fatalf("CloseLong() = %+v, want position_closed", closeEvent)
	}
	committed := engine.Snapshot(closeAt)
	if len(committed.OpenPositions) != 0 || len(committed.ClosedTrades) != 1 {
		t.Fatalf("in-memory close was not complete: %+v", committed)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedRepository, err := OpenSQLiteRepository(path, 5*time.Second)
	if err != nil {
		t.Fatalf("reopen SQLite: %v", err)
	}
	defer reopenedRepository.Close()
	restored, err := NewWithRepository(testConfig(), reopenedRepository)
	if err != nil {
		t.Fatalf("restore engine: %v", err)
	}
	account := restored.Snapshot(closeAt)
	if len(account.OpenPositions) != 0 || len(account.ClosedTrades) != 1 {
		t.Fatalf("SQLite close was not committed atomically: %+v", account)
	}
	trade := account.ClosedTrades[0]
	if trade.ExitPrice <= 0 || trade.ExitNotional <= 0 || trade.ClosedQuantity <= 0 ||
		trade.EntryFee <= 0 || trade.ExitFee <= 0 || trade.EntrySlippageUSDT <= 0 ||
		trade.ExitSlippageUSDT <= 0 || trade.CloseReason != "persistent test close" {
		t.Fatalf("persisted trade fields are incomplete: %+v", trade)
	}
	if math.Abs(account.Balance-committed.Balance) > 1e-9 ||
		math.Abs(trade.RealizedPnL-closeEvent.NetPnL) > 1e-9 ||
		math.Abs(trade.EntryFee-entry.Fee) > 1e-9 {
		t.Fatalf("persisted account/trade does not match committed close: account=%+v trade=%+v event=%+v", account, trade, closeEvent)
	}
	if account.ReservedMargin != 0 || account.Statistics.TradeCount != 1 ||
		account.Statistics.Mode != "Paper Trading" {
		t.Fatalf("restored account statistics or margin are incorrect: %+v", account)
	}
	if err := reopenedRepository.IntegrityCheck(t.Context()); err != nil {
		t.Fatalf("integrity_check after close: %v", err)
	}
}

func TestSQLiteDoesNotOpenDuplicateTradeForProcessedSignal(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	path := filepath.Join(t.TempDir(), "volume-trader.db")
	engine, repository := openPersistentTestEngine(t, path, now)
	signal := testSignal(now)
	open := engine.OpenLong(signal, testBook(now), now)
	if open.Type != "position_opened" && open.Type != "position_partially_opened" {
		t.Fatalf("initial entry was rejected: %+v", open)
	}
	engine.CloseLong("TESTUSDT", flowBook(now.Add(time.Second), 102), now.Add(time.Second), "complete first virtual trade")
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedRepository, err := OpenSQLiteRepository(path, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedRepository.Close()
	restored, err := NewWithRepository(testConfig(), reopenedRepository)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := restored.OpenLong(signal, testBook(now), now)
	if duplicate.Type != "entry_rejected" {
		t.Fatalf("replayed signal was allowed to open a second trade: %+v", duplicate)
	}
	account := restored.Snapshot(now)
	if len(account.OpenPositions) != 0 || len(account.ClosedTrades) != 1 {
		t.Fatalf("duplicate replay changed trade history: %+v", account)
	}
}

func TestPaperStatisticsAndMaximumDrawdown(t *testing.T) {
	trades := []Position{
		{
			TradeID: "one", Symbol: "AAAUSDT", RealizedPnL: 20,
			InitialRiskUSDT: 10, EntryFee: 0.5, ExitFee: 0.5,
			EntrySlippageUSDT: 0.1, ExitSlippageUSDT: 0.2,
			FundingPaid: 0.25, CloseReason: "Order Flow Exit",
		},
		{
			TradeID: "two", Symbol: "BBBUSDT", RealizedPnL: -10,
			InitialRiskUSDT: 5, EntryFee: 0.4, ExitFee: 0.4,
			EntrySlippageUSDT: 0.15, ExitSlippageUSDT: 0.25,
			FundingPaid: -0.1, CloseReason: "Trailing Stop",
		},
	}
	start := time.Unix(1_800_000_000, 0).UTC()
	equity := []EquityPoint{
		{At: start, Equity: 1000, Balance: 1000},
		{At: start.Add(time.Second), Equity: 1100, Balance: 1100},
		{At: start.Add(2 * time.Second), Equity: 990, Balance: 990},
		{At: start.Add(3 * time.Second), Equity: 1050, Balance: 1050},
	}
	stats := calculateStatistics(trades, equity, 1.8, -0.15)
	if stats.Mode != "Paper Trading" || stats.TradeCount != 2 ||
		stats.ProfitableTrades != 1 || stats.LosingTrades != 1 ||
		math.Abs(stats.WinRatePercent-50) > 1e-9 || math.Abs(stats.NetPnL-10) > 1e-9 {
		t.Fatalf("trade summary is wrong: %+v", stats)
	}
	if math.Abs(stats.AverageProfit-20) > 1e-9 || math.Abs(stats.AverageLoss+10) > 1e-9 ||
		stats.ProfitFactor == nil || math.Abs(*stats.ProfitFactor-2) > 1e-9 {
		t.Fatalf("average result or Profit Factor is wrong: %+v", stats)
	}
	if math.Abs(stats.MaxDrawdownUSDT-110) > 1e-9 || math.Abs(stats.MaxDrawdownPercent-10) > 1e-9 {
		t.Fatalf("maximum drawdown = %.8f / %.8f%%, want 110 / 10%%", stats.MaxDrawdownUSDT, stats.MaxDrawdownPercent)
	}
	if math.Abs(stats.AverageRealizedRR) > 1e-9 || math.Abs(stats.MaxRealizedRR-2) > 1e-9 {
		t.Fatalf("realized RR statistics are incorrect: %+v", stats)
	}
	if math.Abs(stats.TotalFees-1.8) > 1e-9 || math.Abs(stats.TotalSlippageUSDT-0.7) > 1e-9 ||
		stats.ByCloseReason["Order Flow Exit"] != 1 || stats.ByCloseReason["Trailing Stop"] != 1 {
		t.Fatalf("cost/reason statistics are incomplete: %+v", stats)
	}
	if stats.BySymbol["AAAUSDT"].TradeCount != 1 || stats.BySymbol["BBBUSDT"].LosingTrades != 1 {
		t.Fatalf("per-symbol breakdown is wrong: %+v", stats.BySymbol)
	}
}

func TestProfitFactorIsExplicitlyInfiniteWithoutLosses(t *testing.T) {
	stats := calculateStatistics([]Position{{RealizedPnL: 1}}, []EquityPoint{{Equity: 10}}, 0, 0)
	if !stats.InfiniteProfitFactor || stats.ProfitFactor != nil {
		t.Fatalf("Profit Factor without losing trades must be marked infinite: %+v", stats)
	}
}
