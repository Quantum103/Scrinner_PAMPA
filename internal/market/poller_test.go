package market

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
)

type fakeMarketRESTClient struct {
	instruments []bybit.Instrument
	tickers []bybit.Ticker
	instrumentCalls atomic.Int32
	tickerCalls atomic.Int32
}

func (c *fakeMarketRESTClient) FetchUSDTPerpetualInstruments(context.Context) ([]bybit.Instrument, error) {
	c.instrumentCalls.Add(1)
	return append([]bybit.Instrument(nil), c.instruments...), nil
}

func (c *fakeMarketRESTClient) FetchLinearTickers(context.Context) ([]bybit.Ticker, error) {
	c.tickerCalls.Add(1)
	return append([]bybit.Ticker(nil), c.tickers...), nil
}

type fakePollerSubscriber struct {
	mu sync.Mutex
	symbols []string
	marketData []bybit.MarketData
	updated chan struct{}
	openPositions []string
}

func (s *fakePollerSubscriber) SetSymbols(symbols []string) {
	s.mu.Lock()
	s.symbols = append([]string(nil), symbols...)
	s.mu.Unlock()
}

func (s *fakePollerSubscriber) UpdateMarketData(items []bybit.MarketData) {
	s.mu.Lock()
	s.marketData = append([]bybit.MarketData(nil), items...)
	s.mu.Unlock()
	select { case s.updated <- struct{}{}: default: }
}

func (s *fakePollerSubscriber) ReportMarketDataIssue(string) {}
func (s *fakePollerSubscriber) OpenPositionSymbols() []string {
	return append([]string(nil), s.openPositions...)
}

func (s *fakePollerSubscriber) snapshot() ([]string, []bybit.MarketData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.symbols...), append([]bybit.MarketData(nil), s.marketData...)
}

func TestPollerPublishesFullMarketWithoutAlphabeticalTruncation(t *testing.T) {
	instruments := []bybit.Instrument{
		{Symbol: "BTCUSDT", MinOrderQty: "0.001"},
		{Symbol: "ETHUSDT", MinOrderQty: "0.01"},
	}
	for i := 0; i < 130; i++ {
		symbol := fmt.Sprintf("COIN%03dUSDT", i)
		instruments = append(instruments, bybit.Instrument{Symbol: symbol, MinOrderQty: "1"})
	}
	tickers := make([]bybit.Ticker, 0, len(instruments))
	for i, instrument := range instruments {
		tickers = append(tickers, bybit.Ticker{
			Symbol: instrument.Symbol, LastPrice: fmt.Sprintf("%d", i+1),
			Turnover24h: "1000", FundingRate: "0.0001",
			OpenInterest: "10", OpenInterestValue: "100",
		})
	}
	client := &fakeMarketRESTClient{instruments: instruments, tickers: tickers}
	subscriber := &fakePollerSubscriber{updated: make(chan struct{}, 1), openPositions: []string{"BTCUSDT"}}
	monitor := NewMonitor()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	poller := NewPoller(client, 10*time.Millisecond, time.Hour, []string{"BTCUSDT", "ETHUSDT"}, subscriber, monitor, logger)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); poller.Run(ctx) }()
	select {
	case <-subscriber.updated:
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("Poller не передал первый полный снимок рынка")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Poller не остановился после отмены контекста")
	}

	allSymbols, paperSnapshot := subscriber.snapshot()
	marketSnapshot := monitor.Snapshot()
	if len(paperSnapshot) != len(instruments) {
		t.Fatalf("Paper Trading получил %d инструментов, ожидалось %d", len(paperSnapshot), len(instruments))
	}
	if len(marketSnapshot.Items) != 130 {
		t.Fatalf("Market Monitor получил %d инструментов, ожидалось 130 без BTC/ETH", len(marketSnapshot.Items))
	}
	if marketSnapshot.UpdatedAt.IsZero() { t.Fatal("Market Monitor не сохранил время снимка") }
	if len(allSymbols) != 131 {
		t.Fatalf("подробный список содержит %d символов, ожидалось 130 доступных + 1 открытая позиция", len(allSymbols))
	}
	seen := make(map[string]bool, len(allSymbols))
	for _, symbol := range allSymbols { seen[symbol] = true }
	if !seen["BTCUSDT"] { t.Fatal("открытая позиция BTCUSDT потеряла подписку, несмотря на исключение из мониторинга") }
	if seen["ETHUSDT"] { t.Fatal("ETHUSDT не должен попадать в мониторинг и подробные подписки без открытой позиции") }
	if !seen["COIN129USDT"] { t.Fatal("последний контракт списка потерян — вероятно, список был обрезан") }
	if paperSnapshot[0].Instrument.Symbol != "BTCUSDT" || paperSnapshot[0].Instrument.MinOrderQty != "0.001" {
		t.Fatalf("спецификации исключённого инструмента не переданы Paper Trading: %+v", paperSnapshot[0])
	}
	if paperSnapshot[0].Ticker.FundingRate != "0.0001" {
		t.Fatalf("funding исключённого инструмента не передан Paper Trading: %+v", paperSnapshot[0].Ticker)
	}
	if client.tickerCalls.Load() == 0 { t.Fatal("Poller не запросил полный снимок тикеров") }
}

func TestMonitorReplacesSnapshotAndReturnsDefensiveCopy(t *testing.T) {
	monitor := NewMonitor()
	first := []bybit.MarketData{{Instrument: bybit.Instrument{Symbol: "AAAUSDT"}}}
	monitor.UpdateMarketSnapshot(first, time.Unix(1, 0))
	first[0].Instrument.Symbol = "MUTATED"
	snapshot := monitor.Snapshot()
	if len(snapshot.Items) != 1 || snapshot.Items[0].Instrument.Symbol != "AAAUSDT" {
		t.Fatalf("Monitor сохранил внешнюю мутацию: %+v", snapshot.Items)
	}
	snapshot.Items[0].Instrument.Symbol = "ALSO_MUTATED"
	if monitor.Snapshot().Items[0].Instrument.Symbol != "AAAUSDT" {
		t.Fatal("Snapshot() вернул внутренний изменяемый срез")
	}
	monitor.UpdateMarketSnapshot([]bybit.MarketData{{Instrument: bybit.Instrument{Symbol: "BBBUSDT"}}}, time.Unix(2, 0))
	if monitor.Count() != 1 || monitor.Snapshot().Items[0].Instrument.Symbol != "BBBUSDT" {
		t.Fatal("новый рыночный снимок не заменил предыдущий")
	}
}

func TestExcludeInstrumentsPreservesInputOrderAndDoesNotApplyLimit(t *testing.T) {
	instruments := make([]bybit.Instrument, 150)
	for i := range instruments { instruments[i] = bybit.Instrument{Symbol: fmt.Sprintf("COIN%03dUSDT", i)} }
	instruments[0].Symbol = "BTCUSDT"
	filtered := excludeInstruments(instruments, symbolSet([]string{"btcusdt"}))
	if len(filtered) != 149 { t.Fatalf("после исключения осталось %d инструментов, ожидалось 149", len(filtered)) }
	if filtered[0].Symbol != "COIN001USDT" || filtered[len(filtered)-1].Symbol != "COIN149USDT" {
		t.Fatalf("порядок входного списка изменён: first=%s last=%s", filtered[0].Symbol, filtered[len(filtered)-1].Symbol)
	}
}
