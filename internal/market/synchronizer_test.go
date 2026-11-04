package market

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
)

type fakeSource struct {
	instruments []bybit.Instrument
	tickers []bybit.Ticker
	instrumentErr error
	tickerErr error
}
func (f fakeSource) GetInstruments(context.Context) ([]bybit.Instrument, error) { return f.instruments, f.instrumentErr }
func (f fakeSource) GetTickers(context.Context) ([]bybit.Ticker, error) { return f.tickers, f.tickerErr }

type fakeRepository struct {
	instruments []storage.Instrument
	calls int
	err error
}
func (f *fakeRepository) SyncInstrumentCatalog(_ context.Context, instruments []storage.Instrument) error {
	f.calls++
	f.instruments = instruments
	return f.err
}

func TestSynchronizerMapsTickerSnapshot(t *testing.T) {
	repository := &fakeRepository{}
	source := fakeSource{
		instruments: []bybit.Instrument{
			{Symbol:"BTCUSDT",BaseCoin:"BTC",QuoteCoin:"USDT",SettleCoin:"USDT",ContractType:"LinearPerpetual",Status:"Trading"},
			{Symbol:"ETHUSDT",BaseCoin:"ETH",QuoteCoin:"USDT",SettleCoin:"USDT",ContractType:"LinearPerpetual",Status:"Trading"},
		},
		tickers: []bybit.Ticker{
			{Symbol:"BTCUSDT",LastPrice:"65000",Turnover24h:"1200000000"},
			{Symbol:"ETHUSDT",LastPrice:"3000",Turnover24h:"400000000"},
		},
	}
	syncer, err := NewSynchronizer(source, repository)
	if err != nil { t.Fatal(err) }
	fixed := time.Date(2026,11,4,12,0,0,0,time.UTC)
	syncer.now = func() time.Time { return fixed }
	stats, err := syncer.Sync(context.Background())
	if err != nil { t.Fatalf("Sync() error = %v", err) }
	if stats.Instruments != 2 || stats.WithTicker != 2 || repository.calls != 1 || len(repository.instruments) != 2 {
		t.Fatalf("stats=%+v repository=%+v", stats, repository)
	}
	first := repository.instruments[0]
	if first.Symbol != "BTCUSDT" || first.LastPrice != "65000" || first.Turnover24h != "1200000000" ||
		first.TickerUpdatedAt == nil || !first.TickerUpdatedAt.Equal(fixed) {
		t.Fatalf("incorrect ticker mapping: %+v", first)
	}
}

func TestSynchronizerPreservesCatalogOnIncompleteSnapshot(t *testing.T) {
	tests := []struct { name string; source fakeSource }{
		{name:"empty instruments",source:fakeSource{tickers:[]bybit.Ticker{{Symbol:"BTCUSDT"}}}},
		{name:"empty tickers",source:fakeSource{instruments:[]bybit.Instrument{{Symbol:"BTCUSDT"}}}},
		{name:"instrument API error",source:fakeSource{instrumentErr:errors.New("offline")}},
		{name:"ticker API error",source:fakeSource{instruments:[]bybit.Instrument{{Symbol:"BTCUSDT"}},tickerErr:errors.New("offline")}},
		{name:"missing ticker",source:fakeSource{instruments:[]bybit.Instrument{{Symbol:"BTCUSDT"}},tickers:[]bybit.Ticker{{Symbol:"ETHUSDT"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := &fakeRepository{}
			syncer, err := NewSynchronizer(test.source, repository)
			if err != nil { t.Fatal(err) }
			if _, err := syncer.Sync(context.Background()); err == nil { t.Fatal("expected incomplete snapshot error") }
			if repository.calls != 0 { t.Fatalf("database sync called %d times", repository.calls) }
		})
	}
}

func TestSynchronizerPropagatesRepositoryError(t *testing.T) {
	repository := &fakeRepository{err:errors.New("database unavailable")}
	source := fakeSource{
		instruments:[]bybit.Instrument{{Symbol:"BTCUSDT"}},
		tickers:[]bybit.Ticker{{Symbol:"BTCUSDT",LastPrice:"65000",Turnover24h:"1000"}},
	}
	syncer, err := NewSynchronizer(source, repository)
	if err != nil { t.Fatal(err) }
	if _, err := syncer.Sync(context.Background()); err == nil { t.Fatal("expected database error") }
	if repository.calls != 1 { t.Fatalf("database sync calls = %d", repository.calls) }
}
