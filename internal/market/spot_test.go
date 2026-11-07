package market

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
)

type fakeSpotSource struct {
	instruments   []bybit.SpotInstrument
	tickers       []bybit.SpotTicker
	instrumentErr error
	tickerErr     error
}

func (f fakeSpotSource) GetSpotInstruments(context.Context) ([]bybit.SpotInstrument, error) {
	return f.instruments, f.instrumentErr
}

func (f fakeSpotSource) GetSpotTickers(context.Context) ([]bybit.SpotTicker, error) {
	return f.tickers, f.tickerErr
}

type fakeSpotRepository struct {
	instruments []storage.SpotInstrument
	limit       float64
	hold        time.Duration
	now         time.Time
	watchlist   []storage.SpotWatchlistEntry
	calls       int
	err         error
}

func (f *fakeSpotRepository) SyncSpotCatalog(_ context.Context, instruments []storage.SpotInstrument, limit float64, hold time.Duration, now time.Time) error {
	f.calls++
	f.instruments, f.limit, f.hold, f.now = instruments, limit, hold, now
	return f.err
}

func (f *fakeSpotRepository) ListSpotWatchlist(context.Context) ([]storage.SpotWatchlistEntry, error) {
	return f.watchlist, nil
}

func TestSpotSynchronizerMapsCompleteUSDTUniverse(t *testing.T) {
	repository := &fakeSpotRepository{watchlist: []storage.SpotWatchlistEntry{
		{Symbol: "AAAUSDT", State: storage.SpotWatchlistWatching},
		{Symbol: "BBBUSDT", State: storage.SpotWatchlistOverLimitHold},
	}}
	source := fakeSpotSource{
		instruments: []bybit.SpotInstrument{
			{Symbol: "AAAUSDT", BaseCoin: "AAA", QuoteCoin: "USDT", Status: "Trading"},
			{Symbol: "BBBUSDT", BaseCoin: "BBB", QuoteCoin: "USDT", Status: "Trading"},
		},
		tickers: []bybit.SpotTicker{
			{Symbol: "AAAUSDT", LastPrice: "1.25", Turnover24h: "999999.99"},
			{Symbol: "BBBUSDT", LastPrice: "2.5", Turnover24h: "1000000.01"},
		},
	}
	syncer, err := NewSpotSynchronizer(source, repository)
	if err != nil {
		t.Fatal(err)
	}
	fixed := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	syncer.now = func() time.Time { return fixed }

	stats, err := syncer.Sync(context.Background())
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if stats.Instruments != 2 || stats.Watchlist != 2 || stats.OverLimitHold != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if repository.calls != 1 || len(repository.instruments) != 2 {
		t.Fatalf("repository calls=%d instruments=%+v", repository.calls, repository.instruments)
	}
	if repository.limit != DefaultSpotTurnoverLimit || repository.hold != 24*time.Hour || !repository.now.Equal(fixed) {
		t.Fatalf("unexpected Watchlist policy: limit=%v hold=%v now=%v", repository.limit, repository.hold, repository.now)
	}
	if repository.instruments[0].Turnover24h != "999999.99" ||
		repository.instruments[1].Turnover24h != "1000000.01" ||
		!repository.instruments[0].TickerUpdatedAt.Equal(fixed) {
		t.Fatalf("ticker snapshot was not mapped correctly: %+v", repository.instruments)
	}
}

func TestSpotSynchronizerPreservesPreviousSnapshotOnMissingTicker(t *testing.T) {
	repository := &fakeSpotRepository{}
	source := fakeSpotSource{
		instruments: []bybit.SpotInstrument{{Symbol: "AAAUSDT", BaseCoin: "AAA", QuoteCoin: "USDT", Status: "Trading"}},
		tickers:     []bybit.SpotTicker{{Symbol: "BBBUSDT", LastPrice: "1", Turnover24h: "10"}},
	}
	syncer, err := NewSpotSynchronizer(source, repository)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := syncer.Sync(context.Background()); err == nil {
		t.Fatal("expected missing ticker error")
	}
	if repository.calls != 0 {
		t.Fatalf("repository called %d times for incomplete snapshot", repository.calls)
	}
}

func TestNewSpotSynchronizerRejectsNilDependencies(t *testing.T) {
	if _, err := NewSpotSynchronizer(nil, &fakeSpotRepository{}); err == nil {
		t.Fatal("nil source must fail")
	}
	if _, err := NewSpotSynchronizer(fakeSpotSource{}, nil); err == nil {
		t.Fatal("nil repository must fail")
	}
}

func TestSpotSynchronizerPropagatesSourceAndStorageErrors(t *testing.T) {
	for _, source := range []fakeSpotSource{
		{instrumentErr: errors.New("offline")},
		{instruments: []bybit.SpotInstrument{{Symbol: "AAAUSDT", BaseCoin: "AAA", QuoteCoin: "USDT", Status: "Trading"}}, tickerErr: errors.New("offline")},
	} {
		syncer, _ := NewSpotSynchronizer(source, &fakeSpotRepository{})
		if _, err := syncer.Sync(context.Background()); err == nil {
			t.Fatal("expected source error")
		}
	}
	repository := &fakeSpotRepository{err: errors.New("database unavailable")}
	source := fakeSpotSource{
		instruments: []bybit.SpotInstrument{{Symbol: "AAAUSDT", BaseCoin: "AAA", QuoteCoin: "USDT", Status: "Trading"}},
		tickers:     []bybit.SpotTicker{{Symbol: "AAAUSDT", LastPrice: "1", Turnover24h: "10"}},
	}
	syncer, _ := NewSpotSynchronizer(source, repository)
	if _, err := syncer.Sync(context.Background()); err == nil {
		t.Fatal("expected storage error")
	}
}

func TestSpotSynchronizerRejectsInvalidPriceAndTurnover(t *testing.T) {
	for _, ticker := range []bybit.SpotTicker{
		{Symbol: "AAAUSDT", LastPrice: "0", Turnover24h: "10"},
		{Symbol: "AAAUSDT", LastPrice: "1", Turnover24h: "-1"},
	} {
		repository := &fakeSpotRepository{}
		source := fakeSpotSource{
			instruments: []bybit.SpotInstrument{{Symbol: "AAAUSDT", BaseCoin: "AAA", QuoteCoin: "USDT", Status: "Trading"}},
			tickers:     []bybit.SpotTicker{ticker},
		}
		syncer, _ := NewSpotSynchronizer(source, repository)
		if _, err := syncer.Sync(context.Background()); err == nil || repository.calls != 0 {
			t.Fatalf("ticker %+v should be rejected before persistence", ticker)
		}
	}
}
