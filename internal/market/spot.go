package market

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
)

const (
	DefaultSpotTurnoverLimit = 1_000_000.0
	DefaultSpotOverLimitHold = 24 * time.Hour
)

type SpotMarketDataSource interface {
	GetSpotInstruments(context.Context) ([]bybit.SpotInstrument, error)
	GetSpotTickers(context.Context) ([]bybit.SpotTicker, error)
}

type SpotInstrumentRepository interface {
	SyncSpotCatalog(context.Context, []storage.SpotInstrument, float64, time.Duration, time.Time) error
	ListSpotWatchlist(context.Context) ([]storage.SpotWatchlistEntry, error)
}

type SpotSyncStats struct {
	Instruments   int
	Watchlist     int
	OverLimitHold int
}

type SpotSynchronizer struct {
	source        SpotMarketDataSource
	repository    SpotInstrumentRepository
	now           func() time.Time
	turnoverLimit float64
	overLimitHold time.Duration
}

func NewSpotSynchronizer(source SpotMarketDataSource, repository SpotInstrumentRepository) (*SpotSynchronizer, error) {
	if source == nil {
		return nil, errors.New("Bybit Spot market data source is required")
	}
	if repository == nil {
		return nil, errors.New("Spot instrument repository is required")
	}
	return &SpotSynchronizer{
		source: source, repository: repository, now: time.Now,
		turnoverLimit: DefaultSpotTurnoverLimit, overLimitHold: DefaultSpotOverLimitHold,
	}, nil
}

// Sync obtains a complete Spot catalogue and ticker snapshot before writing.
// A missing ticker is treated as an incomplete snapshot, preserving the
// previous database catalogue and Watchlist.
func (s *SpotSynchronizer) Sync(ctx context.Context) (SpotSyncStats, error) {
	instruments, err := s.source.GetSpotInstruments(ctx)
	if err != nil {
		return SpotSyncStats{}, fmt.Errorf("load Bybit Spot catalogue: %w", err)
	}
	if len(instruments) == 0 {
		return SpotSyncStats{}, errors.New("Bybit Spot catalogue is empty; preserving previous snapshot")
	}
	tickers, err := s.source.GetSpotTickers(ctx)
	if err != nil {
		return SpotSyncStats{}, fmt.Errorf("load Bybit Spot ticker snapshot: %w", err)
	}
	if len(tickers) == 0 {
		return SpotSyncStats{}, errors.New("Bybit Spot ticker snapshot is empty; preserving previous snapshot")
	}

	tickerBySymbol := make(map[string]bybit.SpotTicker, len(tickers))
	for _, ticker := range tickers {
		symbol := strings.ToUpper(strings.TrimSpace(ticker.Symbol))
		if symbol == "" {
			return SpotSyncStats{}, errors.New("Bybit Spot ticker snapshot contains an empty symbol")
		}
		if _, exists := tickerBySymbol[symbol]; exists {
			return SpotSyncStats{}, fmt.Errorf("duplicate Bybit Spot ticker %s; preserving previous snapshot", symbol)
		}
		tickerBySymbol[symbol] = ticker
	}

	updatedAt := s.now().UTC()
	snapshot := make([]storage.SpotInstrument, 0, len(instruments))
	seen := make(map[string]struct{}, len(instruments))
	for _, item := range instruments {
		symbol := strings.ToUpper(strings.TrimSpace(item.Symbol))
		baseCoin := strings.ToUpper(strings.TrimSpace(item.BaseCoin))
		quoteCoin := strings.ToUpper(strings.TrimSpace(item.QuoteCoin))
		if symbol == "" || baseCoin == "" || quoteCoin != "USDT" || item.Status != "Trading" {
			return SpotSyncStats{}, fmt.Errorf("invalid active Bybit Spot USDT instrument %+v; preserving previous snapshot", item)
		}
		if _, exists := seen[symbol]; exists {
			return SpotSyncStats{}, fmt.Errorf("duplicate Bybit Spot instrument %s; preserving previous snapshot", symbol)
		}
		seen[symbol] = struct{}{}
		ticker, ok := tickerBySymbol[symbol]
		if !ok {
			return SpotSyncStats{}, fmt.Errorf("Bybit Spot ticker snapshot is missing %s; preserving previous snapshot", symbol)
		}
		lastPrice, priceErr := strconv.ParseFloat(strings.TrimSpace(ticker.LastPrice), 64)
		turnover, turnoverErr := strconv.ParseFloat(strings.TrimSpace(ticker.Turnover24h), 64)
		if priceErr != nil || math.IsNaN(lastPrice) || math.IsInf(lastPrice, 0) || lastPrice <= 0 {
			return SpotSyncStats{}, fmt.Errorf("invalid last price for Bybit Spot %s", symbol)
		}
		if turnoverErr != nil || math.IsNaN(turnover) || math.IsInf(turnover, 0) || turnover < 0 {
			return SpotSyncStats{}, fmt.Errorf("invalid 24h turnover for Bybit Spot %s", symbol)
		}
		snapshot = append(snapshot, storage.SpotInstrument{
			Symbol: symbol, BaseCoin: baseCoin, QuoteCoin: quoteCoin, Status: "Trading",
			Turnover24h: strings.TrimSpace(ticker.Turnover24h),
			LastPrice:   strings.TrimSpace(ticker.LastPrice), TickerUpdatedAt: updatedAt,
		})
	}
	if len(snapshot) == 0 {
		return SpotSyncStats{}, errors.New("no active Bybit Spot USDT pairs found; preserving previous snapshot")
	}

	if err := s.repository.SyncSpotCatalog(ctx, snapshot, s.turnoverLimit, s.overLimitHold, updatedAt); err != nil {
		return SpotSyncStats{}, fmt.Errorf("persist Bybit Spot catalogue and Watchlist: %w", err)
	}
	watchlist, err := s.repository.ListSpotWatchlist(ctx)
	if err != nil {
		return SpotSyncStats{}, fmt.Errorf("read Bybit Spot Watchlist: %w", err)
	}
	stats := SpotSyncStats{Instruments: len(snapshot), Watchlist: len(watchlist)}
	for _, item := range watchlist {
		if item.State == storage.SpotWatchlistOverLimitHold {
			stats.OverLimitHold++
		}
	}
	return stats, nil
}
