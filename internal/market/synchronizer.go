package market

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
)

type MarketDataSource interface {
	GetInstruments(context.Context) ([]bybit.Instrument, error)
	GetTickers(context.Context) ([]bybit.Ticker, error)
}

type InstrumentRepository interface {
	SyncInstrumentCatalog(context.Context, []storage.Instrument) error
}

type SyncStats struct {
	Instruments int
	WithTicker int
}

type Synchronizer struct {
	source MarketDataSource
	repository InstrumentRepository
	now func() time.Time
}

func NewSynchronizer(source MarketDataSource, repository InstrumentRepository) (*Synchronizer, error) {
	if source == nil {
		return nil, errors.New("market data source is required")
	}
	if repository == nil {
		return nil, errors.New("instrument repository is required")
	}
	return &Synchronizer{source: source, repository: repository, now: time.Now}, nil
}

// Sync writes a snapshot only after both API responses are complete and ticker coverage is total.
// This prevents a partial API response from deactivating or distorting the saved market catalogue.
func (s *Synchronizer) Sync(ctx context.Context) (SyncStats, error) {
	instruments, err := s.source.GetInstruments(ctx)
	if err != nil {
		return SyncStats{}, fmt.Errorf("load Bybit instrument catalogue: %w", err)
	}
	if len(instruments) == 0 {
		return SyncStats{}, errors.New("Bybit instrument catalogue is empty; preserving previous database snapshot")
	}
	tickers, err := s.source.GetTickers(ctx)
	if err != nil {
		return SyncStats{}, fmt.Errorf("load Bybit ticker snapshot: %w", err)
	}
	if len(tickers) == 0 {
		return SyncStats{}, errors.New("Bybit ticker snapshot is empty; preserving previous database snapshot")
	}
	tickerBySymbol := make(map[string]bybit.Ticker, len(tickers))
	for _, ticker := range tickers {
		symbol := strings.ToUpper(strings.TrimSpace(ticker.Symbol))
		if symbol == "" {
			continue
		}
		if _, exists := tickerBySymbol[symbol]; exists {
			return SyncStats{}, fmt.Errorf("duplicate Bybit ticker %s; preserving previous database snapshot", symbol)
		}
		tickerBySymbol[symbol] = ticker
	}

	updatedAt := s.now().UTC()
	snapshot := make([]storage.Instrument, 0, len(instruments))
	for _, item := range instruments {
		symbol := strings.ToUpper(strings.TrimSpace(item.Symbol))
		if symbol == "" {
			return SyncStats{}, errors.New("Bybit catalogue contains an empty symbol; preserving previous database snapshot")
		}
		ticker, ok := tickerBySymbol[symbol]
		if !ok {
			return SyncStats{}, fmt.Errorf("Bybit ticker snapshot is missing %s; preserving previous database snapshot", symbol)
		}
		snapshot = append(snapshot, storage.Instrument{
			Symbol: symbol, BaseCoin: item.BaseCoin, QuoteCoin: item.QuoteCoin,
			SettleCoin: item.SettleCoin, ContractType: item.ContractType, Status: item.Status,
			Turnover24h: ticker.Turnover24h, LastPrice: ticker.LastPrice, TickerUpdatedAt: &updatedAt,
		})
	}
	if err := s.repository.SyncInstrumentCatalog(ctx, snapshot); err != nil {
		return SyncStats{}, fmt.Errorf("save Bybit instrument catalogue: %w", err)
	}
	return SyncStats{Instruments: len(snapshot), WithTicker: len(snapshot)}, nil
}
