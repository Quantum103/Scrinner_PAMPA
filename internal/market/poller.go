package market

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
)

type Poller struct {
	client     *bybit.Client
	interval   time.Duration
	maxSymbols int
	logger     *slog.Logger
}

func NewPoller(client *bybit.Client, interval time.Duration, maxSymbols int, logger *slog.Logger) *Poller {
	return &Poller{
		client:     client,
		interval:   interval,
		maxSymbols: maxSymbols,
		logger:     logger,
	}
}

func (p *Poller) Run(ctx context.Context) {
	p.fetchAndLog(ctx)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.fetchAndLog(ctx)
		}
	}
}

func (p *Poller) fetchAndLog(ctx context.Context) {
	tickers, err := p.client.FetchUSDTPerpetualTickers(ctx)
	if err != nil {
		if ctx.Err() == nil {
			p.logger.Warn("Bybit public market-data request failed", "error", err)
		}
		return
	}

	symbols := make([]string, 0, len(tickers))
	for _, ticker := range tickers {
		symbols = append(symbols, ticker.Symbol)
	}
	sort.Strings(symbols)
	if len(symbols) > p.maxSymbols {
		symbols = symbols[:p.maxSymbols]
	}
	p.logger.Info("Bybit public market-data snapshot received",
		"available_usdt_contracts", len(tickers),
		"tracked_contracts", len(symbols),
	)
	p.logger.Debug("Tracked contracts", "symbols", symbols)
}
