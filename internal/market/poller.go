package market

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
)

const (
	initialInstrumentRetry = 5 * time.Second
	maxInstrumentRetry     = time.Minute
)

type SymbolSubscriber interface {
	SetSymbols(symbols []string)
}

type MarketDataSubscriber interface {
	UpdateMarketData(items []bybit.MarketData)
}

type MarketSnapshotSubscriber interface {
	UpdateMarketSnapshot(items []bybit.MarketData, updatedAt time.Time)
}

type KlineSymbolSubscriber interface {
	SetSymbols(symbols []string)
}

type RankingSnapshotProvider interface {
	Ranking() RankingSnapshot
}

type MarketDataIssueReporter interface {
	ReportMarketDataIssue(message string)
}

type OpenPositionSymbolProvider interface {
	OpenPositionSymbols() []string
}

type MarketRESTClient interface {
	FetchUSDTPerpetualInstruments(ctx context.Context) ([]bybit.Instrument, error)
	FetchLinearTickers(ctx context.Context) ([]bybit.Ticker, error)
}

type Poller struct {
	client                    MarketRESTClient
	interval                  time.Duration
	instrumentRefreshInterval time.Duration
	excludedSymbols           map[string]struct{}
	subscriber                any
	monitor                   MarketSnapshotSubscriber
	klineSymbols              KlineSymbolSubscriber
	logger                    *slog.Logger
}

func NewPoller(client MarketRESTClient, interval, instrumentRefreshInterval time.Duration, excludedSymbols []string, subscriber any, monitor MarketSnapshotSubscriber, klineSymbols KlineSymbolSubscriber, logger *slog.Logger) *Poller {
	if logger == nil { logger = slog.Default() }
	return &Poller{
		client: client,
		interval: interval,
		instrumentRefreshInterval: instrumentRefreshInterval,
		excludedSymbols: symbolSet(excludedSymbols),
		subscriber: subscriber,
		monitor: monitor,
		klineSymbols: klineSymbols,
		logger: logger,
	}
}

func (p *Poller) Run(ctx context.Context) {
	marketTicker := time.NewTicker(p.interval)
	defer marketTicker.Stop()

	instrumentTimer := time.NewTimer(0)
	defer instrumentTimer.Stop()

	instruments := make([]bybit.Instrument, 0)
	monitorInstruments := make([]bybit.Instrument, 0)
	retryDelay := initialInstrumentRetry

	for {
		select {
		case <-ctx.Done():
			return
		case <-instrumentTimer.C:
			updated, err := p.client.FetchUSDTPerpetualInstruments(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				p.logger.Warn("Не удалось обновить список контрактов Bybit", "ошибка", err)
				p.reportMarketDataIssue("Не удалось обновить список контрактов Bybit: " + err.Error())
				resetTimer(instrumentTimer, retryDelay)
				retryDelay *= 2
				if retryDelay > maxInstrumentRetry {
					retryDelay = maxInstrumentRetry
				}
				continue
			}

			instruments = append(instruments[:0], updated...)
			monitorInstruments = excludeInstruments(instruments, p.excludedSymbols)
			if p.klineSymbols != nil {
				symbols := make([]string, 0, len(monitorInstruments))
				for _, instrument := range monitorInstruments {
					symbols = append(symbols, instrument.Symbol)
				}
				p.klineSymbols.SetSymbols(symbols)
			}
			detailedSymbols := p.syncDetailedSubscriptions(monitorInstruments)
			openPositions := p.openPositionSymbols()
			retryDelay = initialInstrumentRetry
			p.logger.Info("Список контрактов Bybit обновлён",
				"контрактов_доступно", len(instruments),
				"исключено_из_мониторинга", len(instruments)-len(monitorInstruments),
				"контрактов_в_мониторинге", len(monitorInstruments),
				"подробных_подписок", len(detailedSymbols),
				"открытых_позиций_в_подробном_наблюдении", len(openPositions),
			)
			resetTimer(instrumentTimer, p.instrumentRefreshInterval)

		case <-marketTicker.C:
			if len(instruments) == 0 {
				continue
			}
			tickers, err := p.client.FetchLinearTickers(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				p.logger.Warn("Не удалось обновить рыночные данные Bybit", "ошибка", err)
				p.reportMarketDataIssue("Не удалось обновить рыночные данные Bybit: " + err.Error())
				continue
			}

			allSnapshot := bybit.JoinMarketData(instruments, tickers)
			monitorSnapshot := bybit.JoinMarketData(monitorInstruments, tickers)
			updatedAt := time.Now()
			if p.monitor != nil {
				p.monitor.UpdateMarketSnapshot(monitorSnapshot, updatedAt)
			}
			detailedSymbols := p.syncDetailedSubscriptions(monitorInstruments)
			// Paper Trading получает полный набор спецификаций/funding, включая исключённые монеты.
			if subscriber, ok := p.subscriber.(MarketDataSubscriber); ok {
				subscriber.UpdateMarketData(allSnapshot)
			}
			p.logger.Info("Получен полный снимок рыночных данных Bybit",
				"тикеров_linear", len(tickers),
				"контрактов_со_спецификациями", len(allSnapshot),
				"контрактов_в_мониторинге", len(monitorSnapshot),
			)
			p.logger.Debug("Подробные WebSocket-подписки синхронизированы",
				"символов", len(detailedSymbols),
			)
			for _, item := range allSnapshot {
				ticker := item.Ticker
				p.logger.Debug("Рыночные данные контракта",
					"символ", item.Instrument.Symbol,
					"последняя_цена", ticker.LastPrice,
					"mark_price", ticker.MarkPrice,
					"лучшая_покупка", ticker.Bid1Price,
					"размер_лучшая_покупка", ticker.Bid1Size,
					"лучшая_продажа", ticker.Ask1Price,
					"размер_лучшая_продажа", ticker.Ask1Size,
					"объём_24ч", ticker.Volume24h,
					"оборот_24ч", ticker.Turnover24h,
					"изменение_цены_24ч", ticker.Price24hPcnt,
					"open_interest", ticker.OpenInterest,
					"open_interest_value", ticker.OpenInterestValue,
					"ставка_funding", ticker.FundingRate,
					"следующий_funding", ticker.NextFundingTime,
					"минимальное_количество", item.Instrument.MinOrderQty,
					"шаг_количества", item.Instrument.QtyStep,
					"минимальный_номинал", item.Instrument.MinNotionalValue,
					"шаг_цены", item.Instrument.TickSize,
					"максимальный_объём_маркет", item.Instrument.MaxMktOrderQty,
					"интервал_funding_минут", item.Instrument.FundingInterval,
				)
			}
		}
	}
}

func symbolSet(symbols []string) map[string]struct{} {
	result := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		symbol = normalizeSymbol(symbol)
		if symbol != "" { result[symbol] = struct{}{} }
	}
	return result
}

func normalizeSymbol(symbol string) string { return strings.ToUpper(strings.TrimSpace(symbol)) }

func excludeInstruments(instruments []bybit.Instrument, excluded map[string]struct{}) []bybit.Instrument {
	result := make([]bybit.Instrument, 0, len(instruments))
	for _, instrument := range instruments {
		if _, skip := excluded[normalizeSymbol(instrument.Symbol)]; skip { continue }
		result = append(result, instrument)
	}
	return result
}

func symbolsWithOpenPositions(instruments []bybit.Instrument, openPositions []string) []string {
	result := make([]string, 0, len(instruments)+len(openPositions))
	seen := make(map[string]struct{}, len(instruments)+len(openPositions))
	for _, instrument := range instruments {
		symbol := normalizeSymbol(instrument.Symbol)
		if symbol == "" { continue }
		if _, exists := seen[symbol]; exists { continue }
		seen[symbol] = struct{}{}
		result = append(result, symbol)
	}
	for _, rawSymbol := range openPositions {
		symbol := normalizeSymbol(rawSymbol)
		if symbol == "" { continue }
		if _, exists := seen[symbol]; exists { continue }
		seen[symbol] = struct{}{}
		result = append(result, symbol)
	}
	return result
}

func (p *Poller) syncDetailedSubscriptions(instruments []bybit.Instrument) []string {
	ranking := RankingSnapshot{}
	if provider, ok := p.monitor.(RankingSnapshotProvider); ok {
		ranking = provider.Ranking()
	}
	symbols := selectedRankedSymbols(instruments, ranking, time.Now())

	// Открытые позиции сохраняются вне зависимости от рейтинга и списка исключений.
	symbols = symbolsWithOpenPositions(symbolsToInstruments(symbols), p.openPositionSymbols())
	if subscriber, ok := p.subscriber.(SymbolSubscriber); ok {
		subscriber.SetSymbols(symbols)
	}
	return symbols
}

func symbolsToInstruments(symbols []string) []bybit.Instrument {
	result := make([]bybit.Instrument, 0, len(symbols))
	for _, symbol := range symbols {
		result = append(result, bybit.Instrument{Symbol: symbol})
	}
	return result
}

func selectedRankedSymbols(instruments []bybit.Instrument, ranking RankingSnapshot, now time.Time) []string {
	if ranking.Status != RankingStatusReady ||
		ranking.UpdatedAt.IsZero() ||
		now.Before(ranking.UpdatedAt) ||
		now.Sub(ranking.UpdatedAt) > 5*time.Second {
		return nil
	}

	active := make(map[string]struct{}, len(instruments))
	for _, instrument := range instruments {
		if symbol := normalizeSymbol(instrument.Symbol); symbol != "" {
			active[symbol] = struct{}{}
		}
	}
	seen := make(map[string]struct{}, len(ranking.Candidates)+len(ranking.Accelerators))
	result := make([]string, 0, len(ranking.Candidates)+len(ranking.Accelerators))
	for _, group := range [][]RankedSymbol{ranking.Candidates, ranking.Accelerators} {
		for _, item := range group {
			symbol := normalizeSymbol(item.Symbol)
			if symbol == "" {
				continue
			}
			if _, exists := active[symbol]; !exists {
				continue
			}
			if _, exists := seen[symbol]; exists {
				continue
			}
			seen[symbol] = struct{}{}
			result = append(result, symbol)
		}
	}
	return result
}

func (p *Poller) openPositionSymbols() []string {
	provider, ok := p.subscriber.(OpenPositionSymbolProvider)
	if !ok { return nil }
	return provider.OpenPositionSymbols()
}

func (p *Poller) reportMarketDataIssue(message string) {
	if reporter, ok := p.subscriber.(MarketDataIssueReporter); ok {
		reporter.ReportMarketDataIssue(message)
	}
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}
