package market

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
)

const (
	initialInstrumentRetry = 5 * time.Second
	maxInstrumentRetry     = time.Minute
)

type Poller struct {
	client                    *bybit.Client
	interval                  time.Duration
	instrumentRefreshInterval time.Duration
	maxSymbols                int
	logger                    *slog.Logger
}

func NewPoller(client *bybit.Client, interval, instrumentRefreshInterval time.Duration, maxSymbols int, logger *slog.Logger) *Poller {
	return &Poller{
		client:                    client,
		interval:                  interval,
		instrumentRefreshInterval: instrumentRefreshInterval,
		maxSymbols:                maxSymbols,
		logger:                    logger,
	}
}

func (p *Poller) Run(ctx context.Context) {
	marketTicker := time.NewTicker(p.interval)
	defer marketTicker.Stop()

	instrumentTimer := time.NewTimer(0)
	defer instrumentTimer.Stop()

	instruments := make([]bybit.Instrument, 0)
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
				resetTimer(instrumentTimer, retryDelay)
				retryDelay *= 2
				if retryDelay > maxInstrumentRetry {
					retryDelay = maxInstrumentRetry
				}
				continue
			}

			available := len(updated)
			sort.Slice(updated, func(i, j int) bool {
				return updated[i].Symbol < updated[j].Symbol
			})
			if len(updated) > p.maxSymbols {
				updated = updated[:p.maxSymbols]
			}
			instruments = updated
			retryDelay = initialInstrumentRetry
			p.logger.Info("Список контрактов Bybit обновлён",
				"контрактов_доступно", available,
				"контрактов_отслеживается", len(instruments),
			)
			resetTimer(instrumentTimer, p.instrumentRefreshInterval)

		case <-marketTicker.C:
			if len(instruments) == 0 {
				continue
			}
			snapshot, err := p.client.FetchMarketData(ctx, instruments)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				p.logger.Warn("Не удалось обновить рыночные данные Bybit", "ошибка", err)
				continue
			}

			p.logger.Info("Получен снимок рыночных данных Bybit",
				"контрактов_в_снимке", len(snapshot),
			)
			for _, item := range snapshot {
				ticker := item.Ticker
				p.logger.Debug("Рыночные данные контракта",
					"символ", item.Instrument.Symbol,
					"последняя_цена", ticker.LastPrice,
					"лучшая_покупка", ticker.BestBidPrice,
					"лучшая_продажа", ticker.BestAskPrice,
					"объём_24ч", ticker.Volume24h,
					"оборот_24ч", ticker.Turnover24h,
					"изменение_цены_24ч", ticker.Price24hPcnt,
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

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}
