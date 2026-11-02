package market

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"

)

const (
	RankingStatusWarmingUp = "warming_up"
	RankingStatusReady     = "ready"
	RankingStatusStale     = "stale"

	// CandidateCount — отдельная группа монет по накоплению котируемого оборота.
	CandidateCount = 25

	accumulationWindowCandles = 5
	accelerationWindowCandles = 3
	rankingMarketMaxAge       = 10 * time.Second
)

// RankerConfig управляет только частотой пересчёта и размером второй группы.
// Первая группа намеренно фиксирована на TOP-25 согласно требованиям скринера.
type RankerConfig struct {
	UpdateInterval     time.Duration
	AccelerationLeaders int
}

// RankedSymbol содержит исходные метрики, по которым инструмент попал в рейтинг.
type RankedSymbol struct {
	Rank                              int
	Symbol                            string
	Turnover5mUSDT                    float64
	AveragePrevious5mTurnoverUSDT     float64
	TurnoverMultiple                  float64
	TurnoverMultipleAvailable         bool
	Volume5mBase                      float64
	LastPrice                         float64
	Last3mChangePct                   float64
	Previous3mChangePct               float64
	PriceAccelerationPP               float64
}

// RankingSnapshot — два непересекающихся списка, максимум 25 + 10 символов.
type RankingSnapshot struct {
	UpdatedAt      time.Time
	MarketUpdatedAt time.Time
	Status         string
	Reason         string
	MarketSymbols  int
	ReadySymbols   int
	Candidates     []RankedSymbol
	Accelerators   []RankedSymbol
}

type Ranker struct {
	candles *CandleStore
	cfg     RankerConfig
}

func NewRanker(candles *CandleStore, cfg RankerConfig) (*Ranker, error) {
	if candles == nil {
		return nil, errors.New("CandleStore для рейтинга не задан")
	}
	if cfg.UpdateInterval < 100*time.Millisecond || cfg.UpdateInterval > time.Minute {
		return nil, errors.New("интервал рейтинга должен быть в диапазоне 100ms..1m")
	}
	if cfg.AccelerationLeaders < 5 || cfg.AccelerationLeaders > 10 {
		return nil, errors.New("количество лидеров ускорения должно быть в диапазоне 5..10")
	}
	return &Ranker{candles: candles, cfg: cfg}, nil
}

// Run обновляет рейтинг независимо от REST Poller и детальных потоков сделок/стакана.
func (r *Ranker) Run(ctx context.Context, monitor *Monitor) {
	if monitor == nil {
		return
	}
	ticker := time.NewTicker(r.cfg.UpdateInterval)
	defer ticker.Stop()

	r.update(monitor)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.update(monitor)
		}
	}
}

func (r *Ranker) update(monitor *Monitor) {
	snapshot := r.Build(monitor.Snapshot(), time.Now())
	monitor.UpdateRanking(snapshot)
}

// Build вычисляет рейтинги только по непрерывным и свежим минутным свечам.
// TOP-25 сортируется по обороту последних пяти закрытых минут в USDT.
// Лидеры ускорения сравнивают доходность последних трёх минут с предыдущими тремя.
func (r *Ranker) Build(marketSnapshot MonitorSnapshot, now time.Time) RankingSnapshot {
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()
	result := RankingSnapshot{
		UpdatedAt:       now,
		MarketUpdatedAt: marketSnapshot.UpdatedAt,
		Status:          RankingStatusWarmingUp,
		MarketSymbols:   len(marketSnapshot.Items),
		Candidates:      make([]RankedSymbol, 0, CandidateCount),
		Accelerators:    make([]RankedSymbol, 0, r.cfg.AccelerationLeaders),
	}
	if marketSnapshot.UpdatedAt.IsZero() {
		result.Reason = "ожидается первый REST-снимок рынка"
		return result
	}
	marketAge := now.Sub(marketSnapshot.UpdatedAt)
	if marketAge < 0 || marketAge > rankingMarketMaxAge {
		result.Status = RankingStatusStale
		result.Reason = "REST-снимок рынка устарел"
		return result
	}
	result.Status = RankingStatusReady

	candidates := make([]RankedSymbol, 0, len(marketSnapshot.Items))
	accelerators := make([]RankedSymbol, 0, len(marketSnapshot.Items))
	seen := make(map[string]struct{}, len(marketSnapshot.Items))

	for _, item := range marketSnapshot.Items {
		symbol := normalizeSymbol(item.Instrument.Symbol)
		if symbol == "" {
			continue
		}
		if _, exists := seen[symbol]; exists {
			continue
		}
		seen[symbol] = struct{}{}

		candleSnapshot, ok := r.candles.Snapshot(symbol, now)
		if !ok || candleSnapshot.Status != CandleStatusReady ||
			candleSnapshot.Current == nil ||
			!hasContinuousHistory(candleSnapshot.History, 20, now.Truncate(time.Minute)) {
			continue
		}
		metrics, ok := rankingMetrics(candleSnapshot.History)
		if !ok {
			continue
		}
		metrics.Symbol = symbol
		result.ReadySymbols++

		if metrics.Turnover5mUSDT > 0 {
			candidates = append(candidates, metrics)
		}
		if metrics.Last3mChangePct > 0 && metrics.PriceAccelerationPP > 0 {
			accelerators = append(accelerators, metrics)
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Turnover5mUSDT != candidates[j].Turnover5mUSDT {
			return candidates[i].Turnover5mUSDT > candidates[j].Turnover5mUSDT
		}
		if candidates[i].TurnoverMultipleAvailable && candidates[j].TurnoverMultipleAvailable &&
			candidates[i].TurnoverMultiple != candidates[j].TurnoverMultiple {
			return candidates[i].TurnoverMultiple > candidates[j].TurnoverMultiple
		}
		return candidates[i].Symbol < candidates[j].Symbol
	})
	if len(candidates) > CandidateCount {
		candidates = candidates[:CandidateCount]
	}
	for i := range candidates {
		candidates[i].Rank = i + 1
	}
	result.Candidates = candidates

	candidateSymbols := make(map[string]struct{}, len(candidates))
	for _, item := range candidates {
		candidateSymbols[item.Symbol] = struct{}{}
	}
	filteredAccelerators := accelerators[:0]
	for _, item := range accelerators {
		if _, overlaps := candidateSymbols[item.Symbol]; overlaps {
			continue
		}
		filteredAccelerators = append(filteredAccelerators, item)
	}
	sort.Slice(filteredAccelerators, func(i, j int) bool {
		if filteredAccelerators[i].PriceAccelerationPP != filteredAccelerators[j].PriceAccelerationPP {
			return filteredAccelerators[i].PriceAccelerationPP > filteredAccelerators[j].PriceAccelerationPP
		}
		if filteredAccelerators[i].Last3mChangePct != filteredAccelerators[j].Last3mChangePct {
			return filteredAccelerators[i].Last3mChangePct > filteredAccelerators[j].Last3mChangePct
		}
		if filteredAccelerators[i].Turnover5mUSDT != filteredAccelerators[j].Turnover5mUSDT {
			return filteredAccelerators[i].Turnover5mUSDT > filteredAccelerators[j].Turnover5mUSDT
		}
		return filteredAccelerators[i].Symbol < filteredAccelerators[j].Symbol
	})
	if len(filteredAccelerators) > r.cfg.AccelerationLeaders {
		filteredAccelerators = filteredAccelerators[:r.cfg.AccelerationLeaders]
	}
	for i := range filteredAccelerators {
		filteredAccelerators[i].Rank = i + 1
	}
	result.Accelerators = filteredAccelerators
	return result
}

func rankingMetrics(history []Candle) (RankedSymbol, bool) {
	if len(history) < 20 {
		return RankedSymbol{}, false
	}
	history = history[len(history)-20:]
	for i, candle := range history {
		if !finiteRankingValue(candle.Close) || candle.Close <= 0 ||
			!finiteRankingValue(candle.Turnover) || candle.Turnover < 0 ||
			!finiteRankingValue(candle.Volume) || candle.Volume < 0 {
			return RankedSymbol{}, false
		}
		if i > 0 && !candle.StartTime.Equal(history[i-1].StartTime.Add(time.Minute)) {
			return RankedSymbol{}, false
		}
	}

	var recentTurnover, recentVolume, previousTurnover float64
	for i := len(history) - accumulationWindowCandles; i < len(history); i++ {
		recentTurnover += history[i].Turnover
		recentVolume += history[i].Volume
	}
	for i := 0; i < len(history)-accumulationWindowCandles; i++ {
		previousTurnover += history[i].Turnover
	}
	averagePrevious5m := previousTurnover / 3 // 15 предыдущих минут / 3 пятиминутных окна.

	startRecent := len(history) - 1 - accelerationWindowCandles
	startPrevious := startRecent - accelerationWindowCandles
	last3mChange := (history[len(history)-1].Close/history[startRecent].Close - 1) * 100
	previous3mChange := (history[startRecent].Close/history[startPrevious].Close - 1) * 100
	acceleration := last3mChange - previous3mChange

	result := RankedSymbol{
		Turnover5mUSDT:                recentTurnover,
		AveragePrevious5mTurnoverUSDT: averagePrevious5m,
		Volume5mBase:                  recentVolume,
		LastPrice:                     history[len(history)-1].Close,
		Last3mChangePct:               last3mChange,
		Previous3mChangePct:           previous3mChange,
		PriceAccelerationPP:           acceleration,
	}
	if averagePrevious5m > 0 {
		result.TurnoverMultiple = recentTurnover / averagePrevious5m
		result.TurnoverMultipleAvailable = true
	}
	if !finiteRankingValue(result.Turnover5mUSDT) ||
		!finiteRankingValue(result.Volume5mBase) ||
		!finiteRankingValue(result.Last3mChangePct) ||
		!finiteRankingValue(result.Previous3mChangePct) ||
		!finiteRankingValue(result.PriceAccelerationPP) ||
		(result.TurnoverMultipleAvailable && !finiteRankingValue(result.TurnoverMultiple)) {
		return RankedSymbol{}, false
	}
	return result, true
}

func finiteRankingValue(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func cloneRankingSnapshot(snapshot RankingSnapshot) RankingSnapshot {
	snapshot.Candidates = append([]RankedSymbol(nil), snapshot.Candidates...)
	snapshot.Accelerators = append([]RankedSymbol(nil), snapshot.Accelerators...)
	return snapshot
}

