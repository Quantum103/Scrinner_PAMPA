// Package analysis агрегирует рыночные события и рассчитывает метрики торговой активности.
package analysis

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

const (
	analysisBucketSize     = 100 * time.Millisecond
	StatusReady            = "ready"
	StatusInsufficientData = "insufficient_data"
	StatusStale            = "stale"
	StatusIncomplete       = "incomplete"
	StatusDisconnected     = "disconnected"
)

var (
	ErrTradeTimestampOutOfRange = errors.New("время сделки слишком далеко от времени получения")
	ErrTradeOutOfOrder           = errors.New("получена сделка за пределами допустимого порядка времени")
)

type Config struct {
	RVOLInterval                  time.Duration
	RVOLLookbackIntervals         int
	FlowWindow                    time.Duration
	CVDWindow                     time.Duration
	MomentumWindows               []time.Duration
	VolumeAccelerationWindow      time.Duration
	AbsorptionMinBuyShare         float64
	AbsorptionBuyVolumeMultiplier float64
	AbsorptionMaxPriceChangePct   float64
	OrderBookLevels               int
}

func DefaultConfig() Config {
	return Config{
		RVOLInterval:                  time.Minute,
		RVOLLookbackIntervals:         20,
		FlowWindow:                    time.Minute,
		CVDWindow:                     15 * time.Minute,
		MomentumWindows:               []time.Duration{15 * time.Second, time.Minute, 5 * time.Minute},
		VolumeAccelerationWindow:      10 * time.Second,
		AbsorptionMinBuyShare:         0.70,
		AbsorptionBuyVolumeMultiplier: 1.5,
		AbsorptionMaxPriceChangePct:   0.05,
		OrderBookLevels:               10,
	}
}

func (c Config) Validate() error {
	if c.RVOLInterval < time.Second || c.RVOLInterval%time.Second != 0 {
		return errors.New("интервал RVOL должен быть не меньше секунды и кратен секунде")
	}
	if c.RVOLLookbackIntervals <= 0 || c.RVOLLookbackIntervals > 10000 {
		return errors.New("количество базовых интервалов RVOL должно быть в диапазоне 1..10000")
	}
	if c.RVOLInterval > 24*time.Hour/time.Duration(c.RVOLLookbackIntervals+2) {
		return errors.New("история RVOL не должна превышать 24 часа")
	}
	windows := []struct {
		name  string
		value time.Duration
	}{
		{name: "окно taker-потока", value: c.FlowWindow},
		{name: "окно CVD", value: c.CVDWindow},
		{name: "окно ускорения объёма", value: c.VolumeAccelerationWindow},
	}
	for _, window := range windows {
		if window.value < time.Second || window.value%time.Second != 0 {
			return fmt.Errorf("%s должно быть не меньше секунды и кратно секунде", window.name)
		}
		maximum := 24 * time.Hour
		if window.name != "окно CVD" {
			maximum /= 2 // Для ускорения и absorption нужны два соседних равных окна.
		}
		if window.value > maximum {
			return fmt.Errorf("%s не должно превышать %s", window.name, maximum)
		}
	}
	if len(c.MomentumWindows) == 0 {
		return errors.New("необходимо задать хотя бы один интервал momentum")
	}
	seen := make(map[time.Duration]struct{}, len(c.MomentumWindows))
	for _, window := range c.MomentumWindows {
		if window < time.Second || window%time.Second != 0 || window > 24*time.Hour {
			return errors.New("интервалы momentum должны быть в диапазоне от 1 секунды до 24 часов и кратны секунде")
		}
		if _, ok := seen[window]; ok {
			return fmt.Errorf("интервал momentum %s указан повторно", window)
		}
		seen[window] = struct{}{}
	}
	if c.AbsorptionMinBuyShare <= 0 || c.AbsorptionMinBuyShare > 1 || math.IsNaN(c.AbsorptionMinBuyShare) {
		return errors.New("порог доли покупок для absorption должен быть в диапазоне (0, 1]")
	}
	if c.AbsorptionBuyVolumeMultiplier <= 1 || math.IsNaN(c.AbsorptionBuyVolumeMultiplier) || math.IsInf(c.AbsorptionBuyVolumeMultiplier, 0) {
		return errors.New("множитель объёма покупок для absorption должен быть больше 1")
	}
	if c.AbsorptionMaxPriceChangePct <= 0 || math.IsNaN(c.AbsorptionMaxPriceChangePct) || math.IsInf(c.AbsorptionMaxPriceChangePct, 0) {
		return errors.New("максимальное изменение цены для absorption должно быть больше нуля")
	}
	if c.OrderBookLevels <= 0 || c.OrderBookLevels > 50 {
		return errors.New("количество уровней стакана для анализа должно быть в диапазоне 1..50")
	}
	return nil
}

type Trade struct {
	Time       time.Time
	ReceivedAt time.Time
	Price      float64
	Quantity   float64
	Side       string
}

type Level struct {
	Price float64
	Size  float64
}

type Book struct {
	Valid      bool
	Fresh      bool
	ReceivedAt time.Time
	BestBid    float64
	BestAsk    float64
	Bids       []Level
	Asks       []Level
}

type Snapshot struct {
	Symbol                    string             `json:"symbol"`
	Status                    string             `json:"status"`
	Reason                    string             `json:"reason,omitempty"`
	UpdatedAt                 time.Time          `json:"updated_at"`
	CVDResetAt                time.Time          `json:"cvd_reset_at,omitempty"`
	CVDResetReason            string             `json:"cvd_reset_reason,omitempty"`
	RVOL                      *float64           `json:"rvol,omitempty"`
	RVOLCurrentVolume         *float64           `json:"rvol_current_volume,omitempty"`
	RVOLAverageVolume         *float64           `json:"rvol_average_volume,omitempty"`
	TakerBuyVolume            *float64           `json:"taker_buy_volume,omitempty"`
	TakerSellVolume           *float64           `json:"taker_sell_volume,omitempty"`
	TakerBuySharePercent      *float64           `json:"taker_buy_share_percent,omitempty"`
	TakerSellSharePercent     *float64           `json:"taker_sell_share_percent,omitempty"`
	Delta                     *float64           `json:"delta,omitempty"`
	CVD                       *float64           `json:"cvd,omitempty"`
	MomentumPercent           map[string]float64 `json:"momentum_percent,omitempty"`
	VolumeAccelerationPercent *float64           `json:"volume_acceleration_percent,omitempty"`
	SpreadBPS                 *float64           `json:"spread_bps,omitempty"`
	BidLiquidityQuote         *float64           `json:"bid_liquidity_quote,omitempty"`
	AskLiquidityQuote         *float64           `json:"ask_liquidity_quote,omitempty"`
	Absorption                *bool              `json:"absorption,omitempty"`
	LastPrice                 *float64           `json:"last_price,omitempty"`
	FlowTradeCount            int                `json:"flow_trade_count"`
	LargestTradeSharePercent  *float64           `json:"largest_trade_share_percent,omitempty"`
	BidBookLevels             int                `json:"bid_book_levels"`
	AskBookLevels             int                `json:"ask_book_levels"`
}

type secondBucket struct {
	BuyVolume  float64
	SellVolume float64
	Total        float64
	TradeCount   int
	LargestTrade float64
	LastPrice    float64
	LastAt     time.Time
}

type symbolState struct {
	startedAt         time.Time
	resetAt           time.Time
	resetReason       string
	afterGap          bool
	connected         bool
	lastPrunedSecond  int64
	lastTradeTime     time.Time
	lastTradeReceived time.Time
	lastPrice         float64
	buckets           map[int64]secondBucket
	intervalVolumes   map[int64]float64
}

type Analyzer struct {
	mu             sync.RWMutex
	cfg            Config
	maxDataAge     time.Duration
	retention      time.Duration
	maxWarmup      time.Duration
	states         map[string]*symbolState
	momentumWindow []time.Duration
}

func New(cfg Config, maxDataAge time.Duration) (*Analyzer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if maxDataAge <= 0 {
		return nil, errors.New("максимальный возраст рыночных данных должен быть больше нуля")
	}
	maxMomentum := time.Duration(0)
	for _, window := range cfg.MomentumWindows {
		if window > maxMomentum {
			maxMomentum = window
		}
	}
	retention := maxDuration(
		cfg.RVOLInterval*time.Duration(cfg.RVOLLookbackIntervals+2),
		cfg.CVDWindow,
		cfg.FlowWindow*2,
		maxMomentum,
		cfg.VolumeAccelerationWindow*2,
	) + time.Minute
	warmup := maxDuration(
		cfg.RVOLInterval*time.Duration(cfg.RVOLLookbackIntervals+1),
		cfg.CVDWindow,
		cfg.FlowWindow,
		maxMomentum,
		cfg.VolumeAccelerationWindow*2,
	)
	return &Analyzer{
		cfg:            cfg,
		maxDataAge:     maxDataAge,
		retention:      retention,
		maxWarmup:      warmup,
		states:         make(map[string]*symbolState),
		momentumWindow: append([]time.Duration(nil), cfg.MomentumWindows...),
	}, nil
}

func maxDuration(values ...time.Duration) time.Duration {
	var result time.Duration
	for _, value := range values {
		if value > result {
			result = value
		}
	}
	return result
}

// StartSession сбрасывает скользящие данные при каждом переподключении.
func (a *Analyzer) StartSession(symbol string, at time.Time, reconnected bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	reason := "первичный запуск"
	if reconnected {
		reason = "переподключение WebSocket"
	}
	a.states[symbol] = newSymbolState(at, reconnected, reason)
}

func newSymbolState(at time.Time, afterGap bool, reason string) *symbolState {
	return &symbolState{
		startedAt:       at,
		resetAt:         at,
		resetReason:     reason,
		afterGap:        afterGap,
		connected:       true,
		buckets:         make(map[int64]secondBucket),
		intervalVolumes: make(map[int64]float64),
	}
}

// MarkDisconnected очищает накопленную историю: между соединениями сделки могли быть пропущены.
func (a *Analyzer) MarkDisconnected(symbol string, at time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	state := a.states[symbol]
	if state == nil {
		state = newSymbolState(at, true, "разрыв WebSocket")
		a.states[symbol] = state
		state.connected = false
		return
	}
	state = newSymbolState(at, true, "разрыв WebSocket")
	state.connected = false
	a.states[symbol] = state
}

// MarkGap сбрасывает окна после обнаруженной потери непрерывности рыночных событий.
func (a *Analyzer) MarkGap(symbol string, at time.Time, reason string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	state := newSymbolState(at, true, reason)
	a.states[symbol] = state
}

// AddTrade обновляет единый набор секундных и минутных агрегатов на каждую принятую сделку.
func (a *Analyzer) AddTrade(symbol string, trade Trade) error {
	if symbol == "" || trade.Time.IsZero() || trade.ReceivedAt.IsZero() || trade.Price <= 0 || trade.Quantity <= 0 ||
		math.IsNaN(trade.Price) || math.IsNaN(trade.Quantity) || math.IsInf(trade.Price, 0) || math.IsInf(trade.Quantity, 0) {
		return errors.New("для анализа получена некорректная сделка")
	}
	if trade.Side != "Buy" && trade.Side != "Sell" {
		return fmt.Errorf("неизвестная сторона агрессора %q", trade.Side)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	state := a.states[symbol]
	if state == nil {
		state = newSymbolState(trade.ReceivedAt, false, "первичный запуск")
		a.states[symbol] = state
	}
	if !state.connected {
		return fmt.Errorf("сделка %s получена при отключённом анализаторе", symbol)
	}
	if trade.Time.After(trade.ReceivedAt.Add(a.maxDataAge)) || trade.ReceivedAt.Sub(trade.Time) > a.maxDataAge {
		state = newSymbolState(trade.ReceivedAt, true, "время сделки вышло за пределы допустимого рассогласования")
		a.states[symbol] = state
		return fmt.Errorf("%w: биржевое время %s, время получения %s, допуск %s",
			ErrTradeTimestampOutOfRange, trade.Time.Format(time.RFC3339Nano), trade.ReceivedAt.Format(time.RFC3339Nano), a.maxDataAge)
	}
	if !state.lastTradeTime.IsZero() && trade.Time.Before(state.lastTradeTime.Add(-a.maxDataAge)) {
		previousTradeTime := state.lastTradeTime
		state = newSymbolState(trade.ReceivedAt, true, "обнаружена старая или переставленная сделка")
		a.states[symbol] = state
		return fmt.Errorf("%w: время сделки %s, предыдущая сделка %s, допуск %s",
			ErrTradeOutOfOrder, trade.Time.Format(time.RFC3339Nano), previousTradeTime.Format(time.RFC3339Nano), a.maxDataAge)
	}

	bucketKey := trade.Time.UnixNano() / int64(analysisBucketSize)
	bucket := state.buckets[bucketKey]
	if trade.Side == "Buy" {
		bucket.BuyVolume += trade.Quantity
	} else {
		bucket.SellVolume += trade.Quantity
	}
	bucket.Total += trade.Quantity
	bucket.TradeCount++
	if trade.Quantity > bucket.LargestTrade {
		bucket.LargestTrade = trade.Quantity
	}
	if bucket.LastAt.IsZero() || !trade.Time.Before(bucket.LastAt) {
		bucket.LastPrice = trade.Price
		bucket.LastAt = trade.Time
	}
	state.buckets[bucketKey] = bucket

	intervalSeconds := int64(a.cfg.RVOLInterval / time.Second)
	intervalKey := floorDiv(trade.Time.Unix(), intervalSeconds)
	state.intervalVolumes[intervalKey] += trade.Quantity
	if trade.Time.After(state.lastTradeTime) || state.lastTradeTime.IsZero() {
		state.lastTradeTime = trade.Time
		state.lastPrice = trade.Price
	}
	if trade.ReceivedAt.After(state.lastTradeReceived) || state.lastTradeReceived.IsZero() {
		state.lastTradeReceived = trade.ReceivedAt
	}
	currentSecond := trade.Time.Unix()
	if currentSecond > state.lastPrunedSecond {
		a.prune(state, trade.Time)
		state.lastPrunedSecond = currentSecond
	}
	return nil
}

func floorDiv(value, divisor int64) int64 {
	quotient := value / divisor
	if value < 0 && value%divisor != 0 {
		quotient--
	}
	return quotient
}

func (a *Analyzer) prune(state *symbolState, latest time.Time) {
	cutoff := latest.Add(-a.retention)
	oldestBucket := cutoff.UnixNano() / int64(analysisBucketSize)
	for key := range state.buckets {
		if key < oldestBucket {
			delete(state.buckets, key)
		}
	}
	intervalSeconds := int64(a.cfg.RVOLInterval / time.Second)
	oldestInterval := floorDiv(cutoff.Unix(), intervalSeconds)
	for key := range state.intervalVolumes {
		if key < oldestInterval {
			delete(state.intervalVolumes, key)
		}
	}
}

func (a *Analyzer) Snapshot(symbol string, now time.Time, connected bool, book Book) Snapshot {
	result := Snapshot{Symbol: symbol, Status: StatusInsufficientData, UpdatedAt: now}
	a.mu.RLock()
	defer a.mu.RUnlock()
	state := a.states[symbol]
	if state == nil {
		result.Reason = "анализатор ещё не получил данные по инструменту"
		return result
	}
	result.CVDResetAt = state.resetAt
	result.CVDResetReason = state.resetReason
	if !connected || !state.connected {
		result.Status = StatusDisconnected
		result.Reason = "WebSocket-соединение неактивно"
		return result
	}
	if state.afterGap && now.Sub(state.startedAt) < a.maxWarmup {
		result.Status = StatusIncomplete
		result.Reason = fmt.Sprintf("после потери данных требуется непрерывно собрать историю до %s", state.startedAt.Add(a.maxWarmup).Format(time.RFC3339))
		return result
	}
	if !book.Valid {
		if book.ReceivedAt.IsZero() {
			result.Reason = "ожидается корректный snapshot стакана"
			return result
		}
		result.Status = StatusIncomplete
		result.Reason = "стакан не прошёл проверку целостности"
		return result
	}
	if !book.Fresh || book.ReceivedAt.IsZero() || now.Before(book.ReceivedAt) || now.Sub(book.ReceivedAt) > a.maxDataAge {
		result.Status = StatusStale
		result.Reason = "стакан устарел"
		return result
	}
	if state.lastTradeTime.IsZero() || state.lastTradeReceived.IsZero() {
		result.Reason = "ещё не получено ни одной сделки"
		return result
	}
	if now.Before(state.lastTradeReceived) || now.Sub(state.lastTradeReceived) > a.maxDataAge ||
		now.Before(state.lastTradeTime) || now.Sub(state.lastTradeTime) > a.maxDataAge {
		result.Status = StatusStale
		result.Reason = "последняя сделка устарела"
		return result
	}

	ready := true
	currentVolume, averageVolume, rvolOK := a.calculateRVOL(state, now)
	if rvolOK {
		result.RVOLCurrentVolume = floatPointer(currentVolume)
		result.RVOLAverageVolume = floatPointer(averageVolume)
		result.RVOL = floatPointer(currentVolume / averageVolume)
	} else {
		ready = false
	}

	flow, flowOK := a.sumFlow(state, now.Add(-a.cfg.FlowWindow), now)
	if flowOK && flow.Total > 0 {
		result.FlowTradeCount = flow.TradeCount
		result.LargestTradeSharePercent = floatPointer(flow.LargestTrade / flow.Total * 100)
		result.TakerBuyVolume = floatPointer(flow.Buy)
		result.TakerSellVolume = floatPointer(flow.Sell)
		result.TakerBuySharePercent = floatPointer(flow.Buy / flow.Total * 100)
		result.TakerSellSharePercent = floatPointer(flow.Sell / flow.Total * 100)
		result.Delta = floatPointer(flow.Buy - flow.Sell)
	} else {
		ready = false
	}

	cvd, cvdOK := a.calculateCVD(state, now)
	if cvdOK {
		result.CVD = floatPointer(cvd)
	} else {
		ready = false
	}

	result.LastPrice = floatPointer(state.lastPrice)
	result.MomentumPercent = make(map[string]float64, len(a.momentumWindow))
	momentumFlow, flowMomentumOK := a.calculateMomentum(state, now, a.cfg.FlowWindow)
	for _, window := range a.momentumWindow {
		momentum, ok := a.calculateMomentum(state, now, window)
		if !ok {
			ready = false
			continue
		}
		result.MomentumPercent[window.String()] = momentum
	}
	if len(result.MomentumPercent) == 0 {
		result.MomentumPercent = nil
	}

	acceleration, accelerationOK := a.calculateVolumeAcceleration(state, now)
	if accelerationOK {
		result.VolumeAccelerationPercent = floatPointer(acceleration)
	} else {
		ready = false
	}

	spread, bidLiquidity, askLiquidity, bookOK := a.calculateBook(book)
	if bookOK {
		result.SpreadBPS = floatPointer(spread)
		result.BidLiquidityQuote = floatPointer(bidLiquidity)
		result.AskLiquidityQuote = floatPointer(askLiquidity)
		result.BidBookLevels = len(book.Bids)
		result.AskBookLevels = len(book.Asks)
	} else {
		ready = false
	}

	absorption, absorptionOK := a.calculateAbsorption(state, now, flow, flowOK, momentumFlow, flowMomentumOK)
	if absorptionOK {
		result.Absorption = boolPointer(absorption)
	} else {
		ready = false
	}

	if ready {
		result.Status = StatusReady
		result.Reason = "все показатели рассчитаны на непрерывных актуальных данных"
	} else {
		result.Status = StatusInsufficientData
		result.Reason = "не хватает истории для одного или нескольких показателей"
	}
	return result
}

type flowTotals struct {
	Buy          float64
	Sell         float64
	Total        float64
	TradeCount   int
	LargestTrade float64
}

func (a *Analyzer) sumFlow(state *symbolState, from, to time.Time) (flowTotals, bool) {
	if state.startedAt.After(from) {
		return flowTotals{}, false
	}
	var result flowTotals
	for bucketIndex, bucket := range state.buckets {
		at := time.Unix(0, bucketIndex*int64(analysisBucketSize))
		if at.Before(from) || !at.Before(to) {
			continue
		}
		result.Buy += bucket.BuyVolume
		result.Sell += bucket.SellVolume
		result.Total += bucket.Total
		result.TradeCount += bucket.TradeCount
		if bucket.LargestTrade > result.LargestTrade {
			result.LargestTrade = bucket.LargestTrade
		}
	}
	return result, result.Total > 0
}

func (a *Analyzer) calculateRVOL(state *symbolState, now time.Time) (float64, float64, bool) {
	intervalSeconds := int64(a.cfg.RVOLInterval / time.Second)
	currentKey := floorDiv(now.Unix(), intervalSeconds)
	currentStart := time.Unix(currentKey*intervalSeconds, 0)
	elapsed := now.Sub(currentStart)
	if elapsed < analysisBucketSize {
		return 0, 0, false
	}
	firstBaselineKey := currentKey - int64(a.cfg.RVOLLookbackIntervals)
	baselineStart := time.Unix(firstBaselineKey*intervalSeconds, 0)
	if state.startedAt.After(baselineStart) {
		return 0, 0, false
	}

	// Сравниваем одинаковую часть каждого завершённого интервала: например, первые 30 секунд текущей минуты с первыми 30 секундами предыдущих 20 минут.
	baselineIntervals := make([]float64, a.cfg.RVOLLookbackIntervals)
	for bucketIndex, bucket := range state.buckets {
		bucketStart := time.Unix(0, bucketIndex*int64(analysisBucketSize))
		if bucketStart.Before(baselineStart) || !bucketStart.Before(currentStart) {
			continue
		}
		relative := bucketStart.Sub(baselineStart)
		intervalIndex := int(relative / a.cfg.RVOLInterval)
		withinInterval := relative % a.cfg.RVOLInterval
		if intervalIndex >= 0 && intervalIndex < len(baselineIntervals) && withinInterval < elapsed {
			baselineIntervals[intervalIndex] += bucket.Total
		}
	}
	var baseline float64
	for _, volume := range baselineIntervals {
		baseline += volume
	}
	average := baseline / float64(a.cfg.RVOLLookbackIntervals)
	if average <= 0 {
		return 0, 0, false
	}
	current := state.intervalVolumes[currentKey]
	return current, average, true
}

func (a *Analyzer) calculateCVD(state *symbolState, now time.Time) (float64, bool) {
	from := now.Add(-a.cfg.CVDWindow)
	if state.startedAt.After(from) {
		return 0, false
	}
	var cvd float64
	for bucketIndex, bucket := range state.buckets {
		at := time.Unix(0, bucketIndex*int64(analysisBucketSize))
		if at.Before(from) || at.After(now) {
			continue
		}
		cvd += bucket.BuyVolume - bucket.SellVolume
	}
	return cvd, true
}

func (a *Analyzer) calculateMomentum(state *symbolState, now time.Time, window time.Duration) (float64, bool) {
	from := now.Add(-window)
	if state.startedAt.After(from) {
		return 0, false
	}
	var referencePrice float64
	var referenceAt time.Time
	for _, bucket := range state.buckets {
		if bucket.LastAt.IsZero() || bucket.LastAt.After(from) {
			continue
		}
		if referenceAt.IsZero() || bucket.LastAt.After(referenceAt) {
			referenceAt = bucket.LastAt
			referencePrice = bucket.LastPrice
		}
	}
	if referenceAt.IsZero() || from.Sub(referenceAt) > a.maxDataAge || referencePrice <= 0 || state.lastPrice <= 0 {
		return 0, false
	}
	return (state.lastPrice/referencePrice - 1) * 100, true
}

func (a *Analyzer) calculateVolumeAcceleration(state *symbolState, now time.Time) (float64, bool) {
	window := a.cfg.VolumeAccelerationWindow
	if state.startedAt.After(now.Add(-2 * window)) {
		return 0, false
	}
	current, previous := 0.0, 0.0
	currentStart := now.Add(-window)
	previousStart := now.Add(-2 * window)
	for bucketIndex, bucket := range state.buckets {
		at := time.Unix(0, bucketIndex*int64(analysisBucketSize))
		switch {
		case !at.Before(currentStart) && !at.After(now):
			current += bucket.Total
		case !at.Before(previousStart) && at.Before(currentStart):
			previous += bucket.Total
		}
	}
	if previous <= 0 {
		return 0, false
	}
	return (current/previous - 1) * 100, true
}

func (a *Analyzer) calculateBook(book Book) (float64, float64, float64, bool) {
	if book.BestBid <= 0 || book.BestAsk <= book.BestBid || len(book.Bids) == 0 || len(book.Asks) == 0 {
		return 0, 0, 0, false
	}
	mid := (book.BestBid + book.BestAsk) / 2
	spread := (book.BestAsk - book.BestBid) / mid * 10000
	bidLiquidity := sumLiquidity(book.Bids, a.cfg.OrderBookLevels)
	askLiquidity := sumLiquidity(book.Asks, a.cfg.OrderBookLevels)
	return spread, bidLiquidity, askLiquidity, true
}

func sumLiquidity(levels []Level, limit int) float64 {
	if len(levels) > limit {
		levels = levels[:limit]
	}
	var total float64
	for _, level := range levels {
		total += level.Price * level.Size
	}
	return total
}

func (a *Analyzer) calculateAbsorption(state *symbolState, now time.Time, current flowTotals, flowOK bool, momentum float64, momentumOK bool) (bool, bool) {
	if !flowOK || !momentumOK || current.Total <= 0 || state.startedAt.After(now.Add(-2*a.cfg.FlowWindow)) {
		return false, false
	}
	previous, ok := a.sumFlow(state, now.Add(-2*a.cfg.FlowWindow), now.Add(-a.cfg.FlowWindow))
	if !ok || previous.Buy <= 0 {
		return false, false
	}
	buyShare := current.Buy / current.Total
	isAbsorption := buyShare >= a.cfg.AbsorptionMinBuyShare &&
		current.Buy >= previous.Buy*a.cfg.AbsorptionBuyVolumeMultiplier &&
		math.Abs(momentum) <= a.cfg.AbsorptionMaxPriceChangePct
	return isAbsorption, true
}

func floatPointer(value float64) *float64 { return &value }
func boolPointer(value bool) *bool        { return &value }
