// Package strategy формирует подтверждённые сигналы LONG по агрегированным метрикам рынка.
package strategy

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	marketanalysis "github.com/Quantum103/Scrinner_PAMPA/internal/analysis"
)

const (
	DecisionDisabled   = "disabled"
	DecisionWaiting    = "waiting_data"
	DecisionConfirming = "confirming"
	DecisionCooldown   = "cooldown"
	DecisionLatched    = "signal_latched"
)

type Config struct {
	Enabled                    bool
	MinRVOL                    float64
	MinTakerBuySharePercent    float64
	MaxPriceChangePercent      float64
	PriceChangeWindow          time.Duration
	MaxSpreadBPS               float64
	MinBidLiquidityQuote       float64
	MinAskLiquidityQuote       float64
	MinBookLevels              int
	MinTradesInFlowWindow      int
	MaxSingleTradeSharePercent float64
	ConfirmationObservations   int
	ObservationInterval        time.Duration
	MaxObservationGap          time.Duration
	SignalCooldown             time.Duration
	RearmObservations          int
	SignalHistoryLimit         int
}

func DefaultConfig() Config {
	return Config{
		Enabled:                    true,
		MinRVOL:                    3,
		MinTakerBuySharePercent:    65,
		MaxPriceChangePercent:      1.5,
		PriceChangeWindow:          5 * time.Minute,
		MaxSpreadBPS:               15,
		MinBidLiquidityQuote:       5000,
		MinAskLiquidityQuote:       5000,
		MinBookLevels:              5,
		MinTradesInFlowWindow:      5,
		MaxSingleTradeSharePercent: 35,
		ConfirmationObservations:   3,
		ObservationInterval:        time.Second,
		MaxObservationGap:          3 * time.Second,
		SignalCooldown:             time.Minute,
		RearmObservations:          2,
		SignalHistoryLimit:         20,
	}
}

func (c Config) Validate() error {
	if c.MinRVOL <= 0 || !finite(c.MinRVOL) {
		return errors.New("ENTRY_MIN_RVOL должен быть положительным")
	}
	if c.MinTakerBuySharePercent <= 50 || c.MinTakerBuySharePercent > 100 || !finite(c.MinTakerBuySharePercent) {
		return errors.New("ENTRY_MIN_TAKER_BUY_SHARE_PERCENT должен быть в диапазоне (50, 100]")
	}
	if c.MaxPriceChangePercent <= 0 || !finite(c.MaxPriceChangePercent) {
		return errors.New("ENTRY_MAX_PRICE_CHANGE_PERCENT должен быть положительным")
	}
	if c.PriceChangeWindow < time.Second || c.PriceChangeWindow%time.Second != 0 {
		return errors.New("ENTRY_PRICE_CHANGE_WINDOW должен быть не меньше секунды и кратен секунде")
	}
	if c.MaxSpreadBPS <= 0 || !finite(c.MaxSpreadBPS) {
		return errors.New("ENTRY_MAX_SPREAD_BPS должен быть положительным")
	}
	if c.MinBidLiquidityQuote <= 0 || !finite(c.MinBidLiquidityQuote) || c.MinAskLiquidityQuote <= 0 || !finite(c.MinAskLiquidityQuote) {
		return errors.New("минимальная ликвидность bid/ask должна быть положительной")
	}
	if c.MinBookLevels <= 0 || c.MinBookLevels > 50 {
		return errors.New("ENTRY_MIN_BOOK_LEVELS должен быть в диапазоне 1..50")
	}
	if c.MinTradesInFlowWindow < 2 || c.MinTradesInFlowWindow > 100000 {
		return errors.New("ENTRY_MIN_TRADES_IN_FLOW_WINDOW должен быть в диапазоне 2..100000")
	}
	if c.MaxSingleTradeSharePercent <= 0 || c.MaxSingleTradeSharePercent >= 100 || !finite(c.MaxSingleTradeSharePercent) {
		return errors.New("ENTRY_MAX_SINGLE_TRADE_SHARE_PERCENT должен быть в диапазоне (0, 100)")
	}
	if c.ConfirmationObservations < 2 || c.ConfirmationObservations > 100 {
		return errors.New("ENTRY_CONFIRMATION_OBSERVATIONS должен быть в диапазоне 2..100")
	}
	if c.ObservationInterval < 100*time.Millisecond || c.ObservationInterval > time.Minute {
		return errors.New("ENTRY_OBSERVATION_INTERVAL должен быть в диапазоне от 100ms до 1m")
	}
	if c.MaxObservationGap < c.ObservationInterval || c.MaxObservationGap > 10*c.ObservationInterval {
		return errors.New("ENTRY_MAX_OBSERVATION_GAP должен быть от одного до десяти интервалов наблюдения")
	}
	if c.SignalCooldown <= 0 {
		return errors.New("ENTRY_SIGNAL_COOLDOWN должен быть положительным")
	}
	if c.RearmObservations <= 0 || c.RearmObservations > 100 {
		return errors.New("ENTRY_REARM_OBSERVATIONS должен быть в диапазоне 1..100")
	}
	if c.SignalHistoryLimit <= 0 || c.SignalHistoryLimit > 1000 {
		return errors.New("ENTRY_SIGNAL_HISTORY_LIMIT должен быть в диапазоне 1..1000")
	}
	return nil
}

type Signal struct {
	Symbol               string    `json:"symbol"`
	Direction            string    `json:"direction"`
	Price                float64   `json:"price"`
	Time                 time.Time `json:"time"`
	RVOL                 float64   `json:"rvol"`
	TakerBuySharePercent float64   `json:"taker_buy_share_percent"`
	TakerBuyVolume       float64   `json:"taker_buy_volume"`
	Delta                float64   `json:"delta"`
	CVD                  float64   `json:"cvd"`
	PriceChangePercent   float64   `json:"price_change_percent"`
	PriceChangeWindow    string    `json:"price_change_window"`
	SpreadBPS            float64   `json:"spread_bps"`
	BidLiquidityQuote    float64   `json:"bid_liquidity_quote"`
	AskLiquidityQuote    float64   `json:"ask_liquidity_quote"`
	Reason               string    `json:"reason"`
}

type Decision struct {
	State                string    `json:"state"`
	Reason               string    `json:"reason"`
	ConfirmationCount    int       `json:"confirmation_count"`
	ConfirmationRequired int       `json:"confirmation_required"`
	CooldownUntil        time.Time `json:"cooldown_until,omitempty"`
	LastSignal           *Signal   `json:"last_signal,omitempty"`
	History              []Signal  `json:"history,omitempty"`
}

type symbolState struct {
	lastObservationAt  time.Time
	lastCVD            *float64
	confirmations      int
	failedObservations int
	signalLatched      bool
	cooldownUntil      time.Time
	lastSignal         *Signal
	history            []Signal
	lastDecision       Decision
}

type Engine struct {
	mu     sync.RWMutex
	cfg    Config
	states map[string]*symbolState
}

func New(cfg Config) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Engine{cfg: cfg, states: make(map[string]*symbolState)}, nil
}

// ResetProgress сбрасывает подтверждение после потери соединения, сохраняя историю уже созданных сигналов.
func (e *Engine) ResetProgress(symbol, reason string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	state := e.ensure(symbol)
	state.lastObservationAt = time.Time{}
	state.lastCVD = nil
	state.confirmations = 0
	state.failedObservations = 0
	state.lastDecision = e.decision(state, DecisionWaiting, reason)
}

func (e *Engine) Evaluate(symbol string, market marketanalysis.Snapshot, usable bool, bidLevels, askLevels int, now time.Time) (Decision, *Signal) {
	e.mu.Lock()
	defer e.mu.Unlock()
	state := e.ensure(symbol)

	if !e.cfg.Enabled {
		state.lastDecision = e.decision(state, DecisionDisabled, "поиск точек входа отключён конфигурацией")
		return state.lastDecision, nil
	}
	if !usable || market.Status != marketanalysis.StatusReady {
		state.confirmations = 0
		state.lastObservationAt = now
		state.lastCVD = nil
		state.lastDecision = e.decision(state, DecisionWaiting, dataReason(market, usable))
		return state.lastDecision, nil
	}
	if now.IsZero() || market.UpdatedAt.IsZero() || now.Before(market.UpdatedAt) {
		state.confirmations = 0
		state.lastObservationAt = now
		state.lastCVD = nil
		state.lastDecision = e.decision(state, DecisionWaiting, "снимок рынка устарел или имеет некорректное время")
		return state.lastDecision, nil
	}
	if !state.lastObservationAt.IsZero() {
		elapsed := now.Sub(state.lastObservationAt)
		if elapsed < e.cfg.ObservationInterval {
			return state.lastDecision, nil
		}
		if elapsed > e.cfg.MaxObservationGap {
			state.confirmations = 0
			state.failedObservations = 0
			state.lastCVD = nil
		}
	}
	state.lastObservationAt = now

	ok, reason, values := e.conditions(market, bidLevels, askLevels, state.lastCVD)
	if market.CVD != nil {
		currentCVD := *market.CVD
		state.lastCVD = &currentCVD
	} else {
		state.lastCVD = nil
	}

	if !ok {
		state.confirmations = 0
		state.failedObservations++
		if state.signalLatched && state.failedObservations >= e.cfg.RearmObservations {
			state.signalLatched = false
			state.failedObservations = 0
		}
		if now.Before(state.cooldownUntil) {
			state.lastDecision = e.decision(state, DecisionCooldown, fmt.Sprintf("%s; повторный сигнал возможен после cooldown", reason))
		} else if state.signalLatched {
			state.lastDecision = e.decision(state, DecisionLatched, fmt.Sprintf("%s; сигнал уже формировался, ожидается повторная активация условий", reason))
		} else {
			state.lastDecision = e.decision(state, DecisionWaiting, reason)
		}
		return state.lastDecision, nil
	}

	state.failedObservations = 0
	if state.signalLatched {
		state.confirmations = 0
		state.lastDecision = e.decision(state, DecisionLatched, "сигнал уже выдавался; ожидается сброс условий для повторной активации")
		return state.lastDecision, nil
	}
	if state.confirmations < e.cfg.ConfirmationObservations {
		state.confirmations++
	}
	if state.confirmations < e.cfg.ConfirmationObservations {
		state.lastDecision = e.decision(state, DecisionConfirming, fmt.Sprintf("условия LONG подтверждены %d из %d последовательных наблюдений", state.confirmations, e.cfg.ConfirmationObservations))
		return state.lastDecision, nil
	}
	if now.Before(state.cooldownUntil) {
		state.lastDecision = e.decision(state, DecisionCooldown, "условия подтверждены, но действует защита от повторных сигналов")
		return state.lastDecision, nil
	}
	if state.signalLatched {
		state.lastDecision = e.decision(state, DecisionLatched, "сигнал уже выдавался; ожидается сброс условий для повторной активации")
		return state.lastDecision, nil
	}

	signal := &Signal{
		Symbol: symbol, Direction: "LONG", Price: values.price, Time: now.UTC(),
		RVOL: values.rvol, TakerBuySharePercent: values.buyShare, TakerBuyVolume: values.buyVolume,
		Delta: values.delta, CVD: values.cvd, PriceChangePercent: values.priceChange,
		PriceChangeWindow: e.cfg.PriceChangeWindow.String(), SpreadBPS: values.spread,
		BidLiquidityQuote: values.bidLiquidity, AskLiquidityQuote: values.askLiquidity,
		Reason: "RVOL, taker-buy, положительная Delta и растущий CVD подтверждены " + fmt.Sprintf("%d последовательными наблюдениями; цена не выросла выше %.2f%%, absorption отсутствует, ликвидность и спред соответствуют ограничениям", e.cfg.ConfirmationObservations, e.cfg.MaxPriceChangePercent),
	}
	state.lastSignal = signal
	state.history = append(state.history, *signal)
	if len(state.history) > e.cfg.SignalHistoryLimit {
		state.history = append([]Signal(nil), state.history[len(state.history)-e.cfg.SignalHistoryLimit:]...)
	}
	state.signalLatched = true
	state.cooldownUntil = now.Add(e.cfg.SignalCooldown)
	state.confirmations = 0
	state.lastDecision = e.decision(state, DecisionLatched, "сформирован новый сигнал LONG; реальный ордер не отправлялся")
	return state.lastDecision, signal
}

func (e *Engine) Snapshot(symbol string) Decision {
	e.mu.RLock()
	defer e.mu.RUnlock()
	state := e.states[symbol]
	if state == nil {
		return Decision{State: DecisionWaiting, Reason: "ещё нет наблюдений стратегии", ConfirmationRequired: e.cfg.ConfirmationObservations}
	}
	return cloneDecision(state.lastDecision)
}

func (e *Engine) ensure(symbol string) *symbolState {
	state := e.states[symbol]
	if state == nil {
		state = &symbolState{}
		state.lastDecision = e.decision(state, DecisionWaiting, "ожидание первого наблюдения")
		e.states[symbol] = state
	}
	return state
}

func (e *Engine) decision(state *symbolState, status, reason string) Decision {
	return Decision{
		State: status, Reason: reason, ConfirmationCount: state.confirmations,
		ConfirmationRequired: e.cfg.ConfirmationObservations, CooldownUntil: state.cooldownUntil,
		LastSignal: cloneSignal(state.lastSignal), History: append([]Signal(nil), state.history...),
	}
}

func cloneDecision(value Decision) Decision {
	value.LastSignal = cloneSignal(value.LastSignal)
	value.History = append([]Signal(nil), value.History...)
	return value
}

func cloneSignal(value *Signal) *Signal {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

type signalValues struct {
	price, rvol, buyShare, buyVolume, delta, cvd, priceChange, spread, bidLiquidity, askLiquidity float64
}

func (e *Engine) conditions(m marketanalysis.Snapshot, bidLevels, askLevels int, previousCVD *float64) (bool, string, signalValues) {
	var values signalValues
	if m.RVOL == nil || m.TakerBuySharePercent == nil || m.TakerBuyVolume == nil || m.Delta == nil || m.CVD == nil ||
		m.LastPrice == nil || m.SpreadBPS == nil || m.BidLiquidityQuote == nil || m.AskLiquidityQuote == nil ||
		m.LargestTradeSharePercent == nil {
		return false, "не хватает обязательных показателей рынка", values
	}
	moveKey := e.cfg.PriceChangeWindow.String()
	priceChange, exists := m.MomentumPercent[moveKey]
	if !exists {
		return false, fmt.Sprintf("нет Momentum за настроенное окно %s", moveKey), values
	}
	values = signalValues{
		price: *m.LastPrice, rvol: *m.RVOL, buyShare: *m.TakerBuySharePercent, buyVolume: *m.TakerBuyVolume,
		delta: *m.Delta, cvd: *m.CVD, priceChange: priceChange, spread: *m.SpreadBPS,
		bidLiquidity: *m.BidLiquidityQuote, askLiquidity: *m.AskLiquidityQuote,
	}
	checks := []struct {
		ok     bool
		reason string
	}{
		{values.rvol >= e.cfg.MinRVOL, fmt.Sprintf("RVOL %.2f ниже порога %.2f", values.rvol, e.cfg.MinRVOL)},
		{values.buyShare >= e.cfg.MinTakerBuySharePercent, fmt.Sprintf("доля Taker Buy %.2f%% ниже порога %.2f%%", values.buyShare, e.cfg.MinTakerBuySharePercent)},
		{values.delta > 0, "Delta не положительная"},
		{values.cvd > 0, "CVD не положительный"},
		{previousCVD == nil || values.cvd > *previousCVD, "CVD не растёт относительно предыдущего наблюдения"},
		{values.priceChange <= e.cfg.MaxPriceChangePercent, fmt.Sprintf("цена уже выросла на %.3f%% за %s, максимум %.3f%%", values.priceChange, moveKey, e.cfg.MaxPriceChangePercent)},
		{m.Absorption != nil && !*m.Absorption, "обнаружено поглощение покупок"},
		{values.spread <= e.cfg.MaxSpreadBPS, fmt.Sprintf("спред %.2f bps превышает %.2f bps", values.spread, e.cfg.MaxSpreadBPS)},
		{values.bidLiquidity >= e.cfg.MinBidLiquidityQuote, fmt.Sprintf("ликвидность bid %.2f меньше %.2f USDT", values.bidLiquidity, e.cfg.MinBidLiquidityQuote)},
		{values.askLiquidity >= e.cfg.MinAskLiquidityQuote, fmt.Sprintf("ликвидность ask %.2f меньше %.2f USDT", values.askLiquidity, e.cfg.MinAskLiquidityQuote)},
		{bidLevels >= e.cfg.MinBookLevels && askLevels >= e.cfg.MinBookLevels, fmt.Sprintf("недостаточная глубина стакана: bid=%d ask=%d, нужно минимум %d уровней", bidLevels, askLevels, e.cfg.MinBookLevels)},
		{m.FlowTradeCount >= e.cfg.MinTradesInFlowWindow, fmt.Sprintf("в окне недостаточно сделок: %d, нужно минимум %d", m.FlowTradeCount, e.cfg.MinTradesInFlowWindow)},
		{*m.LargestTradeSharePercent <= e.cfg.MaxSingleTradeSharePercent, fmt.Sprintf("одна сделка составляет %.2f%% потока, максимум %.2f%%", *m.LargestTradeSharePercent, e.cfg.MaxSingleTradeSharePercent)},
	}
	for _, check := range checks {
		if !check.ok {
			return false, check.reason, values
		}
	}
	return true, "все фильтры LONG выполнены", values
}

func dataReason(m marketanalysis.Snapshot, usable bool) string {
	if !usable {
		return "рыночные данные непригодны для сигнала: соединение, стакан, последняя сделка или непрерывность потока не подтверждены"
	}
	if m.Reason != "" {
		return fmt.Sprintf("анализатор не готов (%s): %s", m.Status, m.Reason)
	}
	return fmt.Sprintf("анализатор не готов: статус %s", m.Status)
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
