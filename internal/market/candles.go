package market

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
)

const (
	CandleStatusWarmingUp    = "warming_up"
	CandleStatusReady        = "ready"
	CandleStatusStale        = "stale"
	CandleStatusDisconnected = "disconnected"
)

var ErrCandleGap = errors.New("обнаружен пропуск минутной свечи")

const candleInterval = time.Minute

// Candle использует turnover в USDT для межрыночного сравнения.
// Volume остаётся объёмом в базовой монете.
type Candle struct {
	StartTime time.Time
	EndTime   time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	Turnover  float64
	Confirmed bool
	UpdatedAt time.Time
}

type CandleSnapshot struct {
	Symbol        string
	Status        string
	History       []Candle
	Current       *Candle
	HistoryReady  bool
	Connected     bool
	Fresh         bool
	Active        bool
	LastUpdatedAt time.Time
	LastError     string
}

type candleState struct {
	active        bool
	connected     bool
	everConnected bool
	historyReady  bool
	history       []Candle
	wsConfirmed   map[int64]Candle
	current       *Candle
	lastUpdatedAt time.Time
	lastError     string
}

// CandleStore хранит ограниченную историю отдельно от Store сделок и стаканов.
// На каждый активный инструмент остаётся максимум 20 закрытых и одна текущая свеча.
type CandleStore struct {
	mu             sync.RWMutex
	historyLimit   int
	maxDataAge     time.Duration
	states         map[string]*candleState
}

func NewCandleStore(maxDataAge time.Duration) *CandleStore {
	if maxDataAge <= 0 {
		maxDataAge = 90 * time.Second
	}
	return &CandleStore{
		historyLimit: 20,
		maxDataAge:   maxDataAge,
		states:       make(map[string]*candleState),
	}
}

func (s *CandleStore) Activate(symbol string) {
	symbol = normalizeSymbol(symbol)
	if symbol == "" {
		return
	}
	s.mu.Lock()
	s.states[symbol] = &candleState{
		active:      true,
		wsConfirmed: make(map[int64]Candle),
	}
	s.mu.Unlock()
}

func (s *CandleStore) MarkInactive(symbol string) {
	s.mu.Lock()
	delete(s.states, normalizeSymbol(symbol))
	s.mu.Unlock()
}

func (s *CandleStore) SetHistoryError(symbol string, err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.states[normalizeSymbol(symbol)]
	if state == nil || !state.active {
		return
	}
	state.historyReady = false
	state.lastError = err.Error()
}

// LoadHistory принимает REST-историю, отбрасывает текущую незавершённую минуту,
// объединяет её с закрытыми свечами, уже пришедшими по WebSocket, и проверяет непрерывность.
func (s *CandleStore) LoadHistory(symbol string, candles []bybit.Kline, now time.Time) error {
	symbol = normalizeSymbol(symbol)
	if symbol == "" {
		return errors.New("пустой символ Kline")
	}
	if now.IsZero() {
		now = time.Now()
	}
	currentMinute := now.UTC().Truncate(candleInterval)
	byStart := make(map[int64]Candle, len(candles)+20)

	for _, item := range candles {
		candle := candleFromREST(item)
		if err := validateCandle(candle); err != nil {
			return fmt.Errorf("история %s: %w", symbol, err)
		}
		if !candle.StartTime.Before(currentMinute) {
			continue
		}
		byStart[candle.StartTime.UnixMilli()] = candle
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.states[symbol]
	if state == nil || !state.active {
		return nil
	}

	// Более свежие подтверждённые WebSocket-свечи имеют приоритет над REST,
	// если они успели прийти во время параллельной загрузки истории.
	for key, candle := range state.wsConfirmed {
		byStart[key] = candle
	}

	merged := make([]Candle, 0, len(byStart))
	for _, candle := range byStart {
		merged = append(merged, candle)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].StartTime.Before(merged[j].StartTime)
	})
	merged = deduplicateCandles(merged)
	if len(merged) > s.historyLimit {
		merged = merged[len(merged)-s.historyLimit:]
	}
	state.history = merged
	state.historyReady = hasContinuousHistory(merged, s.historyLimit, currentMinute)
	state.lastError = ""
	if !state.historyReady {
		state.lastError = fmt.Sprintf("для готовности нужно %d последовательных завершённых минутных свечей; получено %d", s.historyLimit, len(merged))
	}
	return nil
}

// ApplyUpdate обновляет только текущую свечу либо закрывает одну минуту.
// Повторные обновления одного timestamp не добавляют новые записи истории.
func (s *CandleStore) ApplyUpdate(symbol string, candle Candle, receivedAt time.Time) error {
	symbol = normalizeSymbol(symbol)
	if symbol == "" {
		return errors.New("пустой символ Kline")
	}
	if receivedAt.IsZero() {
		return errors.New("отсутствует время получения Kline")
	}
	if err := validateCandle(candle); err != nil {
		return err
	}
	if !candle.StartTime.Equal(candle.StartTime.UTC().Truncate(candleInterval)) {
		return errors.New("startTime минутной свечи не выровнен по минуте")
	}
	if candle.StartTime.After(receivedAt.UTC().Truncate(candleInterval).Add(candleInterval)) {
		return errors.New("получена свеча из будущего")
	}
	candle.UpdatedAt = receivedAt.UTC()

	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.states[symbol]
	if state == nil || !state.active {
		return nil
	}

	state.connected = true
	state.everConnected = true
	state.lastUpdatedAt = receivedAt.UTC()
	if candle.Confirmed {
		key := candle.StartTime.UnixMilli()
		if state.history == nil {
			state.history = make([]Candle, 0, s.historyLimit)
		}
		if _, exists := findCandle(state.history, key); exists {
			replaceCandle(state.history, key, candle)
		} else {
			if len(state.history) > 0 {
				last := state.history[len(state.history)-1]
				if !candle.StartTime.Equal(last.StartTime.Add(candleInterval)) {
					state.historyReady = false
					state.lastError = fmt.Sprintf("%v: после %s пришла %s", ErrCandleGap, last.StartTime, candle.StartTime)
					state.wsConfirmed[key] = candle
					trimWSConfirmed(state.wsConfirmed, s.historyLimit)
					return ErrCandleGap
				}
			}
			state.history = append(state.history, candle)
		}
		state.wsConfirmed[key] = candle
		trimWSConfirmed(state.wsConfirmed, s.historyLimit)
		if len(state.history) > s.historyLimit {
			state.history = append([]Candle(nil), state.history[len(state.history)-s.historyLimit:]...)
		}
		if state.current != nil && !state.current.StartTime.After(candle.StartTime) {
			state.current = nil
		}
		state.historyReady = hasContinuousHistory(state.history, s.historyLimit, receivedAt.UTC().Truncate(candleInterval))
		if state.historyReady {
			state.lastError = ""
		}
		return nil
	}

	if state.current != nil {
		if candle.StartTime.Before(state.current.StartTime) {
			return nil
		}
		if candle.StartTime.After(state.current.StartTime) {
			state.historyReady = false
			state.lastError = fmt.Sprintf("текущая свеча %s сменилась на %s без confirm=true", state.current.StartTime, candle.StartTime)
			state.current = &candle
			return ErrCandleGap
		}
	}
	state.current = &candle
	return nil
}

func (s *CandleStore) MarkDisconnected(symbol string, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.states[normalizeSymbol(symbol)]
	if state == nil || !state.active {
		return
	}
	state.connected = false
	state.historyReady = false
	state.current = nil
	state.lastUpdatedAt = time.Time{}
	state.wsConfirmed = make(map[int64]Candle)
	if reason == "" {
		reason = "WebSocket-сегмент отключён; требуется обновить историю"
	}
	state.lastError = reason
}

func (s *CandleStore) Snapshot(symbol string, now time.Time) (CandleSnapshot, bool) {
	symbol = normalizeSymbol(symbol)
	s.mu.RLock()
	defer s.mu.RUnlock()
	state := s.states[symbol]
	if state == nil || !state.active {
		return CandleSnapshot{}, false
	}

	fresh := !state.lastUpdatedAt.IsZero() &&
		!now.Before(state.lastUpdatedAt) &&
		now.Sub(state.lastUpdatedAt) <= s.maxDataAge
	status := CandleStatusWarmingUp
	switch {
	case state.everConnected && !state.connected:
		status = CandleStatusDisconnected
	case !state.historyReady:
		status = CandleStatusWarmingUp
	case !state.everConnected:
		status = CandleStatusWarmingUp
	case !fresh:
		status = CandleStatusStale
	case state.current == nil || !state.current.StartTime.Equal(now.UTC().Truncate(candleInterval)):
		status = CandleStatusWarmingUp
	default:
		status = CandleStatusReady
	}
	result := CandleSnapshot{
		Symbol: symbol,
		Status: status,
		History: append([]Candle(nil), state.history...),
		HistoryReady: state.historyReady,
		Connected: state.connected,
		Fresh: fresh,
		Active: state.active,
		LastUpdatedAt: state.lastUpdatedAt,
		LastError: state.lastError,
	}
	if state.current != nil {
		current := *state.current
		result.Current = &current
	}
	return result, true
}

func (s *CandleStore) ActiveSymbols() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]string, 0, len(s.states))
	for symbol, state := range s.states {
		if state.active {
			result = append(result, symbol)
		}
	}
	sort.Strings(result)
	return result
}

func candleFromREST(item bybit.Kline) Candle {
	return Candle{
		StartTime: item.StartTime.UTC(),
		EndTime: item.StartTime.UTC().Add(candleInterval - time.Millisecond),
		Open: item.Open, High: item.High, Low: item.Low, Close: item.Close,
		Volume: item.Volume, Turnover: item.Turnover, Confirmed: true,
	}
}

func validateCandle(c Candle) error {
	if c.StartTime.IsZero() || !finiteCandle(c.Open) || !finiteCandle(c.High) ||
		!finiteCandle(c.Low) || !finiteCandle(c.Close) || !finiteCandle(c.Volume) ||
		!finiteCandle(c.Turnover) || c.Open <= 0 || c.High <= 0 || c.Low <= 0 ||
		c.Close <= 0 || c.Volume < 0 || c.Turnover < 0 {
		return errors.New("некорректные значения минутной свечи")
	}
	if c.High < math.Max(c.Open, c.Close) || c.Low > math.Min(c.Open, c.Close) || c.High < c.Low {
		return errors.New("некорректные high/low минутной свечи")
	}
	return nil
}

func finiteCandle(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func hasContinuousHistory(candles []Candle, required int, nowMinute time.Time) bool {
	if required <= 0 || len(candles) < required {
		return false
	}
	recent := candles[len(candles)-required:]
	for i := 1; i < len(recent); i++ {
		if !recent[i].StartTime.Equal(recent[i-1].StartTime.Add(candleInterval)) {
			return false
		}
	}
	if nowMinute.IsZero() {
		return true
	}
	latestAllowed := nowMinute.UTC().Truncate(candleInterval).Add(-candleInterval)
	return recent[len(recent)-1].StartTime.Equal(latestAllowed)
}

func deduplicateCandles(candles []Candle) []Candle {
	if len(candles) < 2 {
		return candles
	}
	result := candles[:0]
	for _, candle := range candles {
		if len(result) > 0 && result[len(result)-1].StartTime.Equal(candle.StartTime) {
			result[len(result)-1] = candle
			continue
		}
		result = append(result, candle)
	}
	return result
}

func findCandle(candles []Candle, key int64) (Candle, bool) {
	for _, candle := range candles {
		if candle.StartTime.UnixMilli() == key {
			return candle, true
		}
	}
	return Candle{}, false
}

func replaceCandle(candles []Candle, key int64, replacement Candle) {
	for index := range candles {
		if candles[index].StartTime.UnixMilli() == key {
			candles[index] = replacement
			return
		}
	}
}

func trimWSConfirmed(candles map[int64]Candle, limit int) {
	if len(candles) <= limit {
		return
	}
	keys := make([]int64, 0, len(candles))
	for key := range candles {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, key := range keys[:len(keys)-limit] {
		delete(candles, key)
	}
}
