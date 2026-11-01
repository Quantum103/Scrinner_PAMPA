package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	marketanalysis "github.com/Quantum103/Scrinner_PAMPA/internal/analysis"
	marketstrategy "github.com/Quantum103/Scrinner_PAMPA/internal/strategy"
)

type Trade struct {
	Symbol    string    `json:"symbol"`
	Price     string    `json:"price"`
	Quantity  string    `json:"quantity"`
	Side      string    `json:"side"`
	TradeTime time.Time `json:"timestamp"`
	TradeID   string    `json:"trade_id"`
	Received  time.Time `json:"received_at"`
}

type rawTrade struct {
	Timestamp int64  `json:"T"`
	Symbol    string `json:"s"`
	Side      string `json:"S"`
	Quantity  string `json:"v"`
	Price     string `json:"p"`
	ID        string `json:"i"`
}

type tradeMessage struct {
	Topic string     `json:"topic"`
	Type  string     `json:"type"`
	TS    int64      `json:"ts"`
	Data  []rawTrade `json:"data"`
}

type SymbolSnapshot struct {
	Analysis            marketanalysis.Snapshot `json:"analysis"`
	Symbol              string                  `json:"symbol"`
	OrderBook           OrderBookSnapshot       `json:"orderbook"`
	LastTrade           *Trade                  `json:"last_trade,omitempty"`
	Connected           bool                    `json:"connected"`
	TradeGap            bool                    `json:"trade_gap"`
	DroppedTrades       uint64                  `json:"dropped_trades"`
	DuplicateTrades     uint64                  `json:"duplicate_trades"`
	LastTradeReceivedAt time.Time               `json:"last_trade_received_at"`
	UsableForSignal     bool                    `json:"usable_for_signal"`
	EntryStrategy       marketstrategy.Decision `json:"entry_strategy"`
}

type symbolState struct {
	book          *orderBook
	connected     bool
	everConnected bool
	tradeGap      bool
	lastTrade     Trade
	hasTrade      bool
	lastTradeAt   time.Time
	trades        []Trade
	tradeStart    int
	tradeCount    int
	droppedTrades uint64
	duplicates    uint64
	lastStrategyEvaluationAt time.Time
	seenIDs       map[string]struct{}
	seenOrder     []string
	seenNext      int
}

// Store обрабатывает WebSocket-сообщения синхронно, без каналов событий, которые могут переполниться.
type Store struct {
	mu            sync.RWMutex
	states        map[string]*symbolState
	depth         int
	maxDataAge    time.Duration
	tradeCapacity int
	seenCapacity  int
	analyzer         *marketanalysis.Analyzer
	strategy         *marketstrategy.Engine
	strategyInterval time.Duration
	pendingSignals   []marketstrategy.Signal
}

func NewStore(depth int, maxDataAge time.Duration, tradeCapacity, seenCapacity int) (*Store, error) {
	return NewStoreWithAnalysis(depth, maxDataAge, tradeCapacity, seenCapacity, marketanalysis.DefaultConfig())
}

func NewStoreWithAnalysis(depth int, maxDataAge time.Duration, tradeCapacity, seenCapacity int, analysisConfig marketanalysis.Config) (*Store, error) {
	return NewStoreWithAnalysisAndStrategy(depth, maxDataAge, tradeCapacity, seenCapacity, analysisConfig, marketstrategy.DefaultConfig())
}

func NewStoreWithAnalysisAndStrategy(depth int, maxDataAge time.Duration, tradeCapacity, seenCapacity int, analysisConfig marketanalysis.Config, strategyConfig marketstrategy.Config) (*Store, error) {
	if depth <= 0 || maxDataAge <= 0 || tradeCapacity <= 0 || seenCapacity <= 0 {
		return nil, errors.New("глубина, срок актуальности и размеры буферов должны быть больше нуля")
	}
	analyzer, err := marketanalysis.New(analysisConfig, maxDataAge)
	if err != nil {
		return nil, fmt.Errorf("создать анализатор рынка: %w", err)
	}
	entryStrategy, err := marketstrategy.New(strategyConfig)
	if err != nil {
		return nil, fmt.Errorf("создать стратегию поиска входа: %w", err)
	}
	return &Store{
		states:           make(map[string]*symbolState),
		depth:            depth,
		maxDataAge:       maxDataAge,
		tradeCapacity:    tradeCapacity,
		seenCapacity:     seenCapacity,
		analyzer:         analyzer,
		strategy:         entryStrategy,
		strategyInterval: strategyConfig.ObservationInterval,
	}, nil
}

func (s *Store) ensure(symbol string) *symbolState {
	state := s.states[symbol]
	if state == nil {
		state = &symbolState{
			book:      newOrderBook(),
			trades:    make([]Trade, s.tradeCapacity),
			seenIDs:   make(map[string]struct{}, s.seenCapacity),
			seenOrder: make([]string, s.seenCapacity),
		}
		s.states[symbol] = state
	}
	return state
}

func (s *Store) StartSession(symbols []string) {
	s.startSessionAt(symbols, time.Now())
}

func (s *Store) startSessionAt(symbols []string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, symbol := range symbols {
		state := s.ensure(symbol)
		reconnected := state.everConnected
		if reconnected {
			state.tradeGap = true
		}
		state.connected = true
		state.everConnected = true
		state.book.invalidate()
		state.lastStrategyEvaluationAt = time.Time{}
		s.analyzer.StartSession(symbol, at, reconnected)
		s.strategy.ResetProgress(symbol, "новая WebSocket-сессия; накопление последовательных наблюдений начнётся заново")
	}
}

func (s *Store) MarkDisconnected(symbols []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, symbol := range symbols {
		state := s.ensure(symbol)
		if state.connected {
			state.tradeGap = true
		}
		state.connected = false
		state.book.invalidate()
		state.lastStrategyEvaluationAt = time.Time{}
		s.analyzer.MarkDisconnected(symbol, time.Now())
		s.strategy.ResetProgress(symbol, "WebSocket отключён; новые сигналы заблокированы")
	}
}

func (s *Store) MarkRemoved(activeSymbols []string) {
	active := make(map[string]struct{}, len(activeSymbols))
	for _, symbol := range activeSymbols {
		active[symbol] = struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for symbol, state := range s.states {
		if _, ok := active[symbol]; !ok {
			state.connected = false
			state.book.invalidate()
			state.lastStrategyEvaluationAt = time.Time{}
			s.analyzer.MarkDisconnected(symbol, time.Now())
			s.strategy.ResetProgress(symbol, "инструмент удалён из активной подписки")
		}
	}
}

func (s *Store) ApplyOrderBook(message []byte, receivedAt time.Time) error {
	var parsed orderBookMessage
	if err := json.Unmarshal(message, &parsed); err != nil {
		return fmt.Errorf("разобрать сообщение стакана: %w", err)
	}
	if parsed.Data.Symbol == "" {
		return errors.New("сообщение стакана не содержит символ")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.ensure(parsed.Data.Symbol)
	if !state.connected {
		state.book.invalidate()
		return errors.New("получено обновление стакана вне активной WebSocket-сессии")
	}
	if err := state.book.apply(parsed, s.depth, s.maxDataAge, receivedAt); err != nil {
		// Сохраняем время некорректного обновления: анализатор отличит повреждённый стакан от ещё не полученного snapshot.
		state.book.receivedAt = receivedAt
		state.lastStrategyEvaluationAt = time.Time{}
		s.strategy.ResetProgress(parsed.Data.Symbol, "ошибка обновления стакана; подтверждение входа сброшено")
		return err
	}
	s.evaluateStrategy(parsed.Data.Symbol, receivedAt)
	return nil
}

func (s *Store) ApplyTrades(message []byte, receivedAt time.Time) error {
	var parsed tradeMessage
	if err := json.Unmarshal(message, &parsed); err != nil {
		return fmt.Errorf("разобрать сообщение сделок: %w", err)
	}
	if len(parsed.Data) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	touched := make(map[string]struct{}, len(parsed.Data))
	for _, item := range parsed.Data {
		if item.Symbol == "" || item.ID == "" || item.Timestamp <= 0 {
			return errors.New("сделка не содержит символ, ID или время")
		}
		if item.Side != "Buy" && item.Side != "Sell" {
			return fmt.Errorf("сделка %s имеет неизвестную сторону %q", item.ID, item.Side)
		}
		if _, err := parsePositive(item.Price); err != nil {
			return fmt.Errorf("сделка %s: %w", item.ID, err)
		}
		if _, err := parsePositive(item.Quantity); err != nil {
			return fmt.Errorf("сделка %s: %w", item.ID, err)
		}

		state := s.ensure(item.Symbol)
		if !state.connected {
			return fmt.Errorf("сделка для %s получена вне активной WebSocket-сессии", item.Symbol)
		}
		if state.hasTrade && (receivedAt.Before(state.lastTradeAt) || receivedAt.Sub(state.lastTradeAt) > s.maxDataAge) {
			state.tradeGap = true
			s.analyzer.MarkGap(item.Symbol, receivedAt, "обнаружен разрыв между полученными сделками")
			s.strategy.ResetProgress(item.Symbol, "обнаружен разрыв между полученными сделками")
		}
		if _, exists := state.seenIDs[item.ID]; exists {
			state.duplicates++
			continue
		}
		state.rememberTradeID(item.ID, s.seenCapacity)

		tradeTime := time.UnixMilli(item.Timestamp).UTC()
		trade := Trade{
			Symbol:    item.Symbol,
			Price:     item.Price,
			Quantity:  item.Quantity,
			Side:      item.Side,
			TradeTime: tradeTime,
			TradeID:   item.ID,
			Received:  receivedAt,
		}
		if err := s.analyzer.AddTrade(item.Symbol, marketanalysis.Trade{
			Time: trade.TradeTime, ReceivedAt: trade.Received,
			Price: mustParsePositive(trade.Price), Quantity: mustParsePositive(trade.Quantity), Side: trade.Side,
		}); err != nil {
			state.tradeGap = true
			return fmt.Errorf("обновить анализатор рынка для %s: %w", item.Symbol, err)
		}
		state.lastTrade = trade
		state.lastTradeAt = receivedAt
		touched[item.Symbol] = struct{}{}
		state.hasTrade = true
		if state.tradeCount == s.tradeCapacity {
			state.tradeStart = (state.tradeStart + 1) % s.tradeCapacity
			state.tradeCount--
			state.droppedTrades++
			state.tradeGap = true
		}
		index := (state.tradeStart + state.tradeCount) % s.tradeCapacity
		state.trades[index] = trade
		state.tradeCount++
	}
	for symbol := range touched {
		s.evaluateStrategy(symbol, receivedAt)
	}
	return nil
}

func (s *symbolState) rememberTradeID(id string, capacity int) {
	if len(s.seenIDs) == capacity {
		oldest := s.seenOrder[s.seenNext]
		delete(s.seenIDs, oldest)
	}
	s.seenOrder[s.seenNext] = id
	s.seenIDs[id] = struct{}{}
	s.seenNext = (s.seenNext + 1) % capacity
}

// evaluateStrategy вызывается после каждого принятого обновления стакана или пачки сделок.
// Стратегия получает только полностью готовый анализатором и актуальный срез рынка.
func (s *Store) evaluateStrategy(symbol string, now time.Time) {
	state := s.states[symbol]
	if state == nil {
		return
	}
	if !state.lastStrategyEvaluationAt.IsZero() && now.Sub(state.lastStrategyEvaluationAt) < s.strategyInterval {
		return
	}
	state.lastStrategyEvaluationAt = now
	bookSnapshot := state.book.snapshot(symbol)
	bookSnapshot.Fresh = withinAge(now, state.book.receivedAt, s.maxDataAge)
	analysisSnapshot := s.analyzer.Snapshot(symbol, now, state.connected, analysisBook(bookSnapshot))
	usable := state.connected && state.book.valid && bookSnapshot.Fresh &&
		state.hasTrade && withinAge(now, state.lastTradeAt, s.maxDataAge) &&
		withinAge(now, state.lastTrade.TradeTime, s.maxDataAge) && !state.tradeGap
	_, signal := s.strategy.Evaluate(symbol, analysisSnapshot, usable,
		len(bookSnapshot.Bids), len(bookSnapshot.Asks), now)
	if signal != nil {
		s.pendingSignals = append(s.pendingSignals, *signal)
	}
}

// DrainSignals отдаёт новые сигналы логгеру/потребителю ровно один раз.
func (s *Store) DrainSignals() []marketstrategy.Signal {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := append([]marketstrategy.Signal(nil), s.pendingSignals...)
	s.pendingSignals = nil
	return result
}

func (s *Store) Snapshot(symbol string, now time.Time) (SymbolSnapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state := s.states[symbol]
	if state == nil {
		return SymbolSnapshot{}, false
	}

	bookSnapshot := state.book.snapshot(symbol)
	bookSnapshot.Fresh = withinAge(now, state.book.receivedAt, s.maxDataAge)
	snapshot := SymbolSnapshot{
		Symbol:              symbol,
		OrderBook:           bookSnapshot,
		Connected:           state.connected,
		TradeGap:            state.tradeGap,
		DroppedTrades:       state.droppedTrades,
		DuplicateTrades:     state.duplicates,
		LastTradeReceivedAt: state.lastTradeAt,
		EntryStrategy:       s.strategy.Snapshot(symbol),
}
	if state.hasTrade {
		lastTrade := state.lastTrade
		snapshot.LastTrade = &lastTrade
	}
	snapshot.Analysis = s.analyzer.Snapshot(symbol, now, state.connected, analysisBook(bookSnapshot))
	snapshot.UsableForSignal = state.connected && state.book.valid && bookSnapshot.Fresh &&
		state.hasTrade && withinAge(now, state.lastTradeAt, s.maxDataAge) &&
		withinAge(now, state.lastTrade.TradeTime, s.maxDataAge) && !state.tradeGap
	return snapshot, true
}

// DrainTrades возвращает накопленные сделки в порядке поступления. Флаг пропуска остаётся активным до подтверждения восстановления.
func (s *Store) DrainTrades(symbol string, limit int) []Trade {
	if limit <= 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.states[symbol]
	if state == nil || state.tradeCount == 0 {
		return nil
	}
	if limit > state.tradeCount {
		limit = state.tradeCount
	}
	result := make([]Trade, limit)
	for i := 0; i < limit; i++ {
		index := (state.tradeStart + i) % s.tradeCapacity
		result[i] = state.trades[index]
	}
	state.tradeStart = (state.tradeStart + limit) % s.tradeCapacity
	state.tradeCount -= limit
	return result
}

// AcknowledgeTradeGap вызывается потребителем только после восстановления скользящего окна сделок.
func (s *Store) AcknowledgeTradeGap(symbol string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state := s.states[symbol]; state != nil && state.connected {
		state.tradeGap = false
	}
}

func (s *Store) Symbols() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]string, 0, len(s.states))
	for symbol := range s.states {
		result = append(result, symbol)
	}
	return result
}

func analysisBook(snapshot OrderBookSnapshot) marketanalysis.Book {
	book := marketanalysis.Book{
		Valid: snapshot.Valid, Fresh: snapshot.Fresh,
		ReceivedAt: snapshot.ReceivedAt,
		BestBid:    mustParsePositive(snapshot.BestBid),
		BestAsk:    mustParsePositive(snapshot.BestAsk),
		Bids:       make([]marketanalysis.Level, 0, len(snapshot.Bids)),
		Asks:       make([]marketanalysis.Level, 0, len(snapshot.Asks)),
	}
	for _, level := range snapshot.Bids {
		book.Bids = append(book.Bids, marketanalysis.Level{Price: mustParsePositive(level.Price), Size: mustParsePositive(level.Size)})
	}
	for _, level := range snapshot.Asks {
		book.Asks = append(book.Asks, marketanalysis.Level{Price: mustParsePositive(level.Price), Size: mustParsePositive(level.Size)})
	}
	return book
}

func mustParsePositive(value string) float64 {
	parsed, _ := strconv.ParseFloat(value, 64)
	return parsed
}

func withinAge(now, timestamp time.Time, maxAge time.Duration) bool {
	if timestamp.IsZero() || now.Before(timestamp) {
		return false
	}
	return now.Sub(timestamp) <= maxAge
}

func parsePositive(value string) (float64, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed <= 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("ожидалось положительное число, получено %q", value)
	}
	return parsed, nil
}
