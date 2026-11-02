package market

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
)

const (
	klineHeartbeatInterval = 20 * time.Second
	klineReadTimeout       = 60 * time.Second
	klineWriteTimeout      = 5 * time.Second
	klineMaxMessageSize    = 4 << 20
	klineMaxArgsCharacters = 21_000
	klineSubscriptionBatchPause = 50 * time.Millisecond
)

type KlineRESTClient interface {
	FetchMinuteKlines(ctx context.Context, symbol string, limit int) ([]bybit.Kline, error)
}

type KlineConfig struct {
	HistoryLimit            int
	HistoryWorkers          int
	MaxSymbolsPerConnection int
	MaxArgsPerSubscribe     int
	MaxShards               int
	MaxDataAge              time.Duration
	ReconnectMin            time.Duration
	ReconnectMax            time.Duration
}

type klineWireConn interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(messageType int, data []byte) error
	SetReadLimit(limit int64)
	SetReadDeadline(deadline time.Time) error
	SetWriteDeadline(deadline time.Time) error
	SetPongHandler(handler func(appData string) error)
	Close() error
}

type klineConnector interface {
	Dial(ctx context.Context, url string) (klineWireConn, error)
}

type gorillaKlineConnector struct{}

func (gorillaKlineConnector) Dial(ctx context.Context, url string) (klineWireConn, error) {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

type klineSerializedWriter struct {
	mu   sync.Mutex
	conn klineWireConn
}

func (w *klineSerializedWriter) writeJSON(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.conn.SetWriteDeadline(time.Now().Add(klineWriteTimeout)); err != nil {
		return err
	}
	return w.conn.WriteMessage(websocket.TextMessage, payload)
}

type klineSubscriptionError struct {
	message string
}

func (e *klineSubscriptionError) Error() string { return e.message }

type klineSubscriptionBatch struct {
	Operation string   `json:"op"`
	Arguments []string `json:"args"`
}

type klineShard struct {
	id      int
	mu      sync.RWMutex
	symbols map[string]struct{}
	notify  chan struct{}
}

func newKlineShard(id int) *klineShard {
	return &klineShard{id: id, symbols: make(map[string]struct{}), notify: make(chan struct{}, 1)}
}

func (s *klineShard) setSymbol(symbol string, enabled bool) {
	s.mu.Lock()
	if enabled {
		s.symbols[symbol] = struct{}{}
	} else {
		delete(s.symbols, symbol)
	}
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *klineShard) snapshotSymbols() []string {
	s.mu.RLock()
	result := make([]string, 0, len(s.symbols))
	for symbol := range s.symbols {
		result = append(result, symbol)
	}
	s.mu.RUnlock()
	sort.Strings(result)
	return result
}

type historyJob struct {
	symbol     string
	generation uint64
}

type historyResult struct {
	symbol     string
	generation uint64
	candles    []bybit.Kline
	err        error
}

type klineManagerEvent struct {
	shardID      int
	disconnected []string
	refresh      []string
	reason       string
}

type KlineManager struct {
	client    KlineRESTClient
	url       string
	cfg       KlineConfig
	store     *CandleStore
	logger    *slog.Logger
	connector klineConnector

	mu       sync.RWMutex
	symbols  []string
	updates  chan struct{}
	eventMu  sync.RWMutex
	events   chan klineManagerEvent
}

func NewKlineManager(client KlineRESTClient, url string, cfg KlineConfig, store *CandleStore, logger *slog.Logger) (*KlineManager, error) {
	if client == nil {
		return nil, errors.New("Kline REST client не задан")
	}
	if store == nil {
		return nil, errors.New("CandleStore не задан")
	}
	if strings.TrimSpace(url) == "" {
		return nil, errors.New("публичный WebSocket URL не задан")
	}
	if cfg.HistoryLimit < 21 || cfg.HistoryLimit > 1000 {
		return nil, errors.New("Kline history limit должен быть в диапазоне 21..1000")
	}
	if cfg.HistoryWorkers < 1 || cfg.HistoryWorkers > 32 {
		return nil, errors.New("Kline history workers должен быть в диапазоне 1..32")
	}
	if cfg.MaxSymbolsPerConnection < 1 || cfg.MaxSymbolsPerConnection > 100 {
		return nil, errors.New("Kline symbols per connection должен быть в диапазоне 1..100")
	}
	if cfg.MaxArgsPerSubscribe < 1 || cfg.MaxArgsPerSubscribe > 100 {
		return nil, errors.New("Kline args per subscribe должен быть в диапазоне 1..100")
	}
	if cfg.MaxShards < 1 || cfg.MaxShards > 100 {
		return nil, errors.New("Kline max shards должен быть в диапазоне 1..100")
	}
	if cfg.MaxDataAge <= 0 || cfg.ReconnectMin <= 0 || cfg.ReconnectMax < cfg.ReconnectMin {
		return nil, errors.New("некорректные интервалы актуальности или переподключения Kline")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &KlineManager{
		client: client,
		url: strings.TrimSpace(url),
		cfg: cfg,
		store: store,
		logger: logger,
		connector: gorillaKlineConnector{},
		updates: make(chan struct{}, 1),
	}, nil
}

// SetSymbols передаёт полный список для минутного мониторинга.
// Изменения обрабатываются как diff и не переподключают работающие сегменты.
func (m *KlineManager) SetSymbols(symbols []string) {
	normalized := normalizeKlineSymbols(symbols)
	m.mu.Lock()
	if equalKlineSymbols(m.symbols, normalized) {
		m.mu.Unlock()
		return
	}
	m.symbols = normalized
	m.mu.Unlock()
	select {
	case m.updates <- struct{}{}:
	default:
	}
}

func normalizeKlineSymbols(symbols []string) []string {
	seen := make(map[string]struct{}, len(symbols))
	result := make([]string, 0, len(symbols))
	for _, raw := range symbols {
		symbol := normalizeSymbol(raw)
		if symbol == "" || !strings.HasSuffix(symbol, "USDT") {
			continue
		}
		if _, exists := seen[symbol]; exists {
			continue
		}
		seen[symbol] = struct{}{}
		result = append(result, symbol)
	}
	sort.Strings(result)
	return result
}

func equalKlineSymbols(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (m *KlineManager) requestedSymbols() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]string(nil), m.symbols...)
}

// Run держит ограниченный пул исторической загрузки и независимые WebSocket-сегменты.
func (m *KlineManager) Run(ctx context.Context) {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	queueCapacity := m.cfg.MaxShards * m.cfg.MaxSymbolsPerConnection
	if queueCapacity < 256 {
		queueCapacity = 256
	}
	historyJobs := make(chan historyJob, queueCapacity)
	historyResults := make(chan historyResult, queueCapacity)
	events := make(chan klineManagerEvent, queueCapacity)
	m.eventMu.Lock()
	m.events = events
	m.eventMu.Unlock()
	defer func() {
		m.eventMu.Lock()
		m.events = nil
		m.eventMu.Unlock()
	}()

	var historyWG sync.WaitGroup
	for worker := 0; worker < m.cfg.HistoryWorkers; worker++ {
		historyWG.Add(1)
		go m.historyWorker(workerCtx, historyJobs, historyResults, &historyWG)
	}

	shards := make(map[int]*klineShard)
	assignments := make(map[string]*klineShard)
	generations := make(map[string]uint64)
	pendingHistory := make(map[string]uint64)
	var shardWG sync.WaitGroup

	scheduleHistory := func(symbol string) {
		generation := generations[symbol]
		if current, exists := pendingHistory[symbol]; exists && current == generation {
			return
		}
		job := historyJob{symbol: symbol, generation: generation}
		select {
		case historyJobs <- job:
			pendingHistory[symbol] = generation
		case <-workerCtx.Done():
		default:
			m.logger.Warn("Очередь исторических Kline переполнена; символ останется warming_up", "symbol", symbol)
		}
	}

	createShard := func(id int) *klineShard {
		shard := newKlineShard(id)
		shards[id] = shard
		shardWG.Add(1)
		go func() {
			defer shardWG.Done()
			m.runShard(workerCtx, shard, events)
		}()
		return shard
	}

	chooseShard := func() *klineShard {
		var selected *klineShard
		for _, shard := range shards {
			count := len(shard.snapshotSymbols())
			if count >= m.cfg.MaxSymbolsPerConnection {
				continue
			}
			if selected == nil || count < len(selected.snapshotSymbols()) ||
				(count == len(selected.snapshotSymbols()) && shard.id < selected.id) {
				selected = shard
			}
		}
		if selected != nil {
			return selected
		}
		for id := 0; id < m.cfg.MaxShards; id++ {
			if _, exists := shards[id]; !exists {
				return createShard(id)
			}
		}
		return nil
	}

	reconcileSymbols := func() {
		requested := m.requestedSymbols()
		wanted := make(map[string]struct{}, len(requested))
		for _, symbol := range requested {
			wanted[symbol] = struct{}{}
		}

		removed := make([]string, 0)
		for symbol := range assignments {
			if _, keep := wanted[symbol]; !keep {
				removed = append(removed, symbol)
			}
		}
		sort.Strings(removed)
		for _, symbol := range removed {
			assignments[symbol].setSymbol(symbol, false)
			delete(assignments, symbol)
			generations[symbol]++
			delete(pendingHistory, symbol)
			m.store.MarkInactive(symbol)
		}

		for _, symbol := range requested {
			if _, exists := assignments[symbol]; exists {
				continue
			}
			generations[symbol]++
			m.store.Activate(symbol)
			shard := chooseShard()
			if shard == nil {
				m.store.SetHistoryError(symbol, fmt.Errorf("достигнут лимит Kline сегментов: %d", m.cfg.MaxShards))
				m.logger.Error("Нет свободного Kline WebSocket-сегмента", "symbol", symbol, "max_shards", m.cfg.MaxShards)
				continue
			}
			assignments[symbol] = shard
			shard.setSymbol(symbol, true)
			scheduleHistory(symbol)
		}
	}

	for {
		select {
		case <-workerCtx.Done():
			shardWG.Wait()
			close(historyJobs)
			historyWG.Wait()
			return
		case <-m.updates:
			reconcileSymbols()
		case result := <-historyResults:
			if generation, exists := pendingHistory[result.symbol]; exists && generation == result.generation {
				delete(pendingHistory, result.symbol)
			}
			if generations[result.symbol] != result.generation {
				continue
			}
			if result.err != nil {
				m.store.SetHistoryError(result.symbol, result.err)
				m.logger.Warn("Не удалось прогреть историю минутных свечей",
					"symbol", result.symbol, "status", CandleStatusWarmingUp, "error", result.err)
				continue
			}
			if err := m.store.LoadHistory(result.symbol, result.candles, time.Now().UTC()); err != nil {
				m.store.SetHistoryError(result.symbol, err)
				m.logger.Warn("Не удалось сохранить историю минутных свечей", "symbol", result.symbol, "error", err)
				continue
			}
			snapshot, ok := m.store.Snapshot(result.symbol, time.Now())
			if ok {
				m.logger.Debug("История минутных свечей загружена",
					"symbol", result.symbol, "candles", len(snapshot.History), "status", snapshot.Status)
			}
		case event := <-events:
			for _, symbol := range event.disconnected {
				if assignments[symbol] == nil || assignments[symbol].id != event.shardID {
					continue
				}
				m.store.MarkDisconnected(symbol, event.reason)
				scheduleHistory(symbol)
			}
			for _, symbol := range event.refresh {
				if assignments[symbol] != nil {
					scheduleHistory(symbol)
				}
			}
		}
	}
}

func (m *KlineManager) historyWorker(ctx context.Context, jobs <-chan historyJob, results chan<- historyResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			candles, err := m.client.FetchMinuteKlines(ctx, job.symbol, m.cfg.HistoryLimit)
			result := historyResult{symbol: job.symbol, generation: job.generation, candles: candles, err: err}
			select {
			case results <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (m *KlineManager) runShard(ctx context.Context, shard *klineShard, events chan<- klineManagerEvent) {
	backoff := m.cfg.ReconnectMin
	for {
		if ctx.Err() != nil {
			return
		}
		if len(shard.snapshotSymbols()) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-shard.notify:
				continue
			}
		}
		err := m.connectAndServe(ctx, shard, events)
		if ctx.Err() != nil {
			return
		}
		symbols := shard.snapshotSymbols()
		if len(symbols) > 0 {
			event := klineManagerEvent{shardID: shard.id, disconnected: symbols, reason: "Kline WebSocket потерял соединение; история будет загружена заново"}
			select {
			case events <- event:
			case <-ctx.Done():
				return
			}
		}
		m.logger.Warn("Kline WebSocket-сегмент отключён; выполняется независимое восстановление",
			"segment", shard.id, "symbols", len(symbols), "error", err, "retry_after", backoff)
		delay := backoff + time.Duration(shard.id)*25*time.Millisecond
		if waitErr := waitKlineContext(ctx, delay); waitErr != nil {
			return
		}
		backoff *= 2
		if backoff > m.cfg.ReconnectMax {
			backoff = m.cfg.ReconnectMax
		}
	}
}

func (m *KlineManager) connectAndServe(ctx context.Context, shard *klineShard, events chan<- klineManagerEvent) error {
	conn, err := m.connector.Dial(ctx, m.url)
	if err != nil {
		return fmt.Errorf("подключиться к Kline WebSocket: %w", err)
	}
	defer conn.Close()

	writer := &klineSerializedWriter{conn: conn}
	conn.SetReadLimit(klineMaxMessageSize)
	if err := conn.SetReadDeadline(time.Now().Add(klineReadTimeout)); err != nil {
		return err
	}
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(klineReadTimeout))
	})

	active := make(map[string]struct{})
	if err := m.syncSubscriptions(ctx, writer, shard.snapshotSymbols(), active); err != nil {
		return fmt.Errorf("создать начальные Kline-подписки сегмента %d: %w", shard.id, err)
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- m.writeLoop(sessionCtx, cancel, writer, shard, active)
	}()

	closeWatcher := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-sessionCtx.Done():
			_ = conn.Close()
		case <-closeWatcher:
		}
	}()

	readErr := m.readLoop(sessionCtx, conn, shard, events)
	cancel()
	close(closeWatcher)
	_ = conn.Close()
	<-watcherDone
	writerErr := <-writerDone
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if writerErr != nil {
		return writerErr
	}
	if readErr == nil {
		return errors.New("Kline WebSocket завершил чтение без причины")
	}
	return readErr
}

func (m *KlineManager) writeLoop(ctx context.Context, cancel context.CancelFunc, writer *klineSerializedWriter, shard *klineShard, active map[string]struct{}) error {
	ticker := time.NewTicker(klineHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-shard.notify:
			if err := m.syncSubscriptions(ctx, writer, shard.snapshotSymbols(), active); err != nil {
				cancel()
				return fmt.Errorf("обновить Kline-подписки сегмента %d: %w", shard.id, err)
			}
		case <-ticker.C:
			if err := writer.writeJSON(map[string]string{"op": "ping"}); err != nil {
				cancel()
				return fmt.Errorf("heartbeat Kline сегмента %d: %w", shard.id, err)
			}
		}
	}
}

func (m *KlineManager) syncSubscriptions(ctx context.Context, writer *klineSerializedWriter, symbols []string, active map[string]struct{}) error {
	desired := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		desired["kline.1."+symbol] = struct{}{}
	}
	added := make([]string, 0)
	removed := make([]string, 0)
	for topic := range desired {
		if _, exists := active[topic]; !exists {
			added = append(added, topic)
		}
	}
	for topic := range active {
		if _, exists := desired[topic]; !exists {
			removed = append(removed, topic)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)

	for _, batches := range []struct {
		operation string
		topics    []string
	}{
		{operation: "unsubscribe", topics: removed},
		{operation: "subscribe", topics: added},
	} {
		commands := chunkKlineTopics(batches.operation, batches.topics, m.cfg.MaxArgsPerSubscribe)
		for index, batch := range commands {
			if err := writer.writeJSON(batch); err != nil {
				return err
			}
			if index < len(commands)-1 {
				if err := waitKlineContext(ctx, klineSubscriptionBatchPause); err != nil {
					return err
				}
			}
		}
	}
	for topic := range active {
		if _, exists := desired[topic]; !exists {
			delete(active, topic)
		}
	}
	for topic := range desired {
		active[topic] = struct{}{}
	}
	return nil
}

func chunkKlineTopics(operation string, topics []string, maxArgs int) []klineSubscriptionBatch {
	if len(topics) == 0 {
		return nil
	}
	result := make([]klineSubscriptionBatch, 0, (len(topics)+maxArgs-1)/maxArgs)
	for start := 0; start < len(topics); {
		end := start
		characters := 0
		for end < len(topics) && end-start < maxArgs {
			next := len(topics[end])
			if end > start {
				next++
			}
			if end > start && characters+next > klineMaxArgsCharacters {
				break
			}
			characters += next
			end++
		}
		if end == start {
			end++
		}
		result = append(result, klineSubscriptionBatch{
			Operation: operation,
			Arguments: append([]string(nil), topics[start:end]...),
		})
		start = end
	}
	return result
}

type klineEnvelope struct {
	Topic     string       `json:"topic"`
	Type      string       `json:"type"`
	Operation string       `json:"op"`
	Success   *bool        `json:"success"`
	Message   string       `json:"ret_msg"`
	Data      []rawKlineWS `json:"data"`
}

type rawKlineWS struct {
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
	Interval  string `json:"interval"`
	Open      string `json:"open"`
	Close     string `json:"close"`
	High      string `json:"high"`
	Low       string `json:"low"`
	Volume    string `json:"volume"`
	Turnover  string `json:"turnover"`
	Confirm   bool   `json:"confirm"`
	Timestamp int64  `json:"timestamp"`
}

func (m *KlineManager) readLoop(ctx context.Context, conn klineWireConn, shard *klineShard, events chan<- klineManagerEvent) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("прочитать Kline WebSocket: %w", err)
		}
		if messageType != websocket.TextMessage {
			return fmt.Errorf("неожиданный тип Kline WebSocket-сообщения: %d", messageType)
		}
		if err := conn.SetReadDeadline(time.Now().Add(klineReadTimeout)); err != nil {
			return err
		}
		if err := m.handleMessage(payload, time.Now().UTC()); err != nil {
			var subscriptionErr *klineSubscriptionError
			if errors.As(err, &subscriptionErr) {
				return subscriptionErr
			}
			m.logger.Warn("Некорректное обновление минутной свечи", "segment", shard.id, "error", err)
		}
	}
}

func (m *KlineManager) handleMessage(payload []byte, receivedAt time.Time) error {
	var envelope klineEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("разобрать Kline WebSocket JSON: %w", err)
	}
	if envelope.Operation == "pong" || envelope.Operation == "ping" {
		return nil
	}
	if envelope.Operation == "subscribe" || envelope.Operation == "unsubscribe" {
		if envelope.Success != nil && !*envelope.Success {
			return &klineSubscriptionError{message: fmt.Sprintf("Bybit отклонила %s Kline-подписку: %s", envelope.Operation, envelope.Message)}
		}
		return nil
	}
	symbol, ok := klineSymbolFromTopic(envelope.Topic)
	if !ok {
		return nil
	}
	if envelope.Type != "snapshot" {
		return fmt.Errorf("Kline %s: неизвестный тип сообщения %q", symbol, envelope.Type)
	}
	for _, raw := range envelope.Data {
		candle, err := parseKlineWS(raw)
		if err != nil {
			m.requestHistoryRefresh(symbol)
			return fmt.Errorf("Kline %s: %w", symbol, err)
		}
		if err := m.store.ApplyUpdate(symbol, candle, receivedAt); err != nil {
			m.requestHistoryRefresh(symbol)
			return fmt.Errorf("Kline %s: %w", symbol, err)
		}
	}
	return nil
}

func (m *KlineManager) requestHistoryRefresh(symbol string) {
	select {
	case m.eventChannel() <- klineManagerEvent{refresh: []string{symbol}}:
	default:
		m.logger.Warn("Очередь восстановления истории Kline переполнена", "symbol", symbol)
	}
}

func (m *KlineManager) eventChannel() chan<- klineManagerEvent {
	m.eventMu.RLock()
	defer m.eventMu.RUnlock()
	return m.events
}

func klineSymbolFromTopic(topic string) (string, bool) {
	parts := strings.Split(topic, ".")
	if len(parts) != 3 || parts[0] != "kline" || parts[1] != "1" || parts[2] == "" {
		return "", false
	}
	return normalizeSymbol(parts[2]), true
}

func parseKlineWS(raw rawKlineWS) (Candle, error) {
	if raw.Interval != "1" || raw.Start <= 0 || raw.End <= 0 {
		return Candle{}, errors.New("отсутствует interval/start/end либо интервал не равен 1")
	}
	parse := func(name, value string) (float64, error) {
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, fmt.Errorf("%s=%q: %w", name, value, err)
		}
		return parsed, nil
	}
	open, err := parse("open", raw.Open)
	if err != nil { return Candle{}, err }
	high, err := parse("high", raw.High)
	if err != nil { return Candle{}, err }
	low, err := parse("low", raw.Low)
	if err != nil { return Candle{}, err }
	closePrice, err := parse("close", raw.Close)
	if err != nil { return Candle{}, err }
	volume, err := parse("volume", raw.Volume)
	if err != nil { return Candle{}, err }
	turnover, err := parse("turnover", raw.Turnover)
	if err != nil { return Candle{}, err }
	return Candle{
		StartTime: time.UnixMilli(raw.Start).UTC(),
		EndTime: time.UnixMilli(raw.End).UTC(),
		Open: open, High: high, Low: low, Close: closePrice,
		Volume: volume, Turnover: turnover, Confirmed: raw.Confirm,
	}, nil
}

func waitKlineContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
