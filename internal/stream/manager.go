package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	marketpaper "github.com/Quantum103/Scrinner_PAMPA/internal/paper"
	marketanalysis "github.com/Quantum103/Scrinner_PAMPA/internal/analysis"
	marketstrategy "github.com/Quantum103/Scrinner_PAMPA/internal/strategy"
)

const (
	defaultDepth                      = 50
	defaultSymbolsPerConnection       = 25
	defaultArgsPerSubscribe           = 10
	defaultHeartbeatInterval          = 20 * time.Second
	defaultReadTimeout                = 60 * time.Second
	defaultReconnectMin               = time.Second
	defaultReconnectMax               = 30 * time.Second
	subscriptionBatchPause            = 100 * time.Millisecond
	maxWebSocketMessageSize     int64 = 1 << 20
	webSocketCloseMessage             = 8
)

type Config struct {
	URL                     string
	Depth                   int
	MaxSymbolsPerConnection int
	MaxArgsPerSubscribe     int
	HeartbeatInterval       time.Duration
	ReadTimeout             time.Duration
	MaxDataAge              time.Duration
	ReconnectMin            time.Duration
	ReconnectMax            time.Duration
	TradeBufferCapacity     int
	SeenTradeIDCapacity     int
	Analysis                marketanalysis.Config
	Strategy                marketstrategy.Config
	Paper                   marketpaper.Config
	PaperRepository         marketpaper.Repository
}

type wireConn interface {
	ReadMessage() (messageType int, data []byte, err error)
	WriteJSON(value any) error
	SetReadDeadline(deadline time.Time) error
	SetWriteDeadline(deadline time.Time) error
	SetPongHandler(handler func(appData string) error)
	SetReadLimit(limit int64)
	WriteControl(messageType int, data []byte, deadline time.Time) error
	Close() error
}

type connector interface {
	Dial(ctx context.Context, url string) (wireConn, error)
}

type Observer interface {
	OnSignal(marketstrategy.Signal)
	OnPaperEvent(marketpaper.Event, marketpaper.AccountSnapshot)
	OnConnectionStatus(ConnectionStatus)
	OnMarketDataIssue(string)
}

type ConnectionStatus struct {
	ConnectedShards int
	TotalShards     int
	LastError       string
	At              time.Time
}

type ManagerStatus struct {
	Running         bool
	ConnectedShards int
	TotalShards     int
	LastError       string
	EntryPaused     bool
	DailyLossReached bool
	TrackedSymbols  int
}

type Manager struct {
	cfg       Config
	store     *Store
	logger   *slog.Logger
	connector connector

	mu                sync.Mutex
	running           bool
	symbols           []string
	updates           chan struct{}
	observer          Observer
	shardConnections  map[string]bool
	totalShards       int
	lastConnectionErr string
}

type wsEnvelope struct {
	Operation string `json:"op"`
	Success   *bool  `json:"success"`
	Message   string `json:"ret_msg"`
	Topic     string `json:"topic"`
}

type subscriptionBatch struct {
	Operation string   `json:"op"`
	Arguments []string `json:"args"`
}

const maxOrderBookResyncAttempts = 2

type orderBookResyncError struct {
	topic  string
	symbol string
	cause  error
}

func (e *orderBookResyncError) Error() string { return e.cause.Error() }
func (e *orderBookResyncError) Unwrap() error { return e.cause }

// serializedWriter не допускает одновременную запись heartbeat и команд подписки.
// Gorilla WebSocket разрешает только одного конкурентного writer.
type serializedWriter struct {
	conn wireConn
	mu   sync.Mutex
}

func (w *serializedWriter) writeJSON(value any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	return w.conn.WriteJSON(value)
}

func DefaultConfig(url string) Config {
	return Config{
		URL:                     url,
		Depth:                   defaultDepth,
		MaxSymbolsPerConnection: defaultSymbolsPerConnection,
		MaxArgsPerSubscribe:     defaultArgsPerSubscribe,
		HeartbeatInterval:       defaultHeartbeatInterval,
		ReadTimeout:             defaultReadTimeout,
		MaxDataAge:              5 * time.Second,
		ReconnectMin:            defaultReconnectMin,
		ReconnectMax:            defaultReconnectMax,
		TradeBufferCapacity:     4096,
		SeenTradeIDCapacity:     4096,
		Analysis:                marketanalysis.DefaultConfig(),
		Strategy:                marketstrategy.DefaultConfig(),
		Paper:                   marketpaper.DefaultConfig(),
	}
}

func NewManager(cfg Config, logger *slog.Logger) (*Manager, error) {
	if cfg.URL == "" || cfg.Depth != 50 || cfg.MaxSymbolsPerConnection <= 0 || cfg.MaxSymbolsPerConnection > 25 ||
		cfg.MaxArgsPerSubscribe <= 0 || cfg.MaxArgsPerSubscribe > 10 || cfg.HeartbeatInterval <= 0 ||
		cfg.ReadTimeout <= cfg.HeartbeatInterval || cfg.MaxDataAge <= 0 || cfg.ReconnectMin <= 0 ||
		cfg.ReconnectMax < cfg.ReconnectMin || cfg.TradeBufferCapacity <= 0 || cfg.SeenTradeIDCapacity <= 0 {
		return nil, errors.New("некорректные параметры публичного WebSocket Bybit")
	}
	store, err := NewStoreWithAnalysisStrategyAndPaperRepository(cfg.Depth, cfg.MaxDataAge, cfg.TradeBufferCapacity, cfg.SeenTradeIDCapacity, cfg.Analysis, cfg.Strategy, cfg.Paper, cfg.PaperRepository)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		cfg:              cfg,
		store:            store,
		logger:           logger,
		connector:        newGorillaConnector(),
		updates:          make(chan struct{}, 1),
		shardConnections: make(map[string]bool),
	}, nil
}

func (m *Manager) Store() *Store {
	return m.store
}

func (m *Manager) SetObserver(observer Observer) {
	m.mu.Lock()
	m.observer = observer
	m.mu.Unlock()
}

func (m *Manager) SetEntryPaused(paused bool) {
	m.store.SetEntryPaused(paused)
}

func (m *Manager) SetDailyLossReached(reached bool) {
	m.store.SetDailyLossReached(reached)
}

func (m *Manager) EntryPaused() bool {
	return m.store.EntryPaused()
}

func (m *Manager) DailyLossReached() bool {
	return m.store.DailyLossReached()
}

func (m *Manager) Status() ManagerStatus {
	m.mu.Lock()
	connected := 0
	for _, active := range m.shardConnections {
		if active {
			connected++
		}
	}
	status := ManagerStatus{
		Running: m.running, ConnectedShards: connected, TotalShards: m.totalShards,
		LastError: m.lastConnectionErr, EntryPaused: m.store.EntryPaused(),
		DailyLossReached: m.store.DailyLossReached(), TrackedSymbols: len(m.symbols),
	}
	m.mu.Unlock()
	return status
}

func (m *Manager) ReportMarketDataIssue(message string) {
	message = strings.TrimSpace(message)
	if message == "" {
		return
	}
	m.mu.Lock()
	observer := m.observer
	m.mu.Unlock()
	if observer != nil {
		observer.OnMarketDataIssue(message)
	}
}

func shardKey(symbols []string) string {
	return strings.Join(symbols, ",")
}

func (m *Manager) setShardConnection(symbols []string, connected bool, err error) {
	key := shardKey(symbols)
	m.mu.Lock()
	previous := m.shardConnections[key]
	m.shardConnections[key] = connected
	if connected {
		m.lastConnectionErr = ""
	}
	if err != nil {
		m.lastConnectionErr = err.Error()
	}
	count := 0
	for _, active := range m.shardConnections {
		if active {
			count++
		}
	}
	total := m.totalShards
	observer := m.observer
	changed := previous != connected
	status := ConnectionStatus{
		ConnectedShards: count, TotalShards: total,
		LastError: m.lastConnectionErr, At: time.Now(),
	}
	m.mu.Unlock()
	if observer != nil && changed {
		observer.OnConnectionStatus(status)
	} else if observer != nil && err != nil {
		// Первое подключение тоже может завершиться ошибкой: его потеря
		// не является переходом true -> false, но об этом нужно уведомить.
		observer.OnMarketDataIssue("Не удалось подключиться к публичному WebSocket Bybit: " + err.Error())
	}
}

// UpdateMarketData передаёт симулятору спецификации контракта и публичные funding-данные Bybit.
func (m *Manager) UpdateMarketData(items []bybit.MarketData) {
	m.store.UpdateMarketData(items, time.Now())
	m.logPaperEvents()
}

func (m *Manager) PaperSnapshot() marketpaper.AccountSnapshot {
	return m.store.PaperSnapshot(time.Now())
}

func (m *Manager) PaperStatistics() marketpaper.PaperStatistics {
	return m.store.PaperStatistics(time.Now())
}

// SetSymbols объединяет частые обновления: важен только последний полный список инструментов.
func (m *Manager) SetSymbols(symbols []string) {
	normalized := normalizeSymbols(symbols)
	m.mu.Lock()
	if equalStrings(m.symbols, normalized) {
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

func (m *Manager) Run(ctx context.Context) {
	m.mu.Lock()
	m.running = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.running = false
		m.mu.Unlock()
	}()

	var shardCancel context.CancelFunc
	var shardWG sync.WaitGroup

	restart := func(symbols []string) {
		if shardCancel != nil {
			shardCancel()
			shardWG.Wait()
		}
		m.store.MarkRemoved(symbols)
		if len(symbols) == 0 {
			m.logger.Info("Нет инструментов для WebSocket-подписок")
			shardCancel = nil
			return
		}

		shardCtx, cancel := context.WithCancel(ctx)
		shardCancel = cancel
		shards := splitSymbols(symbols, m.cfg.MaxSymbolsPerConnection)
		m.mu.Lock()
		m.totalShards = len(shards)
		m.shardConnections = make(map[string]bool, len(shards))
		m.mu.Unlock()
		m.logger.Info("Запуск публичных WebSocket-соединений Bybit",
			"инструментов", len(symbols),
			"соединений", len(shards),
			"глубина_стакана", m.cfg.Depth,
		)
		for _, shard := range shards {
			shard := append([]string(nil), shard...)
			shardWG.Add(1)
			go func() {
				defer shardWG.Done()
				m.runShard(shardCtx, shard)
			}()
		}
	}

	for {
		select {
		case <-ctx.Done():
			if shardCancel != nil {
				shardCancel()
				shardWG.Wait()
			}
			return
		case <-m.updates:
			m.mu.Lock()
			symbols := append([]string(nil), m.symbols...)
			m.mu.Unlock()
			restart(symbols)
		}
	}
}

func (m *Manager) runShard(ctx context.Context, symbols []string) {
	backoff := m.cfg.ReconnectMin
	for {
		if ctx.Err() != nil {
			return
		}
		err := m.connectAndServe(ctx, symbols)
		m.store.MarkDisconnected(symbols)
		if ctx.Err() != nil {
			return
		}
		m.setShardConnection(symbols, false, err)
		m.logger.Warn("WebSocket Bybit отключён; выполняется переподключение",
			"инструментов", len(symbols),
			"ошибка", err,
			"повтор_через", backoff,
		)
		if err := waitContext(ctx, backoff); err != nil {
			return
		}
		backoff *= 2
		if backoff > m.cfg.ReconnectMax {
			backoff = m.cfg.ReconnectMax
		}
	}
}

func (m *Manager) connectAndServe(ctx context.Context, symbols []string) error {
	conn, err := m.connector.Dial(ctx, m.cfg.URL)
	if err != nil {
		return fmt.Errorf("подключиться к Bybit: %w", err)
	}
	defer conn.Close()
	writer := &serializedWriter{conn: conn}
	conn.SetReadLimit(maxWebSocketMessageSize)
	_ = conn.SetReadDeadline(time.Now().Add(m.cfg.ReadTimeout))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(m.cfg.ReadTimeout))
	})

	for _, batch := range buildSubscriptionBatches(symbols, m.cfg.Depth, m.cfg.MaxArgsPerSubscribe) {
		if err := writer.writeJSON(batch); err != nil {
			return fmt.Errorf("отправить подписку Bybit: %w", err)
		}
		if err := waitContext(ctx, subscriptionBatchPause); err != nil {
			return err
		}
	}
	m.store.StartSession(symbols)
	m.setShardConnection(symbols, true, nil)
	m.logger.Info("Публичный WebSocket Bybit подключён; подписки отправлены",
		"инструментов", len(symbols),
		"тем_подписки", len(symbols)*2,
	)

	sessionCtx, cancel := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	heartbeatErr := make(chan error, 1)
	go m.heartbeat(sessionCtx, cancel, writer, heartbeatDone, heartbeatErr)
	closeOnCancel := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-sessionCtx.Done():
			_ = conn.WriteControl(webSocketCloseMessage, []byte{0x03, 0xE8}, time.Now().Add(time.Second))
			_ = conn.Close()
		case <-closeOnCancel:
		}
	}()

	readErr := m.readLoop(sessionCtx, conn, writer)
	cancel()
	close(closeOnCancel)
	<-watcherDone
	<-heartbeatDone
	if ctx.Err() != nil {
		return ctx.Err()
	}
	select {
	case err := <-heartbeatErr:
		return err
	default:
	}
	if readErr == nil {
		return errors.New("соединение WebSocket завершилось без причины")
	}
	return readErr
}

func (m *Manager) heartbeat(ctx context.Context, cancel context.CancelFunc, writer *serializedWriter, done chan<- struct{}, errors chan<- error) {
	defer close(done)
	ticker := time.NewTicker(m.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := writer.writeJSON(map[string]string{"op": "ping"}); err != nil {
				m.logger.Warn("Не удалось отправить heartbeat Bybit", "ошибка", err)
				select {
				case errors <- err:
				default:
				}
				cancel()
				return
			}
		}
	}
}

func (m *Manager) readLoop(ctx context.Context, conn wireConn, writer *serializedWriter) error {
	pendingSnapshots := make(map[string]int)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("прочитать WebSocket-сообщение: %w", err)
		}
		if messageType != 1 {
			return fmt.Errorf("получен неожиданный тип WebSocket-сообщения: %d", messageType)
		}
		if err := conn.SetReadDeadline(time.Now().Add(m.cfg.ReadTimeout)); err != nil {
			return fmt.Errorf("обновить таймаут чтения WebSocket: %w", err)
		}

		var header struct {
			Topic string `json:"topic"`
			Type  string `json:"type"`
		}
		if err := json.Unmarshal(payload, &header); err != nil {
			return fmt.Errorf("не удалось разобрать заголовок сообщения Bybit: %w", err)
		}

		// После запроса повторной подписки локальная книга остаётся невалидной.
		// Не применяем пришедшие до snapshot delta и не запускаем повторную
		// синхронизацию на каждое такое сообщение.
		if pendingSnapshots[header.Topic] > 0 && header.Type == "delta" {
			continue
		}

		if err := m.handleMessage(payload, time.Now()); err != nil {
			var resyncErr *orderBookResyncError
			if !errors.As(err, &resyncErr) {
				return err
			}
			attempts := pendingSnapshots[resyncErr.topic]
			if attempts >= maxOrderBookResyncAttempts {
				m.logger.Error("Не удалось восстановить стакан после повторных подписок; символ исключён из торговых сигналов",
					"symbol", resyncErr.symbol, "topic", resyncErr.topic, "error", err)
				continue
			}
			attempts++
			pendingSnapshots[resyncErr.topic] = attempts
			if err := m.resubscribeOrderBook(ctx, writer, resyncErr.topic); err != nil {
				return fmt.Errorf("повторно подписаться на стакан %s: %w", resyncErr.symbol, err)
			}
			m.logger.Warn("Запрошен новый snapshot стакана без отключения WebSocket-соединения",
				"symbol", resyncErr.symbol, "topic", resyncErr.topic,
				"attempt", attempts, "error", err)
			continue
		}

		if strings.HasPrefix(header.Topic, "orderbook.") && header.Type == "snapshot" {
			delete(pendingSnapshots, header.Topic)
		}
	}
}

func (m *Manager) resubscribeOrderBook(ctx context.Context, writer *serializedWriter, topic string) error {
	if err := writer.writeJSON(subscriptionBatch{Operation: "unsubscribe", Arguments: []string{topic}}); err != nil {
		return fmt.Errorf("отменить старую подписку: %w", err)
	}
	if err := waitContext(ctx, subscriptionBatchPause); err != nil {
		return err
	}
	if err := writer.writeJSON(subscriptionBatch{Operation: "subscribe", Arguments: []string{topic}}); err != nil {
		return fmt.Errorf("создать новую подписку: %w", err)
	}
	return nil
}

func (m *Manager) handleMessage(payload []byte, receivedAt time.Time) error {
	var envelope wsEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("не удалось разобрать сообщение Bybit: %w", err)
	}
	if envelope.Operation == "pong" {
		return nil
	}
	if envelope.Operation == "ping" {
		return nil
	}
	if envelope.Operation == "subscribe" || envelope.Operation == "unsubscribe" {
		if envelope.Success != nil && !*envelope.Success {
			return fmt.Errorf("Bybit отклонила команду %s: %s", envelope.Operation, envelope.Message)
		}
		return nil
	}
	if envelope.Topic == "" {
		return nil
	}

	switch {
	case strings.HasPrefix(envelope.Topic, "orderbook."):
		var message orderBookMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			return fmt.Errorf("разобрать стакан Bybit: %w", err)
		}
		if symbolFromTopic(envelope.Topic) != message.Data.Symbol {
			return fmt.Errorf("символ в topic %q не совпадает с данными %q", envelope.Topic, message.Data.Symbol)
		}
		if err := m.store.ApplyOrderBook(payload, receivedAt); err != nil {
			wrapped := fmt.Errorf("обновить стакан %s: %w", message.Data.Symbol, err)
			m.ReportMarketDataIssue(wrapped.Error())
			if errors.Is(err, ErrSnapshotRequired) || errors.Is(err, ErrInvalidOrderbook) {
				return &orderBookResyncError{
					topic: envelope.Topic, symbol: message.Data.Symbol, cause: wrapped,
				}
			}
			return wrapped
		}
		m.logSignals()
	case strings.HasPrefix(envelope.Topic, "publicTrade."):
		var message tradeMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			return fmt.Errorf("разобрать сделки Bybit: %w", err)
		}
		for _, trade := range message.Data {
			if symbolFromTopic(envelope.Topic) != trade.Symbol {
				return fmt.Errorf("символ в topic %q не совпадает с данными %q", envelope.Topic, trade.Symbol)
			}
		}
		if err := m.store.ApplyTrades(payload, receivedAt); err != nil {
			wrapped := fmt.Errorf("сохранить сделки: %w", err)
			m.ReportMarketDataIssue(wrapped.Error())
			return wrapped
		}
		m.logSignals()
	}
	return nil
}

func buildSubscriptionBatches(symbols []string, depth, batchSize int) []subscriptionBatch {
	if depth <= 0 || batchSize <= 0 {
		return nil
	}
	arguments := make([]string, 0, len(symbols)*2)
	for _, symbol := range symbols {
		arguments = append(arguments,
			fmt.Sprintf("publicTrade.%s", symbol),
			fmt.Sprintf("orderbook.%d.%s", depth, symbol),
		)
	}
	batches := make([]subscriptionBatch, 0, (len(arguments)+batchSize-1)/batchSize)
	for start := 0; start < len(arguments); start += batchSize {
		end := start + batchSize
		if end > len(arguments) {
			end = len(arguments)
		}
		batches = append(batches, subscriptionBatch{
			Operation: "subscribe",
			Arguments: append([]string(nil), arguments[start:end]...),
		})
	}
	return batches
}

func splitSymbols(symbols []string, maxPerConnection int) [][]string {
	if maxPerConnection <= 0 {
		return nil
	}
	var result [][]string
	for start := 0; start < len(symbols); start += maxPerConnection {
		end := start + maxPerConnection
		if end > len(symbols) {
			end = len(symbols)
		}
		result = append(result, append([]string(nil), symbols[start:end]...))
	}
	return result
}

func normalizeSymbols(symbols []string) []string {
	seen := make(map[string]struct{}, len(symbols))
	result := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		symbol = strings.ToUpper(strings.TrimSpace(symbol))
		if !strings.HasSuffix(symbol, "USDT") || strings.ContainsAny(symbol, ". /\\\t\r\n") {
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

func equalStrings(left, right []string) bool {
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

func symbolFromTopic(topic string) string {
	parts := strings.Split(topic, ".")
	if len(parts) < 2 {
		return ""
	}
	return parts[len(parts)-1]
}

func waitContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (m *Manager) logSignals() {
	for _, signal := range m.store.DrainSignals() {
		m.mu.Lock()
		observer := m.observer
		m.mu.Unlock()
		if observer != nil {
			observer.OnSignal(signal)
		}
		m.logger.Info("Сформирован сигнал LONG",
			"symbol", signal.Symbol,
			"price", signal.Price,
			"rvol", signal.RVOL,
			"taker_buy_percent", signal.TakerBuySharePercent,
			"delta", signal.Delta,
			"cvd", signal.CVD,
			"price_change_percent", signal.PriceChangePercent,
			"spread_bps", signal.SpreadBPS,
			"reason", signal.Reason,
		)
	}
	m.logPaperEvents()
}

func (m *Manager) logPaperEvents() {
	for _, event := range m.store.DrainPaperEvents() {
		attributes := []any{
			"symbol", event.Symbol, "type", event.Type, "reason", event.Reason,
			"price", event.Price, "quantity", event.Quantity,
			"notional_usdt", event.Notional, "margin_usdt", event.Margin,
			"leverage", event.Leverage, "fee_usdt", event.Fee,
			"risk_usdt", event.RiskUSDT, "risk_percent", event.RiskPercent,
			"gross_pnl_usdt", event.GrossPnL, "net_pnl_usdt", event.NetPnL,
			"funding_cash_flow_usdt", event.Funding,
			"remaining_quantity", event.RemainingQuantity,
		}
		switch event.Type {
		case "entry_rejected", "close_rejected", "funding_data_unavailable", "funding_skipped", "funding_backlog", "persistence_error":
			m.logger.Warn("Событие Paper Trading", attributes...)
		default:
			m.logger.Info("Событие Paper Trading", attributes...)
		}
		m.mu.Lock()
		observer := m.observer
		m.mu.Unlock()
		if observer != nil {
			observer.OnPaperEvent(event, m.PaperSnapshot())
		}
		if event.Type == "funding_data_unavailable" || event.Type == "funding_skipped" || event.Type == "persistence_error" {
			m.ReportMarketDataIssue(event.Reason)
		}
	}
}
