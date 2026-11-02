package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type fakeRead struct {
	messageType int
	payload     []byte
	err         error
}

type fakeWireConn struct {
	reads         chan fakeRead
	closeOnce     sync.Once
	mu            sync.Mutex
	writes        [][]byte
	readLimit     int64
	readDeadline  time.Time
	writeDeadline time.Time
	pongHandler   func(string) error
}

func newFakeWireConn() *fakeWireConn {
	return &fakeWireConn{reads: make(chan fakeRead, 4)}
}

func (c *fakeWireConn) ReadMessage() (int, []byte, error) {
	item, ok := <-c.reads
	if !ok {
		return 0, nil, io.EOF
	}
	return item.messageType, item.payload, item.err
}

func (c *fakeWireConn) WriteJSON(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.writes = append(c.writes, payload)
	c.mu.Unlock()
	return nil
}

func (c *fakeWireConn) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.readDeadline = deadline
	c.mu.Unlock()
	return nil
}

func (c *fakeWireConn) SetWriteDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.writeDeadline = deadline
	c.mu.Unlock()
	return nil
}

func (c *fakeWireConn) SetPongHandler(handler func(string) error) {
	c.mu.Lock()
	c.pongHandler = handler
	c.mu.Unlock()
}

func (c *fakeWireConn) SetReadLimit(limit int64) {
	c.mu.Lock()
	c.readLimit = limit
	c.mu.Unlock()
}

func (c *fakeWireConn) WriteControl(_ int, _ []byte, _ time.Time) error { return nil }

func (c *fakeWireConn) Close() error {
	c.closeOnce.Do(func() { close(c.reads) })
	return nil
}

func (c *fakeWireConn) writeSnapshot() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([][]byte, len(c.writes))
	for i := range c.writes {
		result[i] = append([]byte(nil), c.writes[i]...)
	}
	return result
}

type fakeConnector struct {
	mu     sync.Mutex
	conns  []wireConn
	dials  int
	dialed chan int
}

func (d *fakeConnector) Dial(_ context.Context, _ string) (wireConn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	index := d.dials
	d.dials++
	select {
	case d.dialed <- d.dials:
	default:
	}
	if index >= len(d.conns) {
		return nil, errors.New("нет тестового соединения")
	}
	return d.conns[index], nil
}

func testManager(t *testing.T) *Manager {
	t.Helper()
	cfg := DefaultConfig("wss://example.test/v5/public/linear")
	cfg.HeartbeatInterval = 10 * time.Millisecond
	cfg.ReadTimeout = 100 * time.Millisecond
	cfg.ReconnectMin = 5 * time.Millisecond
	cfg.ReconnectMax = 20 * time.Millisecond
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager, err := NewManager(cfg, logger)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	return manager
}

func TestRunShardReconnectsAndResubscribes(t *testing.T) {
	manager := testManager(t)
	first := newFakeWireConn()
	first.reads <- fakeRead{err: errors.New("искусственный разрыв соединения")}
	second := newFakeWireConn()
	dialer := &fakeConnector{conns: []wireConn{first, second}, dialed: make(chan int, 4)}
	manager.connector = dialer

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		manager.runShard(ctx, []string{"BTCUSDT"})
	}()

	select {
	case count := <-dialer.dialed:
		if count != 1 {
			t.Fatalf("первое подключение имеет номер %d", count)
		}
	case <-time.After(time.Second):
		t.Fatal("первое подключение не выполнено")
	}
	select {
	case count := <-dialer.dialed:
		if count != 2 {
			t.Fatalf("второе подключение имеет номер %d", count)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("после разрыва не произошло переподключение")
	}

	waitFor(t, time.Second, func() bool { return len(second.writeSnapshot()) > 0 })
	if !hasSubscribe(second.writeSnapshot()) {
		t.Fatal("после переподключения не отправлена повторная подписка")
	}
	if !hasPing(second.writeSnapshot()) {
		waitFor(t, time.Second, func() bool { return hasPing(second.writeSnapshot()) })
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runShard не остановился после отмены контекста")
	}
}

func TestReadLoopProcessesArtificialBybitMessages(t *testing.T) {
	manager := testManager(t)
	manager.store.StartSession([]string{"BTCUSDT"})
	now := time.Now()
	book := withFreshTimestamp(`{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","1"]],"u":10,"seq":10}}`, now)
	trade := withFreshTimestamp(`{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"BTCUSDT","S":"Buy","v":"0.1","p":"100.5","i":"trade-1"}]}`, now)
	if err := manager.handleMessage(book, now); err != nil {
		t.Fatalf("snapshot message error = %v", err)
	}
	if err := manager.handleMessage(trade, now); err != nil {
		t.Fatalf("trade message error = %v", err)
	}
	state, _ := manager.store.Snapshot("BTCUSDT", now)
	if !state.OrderBook.Valid || state.LastTrade == nil || state.LastTrade.TradeID != "trade-1" {
		t.Fatalf("artificial Bybit messages were not processed: %+v", state)
	}
}

func TestReadLoopContinuesAfterStaleTrade(t *testing.T) {
	manager := testManager(t)
	manager.store.StartSession([]string{"BTCUSDT", "ETHUSDT"})
	conn := newFakeWireConn()
	now := time.Now()

	staleTrade := fmt.Sprintf(`{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":%d,"data":[{"T":%d,"s":"BTCUSDT","S":"Buy","v":"0.1","p":"100","i":"stale-btc-trade"}]}`,
		now.UnixMilli(), now.Add(-6*time.Second).UnixMilli())
	freshTrade := fmt.Sprintf(`{"topic":"publicTrade.ETHUSDT","type":"snapshot","ts":%d,"data":[{"T":%d,"s":"ETHUSDT","S":"Buy","v":"0.1","p":"200","i":"fresh-eth-trade"}]}`,
		now.UnixMilli(), now.Add(-100*time.Millisecond).UnixMilli())

	conn.reads <- fakeRead{messageType: 1, payload: []byte(staleTrade)}
	conn.reads <- fakeRead{messageType: 1, payload: []byte(freshTrade)}
	_ = conn.Close()

	err := manager.readLoop(context.Background(), conn, &serializedWriter{conn: conn})
	if !errors.Is(err, io.EOF) {
		t.Fatalf("readLoop error = %v, want EOF after both messages", err)
	}

	btc, ok := manager.store.Snapshot("BTCUSDT", time.Now())
	if !ok || !btc.TradeGap || btc.LastTrade != nil {
		t.Fatalf("stale BTC trade was not discarded safely: %+v", btc)
	}
	eth, ok := manager.store.Snapshot("ETHUSDT", time.Now())
	if !ok || eth.LastTrade == nil || eth.LastTrade.TradeID != "fresh-eth-trade" {
		t.Fatalf("stale BTC trade interrupted ETH stream: %+v", eth)
	}
}


func TestStableShardAssignmentsPreservesExistingMembership(t *testing.T) {
	runtime0 := newManagedShard([]string{"AAAUSDT", "BBBUSDT"}, func() {})
	runtime1 := newManagedShard([]string{"CCCUSDT"}, func() {})
	assignments, nextID := stableShardAssignments(
		map[int]*managedShard{0: runtime0, 1: runtime1},
		[]string{"AAAUSDT", "BBBUSDT", "DDDUSDT"},
		2,
		2,
	)
	if nextID != 2 {
		t.Fatalf("next shard ID = %d, want 2", nextID)
	}
	if !equalStrings(assignments[0], []string{"AAAUSDT", "BBBUSDT"}) {
		t.Fatalf("existing full shard was rearranged: %v", assignments[0])
	}
	if !equalStrings(assignments[1], []string{"DDDUSDT"}) {
		t.Fatalf("new symbol was not placed into the least-loaded shard: %v", assignments[1])
	}
}

func TestDynamicSubscriptionsUpdateWithoutDroppingConnection(t *testing.T) {
	manager := testManager(t)
	conn := newFakeWireConn()
	dialer := &fakeConnector{conns: []wireConn{conn}, dialed: make(chan int, 4)}
	manager.connector = dialer
	runtime := newManagedShard([]string{"BTCUSDT"}, func() {})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- manager.connectAndServeDynamic(ctx, runtime)
	}()

	waitFor(t, time.Second, func() bool {
		return hasOperationForTopic(conn.writeSnapshot(), "subscribe", "publicTrade.BTCUSDT") &&
			hasOperationForTopic(conn.writeSnapshot(), "subscribe", "orderbook.50.BTCUSDT")
	})
	runtime.setSymbols([]string{"BTCUSDT", "ETHUSDT"})
	waitFor(t, time.Second, func() bool {
		return hasOperationForTopic(conn.writeSnapshot(), "subscribe", "publicTrade.ETHUSDT") &&
			hasOperationForTopic(conn.writeSnapshot(), "subscribe", "orderbook.50.ETHUSDT")
	})
	runtime.setSymbols([]string{"ETHUSDT"})
	waitFor(t, time.Second, func() bool {
		return hasOperationForTopic(conn.writeSnapshot(), "unsubscribe", "publicTrade.BTCUSDT") &&
			hasOperationForTopic(conn.writeSnapshot(), "unsubscribe", "orderbook.50.BTCUSDT") &&
			equalStrings(runtime.snapshotActiveSymbols(), []string{"ETHUSDT"})
	})

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("динамическое WS-соединение не остановилось после отмены контекста")
	}
	if dialer.dials != 1 {
		t.Fatalf("динамические subscribe/unsubscribe вызвали %d подключений, ожидалось одно", dialer.dials)
	}
}

func hasOperationForTopic(messages [][]byte, operation, topic string) bool {
	for _, message := range messages {
		var value subscriptionBatch
		if json.Unmarshal(message, &value) == nil && value.Operation == operation {
			for _, argument := range value.Arguments {
				if argument == topic {
					return true
				}
			}
		}
	}
	return false
}

func TestSetSymbolsCoalescesAndSorts(t *testing.T) {
	manager := testManager(t)
	manager.SetSymbols([]string{"ethusdt", "BTCUSDT", "BTCUSDT", "bad symbol"})
	manager.mu.Lock()
	got := append([]string(nil), manager.symbols...)
	manager.mu.Unlock()
	if len(got) != 2 || got[0] != "BTCUSDT" || got[1] != "ETHUSDT" {
		t.Fatalf("normalized symbols = %v", got)
	}
	select {
	case <-manager.updates:
	default:
		t.Fatal("изменение списка инструментов не отправило уведомление")
	}
}

func hasSubscribe(messages [][]byte) bool {
	for _, message := range messages {
		var value map[string]any
		if json.Unmarshal(message, &value) == nil && value["op"] == "subscribe" {
			return true
		}
	}
	return false
}

func hasPing(messages [][]byte) bool {
	for _, message := range messages {
		var value map[string]any
		if json.Unmarshal(message, &value) == nil && value["op"] == "ping" {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("условие не выполнено до истечения таймаута")
}


func TestReadLoopResubscribesOnlyBrokenOrderbookTopic(t *testing.T) {
	manager := testManager(t)
	manager.store.StartSession([]string{"BTCUSDT", "ETHUSDT"})
	conn := newFakeWireConn()
	now := time.Now()

	delta := withFreshTimestamp(`{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1,"data":{"s":"BTCUSDT","b":[["100","2"]],"a":[],"u":10,"seq":10}}`, now)
	snapshot := withFreshTimestamp(`{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","1"]],"u":10,"seq":10}}`, now.Add(time.Millisecond))
	trade := withFreshTimestamp(`{"topic":"publicTrade.ETHUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"ETHUSDT","S":"Buy","v":"0.1","p":"200","i":"eth-trade-1"}]}`, now.Add(2*time.Millisecond))

	conn.reads <- fakeRead{messageType: 1, payload: delta}
	conn.reads <- fakeRead{messageType: 1, payload: snapshot}
	conn.reads <- fakeRead{messageType: 1, payload: trade}
	_ = conn.Close()

	writer := &serializedWriter{conn: conn}
	err := manager.readLoop(context.Background(), conn, writer)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("readLoop error = %v, want EOF after all queued messages", err)
	}

	writes := conn.writeSnapshot()
	if len(writes) != 2 {
		t.Fatalf("число команд повторной подписки = %d, ожидалось 2: %s", len(writes), writes)
	}
	for i, expectedOperation := range []string{"unsubscribe", "subscribe"} {
		var command struct {
			Operation string   `json:"op"`
			Arguments []string `json:"args"`
		}
		if err := json.Unmarshal(writes[i], &command); err != nil {
			t.Fatalf("команда %d не является JSON: %v", i, err)
		}
		if command.Operation != expectedOperation {
			t.Errorf("команда %d op = %q, ожидалось %q", i, command.Operation, expectedOperation)
		}
		if len(command.Arguments) != 1 || command.Arguments[0] != "orderbook.50.BTCUSDT" {
			t.Errorf("команда %d затронула не только сломанный стакан: %+v", i, command.Arguments)
		}
	}

	btc, ok := manager.store.Snapshot("BTCUSDT", now.Add(time.Second))
	if !ok || !btc.OrderBook.Valid {
		t.Fatalf("BTCUSDT snapshot не восстановлен: %+v", btc.OrderBook)
	}
	eth, ok := manager.store.Snapshot("ETHUSDT", now.Add(time.Second))
	if !ok || eth.LastTrade == nil || eth.LastTrade.TradeID != "eth-trade-1" {
		t.Fatalf("поток ETHUSDT остановился из-за ошибки BTCUSDT: %+v", eth)
	}
}
