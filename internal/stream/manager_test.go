package stream

import (
	"context"
	"encoding/json"
	"errors"
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
