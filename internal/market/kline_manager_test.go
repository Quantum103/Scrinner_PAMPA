package market

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
)

type fakeKlineREST struct {
	calls atomic.Int32
}

func (f *fakeKlineREST) FetchMinuteKlines(_ context.Context, _ string, _ int) ([]bybit.Kline, error) {
	f.calls.Add(1)
	return nil, nil
}

type fakeKlineRead struct {
	messageType int
	payload     []byte
	err         error
}

type fakeKlineWireConn struct {
	reads   chan fakeKlineRead
	writes  chan []byte
	closed  chan struct{}
	closeMu sync.Once
}

func newFakeKlineWireConn() *fakeKlineWireConn {
	return &fakeKlineWireConn{
		reads: make(chan fakeKlineRead, 16),
		writes: make(chan []byte, 64),
		closed: make(chan struct{}),
	}
}

func (c *fakeKlineWireConn) ReadMessage() (int, []byte, error) {
	select {
	case <-c.closed:
		return 0, nil, io.EOF
	case message := <-c.reads:
		if message.err != nil {
			return 0, nil, message.err
		}
		return message.messageType, message.payload, nil
	}
}

func (c *fakeKlineWireConn) WriteMessage(_ int, payload []byte) error {
	copyPayload := append([]byte(nil), payload...)
	select {
	case <-c.closed:
		return io.ErrClosedPipe
	case c.writes <- copyPayload:
		return nil
	}
}

func (c *fakeKlineWireConn) SetReadLimit(int64) {}
func (c *fakeKlineWireConn) SetReadDeadline(time.Time) error { return nil }
func (c *fakeKlineWireConn) SetWriteDeadline(time.Time) error { return nil }
func (c *fakeKlineWireConn) SetPongHandler(func(string) error) {}
func (c *fakeKlineWireConn) Close() error {
	c.closeMu.Do(func() { close(c.closed) })
	return nil
}

type fakeKlineConnector struct {
	conns []*fakeKlineWireConn
	dials atomic.Int32
}

func (c *fakeKlineConnector) Dial(context.Context, string) (klineWireConn, error) {
	index := int(c.dials.Add(1)) - 1
	if index >= len(c.conns) {
		return nil, errors.New("unexpected reconnect during dynamic subscription test")
	}
	return c.conns[index], nil
}

func testKlineConfig() KlineConfig {
	return KlineConfig{
		HistoryLimit: 21,
		HistoryWorkers: 1,
		MaxSymbolsPerConnection: 2,
		MaxArgsPerSubscribe: 2,
		MaxShards: 2,
		MaxDataAge: 90 * time.Second,
		ReconnectMin: 10 * time.Millisecond,
		ReconnectMax: 50 * time.Millisecond,
	}
}

func waitKlineCommand(t *testing.T, conn *fakeKlineWireConn, operation, topic string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case payload := <-conn.writes:
			var command klineSubscriptionBatch
			if err := json.Unmarshal(payload, &command); err != nil {
				t.Fatalf("команда не JSON: %v", err)
			}
			if command.Operation != operation {
				continue
			}
			for _, argument := range command.Arguments {
				if argument == topic {
					return
				}
			}
		case <-deadline:
			t.Fatalf("не получена %s-команда для %s", operation, topic)
		}
	}
}

func waitKlineTopics(t *testing.T, conn *fakeKlineWireConn, operation string, topics ...string) {
	t.Helper()
	pending := make(map[string]struct{}, len(topics))
	for _, topic := range topics { pending[topic] = struct{}{} }
	deadline := time.After(2 * time.Second)
	for len(pending) > 0 {
		select {
		case payload := <-conn.writes:
			var command klineSubscriptionBatch
			if err := json.Unmarshal(payload, &command); err != nil { t.Fatalf("команда не JSON: %v", err) }
			if command.Operation != operation { continue }
			for _, argument := range command.Arguments { delete(pending, argument) }
		case <-deadline:
			t.Fatalf("не получена %s-команда для тем %v", operation, pending)
		}
	}
}

func TestKlineManagerChangesTopicsWithoutReconnectingShard(t *testing.T) {
	conn := newFakeKlineWireConn()
	connector := &fakeKlineConnector{conns: []*fakeKlineWireConn{conn}}
	rest := &fakeKlineREST{}
	store := NewCandleStore(90 * time.Second)
	manager, err := NewKlineManager(rest, "wss://example.test/v5/public/linear", testKlineConfig(), store, nil)
	if err != nil { t.Fatal(err) }
	manager.setConnector(connector)
	manager.SetSymbols([]string{"AAAUSDT"})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		manager.Run(ctx)
	}()

	waitKlineCommand(t, conn, "subscribe", "kline.1.AAAUSDT")
	manager.SetSymbols([]string{"AAAUSDT", "BBBUSDT"})
	waitKlineCommand(t, conn, "subscribe", "kline.1.BBBUSDT")
	manager.SetSymbols([]string{"BBBUSDT"})
	waitKlineCommand(t, conn, "unsubscribe", "kline.1.AAAUSDT")
	manager.SetSymbols([]string{"BBBUSDT", "CCCUSDT"})
	waitKlineCommand(t, conn, "subscribe", "kline.1.CCCUSDT")

	if got := connector.dials.Load(); got != 1 {
		t.Fatalf("динамическое изменение тем создало %d соединений, ожидалось одно", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("KlineManager не завершился после отмены контекста")
	}
}

func TestKlineManagerKeepsExistingSymbolsOnTheirShardWhenUniverseChanges(t *testing.T) {
	connA := newFakeKlineWireConn()
	connB := newFakeKlineWireConn()
	connector := &fakeKlineConnector{conns: []*fakeKlineWireConn{connA, connB}}
	config := testKlineConfig()
	config.MaxSymbolsPerConnection = 2
	config.MaxShards = 3
	store := NewCandleStore(time.Minute)
	manager, err := NewKlineManager(&fakeKlineREST{}, "wss://example.test/v5/public/linear", config, store, nil)
	if err != nil { t.Fatal(err) }
	manager.setConnector(connector)
	manager.SetSymbols([]string{"AAAUSDT", "BBBUSDT", "CCCUSDT"})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); manager.Run(ctx) }()

	waitKlineTopics(t, connA, "subscribe", "kline.1.AAAUSDT", "kline.1.BBBUSDT")
	waitKlineCommand(t, connB, "subscribe", "kline.1.CCCUSDT")

	// Освобождаем слот в первом сегменте и добавляем новый инструмент.
	// BBB остаётся на первом сегменте, CCC — на втором.
	manager.SetSymbols([]string{"BBBUSDT", "CCCUSDT", "DDDUSDT"})
	waitKlineCommand(t, connA, "unsubscribe", "kline.1.AAAUSDT")
	waitKlineCommand(t, connA, "subscribe", "kline.1.DDDUSDT")
	if got := connector.dials.Load(); got != 2 {
		t.Fatalf("состав рынка изменил %d WebSocket-соединений, ожидалось 2 стабильных сегмента", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("KlineManager не остановился после отмены контекста")
	}
}

func TestKlineManagerHandlesCurrentConfirmAndDuplicateUpdates(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 34, 30, 0, time.UTC)
	store := NewCandleStore(time.Minute)
	store.Activate("AAAUSDT")
	if err := store.LoadHistory("AAAUSDT", testMinuteHistory(now, 20), now); err != nil { t.Fatal(err) }
	manager, err := NewKlineManager(&fakeKlineREST{}, "wss://example.test/v5/public/linear", testKlineConfig(), store, nil)
	if err != nil { t.Fatal(err) }

	start := now.Truncate(time.Minute).UnixMilli()
	makeMessage := func(confirm bool, turnover string) []byte {
		return []byte(fmt.Sprintf(`{"topic":"kline.1.AAAUSDT","type":"snapshot","data":[{"start":%d,"end":%d,"interval":"1","open":"100","high":"103","low":"99","close":"102","volume":"2","turnover":"%s","confirm":%t,"timestamp":%d}]}`,
			start, start+59_999, turnover, confirm, now.UnixMilli()))
	}

	if err := manager.handleMessage(makeMessage(false, "50"), now); err != nil { t.Fatal(err) }
	if err := manager.handleMessage(makeMessage(false, "125"), now.Add(time.Second)); err != nil { t.Fatal(err) }
	snapshot, _ := store.Snapshot("AAAUSDT", now.Add(time.Second))
	if len(snapshot.History) != 20 || snapshot.Current == nil || snapshot.Current.Turnover != 125 {
		t.Fatalf("текущая свеча была добавлена в историю или не обновилась: %+v", snapshot)
	}
	if err := manager.handleMessage(makeMessage(true, "150"), now.Add(2*time.Second)); err != nil { t.Fatal(err) }
	if err := manager.handleMessage(makeMessage(true, "150"), now.Add(3*time.Second)); err != nil { t.Fatal(err) }
	snapshot, _ = store.Snapshot("AAAUSDT", now.Add(3*time.Second))
	if len(snapshot.History) != 20 || snapshot.History[19].StartTime.UnixMilli() != start ||
		snapshot.History[19].Turnover != 150 || snapshot.Current != nil {
		t.Fatalf("confirm/дубликат обработаны неверно: %+v", snapshot)
	}
}

func TestKlineTopicValidationAndChunkLimits(t *testing.T) {
	if _, ok := klineSymbolFromTopic("kline.1.BTCUSDT"); !ok {
		t.Fatal("валидная тема не распознана")
	}
	if _, ok := klineSymbolFromTopic("kline.5.BTCUSDT"); ok {
		t.Fatal("неверный интервал должен быть отклонён")
	}
	topics := make([]string, 25)
	for index := range topics { topics[index] = fmt.Sprintf("kline.1.COIN%02dUSDT", index) }
	batches := chunkKlineTopics("subscribe", topics, 10)
	if len(batches) != 3 || len(batches[0].Arguments) != 10 || len(batches[1].Arguments) != 10 || len(batches[2].Arguments) != 5 {
		t.Fatalf("неверно разбиты команды: %+v", batches)
	}
}


func TestKlineManagerReconnectsDisconnectedSegmentAndResubscribes(t *testing.T) {
	first := newFakeKlineWireConn()
	first.reads <- fakeKlineRead{err: io.EOF}
	second := newFakeKlineWireConn()
	connector := &fakeKlineConnector{conns: []*fakeKlineWireConn{first, second}}
	store := NewCandleStore(90 * time.Second)
	manager, err := NewKlineManager(&fakeKlineREST{}, "wss://example.test/v5/public/linear", testKlineConfig(), store, nil)
	if err != nil { t.Fatal(err) }
	manager.setConnector(connector)
	manager.SetSymbols([]string{"AAAUSDT"})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); manager.Run(ctx) }()

	waitKlineCommand(t, first, "subscribe", "kline.1.AAAUSDT")
	waitKlineCommand(t, second, "subscribe", "kline.1.AAAUSDT")
	if got := connector.dials.Load(); got != 2 {
		t.Fatalf("после разрыва сегмент подключался %d раз, ожидалось 2", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("KlineManager не остановился после отмены контекста")
	}
}
