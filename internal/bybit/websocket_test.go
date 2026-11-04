package bybit

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestParseKlineMessageOnlyReturnsConfirmedCandles(t *testing.T) {
	payload := []byte(`{
		"topic":"kline.5.BTCUSDT",
		"type":"snapshot",
		"data":[
			{"start":1793793600000,"end":1793793899999,"interval":"5","open":"100","close":"102","high":"103","low":"99","volume":"10","turnover":"1000","confirm":false},
			{"start":1793793900000,"end":1793794199999,"interval":"5","open":"102","close":"105","high":"106","low":"101","volume":"12","turnover":"1260","confirm":true}
		]
	}`)
	got, err := parseKlineMessage(payload)
	if err != nil {
		t.Fatalf("parseKlineMessage() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("returned %d candles, want one confirmed candle: %+v", len(got), got)
	}
	if got[0].Symbol != "BTCUSDT" || got[0].Timeframe != "5" ||
		got[0].ClosePrice != 105 || got[0].Volume != 12 {
		t.Fatalf("unexpected parsed candle: %+v", got[0])
	}
	wantStart := time.UnixMilli(1793793900000).UTC()
	if !got[0].Start.Equal(wantStart) || !got[0].Close.Equal(wantStart.Add(5*time.Minute)) {
		t.Fatalf("candle interval = %s..%s", got[0].Start, got[0].Close)
	}
}

func TestParseKlineMessageRejectsInvalidConfirmedCandle(t *testing.T) {
	payload := []byte(`{
		"topic":"kline.5.BTCUSDT",
		"data":[
			{"start":1793793900000,"end":1793794199999,"interval":"5","open":"100","close":"90","high":"95","low":"99","volume":"12","turnover":"1260","confirm":true}
		]
	}`)
	if _, err := parseKlineMessage(payload); err == nil {
		t.Fatal("invalid OHLC range must be rejected")
	}
}

func TestParseKlineMessageHandlesControlFrames(t *testing.T) {
	for _, payload := range []string{
		`{"op":"pong","success":true}`,
		`{"op":"subscribe","success":true,"ret_msg":""}`,
	} {
		got, err := parseKlineMessage([]byte(payload))
		if err != nil || len(got) != 0 {
			t.Fatalf("control frame %s returned candles=%+v error=%v", payload, got, err)
		}
	}
	if _, err := parseKlineMessage([]byte(`{"op":"subscribe","success":false,"ret_msg":"invalid topic","retCode":10001}`)); err == nil {
		t.Fatal("rejected subscription acknowledgement must return an error")
	}
}

func TestBuildKlineTopicsValidatesAndDeduplicates(t *testing.T) {
	got, err := buildKlineTopics([]KlineSubscription{
		{Symbol: "btcusdt", Timeframe: "5"},
		{Symbol: "ETHUSDT", Timeframe: "15"},
		{Symbol: "BTCUSDT", Timeframe: "5"},
	})
	if err != nil {
		t.Fatalf("buildKlineTopics() error = %v", err)
	}
	if len(got) != 2 || got[0] != "kline.15.ETHUSDT" || got[1] != "kline.5.BTCUSDT" {
		t.Fatalf("unexpected topics: %#v", got)
	}
	if _, err := buildKlineTopics([]KlineSubscription{{Symbol: "BTC-USDT", Timeframe: "5"}}); err == nil {
		t.Fatal("symbol with a separator must be rejected")
	}
	if _, err := buildKlineTopics([]KlineSubscription{{Symbol: "BTCUSDT", Timeframe: "30"}}); err == nil {
		t.Fatal("unsupported timeframe must be rejected")
	}
}

func TestPublicStreamReconnectsAndResubscribes(t *testing.T) {
	var connections atomic.Int32
	var subscriptions atomic.Int32
	var connected atomic.Int32
	var disconnected atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		attempt := connections.Add(1)
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var request struct {
			Operation string   `json:"op"`
			Arguments []string `json:"args"`
		}
		if json.Unmarshal(payload, &request) != nil || request.Operation != "subscribe" ||
			len(request.Arguments) != 1 || request.Arguments[0] != "kline.5.BTCUSDT" {
			return
		}
		subscriptions.Add(1)
		if attempt == 1 {
			return
		}
		message := map[string]any{
			"topic": "kline.5.BTCUSDT",
			"data": []any{
				map[string]any{
					"start": 1793793900000, "end": 1793794199999, "interval": "5",
					"open": "102", "close": "105", "high": "106", "low": "101",
					"volume": "12", "turnover": "1260", "confirm": true,
				},
			},
		}
		_ = conn.WriteJSON(message)
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()

	stream, err := NewPublicStream("ws" + strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatal(err)
	}
	stream.reconnectMin = time.Millisecond
	stream.reconnectMax = 5 * time.Millisecond
	stream.heartbeatInterval = 5 * time.Millisecond
	stream.readTimeout = time.Second
	stream.writeTimeout = time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	candleReceived := make(chan Candle, 1)
	runDone := make(chan error, 1)
	go func() {
		runDone <- stream.Run(ctx, []KlineSubscription{{Symbol: "BTCUSDT", Timeframe: "5"}}, StreamHandlers{
			OnConnected: func(context.Context) error {
				connected.Add(1)
				return nil
			},
			OnCandle: func(_ context.Context, candle Candle) error {
				candleReceived <- candle
				cancel()
				return nil
			},
			OnDisconnect: func(error) {
				disconnected.Add(1)
			},
		})
	}()

	select {
	case candle := <-candleReceived:
		if candle.Symbol != "BTCUSDT" || candle.ClosePrice != 105 {
			t.Fatalf("unexpected WebSocket candle: %+v", candle)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not reconnect and deliver a confirmed candle")
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop after context cancellation")
	}
	if connections.Load() < 2 || subscriptions.Load() < 2 || connected.Load() < 2 || disconnected.Load() < 1 {
		t.Fatalf("reconnect counters: connections=%d subscriptions=%d connected=%d disconnected=%d",
			connections.Load(), subscriptions.Load(), connected.Load(), disconnected.Load())
	}
}
