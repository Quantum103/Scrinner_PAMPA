package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestBuildSpotTickerTopicsValidatesAndDeduplicates(t *testing.T) {
	got, err := buildSpotTickerTopics([]string{"abcusdt", "BTCUSDT", "ABCUSDT"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"tickers.ABCUSDT", "tickers.BTCUSDT"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("topics = %#v, want %#v", got, want)
	}
	for _, invalid := range []string{"", "ABC-USDT", "ABC/USDT"} {
		if _, err := buildSpotTickerTopics([]string{invalid}); err == nil {
			t.Fatalf("expected invalid symbol %q to fail", invalid)
		}
	}
}

func TestBuildSpotTickerTopicsEnforcesConnectionLimit(t *testing.T) {
	stream, err := NewPublicSpotStream("wss://stream.bybit.com/v5/public/spot")
	if err != nil {
		t.Fatal(err)
	}
	symbols := make([]string, 101)
	for i := range symbols {
		symbols[i] = fmt.Sprintf("COIN%dUSDT", i)
	}
	err = stream.Run(context.Background(), symbols, SpotStreamHandlers{
		OnTicker: func(context.Context, SpotPriceUpdate) error { return nil },
	})
	if err == nil || !strings.Contains(err.Error(), "at most 100") {
		t.Fatalf("expected 100-topic connection limit error, got %v", err)
	}
}

func TestParseSpotTickerMessage(t *testing.T) {
	timestamp := time.Date(2026, 11, 8, 12, 34, 56, 0, time.UTC)
	payload := fmt.Sprintf(`{"topic":"tickers.ABCUSDT","type":"snapshot","ts":%d,"data":{"symbol":"ABCUSDT","lastPrice":"0.00482"}}`, timestamp.UnixMilli())
	update, ok, err := parseSpotTickerMessage([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || update.Symbol != "ABCUSDT" || update.Price != 0.00482 || !update.At.Equal(timestamp) {
		t.Fatalf("unexpected parsed ticker: %+v ok=%v", update, ok)
	}

	_, ok, err = parseSpotTickerMessage([]byte(`{"op":"subscribe","success":true,"ret_msg":"subscribe"}`))
	if err != nil || ok {
		t.Fatalf("subscription acknowledgement should be ignored: ok=%v error=%v", ok, err)
	}
	_, _, err = parseSpotTickerMessage([]byte(`{"op":"subscribe","success":false,"ret_msg":"bad topic","retCode":10001}`))
	if err == nil {
		t.Fatal("rejected subscription must return an error")
	}
	_, ok, err = parseSpotTickerMessage([]byte(`{"topic":"publicTrade.ABCUSDT","data":[]}`))
	if err != nil || ok {
		t.Fatalf("unrelated topic should be ignored: ok=%v error=%v", ok, err)
	}
}

func TestParseSpotTickerMessageRejectsBadData(t *testing.T) {
	for _, payload := range []string{
		`{"topic":"tickers.ABCUSDT","ts":1,"data":{"symbol":"ABCUSDT","lastPrice":"0"}}`,
		`{"topic":"tickers.ABCUSDT","ts":1,"data":{"symbol":"OTHERUSDT","lastPrice":"1"}}`,
		`{"topic":"tickers.ABCUSDT","ts":0,"data":{"symbol":"ABCUSDT","lastPrice":"1"}}`,
	} {
		if _, _, err := parseSpotTickerMessage([]byte(payload)); err == nil {
			t.Fatalf("expected invalid payload to fail: %s", payload)
		}
	}
}

func TestPublicSpotStreamSubscribesInBatchesAndDeliversTicker(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for expected := 0; expected < 2; expected++ {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var request struct {
				Op   string   `json:"op"`
				Args []string `json:"args"`
			}
			if json.Unmarshal(payload, &request) != nil || request.Op != "subscribe" || len(request.Args) > 10 {
				return
			}
			_ = conn.WriteJSON(map[string]any{"op": "subscribe", "success": true, "ret_msg": "subscribe"})
		}
		_ = conn.WriteJSON(map[string]any{
			"topic": "tickers.COIN10USDT", "type": "snapshot", "ts": time.Now().UnixMilli(),
			"data": map[string]string{"symbol": "COIN10USDT", "lastPrice": "2.5"},
		})
		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()

	stream, err := NewPublicSpotStream("ws" + strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatal(err)
	}
	stream.heartbeatInterval = time.Hour
	symbols := make([]string, 11)
	for i := range symbols {
		symbols[i] = "COIN" + strconv.Itoa(i) + "USDT"
	}
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan SpotPriceUpdate, 1)
	done := make(chan error, 1)
	go func() {
		done <- stream.Run(ctx, symbols, SpotStreamHandlers{
			OnTicker: func(_ context.Context, update SpotPriceUpdate) error {
				got <- update
				cancel()
				return nil
			},
		})
	}()
	select {
	case update := <-got:
		if update.Symbol != "COIN10USDT" || update.Price != 2.5 {
			t.Fatalf("unexpected update: %+v", update)
		}
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("timed out waiting for Spot ticker update")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Spot stream returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Spot stream did not stop after context cancellation")
	}
}
