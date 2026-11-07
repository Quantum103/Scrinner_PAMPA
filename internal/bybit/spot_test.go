package bybit

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestGetSpotInstrumentsFiltersActiveUSDTPairs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v5/market/instruments-info" || r.URL.Query().Get("category") != "spot" || r.URL.Query().Get("status") != "Trading" {
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
		if r.URL.Query().Has("cursor") || r.URL.Query().Has("limit") {
			t.Fatalf("Spot instruments endpoint must not be paginated: %s", r.URL.RawQuery)
		}
		writeResult(w, map[string]any{"list": []any{
			map[string]any{"symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT", "status": "Trading"},
			map[string]any{"symbol": "ABCUSDC", "baseCoin": "ABC", "quoteCoin": "USDC", "status": "Trading"},
			map[string]any{"symbol": "DEFUSDT", "baseCoin": "DEF", "quoteCoin": "USDT", "status": "PendingOpen"},
			map[string]any{"symbol": "xyzusdt", "baseCoin": "xyz", "quoteCoin": "usdt", "status": "Trading"},
		}})
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.GetSpotInstruments(context.Background())
	if err != nil {
		t.Fatalf("GetSpotInstruments() error = %v", err)
	}
	if len(got) != 2 || got[0].Symbol != "BTCUSDT" || got[1].Symbol != "XYZUSDT" {
		t.Fatalf("unexpected Spot instruments: %+v", got)
	}
}

func TestGetSpotInstrumentsRejectsNoUSDTPairs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeResult(w, map[string]any{"list": []any{
			map[string]any{"symbol": "ABCUSDC", "baseCoin": "ABC", "quoteCoin": "USDC", "status": "Trading"},
		}})
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetSpotInstruments(context.Background()); err == nil {
		t.Fatal("expected error when no active USDT Spot pair is returned")
	}
}

func TestGetSpotTickersMapsTurnoverAndPrice(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v5/market/tickers" || r.URL.Query().Get("category") != "spot" {
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
		writeResult(w, map[string]any{"list": []any{
			map[string]any{"symbol": "ABCUSDT", "lastPrice": "0.00482", "turnover24h": "347821.45"},
			map[string]any{"symbol": "BTCUSDT", "lastPrice": "65000", "turnover24h": "1200000000"},
		}})
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.GetSpotTickers(context.Background())
	if err != nil {
		t.Fatalf("GetSpotTickers() error = %v", err)
	}
	if len(got) != 2 || got[0].Symbol != "ABCUSDT" || got[0].LastPrice != "0.00482" || got[0].Turnover24h != "347821.45" {
		t.Fatalf("unexpected Spot tickers: %+v", got)
	}
}

func TestGetSpotTickersRejectsDuplicateSymbols(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"retCode": 0, "retMsg": "OK", "result": map[string]any{"list": []any{
			map[string]any{"symbol": "ABCUSDT", "lastPrice": "1", "turnover24h": "10"},
			map[string]any{"symbol": "ABCUSDT", "lastPrice": "1", "turnover24h": "10"},
		}}})
	}))
	defer server.Close()
	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetSpotTickers(context.Background()); err == nil {
		t.Fatal("expected duplicate Spot ticker error")
	}
}

func TestGetSpotKlinesAtReturnsOnlyClosedOneMinuteCandles(t *testing.T) {
	serverTime := time.Date(2026, 11, 8, 12, 7, 30, 0, time.UTC)
	closedStart := time.Date(2026, 11, 8, 12, 6, 0, 0, time.UTC).UnixMilli()
	openStart := time.Date(2026, 11, 8, 12, 7, 0, 0, time.UTC).UnixMilli()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v5/market/kline" ||
			r.URL.Query().Get("category") != "spot" ||
			r.URL.Query().Get("symbol") != "ABCUSDT" ||
			r.URL.Query().Get("interval") != "1" ||
			r.URL.Query().Get("limit") != "20" {
			t.Fatalf("unexpected Spot kline request: %s", r.URL.String())
		}
		if got := r.URL.Query().Get("end"); got != strconv.FormatInt(serverTime.UnixMilli(), 10) {
			t.Fatalf("end = %q, want %d", got, serverTime.UnixMilli())
		}
		writeResult(w, map[string]any{"list": [][]string{
			{strconv.FormatInt(openStart, 10), "101", "103", "100", "102", "20", "2000"},
			{strconv.FormatInt(closedStart, 10), "99", "102", "98", "101", "10", "1000"},
		}})
	}))
	defer server.Close()

	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	candles, err := client.GetSpotKlinesAt(context.Background(), "abcusdt", 20, serverTime)
	if err != nil {
		t.Fatalf("GetSpotKlinesAt() error = %v", err)
	}
	if len(candles) != 1 || candles[0].Timeframe != "1" ||
		candles[0].Start.UnixMilli() != closedStart ||
		!candles[0].Close.Equal(serverTime.Add(-30*time.Second)) ||
		candles[0].ClosePrice != 101 {
		t.Fatalf("unexpected Spot historical candles: %+v", candles)
	}
}

func TestGetSpotKlinesAtValidatesArguments(t *testing.T) {
	client, err := NewClient("https://api.bybit.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, test := range []struct {
		symbol     string
		limit      int
		serverTime time.Time
	}{
		{symbol: "", limit: 20, serverTime: now},
		{symbol: "BAD-SYMBOL", limit: 20, serverTime: now},
		{symbol: "ABCUSDT", limit: 0, serverTime: now},
		{symbol: "ABCUSDT", limit: 1001, serverTime: now},
		{symbol: "ABCUSDT", limit: 20, serverTime: time.Time{}},
	} {
		if _, err := client.GetSpotKlinesAt(context.Background(), test.symbol, test.limit, test.serverTime); err == nil {
			t.Fatalf("expected invalid arguments to fail: %+v", test)
		}
	}
}
