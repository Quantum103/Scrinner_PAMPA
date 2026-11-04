package bybit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func writeResult(w http.ResponseWriter, result any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"retCode": 0, "retMsg": "OK", "result": result})
}

func TestGetInstrumentsPaginatesAndFilters(t *testing.T) {
	var pages atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("category") != "linear" ||
			r.URL.Query().Get("status") != "Trading" || r.URL.Query().Get("limit") != "1000" {
			t.Fatalf("unexpected query: %s", r.URL.RawQuery)
		}
		switch r.URL.Query().Get("cursor") {
		case "":
			pages.Add(1)
			writeResult(w, map[string]any{
				"nextPageCursor": "page-two",
				"list": []any{
					map[string]any{"symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT", "settleCoin": "USDT", "contractType": "LinearPerpetual", "status": "Trading"},
					map[string]any{"symbol": "BTCUSDC", "baseCoin": "BTC", "quoteCoin": "USDC", "settleCoin": "USDC", "contractType": "LinearPerpetual", "status": "Trading"},
					map[string]any{"symbol": "BTCUSDT-30JUN27", "baseCoin": "BTC", "quoteCoin": "USDT", "settleCoin": "USDT", "contractType": "LinearFutures", "status": "Trading"},
				},
			})
		case "page-two":
			pages.Add(1)
			writeResult(w, map[string]any{
				"nextPageCursor": "",
				"list": []any{
					map[string]any{"symbol": "ETHUSDT", "baseCoin": "ETH", "quoteCoin": "USDT", "settleCoin": "USDT", "contractType": "LinearPerpetual", "status": "Trading"},
					map[string]any{"symbol": "XRPUSDT", "baseCoin": "XRP", "quoteCoin": "USDT", "settleCoin": "USDT", "contractType": "LinearPerpetual", "status": "PendingOpen"},
				},
			})
		default:
			t.Fatalf("unexpected cursor: %s", r.URL.Query().Get("cursor"))
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.GetInstruments(context.Background())
	if err != nil {
		t.Fatalf("GetInstruments() error = %v", err)
	}
	if pages.Load() != 2 || len(got) != 2 || got[0].Symbol != "BTCUSDT" || got[1].Symbol != "ETHUSDT" {
		t.Fatalf("pages=%d instruments=%+v", pages.Load(), got)
	}
}

func TestGetKlinesReturnsClosedChronologicalCandles(t *testing.T) {
	serverTime := time.Date(2026, 11, 4, 12, 7, 30, 0, time.UTC)
	currentStart := time.Date(2026, 11, 4, 12, 5, 0, 0, time.UTC).UnixMilli()
	previousStart := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC).UnixMilli()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v5/market/time":
			writeResult(w, map[string]any{
				"timeSecond": strconv.FormatInt(serverTime.Unix(), 10),
				"timeNano":   strconv.FormatInt(serverTime.UnixNano(), 10),
			})
		case "/v5/market/kline":
			if r.URL.Query().Get("symbol") != "BTCUSDT" || r.URL.Query().Get("interval") != "5" {
				t.Fatalf("unexpected kline query: %s", r.URL.RawQuery)
			}
			writeResult(w, map[string]any{"list": [][]string{
				{strconv.FormatInt(currentStart, 10), "101", "103", "100", "102", "20", "2000"},
				{strconv.FormatInt(previousStart, 10), "99", "102", "98", "101", "10", "1000"},
			}})
		default:
			t.Fatalf("unexpected endpoint: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.GetKlines(context.Background(), "btcusdt", "5", 100)
	if err != nil {
		t.Fatalf("GetKlines() error = %v", err)
	}
	if len(got) != 1 || got[0].Start.UnixMilli() != previousStart || got[0].ClosePrice != 101 {
		t.Fatalf("unexpected closed candles: %+v", got)
	}
}

func TestGetTickersRetriesRateLimit(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"retCode": 10006, "retMsg": "too many visits", "result": map[string]any{}})
			return
		}
		writeResult(w, map[string]any{"list": []any{
			map[string]any{"symbol": "BTCUSDT", "lastPrice": "65000", "turnover24h": "1000"},
		}})
	}))
	defer server.Close()

	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.GetTickers(context.Background())
	if err != nil {
		t.Fatalf("GetTickers() error = %v", err)
	}
	if requests.Load() != 2 || len(got) != 1 || got[0].Symbol != "BTCUSDT" {
		t.Fatalf("requests=%d tickers=%+v", requests.Load(), got)
	}
}

func TestGetTickersReturnsPermanentBybitError(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"retCode": 10001, "retMsg": "invalid parameter", "result": map[string]any{}})
	}))
	defer server.Close()

	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetTickers(context.Background())
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != 10001 || requests.Load() != 1 {
		t.Fatalf("error=%v requests=%d", err, requests.Load())
	}
}

func TestNewClientAndKlineParametersAreValidated(t *testing.T) {
	for _, value := range []string{"", "api.bybit.com", "ftp://api.bybit.com", "https://api.bybit.com?x=1"} {
		t.Run(fmt.Sprintf("base_%q", value), func(t *testing.T) {
			if _, err := NewClient(value, nil); err == nil {
				t.Fatal("expected invalid base URL error")
			}
		})
	}
	client, err := NewClient("https://api.bybit.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetKlines(context.Background(), "BTCUSDT", "1", 100); err == nil {
		t.Fatal("unsupported timeframe must fail")
	}
	if _, err := client.GetKlines(context.Background(), "BTCUSDT", "5", 1001); err == nil {
		t.Fatal("oversized kline limit must fail")
	}
}

func TestGetKlinesSupports30MinuteInterval(t *testing.T) {
	serverTime := time.Date(2026, 11, 4, 12, 35, 0, 0, time.UTC)
	closedStart := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC).UnixMilli()
	openStart := time.Date(2026, 11, 4, 12, 30, 0, 0, time.UTC).UnixMilli()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v5/market/kline" || r.URL.Query().Get("interval") != "30" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
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
	candles, err := client.GetKlinesAt(context.Background(), "BTCUSDT", "30", 100, serverTime)
	if err != nil {
		t.Fatalf("GetKlinesAt(30) error = %v", err)
	}
	if len(candles) != 1 || candles[0].Timeframe != "30" ||
		candles[0].Start.UnixMilli() != closedStart ||
		!candles[0].Close.Equal(serverTime.Add(-5*time.Minute)) {
		t.Fatalf("unexpected 30-minute closed candles: %+v", candles)
	}
}

func TestGetKlinesAtReusesProvidedServerTime(t *testing.T) {
	serverTime := time.Date(2026, 11, 4, 12, 7, 30, 0, time.UTC)
	start := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC).UnixMilli()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v5/market/time" {
			t.Fatal("GetKlinesAt must not request exchange time again")
		}
		if r.URL.Path != "/v5/market/kline" {
			t.Fatalf("unexpected endpoint %s", r.URL.Path)
		}
		writeResult(w, map[string]any{"list": [][]string{
			{strconv.FormatInt(start, 10), "100", "102", "99", "101", "10", "1000"},
		}})
	}))
	defer server.Close()

	client, err := NewClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	candles, err := client.GetKlinesAt(context.Background(), "BTCUSDT", "5", 100, serverTime)
	if err != nil {
		t.Fatalf("GetKlinesAt() error = %v", err)
	}
	if len(candles) != 1 || candles[0].ClosePrice != 101 {
		t.Fatalf("unexpected candles: %+v", candles)
	}
	if _, err := client.GetKlinesAt(context.Background(), "BTCUSDT", "5", 100, time.Time{}); err == nil {
		t.Fatal("empty exchange time must be rejected")
	}
}
