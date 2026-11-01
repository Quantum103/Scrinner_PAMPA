package bybit

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchUSDTPerpetualTickers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v5/market/instruments-info":
			if r.URL.Query().Get("category") != "linear" || r.URL.Query().Get("limit") != "1000" {
				t.Errorf("unexpected instruments query: %s", r.URL.RawQuery)
			}
			fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"list":[`+
				`{"symbol":"BTCUSDT","contractType":"LinearPerpetual","status":"Trading","quoteCoin":"USDT"},`+
				`{"symbol":"BTCUSDC","contractType":"LinearPerpetual","status":"Trading","quoteCoin":"USDC"},`+
				`{"symbol":"ETHUSDT","contractType":"LinearFutures","status":"Trading","quoteCoin":"USDT"},`+
				`{"symbol":"XRPUSDT","contractType":"LinearPerpetual","status":"PreLaunch","quoteCoin":"USDT"}]}}`)
		case "/v5/market/tickers":
			if r.URL.Query().Get("category") != "linear" {
				t.Errorf("category = %q", r.URL.Query().Get("category"))
			}
			fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"list":[`+
				`{"symbol":"ETHUSDT","lastPrice":"2000"},`+
				`{"symbol":"BTCUSDC","lastPrice":"60000"},`+
				`{"symbol":"BTCUSDT","lastPrice":"60000"},`+
				`{"symbol":"XRPUSDT","lastPrice":"1"}]}}`)
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	tickers, err := client.FetchUSDTPerpetualTickers(context.Background())
	if err != nil {
		t.Fatalf("FetchUSDTPerpetualTickers() error = %v", err)
	}
	if len(tickers) != 1 {
		t.Fatalf("ticker count = %d, want 1", len(tickers))
	}
	if tickers[0].Symbol != "BTCUSDT" {
		t.Errorf("symbol = %q, want BTCUSDT", tickers[0].Symbol)
	}
}

func TestFetchUSDTPerpetualTickersHandlesAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v5/market/instruments-info" {
			fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"list":[{"symbol":"BTCUSDT","contractType":"LinearPerpetual","status":"Trading","quoteCoin":"USDT"}]}}`)
			return
		}
		fmt.Fprint(w, `{"retCode":10001,"retMsg":"invalid request","result":{"list":[]}}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	if _, err := client.FetchUSDTPerpetualTickers(context.Background()); err == nil {
		t.Fatal("FetchUSDTPerpetualTickers() error = nil, want API error")
	}
}

func TestFetchUSDTPerpetualTickersRejectsRepeatedCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":"same","list":[]}}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	if _, err := client.FetchUSDTPerpetualTickers(context.Background()); err == nil {
		t.Fatal("FetchUSDTPerpetualTickers() error = nil, want pagination error")
	}
}
