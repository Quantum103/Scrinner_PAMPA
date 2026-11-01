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
		if r.URL.Path != "/v5/market/tickers" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if got := r.URL.Query().Get("category"); got != "linear" {
			t.Errorf("category = %q", got)
		}
		fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"list":[`+
			`{"symbol":"ETHUSDT","lastPrice":"2000"},`+
			`{"symbol":"BTCUSDC","lastPrice":"60000"},`+
			`{"symbol":"BTCUSDT","lastPrice":"60000"}]}}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	tickers, err := client.FetchUSDTPerpetualTickers(context.Background())
	if err != nil {
		t.Fatalf("FetchUSDTPerpetualTickers() error = %v", err)
	}
	if len(tickers) != 2 {
		t.Fatalf("ticker count = %d, want 2", len(tickers))
	}
	if tickers[0].Symbol != "BTCUSDT" || tickers[1].Symbol != "ETHUSDT" {
		t.Errorf("symbols = [%s, %s]", tickers[0].Symbol, tickers[1].Symbol)
	}
}

func TestFetchUSDTPerpetualTickersHandlesAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"retCode":10001,"retMsg":"invalid request","result":{"list":[]}}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	if _, err := client.FetchUSDTPerpetualTickers(context.Background()); err == nil {
		t.Fatal("FetchUSDTPerpetualTickers() error = nil, want API error")
	}
}
