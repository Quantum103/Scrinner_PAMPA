package bybit

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFetchUSDTPerpetualInstrumentsPaginatesAndFilters(t *testing.T) {
	var pageTwoCalled atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v5/market/instruments-info" {
			t.Errorf("неожиданный путь: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		query := r.URL.Query()
		if query.Get("category") != "linear" || query.Get("status") != "Trading" || query.Get("limit") != "1000" {
			t.Errorf("неверные параметры запроса инструментов: %s", r.URL.RawQuery)
		}

		switch query.Get("cursor") {
		case "":
			fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":"next-page","list":[`+
				`{"symbol":"BTCUSDT","contractType":"LinearPerpetual","status":"Trading","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","priceFilter":{"tickSize":"0.1"},"lotSizeFilter":{"minOrderQty":"0.001","qtyStep":"0.001","minNotionalValue":"5","maxMktOrderQty":"100"},"fundingInterval":480},`+
				`{"symbol":"BTCUSDC","contractType":"LinearPerpetual","status":"Trading","quoteCoin":"USDC","settleCoin":"USDC"},`+
				`{"symbol":"ETHUSDT","contractType":"LinearFutures","status":"Trading","quoteCoin":"USDT","settleCoin":"USDT"},`+
				`{"symbol":"XRPUSDT","contractType":"LinearPerpetual","status":"PreLaunch","quoteCoin":"USDT","settleCoin":"USDT"},`+
				`{"symbol":"ADAUSDT","contractType":"LinearPerpetual","status":"Trading","quoteCoin":"USDT","settleCoin":"USDC"}]}}`)
		case "next-page":
			pageTwoCalled.Store(true)
			fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":"","list":[`+
				`{"symbol":"ETHUSDT","contractType":"LinearPerpetual","status":"Trading","baseCoin":"ETH","quoteCoin":"USDT","settleCoin":"USDT","priceFilter":{"tickSize":"0.01"},"lotSizeFilter":{"minOrderQty":"0.01","qtyStep":"0.01","minNotionalValue":"10","maxMktOrderQty":"500"},"fundingInterval":240}]}}`)
		default:
			t.Errorf("неожиданный cursor: %q", query.Get("cursor"))
			http.Error(w, "неверный cursor", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	instruments, err := client.FetchUSDTPerpetualInstruments(context.Background())
	if err != nil {
		t.Fatalf("FetchUSDTPerpetualInstruments() вернула ошибку: %v", err)
	}
	if !pageTwoCalled.Load() {
		t.Fatal("вторая страница не была запрошена")
	}
	if len(instruments) != 2 {
		t.Fatalf("получено %d контрактов, ожидалось 2", len(instruments))
	}
	if instruments[0].Symbol != "BTCUSDT" || instruments[1].Symbol != "ETHUSDT" {
		t.Fatalf("неверный список контрактов: %+v", instruments)
	}
	if instruments[0].MinOrderQty != "0.001" || instruments[0].QtyStep != "0.001" ||
		instruments[0].MinNotionalValue != "5" || instruments[0].TickSize != "0.1" ||
		instruments[0].MaxMktOrderQty != "100" || instruments[0].FundingInterval != 480 {
		t.Fatalf("спецификации BTCUSDT не перенесены из ответа: %+v", instruments[0])
	}
	if instruments[1].MinOrderQty != "0.01" || instruments[1].QtyStep != "0.01" ||
		instruments[1].MinNotionalValue != "10" || instruments[1].MaxMktOrderQty != "500" {
		t.Fatalf("спецификации ETHUSDT не перенесены из ответа: %+v", instruments[1])
	}
}

func TestFetchUSDTPerpetualInstrumentsLoadsMoreThan500Contracts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		start, end := 0, 500
		cursor := query.Get("cursor")
		if cursor == "page-2" {
			start, end = 500, 600
		} else if cursor != "" {
			t.Errorf("неожиданный cursor: %q", cursor)
			http.Error(w, "неверный cursor", http.StatusBadRequest)
			return
		}

		w.Write([]byte(`{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":"`))
		if end == 500 {
			w.Write([]byte(`page-2`))
		}
		w.Write([]byte(`","list":[`))
		for i := start; i < end; i++ {
			if i > start {
				w.Write([]byte(","))
			}
			fmt.Fprintf(w, `{"symbol":"COIN%03dUSDT","contractType":"LinearPerpetual","status":"Trading","quoteCoin":"USDT","settleCoin":"USDT"}`, i)
		}
		w.Write([]byte(`]}}`))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	instruments, err := client.FetchUSDTPerpetualInstruments(context.Background())
	if err != nil {
		t.Fatalf("FetchUSDTPerpetualInstruments() вернула ошибку: %v", err)
	}
	if len(instruments) != 600 {
		t.Fatalf("получено %d контрактов, ожидалось 600", len(instruments))
	}
	if instruments[599].Symbol != "COIN599USDT" {
		t.Errorf("последний контракт = %q, ожидался COIN599USDT", instruments[599].Symbol)
	}
}

func TestFetchLinearTickersParsesMarketFields(t *testing.T) {
	const nextFundingMillis int64 = 1793510400000
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v5/market/tickers" || r.URL.Query().Get("category") != "linear" {
			t.Errorf("неверный запрос тикеров: %s %s", r.Method, r.URL.String())
		}
		fmt.Fprintf(w, `{"retCode":0,"retMsg":"OK","result":{"list":[{"symbol":"BTCUSDT","lastPrice":"68000.1","markPrice":"68000.15","bid1Price":"68000.0","bid1Size":"12.5","ask1Price":"68000.2","ask1Size":"8.75","volume24h":"12345.6","turnover24h":"839000000","price24hPcnt":"0.0125","openInterest":"7654321","openInterestValue":"520000000","fundingRate":"0.0001","nextFundingTime":"%d"}]}}`, nextFundingMillis)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	tickers, err := client.FetchLinearTickers(context.Background())
	if err != nil {
		t.Fatalf("FetchLinearTickers() вернула ошибку: %v", err)
	}
	if len(tickers) != 1 {
		t.Fatalf("получено %d тикеров, ожидался 1", len(tickers))
	}

	ticker := tickers[0]
	if ticker.LastPrice != "68000.1" || ticker.MarkPrice != "68000.15" ||
		ticker.Bid1Price != "68000.0" || ticker.Bid1Size != "12.5" ||
		ticker.Ask1Price != "68000.2" || ticker.Ask1Size != "8.75" ||
		ticker.BestBidPrice != "68000.0" || ticker.BestAskPrice != "68000.2" {
		t.Errorf("цены или размеры best bid/ask не перенесены: %+v", ticker)
	}
	if ticker.Volume24h != "12345.6" || ticker.Turnover24h != "839000000" {
		t.Errorf("объём и оборот не перенесены: %+v", ticker)
	}
	if ticker.Price24hPcnt != "0.0125" || ticker.OpenInterest != "7654321" ||
		ticker.OpenInterestValue != "520000000" || ticker.FundingRate != "0.0001" {
		t.Errorf("изменение цены, Open Interest или funding не перенесены: %+v", ticker)
	}
	if !ticker.NextFundingTime.Equal(time.UnixMilli(nextFundingMillis).UTC()) {
		t.Errorf("NextFundingTime = %s, ожидалось %s", ticker.NextFundingTime, time.UnixMilli(nextFundingMillis).UTC())
	}
}

func TestFetchMarketDataJoinsInstrumentSpecificationsAndTicker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"list":[{"symbol":"BTCUSDT","lastPrice":"100","markPrice":"100.05","bid1Price":"99","ask1Price":"101","volume24h":"10","turnover24h":"1000","price24hPcnt":"0.01","fundingRate":"0.0002","nextFundingTime":"1793510400000"},{"symbol":"OTHERUSDT","lastPrice":"50"}]}}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	instruments := []Instrument{{Symbol: "BTCUSDT", MinOrderQty: "0.001", QtyStep: "0.001", TickSize: "0.1"}}
	marketData, err := client.FetchMarketData(context.Background(), instruments)
	if err != nil {
		t.Fatalf("FetchMarketData() вернула ошибку: %v", err)
	}
	if len(marketData) != 1 {
		t.Fatalf("получено %d записей, ожидалась 1", len(marketData))
	}
	if marketData[0].Instrument.MinOrderQty != "0.001" || marketData[0].Ticker.LastPrice != "100" || marketData[0].Ticker.MarkPrice != "100.05" {
		t.Fatalf("рыночные данные и спецификация не объединены: %+v", marketData[0])
	}
}

func TestFetchUSDTPerpetualInstrumentsRejectsRepeatedCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":"same","list":[]}}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	if _, err := client.FetchUSDTPerpetualInstruments(context.Background()); err == nil {
		t.Fatal("ожидалась ошибка при повторяющемся nextPageCursor")
	}
}

func TestGetRetriesRateLimitResponse(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			fmt.Fprint(w, `{"retCode":10006,"retMsg":"Too many visits!","result":{"list":[]}}`)
			return
		}
		fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"list":[]}}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	if _, err := client.FetchLinearTickers(context.Background()); err != nil {
		t.Fatalf("запрос после временного ограничения завершился ошибкой: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("выполнено %d запросов, ожидалось 2", got)
	}
}

func TestGetDoesNotRetryPermanentAPIError(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		fmt.Fprint(w, `{"retCode":10001,"retMsg":"invalid request","result":{"list":[]}}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	if _, err := client.FetchLinearTickers(context.Background()); err == nil {
		t.Fatal("ожидалась ошибка API")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("выполнено %d запросов, ожидался 1 без повторов", got)
	}
}

func TestGetRetriesTemporaryBybitServerError(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			fmt.Fprint(w, `{"retCode":10016,"retMsg":"Server error","result":{"list":[]}}`)
			return
		}
		fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"list":[]}}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	if _, err := client.FetchLinearTickers(context.Background()); err != nil {
		t.Fatalf("запрос после временной ошибки Bybit завершился ошибкой: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("выполнено %d запросов, ожидалось 2", got)
	}
}

func TestGetRetriesHTTPTooManyRequests(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"list":[]}}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	if _, err := client.FetchLinearTickers(context.Background()); err != nil {
		t.Fatalf("запрос после HTTP 429 завершился ошибкой: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("выполнено %d запросов, ожидалось 2", got)
	}
}


func TestForbiddenResponseIncludesServerDetailsWithoutRetry(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("CF-Ray", "test-ray-id")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"retCode":10009,"retMsg":"Service Restricted from this region"}`)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	var response apiResponse
	err := client.get(context.Background(), "/v5/market/time", nil, &response)
	if err == nil {
		t.Fatal("ожидалась ошибка HTTP 403")
	}
	for _, expected := range []string{
		"HTTP 403",
		"retCode=10009",
		"Service Restricted from this region",
		"cf-ray=test-ray-id",
	} {
		if !strings.Contains(err.Error(), expected) {
			t.Errorf("ошибка %q не содержит %q", err, expected)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("число запросов = %d, ожидался один запрос без повтора", got)
	}
}


func TestJoinMarketDataDoesNotTruncateLargeInstrumentLists(t *testing.T) {
	instruments := make([]Instrument, 650)
	tickers := make([]Ticker, 650)
	for i := 0; i < 650; i++ {
		symbol := fmt.Sprintf("COIN%03dUSDT", i)
		instruments[i] = Instrument{Symbol: symbol, MinOrderQty: "1"}
		tickers[i] = Ticker{Symbol: symbol, LastPrice: fmt.Sprintf("%d", i+1)}
	}
	joined := JoinMarketData(instruments, tickers)
	if len(joined) != 650 { t.Fatalf("объединено %d инструментов, ожидалось 650", len(joined)) }
	if joined[0].Instrument.Symbol != "COIN000USDT" || joined[649].Instrument.Symbol != "COIN649USDT" {
		t.Fatalf("порядок или конец списка потерян: first=%s last=%s", joined[0].Instrument.Symbol, joined[649].Instrument.Symbol)
	}
	if joined[649].Instrument.MinOrderQty != "1" || joined[649].Ticker.LastPrice != "650" {
		t.Fatalf("спецификация или тикер последнего инструмента потеряны: %+v", joined[649])
	}
}

func TestJoinMarketDataSkipsOnlySymbolsWithoutTicker(t *testing.T) {
	joined := JoinMarketData(
		[]Instrument{{Symbol: "AAAUSDT"}, {Symbol: "BBBUSDT"}},
		[]Ticker{{Symbol: "BBBUSDT", LastPrice: "2"}},
	)
	if len(joined) != 1 || joined[0].Instrument.Symbol != "BBBUSDT" {
		t.Fatalf("неожиданный результат объединения: %+v", joined)
	}
}


func TestFetchMinuteKlinesSortsResponseChronologically(t *testing.T) {
	oldest := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC).UnixMilli()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if r.URL.Path != "/v5/market/kline" || query.Get("category") != "linear" ||
			query.Get("symbol") != "AAAUSDT" || query.Get("interval") != "1" || query.Get("limit") != "21" {
			t.Errorf("неверный запрос Kline: %s", r.URL.String())
		}
		fmt.Fprintf(w, `{"retCode":0,"retMsg":"OK","result":{"list":[
			["%d","11","13","10","12","2.5","30"],
			["%d","10","12","9","11","1.5","15"]
		]}}`, oldest+60_000, oldest)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	client.httpClient = server.Client()
	candles, err := client.FetchMinuteKlines(context.Background(), "aaausdt", 21)
	if err != nil { t.Fatalf("FetchMinuteKlines() error = %v", err) }
	if len(candles) != 2 { t.Fatalf("получено %d свечей, ожидалось 2", len(candles)) }
	if !candles[0].StartTime.Equal(time.UnixMilli(oldest).UTC()) ||
		!candles[1].StartTime.Equal(time.UnixMilli(oldest+60_000).UTC()) {
		t.Fatalf("свечи не отсортированы: %+v", candles)
	}
	if candles[0].Volume != 1.5 || candles[0].Turnover != 15 || candles[1].Turnover != 30 {
		t.Fatalf("volume/turnover прочитаны неверно: %+v", candles)
	}
}

func TestFetchMinuteKlinesRejectsInvalidLimitAndRows(t *testing.T) {
	client := NewClient("https://example.test")
	if _, err := client.FetchMinuteKlines(context.Background(), "AAAUSDT", 20); err == nil {
		t.Fatal("ожидалась ошибка при запросе менее 21 свечи")
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"retCode":0,"retMsg":"OK","result":{"list":[["1","bad","2","1","2","1","2"]]}}`)
	}))
	defer server.Close()
	client = NewClient(server.URL)
	client.httpClient = server.Client()
	if _, err := client.FetchMinuteKlines(context.Background(), "AAAUSDT", 21); err == nil {
		t.Fatal("ожидалась ошибка для некорректной свечи")
	}
}
