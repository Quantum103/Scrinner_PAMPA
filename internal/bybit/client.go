package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const maxResponseBytes = 8 << 20

type Client struct {
	baseURL    string
	httpClient *http.Client
}

type Ticker struct {
	Symbol      string `json:"symbol"`
	LastPrice   string `json:"lastPrice"`
	Bid1Price   string `json:"bid1Price"`
	Ask1Price   string `json:"ask1Price"`
	Volume24h   string `json:"volume24h"`
	Turnover24h string `json:"turnover24h"`
}

type tickerResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		List []Ticker `json:"list"`
	} `json:"result"`
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// FetchUSDTPerpetualTickers reads public linear tickers and keeps USDT symbols.
func (c *Client) FetchUSDTPerpetualTickers(ctx context.Context) ([]Ticker, error) {
	endpoint, err := url.Parse(c.baseURL + "/v5/market/tickers")
	if err != nil {
		return nil, fmt.Errorf("build Bybit ticker URL: %w", err)
	}
	query := endpoint.Query()
	query.Set("category", "linear")
	endpoint.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create Bybit ticker request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request Bybit public tickers: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Bybit public ticker endpoint returned HTTP %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("read Bybit ticker response: %w", err)
	}
	var payload tickerResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode Bybit ticker response: %w", err)
	}
	if payload.RetCode != 0 {
		return nil, fmt.Errorf("Bybit public ticker API error %d: %s", payload.RetCode, payload.RetMsg)
	}

	tickers := make([]Ticker, 0, len(payload.Result.List))
	for _, ticker := range payload.Result.List {
		if strings.HasSuffix(ticker.Symbol, "USDT") {
			tickers = append(tickers, ticker)
		}
	}
	sort.Slice(tickers, func(i, j int) bool {
		return tickers[i].Symbol < tickers[j].Symbol
	})
	return tickers, nil
}
