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

const (
	maxResponseBytes    = 8 << 20
	instrumentPageLimit = 1000
)

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

type instrument struct {
	Symbol       string `json:"symbol"`
	ContractType string `json:"contractType"`
	Status       string `json:"status"`
	QuoteCoin    string `json:"quoteCoin"`
}

type apiResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
}

type instrumentsResponse struct {
	apiResponse
	Result struct {
		NextPageCursor string       `json:"nextPageCursor"`
		List           []instrument `json:"list"`
	} `json:"result"`
}

type tickerResponse struct {
	apiResponse
	Result struct {
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

// FetchUSDTPerpetualTickers returns live tickers for active USDT-settled perpetual contracts.
func (c *Client) FetchUSDTPerpetualTickers(ctx context.Context) ([]Ticker, error) {
	symbols, err := c.fetchUSDTPerpetualSymbols(ctx)
	if err != nil {
		return nil, err
	}

	var payload tickerResponse
	if err := c.get(ctx, "/v5/market/tickers", url.Values{"category": {"linear"}}, &payload); err != nil {
		return nil, err
	}
	if payload.RetCode != 0 {
		return nil, fmt.Errorf("Bybit public ticker API error %d: %s", payload.RetCode, payload.RetMsg)
	}

	tickers := make([]Ticker, 0, len(symbols))
	for _, ticker := range payload.Result.List {
		if _, ok := symbols[ticker.Symbol]; ok {
			tickers = append(tickers, ticker)
		}
	}
	sort.Slice(tickers, func(i, j int) bool {
		return tickers[i].Symbol < tickers[j].Symbol
	})
	return tickers, nil
}

func (c *Client) fetchUSDTPerpetualSymbols(ctx context.Context) (map[string]struct{}, error) {
	query := url.Values{
		"category": {"linear"},
		"limit":    {fmt.Sprint(instrumentPageLimit)},
	}
	symbols := make(map[string]struct{})
	seenCursors := make(map[string]struct{})

	for page := 0; ; page++ {
		if page >= 20 {
			return nil, fmt.Errorf("Bybit instruments-info pagination exceeded 20 pages")
		}
		var payload instrumentsResponse
		if err := c.get(ctx, "/v5/market/instruments-info", query, &payload); err != nil {
			return nil, err
		}
		if payload.RetCode != 0 {
			return nil, fmt.Errorf("Bybit instruments-info API error %d: %s", payload.RetCode, payload.RetMsg)
		}
		for _, item := range payload.Result.List {
			if item.ContractType == "LinearPerpetual" && item.QuoteCoin == "USDT" && item.Status == "Trading" {
				symbols[item.Symbol] = struct{}{}
			}
		}

		cursor := payload.Result.NextPageCursor
		if cursor == "" {
			break
		}
		if _, exists := seenCursors[cursor]; exists {
			return nil, fmt.Errorf("Bybit instruments-info returned a repeated pagination cursor")
		}
		seenCursors[cursor] = struct{}{}
		query.Set("cursor", cursor)
	}
	return symbols, nil
}

func (c *Client) get(ctx context.Context, path string, query url.Values, target any) error {
	endpoint, err := url.Parse(c.baseURL + path)
	if err != nil {
		return fmt.Errorf("build Bybit request URL: %w", err)
	}
	endpoint.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("create Bybit public request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request Bybit public API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Bybit public API returned HTTP %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("read Bybit public API response: %w", err)
	}
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("decode Bybit public API response: %w", err)
	}
	return nil
}
