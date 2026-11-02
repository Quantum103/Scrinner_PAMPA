package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxResponseBytes       = 8 << 20
	instrumentPageLimit    = 1000
	maxRetries             = 4
	initialRetryDelay      = 500 * time.Millisecond
	maxLocalRetryDelay     = 8 * time.Second
	minRequestInterval     = 200 * time.Millisecond
	requestTimeout         = 10 * time.Second
	bybitTooManyVisitsCode = 10006
)

type Client struct {
	baseURL    string
	httpClient *http.Client

	rateMu        sync.Mutex
	nextRequestAt time.Time
}

type Instrument struct {
	Symbol           string
	ContractType     string
	Status           string
	BaseCoin         string
	QuoteCoin        string
	SettleCoin       string
	MinOrderQty      string
	QtyStep          string
	MinNotionalValue string
	TickSize         string
	MaxMktOrderQty   string
	FundingInterval  int
}

type Ticker struct {
	Symbol          string
	LastPrice       string
	MarkPrice       string
	BestBidPrice    string
	BestAskPrice    string
	Volume24h       string
	Turnover24h     string
	Price24hPcnt    string
	FundingRate     string
	NextFundingTime time.Time
}

type MarketData struct {
	Instrument Instrument
	Ticker     Ticker
}

type apiResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
}

type apiErrorProvider interface {
	apiError() (int, string)
}

func (r apiResponse) apiError() (int, string) {
	return r.RetCode, r.RetMsg
}

type instrumentsResponse struct {
	apiResponse
	Result struct {
		NextPageCursor string       `json:"nextPageCursor"`
		List           []instrument `json:"list"`
	} `json:"result"`
}

type instrument struct {
	Symbol       string `json:"symbol"`
	ContractType string `json:"contractType"`
	Status       string `json:"status"`
	BaseCoin     string `json:"baseCoin"`
	QuoteCoin    string `json:"quoteCoin"`
	SettleCoin   string `json:"settleCoin"`
	PriceFilter  struct {
		TickSize string `json:"tickSize"`
	} `json:"priceFilter"`
	LotSizeFilter struct {
		MinOrderQty      string `json:"minOrderQty"`
		QtyStep          string `json:"qtyStep"`
		MinNotionalValue string `json:"minNotionalValue"`
		MaxMktOrderQty   string `json:"maxMktOrderQty"`
	} `json:"lotSizeFilter"`
	FundingInterval int `json:"fundingInterval"`
}

type tickerResponse struct {
	apiResponse
	Result struct {
		List []rawTicker `json:"list"`
	} `json:"result"`
}

type rawTicker struct {
	Symbol          string `json:"symbol"`
	LastPrice       string `json:"lastPrice"`
	MarkPrice       string `json:"markPrice"`
	Bid1Price       string `json:"bid1Price"`
	Ask1Price       string `json:"ask1Price"`
	Volume24h       string `json:"volume24h"`
	Turnover24h     string `json:"turnover24h"`
	Price24hPcnt    string `json:"price24hPcnt"`
	FundingRate     string `json:"fundingRate"`
	NextFundingTime string `json:"nextFundingTime"`
}

type apiError struct {
	Code    int
	Message string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("ошибка API Bybit %d: %s", e.Code, e.Message)
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: requestTimeout,
		},
	}
}

// FetchUSDTPerpetualInstruments получает все активные USDT-линейные бессрочные контракты и их спецификации.
func (c *Client) FetchUSDTPerpetualInstruments(ctx context.Context) ([]Instrument, error) {
	query := url.Values{
		"category": {"linear"},
		"status":   {"Trading"},
		"limit":    {strconv.Itoa(instrumentPageLimit)},
	}
	seenCursors := make(map[string]struct{})
	instruments := make([]Instrument, 0)

	for {
		var payload instrumentsResponse
		if err := c.get(ctx, "/v5/market/instruments-info", query, &payload); err != nil {
			return nil, fmt.Errorf("получить спецификации контрактов: %w", err)
		}

		for _, item := range payload.Result.List {
			if item.ContractType != "LinearPerpetual" ||
				item.Status != "Trading" ||
				item.QuoteCoin != "USDT" ||
				item.SettleCoin != "USDT" {
				continue
			}

			instruments = append(instruments, Instrument{
				Symbol:           item.Symbol,
				ContractType:     item.ContractType,
				Status:           item.Status,
				BaseCoin:         item.BaseCoin,
				QuoteCoin:        item.QuoteCoin,
				SettleCoin:       item.SettleCoin,
				MinOrderQty:      item.LotSizeFilter.MinOrderQty,
				QtyStep:          item.LotSizeFilter.QtyStep,
				MinNotionalValue: item.LotSizeFilter.MinNotionalValue,
				TickSize:         item.PriceFilter.TickSize,
				MaxMktOrderQty:   item.LotSizeFilter.MaxMktOrderQty,
				FundingInterval:  item.FundingInterval,
			})
		}

		cursor := payload.Result.NextPageCursor
		if cursor == "" {
			break
		}
		if _, exists := seenCursors[cursor]; exists {
			return nil, fmt.Errorf("Bybit вернула повторяющийся nextPageCursor: %q", cursor)
		}
		seenCursors[cursor] = struct{}{}
		query.Set("cursor", cursor)
	}

	return instruments, nil
}

// FetchLinearTickers получает публичные рыночные тикеры линейного рынка Bybit.
func (c *Client) FetchLinearTickers(ctx context.Context) ([]Ticker, error) {
	query := url.Values{"category": {"linear"}}
	var payload tickerResponse
	if err := c.get(ctx, "/v5/market/tickers", query, &payload); err != nil {
		return nil, fmt.Errorf("получить рыночные тикеры: %w", err)
	}

	tickers := make([]Ticker, 0, len(payload.Result.List))
	for _, item := range payload.Result.List {
		nextFundingTime := time.Time{}
		if item.NextFundingTime != "" && item.NextFundingTime != "0" {
			timestamp, err := strconv.ParseInt(item.NextFundingTime, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("тикер %s: некорректный nextFundingTime %q: %w", item.Symbol, item.NextFundingTime, err)
			}
			nextFundingTime = time.UnixMilli(timestamp).UTC()
		}

		tickers = append(tickers, Ticker{
			Symbol:          item.Symbol,
			LastPrice:       item.LastPrice,
			MarkPrice:       item.MarkPrice,
			BestBidPrice:    item.Bid1Price,
			BestAskPrice:    item.Ask1Price,
			Volume24h:       item.Volume24h,
			Turnover24h:     item.Turnover24h,
			Price24hPcnt:    item.Price24hPcnt,
			FundingRate:     item.FundingRate,
			NextFundingTime: nextFundingTime,
		})
	}
	return tickers, nil
}

// FetchMarketData объединяет тикеры с заранее полученными спецификациями контрактов по символу.
func (c *Client) FetchMarketData(ctx context.Context, instruments []Instrument) ([]MarketData, error) {
	tickers, err := c.FetchLinearTickers(ctx)
	if err != nil {
		return nil, err
	}

	tickersBySymbol := make(map[string]Ticker, len(tickers))
	for _, ticker := range tickers {
		tickersBySymbol[ticker.Symbol] = ticker
	}

	result := make([]MarketData, 0, len(instruments))
	for _, item := range instruments {
		ticker, exists := tickersBySymbol[item.Symbol]
		if !exists {
			continue
		}
		result = append(result, MarketData{
			Instrument: item,
			Ticker:     ticker,
		})
	}
	return result, nil
}

func (c *Client) get(ctx context.Context, path string, query url.Values, target any) error {
	endpoint, err := url.Parse(c.baseURL + path)
	if err != nil {
		return fmt.Errorf("сформировать URL Bybit: %w", err)
	}
	endpoint.RawQuery = query.Encode()

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := c.waitForRequest(ctx); err != nil {
			return err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return fmt.Errorf("создать публичный запрос Bybit: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "volume-trader/1.0")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if attempt == maxRetries {
				return fmt.Errorf("запрос Bybit: %w", err)
			}
			if err := waitContext(ctx, retryDelay(nil, attempt)); err != nil {
				return err
			}
			continue
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
		headers := resp.Header.Clone()
		statusCode := resp.StatusCode
		resp.Body.Close()

		if readErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if attempt == maxRetries {
				return fmt.Errorf("прочитать ответ Bybit: %w", readErr)
			}
			if err := waitContext(ctx, retryDelay(headers, attempt)); err != nil {
				return err
			}
			continue
		}
		if len(body) > maxResponseBytes {
			return fmt.Errorf("ответ Bybit превышает допустимый размер %d байт", maxResponseBytes)
		}

		if statusCode != http.StatusOK {
			requestErr := formatHTTPStatusError(statusCode, body, headers)
			if !isRetryableStatus(statusCode) || attempt == maxRetries {
				return requestErr
			}
			if err := waitContext(ctx, retryDelay(headers, attempt)); err != nil {
				return err
			}
			continue
		}

		if err := json.Unmarshal(body, target); err != nil {
			return fmt.Errorf("разобрать JSON-ответ Bybit: %w", err)
		}
		if provider, ok := target.(apiErrorProvider); ok {
			code, message := provider.apiError()
			if code != 0 {
				requestErr := &apiError{Code: code, Message: message}
				if !isRetryableAPIError(code) || attempt == maxRetries {
					return requestErr
				}
				if err := waitContext(ctx, retryDelay(headers, attempt)); err != nil {
					return err
				}
				continue
			}
		}
		return nil
	}

	return fmt.Errorf("Bybit: превышено количество повторных попыток")
}

func formatHTTPStatusError(statusCode int, body []byte, headers http.Header) error {
	detail := strings.TrimSpace(string(body))
	var response struct {
		RetCode int    `json:"retCode"`
		RetMsg  string `json:"retMsg"`
	}
	if json.Unmarshal(body, &response) == nil && (response.RetCode != 0 || response.RetMsg != "") {
		detail = fmt.Sprintf("retCode=%d retMsg=%s", response.RetCode, response.RetMsg)
	}
	detail = strings.Join(strings.Fields(detail), " ")
	if len(detail) > 512 {
		detail = detail[:512] + "..."
	}

	message := fmt.Sprintf("Bybit вернула HTTP %d: %s", statusCode, http.StatusText(statusCode))
	if detail != "" {
		message += "; ответ сервера: " + detail
	}
	for _, name := range []string{
		"CF-Ray",
		"X-Bapi-Limit",
		"X-Bapi-Limit-Status",
		"X-Bapi-Limit-Reset-Timestamp",
	} {
		if value := strings.TrimSpace(headers.Get(name)); value != "" {
			message += fmt.Sprintf("; %s=%s", strings.ToLower(name), value)
		}
	}
	return fmt.Errorf("%s", message)
}

func (c *Client) waitForRequest(ctx context.Context) error {
	c.rateMu.Lock()
	now := time.Now()
	delay := time.Until(c.nextRequestAt)
	if delay < 0 {
		delay = 0
	}
	c.nextRequestAt = now.Add(delay).Add(minRequestInterval)
	c.rateMu.Unlock()

	return waitContext(ctx, delay)
}

func retryDelay(headers http.Header, attempt int) time.Duration {
	delay := initialRetryDelay << attempt
	if delay > maxLocalRetryDelay {
		delay = maxLocalRetryDelay
	}

	if headers == nil {
		return delay
	}
	if value := headers.Get("Retry-After"); value != "" {
		if seconds, err := strconv.ParseFloat(value, 64); err == nil {
			serverDelay := time.Duration(seconds * float64(time.Second))
			if serverDelay > delay {
				delay = serverDelay
			}
		} else if retryAt, err := http.ParseTime(value); err == nil {
			if serverDelay := time.Until(retryAt); serverDelay > delay {
				delay = serverDelay
			}
		}
	}

	if value := headers.Get("X-Bapi-Limit-Reset-Timestamp"); value != "" {
		if timestamp, err := strconv.ParseInt(value, 10, 64); err == nil {
			if serverDelay := time.Until(time.UnixMilli(timestamp)); serverDelay > delay {
				delay = serverDelay
			}
		}
	}
	return delay
}

func isRetryableStatus(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || statusCode >= 500
}

func isRetryableAPIError(code int) bool {
	switch code {
	case bybitTooManyVisitsCode, 10000, 10016, 429:
		return true
	default:
		return false
	}
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
