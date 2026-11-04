package bybit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxResponseBytes = 8 << 20
	maxAttempts      = 3
	requestInterval  = 100 * time.Millisecond
)

// Client reads public market data only. It has no private or trading endpoints.
type Client struct {
	baseURL     string
	httpClient  *http.Client
	rateMu      sync.Mutex
	nextRequest time.Time
}

func NewClient(baseURL string, httpClient *http.Client) (*Client, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return nil, fmt.Errorf("parse Bybit REST URL: %w", err)
	}
	if (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("Bybit REST URL must be an absolute HTTP(S) URL without query or fragment")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), httpClient: httpClient}, nil
}

type APIError struct {
	HTTPStatus int
	Code       int64
	Message    string
}

func (e *APIError) Error() string {
	if e.HTTPStatus != 0 {
		return fmt.Sprintf("Bybit REST HTTP %d: %s", e.HTTPStatus, e.Message)
	}
	return fmt.Sprintf("Bybit REST error %d: %s", e.Code, e.Message)
}

func (e *APIError) temporary() bool {
	return e.HTTPStatus == http.StatusTooManyRequests || e.HTTPStatus >= 500 ||
		e.Code == 10000 || e.Code == 10006 || e.Code == 10016
}

type Instrument struct {
	Symbol       string
	BaseCoin     string
	QuoteCoin    string
	SettleCoin   string
	ContractType string
	Status       string
}

type Ticker struct {
	Symbol      string
	LastPrice   string
	Turnover24h string
}

type Candle struct {
	Symbol     string
	Timeframe  string
	Start      time.Time
	Close      time.Time
	Open       float64
	High       float64
	Low        float64
	ClosePrice float64
	Volume     float64
	Turnover   float64
}

// GetInstruments walks the complete cursor-paginated Trading catalogue and keeps
// only USDT-quoted, USDT-settled LinearPerpetual contracts.
func (c *Client) GetInstruments(ctx context.Context) ([]Instrument, error) {
	const maxPages = 100
	cursor := ""
	seenCursors := make(map[string]struct{})
	seenSymbols := make(map[string]struct{})
	result := make([]Instrument, 0, 700)

	for page := 0; page < maxPages; page++ {
		query := url.Values{
			"category": {"linear"},
			"status":   {"Trading"},
			"limit":    {"1000"},
		}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		var response struct {
			NextPageCursor string
			List           []struct {
				Symbol       string
				BaseCoin     string
				QuoteCoin    string
				SettleCoin   string
				ContractType string
				Status       string
			}
		}
		if err := c.get(ctx, "/v5/market/instruments-info", query, &response); err != nil {
			return nil, fmt.Errorf("get Bybit instrument page %d: %w", page+1, err)
		}
		for _, item := range response.List {
			item.Symbol = strings.ToUpper(strings.TrimSpace(item.Symbol))
			if item.Symbol == "" || item.Status != "Trading" ||
				item.ContractType != "LinearPerpetual" ||
				item.QuoteCoin != "USDT" || item.SettleCoin != "USDT" {
				continue
			}
			if _, exists := seenSymbols[item.Symbol]; exists {
				return nil, fmt.Errorf("Bybit returned duplicate instrument %s", item.Symbol)
			}
			seenSymbols[item.Symbol] = struct{}{}
			result = append(result, Instrument{
				Symbol: item.Symbol, BaseCoin: item.BaseCoin, QuoteCoin: item.QuoteCoin,
				SettleCoin: item.SettleCoin, ContractType: item.ContractType, Status: item.Status,
			})
		}
		next := strings.TrimSpace(response.NextPageCursor)
		if next == "" {
			if len(result) == 0 {
				return nil, errors.New("Bybit returned an empty USDT LinearPerpetual catalogue")
			}
			return result, nil
		}
		if _, exists := seenCursors[next]; exists || next == cursor {
			return nil, errors.New("Bybit instrument pagination returned a repeated cursor")
		}
		seenCursors[next] = struct{}{}
		cursor = next
	}
	return nil, fmt.Errorf("Bybit instrument pagination exceeded %d pages", maxPages)
}

func (c *Client) GetTickers(ctx context.Context) ([]Ticker, error) {
	query := url.Values{"category": {"linear"}}
	var response struct {
		List []struct {
			Symbol      string
			LastPrice   string
			Turnover24h string
		}
	}
	if err := c.get(ctx, "/v5/market/tickers", query, &response); err != nil {
		return nil, fmt.Errorf("get Bybit linear tickers: %w", err)
	}
	if len(response.List) == 0 {
		return nil, errors.New("Bybit returned an empty Linear ticker snapshot")
	}
	tickers := make([]Ticker, 0, len(response.List))
	seen := make(map[string]struct{}, len(response.List))
	for _, item := range response.List {
		symbol := strings.ToUpper(strings.TrimSpace(item.Symbol))
		if symbol == "" {
			return nil, errors.New("Bybit ticker contains an empty symbol")
		}
		if _, exists := seen[symbol]; exists {
			return nil, fmt.Errorf("Bybit returned duplicate ticker %s", symbol)
		}
		seen[symbol] = struct{}{}
		tickers = append(tickers, Ticker{Symbol: symbol, LastPrice: item.LastPrice, Turnover24h: item.Turnover24h})
	}
	return tickers, nil
}

func (c *Client) GetServerTime(ctx context.Context) (time.Time, error) {
	var response struct {
		TimeSecond string
		TimeNano   string
	}
	if err := c.get(ctx, "/v5/market/time", nil, &response); err != nil {
		return time.Time{}, fmt.Errorf("get Bybit server time: %w", err)
	}
	if response.TimeNano != "" {
		nanos, err := strconv.ParseInt(response.TimeNano, 10, 64)
		if err != nil || nanos <= 0 {
			return time.Time{}, fmt.Errorf("Bybit returned invalid nanosecond server time %q", response.TimeNano)
		}
		return time.Unix(0, nanos).UTC(), nil
	}
	seconds, err := strconv.ParseInt(response.TimeSecond, 10, 64)
	if err != nil || seconds <= 0 {
		return time.Time{}, errors.New("Bybit response does not contain a valid server timestamp")
	}
	return time.Unix(seconds, 0).UTC(), nil
}

// GetKlines returns closed candles from oldest to newest; the forming candle is excluded.
func (c *Client) GetKlines(ctx context.Context, symbol, timeframe string, limit int) ([]Candle, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return nil, errors.New("Bybit kline symbol is required")
	}
	if _, err := timeframeDuration(timeframe); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 1000 {
		return nil, errors.New("Bybit kline limit must be between 1 and 1000")
	}
	serverTime, err := c.GetServerTime(ctx)
	if err != nil {
		return nil, err
	}
	return c.GetKlinesAt(ctx, symbol, timeframe, limit, serverTime)
}

// GetKlinesAt reads candles using a shared exchange timestamp. A scanner can
// fetch server time once and reuse it for every instrument/timeframe in a cycle.
func (c *Client) GetKlinesAt(ctx context.Context, symbol, timeframe string, limit int, serverTime time.Time) ([]Candle, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return nil, errors.New("Bybit kline symbol is required")
	}
	interval, err := timeframeDuration(timeframe)
	if err != nil {
		return nil, err
	}
	if limit < 1 || limit > 1000 {
		return nil, errors.New("Bybit kline limit must be between 1 and 1000")
	}
	if serverTime.IsZero() {
		return nil, errors.New("Bybit exchange time is required")
	}
	serverTime = serverTime.UTC()
	query := url.Values{
		"category": {"linear"}, "symbol": {symbol},
		"interval": {timeframe}, "limit": {strconv.Itoa(limit)},
	}
	var response struct {
		List [][]string
	}
	if err := c.get(ctx, "/v5/market/kline", query, &response); err != nil {
		return nil, fmt.Errorf("get Bybit %s %s klines: %w", symbol, timeframe, err)
	}
	candles := make([]Candle, 0, len(response.List))
	seenStarts := make(map[int64]struct{}, len(response.List))
	for index, values := range response.List {
		if len(values) < 7 {
			return nil, fmt.Errorf("Bybit candle %d has %d fields, want at least 7", index, len(values))
		}
		startMillis, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil || startMillis <= 0 {
			return nil, fmt.Errorf("Bybit candle %d has invalid start timestamp %q", index, values[0])
		}
		if _, exists := seenStarts[startMillis]; exists {
			return nil, fmt.Errorf("Bybit returned duplicate candle at %s", time.UnixMilli(startMillis).UTC())
		}
		seenStarts[startMillis] = struct{}{}
		start := time.UnixMilli(startMillis).UTC()
		closeTime := start.Add(interval)
		if closeTime.After(serverTime) {
			continue
		}
		numbers := make([]float64, 6)
		for field := range numbers {
			numbers[field], err = strconv.ParseFloat(values[field+1], 64)
			if err != nil || math.IsNaN(numbers[field]) || math.IsInf(numbers[field], 0) {
				return nil, fmt.Errorf("Bybit candle %d has invalid numeric field %d", index, field+1)
			}
		}
		openPrice, highPrice, lowPrice, closePrice := numbers[0], numbers[1], numbers[2], numbers[3]
		volume, turnover := numbers[4], numbers[5]
		if openPrice <= 0 || highPrice <= 0 || lowPrice <= 0 || closePrice <= 0 ||
			highPrice < lowPrice || highPrice < openPrice || highPrice < closePrice ||
			lowPrice > openPrice || lowPrice > closePrice || volume < 0 || turnover < 0 {
			return nil, fmt.Errorf("Bybit candle %d contains an impossible OHLCV range", index)
		}
		candles = append(candles, Candle{
			Symbol: symbol, Timeframe: timeframe, Start: start, Close: closeTime,
			Open: openPrice, High: highPrice, Low: lowPrice, ClosePrice: closePrice,
			Volume: volume, Turnover: turnover,
		})
	}
	sort.Slice(candles, func(i, j int) bool { return candles[i].Start.Before(candles[j].Start) })
	return candles, nil
}

func timeframeDuration(timeframe string) (time.Duration, error) {
	switch timeframe {
	case "5":
		return 5 * time.Minute, nil
	case "15":
		return 15 * time.Minute, nil
	case "60":
		return time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported Bybit timeframe %q: supported values are 5, 15 and 60", timeframe)
	}
}

func (c *Client) get(ctx context.Context, path string, query url.Values, destination any) error {
	endpoint := c.baseURL + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := c.waitForRequestSlot(ctx); err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return fmt.Errorf("build Bybit request: %w", err)
		}
		request.Header.Set("Accept", "application/json")
		request.Header.Set("User-Agent", "rsi-scanner/1.0")
		response, err := c.httpClient.Do(request)
		if err != nil {
			lastErr = fmt.Errorf("send Bybit request: %w", err)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if attempt+1 < maxAttempts {
				if err := waitRetry(ctx, attempt); err != nil {
					return err
				}
				continue
			}
			return lastErr
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
		closeErr := response.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("read Bybit response: %w", readErr)
			if attempt+1 < maxAttempts {
				if err := waitRetry(ctx, attempt); err != nil {
					return err
				}
				continue
			}
			return lastErr
		}
		if closeErr != nil {
			return fmt.Errorf("close Bybit response: %w", closeErr)
		}
		if len(body) > maxResponseBytes {
			return fmt.Errorf("Bybit response exceeded %d bytes", maxResponseBytes)
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			message := strings.TrimSpace(string(body))
			if len(message) > 300 {
				message = message[:300]
			}
			apiErr := &APIError{HTTPStatus: response.StatusCode, Message: message}
			lastErr = apiErr
			if apiErr.temporary() && attempt+1 < maxAttempts {
				if err := waitRetry(ctx, attempt); err != nil {
					return err
				}
				continue
			}
			return apiErr
		}
		var envelope struct {
			RetCode int64
			RetMsg  string
			Result  json.RawMessage
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			return fmt.Errorf("decode Bybit response envelope: %w", err)
		}
		if envelope.RetCode != 0 {
			apiErr := &APIError{Code: envelope.RetCode, Message: envelope.RetMsg}
			lastErr = apiErr
			if apiErr.temporary() && attempt+1 < maxAttempts {
				if err := waitRetry(ctx, attempt); err != nil {
					return err
				}
				continue
			}
			return apiErr
		}
		if len(envelope.Result) == 0 || string(envelope.Result) == "null" {
			return errors.New("Bybit response has no result object")
		}
		if err := json.Unmarshal(envelope.Result, destination); err != nil {
			return fmt.Errorf("decode Bybit result: %w", err)
		}
		return nil
	}
	return lastErr
}

func (c *Client) waitForRequestSlot(ctx context.Context) error {
	c.rateMu.Lock()
	slot := c.nextRequest
	if now := time.Now(); slot.Before(now) {
		slot = now
	}
	c.nextRequest = slot.Add(requestInterval)
	c.rateMu.Unlock()
	if delay := time.Until(slot); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

func waitRetry(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(200*(1<<attempt)) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
