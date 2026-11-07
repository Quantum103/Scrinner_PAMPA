package bybit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"time"
	"strings"
)

// SpotInstrument describes an active Bybit Spot trading pair.
type SpotInstrument struct {
	Symbol    string
	BaseCoin  string
	QuoteCoin string
	Status    string
}

// SpotTicker contains the current Spot price and rolling 24-hour quote turnover.
type SpotTicker struct {
	Symbol      string
	LastPrice   string
	Turnover24h string
}

// GetSpotInstruments returns the complete Spot catalogue. Bybit's Spot
// instruments-info endpoint does not support pagination, so it is read once.
func (c *Client) GetSpotInstruments(ctx context.Context) ([]SpotInstrument, error) {
	query := url.Values{"category": {"spot"}, "status": {"Trading"}}
	var response struct {
		List []struct {
			Symbol    string
			BaseCoin  string
			QuoteCoin string
			Status    string
		}
	}
	if err := c.get(ctx, "/v5/market/instruments-info", query, &response); err != nil {
		return nil, fmt.Errorf("get Bybit Spot instruments: %w", err)
	}
	if len(response.List) == 0 {
		return nil, errors.New("Bybit returned an empty Spot instrument catalogue")
	}

	result := make([]SpotInstrument, 0, len(response.List))
	seen := make(map[string]struct{}, len(response.List))
	for _, item := range response.List {
		symbol := strings.ToUpper(strings.TrimSpace(item.Symbol))
		baseCoin := strings.ToUpper(strings.TrimSpace(item.BaseCoin))
		quoteCoin := strings.ToUpper(strings.TrimSpace(item.QuoteCoin))
		status := strings.TrimSpace(item.Status)
		if symbol == "" || baseCoin == "" || quoteCoin == "" {
			return nil, errors.New("Bybit Spot instrument contains an empty symbol or coin")
		}
		if status != "Trading" || quoteCoin != "USDT" {
			continue
		}
		if _, exists := seen[symbol]; exists {
			return nil, fmt.Errorf("Bybit returned duplicate Spot instrument %s", symbol)
		}
		seen[symbol] = struct{}{}
		result = append(result, SpotInstrument{Symbol: symbol, BaseCoin: baseCoin, QuoteCoin: quoteCoin, Status: status})
	}
	if len(result) == 0 {
		return nil, errors.New("Bybit Spot catalogue contains no active USDT pairs")
	}
	return result, nil
}

// GetSpotTickers returns a current ticker snapshot for every Spot symbol.
// For USDT pairs turnover24h is denominated in USDT.
func (c *Client) GetSpotTickers(ctx context.Context) ([]SpotTicker, error) {
	query := url.Values{"category": {"spot"}}
	var response struct {
		List []struct {
			Symbol      string
			LastPrice   string
			Turnover24h string
		}
	}
	if err := c.get(ctx, "/v5/market/tickers", query, &response); err != nil {
		return nil, fmt.Errorf("get Bybit Spot tickers: %w", err)
	}
	if len(response.List) == 0 {
		return nil, errors.New("Bybit returned an empty Spot ticker snapshot")
	}

	result := make([]SpotTicker, 0, len(response.List))
	seen := make(map[string]struct{}, len(response.List))
	for _, item := range response.List {
		symbol := strings.ToUpper(strings.TrimSpace(item.Symbol))
		if symbol == "" {
			return nil, errors.New("Bybit Spot ticker contains an empty symbol")
		}
		if _, exists := seen[symbol]; exists {
			return nil, fmt.Errorf("Bybit returned duplicate Spot ticker %s", symbol)
		}
		seen[symbol] = struct{}{}
		result = append(result, SpotTicker{Symbol: symbol, LastPrice: strings.TrimSpace(item.LastPrice), Turnover24h: strings.TrimSpace(item.Turnover24h)})
	}
	return result, nil
}


// GetSpotKlines reads closed one-minute Spot candles using the current Bybit
// server time. It is intended to seed the rolling momentum reference window.
func (c *Client) GetSpotKlines(ctx context.Context, symbol string, limit int) ([]Candle, error) {
	serverTime, err := c.GetServerTime(ctx)
	if err != nil {
		return nil, err
	}
	return c.GetSpotKlinesAt(ctx, symbol, limit, serverTime)
}

// GetSpotKlinesAt reads closed one-minute Spot candles using a shared exchange
// timestamp. Returned candles are sorted from oldest to newest.
func (c *Client) GetSpotKlinesAt(ctx context.Context, symbol string, limit int, serverTime time.Time) ([]Candle, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return nil, errors.New("Bybit Spot kline symbol is required")
	}
	for _, char := range symbol {
		if !(char >= 'A' && char <= 'Z') && !(char >= '0' && char <= '9') {
			return nil, fmt.Errorf("invalid Bybit Spot kline symbol %q", symbol)
		}
	}
	if limit < 1 || limit > 1000 {
		return nil, errors.New("Bybit Spot kline limit must be between 1 and 1000")
	}
	if serverTime.IsZero() {
		return nil, errors.New("Bybit exchange time is required")
	}
	serverTime = serverTime.UTC()
	const interval = time.Minute
	query := url.Values{
		"category": {"spot"}, "symbol": {symbol},
		"interval": {"1"}, "limit": {strconv.Itoa(limit)},
		"end": {strconv.FormatInt(serverTime.UnixMilli(), 10)},
	}
	var response struct {
		List [][]string
	}
	if err := c.get(ctx, "/v5/market/kline", query, &response); err != nil {
		return nil, fmt.Errorf("get Bybit Spot %s one-minute klines: %w", symbol, err)
	}

	candles := make([]Candle, 0, len(response.List))
	seenStarts := make(map[int64]struct{}, len(response.List))
	for index, values := range response.List {
		if len(values) < 7 {
			return nil, fmt.Errorf("Bybit Spot candle %d has %d fields, want at least 7", index, len(values))
		}
		startMillis, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil || startMillis <= 0 {
			return nil, fmt.Errorf("Bybit Spot candle %d has invalid start timestamp %q", index, values[0])
		}
		if _, exists := seenStarts[startMillis]; exists {
			return nil, fmt.Errorf("Bybit returned duplicate Spot candle at %s", time.UnixMilli(startMillis).UTC())
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
				return nil, fmt.Errorf("Bybit Spot candle %d has invalid numeric field %d", index, field+1)
			}
		}
		openPrice, highPrice, lowPrice, closePrice := numbers[0], numbers[1], numbers[2], numbers[3]
		volume, turnover := numbers[4], numbers[5]
		if openPrice <= 0 || highPrice <= 0 || lowPrice <= 0 || closePrice <= 0 ||
			highPrice < lowPrice || highPrice < openPrice || highPrice < closePrice ||
			lowPrice > openPrice || lowPrice > closePrice || volume < 0 || turnover < 0 {
			return nil, fmt.Errorf("Bybit Spot candle %d contains an impossible OHLCV range", index)
		}
		candles = append(candles, Candle{
			Symbol: symbol, Timeframe: "1", Start: start, Close: closeTime,
			Open: openPrice, High: highPrice, Low: lowPrice, ClosePrice: closePrice,
			Volume: volume, Turnover: turnover,
		})
	}
	sort.Slice(candles, func(i, j int) bool { return candles[i].Start.Before(candles[j].Start) })
	return candles, nil
}
