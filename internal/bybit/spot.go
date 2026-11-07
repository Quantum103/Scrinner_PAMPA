package bybit

import (
	"context"
	"errors"
	"fmt"
	"net/url"
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
