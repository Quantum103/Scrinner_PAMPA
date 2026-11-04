package bybit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	streamHeartbeatInterval = 20 * time.Second
	streamReadTimeout       = 55 * time.Second
	streamWriteTimeout      = 5 * time.Second
	streamReconnectMin      = time.Second
	streamReconnectMax      = 30 * time.Second
	subscriptionBatchSize   = 50
	streamMaxMessageBytes   = 1 << 20
)

// KlineSubscription identifies one public linear candle stream.
type KlineSubscription struct {
	Symbol    string
	Timeframe string
}

// StreamHandlers contains callbacks executed by the WebSocket reader goroutine.
// OnConnected runs after subscription requests have been sent and before messages
// are consumed, allowing the caller to reconcile REST history before live events.
type StreamHandlers struct {
	OnConnected  func(context.Context) error
	OnCandle     func(context.Context, Candle) error
	OnDisconnect func(error)
}

// PublicStream manages a public Bybit V5 linear WebSocket connection.
type PublicStream struct {
	url               string
	dialer            *websocket.Dialer
	heartbeatInterval time.Duration
	readTimeout       time.Duration
	writeTimeout      time.Duration
	reconnectMin      time.Duration
	reconnectMax      time.Duration
}

func NewPublicStream(rawURL string) (*PublicStream, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, fmt.Errorf("parse Bybit public WebSocket URL: %w", err)
	}
	if (parsed.Scheme != "wss" && parsed.Scheme != "ws") || parsed.Host == "" ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("Bybit public WebSocket URL must be absolute ws(s) URL without query or fragment")
	}
	return &PublicStream{
		url: strings.TrimRight(rawURL, "/"),
		dialer: &websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
			EnableCompression: false,
		},
		heartbeatInterval: streamHeartbeatInterval,
		readTimeout:       streamReadTimeout,
		writeTimeout:      streamWriteTimeout,
		reconnectMin:      streamReconnectMin,
		reconnectMax:      streamReconnectMax,
	}, nil
}

// Run reconnects indefinitely until ctx is cancelled. Every new connection
// gets the full subscription set, followed by a caller-provided REST backfill.
func (s *PublicStream) Run(ctx context.Context, subscriptions []KlineSubscription, handlers StreamHandlers) error {
	if s == nil {
		return errors.New("Bybit public WebSocket stream is nil")
	}
	if handlers.OnCandle == nil {
		return errors.New("Bybit public WebSocket candle handler is required")
	}
	topics, err := buildKlineTopics(subscriptions)
	if err != nil {
		return err
	}
	if len(topics) == 0 {
		return errors.New("Bybit public WebSocket requires at least one kline subscription")
	}

	backoff := s.reconnectMin
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		err := s.runConnection(ctx, topics, handlers)
		if ctx.Err() != nil {
			return nil
		}
		if handlers.OnDisconnect != nil {
			handlers.OnDisconnect(err)
		}

		delay := jitter(backoff)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		backoff *= 2
		if backoff > s.reconnectMax {
			backoff = s.reconnectMax
		}
	}
}

func (s *PublicStream) runConnection(ctx context.Context, topics []string, handlers StreamHandlers) error {
	conn, response, err := s.dialer.DialContext(ctx, s.url, nil)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return fmt.Errorf("connect to Bybit public WebSocket: %w", err)
	}
	conn.SetReadLimit(streamMaxMessageBytes)
	_ = conn.SetReadDeadline(time.Now().Add(s.readTimeout))
	defer conn.Close()

	for start := 0; start < len(topics); start += subscriptionBatchSize {
		end := start + subscriptionBatchSize
		if end > len(topics) {
			end = len(topics)
		}
		request := struct {
			Operation string   `json:"op"`
			Arguments []string `json:"args"`
		}{Operation: "subscribe", Arguments: topics[start:end]}
		_ = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
		if err := conn.WriteJSON(request); err != nil {
			return fmt.Errorf("subscribe to Bybit kline topics: %w", err)
		}
	}

	connectionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(s.heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-connectionCtx.Done():
				_ = conn.Close()
				return
			case <-ticker.C:
				_ = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
				if err := conn.WriteJSON(map[string]string{"op": "ping"}); err != nil {
					_ = conn.Close()
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		_ = conn.Close()
		<-heartbeatDone
	}()

	if handlers.OnConnected != nil {
		if err := handlers.OnConnected(ctx); err != nil {
			return fmt.Errorf("reconcile scanner after WebSocket connection: %w", err)
		}
	}
	_ = conn.SetReadDeadline(time.Now().Add(s.readTimeout))

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read Bybit public WebSocket message: %w", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(s.readTimeout))

		candles, err := parseKlineMessage(payload)
		if err != nil {
			return fmt.Errorf("decode Bybit public WebSocket message: %w", err)
		}
		for _, candle := range candles {
			if err := handlers.OnCandle(ctx, candle); err != nil {
				return fmt.Errorf("process confirmed %s %s WebSocket candle: %w", candle.Symbol, candle.Timeframe, err)
			}
		}
	}
}

func buildKlineTopics(subscriptions []KlineSubscription) ([]string, error) {
	topics := make([]string, 0, len(subscriptions))
	seen := make(map[string]struct{}, len(subscriptions))
	for _, subscription := range subscriptions {
		symbol := strings.ToUpper(strings.TrimSpace(subscription.Symbol))
		if symbol == "" {
			return nil, errors.New("WebSocket subscription symbol is required")
		}
		for _, char := range symbol {
			if !(char >= 'A' && char <= 'Z') && !(char >= '0' && char <= '9') {
				return nil, fmt.Errorf("invalid WebSocket subscription symbol %q", symbol)
			}
		}
		if _, err := timeframeDuration(subscription.Timeframe); err != nil {
			return nil, err
		}
		topic := "kline." + subscription.Timeframe + "." + symbol
		if _, exists := seen[topic]; exists {
			continue
		}
		seen[topic] = struct{}{}
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics, nil
}

func parseKlineMessage(payload []byte) ([]Candle, error) {
	var envelope struct {
		Topic   string          `json:"topic"`
		Op      string          `json:"op"`
		Success *bool           `json:"success"`
		RetMsg  string          `json:"ret_msg"`
		RetCode int64           `json:"retCode"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if envelope.Op != "" {
		if envelope.Op == "subscribe" && (envelope.Success != nil && !*envelope.Success || envelope.RetCode != 0) {
			return nil, fmt.Errorf("subscription rejected: code=%d message=%s", envelope.RetCode, envelope.RetMsg)
		}
		return nil, nil
	}
	if !strings.HasPrefix(envelope.Topic, "kline.") {
		return nil, nil
	}
	parts := strings.Split(envelope.Topic, ".")
	if len(parts) != 3 || parts[2] == "" {
		return nil, fmt.Errorf("invalid kline topic %q", envelope.Topic)
	}
	timeframe := parts[1]
	interval, err := timeframeDuration(timeframe)
	if err != nil {
		return nil, err
	}
	symbol := strings.ToUpper(parts[2])

	var values []struct {
		Start     int64  `json:"start"`
		End       int64  `json:"end"`
		Interval  string `json:"interval"`
		Open      string `json:"open"`
		High      string `json:"high"`
		Low       string `json:"low"`
		Close     string `json:"close"`
		Volume    string `json:"volume"`
		Turnover  string `json:"turnover"`
		Confirm   bool   `json:"confirm"`
	}
	if len(envelope.Data) == 0 {
		return nil, errors.New("kline message has no data")
	}
	if err := json.Unmarshal(envelope.Data, &values); err != nil {
		return nil, fmt.Errorf("decode kline data: %w", err)
	}

	candles := make([]Candle, 0, len(values))
	for index, value := range values {
		if !value.Confirm {
			continue
		}
		if value.Interval != timeframe || value.Start <= 0 || value.End < value.Start ||
			time.UnixMilli(value.End).After(time.UnixMilli(value.Start).Add(interval)) {
			return nil, fmt.Errorf("confirmed kline %d has invalid interval or timestamps", index)
		}
		fields := []string{value.Open, value.High, value.Low, value.Close, value.Volume, value.Turnover}
		numbers := make([]float64, len(fields))
		for field, raw := range fields {
			numbers[field], err = strconv.ParseFloat(raw, 64)
			if err != nil || math.IsNaN(numbers[field]) || math.IsInf(numbers[field], 0) {
				return nil, fmt.Errorf("confirmed kline %d has invalid numeric field %d", index, field)
			}
		}
		openPrice, highPrice, lowPrice, closePrice := numbers[0], numbers[1], numbers[2], numbers[3]
		volume, turnover := numbers[4], numbers[5]
		if openPrice <= 0 || highPrice <= 0 || lowPrice <= 0 || closePrice <= 0 ||
			highPrice < lowPrice || highPrice < openPrice || highPrice < closePrice ||
			lowPrice > openPrice || lowPrice > closePrice || volume < 0 || turnover < 0 {
			return nil, fmt.Errorf("confirmed kline %d contains an impossible OHLCV range", index)
		}
		start := time.UnixMilli(value.Start).UTC()
		candles = append(candles, Candle{
			Symbol: symbol, Timeframe: timeframe, Start: start, Close: start.Add(interval),
			Open: openPrice, High: highPrice, Low: lowPrice, ClosePrice: closePrice,
			Volume: volume, Turnover: turnover,
		})
	}
	return candles, nil
}

func jitter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}
	spread := int64(delay / 5)
	if spread == 0 {
		return delay
	}
	return delay - time.Duration(spread) + time.Duration(rand.Int63n(spread*2+1))
}
