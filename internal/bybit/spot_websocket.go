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
	spotStreamHeartbeatInterval = 20 * time.Second
	spotStreamReadTimeout       = 55 * time.Second
	spotStreamWriteTimeout      = 5 * time.Second
	spotStreamReconnectMin      = time.Second
	spotStreamReconnectMax      = 30 * time.Second
	spotSubscriptionBatchSize   = 10
	spotMaximumTopicsPerConn    = 100
	spotStreamMaxMessageBytes   = 1 << 20
)

// SpotPriceUpdate is the latest trade price published by Bybit's Spot ticker
// stream. At is Bybit's exchange-generated message timestamp.
type SpotPriceUpdate struct {
	Symbol string
	Price  float64
	At     time.Time
}

// SpotStreamHandlers contains callbacks for incoming Spot ticker updates.
type SpotStreamHandlers struct {
	OnTicker     func(context.Context, SpotPriceUpdate) error
	OnDisconnect func(error)
}

// PublicSpotStream manages a Bybit V5 public Spot ticker connection.
// Bybit limits Spot subscribe requests to 10 topics per request and currently
// documents up to 100 topics per public connection.
type PublicSpotStream struct {
	url               string
	dialer            *websocket.Dialer
	heartbeatInterval time.Duration
	readTimeout       time.Duration
	writeTimeout      time.Duration
	reconnectMin      time.Duration
	reconnectMax      time.Duration
}

func NewPublicSpotStream(rawURL string) (*PublicSpotStream, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, fmt.Errorf("parse Bybit Spot WebSocket URL: %w", err)
	}
	if (parsed.Scheme != "wss" && parsed.Scheme != "ws") || parsed.Host == "" ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("Bybit Spot WebSocket URL must be absolute ws(s) URL without query or fragment")
	}
	return &PublicSpotStream{
		url: strings.TrimRight(rawURL, "/"),
		dialer: &websocket.Dialer{
			HandshakeTimeout: 10 * time.Second, EnableCompression: false,
		},
		heartbeatInterval: spotStreamHeartbeatInterval,
		readTimeout:       spotStreamReadTimeout,
		writeTimeout:      spotStreamWriteTimeout,
		reconnectMin:      spotStreamReconnectMin,
		reconnectMax:      spotStreamReconnectMax,
	}, nil
}

// Run reconnects until ctx is cancelled. Each connection subscribes to no more
// than 100 tickers, sent in batches of no more than 10 topics.
func (s *PublicSpotStream) Run(ctx context.Context, symbols []string, handlers SpotStreamHandlers) error {
	if s == nil {
		return errors.New("Bybit public Spot WebSocket stream is nil")
	}
	if handlers.OnTicker == nil {
		return errors.New("Bybit Spot ticker handler is required")
	}
	topics, err := buildSpotTickerTopics(symbols)
	if err != nil {
		return err
	}
	if len(topics) == 0 {
		return nil
	}
	if len(topics) > spotMaximumTopicsPerConn {
		return fmt.Errorf("Bybit Spot connection supports at most %d ticker topics", spotMaximumTopicsPerConn)
	}

	backoff := s.reconnectMin
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := s.runConnection(ctx, topics, handlers)
		if ctx.Err() != nil {
			return nil
		}
		if handlers.OnDisconnect != nil {
			handlers.OnDisconnect(err)
		}
		timer := time.NewTimer(spotStreamJitter(backoff))
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

func (s *PublicSpotStream) runConnection(ctx context.Context, topics []string, handlers SpotStreamHandlers) error {
	conn, response, err := s.dialer.DialContext(ctx, s.url, nil)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return fmt.Errorf("connect to Bybit public Spot WebSocket: %w", err)
	}
	conn.SetReadLimit(spotStreamMaxMessageBytes)
	_ = conn.SetReadDeadline(time.Now().Add(s.readTimeout))
	defer conn.Close()

	for start := 0; start < len(topics); start += spotSubscriptionBatchSize {
		end := start + spotSubscriptionBatchSize
		if end > len(topics) {
			end = len(topics)
		}
		request := struct {
			Operation string   `json:"op"`
			Arguments []string `json:"args"`
		}{Operation: "subscribe", Arguments: topics[start:end]}
		_ = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
		if err := conn.WriteJSON(request); err != nil {
			return fmt.Errorf("subscribe to Bybit Spot ticker topics: %w", err)
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

	_ = conn.SetReadDeadline(time.Now().Add(s.readTimeout))
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read Bybit public Spot WebSocket message: %w", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(s.readTimeout))

		update, ok, err := parseSpotTickerMessage(payload)
		if err != nil {
			return fmt.Errorf("decode Bybit Spot ticker message: %w", err)
		}
		if !ok {
			continue
		}
		if err := handlers.OnTicker(ctx, update); err != nil {
			return fmt.Errorf("process Bybit Spot ticker %s: %w", update.Symbol, err)
		}
	}
}

func buildSpotTickerTopics(symbols []string) ([]string, error) {
	topics := make([]string, 0, len(symbols))
	seen := make(map[string]struct{}, len(symbols))
	for _, value := range symbols {
		symbol := strings.ToUpper(strings.TrimSpace(value))
		if symbol == "" {
			return nil, errors.New("Bybit Spot ticker symbol is required")
		}
		for _, char := range symbol {
			if !(char >= 'A' && char <= 'Z') && !(char >= '0' && char <= '9') {
				return nil, fmt.Errorf("invalid Bybit Spot ticker symbol %q", symbol)
			}
		}
		topic := "tickers." + symbol
		if _, exists := seen[topic]; exists {
			continue
		}
		seen[topic] = struct{}{}
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics, nil
}

func parseSpotTickerMessage(payload []byte) (SpotPriceUpdate, bool, error) {
	var envelope struct {
		Topic   string          `json:"topic"`
		Op      string          `json:"op"`
		Success *bool           `json:"success"`
		RetMsg  string          `json:"ret_msg"`
		RetCode int64           `json:"retCode"`
		TS      int64           `json:"ts"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return SpotPriceUpdate{}, false, fmt.Errorf("invalid JSON: %w", err)
	}
	if envelope.Op != "" {
		if envelope.Op == "subscribe" && ((envelope.Success != nil && !*envelope.Success) || envelope.RetCode != 0) {
			return SpotPriceUpdate{}, false, fmt.Errorf("subscription rejected: code=%d message=%s", envelope.RetCode, envelope.RetMsg)
		}
		return SpotPriceUpdate{}, false, nil
	}
	if !strings.HasPrefix(envelope.Topic, "tickers.") {
		return SpotPriceUpdate{}, false, nil
	}
	parts := strings.Split(envelope.Topic, ".")
	if len(parts) != 2 || parts[1] == "" {
		return SpotPriceUpdate{}, false, fmt.Errorf("invalid Spot ticker topic %q", envelope.Topic)
	}
	var data struct {
		Symbol    string `json:"symbol"`
		LastPrice string `json:"lastPrice"`
	}
	if len(envelope.Data) == 0 {
		return SpotPriceUpdate{}, false, errors.New("Spot ticker message has no data")
	}
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		return SpotPriceUpdate{}, false, fmt.Errorf("decode Spot ticker data: %w", err)
	}
	symbol := strings.ToUpper(strings.TrimSpace(data.Symbol))
	if symbol == "" || symbol != strings.ToUpper(parts[1]) {
		return SpotPriceUpdate{}, false, fmt.Errorf("Spot ticker topic %q does not match symbol %q", envelope.Topic, data.Symbol)
	}
	price, err := strconv.ParseFloat(data.LastPrice, 64)
	if err != nil || math.IsNaN(price) || math.IsInf(price, 0) || price <= 0 {
		return SpotPriceUpdate{}, false, fmt.Errorf("Spot ticker %s has invalid lastPrice %q", symbol, data.LastPrice)
	}
	if envelope.TS <= 0 {
		return SpotPriceUpdate{}, false, fmt.Errorf("Spot ticker %s has invalid exchange timestamp %d", symbol, envelope.TS)
	}
	return SpotPriceUpdate{Symbol: symbol, Price: price, At: time.UnixMilli(envelope.TS).UTC()}, true, nil
}

func spotStreamJitter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}
	spread := int64(delay / 5)
	if spread == 0 {
		return delay
	}
	return delay - time.Duration(spread) + time.Duration(rand.Int63n(spread*2+1))
}
