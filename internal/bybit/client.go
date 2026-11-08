package bybit

import (
	"context"
	"encoding/json"
	"errors"
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
	maxResponseBytes = 8 << 20
	maxAttempts      = 3
	requestInterval  = 100 * time.Millisecond
)

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
		request.Header.Set("User-Agent", "spot-scanner/1.0")
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
