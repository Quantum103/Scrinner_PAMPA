package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type apiClient struct {
	token   string
	baseURL string
	http    *http.Client
}

type apiEnvelope struct {
	OK          bool            `json:"ok"`
	Description string          `json:"description"`
	Result      json.RawMessage `json:"result"`
}

func (c *apiClient) call(ctx context.Context, method string, payload any, output any) error {
	if c.token == "" {
		return errors.New("Telegram bot token не задан")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("сериализовать запрос Telegram: %w", err)
	}
	endpoint := strings.TrimRight(c.baseURL, "/") + "/bot" + c.token + "/" + method
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("создать запрос Telegram %s: %w", method, err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("запрос Telegram %s: %w", method, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("прочитать ответ Telegram %s: %w", method, err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Telegram API %s вернул HTTP %d", method, response.StatusCode)
	}
	var envelope apiEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("разобрать ответ Telegram %s: %w", method, err)
	}
	if !envelope.OK {
		description := strings.TrimSpace(envelope.Description)
		if description == "" {
			description = "неизвестная ошибка API"
		}
		return fmt.Errorf("Telegram API %s: %s", method, description)
	}
	if output != nil && len(envelope.Result) > 0 {
		if err := json.Unmarshal(envelope.Result, output); err != nil {
			return fmt.Errorf("разобрать результат Telegram %s: %w", method, err)
		}
	}
	return nil
}

func (c *apiClient) getUpdates(ctx context.Context, offset int64, timeoutSeconds int) ([]Update, error) {
	var updates []Update
	err := c.call(ctx, "getUpdates", map[string]any{
		"offset": offset, "timeout": timeoutSeconds, "allowed_updates": []string{"message"},
	}, &updates)
	return updates, err
}

func (c *apiClient) sendMessage(ctx context.Context, chatID int64, message string) error {
	return c.call(ctx, "sendMessage", map[string]any{
		"chat_id": chatID, "text": message, "disable_web_page_preview": true,
	}, nil)
}

type Update struct {
	UpdateID int64 `json:"update_id"`
	Message  *Message `json:"message"`
}

type Message struct {
	From *User `json:"from"`
	Chat Chat `json:"chat"`
	Date int64 `json:"date"`
	Text string `json:"text"`
}

type User struct {
	ID int64 `json:"id"`
}

type Chat struct {
	ID int64 `json:"id"`
	Type string `json:"type"`
}

func defaultHTTPClient() *http.Client {
	return &http.Client{Timeout: 40 * time.Second}
}
