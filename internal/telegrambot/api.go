package telegrambot

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

const (
	defaultTelegramAPIURL = "https://api.telegram.org"
	maxTelegramBodyBytes  = 4 << 20
)

type apiClient struct {
	token      string
	baseURL    string
	httpClient *http.Client
}

func newAPIClient(token string, client *http.Client, baseURL string) (*apiClient, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, errors.New("Telegram bot token is required")
	}
	if strings.ContainsAny(token, "/?#") {
		return nil, errors.New("Telegram bot token contains invalid URL characters")
	}
	if client == nil {
		client = &http.Client{Timeout: 35 * time.Second}
	}
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultTelegramAPIURL
	}
	baseURL = strings.TrimRight(baseURL, "/")
	if !strings.HasPrefix(baseURL, "https://") && !strings.HasPrefix(baseURL, "http://") {
		return nil, errors.New("Telegram API base URL must use http:// or https://")
	}
	return &apiClient{token: token, baseURL: baseURL, httpClient: client}, nil
}

type telegramAPIError struct {
	Method      string
	Code        int
	Description string
}

func (e *telegramAPIError) Error() string {
	return fmt.Sprintf("Telegram %s failed (code %d): %s", e.Method, e.Code, e.Description)
}

func isPermanentTelegramDeliveryError(err error) bool {
	var apiErr *telegramAPIError
	if !errors.As(err, &apiErr) {
		return false
	}
	if apiErr.Code == http.StatusForbidden {
		return true
	}
	description := strings.ToLower(apiErr.Description)
	if apiErr.Code != http.StatusBadRequest {
		return false
	}
	for _, phrase := range []string{
		"chat not found",
		"user is deactivated",
		"bot was blocked by the user",
		"bot was kicked from the group chat",
		"can't initiate conversation",
	} {
		if strings.Contains(description, phrase) {
			return true
		}
	}
	return false
}

type apiEnvelope struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	Description string          `json:"description"`
	ErrorCode   int             `json:"error_code"`
}

func (c *apiClient) call(ctx context.Context, method string, requestBody any, destination any) error {
	body, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("encode Telegram %s request: %w", method, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/bot"+c.token+"/"+method, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build Telegram %s request: %w", method, err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("send Telegram %s request: %w", method, err)
	}
	defer response.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(response.Body, maxTelegramBodyBytes+1))
	if err != nil {
		return fmt.Errorf("read Telegram %s response: %w", method, err)
	}
	if len(payload) > maxTelegramBodyBytes {
		return fmt.Errorf("Telegram %s response exceeded %d bytes", method, maxTelegramBodyBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message := strings.TrimSpace(string(payload))
		if len(message) > 300 {
			message = message[:300]
		}
		return &telegramAPIError{Method: method, Code: response.StatusCode, Description: message}
	}

	var envelope apiEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("decode Telegram %s response: %w", method, err)
	}
	if !envelope.OK {
		return &telegramAPIError{Method: method, Code: envelope.ErrorCode, Description: envelope.Description}
	}
	if destination != nil && len(envelope.Result) > 0 && string(envelope.Result) != "null" {
		if err := json.Unmarshal(envelope.Result, destination); err != nil {
			return fmt.Errorf("decode Telegram %s result: %w", method, err)
		}
	}
	return nil
}

func (c *apiClient) getUpdates(ctx context.Context, offset, timeout int64) ([]telegramUpdate, error) {
	var updates []telegramUpdate
	err := c.call(ctx, "getUpdates", map[string]any{
		"offset": offset, "limit": 100, "timeout": timeout,
		"allowed_updates": []string{"message", "callback_query"},
	}, &updates)
	return updates, err
}

func (c *apiClient) sendMessage(ctx context.Context, chatID int64, message string, markup *inlineKeyboard) error {
	request := map[string]any{"chat_id": chatID, "text": message, "disable_web_page_preview": true}
	if markup != nil {
		request["reply_markup"] = markup
	}
	return c.call(ctx, "sendMessage", request, nil)
}

func (c *apiClient) editMessageText(ctx context.Context, chatID, messageID int64, message string, markup *inlineKeyboard) error {
	request := map[string]any{
		"chat_id": chatID, "message_id": messageID, "text": message,
		"disable_web_page_preview": true,
	}
	if markup != nil {
		request["reply_markup"] = markup
	}
	return c.call(ctx, "editMessageText", request, nil)
}

func (c *apiClient) deleteMessage(ctx context.Context, chatID, messageID int64) error {
	return c.call(ctx, "deleteMessage", map[string]any{"chat_id": chatID, "message_id": messageID}, nil)
}

func (c *apiClient) answerCallbackQuery(ctx context.Context, callbackID, message string) error {
	request := map[string]any{"callback_query_id": callbackID}
	if message != "" {
		request["text"] = message
	}
	return c.call(ctx, "answerCallbackQuery", request, nil)
}

func (c *apiClient) deleteWebhook(ctx context.Context) error {
	return c.call(ctx, "deleteWebhook", map[string]any{"drop_pending_updates": false}, nil)
}

func (c *apiClient) getMe(ctx context.Context) (telegramUser, error) {
	var user telegramUser
	err := c.call(ctx, "getMe", map[string]any{}, &user)
	return user, err
}

func (c *apiClient) setMyCommands(ctx context.Context) error {
	commands := []map[string]string{
		{"command": "start", "description": "Включить уведомления о росте"},
		{"command": "stop", "description": "Отключить уведомления"},
		{"command": "history", "description": "История сигналов роста"},
		{"command": "clearhistory", "description": "Очистить свою историю"},
		{"command": "help", "description": "Справка"},
	}
	return c.call(ctx, "setMyCommands", map[string]any{"commands": commands}, nil)
}
