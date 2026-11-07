package telegrambot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTelegramAPIGetUpdatesUsesOffsetAndAllowedUpdateTypes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/bottest-token/getUpdates" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var request struct {
			Offset        int64    `json:"offset"`
			Limit         int      `json:"limit"`
			Timeout       int64    `json:"timeout"`
			AllowedUpdate []string `json:"allowed_updates"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.Offset != 1234 || request.Limit != 100 || request.Timeout != 25 {
			t.Fatalf("unexpected getUpdates request: %+v", request)
		}
		if len(request.AllowedUpdate) != 2 || request.AllowedUpdate[0] != "message" ||
			request.AllowedUpdate[1] != "callback_query" {
			t.Fatalf("unexpected allowed_updates: %#v", request.AllowedUpdate)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":[{"update_id":1234,"message":{"message_id":7,"text":"/start","chat":{"id":99,"type":"private"},"from":{"id":99,"first_name":"Test"}}}]}`))
	}))
	defer server.Close()

	client, err := newAPIClient("test-token", server.Client(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	updates, err := client.getUpdates(context.Background(), 1234, 25)
	if err != nil {
		t.Fatalf("getUpdates() error = %v", err)
	}
	if len(updates) != 1 || updates[0].UpdateID != 1234 ||
		updates[0].Message == nil || updates[0].Message.From.ID != 99 {
		t.Fatalf("unexpected Telegram updates: %+v", updates)
	}
}

func TestTelegramAPIReportsTelegramErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":false,"error_code":403,"description":"bot was blocked by the user"}`))
	}))
	defer server.Close()

	client, err := newAPIClient("test-token", server.Client(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = client.sendMessage(context.Background(), 123, "hello", nil)
	if err == nil || !containsText(err.Error(), "403") || !containsText(err.Error(), "blocked") {
		t.Fatalf("sendMessage error = %v, want Telegram 403 error", err)
	}
}

func containsText(value, part string) bool {
	for index := 0; index+len(part) <= len(value); index++ {
		if value[index:index+len(part)] == part {
			return true
		}
	}
	return false
}

func TestTelegramAPIDeleteMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/bottest-token/deleteMessage" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var request struct {
			ChatID    int64 `json:"chat_id"`
			MessageID int64 `json:"message_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.ChatID != 42 || request.MessageID != 99 {
			t.Fatalf("unexpected deleteMessage request: %+v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
	}))
	defer server.Close()

	client, err := newAPIClient("test-token", server.Client(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.deleteMessage(context.Background(), 42, 99); err != nil {
		t.Fatalf("deleteMessage() error = %v", err)
	}
}

func TestPermanentTelegramDeliveryErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "blocked user",
			err: &telegramAPIError{Method: "sendMessage", Code: 403, Description: "bot was blocked by the user"},
			want: true,
		},
		{
			name: "missing private chat",
			err: &telegramAPIError{Method: "sendMessage", Code: 400, Description: "Bad Request: chat not found"},
			want: true,
		},
		{
			name: "rate limit",
			err: &telegramAPIError{Method: "sendMessage", Code: 429, Description: "Too Many Requests"},
			want: false,
		},
		{
			name: "server error",
			err: &telegramAPIError{Method: "sendMessage", Code: 500, Description: "Internal Server Error"},
			want: false,
		},
		{
			name: "network error",
			err: errors.New("connection reset"),
			want: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isPermanentTelegramDeliveryError(test.err); got != test.want {
				t.Fatalf("isPermanentTelegramDeliveryError() = %t, want %t", got, test.want)
			}
		})
	}
}
