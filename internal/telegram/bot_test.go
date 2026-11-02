package telegram

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	marketpaper "github.com/Quantum103/Scrinner_PAMPA/internal/paper"
	"github.com/Quantum103/Scrinner_PAMPA/internal/stream"
)

func newTestManager(t *testing.T) *stream.Manager {
	t.Helper()
	cfg := stream.DefaultConfig("wss://example.test/v5/public/linear")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager, err := stream.NewManager(cfg, logger)
	if err != nil {
		t.Fatalf("stream.NewManager() error = %v", err)
	}
	return manager
}

type messageCapture struct {
	mu       sync.Mutex
	messages []string
}

func TestAllowlistControlsPauseAndResumeCommands(t *testing.T) {
	var capture messageCapture
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/sendMessage") {
			t.Errorf("unexpected Telegram API method: %s", r.URL.Path)
		}
		var request struct {
			ChatID int64  `json:"chat_id"`
			Text   string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode sendMessage request: %v", err)
		}
		capture.mu.Lock()
		capture.messages = append(capture.messages, request.Text)
		capture.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true,"result":{}}`)
	}))
	defer server.Close()

	manager := newTestManager(t)
	bot, err := NewBot(Config{
		Token: "test-token", AllowedUserIDs: []int64{12345},
		DailyLossLimitPercent: 5, APIBaseURL: server.URL,
	}, manager, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewBot() error = %v", err)
	}

	// Неавторизованный пользователь не может управлять приложением.
	bot.processUpdate(context.Background(), Update{Message: &Message{
		From: &User{ID: 99999}, Chat: Chat{ID: 99999, Type: "private"}, Text: "/pause",
	}})
	if manager.EntryPaused() {
		t.Fatal("unauthorized user paused the entry strategy")
	}
	capture.mu.Lock()
	if len(capture.messages) != 0 {
		t.Fatalf("unauthorized command generated a response: %v", capture.messages)
	}
	capture.mu.Unlock()

	bot.processUpdate(context.Background(), Update{Message: &Message{
		From: &User{ID: 12345}, Chat: Chat{ID: 12345, Type: "private"}, Text: "/pause@volume_trader_bot",
	}})
	if !manager.EntryPaused() {
		t.Fatal("authorized /pause did not stop new entries")
	}
	bot.processUpdate(context.Background(), Update{Message: &Message{
		From: &User{ID: 12345}, Chat: Chat{ID: 12345, Type: "private"}, Text: "/resume",
	}})
	if manager.EntryPaused() {
		t.Fatal("authorized /resume did not resume new entries")
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.messages) != 2 ||
		!strings.Contains(strings.ToLower(capture.messages[0]), "сопровождение") ||
		!strings.Contains(capture.messages[1], "возобновлён") {
		t.Fatalf("unexpected command responses: %v", capture.messages)
	}
}

func TestResumeCannotOverrideDailyLossLock(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true,"result":{}}`)
	}))
	defer server.Close()
	manager := newTestManager(t)
	bot, err := NewBot(Config{
		Token: "test-token", AllowedUserIDs: []int64{1}, DailyLossLimitPercent: 5, APIBaseURL: server.URL,
	}, manager, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	manager.SetDailyLossReached(true)
	// processUpdate response API can fail because this test only verifies the control state.
	bot.processUpdate(context.Background(), Update{Message: &Message{
		From: &User{ID: 1}, Chat: Chat{ID: 1, Type: "private"}, Text: "/resume",
	}})
	if !manager.EntryPaused() {
		t.Fatal("/resume bypassed the daily loss limit")
	}
}

func TestDailyStartingEquityUsesFirstPointOfCurrentUTCDay(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	history := []marketpaper.EquityPoint{
		{At: now.Add(-24 * time.Hour), Equity: 10},
		{At: now.Add(-time.Hour), Equity: 11},
		{At: now.Add(time.Hour), Equity: 9},
	}
	start, ok := dailyStartingEquity(history, now)
	if !ok || start != 11 {
		t.Fatalf("daily starting equity = %.4f, found=%v; want 11", start, ok)
	}
	if _, ok := dailyStartingEquity(history[:1], now); ok {
		t.Fatal("yesterday's equity point was incorrectly used as today's baseline")
	}
}

func TestClosedTradeNotificationContainsAllExecutionCostsAndBalance(t *testing.T) {
	manager := newTestManager(t)
	bot, err := NewBot(Config{
		Token: "test-token", AllowedUserIDs: []int64{123},
		DailyLossLimitPercent: 5,
	}, manager, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	closedAt := time.Now().UTC()
	trade := marketpaper.Position{
		Symbol: "BTCUSDT", Direction: "LONG", EntryPrice: 100, ExitPrice: 102,
		ClosedQuantity: 2, EntryFee: 0.1, ExitFee: 0.2,
		EntrySlippageUSDT: 0.03, ExitSlippageUSDT: 0.04,
		FundingPaid: 0.05, RealizedPnL: 3.61,
		CloseReason: "сработал виртуальный Trailing Stop", ClosedAt: closedAt,
	}
	bot.OnPaperEvent(marketpaper.Event{
		Type: "position_closed", Symbol: "BTCUSDT", At: closedAt,
	}, marketpaper.AccountSnapshot{Balance: 13.61, ClosedTrades: []marketpaper.Position{trade}})

	var message string
	select {
	case message = <-bot.notifications:
	case <-time.After(time.Second):
		t.Fatal("close notification was not queued")
	}
	for _, expected := range []string{
		"Paper Trading", "Цена выхода", "Причина выхода", "PnL до издержек",
		"Комиссии", "Проскальзывание", "Funding", "Чистый PnL", "Новый виртуальный баланс",
	} {
		if !strings.Contains(message, expected) {
			t.Errorf("notification does not contain %q: %s", expected, message)
		}
	}
	if !strings.Contains(message, "13.610000") {
		t.Fatalf("new balance is missing: %s", message)
	}
}
