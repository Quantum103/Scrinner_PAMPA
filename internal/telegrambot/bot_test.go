package telegrambot

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
)

type fakeTelegramAPI struct {
	sentMessages []string
	edited       []string
	answered     int
	deleted      []int64
	sendErr      error
}

func (f *fakeTelegramAPI) getUpdates(context.Context, int64, int64) ([]telegramUpdate, error) {
	return nil, nil
}
func (f *fakeTelegramAPI) sendMessage(_ context.Context, _ int64, message string, _ *inlineKeyboard) error {
	f.sentMessages = append(f.sentMessages, message)
	return f.sendErr
}
func (f *fakeTelegramAPI) editMessageText(_ context.Context, _, _ int64, message string, _ *inlineKeyboard) error {
	f.edited = append(f.edited, message)
	return nil
}
func (f *fakeTelegramAPI) answerCallbackQuery(context.Context, string, string) error {
	f.answered++
	return nil
}
func (f *fakeTelegramAPI) deleteWebhook(context.Context) error { return nil }
func (f *fakeTelegramAPI) deleteMessage(_ context.Context, _ int64, messageID int64) error {
	f.deleted = append(f.deleted, messageID)
	return nil
}
func (f *fakeTelegramAPI) getMe(context.Context) (telegramUser, error) {
	return telegramUser{ID: 1000, Username: "spot_scanner_bot"}, nil
}
func (f *fakeTelegramAPI) setMyCommands(context.Context) error { return nil }

type fakeTelegramRepository struct {
	users      map[int64]storage.TelegramUser
	subs       map[int64]bool
	clearCalls int
}

func newFakeTelegramRepository() *fakeTelegramRepository {
	return &fakeTelegramRepository{
		users: make(map[int64]storage.TelegramUser),
		subs:  make(map[int64]bool),
	}
}
func (f *fakeTelegramRepository) EnsureTelegramUser(_ context.Context, user storage.TelegramUser) (storage.TelegramUser, error) {
	if saved, ok := f.users[user.TelegramID]; ok {
		return saved, nil
	}
	user.ID = int64(len(f.users) + 1)
	user.IsActive = true
	f.users[user.TelegramID] = user
	return user, nil
}
func (f *fakeTelegramRepository) ActivateTelegramUser(_ context.Context, telegramID int64) error {
	user, ok := f.users[telegramID]
	if !ok {
		return storage.ErrNotFound
	}
	user.IsActive = true
	f.users[telegramID] = user
	return nil
}
func (f *fakeTelegramRepository) GetUserByTelegramID(_ context.Context, telegramID int64) (storage.TelegramUser, error) {
	user, ok := f.users[telegramID]
	if !ok {
		return storage.TelegramUser{}, storage.ErrNotFound
	}
	return user, nil
}
func (f *fakeTelegramRepository) SubscribeSpotAlerts(_ context.Context, userID int64) error {
	f.subs[userID] = true
	return nil
}
func (f *fakeTelegramRepository) UnsubscribeSpotAlerts(_ context.Context, userID int64) error {
	delete(f.subs, userID)
	return nil
}
func (f *fakeTelegramRepository) ListUserSpotAlertHistory(context.Context, int64, int, int) ([]storage.SpotAlertHistoryItem, error) {
	return nil, nil
}
func (f *fakeTelegramRepository) ClearUserSpotAlertHistory(context.Context, int64) (int64, error) {
	f.clearCalls++
	return 3, nil
}
func (f *fakeTelegramRepository) ClaimPendingSpotAlertDeliveries(context.Context, int, time.Duration) ([]storage.SpotAlertDelivery, error) {
	return nil, nil
}
func (f *fakeTelegramRepository) MarkSpotAlertDeliverySent(context.Context, int64, int64) error {
	return nil
}
func (f *fakeTelegramRepository) MarkSpotAlertDeliveryFailed(context.Context, int64, int64, time.Time, error) error {
	return nil
}

func TestStartAndStopControlSpotSubscription(t *testing.T) {
	api := &fakeTelegramAPI{}
	repository := newFakeTelegramRepository()
	user := storage.TelegramUser{ID: 1, TelegramID: 42, IsActive: true, FirstName: "Test"}
	repository.users[user.TelegramID] = user
	bot, err := newWithAPI(api, "long-enough-password", repository, nil)
	if err != nil {
		t.Fatal(err)
	}
	message := &telegramMessage{
		MessageID: 1, From: &telegramUser{ID: 42, FirstName: "Test"},
		Chat: telegramChat{ID: 42, Type: "private"}, Text: "/start",
	}
	if err := bot.handleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if !repository.subs[user.ID] {
		t.Fatal("/start did not subscribe to Spot alerts")
	}
	message.Text = "/stop"
	if err := bot.handleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if repository.subs[user.ID] {
		t.Fatal("/stop did not unsubscribe from Spot alerts")
	}
}

func TestClearHistoryRequiresExplicitConfirmation(t *testing.T) {
	api := &fakeTelegramAPI{}
	repository := newFakeTelegramRepository()
	repository.users[42] = storage.TelegramUser{ID: 1, TelegramID: 42, IsActive: true}
	bot, err := newWithAPI(api, "long-enough-password", repository, nil)
	if err != nil {
		t.Fatal(err)
	}
	message := &telegramMessage{
		MessageID: 7, From: &telegramUser{ID: 42},
		Chat: telegramChat{ID: 42, Type: "private"}, Text: "/clearhistory",
	}
	if err := bot.handleMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if repository.clearCalls != 0 {
		t.Fatal("history was cleared before confirmation")
	}
	callback := &telegramCallback{
		ID: "callback", From: &telegramUser{ID: 42},
		Message: &telegramMessage{MessageID: 7, Chat: telegramChat{ID: 42, Type: "private"}},
		Data:    "clear_history:confirm",
	}
	if err := bot.handleCallback(context.Background(), callback); err != nil {
		t.Fatal(err)
	}
	if repository.clearCalls != 1 || len(api.edited) != 1 || !strings.Contains(api.edited[0], "3") {
		t.Fatalf("confirmation did not clear and report history: calls=%d edits=%v", repository.clearCalls, api.edited)
	}
}

func TestPasswordMessageIsDeletedAndNotEchoed(t *testing.T) {
	api := &fakeTelegramAPI{}
	repository := newFakeTelegramRepository()
	bot, err := newWithAPI(api, "long-enough-password", repository, nil)
	if err != nil {
		t.Fatal(err)
	}
	message := &telegramMessage{
		MessageID: 9, From: &telegramUser{ID: 77},
		Chat: telegramChat{ID: 77, Type: "private"}, Text: "incorrect-password",
	}
	if err := bot.handlePasswordAuthentication(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if len(api.deleted) != 1 || len(api.sentMessages) != 1 ||
		strings.Contains(api.sentMessages[0], message.Text) {
		t.Fatalf("password was not deleted safely: deleted=%v messages=%v", api.deleted, api.sentMessages)
	}
}

func TestSpotAlertFormattingAndPermanentDeliveryErrors(t *testing.T) {
	message := formatSpotMovement(
		"ABCUSDT", 7.25, 1.08, 1,
		time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC), 900,
	)
	for _, part := range []string{"ABCUSDT", "+7.25%", "15 мин.", "1.08"} {
		if !strings.Contains(message, part) {
			t.Fatalf("formatted alert %q does not contain %q", message, part)
		}
	}
	if !isPermanentTelegramDeliveryError(&telegramAPIError{Code: 403, Description: "bot was blocked by the user"}) {
		t.Fatal("blocked user should be a permanent delivery failure")
	}
	if isPermanentTelegramDeliveryError(errors.New("network timeout")) {
		t.Fatal("network timeout must remain retryable")
	}
}
