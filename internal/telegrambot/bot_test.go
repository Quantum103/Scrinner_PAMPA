package telegrambot

import (
	"context"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
)

type fakeTelegramAPI struct {
	sentMessages []string
	edited       []string
	answered     int
}

func (f *fakeTelegramAPI) getUpdates(context.Context, int64, int64) ([]telegramUpdate, error) {
	return nil, nil
}

func (f *fakeTelegramAPI) sendMessage(_ context.Context, _ int64, message string, _ *inlineKeyboard) error {
	f.sentMessages = append(f.sentMessages, message)
	return nil
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

func (f *fakeTelegramAPI) getMe(context.Context) (telegramUser, error) {
	return telegramUser{ID: 1000, Username: "rsi_scanner_bot"}, nil
}

func (f *fakeTelegramAPI) setMyCommands(context.Context) error { return nil }

type fakeTelegramRepository struct {
	users       map[int64]storage.TelegramUser
	settings    map[int64]storage.UserSettings
	ensureCalls int
	updates     int
}

func newFakeTelegramRepository() *fakeTelegramRepository {
	return &fakeTelegramRepository{
		users:    make(map[int64]storage.TelegramUser),
		settings: make(map[int64]storage.UserSettings),
	}
}

func (f *fakeTelegramRepository) EnsureTelegramUser(_ context.Context, user storage.TelegramUser) (storage.TelegramUser, error) {
	f.ensureCalls++
	if saved, exists := f.users[user.TelegramID]; exists {
		return saved, nil
	}
	user.ID = int64(len(f.users) + 1)
	user.IsActive = true
	f.users[user.TelegramID] = user
	f.settings[user.ID] = storage.DefaultUserSettings(user.ID)
	return user, nil
}

func (f *fakeTelegramRepository) GetUserByTelegramID(_ context.Context, telegramID int64) (storage.TelegramUser, error) {
	user, exists := f.users[telegramID]
	if !exists {
		return storage.TelegramUser{}, storage.ErrNotFound
	}
	return user, nil
}

func (f *fakeTelegramRepository) GetOrCreateUserSettings(_ context.Context, userID int64) (storage.UserSettings, error) {
	settings, exists := f.settings[userID]
	if !exists {
		return storage.UserSettings{}, storage.ErrNotFound
	}
	return settings, nil
}

func (f *fakeTelegramRepository) UpdateUserSettings(_ context.Context, settings storage.UserSettings) error {
	f.settings[settings.UserID] = settings
	f.updates++
	return nil
}

func (f *fakeTelegramRepository) SetInstrumentPreference(context.Context, int64, string, string) error {
	return nil
}

func (f *fakeTelegramRepository) ClearInstrumentPreference(context.Context, int64, string) error {
	return nil
}

func (f *fakeTelegramRepository) ListInstrumentPreferences(context.Context, int64) ([]storage.InstrumentPreference, error) {
	return nil, nil
}

func (f *fakeTelegramRepository) ListUserSignals(context.Context, int64, storage.SignalFilters) ([]storage.Signal, error) {
	return nil, nil
}

func (f *fakeTelegramRepository) ClaimPendingDeliveriesForUsers(context.Context, int, time.Duration, []int64) ([]storage.Delivery, error) {
	return nil, nil
}

func (f *fakeTelegramRepository) MarkDeliverySent(context.Context, int64, int64) error { return nil }

func (f *fakeTelegramRepository) MarkDeliveryFailed(context.Context, int64, int64, time.Time, error) error {
	return nil
}

func newTestBot(t *testing.T) (*Bot, *fakeTelegramAPI, *fakeTelegramRepository) {
	t.Helper()
	api := &fakeTelegramAPI{}
	repository := newFakeTelegramRepository()
	bot, err := newWithAPI(api, []int64{1001}, repository, nil)
	if err != nil {
		t.Fatal(err)
	}
	return bot, api, repository
}

func TestNewRejectsEmptyTelegramAllowlist(t *testing.T) {
	api := &fakeTelegramAPI{}
	repository := newFakeTelegramRepository()
	if _, err := newWithAPI(api, nil, repository, nil); err == nil {
		t.Fatal("empty allowlist must be rejected")
	}
}

func TestNonAllowlistedUserCannotRegister(t *testing.T) {
	bot, api, repository := newTestBot(t)
	err := bot.handleUpdate(context.Background(), telegramUpdate{
		Message: &telegramMessage{
			Chat: telegramChat{ID: 2002, Type: "private"},
			From: &telegramUser{ID: 2002, FirstName: "Unknown"},
			Text: "/start",
		},
	})
	if err != nil {
		t.Fatalf("handleUpdate() error = %v", err)
	}
	if repository.ensureCalls != 0 || len(repository.users) != 0 {
		t.Fatalf("non-allowlisted user was registered: %+v", repository.users)
	}
	if len(api.sentMessages) != 1 || !containsText(api.sentMessages[0], "не разрешён") {
		t.Fatalf("unexpected deny response: %#v", api.sentMessages)
	}
}

func TestAllowedUserCanOpenMenuAndChangeSettings(t *testing.T) {
	bot, api, repository := newTestBot(t)
	err := bot.handleUpdate(context.Background(), telegramUpdate{
		Message: &telegramMessage{
			Chat: telegramChat{ID: 1001, Type: "private"},
			From: &telegramUser{ID: 1001, FirstName: "Authorized"},
			Text: "/start",
		},
	})
	if err != nil {
		t.Fatalf("start update: %v", err)
	}
	user := repository.users[1001]
	if !user.IsActive || user.ID == 0 || len(api.sentMessages) != 1 {
		t.Fatalf("authorized user was not registered and welcomed: user=%+v sent=%v", user, api.sentMessages)
	}

	err = bot.handleAction(context.Background(), 1001, 55, user.ID, "settings:long")
	if err != nil {
		t.Fatalf("change settings: %v", err)
	}
	settings := repository.settings[user.ID]
	if settings.NotifyLong || repository.updates != 1 {
		t.Fatalf("LONG setting was not persisted: %+v", settings)
	}
	if len(api.edited) != 1 || !containsText(api.edited[0], "Уведомления LONG: нет") {
		t.Fatalf("settings screen was not refreshed: %#v", api.edited)
	}
}

func TestInactiveUserIsNotAllowedToUseBot(t *testing.T) {
	bot, api, repository := newTestBot(t)
	repository.users[1001] = storage.TelegramUser{ID: 1, TelegramID: 1001, IsActive: false}
	err := bot.handleUpdate(context.Background(), telegramUpdate{
		Message: &telegramMessage{
			Chat: telegramChat{ID: 1001, Type: "private"},
			From: &telegramUser{ID: 1001},
			Text: "/start",
		},
	})
	if err != nil {
		t.Fatalf("inactive user update: %v", err)
	}
	if len(api.sentMessages) != 1 || !containsText(api.sentMessages[0], "отключён") {
		t.Fatalf("inactive account was not denied: %#v", api.sentMessages)
	}
}

func TestDeliveryRetryDelayIsBounded(t *testing.T) {
	if got := deliveryRetryDelay(1); got != 30*time.Second {
		t.Fatalf("first retry delay = %s", got)
	}
	if got := deliveryRetryDelay(20); got != time.Hour {
		t.Fatalf("maximum retry delay = %s, want 1h", got)
	}
}

func TestValidSymbol(t *testing.T) {
	for _, value := range []string{"BTCUSDT", "ETHUSDT", "1000PEPEUSDT"} {
		if !validSymbol(value) {
			t.Fatalf("valid symbol %q rejected", value)
		}
	}
	for _, value := range []string{"", "BTC-USDT", "BTC/USDT", "BTC USDT", "A"} {
		if validSymbol(value) {
			t.Fatalf("invalid symbol %q accepted", value)
		}
	}
}
