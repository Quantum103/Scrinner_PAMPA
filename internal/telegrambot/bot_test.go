package telegrambot

import (
	"context"
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

func (f *fakeTelegramAPI) deleteMessage(_ context.Context, _ int64, messageID int64) error {
	f.deleted = append(f.deleted, messageID)
	return nil
}

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

func (f *fakeTelegramRepository) ActivateTelegramUser(_ context.Context, telegramID int64) error {
	user, exists := f.users[telegramID]
	if !exists {
		return storage.ErrNotFound
	}
	user.IsActive = true
	f.users[telegramID] = user
	return nil
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

func (f *fakeTelegramRepository) ClaimPendingDeliveries(context.Context, int, time.Duration) ([]storage.Delivery, error) {
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
	bot, err := newWithAPI(api, "correct-horse-battery-staple", repository, nil)
	if err != nil {
		t.Fatal(err)
	}
	return bot, api, repository
}

func TestNewRejectsWeakTelegramAccessPassword(t *testing.T) {
	api := &fakeTelegramAPI{}
	repository := newFakeTelegramRepository()
	if _, err := newWithAPI(api, "short", repository, nil); err == nil {
		t.Fatal("short access password must be rejected")
	}
}

func TestPasswordAuthenticationDeletesPasswordMessageAndPersistsUser(t *testing.T) {
	bot, api, repository := newTestBot(t)
	message := &telegramMessage{
		MessageID: 10,
		Chat:      telegramChat{ID: 1001, Type: "private"},
		From:      &telegramUser{ID: 1001, FirstName: "Authorized"},
		Text:      "/start",
	}
	if err := bot.handleUpdate(context.Background(), telegramUpdate{Message: message}); err != nil {
		t.Fatalf("start update: %v", err)
	}
	if len(repository.users) != 0 || len(api.sentMessages) != 1 || !containsText(api.sentMessages[0], "отправьте общий пароль") {
		t.Fatalf("unauthenticated /start did not request password: users=%+v messages=%#v", repository.users, api.sentMessages)
	}

	message.MessageID = 11
	message.Text = "wrong-password"
	if err := bot.handleUpdate(context.Background(), telegramUpdate{Message: message}); err != nil {
		t.Fatalf("wrong password update: %v", err)
	}
	if len(repository.users) != 0 || len(api.deleted) != 1 || !containsText(api.sentMessages[len(api.sentMessages)-1], "Пароль неверный") {
		t.Fatalf("wrong password was not rejected and deleted: users=%+v deleted=%v messages=%#v", repository.users, api.deleted, api.sentMessages)
	}

	message.MessageID = 12
	message.Text = "correct-horse-battery-staple"
	if err := bot.handleUpdate(context.Background(), telegramUpdate{Message: message}); err != nil {
		t.Fatalf("correct password update: %v", err)
	}
	user, exists := repository.users[1001]
	if !exists || !user.IsActive || user.ID == 0 || len(api.deleted) != 2 || !containsText(api.sentMessages[len(api.sentMessages)-1], "Доступ открыт") {
		t.Fatalf("correct password did not authorize user: user=%+v exists=%v deleted=%v messages=%#v", user, exists, api.deleted, api.sentMessages)
	}
}

func TestPasswordAuthenticationLocksAfterFiveFailures(t *testing.T) {
	bot, api, repository := newTestBot(t)
	message := &telegramMessage{
		Chat: telegramChat{ID: 2002, Type: "private"},
		From: &telegramUser{ID: 2002},
		Text: "incorrect-password",
	}
	for attempt := 0; attempt < maxPasswordFailures; attempt++ {
		message.MessageID = int64(attempt + 1)
		if err := bot.handleUpdate(context.Background(), telegramUpdate{Message: message}); err != nil {
			t.Fatalf("failed password attempt %d: %v", attempt+1, err)
		}
	}
	message.Text = "correct-horse-battery-staple"
	message.MessageID = 10
	if err := bot.handleUpdate(context.Background(), telegramUpdate{Message: message}); err != nil {
		t.Fatalf("locked password attempt: %v", err)
	}
	if len(repository.users) != 0 || !containsText(api.sentMessages[len(api.sentMessages)-1], "Слишком много неверных попыток") {
		t.Fatalf("password lockout was not enforced: users=%+v last=%q", repository.users, api.sentMessages[len(api.sentMessages)-1])
	}
}

func TestInactiveUserMustAuthenticateAgain(t *testing.T) {
	bot, api, repository := newTestBot(t)
	repository.users[1001] = storage.TelegramUser{ID: 1, TelegramID: 1001, IsActive: false}
	message := &telegramMessage{
		MessageID: 7,
		Chat:      telegramChat{ID: 1001, Type: "private"},
		From:      &telegramUser{ID: 1001, FirstName: "Existing"},
		Text:      "correct-horse-battery-staple",
	}
	if err := bot.handleUpdate(context.Background(), telegramUpdate{Message: message}); err != nil {
		t.Fatalf("reauthentication update: %v", err)
	}
	if !repository.users[1001].IsActive || len(api.deleted) != 1 {
		t.Fatalf("inactive user was not reauthenticated: %+v", repository.users[1001])
	}
}

func TestAuthenticatedUserCanOpenMenuAndChangeSettings(t *testing.T) {
	bot, api, repository := newTestBot(t)
	message := &telegramMessage{MessageID: 1, Chat: telegramChat{ID: 1001, Type: "private"}, From: &telegramUser{ID: 1001, FirstName: "Authorized"}, Text: "correct-horse-battery-staple"}
	if err := bot.handleUpdate(context.Background(), telegramUpdate{Message: message}); err != nil {
		t.Fatalf("password update: %v", err)
	}
	user := repository.users[1001]
	if !user.IsActive || user.ID == 0 || len(api.sentMessages) != 1 {
		t.Fatalf("authorized user was not registered and welcomed: user=%+v sent=%v", user, api.sentMessages)
	}
	if err := bot.handleAction(context.Background(), 1001, 55, user.ID, "settings:long"); err != nil {
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

func TestAllowedUserCanSelectOnly30MinuteTimeframe(t *testing.T) {
	bot, api, repository := newTestBot(t)
	repository.settings[1] = storage.UserSettings{
		UserID: 1, RSILength: 14, EMALength: 9, InstrumentLimit: 100,
		NotifyLong: true, NotifyShort: true,
		Timeframes: []string{"5", "15", "60"},
	}

	for _, action := range []string{"tf:30", "tf:5", "tf:15", "tf:60"} {
		if err := bot.changeSettings(context.Background(), 1001, 55, 1, action); err != nil {
			t.Fatalf("changeSettings(%q): %v", action, err)
		}
	}
	settings := repository.settings[1]
	if len(settings.Timeframes) != 1 || settings.Timeframes[0] != "30" {
		t.Fatalf("timeframes = %#v, want only 30m", settings.Timeframes)
	}
	if len(api.edited) != 4 || !containsText(api.edited[len(api.edited)-1], "Таймфреймы: 30m") {
		t.Fatalf("settings screen did not reflect 30m-only selection: %#v", api.edited)
	}

	keyboard := settingsKeyboard(settings)
	found := false
	for _, row := range keyboard.InlineKeyboard {
		for _, button := range row {
			if button.CallbackData == "settings:tf:30" && button.Text == "✅ 30m" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("settings keyboard does not show enabled 30m option")
	}
}

func TestFormatSignalIncludesSidewaysMarketContext(t *testing.T) {
	message := formatSignal(storage.Signal{
		Symbol: "BTCUSDT", Timeframe: "30", Direction: "LONG",
		ClosePrice: 65000, RSI: 32.5, EMARSI: 30.25,
		MarketRegime: "SIDEWAYS",
		MarketADX:    18.12, MarketADXReady: true,
		MarketCHOP: 64.21, MarketCHOPReady: true,
		MarketEMA: 64950.75, MarketEMAReady: true,
	})

	for _, expected := range []string{
		"Рынок: ⚠️ БОКОВОЕ ДВИЖЕНИЕ",
		"⚠️ Повышенный риск ложных сигналов в боковике.",
		"ADX(14): 18.12",
		"CHOP(14): 64.21",
		"EMA(50): 64950.75",
	} {
		if !strings.Contains(message, expected) {
			t.Errorf("formatted signal does not contain %q: %s", expected, message)
		}
	}
}

func TestFormatSignalShowsOnlyReadyMarketMetrics(t *testing.T) {
	message := formatSignal(storage.Signal{
		Symbol: "ETHUSDT", Timeframe: "30", Direction: "SHORT",
		MarketRegime: "TRANSITIONAL",
		MarketADX:    21.5, MarketADXReady: true,
	})

	for _, expected := range []string{
		"Рынок: 🔄 ПЕРЕХОДНОЕ СОСТОЯНИЕ",
		"ADX(14): 21.50",
		"CHOP(14): —",
		"EMA(50): —",
	} {
		if !strings.Contains(message, expected) {
			t.Errorf("formatted signal does not contain %q: %s", expected, message)
		}
	}
	if strings.Contains(message, "Недостаточно истории") {
		t.Fatalf("warmup warning should not replace available metrics: %s", message)
	}
}

func TestFormatSignalHandlesUnknownMarketContextWithoutInventingValues(t *testing.T) {
	message := formatSignal(storage.Signal{
		Symbol: "XRPUSDT", Timeframe: "30", Direction: "LONG",
		MarketRegime: "UNKNOWN",
	})

	for _, expected := range []string{
		"Рынок: ❔ НЕДОСТАТОЧНО ДАННЫХ",
		"ADX(14): —",
		"CHOP(14): —",
		"EMA(50): —",
		"Недостаточно истории для расчёта индикаторов.",
	} {
		if !strings.Contains(message, expected) {
			t.Errorf("formatted signal does not contain %q: %s", expected, message)
		}
	}
}

func TestFormatDeliveryPreservesSidewaysMarketContext(t *testing.T) {
	message := formatDelivery(storage.Delivery{
		Symbol: "BTCUSDT", Timeframe: "30", Direction: "SHORT",
		MarketRegime: "SIDEWAYS",
		MarketADX:    16.75, MarketADXReady: true,
		MarketCHOP: 70.1, MarketCHOPReady: true,
		MarketEMA: 65100, MarketEMAReady: true,
	})

	for _, expected := range []string{
		"Новый RSI-сигнал",
		"Рынок: ⚠️ БОКОВОЕ ДВИЖЕНИЕ",
		"ADX(14): 16.75",
		"CHOP(14): 70.10",
		"EMA(50): 65100",
	} {
		if !strings.Contains(message, expected) {
			t.Errorf("formatted delivery does not contain %q: %s", expected, message)
		}
	}
}

func TestFormatSignalShowsHigherTimeframeTrendAboveMarketContext(t *testing.T) {
	message := formatSignal(storage.Signal{
		Symbol: "BTCUSDT", Timeframe: "30", Direction: "LONG",
		ClosePrice: 65000, RSI: 32.5, EMARSI: 30.25,
		HigherTimeframe: "240", HigherMarketRegime: "UPTREND",
		MarketRegime: "TRANSITIONAL",
	})
	for _, expected := range []string{
		"Тренд старшего ТФ (4h): 📈 ВОСХОДЯЩИЙ ТРЕНД",
		"Рынок: 🔄 ПЕРЕХОДНОЕ СОСТОЯНИЕ",
	} {
		if !strings.Contains(message, expected) {
			t.Errorf("formatted signal does not contain %q: %s", expected, message)
		}
	}
	if strings.Index(message, "Тренд старшего ТФ") > strings.Index(message, "Рынок:") {
		t.Fatalf("higher timeframe trend should appear above lower-timeframe context: %s", message)
	}
}

func TestFormatDeliveryPreservesHigherTimeframeTrend(t *testing.T) {
	message := formatDelivery(storage.Delivery{
		Symbol: "ETHUSDT", Timeframe: "30", Direction: "SHORT",
		HigherTimeframe: "240", HigherMarketRegime: "DOWNTREND",
	})
	if !strings.Contains(message, "Тренд старшего ТФ (4h): 📉 НИСХОДЯЩИЙ ТРЕНД") {
		t.Fatalf("formatted delivery lost higher-timeframe trend: %s", message)
	}
}
