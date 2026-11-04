package telegrambot

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
)

const (
	pollTimeoutSeconds    = 25
	deliveryBatchSize     = 50
	deliveryLease         = 2 * time.Minute
	maxPasswordFailures   = 5
	passwordFailureWindow = 10 * time.Minute
	passwordLockout       = 15 * time.Minute
)

type Repository interface {
	EnsureTelegramUser(context.Context, storage.TelegramUser) (storage.TelegramUser, error)
	ActivateTelegramUser(context.Context, int64) error
	GetUserByTelegramID(context.Context, int64) (storage.TelegramUser, error)
	GetOrCreateUserSettings(context.Context, int64) (storage.UserSettings, error)
	UpdateUserSettings(context.Context, storage.UserSettings) error
	SetInstrumentPreference(context.Context, int64, string, string) error
	ClearInstrumentPreference(context.Context, int64, string) error
	ListInstrumentPreferences(context.Context, int64) ([]storage.InstrumentPreference, error)
	ListUserSignals(context.Context, int64, storage.SignalFilters) ([]storage.Signal, error)
	ClaimPendingDeliveries(context.Context, int, time.Duration) ([]storage.Delivery, error)
	MarkDeliverySent(context.Context, int64, int64) error
	MarkDeliveryFailed(context.Context, int64, int64, time.Time, error) error
}

type telegramAPI interface {
	getUpdates(context.Context, int64, int64) ([]telegramUpdate, error)
	sendMessage(context.Context, int64, string, *inlineKeyboard) error
	editMessageText(context.Context, int64, int64, string, *inlineKeyboard) error
	answerCallbackQuery(context.Context, string, string) error
	deleteWebhook(context.Context) error
	deleteMessage(context.Context, int64, int64) error
	getMe(context.Context) (telegramUser, error)
	setMyCommands(context.Context) error
}

type passwordAttempt struct {
	failures   int
	windowFrom time.Time
	lockedTill time.Time
}

type Bot struct {
	api            telegramAPI
	repository     Repository
	logger         *slog.Logger
	accessPassword string
	authMu         sync.Mutex
	authAttempts   map[int64]passwordAttempt
}

func New(token, accessPassword string, repository Repository, logger *slog.Logger) (*Bot, error) {
	client, err := newAPIClient(token, nil, "")
	if err != nil {
		return nil, err
	}
	return newWithAPI(client, accessPassword, repository, logger)
}

func newWithAPI(api telegramAPI, accessPassword string, repository Repository, logger *slog.Logger) (*Bot, error) {
	if api == nil {
		return nil, errors.New("Telegram API client is required")
	}
	if repository == nil {
		return nil, errors.New("Telegram repository is required")
	}
	accessPassword = strings.TrimSpace(accessPassword)
	if len([]rune(accessPassword)) < 12 {
		return nil, errors.New("Telegram access password must contain at least 12 characters")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Bot{api: api, repository: repository, logger: logger, accessPassword: accessPassword, authAttempts: make(map[int64]passwordAttempt)}, nil
}

// Run starts long polling and the persistent signal-delivery worker.
func (b *Bot) Run(ctx context.Context) error {
	if b == nil {
		return nil
	}
	var me telegramUser
	for {
		setupCtx, setupCancel := context.WithTimeout(ctx, 10*time.Second)
		err := b.api.deleteWebhook(setupCtx)
		if err == nil {
			me, err = b.api.getMe(setupCtx)
		}
		if err == nil {
			if commandErr := b.api.setMyCommands(setupCtx); commandErr != nil && ctx.Err() == nil {
				b.logger.Warn("Telegram bot command registration failed", "error", commandErr)
			}
		}
		setupCancel()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return nil
		}
		b.logger.Error("Telegram bot initialization failed; retry scheduled", "error", err)
		if !wait(ctx, 10*time.Second) {
			return nil
		}
	}
	b.logger.Info("Telegram bot identity verified", "username", me.Username, "telegram_id", me.ID)

	go b.runDeliveryWorker(ctx)
	b.logger.Info("Telegram bot polling started", "access", "shared-password")

	var offset int64
	for {
		if ctx.Err() != nil {
			return nil
		}
		updates, err := b.api.getUpdates(ctx, offset, pollTimeoutSeconds)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			b.logger.Error("Telegram getUpdates failed", "error", err)
			if !wait(ctx, 3*time.Second) {
				return nil
			}
			continue
		}
		for _, update := range updates {
			if update.UpdateID >= offset {
				offset = update.UpdateID + 1
			}
			if err := b.handleUpdate(ctx, update); err != nil {
				b.logger.Error("Telegram update processing failed",
					"update_id", update.UpdateID, "error", err)
			}
		}
	}
}

func (b *Bot) handleUpdate(ctx context.Context, update telegramUpdate) error {
	switch {
	case update.Message != nil:
		return b.handleMessage(ctx, update.Message)
	case update.CallbackQuery != nil:
		return b.handleCallback(ctx, update.CallbackQuery)
	default:
		return nil
	}
}

func (b *Bot) handleMessage(ctx context.Context, message *telegramMessage) error {
	if message == nil || message.From == nil || message.Chat.Type != "private" {
		return nil
	}
	user, err := b.repository.GetUserByTelegramID(ctx, message.From.ID)
	if errors.Is(err, storage.ErrNotFound) || (err == nil && !user.IsActive) {
		return b.handlePasswordAuthentication(ctx, message)
	}
	if err != nil {
		return fmt.Errorf("load Telegram authorization: %w", err)
	}

	user, err = b.repository.EnsureTelegramUser(ctx, storage.TelegramUser{
		TelegramID: message.From.ID, Username: message.From.Username,
		FirstName: message.From.FirstName, LastName: message.From.LastName,
		LanguageCode: message.From.LanguageCode,
	})
	if err != nil {
		return fmt.Errorf("refresh Telegram user profile: %w", err)
	}
	if !user.IsActive {
		return b.api.sendMessage(ctx, message.Chat.ID, authPromptText, nil)
	}

	fields := strings.Fields(strings.TrimSpace(message.Text))
	if len(fields) == 0 {
		return b.sendMenu(ctx, message.Chat.ID, "Главное меню RSI Scanner")
	}
	command := strings.ToLower(strings.TrimPrefix(fields[0], "/"))
	if at := strings.IndexByte(command, '@'); at >= 0 {
		command = command[:at]
	}
	arguments := fields[1:]

	switch command {
	case "start", "menu":
		return b.sendMenu(ctx, message.Chat.ID,
			"Привет, "+displayName(user)+". Это RSI Scanner для Bybit USDT Perpetual.")
	case "signals":
		return b.showHistory(ctx, message.Chat.ID, user.ID, 0, 0)
	case "history":
		return b.showHistory(ctx, message.Chat.ID, user.ID, 0, 0)
	case "settings":
		return b.showSettingsWithUser(ctx, message.Chat.ID, 0, user.ID, "")
	case "instruments":
		return b.showPreferences(ctx, message.Chat.ID, user.ID, 0, false)
	case "include", "exclude":
		if len(arguments) != 1 || !validSymbol(arguments[0]) {
			return b.api.sendMessage(ctx, message.Chat.ID,
				"Формат команды: /"+command+" BTCUSDT", nil)
		}
		preference := "include"
		if command == "exclude" {
			preference = "exclude"
		}
		if err := b.repository.SetInstrumentPreference(ctx, user.ID, arguments[0], preference); err != nil {
			return fmt.Errorf("save instrument preference: %w", err)
		}
		return b.api.sendMessage(ctx, message.Chat.ID,
			fmt.Sprintf("%s добавлен в список %s.", strings.ToUpper(arguments[0]), preferenceLabel(preference)), nil)
	case "reset":
		if len(arguments) != 1 || !validSymbol(arguments[0]) {
			return b.api.sendMessage(ctx, message.Chat.ID, "Формат команды: /reset BTCUSDT", nil)
		}
		if err := b.repository.ClearInstrumentPreference(ctx, user.ID, arguments[0]); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return b.api.sendMessage(ctx, message.Chat.ID, "Для этого инструмента нет ручного правила.", nil)
			}
			return fmt.Errorf("clear instrument preference: %w", err)
		}
		return b.api.sendMessage(ctx, message.Chat.ID,
			strings.ToUpper(arguments[0])+" возвращён к общим фильтрам.", nil)
	case "help":
		return b.api.sendMessage(ctx, message.Chat.ID, helpText, mainMenuKeyboard())
	default:
		return b.api.sendMessage(ctx, message.Chat.ID,
			"Используйте кнопки меню или команду /help.", mainMenuKeyboard())
	}
}

func (b *Bot) handlePasswordAuthentication(ctx context.Context, message *telegramMessage) error {
	password := strings.TrimSpace(message.Text)
	if password == "" || strings.HasPrefix(password, "/") {
		return b.api.sendMessage(ctx, message.Chat.ID, authPromptText, nil)
	}

	deleteCtx, deleteCancel := context.WithTimeout(ctx, 3*time.Second)
	if err := b.api.deleteMessage(deleteCtx, message.Chat.ID, message.MessageID); err != nil && ctx.Err() == nil {
		// Never include the supplied text or password in logs.
		b.logger.Warn("Telegram password message could not be deleted", "telegram_id", message.From.ID, "error", err)
	}
	deleteCancel()

	if retryAfter := b.passwordRetryAfter(message.From.ID); retryAfter > 0 {
		return b.api.sendMessage(ctx, message.Chat.ID, fmt.Sprintf("Слишком много неверных попыток. Повторите через %s.", retryAfter.Round(time.Minute)), nil)
	}

	if subtle.ConstantTimeCompare([]byte(password), []byte(b.accessPassword)) != 1 {
		if retryAfter := b.recordPasswordFailure(message.From.ID); retryAfter > 0 {
			return b.api.sendMessage(ctx, message.Chat.ID, fmt.Sprintf("Слишком много неверных попыток. Доступ к вводу пароля закрыт на %s.", retryAfter.Round(time.Minute)), nil)
		}
		return b.api.sendMessage(ctx, message.Chat.ID, "Пароль неверный. Отправьте правильный пароль отдельным сообщением.", nil)
	}

	b.clearPasswordFailures(message.From.ID)
	user, err := b.repository.EnsureTelegramUser(ctx, storage.TelegramUser{
		TelegramID: message.From.ID, Username: message.From.Username,
		FirstName: message.From.FirstName, LastName: message.From.LastName,
		LanguageCode: message.From.LanguageCode,
	})
	if err != nil {
		return fmt.Errorf("register Telegram user after password authentication: %w", err)
	}
	if !user.IsActive {
		if err := b.repository.ActivateTelegramUser(ctx, user.TelegramID); err != nil {
			return fmt.Errorf("activate Telegram user after password authentication: %w", err)
		}
		user.IsActive = true
	}
	return b.sendMenu(ctx, message.Chat.ID, "Пароль принят. Доступ открыт. Привет, "+displayName(user)+"!")
}

func (b *Bot) passwordRetryAfter(telegramID int64) time.Duration {
	b.authMu.Lock()
	defer b.authMu.Unlock()
	state, ok := b.authAttempts[telegramID]
	if !ok {
		return 0
	}
	now := time.Now()
	if !state.lockedTill.IsZero() && now.Before(state.lockedTill) {
		return time.Until(state.lockedTill)
	}
	if !state.lockedTill.IsZero() || (!state.windowFrom.IsZero() && now.Sub(state.windowFrom) >= passwordFailureWindow) {
		delete(b.authAttempts, telegramID)
	}
	return 0
}

func (b *Bot) recordPasswordFailure(telegramID int64) time.Duration {
	b.authMu.Lock()
	defer b.authMu.Unlock()
	now := time.Now()
	state := b.authAttempts[telegramID]
	if state.windowFrom.IsZero() || now.Sub(state.windowFrom) >= passwordFailureWindow {
		state = passwordAttempt{windowFrom: now}
	}
	state.failures++
	if state.failures >= maxPasswordFailures {
		state.lockedTill = now.Add(passwordLockout)
		state.failures = 0
	}
	b.authAttempts[telegramID] = state
	if !state.lockedTill.IsZero() {
		return time.Until(state.lockedTill)
	}
	return 0
}

func (b *Bot) clearPasswordFailures(telegramID int64) {
	b.authMu.Lock()
	delete(b.authAttempts, telegramID)
	b.authMu.Unlock()
}

func (b *Bot) handleCallback(ctx context.Context, callback *telegramCallback) error {
	if callback == nil {
		return nil
	}
	if err := b.api.answerCallbackQuery(ctx, callback.ID, ""); err != nil {
		b.logger.Warn("answer Telegram callback failed", "error", err)
	}
	if callback.From == nil || callback.Message == nil || callback.Message.Chat.Type != "private" {
		return nil
	}
	user, err := b.repository.GetUserByTelegramID(ctx, callback.From.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return b.api.sendMessage(ctx, callback.Message.Chat.ID, authPromptText, nil)
		}
		return fmt.Errorf("load Telegram user for callback: %w", err)
	}
	if !user.IsActive {
		return b.api.sendMessage(ctx, callback.Message.Chat.ID, authPromptText, nil)
	}
	return b.handleAction(ctx, callback.Message.Chat.ID, callback.Message.MessageID, user.ID, callback.Data)
}

func (b *Bot) handleAction(ctx context.Context, chatID, messageID, userID int64, action string) error {
	switch {
	case action == "menu":
		return b.render(ctx, chatID, messageID, "Главное меню RSI Scanner", mainMenuKeyboard())
	case action == "help":
		return b.render(ctx, chatID, messageID, helpText, mainMenuKeyboard())
	case action == "signals":
		return b.showHistory(ctx, chatID, userID, 0, messageID)
	case action == "history":
		return b.showHistory(ctx, chatID, userID, 0, messageID)
	case strings.HasPrefix(action, "history:"):
		offset, err := strconv.Atoi(strings.TrimPrefix(action, "history:"))
		if err != nil || offset < 0 || offset > 10000 {
			return errors.New("invalid Telegram history page")
		}
		return b.showHistory(ctx, chatID, userID, offset, messageID)
	case action == "settings":
		return b.showSettingsWithUser(ctx, chatID, messageID, userID, "")
	case action == "instruments":
		return b.showPreferences(ctx, chatID, userID, messageID, true)
	case strings.HasPrefix(action, "settings:"):
		change := strings.TrimPrefix(action, "settings:")
		return b.changeSettings(ctx, chatID, messageID, userID, change)
	default:
		return b.render(ctx, chatID, messageID, "Неизвестное действие. Откройте меню.", mainMenuKeyboard())
	}
}

func (b *Bot) sendMenu(ctx context.Context, chatID int64, message string) error {
	return b.api.sendMessage(ctx, chatID, message, mainMenuKeyboard())
}

func mainMenuKeyboard() *inlineKeyboard {
	return &inlineKeyboard{InlineKeyboard: [][]inlineButton{
		{{Text: "📈 Последние сигналы", CallbackData: "signals"}, {Text: "📚 История", CallbackData: "history"}},
		{{Text: "⚙️ Настройки", CallbackData: "settings"}, {Text: "🎛 Инструменты", CallbackData: "instruments"}},
		{{Text: "ℹ️ Справка", CallbackData: "help"}},
	}}
}

func (b *Bot) showHistory(ctx context.Context, chatID, userID int64, offset int, messageID int64) error {
	signals, err := b.repository.ListUserSignals(ctx, userID, storage.SignalFilters{Limit: 10, Offset: offset})
	if err != nil {
		return fmt.Errorf("load Telegram signal history: %w", err)
	}
	var message strings.Builder
	if len(signals) == 0 {
		message.WriteString("Пока нет сигналов, подходящих под ваши настройки.")
	} else {
		if offset == 0 {
			message.WriteString("Последние сигналы\n\n")
		} else {
			fmt.Fprintf(&message, "История сигналов · страница %d\n\n", offset/10+1)
		}
		for index, signal := range signals {
			fmt.Fprintf(&message, "%s\n", formatSignal(signal))
			if index+1 < len(signals) {
				message.WriteByte('\n')
			}
		}
	}
	keyboard := historyKeyboard(offset, len(signals) == 10)
	keyboard.InlineKeyboard = append(keyboard.InlineKeyboard, []inlineButton{{Text: "⬅️ Меню", CallbackData: "menu"}})
	return b.render(ctx, chatID, messageID, message.String(), keyboard)
}

func historyKeyboard(offset int, hasNext bool) *inlineKeyboard {
	buttons := make([]inlineButton, 0, 2)
	if offset > 0 {
		previous := offset - 10
		if previous < 0 {
			previous = 0
		}
		buttons = append(buttons, inlineButton{Text: "← Новее", CallbackData: fmt.Sprintf("history:%d", previous)})
	}
	if hasNext {
		buttons = append(buttons, inlineButton{Text: "Старее →", CallbackData: fmt.Sprintf("history:%d", offset+10)})
	}
	if len(buttons) == 0 {
		return &inlineKeyboard{}
	}
	return &inlineKeyboard{InlineKeyboard: [][]inlineButton{buttons}}
}

func (b *Bot) showSettingsWithUser(ctx context.Context, chatID, messageID, userID int64, notice string) error {
	settings, err := b.repository.GetOrCreateUserSettings(ctx, userID)
	if err != nil {
		return fmt.Errorf("load Telegram user settings: %w", err)
	}
	message := formatSettings(settings)
	if notice != "" {
		message = notice + "\n\n" + message
	}
	return b.render(ctx, chatID, messageID, message, settingsKeyboard(settings))
}

func settingsKeyboard(settings storage.UserSettings) *inlineKeyboard {
	timeframeButtons := make([]inlineButton, 0, 4)
	for _, timeframe := range []string{"5", "15", "30", "60"} {
		label := timeframe + "m"
		if containsTimeframe(settings.Timeframes, timeframe) {
			label = "✅ " + label
		}
		timeframeButtons = append(timeframeButtons, inlineButton{Text: label, CallbackData: "settings:tf:" + timeframe})
	}
	allLabel := "Все в выборке: выкл"
	if settings.ScanAllInstruments {
		allLabel = "Все в выборке: вкл"
	}
	longLabel, shortLabel := "LONG: выкл", "SHORT: выкл"
	if settings.NotifyLong {
		longLabel = "LONG: вкл"
	}
	if settings.NotifyShort {
		shortLabel = "SHORT: вкл"
	}
	return &inlineKeyboard{InlineKeyboard: [][]inlineButton{
		{{Text: "RSI −", CallbackData: "settings:rsi:-"}, {Text: "RSI +", CallbackData: "settings:rsi:+"}},
		{{Text: "EMA −", CallbackData: "settings:ema:-"}, {Text: "EMA +", CallbackData: "settings:ema:+"}},
		{{Text: "Лимит −", CallbackData: "settings:limit:-"}, {Text: "Лимит +", CallbackData: "settings:limit:+"}},
		{{Text: allLabel, CallbackData: "settings:all"}},
		{{Text: longLabel, CallbackData: "settings:long"}, {Text: shortLabel, CallbackData: "settings:short"}},
		timeframeButtons,
		{{Text: "⬅️ Меню", CallbackData: "menu"}},
	}}
}

func formatSettings(settings storage.UserSettings) string {
	timeframes := "не выбраны"
	if len(settings.Timeframes) > 0 {
		labels := make([]string, 0, len(settings.Timeframes))
		for _, timeframe := range settings.Timeframes {
			labels = append(labels, timeframe+"m")
		}
		timeframes = strings.Join(labels, ", ")
	}
	all := "нет"
	if settings.ScanAllInstruments {
		all = "да"
	}
	long, short := "нет", "нет"
	if settings.NotifyLong {
		long = "да"
	}
	if settings.NotifyShort {
		short = "да"
	}
	return fmt.Sprintf(
		"Настройки\nRSI: %d · EMA RSI: %d\nЛимит инструментов пользователя: %d\nВсе инструменты в текущей выборке: %s\nУведомления LONG: %s\nУведомления SHORT: %s\nТаймфреймы: %s\n\nВажно: расчёт RSI/EMA пока использует общие параметры сканера. Персональные RSI/EMA и расширение общей выборки будут подключены в следующем этапе.",
		settings.RSILength, settings.EMALength, settings.InstrumentLimit, all, long, short, timeframes,
	)
}

func (b *Bot) changeSettings(ctx context.Context, chatID, messageID, userID int64, change string) error {
	settings, err := b.repository.GetOrCreateUserSettings(ctx, userID)
	if err != nil {
		return fmt.Errorf("load settings before update: %w", err)
	}
	notice := ""
	switch change {
	case "rsi:-":
		settings.RSILength--
	case "rsi:+":
		settings.RSILength++
	case "ema:-":
		settings.EMALength--
	case "ema:+":
		settings.EMALength++
	case "limit:-":
		settings.InstrumentLimit -= 25
	case "limit:+":
		settings.InstrumentLimit += 25
	case "all":
		settings.ScanAllInstruments = !settings.ScanAllInstruments
	case "long":
		settings.NotifyLong = !settings.NotifyLong
	case "short":
		settings.NotifyShort = !settings.NotifyShort
	case "tf:5", "tf:15", "tf:30", "tf:60":
		timeframe := strings.TrimPrefix(change, "tf:")
		if containsTimeframe(settings.Timeframes, timeframe) {
			filtered := make([]string, 0, len(settings.Timeframes)-1)
			for _, current := range settings.Timeframes {
				if current != timeframe {
					filtered = append(filtered, current)
				}
			}
			if len(filtered) == 0 {
				notice = "Нельзя отключить последний таймфрейм."
			} else {
				settings.Timeframes = filtered
			}
		} else {
			settings.Timeframes = append(settings.Timeframes, timeframe)
		}
	default:
		return errors.New("unknown Telegram settings action")
	}
	if settings.RSILength < 2 {
		settings.RSILength = 2
		notice = "RSI не может быть меньше 2."
	}
	if settings.RSILength > 500 {
		settings.RSILength = 500
		notice = "RSI не может быть больше 500."
	}
	if settings.EMALength < 1 {
		settings.EMALength = 1
		notice = "EMA не может быть меньше 1."
	}
	if settings.EMALength > 500 {
		settings.EMALength = 500
		notice = "EMA не может быть больше 500."
	}
	if settings.RSILength+settings.EMALength > 1000 {
		switch change {
		case "rsi:+":
			settings.RSILength = 1000 - settings.EMALength
		case "ema:+":
			settings.EMALength = 1000 - settings.RSILength
		}
		notice = "Сумма периодов RSI и EMA не может превышать 1000 свечей."
	}
	if settings.InstrumentLimit < 1 {
		settings.InstrumentLimit = 1
		notice = "Минимальный лимит — 1 инструмент."
	}
	if settings.InstrumentLimit > 10000 {
		settings.InstrumentLimit = 10000
		notice = "Максимальный лимит — 10000 инструментов."
	}
	if err := b.repository.UpdateUserSettings(ctx, settings); err != nil {
		return fmt.Errorf("save Telegram settings: %w", err)
	}
	return b.showSettingsWithUser(ctx, chatID, messageID, userID, notice)
}

func (b *Bot) showPreferences(ctx context.Context, chatID, userID, messageID int64, callback bool) error {
	preferences, err := b.repository.ListInstrumentPreferences(ctx, userID)
	if err != nil {
		return fmt.Errorf("load instrument preferences: %w", err)
	}
	var message strings.Builder
	message.WriteString("Ручные правила инструментов\n")
	if len(preferences) == 0 {
		message.WriteString("Пока нет ручных включений или исключений.")
	} else {
		shown := len(preferences)
		if shown > 25 {
			shown = 25
		}
		for _, preference := range preferences[:shown] {
			fmt.Fprintf(&message, "\n%s — %s", preference.Symbol, preferenceLabel(preference.Preference))
		}
		if len(preferences) > shown {
			fmt.Fprintf(&message, "\n… и ещё %d правил", len(preferences)-shown)
		}
	}
	message.WriteString("\n\nКоманды:\n/include BTCUSDT — включить инструмент\n/exclude BTCUSDT — исключить\n/reset BTCUSDT — удалить ручное правило")
	keyboard := &inlineKeyboard{InlineKeyboard: [][]inlineButton{{{Text: "⬅️ Меню", CallbackData: "menu"}}}}
	return b.render(ctx, chatID, conditionalMessageID(callback, messageID), message.String(), keyboard)
}

func conditionalMessageID(callback bool, messageID int64) int64 {
	if callback {
		return messageID
	}
	return 0
}

func (b *Bot) render(ctx context.Context, chatID, messageID int64, message string, keyboard *inlineKeyboard) error {
	if messageID > 0 {
		err := b.api.editMessageText(ctx, chatID, messageID, message, keyboard)
		if err != nil && strings.Contains(strings.ToLower(err.Error()), "message is not modified") {
			return nil
		}
		return err
	}
	return b.api.sendMessage(ctx, chatID, message, keyboard)
}

func (b *Bot) runDeliveryWorker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		deliveries, err := b.repository.ClaimPendingDeliveries(ctx, deliveryBatchSize, deliveryLease)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			b.logger.Error("claim Telegram signal deliveries failed", "error", err)
		} else {
			for _, delivery := range deliveries {
				if ctx.Err() != nil {
					return
				}
				b.deliverSignal(ctx, delivery)
				if !wait(ctx, 50*time.Millisecond) {
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (b *Bot) deliverSignal(ctx context.Context, delivery storage.Delivery) {
	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err := b.api.sendMessage(sendCtx, delivery.TelegramID, formatDelivery(delivery), nil)
	cancel()

	persistCtx, persistCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer persistCancel()
	if err == nil {
		if markErr := b.repository.MarkDeliverySent(persistCtx, delivery.UserID, delivery.SignalID); markErr != nil {
			b.logger.Error("mark Telegram delivery as sent failed",
				"user_id", delivery.UserID, "signal_id", delivery.SignalID, "error", markErr)
		}
		return
	}
	nextAttempt := time.Now().Add(deliveryRetryDelay(delivery.AttemptCount))
	if markErr := b.repository.MarkDeliveryFailed(persistCtx, delivery.UserID, delivery.SignalID, nextAttempt, err); markErr != nil {
		b.logger.Error("mark Telegram delivery as failed",
			"user_id", delivery.UserID, "signal_id", delivery.SignalID,
			"send_error", err, "storage_error", markErr)
		return
	}
	b.logger.Warn("Telegram signal delivery failed; retry scheduled",
		"user_id", delivery.UserID, "signal_id", delivery.SignalID,
		"attempt", delivery.AttemptCount, "retry_at", nextAttempt, "error", err)
}

func deliveryRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := 30 * time.Second
	for index := 1; index < attempt && delay < time.Hour; index++ {
		delay *= 2
	}
	if delay > time.Hour {
		return time.Hour
	}
	return delay
}

func formatSignal(signal storage.Signal) string {
	rsiLength, emaLength := signal.RSILength, signal.EMALength
	if rsiLength == 0 {
		rsiLength = 14
	}
	if emaLength == 0 {
		emaLength = 9
	}
	direction := "🟢 LONG"
	if signal.Direction == "SHORT" {
		direction = "🔴 SHORT"
	}
	message := fmt.Sprintf("%s · %s\n%s\nЦена закрытия: %s\nRSI(%d): %.2f · EMA RSI(%d): %.2f\nСвеча закрылась: %s UTC",
		signal.Timeframe+"m", direction, signal.Symbol, formatPrice(signal.ClosePrice),
		rsiLength, signal.RSI, emaLength, signal.EMARSI, signal.CandleClose.UTC().Format("2006-01-02 15:04"))
	return message + formatMarketContext(signal)
}

func formatMarketContext(signal storage.Signal) string {
	regime := strings.ToUpper(strings.TrimSpace(signal.MarketRegime))
	regimeLabel := "❔ НЕДОСТАТОЧНО ДАННЫХ"
	warning := ""

	switch regime {
	case "SIDEWAYS":
		regimeLabel = "⚠️ БОКОВОЕ ДВИЖЕНИЕ"
		warning = "\n⚠️ Повышенный риск ложных сигналов в боковике."
	case "UPTREND":
		regimeLabel = "📈 ВОСХОДЯЩИЙ ТРЕНД"
	case "DOWNTREND":
		regimeLabel = "📉 НИСХОДЯЩИЙ ТРЕНД"
	case "TRANSITIONAL":
		regimeLabel = "🔄 ПЕРЕХОДНОЕ СОСТОЯНИЕ"
	}

	adx, chop, ema := "—", "—", "—"
	if signal.MarketADXReady {
		adx = fmt.Sprintf("%.2f", signal.MarketADX)
	}
	if signal.MarketCHOPReady {
		chop = fmt.Sprintf("%.2f", signal.MarketCHOP)
	}
	if signal.MarketEMAReady {
		ema = formatPrice(signal.MarketEMA)
	}

	message := fmt.Sprintf("\n\nРынок: %s%s\nADX(14): %s · CHOP(14): %s · EMA(50): %s",
		regimeLabel, warning, adx, chop, ema)
	if !signal.MarketADXReady && !signal.MarketCHOPReady && !signal.MarketEMAReady {
		message += "\nНедостаточно истории для расчёта индикаторов."
	}
	return message
}

func formatDelivery(delivery storage.Delivery) string {
	return "Новый RSI-сигнал\n\n" + formatSignal(storage.Signal{
		Symbol:          delivery.Symbol,
		Timeframe:       delivery.Timeframe,
		Direction:       delivery.Direction,
		CandleStart:     delivery.CandleStart,
		CandleClose:     delivery.CandleClose,
		ClosePrice:      delivery.ClosePrice,
		RSI:             delivery.RSI,
		EMARSI:          delivery.EMARSI,
		RSILength:       delivery.RSILength,
		EMALength:       delivery.EMALength,
		MarketRegime:    delivery.MarketRegime,
		MarketADX:       delivery.MarketADX,
		MarketADXReady:  delivery.MarketADXReady,
		MarketCHOP:      delivery.MarketCHOP,
		MarketCHOPReady: delivery.MarketCHOPReady,
		MarketEMA:       delivery.MarketEMA,
		MarketEMAReady:  delivery.MarketEMAReady,
		DetectedAt:      delivery.DetectedAt,
	})
}

func formatPrice(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func displayName(user storage.TelegramUser) string {
	name := strings.TrimSpace(user.FirstName + " " + user.LastName)
	if name == "" {
		name = user.Username
	}
	if name == "" {
		return "трейдер"
	}
	return name
}

func preferenceLabel(preference string) string {
	if preference == "include" {
		return "принудительно включён"
	}
	return "исключён"
}

func containsTimeframe(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func validSymbol(value string) bool {
	value = strings.ToUpper(strings.TrimSpace(value))
	if len(value) < 3 || len(value) > 30 {
		return false
	}
	for _, char := range value {
		if !(char >= 'A' && char <= 'Z') && !(char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

const (
	authPromptText = "Для доступа к RSI Scanner отправьте общий пароль отдельным сообщением в этот личный чат. После проверки парольное сообщение будет удалено."
	helpText       = "RSI Scanner отслеживает пересечения RSI и EMA RSI на закрытых свечах Bybit USDT Perpetual.\n\n/start — главное меню\n/signals — последние сигналы\n/history — история сигналов\n/settings — настройки и фильтры\n/instruments — ручные правила инструментов\n/include BTCUSDT — включить инструмент\n/exclude BTCUSDT — исключить инструмент\n/reset BTCUSDT — удалить правило"
)
