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
	SubscribeSpotAlerts(context.Context, int64) error
	UnsubscribeSpotAlerts(context.Context, int64) error
	AddSpotAlertRule(context.Context, int64, float64, int) (storage.SpotAlertRule, bool, error)
	ListSpotAlertRules(context.Context, int64) ([]storage.SpotAlertRule, error)
	DeleteSpotAlertRule(context.Context, int64, int64) error
	ListUserSpotAlertHistory(context.Context, int64, int, int) ([]storage.SpotAlertHistoryItem, error)
	ClearUserSpotAlertHistory(context.Context, int64) (int64, error)
	ClaimPendingSpotAlertDeliveries(context.Context, int, time.Duration) ([]storage.SpotAlertDelivery, error)
	MarkSpotAlertDeliverySent(context.Context, int64, int64) error
	MarkSpotAlertDeliveryFailed(context.Context, int64, int64, time.Time, error) error
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
	return &Bot{
		api: api, repository: repository, logger: logger,
		accessPassword: accessPassword, authAttempts: make(map[int64]passwordAttempt),
	}, nil
}

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
	go b.runSpotAlertDeliveryWorker(ctx)
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
				b.logger.Error("Telegram update processing failed", "update_id", update.UpdateID, "error", err)
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
		return b.sendMenu(ctx, message.Chat.ID, "Меню Spot-мониторинга")
	}
	command := strings.ToLower(strings.TrimPrefix(fields[0], "/"))
	if at := strings.IndexByte(command, '@'); at >= 0 {
		command = command[:at]
	}

	switch command {
	case "start":
		if err := b.repository.SubscribeSpotAlerts(ctx, user.ID); err != nil {
			return fmt.Errorf("subscribe Telegram user to Spot alerts: %w", err)
		}
		return b.sendMenu(ctx, message.Chat.ID,
			"Привет, "+displayName(user)+". Уведомления о росте Spot-монет включены. Правила можно настроить командой /rules. Для отключения отправьте /stop.")
	case "menu":
		return b.sendMenu(ctx, message.Chat.ID, "Меню Spot-мониторинга")
	case "stop":
		if err := b.repository.UnsubscribeSpotAlerts(ctx, user.ID); err != nil {
			return fmt.Errorf("unsubscribe Telegram user from Spot alerts: %w", err)
		}
		return b.api.sendMessage(ctx, message.Chat.ID, "Уведомления о росте Spot-монет отключены.", nil)
	case "rules":
		return b.showRules(ctx, message.Chat.ID, user.ID, 0)
	case "add":
		return b.addRule(ctx, message.Chat.ID, user.ID, fields[1:])
	case "remove":
		return b.removeRule(ctx, message.Chat.ID, user.ID, fields[1:])
	case "history":
		return b.showHistory(ctx, message.Chat.ID, user.ID, 0, 0)
	case "clearhistory":
		return b.api.sendMessage(ctx, message.Chat.ID,
			"Очистить вашу историю Spot-сигналов? Это действие нельзя отменить.",
			clearHistoryKeyboard())
	case "help":
		return b.api.sendMessage(ctx, message.Chat.ID, helpText, mainMenuKeyboard())
	default:
		return b.api.sendMessage(ctx, message.Chat.ID, "Используйте кнопки меню или команду /help.", mainMenuKeyboard())
	}
}

func (b *Bot) handlePasswordAuthentication(ctx context.Context, message *telegramMessage) error {
	password := strings.TrimSpace(message.Text)
	if password == "" || strings.HasPrefix(password, "/") {
		return b.api.sendMessage(ctx, message.Chat.ID, authPromptText, nil)
	}

	deleteCtx, deleteCancel := context.WithTimeout(ctx, 3*time.Second)
	if err := b.api.deleteMessage(deleteCtx, message.Chat.ID, message.MessageID); err != nil && ctx.Err() == nil {
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
	}
	return b.sendMenu(ctx, message.Chat.ID,
		"Пароль принят. Доступ открыт. Привет, "+displayName(user)+"!\n\nЧтобы включить уведомления о росте Spot-монет, отправьте /start.")
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
		return b.render(ctx, chatID, messageID, "Меню Spot-мониторинга", mainMenuKeyboard())
	case action == "help":
		return b.render(ctx, chatID, messageID, helpText, mainMenuKeyboard())
	case action == "rules":
		return b.showRules(ctx, chatID, userID, messageID)
	case action == "history":
		return b.showHistory(ctx, chatID, userID, 0, messageID)
	case strings.HasPrefix(action, "history:"):
		offset, err := strconv.Atoi(strings.TrimPrefix(action, "history:"))
		if err != nil || offset < 0 || offset > 10000 {
			return errors.New("invalid Telegram Spot history page")
		}
		return b.showHistory(ctx, chatID, userID, offset, messageID)
	case action == "clear_history":
		return b.render(ctx, chatID, messageID,
			"Очистить вашу историю Spot-сигналов? Это действие нельзя отменить.",
			clearHistoryKeyboard())
	case action == "clear_history:cancel":
		return b.render(ctx, chatID, messageID, "Очистка истории отменена.", mainMenuKeyboard())
	case action == "clear_history:confirm":
		deleted, err := b.repository.ClearUserSpotAlertHistory(ctx, userID)
		if err != nil {
			return fmt.Errorf("clear user's Spot history: %w", err)
		}
		return b.render(ctx, chatID, messageID,
			fmt.Sprintf("История очищена. Удалено записей: %d.", deleted), mainMenuKeyboard())
	default:
		return b.render(ctx, chatID, messageID, "Неизвестное действие. Откройте меню.", mainMenuKeyboard())
	}
}

func (b *Bot) sendMenu(ctx context.Context, chatID int64, message string) error {
	return b.api.sendMessage(ctx, chatID, message, mainMenuKeyboard())
}

func (b *Bot) render(ctx context.Context, chatID, messageID int64, message string, keyboard *inlineKeyboard) error {
	if messageID == 0 {
		return b.api.sendMessage(ctx, chatID, message, keyboard)
	}
	return b.api.editMessageText(ctx, chatID, messageID, message, keyboard)
}

func mainMenuKeyboard() *inlineKeyboard {
	return &inlineKeyboard{InlineKeyboard: [][]inlineButton{
		{{Text: "⚙️ Правила сигналов", CallbackData: "rules"}},
		{{Text: "📚 История роста", CallbackData: "history"}},
		{{Text: "🧹 Очистить историю", CallbackData: "clear_history"}},
		{{Text: "ℹ️ Справка", CallbackData: "help"}},
	}}
}

func clearHistoryKeyboard() *inlineKeyboard {
	return &inlineKeyboard{InlineKeyboard: [][]inlineButton{
		{{Text: "Да, очистить", CallbackData: "clear_history:confirm"}},
		{{Text: "Отмена", CallbackData: "clear_history:cancel"}},
	}}
}


func (b *Bot) showRules(ctx context.Context, chatID, userID, messageID int64) error {
	rules, err := b.repository.ListSpotAlertRules(ctx, userID)
	if err != nil {
		return fmt.Errorf("load Telegram user's Spot rules: %w", err)
	}
	var message strings.Builder
	message.WriteString("Ваши правила сигналов роста:\n\n")
	for _, rule := range rules {
		fmt.Fprintf(&message, "#%d — более %s%% за %s\n",
			rule.ID, formatThreshold(rule.ThresholdPercent), formatWindow(rule.WindowSeconds))
	}
	if len(rules) == 0 {
		message.WriteString("Правила не настроены. Добавьте первое правило командой /add 3 5.\n")
	}
	message.WriteString("\nДобавить правило: /add 3 5\nУдалить правило: /remove 2\n")
	message.WriteString("Формат: /add ПРОЦЕНТ МИНУТЫ. Например, /add 1 1, /add 3 5, /add 10 10.\n")
	message.WriteString("Можно настроить до 20 правил: от 0.01 до 1000% и от 1 до 60 минут.")
	return b.render(ctx, chatID, messageID, message.String(), mainMenuKeyboard())
}

func (b *Bot) addRule(ctx context.Context, chatID, userID int64, args []string) error {
	if len(args) != 2 {
		return b.api.sendMessage(ctx, chatID,
			"Формат добавления: /add ПРОЦЕНТ МИНУТЫ\nПример: /add 3 5 — рост более 3% за 5 минут.", nil)
	}
	thresholdText := strings.TrimSuffix(strings.TrimSpace(args[0]), "%")
	thresholdText = strings.ReplaceAll(thresholdText, ",", ".")
	threshold, err := strconv.ParseFloat(thresholdText, 64)
	if err != nil {
		return b.api.sendMessage(ctx, chatID, "Не удалось прочитать процент. Пример: /add 3.5 5", nil)
	}
	minutes, err := strconv.Atoi(strings.TrimSpace(args[1]))
	if err != nil {
		return b.api.sendMessage(ctx, chatID, "Таймфрейм указывается целым числом минут. Пример: /add 3 5", nil)
	}
	windowSeconds := minutes * 60
	if err := storage.ValidateSpotAlertRule(threshold, windowSeconds); err != nil {
		return b.api.sendMessage(ctx, chatID,
			"Недопустимое правило. Процент: от 0.01 до 1000. Таймфрейм: от 1 до 60 целых минут.", nil)
	}
	rule, created, err := b.repository.AddSpotAlertRule(ctx, userID, threshold, windowSeconds)
	if errors.Is(err, storage.ErrInvalidInput) {
		return b.api.sendMessage(ctx, chatID,
			"Достигнут лимит: можно настроить не более 20 разных правил.", nil)
	}
	if err != nil {
		return fmt.Errorf("add Telegram user's Spot rule: %w", err)
	}
	status := "добавлено"
	if !created {
		status = "уже существует"
	}
	return b.api.sendMessage(ctx, chatID,
		fmt.Sprintf("Правило %s: #%d — более %s%% за %s.", status, rule.ID,
			formatThreshold(rule.ThresholdPercent), formatWindow(rule.WindowSeconds)), mainMenuKeyboard())
}

func (b *Bot) removeRule(ctx context.Context, chatID, userID int64, args []string) error {
	if len(args) != 1 {
		return b.api.sendMessage(ctx, chatID, "Формат удаления: /remove ID\nПосмотреть ID можно командой /rules.", nil)
	}
	ruleID, err := strconv.ParseInt(strings.TrimSpace(args[0]), 10, 64)
	if err != nil || ruleID <= 0 {
		return b.api.sendMessage(ctx, chatID, "ID правила должен быть положительным целым числом.", nil)
	}
	err = b.repository.DeleteSpotAlertRule(ctx, userID, ruleID)
	if errors.Is(err, storage.ErrLastSpotAlertRule) {
		return b.api.sendMessage(ctx, chatID, "Нельзя удалить последнее правило. Сначала добавьте другое через /add.", nil)
	}
	if errors.Is(err, storage.ErrNotFound) {
		return b.api.sendMessage(ctx, chatID, "Правило с таким ID не найдено. Посмотрите список через /rules.", nil)
	}
	if err != nil {
		return fmt.Errorf("delete Telegram user's Spot rule: %w", err)
	}
	return b.api.sendMessage(ctx, chatID, fmt.Sprintf("Правило #%d удалено.", ruleID), mainMenuKeyboard())
}

func formatThreshold(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func formatWindow(windowSeconds int) string {
	if windowSeconds > 0 && windowSeconds%60 == 0 {
		return fmt.Sprintf("%d мин.", windowSeconds/60)
	}
	return (time.Duration(windowSeconds) * time.Second).String()
}

func (b *Bot) showHistory(ctx context.Context, chatID, userID int64, offset int, messageID int64) error {
	items, err := b.repository.ListUserSpotAlertHistory(ctx, userID, 10, offset)
	if err != nil {
		return fmt.Errorf("load Telegram Spot history: %w", err)
	}
	var message strings.Builder
	if len(items) == 0 {
		message.WriteString("История пока пуста.")
	} else {
		if offset == 0 {
			message.WriteString("Последние сигналы роста Spot\n\n")
		} else {
			fmt.Fprintf(&message, "История Spot · страница %d\n\n", offset/10+1)
		}
		for index, item := range items {
			fmt.Fprintf(&message, "%s", formatSpotHistoryItem(item))
			if index+1 < len(items) {
				message.WriteString("\n\n")
			}
		}
	}
	keyboard := historyKeyboard(offset, len(items) == 10)
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

func (b *Bot) runSpotAlertDeliveryWorker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		deliveries, err := b.repository.ClaimPendingSpotAlertDeliveries(ctx, deliveryBatchSize, deliveryLease)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			b.logger.Error("claim Telegram Spot alert deliveries failed", "error", err)
		} else {
			for _, delivery := range deliveries {
				if ctx.Err() != nil {
					return
				}
				b.deliverSpotAlert(ctx, delivery)
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

func (b *Bot) deliverSpotAlert(ctx context.Context, delivery storage.SpotAlertDelivery) {
	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err := b.api.sendMessage(sendCtx, delivery.TelegramID, formatSpotAlert(delivery), nil)
	cancel()

	persistCtx, persistCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer persistCancel()
	if err != nil && isPermanentTelegramDeliveryError(err) {
		if unsubscribeErr := b.repository.UnsubscribeSpotAlerts(persistCtx, delivery.UserID); unsubscribeErr == nil {
			b.logger.Warn("Spot alerts disabled after permanent Telegram delivery error",
				"user_id", delivery.UserID, "alert_id", delivery.AlertID, "error", err)
			return
		} else {
			b.logger.Error("disable Spot subscription after permanent Telegram delivery error failed",
				"user_id", delivery.UserID, "alert_id", delivery.AlertID,
				"send_error", err, "storage_error", unsubscribeErr)
		}
	}
	if err == nil {
		if markErr := b.repository.MarkSpotAlertDeliverySent(persistCtx, delivery.UserID, delivery.AlertID); markErr != nil {
			b.logger.Error("mark Telegram Spot alert delivery as sent failed",
				"user_id", delivery.UserID, "alert_id", delivery.AlertID, "error", markErr)
		}
		return
	}
	nextAttempt := time.Now().Add(deliveryRetryDelay(delivery.AttemptCount))
	if markErr := b.repository.MarkSpotAlertDeliveryFailed(persistCtx, delivery.UserID, delivery.AlertID, nextAttempt, err); markErr != nil {
		b.logger.Error("mark Telegram Spot alert delivery as failed",
			"user_id", delivery.UserID, "alert_id", delivery.AlertID,
			"send_error", err, "storage_error", markErr)
		return
	}
	b.logger.Warn("Telegram Spot alert delivery failed; retry scheduled",
		"user_id", delivery.UserID, "alert_id", delivery.AlertID,
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

func formatSpotAlert(delivery storage.SpotAlertDelivery) string {
	return formatSpotMovement(delivery.Symbol, delivery.ChangePercent, delivery.ThresholdPercent,
		delivery.Price, delivery.ReferencePrice, delivery.DetectedAt, delivery.WindowSeconds)
}

func formatSpotHistoryItem(item storage.SpotAlertHistoryItem) string {
	return formatSpotMovement(item.Symbol, item.ChangePercent, item.ThresholdPercent, item.Price,
		item.ReferencePrice, item.DetectedAt, item.WindowSeconds)
}

func formatSpotMovement(symbol string, change, threshold, price, reference float64, detectedAt time.Time, windowSeconds int) string {
	return fmt.Sprintf(
		"🚀 Рост более %s%% за %s\n%s\nИзменение: %+.2f%%\nТекущая цена: %s\nЦена в начале окна: %s\nОбнаружено: %s UTC",
		formatThreshold(threshold), formatWindow(windowSeconds), symbol, change,
		formatPrice(price), formatPrice(reference), detectedAt.UTC().Format("2006-01-02 15:04:05"),
	)
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
	authPromptText = "Для доступа к боту отправьте общий пароль отдельным сообщением. Сообщение с паролем будет удалено."
	helpText       = "Бот отслеживает Spot-пары USDT с оборотом за 24 часа не выше 1 млн USDT. Правила роста настраиваются отдельно для каждого пользователя.\n\n/start — включить уведомления\n/stop — отключить уведомления\n/rules — мои правила сигналов\n/add 3 5 — добавить рост более 3% за 5 минут\n/remove 2 — удалить правило с ID 2\n/history — история полученных сигналов\n/clearhistory — очистить свою историю"
)
