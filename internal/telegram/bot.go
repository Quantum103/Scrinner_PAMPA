package telegram

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	marketpaper "github.com/Quantum103/Scrinner_PAMPA/internal/paper"
	"github.com/Quantum103/Scrinner_PAMPA/internal/strategy"
	"github.com/Quantum103/Scrinner_PAMPA/internal/stream"
)

const (
	defaultPollTimeoutSeconds = 25
	dailyCheckInterval        = 15 * time.Second
	marketIssueCooldown       = time.Minute
)

type Config struct {
	Token                 string
	AllowedUserIDs        []int64
	DailyLossLimitPercent float64
	PollTimeoutSeconds    int
	APIBaseURL            string
}

type Bot struct {
	cfg       Config
	manager   *stream.Manager
	logger    *slog.Logger
	api       *apiClient
	allowed   map[int64]struct{}
	notifications chan string

	mu                 sync.Mutex
	lastMarketIssues   map[string]time.Time
	lastConnectedCount int
	dailyDate          string
	dailyAlertSent     bool
}

func NewBot(cfg Config, manager *stream.Manager, logger *slog.Logger) (*Bot, error) {
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, errors.New("TELEGRAM_BOT_TOKEN не задан")
	}
	if manager == nil {
		return nil, errors.New("Telegram Bot требует Stream Manager")
	}
	if len(cfg.AllowedUserIDs) == 0 {
		return nil, errors.New("TELEGRAM_ALLOWED_USER_IDS не содержит разрешённых пользователей")
	}
	if cfg.DailyLossLimitPercent <= 0 || cfg.DailyLossLimitPercent > 100 {
		return nil, errors.New("дневной лимит убытка Telegram должен быть в диапазоне (0, 100]")
	}
	if cfg.PollTimeoutSeconds <= 0 || cfg.PollTimeoutSeconds > 50 {
		cfg.PollTimeoutSeconds = defaultPollTimeoutSeconds
	}
	if logger == nil {
		logger = slog.Default()
	}
	allowed := make(map[int64]struct{}, len(cfg.AllowedUserIDs))
	for _, id := range cfg.AllowedUserIDs {
		if id <= 0 {
			return nil, fmt.Errorf("некорректный Telegram ID %d", id)
		}
		if _, exists := allowed[id]; exists {
			return nil, fmt.Errorf("повторяющийся Telegram ID %d", id)
		}
		allowed[id] = struct{}{}
	}
	baseURL := strings.TrimRight(cfg.APIBaseURL, "/")
	if baseURL == "" {
		baseURL = "https://api.telegram.org"
	}
	return &Bot{
		cfg: cfg, manager: manager, logger: logger, allowed: allowed,
		api: &apiClient{token: cfg.Token, baseURL: baseURL, http: defaultHTTPClient()},
		notifications: make(chan string, 256),
		lastMarketIssues: make(map[string]time.Time),
	}, nil
}

// Run запускает уведомления, монитор дневного лимита и Telegram long polling.
func (b *Bot) Run(ctx context.Context) {
	b.manager.SetObserver(b)
	go b.notificationLoop(ctx)
	b.notifyAll(ctx, "Volume Trader запущен. Режим: Paper Trading. Используются только публичные рыночные данные Bybit; реальные ордера не отправляются.")
	go b.monitorDailyLoss(ctx)

	var offset int64
	backoff := 2 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		requestCtx, cancel := context.WithTimeout(ctx, time.Duration(b.cfg.PollTimeoutSeconds+10)*time.Second)
		updates, err := b.api.getUpdates(requestCtx, offset, b.cfg.PollTimeoutSeconds)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			b.logger.Warn("Ошибка Telegram long polling", "ошибка", err)
			if waitContext(ctx, backoff) != nil {
				return
			}
			if backoff < 15*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 2 * time.Second
		for _, update := range updates {
			if update.UpdateID >= offset {
				offset = update.UpdateID + 1
			}
			b.processUpdate(ctx, update)
		}
	}
}

func (b *Bot) processUpdate(ctx context.Context, update Update) {
	if update.Message == nil || update.Message.From == nil {
		return
	}
	message := update.Message
	userID := message.From.ID
	if _, ok := b.allowed[userID]; !ok {
		b.logger.Warn("Отклонена команда от неразрешённого Telegram ID", "telegram_id", userID)
		return
	}
	// Управление разрешено только в личных сообщениях, чтобы исключить команды из групп.
	if message.Chat.Type != "private" || message.Chat.ID != userID {
		return
	}
	fields := strings.Fields(strings.TrimSpace(message.Text))
	if len(fields) == 0 {
		return
	}
	command := strings.SplitN(fields[0], "@", 2)[0]
	switch strings.ToLower(command) {
	case "/start":
		b.sendTo(ctx, message.Chat.ID, "Volume Trader — мониторинг и управление виртуальной торговлей. Режим: Paper Trading, только LONG. Реальные ордера не отправляются.\n\nКоманды: /status, /balance, /position, /stats, /trades, /pause, /resume.")
	case "/status":
		b.sendTo(ctx, message.Chat.ID, b.statusText())
	case "/balance":
		b.sendTo(ctx, message.Chat.ID, b.balanceText())
	case "/position":
		b.sendTo(ctx, message.Chat.ID, b.positionText())
	case "/stats":
		b.sendTo(ctx, message.Chat.ID, b.statsText())
	case "/trades":
		b.sendTo(ctx, message.Chat.ID, b.tradesText())
	case "/pause":
		b.manager.SetEntryPaused(true)
		b.sendTo(ctx, message.Chat.ID, "Поиск новых входов приостановлен. Уже открытая позиция продолжает сопровождаться: Trailing Stop, Order Flow и Funding активны.")
	case "/resume":
		b.manager.SetEntryPaused(false)
		if b.manager.DailyLossReached() {
			b.sendTo(ctx, message.Chat.ID, "Ручная пауза снята, но действует дневной лимит убытка. Новые входы останутся заблокированы до следующего UTC-дня. Открытая позиция продолжает сопровождаться.")
			return
		}
		b.sendTo(ctx, message.Chat.ID, "Поиск новых входов возобновлён. Режим Paper Trading; сопровождение открытых позиций работает.")
	default:
		b.sendTo(ctx, message.Chat.ID, "Неизвестная команда. Доступно: /start, /status, /balance, /position, /stats, /trades, /pause, /resume.")
	}
}

func (b *Bot) statusText() string {
	status := b.manager.Status()
	connection := "нет активного подключения"
	if status.ConnectedShards > 0 {
		connection = fmt.Sprintf("подключено WebSocket-соединений: %d/%d", status.ConnectedShards, status.TotalShards)
	}
	pause := "поиск входов активен"
	if status.EntryPaused {
		pause = "поиск входов приостановлен"
	}
	if status.DailyLossReached {
		pause += " (дневной лимит убытка)"
	}
	message := fmt.Sprintf("Volume Trader: работает\nРежим: Paper Trading\nBybit: %s\nОтслеживается инструментов: %d\nСостояние стратегии: %s",
		connection, status.TrackedSymbols, pause)
	if status.LastError != "" {
		message += "\nПоследняя ошибка подключения: " + status.LastError
	}
	return message
}

func (b *Bot) balanceText() string {
	account := b.manager.PaperSnapshot()
	return fmt.Sprintf(
		"Виртуальный счёт — Paper Trading (USDT)\nБаланс: %.6f\nEquity: %.6f\nСвободная маржа: %.6f\nЗарезервированная маржа: %.6f\nНереализованный PnL: %+.6f\nРеализованный PnL: %+.6f\nКомиссии: %.6f\nFunding (net): %+.6f",
		account.Balance, account.Equity, account.FreeMargin, account.ReservedMargin,
		account.UnrealizedPnL, account.RealizedPnL, account.TotalFees, account.NetFunding,
	)
}

func (b *Bot) positionText() string {
	account := b.manager.PaperSnapshot()
	if len(account.OpenPositions) == 0 {
		return "Открытых виртуальных позиций нет. Режим: Paper Trading."
	}
	var out strings.Builder
	fmt.Fprintf(&out, "Открытая виртуальная позиция — Paper Trading\n")
	for _, position := range account.OpenPositions {
		fmt.Fprintf(&out,
			"%s %s\nВход: %.8f\nКоличество: %.8f\nНоминал входа: %.4f USDT\nMark/Bid: %.8f\nTrailing Stop: %.8f\nПлечо: %.2fx\nНереализованный PnL: %+.6f USDT\nMFE: %.4f USDT (%.3f%%)\nMAE: %.4f USDT (%.3f%%)\nOrder Flow: %d/%d подтверждений",
			position.Symbol, position.Direction, position.EntryPrice, position.RemainingQuantity,
			position.EntryNotional, position.MarkPrice, position.StopPrice, position.Leverage,
			position.UnrealizedPnL, position.MFEUSDT, position.MFEPercent,
			position.MAEUSDT, position.MAEPercent,
			position.OrderFlowConfirmations, position.LastOrderFlowSignalCount,
		)
	}
	return out.String()
}

func (b *Bot) statsText() string {
	stats := b.manager.PaperStatistics()
	profitFactor := "—"
	if stats.InfiniteProfitFactor {
		profitFactor = "∞"
	} else if stats.ProfitFactor != nil {
		profitFactor = fmt.Sprintf("%.3f", *stats.ProfitFactor)
	}
	message := fmt.Sprintf(
		"Статистика Paper Trading\nСделок: %d\nWin Rate: %.2f%%\nNet PnL: %+.6f USDT\nПрибыльных / убыточных: %d / %d\nСредняя прибыль: %+.6f\nСредний убыток: %+.6f\nProfit Factor: %s\nМакс. просадка: %.6f USDT (%.3f%%)\nСредний / максимальный RR: %.3f / %.3f\nКомиссии: %.6f\nПроскальзывание: %.6f USDT\nFunding (net): %+.6f USDT",
		stats.TradeCount, stats.WinRatePercent, stats.NetPnL,
		stats.ProfitableTrades, stats.LosingTrades, stats.AverageProfit, stats.AverageLoss,
		profitFactor, stats.MaxDrawdownUSDT, stats.MaxDrawdownPercent,
		stats.AverageRealizedRR, stats.MaxRealizedRR, stats.TotalFees,
		stats.TotalSlippageUSDT, stats.NetFunding,
	)
	if len(stats.BySymbol) > 0 {
		message += "\n\nПо монетам:"
		for symbol, item := range stats.BySymbol {
			message += fmt.Sprintf("\n%s: %d сделок, PnL %+.5f USDT, win %.1f%%",
				symbol, item.TradeCount, item.NetPnL, item.WinRatePercent)
		}
	}
	if len(stats.ByCloseReason) > 0 {
		message += "\nПричины закрытия:"
		for reason, count := range stats.ByCloseReason {
			message += fmt.Sprintf("\n%s: %d", reason, count)
		}
	}
	return message
}

func (b *Bot) tradesText() string {
	account := b.manager.PaperSnapshot()
	trades := account.ClosedTrades
	if len(trades) == 0 {
		return "Закрытых виртуальных сделок пока нет. Режим: Paper Trading."
	}
	start := len(trades) - 5
	if start < 0 {
		start = 0
	}
	var out strings.Builder
	fmt.Fprintf(&out, "Последние закрытые сделки — Paper Trading\n")
	for i := len(trades) - 1; i >= start; i-- {
		trade := trades[i]
		fmt.Fprintf(&out, "\n%s | %s\nВход: %.8f | Выход: %.8f\nОткрыта: %s\nЗакрыта: %s\nNet PnL: %+.6f USDT\nПричина: %s\n",
			trade.Symbol, trade.Direction, trade.EntryPrice, trade.ExitPrice,
			trade.OpenedAt.Format(time.RFC3339), trade.ClosedAt.Format(time.RFC3339),
			trade.RealizedPnL, trade.CloseReason)
	}
	return out.String()
}

func (b *Bot) OnSignal(signal strategy.Signal) {
	b.notifyAll(context.Background(), fmt.Sprintf(
		"Обнаружен интересный сигнал LONG — Paper Trading\nМонета: %s\nЦена сигнала: %.8f\nRVOL: %.2f\nTaker Buy: %.2f%%\nDelta: %.6f\nCVD: %.6f\nMomentum: %.3f%% (%s)\nSpread: %.2f bps\nBid/Ask liquidity: %.2f / %.2f USDT\nПричины: %s",
		signal.Symbol, signal.Price, signal.RVOL, signal.TakerBuySharePercent,
		signal.Delta, signal.CVD, signal.PriceChangePercent, signal.PriceChangeWindow,
		signal.SpreadBPS, signal.BidLiquidityQuote, signal.AskLiquidityQuote, signal.Reason,
	))
}

func (b *Bot) OnPaperEvent(event marketpaper.Event, account marketpaper.AccountSnapshot) {
	switch event.Type {
	case "position_opened", "position_partially_opened":
		for _, position := range account.OpenPositions {
			if position.Symbol != event.Symbol {
				continue
			}
			b.notifyAll(context.Background(), fmt.Sprintf(
				"Открыта виртуальная LONG-позиция — Paper Trading\nМонета: %s\nЦена входа: %.8f\nНоминал: %.4f USDT\nКоличество: %.8f\nПлечо: %.2fx\nВиртуальный риск: %.6f USDT (%.3f%%)\nНачальный Trailing Stop: %.8f\nКомиссия входа: %.6f USDT\nПроскальзывание входа: %.6f USDT\nПричины входа: %s",
				position.Symbol, position.EntryPrice, position.EntryNotional,
				position.InitialQuantity, position.Leverage, event.RiskUSDT, event.RiskPercent,
				position.StopPrice, position.EntryFee, position.EntrySlippageUSDT, position.EntryReason,
			))
			return
		}
	case "trailing_stop_updated":
		b.notifyAll(context.Background(), fmt.Sprintf(
			"Trailing Stop изменился — Paper Trading\nМонета: %s\nПредыдущее значение: %.8f\nНовое значение: %.8f\n%s",
			event.Symbol, event.PreviousPrice, event.Price, event.Reason,
		))
	case "position_closed":
		for i := len(account.ClosedTrades) - 1; i >= 0; i-- {
			trade := account.ClosedTrades[i]
			if trade.Symbol != event.Symbol || !trade.ClosedAt.Equal(event.At) {
				continue
			}
			grossPnL := (trade.ExitPrice - trade.EntryPrice) * trade.ClosedQuantity
			netFunding := -trade.FundingPaid
			b.notifyAll(context.Background(), fmt.Sprintf(
				"Виртуальная позиция закрыта — Paper Trading\nМонета: %s\nЦена выхода: %.8f\nПричина выхода: %s\nPnL до издержек: %+.6f USDT\nКомиссии: %.6f USDT\nПроскальзывание: %.6f USDT\nFunding (net): %+.6f USDT\nЧистый PnL: %+.6f USDT\nНовый виртуальный баланс: %.6f USDT",
				trade.Symbol, trade.ExitPrice, trade.CloseReason, grossPnL,
				trade.EntryFee+trade.ExitFee, trade.EntrySlippageUSDT+trade.ExitSlippageUSDT,
				netFunding, trade.RealizedPnL, account.Balance,
			))
			return
		}
	}
}

func (b *Bot) OnConnectionStatus(status stream.ConnectionStatus) {
	b.mu.Lock()
	previous := b.lastConnectedCount
	b.lastConnectedCount = status.ConnectedShards
	b.mu.Unlock()
	if status.ConnectedShards < previous {
		b.notifyAll(context.Background(), fmt.Sprintf(
			"Потеряно соединение с Bybit WebSocket. Активно соединений: %d/%d. Переподключение выполняется автоматически. Новые входы при неготовых данных заблокированы. Ошибка: %s",
			status.ConnectedShards, status.TotalShards, status.LastError,
		))
	} else if status.ConnectedShards > previous {
		b.notifyAll(context.Background(), fmt.Sprintf(
			"Соединение с Bybit восстановлено/установлено. Активно соединений: %d/%d. Анализатор заново накапливает данные после сессии. Режим: Paper Trading.",
			status.ConnectedShards, status.TotalShards,
		))
	}
}

func (b *Bot) OnMarketDataIssue(message string) {
	key := message
	if index := strings.IndexByte(key, ':'); index > 0 {
		key = key[:index]
	}
	now := time.Now()
	b.mu.Lock()
	last := b.lastMarketIssues[key]
	if !last.IsZero() && now.Sub(last) < marketIssueCooldown {
		b.mu.Unlock()
		return
	}
	b.lastMarketIssues[key] = now
	b.mu.Unlock()
	b.notifyAll(context.Background(), "Проблема с рыночными данными Bybit. Новые входы по неподтверждённым/устаревшим данным блокируются.\n"+message)
}

func (b *Bot) monitorDailyLoss(ctx context.Context) {
	ticker := time.NewTicker(dailyCheckInterval)
	defer ticker.Stop()
	b.checkDailyLoss(time.Now().UTC())
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			b.checkDailyLoss(now.UTC())
		}
	}
}

func (b *Bot) checkDailyLoss(now time.Time) {
	day := now.Format("2006-01-02")
	b.mu.Lock()
	if b.dailyDate != day {
		b.dailyDate = day
		b.dailyAlertSent = false
		b.mu.Unlock()
		b.manager.SetDailyLossReached(false)
	} else {
		b.mu.Unlock()
	}
	account := b.manager.PaperSnapshot()
	startEquity, found := dailyStartingEquity(account.EquityHistory, now)
	if !found || startEquity <= 0 {
		return
	}
	dailyPnL := account.Equity - startEquity
	lossLimit := startEquity * b.cfg.DailyLossLimitPercent / 100
	if dailyPnL > -lossLimit {
		return
	}
	b.manager.SetDailyLossReached(true)
	b.mu.Lock()
	shouldNotify := !b.dailyAlertSent
	b.dailyAlertSent = true
	b.mu.Unlock()
	if shouldNotify {
		b.notifyAll(context.Background(), fmt.Sprintf(
			"Достигнут дневной лимит убытка Paper Trading\nPnL с начала UTC-дня: %+.6f USDT\nЛимит: %.2f%% (%.6f USDT)\nНовые входы автоматически заблокированы. Уже открытая позиция продолжает сопровождаться Trailing Stop и Order Flow.",
			dailyPnL, b.cfg.DailyLossLimitPercent, lossLimit,
		))
	}
}

func dailyStartingEquity(history []marketpaper.EquityPoint, now time.Time) (float64, bool) {
	date := now.UTC().Format("2006-01-02")
	var first marketpaper.EquityPoint
	found := false
	for _, point := range history {
		if point.At.UTC().Format("2006-01-02") != date {
			continue
		}
		if !found || point.At.Before(first.At) {
			first = point
			found = true
		}
	}
	if !found || !finite(first.Equity) {
		return 0, false
	}
	return first.Equity, true
}

// notifyAll вызывается из WebSocket observer callbacks: постановка уведомления
// в очередь не блокирует обработку рыночных данных сетевым запросом Telegram.
func (b *Bot) notifyAll(_ context.Context, message string) {
	select {
	case b.notifications <- message:
	default:
		b.logger.Warn("Очередь Telegram уведомлений заполнена; уведомление пропущено")
	}
}

func (b *Bot) notificationLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-b.notifications:
			for id := range b.allowed {
				b.sendTo(ctx, id, message)
			}
		}
	}
}

func (b *Bot) sendTo(ctx context.Context, chatID int64, message string) {
	if ctx == nil {
		ctx = context.Background()
	}
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := b.api.sendMessage(requestCtx, chatID, message); err != nil {
		b.logger.Warn("Не удалось отправить Telegram сообщение", "telegram_id", chatID, "ошибка", err)
	}
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func formatTelegramID(id int64) string {
	return strconv.FormatInt(id, 10)
}
