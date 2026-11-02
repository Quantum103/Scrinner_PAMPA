package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	marketanalysis "github.com/Quantum103/Scrinner_PAMPA/internal/analysis"
	marketstrategy "github.com/Quantum103/Scrinner_PAMPA/internal/strategy"
	marketpaper "github.com/Quantum103/Scrinner_PAMPA/internal/paper"
)

const MarketCategory = "linear"

type Config struct {
	LogLevel   string
	Bybit      BybitConfig
	MarketData MarketDataConfig
	WebSocket  WebSocketConfig
	Analysis      marketanalysis.Config
	Strategy      StrategyConfig
	EntryStrategy marketstrategy.Config
	Paper      PaperConfig
	Database   DatabaseConfig
	Costs      CostsConfig
	Telegram   TelegramConfig
}

type BybitConfig struct {
	RESTURL     string
	PublicWSURL string
}

type MarketDataConfig struct {
	Enabled                   bool
	UpdateInterval            time.Duration
	InstrumentRefreshInterval time.Duration
	MaxTrackedSymbols         int
}

type WebSocketConfig struct {
	OrderbookDepth          int
	MaxSymbolsPerConnection int
	MaxArgsPerSubscribe     int
	MaxDataAge              time.Duration
	TradeBufferCapacity     int
	SeenTradeIDCapacity     int
}

type StrategyConfig struct {
	VolumeMultiplier      float64
	TrailingStopPercent   float64
	ReversalConfirmations int
}

type PaperConfig struct {
	Enabled             bool
	InitialDepositUSDT  float64
	TargetRiskPercent   float64
	MaxRiskPercent      float64
	MaxLeverage         float64
	MaxMarginPercent    float64
	MaxOpenPositions    int
	TrailingStopPercent                float64
	OrderFlowExitEnabled               bool
	OrderFlowMinSignals                 int
	OrderFlowConsecutiveIntervals       int
	OrderFlowObservationInterval        time.Duration
	OrderFlowMaxObservationGap          time.Duration
	OrderFlowMaxTakerBuySharePercent    float64
	OrderFlowTakerBuyShareDropPP        float64
	OrderFlowMinTakerSellSharePercent   float64
	OrderFlowCVDMinDeclinePercent       float64
	OrderFlowPriceStallMaxRisePercent   float64
}

type DatabaseConfig struct {
	Path        string
	BusyTimeout time.Duration
}

type CostsConfig struct {
	FeeRate     float64
	SlippageBPS float64
}

type TelegramConfig struct {
	BotToken              string
	AllowedUserIDs        []int64
	DailyLossLimitPercent float64
}

func Load() (Config, error) {
	cfg := Config{
		LogLevel: "info",
		Bybit: BybitConfig{
			RESTURL:     "https://api.bybit.com",
			PublicWSURL: "wss://stream.bybit.com/v5/public/linear",
		},
		MarketData: MarketDataConfig{
			Enabled:                   true,
			UpdateInterval:            2 * time.Second,
			InstrumentRefreshInterval: 15 * time.Minute,
			MaxTrackedSymbols:         100,
		},
		Analysis:      marketanalysis.DefaultConfig(),
		EntryStrategy: marketstrategy.DefaultConfig(),
		WebSocket: WebSocketConfig{
			OrderbookDepth:          50,
			MaxSymbolsPerConnection: 25,
			MaxArgsPerSubscribe:     10,
			MaxDataAge:              5 * time.Second,
			TradeBufferCapacity:     4096,
			SeenTradeIDCapacity:     4096,
		},
		Strategy: StrategyConfig{
			VolumeMultiplier:      2,
			TrailingStopPercent:   2.5,
			ReversalConfirmations: 2,
		},
		Database: DatabaseConfig{
			Path: "./data/volume_trader.db",
			BusyTimeout: 5 * time.Second,
		},
		Paper: PaperConfig{
			Enabled:             true,
			InitialDepositUSDT:  10,
			TargetRiskPercent:   2.5,
			MaxRiskPercent:      3,
			MaxLeverage:         2,
			MaxMarginPercent:    50,
			MaxOpenPositions:    1,
			TrailingStopPercent:                2.5,
			OrderFlowExitEnabled:               true,
			OrderFlowMinSignals:                 3,
			OrderFlowConsecutiveIntervals:       2,
			OrderFlowObservationInterval:        time.Second,
			OrderFlowMaxObservationGap:          3 * time.Second,
			OrderFlowMaxTakerBuySharePercent:    45,
			OrderFlowTakerBuyShareDropPP:        10,
			OrderFlowMinTakerSellSharePercent:   55,
			OrderFlowCVDMinDeclinePercent:       0.5,
			OrderFlowPriceStallMaxRisePercent:   0.02,
		},
		Costs: CostsConfig{
			FeeRate:     0.00055,
			SlippageBPS: 5,
		},
		Telegram: TelegramConfig{
			DailyLossLimitPercent: 5,
		},
	}

	var err error
	if cfg.LogLevel, err = stringValue("APP_LOG_LEVEL", cfg.LogLevel); err != nil {
		return Config{}, err
	}
	if cfg.Bybit.RESTURL, err = stringValue("BYBIT_REST_URL", cfg.Bybit.RESTURL); err != nil {
		return Config{}, err
	}
	if cfg.Bybit.PublicWSURL, err = stringValue("BYBIT_PUBLIC_WS_URL", cfg.Bybit.PublicWSURL); err != nil {
		return Config{}, err
	}
	if cfg.MarketData.Enabled, err = boolValue("MARKET_DATA_ENABLED", cfg.MarketData.Enabled); err != nil {
		return Config{}, err
	}
	if cfg.MarketData.UpdateInterval, err = durationValue("DATA_UPDATE_INTERVAL", cfg.MarketData.UpdateInterval); err != nil {
		return Config{}, err
	}
	if cfg.MarketData.InstrumentRefreshInterval, err = durationValue("INSTRUMENT_REFRESH_INTERVAL", cfg.MarketData.InstrumentRefreshInterval); err != nil {
		return Config{}, err
	}
	if cfg.MarketData.MaxTrackedSymbols, err = intValue("MAX_TRACKED_CONTRACTS", cfg.MarketData.MaxTrackedSymbols); err != nil {
		return Config{}, err
	}
	if cfg.WebSocket.OrderbookDepth, err = intValue("WS_ORDERBOOK_DEPTH", cfg.WebSocket.OrderbookDepth); err != nil {
		return Config{}, err
	}
	if cfg.WebSocket.MaxSymbolsPerConnection, err = intValue("WS_MAX_SYMBOLS_PER_CONNECTION", cfg.WebSocket.MaxSymbolsPerConnection); err != nil {
		return Config{}, err
	}
	if cfg.WebSocket.MaxArgsPerSubscribe, err = intValue("WS_MAX_ARGS_PER_SUBSCRIBE", cfg.WebSocket.MaxArgsPerSubscribe); err != nil {
		return Config{}, err
	}
	if cfg.WebSocket.MaxDataAge, err = durationValue("WS_MAX_DATA_AGE", cfg.WebSocket.MaxDataAge); err != nil {
		return Config{}, err
	}
	if cfg.WebSocket.TradeBufferCapacity, err = intValue("WS_TRADE_BUFFER_CAPACITY", cfg.WebSocket.TradeBufferCapacity); err != nil {
		return Config{}, err
	}
	if cfg.WebSocket.SeenTradeIDCapacity, err = intValue("WS_SEEN_TRADE_ID_CAPACITY", cfg.WebSocket.SeenTradeIDCapacity); err != nil {
		return Config{}, err
	}
	if cfg.Analysis.RVOLInterval, err = durationValue("ANALYSIS_RVOL_INTERVAL", cfg.Analysis.RVOLInterval); err != nil {
		return Config{}, err
	}
	if cfg.Analysis.RVOLLookbackIntervals, err = intValue("ANALYSIS_RVOL_LOOKBACK_INTERVALS", cfg.Analysis.RVOLLookbackIntervals); err != nil {
		return Config{}, err
	}
	if cfg.Analysis.FlowWindow, err = durationValue("ANALYSIS_FLOW_WINDOW", cfg.Analysis.FlowWindow); err != nil {
		return Config{}, err
	}
	if cfg.Analysis.CVDWindow, err = durationValue("ANALYSIS_CVD_WINDOW", cfg.Analysis.CVDWindow); err != nil {
		return Config{}, err
	}
	if cfg.Analysis.MomentumWindows, err = durationListValue("ANALYSIS_MOMENTUM_WINDOWS", cfg.Analysis.MomentumWindows); err != nil {
		return Config{}, err
	}
	if cfg.Analysis.VolumeAccelerationWindow, err = durationValue("ANALYSIS_VOLUME_ACCELERATION_WINDOW", cfg.Analysis.VolumeAccelerationWindow); err != nil {
		return Config{}, err
	}
	if cfg.Analysis.AbsorptionMinBuyShare, err = floatValue("ANALYSIS_ABSORPTION_MIN_BUY_SHARE", cfg.Analysis.AbsorptionMinBuyShare); err != nil {
		return Config{}, err
	}
	if cfg.Analysis.AbsorptionBuyVolumeMultiplier, err = floatValue("ANALYSIS_ABSORPTION_BUY_VOLUME_MULTIPLIER", cfg.Analysis.AbsorptionBuyVolumeMultiplier); err != nil {
		return Config{}, err
	}
	if cfg.Analysis.AbsorptionMaxPriceChangePct, err = floatValue("ANALYSIS_ABSORPTION_MAX_PRICE_CHANGE_PERCENT", cfg.Analysis.AbsorptionMaxPriceChangePct); err != nil {
		return Config{}, err
	}
	if cfg.Analysis.OrderBookLevels, err = intValue("ANALYSIS_ORDERBOOK_LEVELS", cfg.Analysis.OrderBookLevels); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.Enabled, err = boolValue("ENTRY_STRATEGY_ENABLED", cfg.EntryStrategy.Enabled); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.MinRVOL, err = floatValue("ENTRY_MIN_RVOL", cfg.EntryStrategy.MinRVOL); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.MinTakerBuySharePercent, err = floatValue("ENTRY_MIN_TAKER_BUY_SHARE_PERCENT", cfg.EntryStrategy.MinTakerBuySharePercent); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.MaxPriceChangePercent, err = floatValue("ENTRY_MAX_PRICE_CHANGE_PERCENT", cfg.EntryStrategy.MaxPriceChangePercent); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.PriceChangeWindow, err = durationValue("ENTRY_PRICE_CHANGE_WINDOW", cfg.EntryStrategy.PriceChangeWindow); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.MaxSpreadBPS, err = floatValue("ENTRY_MAX_SPREAD_BPS", cfg.EntryStrategy.MaxSpreadBPS); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.MinBidLiquidityQuote, err = floatValue("ENTRY_MIN_BID_LIQUIDITY_QUOTE", cfg.EntryStrategy.MinBidLiquidityQuote); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.MinAskLiquidityQuote, err = floatValue("ENTRY_MIN_ASK_LIQUIDITY_QUOTE", cfg.EntryStrategy.MinAskLiquidityQuote); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.MinBookLevels, err = intValue("ENTRY_MIN_BOOK_LEVELS", cfg.EntryStrategy.MinBookLevels); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.MinTradesInFlowWindow, err = intValue("ENTRY_MIN_TRADES_IN_FLOW_WINDOW", cfg.EntryStrategy.MinTradesInFlowWindow); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.MaxSingleTradeSharePercent, err = floatValue("ENTRY_MAX_SINGLE_TRADE_SHARE_PERCENT", cfg.EntryStrategy.MaxSingleTradeSharePercent); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.ConfirmationObservations, err = intValue("ENTRY_CONFIRMATION_OBSERVATIONS", cfg.EntryStrategy.ConfirmationObservations); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.ObservationInterval, err = durationValue("ENTRY_OBSERVATION_INTERVAL", cfg.EntryStrategy.ObservationInterval); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.MaxObservationGap, err = durationValue("ENTRY_MAX_OBSERVATION_GAP", cfg.EntryStrategy.MaxObservationGap); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.SignalCooldown, err = durationValue("ENTRY_SIGNAL_COOLDOWN", cfg.EntryStrategy.SignalCooldown); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.RearmObservations, err = intValue("ENTRY_REARM_OBSERVATIONS", cfg.EntryStrategy.RearmObservations); err != nil {
		return Config{}, err
	}
	if cfg.EntryStrategy.SignalHistoryLimit, err = intValue("ENTRY_SIGNAL_HISTORY_LIMIT", cfg.EntryStrategy.SignalHistoryLimit); err != nil {
		return Config{}, err
	}
	if cfg.Strategy.VolumeMultiplier, err = floatValue("STRATEGY_VOLUME_MULTIPLIER", cfg.Strategy.VolumeMultiplier); err != nil {
		return Config{}, err
	}
	if cfg.Strategy.TrailingStopPercent, err = floatValue("STRATEGY_TRAILING_STOP_PERCENT", cfg.Strategy.TrailingStopPercent); err != nil {
		return Config{}, err
	}
	if cfg.Strategy.ReversalConfirmations, err = intValue("STRATEGY_REVERSAL_CONFIRMATIONS", cfg.Strategy.ReversalConfirmations); err != nil {
		return Config{}, err
	}
	if cfg.Paper.Enabled, err = boolValue("PAPER_TRADING_ENABLED", cfg.Paper.Enabled); err != nil {
		return Config{}, err
	}
	if cfg.Paper.InitialDepositUSDT, err = floatValue("PAPER_INITIAL_BALANCE_USDT", cfg.Paper.InitialDepositUSDT); err != nil {
		return Config{}, err
	}
	if _, exists := os.LookupEnv("PAPER_INITIAL_BALANCE_USDT"); !exists {
		if cfg.Paper.InitialDepositUSDT, err = floatValue("VIRTUAL_DEPOSIT_USDT", cfg.Paper.InitialDepositUSDT); err != nil {
			return Config{}, err
		}
	}
	if cfg.Paper.TargetRiskPercent, err = floatValue("PAPER_TARGET_RISK_PERCENT", cfg.Paper.TargetRiskPercent); err != nil {
		return Config{}, err
	}
	if cfg.Paper.MaxRiskPercent, err = floatValue("PAPER_MAX_RISK_PERCENT", cfg.Paper.MaxRiskPercent); err != nil {
		return Config{}, err
	}
	if cfg.Paper.MaxLeverage, err = floatValue("PAPER_MAX_LEVERAGE", cfg.Paper.MaxLeverage); err != nil {
		return Config{}, err
	}
	if cfg.Paper.MaxMarginPercent, err = floatValue("PAPER_MAX_MARGIN_PERCENT", cfg.Paper.MaxMarginPercent); err != nil {
		return Config{}, err
	}
	if cfg.Paper.MaxOpenPositions, err = intValue("PAPER_MAX_OPEN_POSITIONS", cfg.Paper.MaxOpenPositions); err != nil {
		return Config{}, err
	}
	if cfg.Paper.TrailingStopPercent, err = floatValue("PAPER_TRAILING_STOP_PERCENT", cfg.Paper.TrailingStopPercent); err != nil {
		return Config{}, err
	}
	if cfg.Paper.OrderFlowExitEnabled, err = boolValue("PAPER_ORDER_FLOW_EXIT_ENABLED", cfg.Paper.OrderFlowExitEnabled); err != nil {
		return Config{}, err
	}
	if cfg.Paper.OrderFlowMinSignals, err = intValue("PAPER_ORDER_FLOW_MIN_SIGNALS", cfg.Paper.OrderFlowMinSignals); err != nil {
		return Config{}, err
	}
	if cfg.Paper.OrderFlowConsecutiveIntervals, err = intValue("PAPER_ORDER_FLOW_CONSECUTIVE_INTERVALS", cfg.Paper.OrderFlowConsecutiveIntervals); err != nil {
		return Config{}, err
	}
	if cfg.Paper.OrderFlowObservationInterval, err = durationValue("PAPER_ORDER_FLOW_OBSERVATION_INTERVAL", cfg.Paper.OrderFlowObservationInterval); err != nil {
		return Config{}, err
	}
	if cfg.Paper.OrderFlowMaxObservationGap, err = durationValue("PAPER_ORDER_FLOW_MAX_OBSERVATION_GAP", cfg.Paper.OrderFlowMaxObservationGap); err != nil {
		return Config{}, err
	}
	if cfg.Paper.OrderFlowMaxTakerBuySharePercent, err = floatValue("PAPER_ORDER_FLOW_MAX_TAKER_BUY_SHARE_PERCENT", cfg.Paper.OrderFlowMaxTakerBuySharePercent); err != nil {
		return Config{}, err
	}
	if cfg.Paper.OrderFlowTakerBuyShareDropPP, err = floatValue("PAPER_ORDER_FLOW_TAKER_BUY_SHARE_DROP_PP", cfg.Paper.OrderFlowTakerBuyShareDropPP); err != nil {
		return Config{}, err
	}
	if cfg.Paper.OrderFlowMinTakerSellSharePercent, err = floatValue("PAPER_ORDER_FLOW_MIN_TAKER_SELL_SHARE_PERCENT", cfg.Paper.OrderFlowMinTakerSellSharePercent); err != nil {
		return Config{}, err
	}
	if cfg.Paper.OrderFlowCVDMinDeclinePercent, err = floatValue("PAPER_ORDER_FLOW_CVD_MIN_DECLINE_PERCENT", cfg.Paper.OrderFlowCVDMinDeclinePercent); err != nil {
		return Config{}, err
	}
	if cfg.Paper.OrderFlowPriceStallMaxRisePercent, err = floatValue("PAPER_ORDER_FLOW_PRICE_STALL_MAX_RISE_PERCENT", cfg.Paper.OrderFlowPriceStallMaxRisePercent); err != nil {
		return Config{}, err
	}
	if _, exists := os.LookupEnv("PAPER_MAX_OPEN_POSITIONS"); !exists {
		if cfg.Paper.MaxOpenPositions, err = intValue("MAX_OPEN_POSITIONS", cfg.Paper.MaxOpenPositions); err != nil {
			return Config{}, err
		}
	}
	if cfg.Costs.FeeRate, err = floatValue("PAPER_FEE_RATE", cfg.Costs.FeeRate); err != nil {
		return Config{}, err
	}
	if _, exists := os.LookupEnv("PAPER_FEE_RATE"); !exists {
		if cfg.Costs.FeeRate, err = floatValue("TRADING_FEE_RATE", cfg.Costs.FeeRate); err != nil {
			return Config{}, err
		}
	}
	if cfg.Costs.SlippageBPS, err = floatValue("PAPER_SLIPPAGE_BPS", cfg.Costs.SlippageBPS); err != nil {
		return Config{}, err
	}
	if _, exists := os.LookupEnv("PAPER_SLIPPAGE_BPS"); !exists {
		if cfg.Costs.SlippageBPS, err = floatValue("SLIPPAGE_BPS", cfg.Costs.SlippageBPS); err != nil {
			return Config{}, err
		}
	}
	if cfg.Database.Path, err = stringValue("APP_DB_PATH", cfg.Database.Path); err != nil {
		return Config{}, err
	}
	if cfg.Database.BusyTimeout, err = durationValue("APP_DB_BUSY_TIMEOUT", cfg.Database.BusyTimeout); err != nil {
		return Config{}, err
	}
	if cfg.Telegram.BotToken, err = stringValue("TELEGRAM_BOT_TOKEN", ""); err != nil {
		return Config{}, err
	}
	if cfg.Telegram.AllowedUserIDs, err = int64ListValue("TELEGRAM_ALLOWED_USER_IDS", nil); err != nil {
		return Config{}, err
	}
	if cfg.Telegram.DailyLossLimitPercent, err = floatValue("TELEGRAM_DAILY_LOSS_LIMIT_PERCENT", cfg.Telegram.DailyLossLimitPercent); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if !oneOf(strings.ToLower(c.LogLevel), "debug", "info", "warn", "error") {
		return fmt.Errorf("APP_LOG_LEVEL должен быть debug, info, warn или error")
	}
	if err := validateURL("BYBIT_REST_URL", c.Bybit.RESTURL, "https"); err != nil {
		return err
	}
	if err := validateURL("BYBIT_PUBLIC_WS_URL", c.Bybit.PublicWSURL, "wss"); err != nil {
		return err
	}
	wsURL, _ := url.ParseRequestURI(c.Bybit.PublicWSURL)
	if wsURL.Path != "/v5/public/linear" {
		return fmt.Errorf("BYBIT_PUBLIC_WS_URL должен указывать на публичный поток Linear (/v5/public/linear)")
	}
	if c.MarketData.UpdateInterval <= 0 {
		return fmt.Errorf("DATA_UPDATE_INTERVAL должен быть больше нуля")
	}
	if c.MarketData.InstrumentRefreshInterval <= 0 {
		return fmt.Errorf("INSTRUMENT_REFRESH_INTERVAL должен быть больше нуля")
	}
	if c.MarketData.MaxTrackedSymbols <= 0 {
		return fmt.Errorf("MAX_TRACKED_CONTRACTS должен быть больше нуля")
	}
	if c.WebSocket.OrderbookDepth != 50 {
		return fmt.Errorf("WS_ORDERBOOK_DEPTH должен быть равен 50 для выбранного потока стакана")
	}
	if c.WebSocket.MaxSymbolsPerConnection <= 0 || c.WebSocket.MaxSymbolsPerConnection > 25 {
		return fmt.Errorf("WS_MAX_SYMBOLS_PER_CONNECTION должен быть в диапазоне от 1 до 25")
	}
	if c.WebSocket.MaxArgsPerSubscribe <= 0 || c.WebSocket.MaxArgsPerSubscribe > 10 {
		return fmt.Errorf("WS_MAX_ARGS_PER_SUBSCRIBE должен быть в диапазоне от 1 до 10")
	}
	if c.WebSocket.MaxDataAge <= 0 {
		return fmt.Errorf("WS_MAX_DATA_AGE должен быть больше нуля")
	}
	if c.WebSocket.TradeBufferCapacity <= 0 || c.WebSocket.SeenTradeIDCapacity <= 0 {
		return fmt.Errorf("размеры буферов WebSocket должны быть больше нуля")
	}
	if err := c.Analysis.Validate(); err != nil {
		return fmt.Errorf("ANALYSIS: %w", err)
	}
	if err := c.EntryStrategy.Validate(); err != nil {
		return fmt.Errorf("ENTRY STRATEGY: %w", err)
	}
	if strings.TrimSpace(c.Database.Path) == "" {
		return fmt.Errorf("APP_DB_PATH не должен быть пустым")
	}
	if c.Database.BusyTimeout <= 0 {
		return fmt.Errorf("APP_DB_BUSY_TIMEOUT должен быть положительным")
	}
	if strings.TrimSpace(c.Telegram.BotToken) == "" && len(c.Telegram.AllowedUserIDs) > 0 {
		return fmt.Errorf("TELEGRAM_BOT_TOKEN требуется при заданном TELEGRAM_ALLOWED_USER_IDS")
	}
	if strings.TrimSpace(c.Telegram.BotToken) != "" && len(c.Telegram.AllowedUserIDs) == 0 {
		return fmt.Errorf("при включённом Telegram укажи хотя бы один ID в TELEGRAM_ALLOWED_USER_IDS")
	}
	if c.Telegram.DailyLossLimitPercent <= 0 || c.Telegram.DailyLossLimitPercent > 100 {
		return fmt.Errorf("TELEGRAM_DAILY_LOSS_LIMIT_PERCENT должен быть в диапазоне (0, 100]")
	}
	seenTelegramIDs := make(map[int64]struct{}, len(c.Telegram.AllowedUserIDs))
	for _, id := range c.Telegram.AllowedUserIDs {
		if id <= 0 {
			return fmt.Errorf("TELEGRAM_ALLOWED_USER_IDS должен содержать только положительные числовые ID")
		}
		if _, exists := seenTelegramIDs[id]; exists {
			return fmt.Errorf("TELEGRAM_ALLOWED_USER_IDS содержит повторяющийся ID %d", id)
		}
		seenTelegramIDs[id] = struct{}{}
	}
	if err := c.PaperEngineConfig().Validate(); err != nil {
		return fmt.Errorf("PAPER TRADING: %w", err)
	}
	foundMoveWindow := false
	for _, window := range c.Analysis.MomentumWindows {
		if window == c.EntryStrategy.PriceChangeWindow {
			foundMoveWindow = true
			break
		}
	}
	if !foundMoveWindow {
		return fmt.Errorf("ENTRY_PRICE_CHANGE_WINDOW=%s отсутствует в ANALYSIS_MOMENTUM_WINDOWS", c.EntryStrategy.PriceChangeWindow)
	}
	if c.Analysis.OrderBookLevels < c.EntryStrategy.MinBookLevels {
		return fmt.Errorf("ANALYSIS_ORDERBOOK_LEVELS=%d не может быть меньше ENTRY_MIN_BOOK_LEVELS=%d", c.Analysis.OrderBookLevels, c.EntryStrategy.MinBookLevels)
	}
	if c.Strategy.VolumeMultiplier <= 0 {
		return fmt.Errorf("STRATEGY_VOLUME_MULTIPLIER должен быть больше нуля")
	}
	if c.Strategy.TrailingStopPercent <= 0 || c.Strategy.TrailingStopPercent >= 100 {
		return fmt.Errorf("STRATEGY_TRAILING_STOP_PERCENT должен быть в диапазоне от 0 до 100")
	}
	if c.Strategy.ReversalConfirmations <= 0 {
		return fmt.Errorf("STRATEGY_REVERSAL_CONFIRMATIONS должен быть больше нуля")
	}
	if c.Costs.FeeRate < 0 || c.Costs.FeeRate >= 1 {
		return fmt.Errorf("TRADING_FEE_RATE должен быть в диапазоне [0, 1)")
	}
	if c.Costs.SlippageBPS < 0 || c.Costs.SlippageBPS > 10000 {
		return fmt.Errorf("SLIPPAGE_BPS должен быть в диапазоне [0, 10000]")
	}
	return nil
}

func stringValue(key, fallback string) (string, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	return strings.TrimSpace(value), nil
}

func intValue(key string, fallback int) (int, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%s: ожидалось целое число: %w", key, err)
	}
	return parsed, nil
}

func floatValue(key string, fallback float64) (float64, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return 0, fmt.Errorf("%s: ожидалось число: %w", key, err)
	}
	return parsed, nil
}

func durationValue(key string, fallback time.Duration) (time.Duration, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%s: ожидалась длительность, например 2s или 500ms: %w", key, err)
	}
	return parsed, nil
}

func durationListValue(key string, fallback []time.Duration) ([]time.Duration, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return append([]time.Duration(nil), fallback...), nil
	}
	parts := strings.Split(value, ",")
	result := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		parsed, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil {
			return nil, fmt.Errorf("%s: некорректный интервал %q: %w", key, part, err)
		}
		result = append(result, parsed)
	}
	return result, nil
}

func boolValue(key string, fallback bool) (bool, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false, fmt.Errorf("%s: ожидалось true или false: %w", key, err)
	}
	return parsed, nil
}

func validateURL(key, rawURL, scheme string) error {
	parsed, err := url.ParseRequestURI(rawURL)
	if err != nil || parsed.Host == "" || parsed.Scheme != scheme {
		return fmt.Errorf("%s должен быть корректным URL со схемой %s", key, scheme)
	}
	return nil
}

func oneOf(value string, options ...string) bool {
	for _, option := range options {
		if value == option {
			return true
		}
	}
	return false
}

func (c Config) PaperEngineConfig() marketpaper.Config {
	return marketpaper.Config{
		Enabled: c.Paper.Enabled,
		InitialBalanceUSDT: c.Paper.InitialDepositUSDT,
		TargetRiskPercent: c.Paper.TargetRiskPercent,
		MaxRiskPercent: c.Paper.MaxRiskPercent,
		MaxLeverage: c.Paper.MaxLeverage,
		MaxMarginPercent: c.Paper.MaxMarginPercent,
		MaxOpenPositions: c.Paper.MaxOpenPositions,
		TrailingStopPercent: c.Paper.TrailingStopPercent,
		OrderFlowExitEnabled: c.Paper.OrderFlowExitEnabled,
		OrderFlowMinSignals: c.Paper.OrderFlowMinSignals,
		OrderFlowConsecutiveIntervals: c.Paper.OrderFlowConsecutiveIntervals,
		OrderFlowObservationInterval: c.Paper.OrderFlowObservationInterval,
		OrderFlowMaxObservationGap: c.Paper.OrderFlowMaxObservationGap,
		OrderFlowMaxTakerBuySharePercent: c.Paper.OrderFlowMaxTakerBuySharePercent,
		OrderFlowTakerBuyShareDropPP: c.Paper.OrderFlowTakerBuyShareDropPP,
		OrderFlowMinTakerSellSharePercent: c.Paper.OrderFlowMinTakerSellSharePercent,
		OrderFlowCVDMinDeclinePercent: c.Paper.OrderFlowCVDMinDeclinePercent,
		OrderFlowPriceStallMaxRisePercent: c.Paper.OrderFlowPriceStallMaxRisePercent,
		FeeRate: c.Costs.FeeRate,
		SlippageBPS: c.Costs.SlippageBPS,
		MaxBookAge: c.WebSocket.MaxDataAge,
	}
}

func int64ListValue(key string, fallback []int64) ([]int64, error) {
	raw, exists := os.LookupEnv(key)
	if !exists {
		return append([]int64(nil), fallback...), nil
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	values := make([]int64, 0, len(parts))
	seen := make(map[int64]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		value, err := strconv.ParseInt(part, 10, 64)
		if err != nil || value <= 0 {
			return nil, fmt.Errorf("%s должен содержать положительные числовые Telegram ID через запятую; некорректное значение %q", key, part)
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("%s содержит повторяющийся ID %d", key, value)
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	return values, nil
}
