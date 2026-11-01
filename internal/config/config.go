package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	marketanalysis "github.com/Quantum103/Scrinner_PAMPA/internal/analysis"
)

const MarketCategory = "linear"

type Config struct {
	LogLevel   string
	Bybit      BybitConfig
	MarketData MarketDataConfig
	WebSocket  WebSocketConfig
	Analysis   marketanalysis.Config
	Strategy   StrategyConfig
	Paper      PaperConfig
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
	InitialDepositUSDT  float64
	RiskPerTradePercent float64
	MaxOpenPositions    int
}

type CostsConfig struct {
	FeeRate     float64
	SlippageBPS float64
}

type TelegramConfig struct {
	BotToken string
	ChatID   string
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
		Analysis: marketanalysis.DefaultConfig(),
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
		Paper: PaperConfig{
			InitialDepositUSDT:  10,
			RiskPerTradePercent: 1,
			MaxOpenPositions:    1,
		},
		Costs: CostsConfig{
			FeeRate:     0.00055,
			SlippageBPS: 5,
		},
		Telegram: TelegramConfig{},
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
	if cfg.Strategy.VolumeMultiplier, err = floatValue("STRATEGY_VOLUME_MULTIPLIER", cfg.Strategy.VolumeMultiplier); err != nil {
		return Config{}, err
	}
	if cfg.Strategy.TrailingStopPercent, err = floatValue("STRATEGY_TRAILING_STOP_PERCENT", cfg.Strategy.TrailingStopPercent); err != nil {
		return Config{}, err
	}
	if cfg.Strategy.ReversalConfirmations, err = intValue("STRATEGY_REVERSAL_CONFIRMATIONS", cfg.Strategy.ReversalConfirmations); err != nil {
		return Config{}, err
	}
	if cfg.Paper.InitialDepositUSDT, err = floatValue("VIRTUAL_DEPOSIT_USDT", cfg.Paper.InitialDepositUSDT); err != nil {
		return Config{}, err
	}
	if cfg.Paper.RiskPerTradePercent, err = floatValue("RISK_PER_TRADE_PERCENT", cfg.Paper.RiskPerTradePercent); err != nil {
		return Config{}, err
	}
	if cfg.Paper.MaxOpenPositions, err = intValue("MAX_OPEN_POSITIONS", cfg.Paper.MaxOpenPositions); err != nil {
		return Config{}, err
	}
	if cfg.Costs.FeeRate, err = floatValue("TRADING_FEE_RATE", cfg.Costs.FeeRate); err != nil {
		return Config{}, err
	}
	if cfg.Costs.SlippageBPS, err = floatValue("SLIPPAGE_BPS", cfg.Costs.SlippageBPS); err != nil {
		return Config{}, err
	}
	if cfg.Telegram.BotToken, err = stringValue("TELEGRAM_BOT_TOKEN", ""); err != nil {
		return Config{}, err
	}
	if cfg.Telegram.ChatID, err = stringValue("TELEGRAM_CHAT_ID", ""); err != nil {
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
	if c.Strategy.VolumeMultiplier <= 0 {
		return fmt.Errorf("STRATEGY_VOLUME_MULTIPLIER должен быть больше нуля")
	}
	if c.Strategy.TrailingStopPercent <= 0 || c.Strategy.TrailingStopPercent >= 100 {
		return fmt.Errorf("STRATEGY_TRAILING_STOP_PERCENT должен быть в диапазоне от 0 до 100")
	}
	if c.Strategy.ReversalConfirmations <= 0 {
		return fmt.Errorf("STRATEGY_REVERSAL_CONFIRMATIONS должен быть больше нуля")
	}
	if c.Paper.InitialDepositUSDT <= 0 {
		return fmt.Errorf("VIRTUAL_DEPOSIT_USDT должен быть больше нуля")
	}
	if c.Paper.RiskPerTradePercent <= 0 || c.Paper.RiskPerTradePercent > 100 {
		return fmt.Errorf("RISK_PER_TRADE_PERCENT должен быть больше 0 и не превышать 100")
	}
	if c.Paper.MaxOpenPositions <= 0 {
		return fmt.Errorf("MAX_OPEN_POSITIONS должен быть больше нуля")
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
