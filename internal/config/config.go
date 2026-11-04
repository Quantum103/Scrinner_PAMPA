package config

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config contains application settings loaded from environment variables.
type Config struct {
	App      AppConfig
	Database DatabaseConfig
	Bybit    BybitConfig
	Scanner  ScannerConfig
	Telegram TelegramConfig
}

type AppConfig struct {
	LogLevel        string
	HTTPAddr        string
	ShutdownTimeout time.Duration
	MigrationsDir   string
}

type DatabaseConfig struct {
	URL      string
	MaxConns int32
	MinConns int32
}

type BybitConfig struct {
	RESTURL     string
	PublicWSURL string
}

type ScannerConfig struct {
	Timeframes            []string
	RSILength             int
	EMALength             int
	InstrumentLimit       int
	RefreshInterval       time.Duration
	ScanInterval          time.Duration
	HistoryCandles        int
	MarketADXSidewaysMax  float64
	MarketCHOPSidewaysMin float64
	MarketADXTrendMin     float64
}

type TelegramConfig struct {
	BotToken       string
	AccessPassword string
}

// Load reads and validates the environment configuration.
func Load() (Config, error) {
	cfg := Config{
		App: AppConfig{
			LogLevel:        strings.ToLower(env("APP_LOG_LEVEL", "info")),
			HTTPAddr:        env("APP_HTTP_ADDR", ":8080"),
			MigrationsDir:   env("MIGRATIONS_DIR", "./migrations"),
			ShutdownTimeout: 10 * time.Second,
		},
		Database: DatabaseConfig{
			URL:      env("DATABASE_URL", "postgres://rsi_scanner:change_me@localhost:5432/rsi_scanner?sslmode=disable"),
			MaxConns: 20,
			MinConns: 2,
		},
		Bybit: BybitConfig{
			RESTURL:     env("BYBIT_REST_URL", "https://api.bybit.com"),
			PublicWSURL: env("BYBIT_PUBLIC_WS_URL", "wss://stream.bybit.com/v5/public/linear"),
		},
		Scanner: ScannerConfig{
			Timeframes:            []string{"30"},
			RSILength:             14,
			EMALength:             9,
			InstrumentLimit:       100,
			RefreshInterval:       15 * time.Minute,
			ScanInterval:          time.Hour,
			HistoryCandles:        300,
			MarketADXSidewaysMax:  20,
			MarketCHOPSidewaysMin: 61.8,
			MarketADXTrendMin:     25,
		},
	}

	var err error
	if cfg.App.ShutdownTimeout, err = durationEnv("APP_SHUTDOWN_TIMEOUT", cfg.App.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if cfg.Scanner.RefreshInterval, err = durationEnv("SCANNER_REFRESH_INTERVAL", cfg.Scanner.RefreshInterval); err != nil {
		return Config{}, err
	}
	reconcileEnv := "SCANNER_RECONCILE_INTERVAL"
	if strings.TrimSpace(os.Getenv(reconcileEnv)) == "" {
		// Keep the previous variable working for existing .env files.
		reconcileEnv = "SCANNER_SCAN_INTERVAL"
	}
	if cfg.Scanner.ScanInterval, err = durationEnv(reconcileEnv, cfg.Scanner.ScanInterval); err != nil {
		return Config{}, err
	}
	if cfg.Database.MaxConns, err = int32Env("DB_POOL_MAX_CONNS", cfg.Database.MaxConns); err != nil {
		return Config{}, err
	}
	if cfg.Database.MinConns, err = int32Env("DB_POOL_MIN_CONNS", cfg.Database.MinConns); err != nil {
		return Config{}, err
	}
	if cfg.Scanner.RSILength, err = intEnv("RSI_LENGTH", cfg.Scanner.RSILength); err != nil {
		return Config{}, err
	}
	if cfg.Scanner.EMALength, err = intEnv("EMA_LENGTH", cfg.Scanner.EMALength); err != nil {
		return Config{}, err
	}
	if cfg.Scanner.InstrumentLimit, err = intEnv("SCANNER_INSTRUMENT_LIMIT", cfg.Scanner.InstrumentLimit); err != nil {
		return Config{}, err
	}
	if cfg.Scanner.HistoryCandles, err = intEnv("SCANNER_HISTORY_CANDLES", cfg.Scanner.HistoryCandles); err != nil {
		return Config{}, err
	}
	if cfg.Scanner.MarketADXSidewaysMax, err = floatEnv("SCANNER_MARKET_ADX_SIDEWAYS_MAX", cfg.Scanner.MarketADXSidewaysMax); err != nil {
		return Config{}, err
	}
	if cfg.Scanner.MarketCHOPSidewaysMin, err = floatEnv("SCANNER_MARKET_CHOP_SIDEWAYS_MIN", cfg.Scanner.MarketCHOPSidewaysMin); err != nil {
		return Config{}, err
	}
	if cfg.Scanner.MarketADXTrendMin, err = floatEnv("SCANNER_MARKET_ADX_TREND_MIN", cfg.Scanner.MarketADXTrendMin); err != nil {
		return Config{}, err
	}
	if value := strings.TrimSpace(os.Getenv("SCANNER_TIMEFRAMES")); value != "" {
		cfg.Scanner.Timeframes = splitCSV(value)
	}
	cfg.Telegram.BotToken = strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN"))
	cfg.Telegram.AccessPassword = strings.TrimSpace(os.Getenv("TELEGRAM_ACCESS_PASSWORD"))

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.App.LogLevel != "debug" && c.App.LogLevel != "info" && c.App.LogLevel != "warn" && c.App.LogLevel != "error" {
		return fmt.Errorf("APP_LOG_LEVEL must be debug, info, warn or error")
	}
	if strings.TrimSpace(c.App.HTTPAddr) == "" {
		return fmt.Errorf("APP_HTTP_ADDR must not be empty")
	}
	if c.App.ShutdownTimeout <= 0 {
		return fmt.Errorf("APP_SHUTDOWN_TIMEOUT must be positive")
	}
	if strings.TrimSpace(c.App.MigrationsDir) == "" {
		return fmt.Errorf("MIGRATIONS_DIR must not be empty")
	}
	if c.Telegram.BotToken != "" && len([]rune(c.Telegram.AccessPassword)) < 12 {
		return fmt.Errorf("TELEGRAM_ACCESS_PASSWORD must contain at least 12 characters when TELEGRAM_BOT_TOKEN is set")
	}
	if !strings.HasPrefix(c.Database.URL, "postgres://") && !strings.HasPrefix(c.Database.URL, "postgresql://") {
		return fmt.Errorf("DATABASE_URL must use postgres:// or postgresql://")
	}
	if c.Database.MaxConns < 1 || c.Database.MinConns < 0 || c.Database.MinConns > c.Database.MaxConns {
		return fmt.Errorf("database pool limits are invalid: min=%d max=%d", c.Database.MinConns, c.Database.MaxConns)
	}
	if !strings.HasPrefix(c.Bybit.RESTURL, "https://") {
		return fmt.Errorf("BYBIT_REST_URL must use https://")
	}
	if !strings.HasPrefix(c.Bybit.PublicWSURL, "wss://") {
		return fmt.Errorf("BYBIT_PUBLIC_WS_URL must use wss://")
	}
	if c.Scanner.RSILength < 2 || c.Scanner.RSILength > 500 || c.Scanner.EMALength < 1 || c.Scanner.EMALength > 500 {
		return fmt.Errorf("RSI_LENGTH must be 2..500 and EMA_LENGTH must be 1..500")
	}
	if c.Scanner.InstrumentLimit < 1 || c.Scanner.InstrumentLimit > 10000 {
		return fmt.Errorf("SCANNER_INSTRUMENT_LIMIT must be positive")
	}
	if c.Scanner.RefreshInterval <= 0 {
		return fmt.Errorf("SCANNER_REFRESH_INTERVAL must be positive")
	}
	if c.Scanner.ScanInterval <= 0 {
		return fmt.Errorf("SCANNER_RECONCILE_INTERVAL must be positive")
	}
	if c.Scanner.HistoryCandles < c.Scanner.RSILength+c.Scanner.EMALength || c.Scanner.HistoryCandles > 1000 {
		return fmt.Errorf("SCANNER_HISTORY_CANDLES must be between RSI_LENGTH + EMA_LENGTH and 1000")
	}
	if !(c.Scanner.MarketADXSidewaysMax > 0 && c.Scanner.MarketADXSidewaysMax < 100) {
		return fmt.Errorf("SCANNER_MARKET_ADX_SIDEWAYS_MAX must be greater than 0 and less than 100")
	}
	if !(c.Scanner.MarketCHOPSidewaysMin > 0 && c.Scanner.MarketCHOPSidewaysMin < 100) {
		return fmt.Errorf("SCANNER_MARKET_CHOP_SIDEWAYS_MIN must be greater than 0 and less than 100")
	}
	if !(c.Scanner.MarketADXTrendMin > 0 && c.Scanner.MarketADXTrendMin < 100) {
		return fmt.Errorf("SCANNER_MARKET_ADX_TREND_MIN must be greater than 0 and less than 100")
	}
	if c.Scanner.MarketADXSidewaysMax >= c.Scanner.MarketADXTrendMin {
		return fmt.Errorf("SCANNER_MARKET_ADX_SIDEWAYS_MAX must be lower than SCANNER_MARKET_ADX_TREND_MIN")
	}
	if len(c.Scanner.Timeframes) == 0 {
		return fmt.Errorf("at least one scanner timeframe must be enabled")
	}
	seen := make(map[string]struct{}, len(c.Scanner.Timeframes))
	for _, timeframe := range c.Scanner.Timeframes {
		if timeframe != "5" && timeframe != "15" && timeframe != "30" && timeframe != "60" {
			return fmt.Errorf("unsupported scanner timeframe %q: supported values are 5, 15, 30 and 60", timeframe)
		}
		if _, ok := seen[timeframe]; ok {
			return fmt.Errorf("duplicate scanner timeframe %q", timeframe)
		}
		seen[timeframe] = struct{}{}
	}
	return nil
}

func env(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return fallback
}

func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return parsed, nil
}

func intEnv(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return parsed, nil
}

func floatEnv(key string, fallback float64) (float64, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number: %w", key, err)
	}
	if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("%s must be a finite number", key)
	}
	return parsed, nil
}

func int32Env(key string, fallback int32) (int32, error) {
	value, err := intEnv(key, int(fallback))
	if err != nil {
		return 0, err
	}
	return int32(value), nil
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
