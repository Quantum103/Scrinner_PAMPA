package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	App      AppConfig
	Database DatabaseConfig
	Bybit    BybitConfig
	Spot     SpotConfig
	Telegram TelegramConfig
}

type AppConfig struct {
	LogLevel        string
	HTTPAddr        string
	ShutdownTimeout time.Duration
	MigrationsDir   string
	DataDir         string
}

type DatabaseConfig struct {
	URL      string
	MaxConns int32
	MinConns int32
}

type BybitConfig struct {
	RESTURL         string
	SpotPublicWSURL string
}

type SpotConfig struct {
	MarketRefreshInterval time.Duration
}

type TelegramConfig struct {
	BotToken       string
	AccessPassword string
}

func Load() (Config, error) {
	cfg := Config{
		App: AppConfig{
			LogLevel:        strings.ToLower(env("APP_LOG_LEVEL", "info")),
			HTTPAddr:        env("APP_HTTP_ADDR", ":8080"),
			MigrationsDir:   env("MIGRATIONS_DIR", "./migrations"),
			DataDir:         env("APP_DATA_DIR", "./data"),
			ShutdownTimeout: 10 * time.Second,
		},
		Database: DatabaseConfig{
			URL:      env("DATABASE_URL", "postgres://spot_scanner:change_me@localhost:5432/spot_scanner?sslmode=disable"),
			MaxConns: 20,
			MinConns: 2,
		},
		Bybit: BybitConfig{
			RESTURL:         env("BYBIT_REST_URL", "https://api.bybit.com"),
			SpotPublicWSURL: env("BYBIT_PUBLIC_SPOT_WS_URL", "wss://stream.bybit.com/v5/public/spot"),
		},
		Spot: SpotConfig{MarketRefreshInterval: time.Minute},
	}

	var err error
	if cfg.App.ShutdownTimeout, err = durationEnv("APP_SHUTDOWN_TIMEOUT", cfg.App.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if cfg.Spot.MarketRefreshInterval, err = durationEnv("SPOT_MARKET_REFRESH_INTERVAL", cfg.Spot.MarketRefreshInterval); err != nil {
		return Config{}, err
	}
	if cfg.Database.MaxConns, err = int32Env("DB_POOL_MAX_CONNS", cfg.Database.MaxConns); err != nil {
		return Config{}, err
	}
	if cfg.Database.MinConns, err = int32Env("DB_POOL_MIN_CONNS", cfg.Database.MinConns); err != nil {
		return Config{}, err
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
	if strings.TrimSpace(c.App.DataDir) == "" {
		return fmt.Errorf("APP_DATA_DIR must not be empty")
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
	if !strings.HasPrefix(c.Bybit.SpotPublicWSURL, "wss://") {
		return fmt.Errorf("BYBIT_PUBLIC_SPOT_WS_URL must use wss://")
	}
	if c.Spot.MarketRefreshInterval <= 0 {
		return fmt.Errorf("SPOT_MARKET_REFRESH_INTERVAL must be positive")
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

func int32Env(key string, fallback int32) (int32, error) {
	value, err := intEnv(key, int(fallback))
	if err != nil {
		return 0, err
	}
	return int32(value), nil
}
