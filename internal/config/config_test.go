package config

import (
	"testing"
	"time"
)

func TestLoadSpotOnlyDefaults(t *testing.T) {
	for _, key := range []string{
		"APP_LOG_LEVEL", "APP_DATA_DIR", "DATABASE_URL",
		"TELEGRAM_BOT_TOKEN", "TELEGRAM_ACCESS_PASSWORD",
		"BYBIT_PUBLIC_SPOT_WS_URL", "SPOT_MARKET_REFRESH_INTERVAL",
	} {
		t.Setenv(key, "")
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Bybit.SpotPublicWSURL != "wss://stream.bybit.com/v5/public/spot" {
		t.Fatalf("unexpected Spot websocket URL: %q", cfg.Bybit.SpotPublicWSURL)
	}
	if cfg.Spot.MarketRefreshInterval != time.Minute {
		t.Fatalf("refresh interval = %s, want 1m", cfg.Spot.MarketRefreshInterval)
	}
	if cfg.App.DataDir != "./data" {
		t.Fatalf("data directory = %q, want ./data", cfg.App.DataDir)
	}
}

func TestLoadSpotConfiguration(t *testing.T) {
	t.Setenv("BYBIT_PUBLIC_SPOT_WS_URL", "wss://spot.example.test/v5/public/spot")
	t.Setenv("SPOT_MARKET_REFRESH_INTERVAL", "45s")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Bybit.SpotPublicWSURL != "wss://spot.example.test/v5/public/spot" ||
		cfg.Spot.MarketRefreshInterval != 45*time.Second {
		t.Fatalf("unexpected Spot configuration: %+v %+v", cfg.Bybit, cfg.Spot)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "invalid refresh duration", key: "SPOT_MARKET_REFRESH_INTERVAL", value: "bad"},
		{name: "invalid websocket scheme", key: "BYBIT_PUBLIC_SPOT_WS_URL", value: "https://example.test"},
		{name: "short Telegram password", key: "TELEGRAM_ACCESS_PASSWORD", value: "short"},
		{name: "invalid log level", key: "APP_LOG_LEVEL", value: "trace"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("TELEGRAM_BOT_TOKEN", "test-token")
			t.Setenv("TELEGRAM_ACCESS_PASSWORD", "long-enough-test-password")
			t.Setenv("SPOT_MARKET_REFRESH_INTERVAL", "")
			t.Setenv("BYBIT_PUBLIC_SPOT_WS_URL", "")
			t.Setenv("APP_LOG_LEVEL", "")
			t.Setenv(test.key, test.value)
			if _, err := Load(); err == nil {
				t.Fatal("Load() expected an error")
			}
		})
	}
}

func TestLoadPoolConfiguration(t *testing.T) {
	t.Setenv("DB_POOL_MAX_CONNS", "12")
	t.Setenv("DB_POOL_MIN_CONNS", "3")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.MaxConns != 12 || cfg.Database.MinConns != 3 {
		t.Fatalf("unexpected pool config: %+v", cfg.Database)
	}
}
