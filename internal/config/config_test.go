package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Bybit.RESTURL != "https://api.bybit.com" {
		t.Errorf("RESTURL = %q", cfg.Bybit.RESTURL)
	}
	if cfg.Bybit.PublicWSURL != "wss://stream.bybit.com/v5/public/linear" {
		t.Errorf("PublicWSURL = %q", cfg.Bybit.PublicWSURL)
	}
	if cfg.MarketData.UpdateInterval != 2*time.Second {
		t.Errorf("UpdateInterval = %s", cfg.MarketData.UpdateInterval)
	}
	if cfg.Paper.InitialDepositUSDT != 10 {
		t.Errorf("InitialDepositUSDT = %v", cfg.Paper.InitialDepositUSDT)
	}
	if MarketCategory != "linear" {
		t.Errorf("MarketCategory = %q", MarketCategory)
	}
}

func TestLoadEnvironmentOverrides(t *testing.T) {
	t.Setenv("DATA_UPDATE_INTERVAL", "750ms")
	t.Setenv("MAX_TRACKED_CONTRACTS", "25")
	t.Setenv("VIRTUAL_DEPOSIT_USDT", "25.5")
	t.Setenv("MARKET_DATA_ENABLED", "false")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.MarketData.UpdateInterval != 750*time.Millisecond {
		t.Errorf("UpdateInterval = %s", cfg.MarketData.UpdateInterval)
	}
	if cfg.MarketData.MaxTrackedSymbols != 25 {
		t.Errorf("MaxTrackedSymbols = %d", cfg.MarketData.MaxTrackedSymbols)
	}
	if cfg.Paper.InitialDepositUSDT != 25.5 {
		t.Errorf("InitialDepositUSDT = %v", cfg.Paper.InitialDepositUSDT)
	}
	if cfg.MarketData.Enabled {
		t.Error("MarketData.Enabled = true, want false")
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	t.Setenv("DATA_UPDATE_INTERVAL", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want an invalid duration error")
	}
}

func TestValidateRejectsNonPublicWebSocketScheme(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	cfg.Bybit.PublicWSURL = "wss://stream.bybit.com/v5/public/linear"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() rejected the public Linear URL: %v", err)
	}
	cfg.Bybit.PublicWSURL = "wss://stream.bybit.com/v5/private"
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want a private stream URL to be rejected")
	}
	cfg.Bybit.PublicWSURL = "https://stream.bybit.com/v5/public/linear"
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want a WebSocket scheme error")
	}
}
