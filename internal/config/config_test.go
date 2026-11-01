package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() вернула ошибку: %v", err)
	}
	if cfg.Bybit.RESTURL != "https://api.bybit.com" {
		t.Errorf("RESTURL = %q", cfg.Bybit.RESTURL)
	}
	if cfg.Bybit.PublicWSURL != "wss://stream.bybit.com/v5/public/linear" {
		t.Errorf("PublicWSURL = %q", cfg.Bybit.PublicWSURL)
	}
	if cfg.MarketData.UpdateInterval != 2*time.Second {
		t.Errorf("интервал обновления = %s", cfg.MarketData.UpdateInterval)
	}
	if cfg.MarketData.InstrumentRefreshInterval != 15*time.Minute {
		t.Errorf("интервал обновления контрактов = %s", cfg.MarketData.InstrumentRefreshInterval)
	}
	if cfg.Paper.InitialDepositUSDT != 10 {
		t.Errorf("виртуальный депозит = %v", cfg.Paper.InitialDepositUSDT)
	}
	if MarketCategory != "linear" {
		t.Errorf("категория рынка = %q", MarketCategory)
	}
}

func TestLoadEnvironmentOverrides(t *testing.T) {
	t.Setenv("DATA_UPDATE_INTERVAL", "750ms")
	t.Setenv("INSTRUMENT_REFRESH_INTERVAL", "5m")
	t.Setenv("MAX_TRACKED_CONTRACTS", "25")
	t.Setenv("VIRTUAL_DEPOSIT_USDT", "25.5")
	t.Setenv("MARKET_DATA_ENABLED", "false")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() вернула ошибку: %v", err)
	}
	if cfg.MarketData.UpdateInterval != 750*time.Millisecond {
		t.Errorf("интервал обновления = %s", cfg.MarketData.UpdateInterval)
	}
	if cfg.MarketData.InstrumentRefreshInterval != 5*time.Minute {
		t.Errorf("интервал обновления контрактов = %s", cfg.MarketData.InstrumentRefreshInterval)
	}
	if cfg.MarketData.MaxTrackedSymbols != 25 {
		t.Errorf("максимум контрактов = %d", cfg.MarketData.MaxTrackedSymbols)
	}
	if cfg.Paper.InitialDepositUSDT != 25.5 {
		t.Errorf("виртуальный депозит = %v", cfg.Paper.InitialDepositUSDT)
	}
	if cfg.MarketData.Enabled {
		t.Error("MARKET_DATA_ENABLED должен быть false")
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	t.Setenv("DATA_UPDATE_INTERVAL", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("ожидалась ошибка при некорректном интервале обновления")
	}
}

func TestValidateRejectsNonPublicWebSocketScheme(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() вернула ошибку: %v", err)
	}
	cfg.Bybit.PublicWSURL = "wss://stream.bybit.com/v5/public/linear"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("публичный WebSocket URL отклонён: %v", err)
	}
	cfg.Bybit.PublicWSURL = "wss://stream.bybit.com/v5/private"
	if err := cfg.Validate(); err == nil {
		t.Fatal("закрытый WebSocket URL должен быть отклонён")
	}
}

func TestValidateRejectsInvalidInstrumentRefreshInterval(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() вернула ошибку: %v", err)
	}
	cfg.MarketData.InstrumentRefreshInterval = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("интервал обновления списка контрактов должен быть больше нуля")
	}
}
