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
		t.Errorf("UpdateInterval = %s", cfg.MarketData.UpdateInterval)
	}
	if cfg.MarketData.InstrumentRefreshInterval != 15*time.Minute {
		t.Errorf("InstrumentRefreshInterval = %s", cfg.MarketData.InstrumentRefreshInterval)
	}
	if cfg.Analysis.RVOLInterval != time.Minute || cfg.Analysis.RVOLLookbackIntervals != 20 {
		t.Errorf("RVOL defaults = %s / %d, want 1m / 20", cfg.Analysis.RVOLInterval, cfg.Analysis.RVOLLookbackIntervals)
	}
	if cfg.Analysis.CVDWindow != 15*time.Minute || len(cfg.Analysis.MomentumWindows) != 3 {
		t.Errorf("analysis windows were not initialized: %+v", cfg.Analysis)
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
	t.Setenv("INSTRUMENT_REFRESH_INTERVAL", "5m")
	t.Setenv("WS_MAX_DATA_AGE", "8s")
	t.Setenv("WS_MAX_SYMBOLS_PER_CONNECTION", "20")
	t.Setenv("MAX_TRACKED_CONTRACTS", "25")
	t.Setenv("VIRTUAL_DEPOSIT_USDT", "25.5")
	t.Setenv("MARKET_DATA_ENABLED", "false")
	t.Setenv("ANALYSIS_RVOL_INTERVAL", "30s")
	t.Setenv("ANALYSIS_RVOL_LOOKBACK_INTERVALS", "12")
	t.Setenv("ANALYSIS_CVD_WINDOW", "8m")
	t.Setenv("ANALYSIS_MOMENTUM_WINDOWS", "10s,30s,2m")
	t.Setenv("ANALYSIS_ABSORPTION_MIN_BUY_SHARE", "0.8")
	t.Setenv("ANALYSIS_ORDERBOOK_LEVELS", "5")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() вернула ошибку: %v", err)
	}
	if cfg.MarketData.UpdateInterval != 750*time.Millisecond {
		t.Errorf("UpdateInterval = %s", cfg.MarketData.UpdateInterval)
	}
	if cfg.MarketData.InstrumentRefreshInterval != 5*time.Minute {
		t.Errorf("InstrumentRefreshInterval = %s", cfg.MarketData.InstrumentRefreshInterval)
	}
	if cfg.MarketData.MaxTrackedSymbols != 25 {
		t.Errorf("MaxTrackedSymbols = %d", cfg.MarketData.MaxTrackedSymbols)
	}
	if cfg.Paper.InitialDepositUSDT != 25.5 {
		t.Errorf("InitialDepositUSDT = %v", cfg.Paper.InitialDepositUSDT)
	}
	if cfg.MarketData.Enabled {
		t.Error("MarketData.Enabled = true, ожидалось false")
	}
	if cfg.Analysis.RVOLInterval != 30*time.Second || cfg.Analysis.RVOLLookbackIntervals != 12 {
		t.Errorf("RVOL overrides = %s / %d", cfg.Analysis.RVOLInterval, cfg.Analysis.RVOLLookbackIntervals)
	}
	if cfg.Analysis.CVDWindow != 8*time.Minute || len(cfg.Analysis.MomentumWindows) != 3 || cfg.Analysis.OrderBookLevels != 5 {
		t.Errorf("analysis overrides were not loaded: %+v", cfg.Analysis)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	t.Setenv("DATA_UPDATE_INTERVAL", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("Load() не вернула ошибку для некорректного интервала")
	}
}

func TestValidateRejectsNonPublicWebSocketScheme(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() вернула ошибку: %v", err)
	}
	cfg.Bybit.PublicWSURL = "wss://stream.bybit.com/v5/public/linear"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() отклонила публичный Linear URL: %v", err)
	}
	cfg.Bybit.PublicWSURL = "wss://stream.bybit.com/v5/private"
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() должна отклонять private WebSocket URL")
	}
	cfg.Bybit.PublicWSURL = "https://stream.bybit.com/v5/public/linear"
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() должна отклонять неверную схему WebSocket URL")
	}
}

func TestValidateRejectsInvalidInstrumentRefreshInterval(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() вернула ошибку: %v", err)
	}
	cfg.MarketData.InstrumentRefreshInterval = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() должна отклонять нулевой интервал обновления контрактов")
	}
}

func TestValidateRejectsUnsafeWebSocketSubscriptionLimits(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() вернула ошибку: %v", err)
	}
	cfg.WebSocket.MaxSymbolsPerConnection = 26
	if err := cfg.Validate(); err == nil {
		t.Fatal("ожидалась ошибка для более чем 25 инструментов на соединение")
	}
	cfg.WebSocket.MaxSymbolsPerConnection = 25
	cfg.WebSocket.MaxArgsPerSubscribe = 11
	if err := cfg.Validate(); err == nil {
		t.Fatal("ожидалась ошибка для пакета более чем из 10 аргументов")
	}
}

func TestValidateRejectsInvalidAnalysisConfiguration(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() вернула ошибку: %v", err)
	}
	cfg.Analysis.RVOLLookbackIntervals = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("ожидалась ошибка для нулевого количества интервалов RVOL")
	}
}
