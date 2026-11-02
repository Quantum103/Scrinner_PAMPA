package config

import (
	"strings"
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
	if cfg.Database.Path != "./data/volume_trader.db" || cfg.Database.BusyTimeout != 5*time.Second {
		t.Errorf("Database defaults = %+v", cfg.Database)
	}
	if MarketCategory != "linear" {
		t.Errorf("MarketCategory = %q", MarketCategory)
	}
}

func TestLoadEnvironmentOverrides(t *testing.T) {
	t.Setenv("DATA_UPDATE_INTERVAL", "750ms")
	t.Setenv("INSTRUMENT_REFRESH_INTERVAL", "5m")
	t.Setenv("WS_MAX_DATA_AGE", "8s")
	t.Setenv("APP_DB_PATH", t.TempDir()+"/paper-test.db")
	t.Setenv("APP_DB_BUSY_TIMEOUT", "8s")
	t.Setenv("WS_MAX_SYMBOLS_PER_CONNECTION", "20")
	t.Setenv("MAX_TRACKED_CONTRACTS", "25")
	t.Setenv("VIRTUAL_DEPOSIT_USDT", "25.5")
	t.Setenv("MARKET_DATA_ENABLED", "false")
	t.Setenv("ANALYSIS_RVOL_INTERVAL", "30s")
	t.Setenv("ANALYSIS_RVOL_LOOKBACK_INTERVALS", "12")
	t.Setenv("ANALYSIS_CVD_WINDOW", "8m")
	t.Setenv("ANALYSIS_MOMENTUM_WINDOWS", "10s,30s,2m")
	t.Setenv("ENTRY_PRICE_CHANGE_WINDOW", "2m")
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
	if !strings.HasSuffix(cfg.Database.Path, "/paper-test.db") || cfg.Database.BusyTimeout != 8*time.Second {
		t.Errorf("Database overrides = %+v", cfg.Database)
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


func TestEntryStrategyDefaultsAndEnvironmentOverrides(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.EntryStrategy.MinRVOL != 3 || cfg.EntryStrategy.MinTakerBuySharePercent != 65 ||
		cfg.EntryStrategy.ConfirmationObservations != 3 {
		t.Fatalf("entry defaults = %+v", cfg.EntryStrategy)
	}

	t.Setenv("ENTRY_MIN_RVOL", "4")
	t.Setenv("ENTRY_MIN_TAKER_BUY_SHARE_PERCENT", "70")
	t.Setenv("ENTRY_CONFIRMATION_OBSERVATIONS", "4")
	t.Setenv("ENTRY_SIGNAL_COOLDOWN", "90s")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load() with strategy overrides error = %v", err)
	}
	if cfg.EntryStrategy.MinRVOL != 4 || cfg.EntryStrategy.MinTakerBuySharePercent != 70 ||
		cfg.EntryStrategy.ConfirmationObservations != 4 || cfg.EntryStrategy.SignalCooldown != 90*time.Second {
		t.Fatalf("strategy overrides = %+v", cfg.EntryStrategy)
	}
}

func TestValidateRejectsEntryStrategyWithoutMatchingMomentumAndBookDepth(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	cfg.EntryStrategy.PriceChangeWindow = 2 * time.Minute
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when entry price window is missing from Momentum configuration")
	}
	cfg.EntryStrategy.PriceChangeWindow = 5 * time.Minute
	cfg.Analysis.OrderBookLevels = 3
	cfg.EntryStrategy.MinBookLevels = 5
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when analyzer book depth is smaller than strategy requirement")
	}
}


func TestPaperTradingDefaultsAndEnvironmentOverrides(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.Paper.Enabled || cfg.Paper.InitialDepositUSDT != 10 ||
		cfg.Paper.TargetRiskPercent != 2.5 || cfg.Paper.MaxRiskPercent != 3 ||
		cfg.Paper.MaxLeverage != 2 || cfg.Paper.MaxMarginPercent != 50 ||
		cfg.Paper.MaxOpenPositions != 1 || cfg.Paper.TrailingStopPercent != 2.5 ||
		!cfg.Paper.OrderFlowExitEnabled || cfg.Paper.OrderFlowMinSignals != 3 ||
		cfg.Paper.OrderFlowConsecutiveIntervals != 2 ||
		cfg.Paper.OrderFlowObservationInterval != time.Second ||
		cfg.Paper.OrderFlowMaxObservationGap != 3*time.Second ||
		cfg.Paper.OrderFlowMaxTakerBuySharePercent != 45 ||
		cfg.Paper.OrderFlowTakerBuyShareDropPP != 10 ||
		cfg.Paper.OrderFlowMinTakerSellSharePercent != 55 ||
		cfg.Paper.OrderFlowCVDMinDeclinePercent != 0.5 ||
		cfg.Paper.OrderFlowPriceStallMaxRisePercent != 0.02 {
		t.Fatalf("unexpected Paper Trading defaults: %+v", cfg.Paper)
	}

	t.Setenv("PAPER_TRADING_ENABLED", "false")
	t.Setenv("PAPER_INITIAL_BALANCE_USDT", "25")
	t.Setenv("PAPER_TARGET_RISK_PERCENT", "2")
	t.Setenv("PAPER_MAX_RISK_PERCENT", "2.8")
	t.Setenv("PAPER_MAX_LEVERAGE", "1.5")
	t.Setenv("PAPER_MAX_MARGIN_PERCENT", "40")
	t.Setenv("PAPER_MAX_OPEN_POSITIONS", "1")
	t.Setenv("PAPER_TRAILING_STOP_PERCENT", "3")
	t.Setenv("PAPER_ORDER_FLOW_EXIT_ENABLED", "false")
	t.Setenv("PAPER_ORDER_FLOW_MIN_SIGNALS", "4")
	t.Setenv("PAPER_ORDER_FLOW_CONSECUTIVE_INTERVALS", "3")
	t.Setenv("PAPER_ORDER_FLOW_OBSERVATION_INTERVAL", "2s")
	t.Setenv("PAPER_ORDER_FLOW_MAX_OBSERVATION_GAP", "5s")
	t.Setenv("PAPER_ORDER_FLOW_MAX_TAKER_BUY_SHARE_PERCENT", "42")
	t.Setenv("PAPER_ORDER_FLOW_TAKER_BUY_SHARE_DROP_PP", "12")
	t.Setenv("PAPER_ORDER_FLOW_MIN_TAKER_SELL_SHARE_PERCENT", "58")
	t.Setenv("PAPER_ORDER_FLOW_CVD_MIN_DECLINE_PERCENT", "0.8")
	t.Setenv("PAPER_ORDER_FLOW_PRICE_STALL_MAX_RISE_PERCENT", "0.03")
	t.Setenv("PAPER_FEE_RATE", "0.0007")
	t.Setenv("PAPER_SLIPPAGE_BPS", "8")

	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load() with Paper Trading overrides error = %v", err)
	}
	if cfg.Paper.Enabled || cfg.Paper.InitialDepositUSDT != 25 ||
		cfg.Paper.TargetRiskPercent != 2 || cfg.Paper.MaxRiskPercent != 2.8 ||
		cfg.Paper.MaxLeverage != 1.5 || cfg.Paper.MaxMarginPercent != 40 ||
		cfg.Paper.TrailingStopPercent != 3 || cfg.Paper.OrderFlowExitEnabled ||
		cfg.Paper.OrderFlowMinSignals != 4 || cfg.Paper.OrderFlowConsecutiveIntervals != 3 ||
		cfg.Paper.OrderFlowObservationInterval != 2*time.Second ||
		cfg.Paper.OrderFlowMaxObservationGap != 5*time.Second ||
		cfg.Paper.OrderFlowMaxTakerBuySharePercent != 42 ||
		cfg.Paper.OrderFlowTakerBuyShareDropPP != 12 ||
		cfg.Paper.OrderFlowMinTakerSellSharePercent != 58 ||
		cfg.Paper.OrderFlowCVDMinDeclinePercent != 0.8 ||
		cfg.Paper.OrderFlowPriceStallMaxRisePercent != 0.03 ||
		cfg.Costs.FeeRate != 0.0007 || cfg.Costs.SlippageBPS != 8 {
		t.Fatalf("Paper Trading overrides were not loaded: paper=%+v costs=%+v", cfg.Paper, cfg.Costs)
	}
}

func TestPaperTradingRejectsLeverageAndMarginAboveHardLimits(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	cfg.Paper.MaxLeverage = 2.1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when Paper Trading leverage exceeds 2x")
	}
	cfg.Paper.MaxLeverage = 2
	cfg.Paper.MaxMarginPercent = 50.1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when margin allocation exceeds 50%")
	}
	cfg.Paper.MaxMarginPercent = 50
	cfg.Paper.MaxOpenPositions = 2
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when more than one position is allowed")
	}
}


func TestPaperOrderFlowExitRejectsInvalidThresholds(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	cfg.Paper.OrderFlowMinSignals = 5
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when more than four Order Flow signal groups are required")
	}
	cfg.Paper.OrderFlowMinSignals = 3
	cfg.Paper.OrderFlowMaxObservationGap = time.Millisecond
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when maximum observation gap is shorter than observation interval")
	}
	cfg.Paper.OrderFlowMaxObservationGap = 3 * time.Second
	cfg.Paper.OrderFlowMinTakerSellSharePercent = 49
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when aggressive sell threshold is below 50%")
	}
}


func TestValidateRejectsEmptyDatabasePathAndInvalidTimeout(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	cfg.Database.Path = "  "
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty SQLite path")
	}
	cfg.Database.Path = "./data/volume_trader.db"
	cfg.Database.BusyTimeout = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for non-positive SQLite busy timeout")
	}
}
