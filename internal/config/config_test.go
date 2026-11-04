package config

import (
	"reflect"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("APP_LOG_LEVEL", "")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SCANNER_TIMEFRAMES", "")
	t.Setenv("SCANNER_RECONCILE_INTERVAL", "")
	t.Setenv("SCANNER_SCAN_INTERVAL", "")
	t.Setenv("TELEGRAM_BOT_TOKEN", "")
	t.Setenv("TELEGRAM_ACCESS_PASSWORD", "")
	t.Setenv("SCANNER_MARKET_ADX_SIDEWAYS_MAX", "")
	t.Setenv("SCANNER_MARKET_CHOP_SIDEWAYS_MIN", "")
	t.Setenv("SCANNER_MARKET_ADX_TREND_MIN", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Scanner.RSILength != 14 || cfg.Scanner.EMALength != 9 {
		t.Fatalf("unexpected indicator defaults: RSI=%d EMA=%d", cfg.Scanner.RSILength, cfg.Scanner.EMALength)
	}
	if cfg.Scanner.InstrumentLimit != 100 {
		t.Fatalf("instrument limit = %d, want 100", cfg.Scanner.InstrumentLimit)
	}
	if cfg.Scanner.ScanInterval != time.Hour {
		t.Fatalf("reconcile interval = %s, want 1h", cfg.Scanner.ScanInterval)
	}
	if cfg.Scanner.MarketADXSidewaysMax != 20 ||
		cfg.Scanner.MarketCHOPSidewaysMin != 61.8 ||
		cfg.Scanner.MarketADXTrendMin != 25 {
		t.Fatalf("unexpected market-regime defaults: %+v", cfg.Scanner)
	}
	if !reflect.DeepEqual(cfg.Scanner.Timeframes, []string{"30"}) {
		t.Fatalf("timeframes = %#v", cfg.Scanner.Timeframes)
	}
}

func TestLoadTelegramAccessPassword(t *testing.T) {
	t.Setenv("TELEGRAM_BOT_TOKEN", "test-token")
	t.Setenv("TELEGRAM_ACCESS_PASSWORD", "correct-horse-battery-staple")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Telegram.AccessPassword != "correct-horse-battery-staple" {
		t.Fatal("Telegram access password was not loaded")
	}
}

func TestLoadRejectsWeakTelegramAccessPassword(t *testing.T) {
	t.Setenv("TELEGRAM_BOT_TOKEN", "test-token")
	t.Setenv("TELEGRAM_ACCESS_PASSWORD", "short")
	if _, err := Load(); err == nil {
		t.Fatal("Load() expected an error for a short Telegram access password")
	}
}

func TestValidateAccepts30MinuteTimeframe(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	cfg.Scanner.Timeframes = []string{"30"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() rejected 30-minute timeframe: %v", err)
	}

	cfg.Scanner.Timeframes = []string{"1"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() expected an error for unsupported timeframe")
	}
}

func TestLoadRejectsInvalidDuration(t *testing.T) {
	t.Setenv("APP_SHUTDOWN_TIMEOUT", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("Load() expected an error for invalid duration")
	}
}

func TestLoadScannerScanInterval(t *testing.T) {
	t.Setenv("SCANNER_SCAN_INTERVAL", "45s")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Scanner.ScanInterval != 45*time.Second {
		t.Fatalf("scan interval = %s, want 45s", cfg.Scanner.ScanInterval)
	}
}

func TestValidateRejectsInvalidScannerLimits(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Scanner.HistoryCandles = 1001
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() expected an error for more than 1000 REST candles")
	}

	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Scanner.ScanInterval = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() expected an error for a zero scanner interval")
	}
}

func TestLoadReconcileIntervalPrefersNewVariable(t *testing.T) {
	t.Setenv("SCANNER_SCAN_INTERVAL", "45s")
	t.Setenv("SCANNER_RECONCILE_INTERVAL", "10m")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Scanner.ScanInterval != 10*time.Minute {
		t.Fatalf("reconcile interval = %s, want 10m", cfg.Scanner.ScanInterval)
	}
}

func TestLoadMarketRegimeThresholds(t *testing.T) {
	t.Setenv("SCANNER_MARKET_ADX_SIDEWAYS_MAX", "18.5")
	t.Setenv("SCANNER_MARKET_CHOP_SIDEWAYS_MIN", "66.2")
	t.Setenv("SCANNER_MARKET_ADX_TREND_MIN", "27.5")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Scanner.MarketADXSidewaysMax != 18.5 ||
		cfg.Scanner.MarketCHOPSidewaysMin != 66.2 ||
		cfg.Scanner.MarketADXTrendMin != 27.5 {
		t.Fatalf("market-regime thresholds = %+v", cfg.Scanner)
	}
}

func TestLoadRejectsInvalidMarketRegimeThresholds(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "sideways ADX is not positive", key: "SCANNER_MARKET_ADX_SIDEWAYS_MAX", value: "0"},
		{name: "sideways CHOP is out of range", key: "SCANNER_MARKET_CHOP_SIDEWAYS_MIN", value: "100"},
		{name: "trend ADX is not finite", key: "SCANNER_MARKET_ADX_TREND_MIN", value: "NaN"},
		{name: "sideways ADX overlaps trend ADX", key: "SCANNER_MARKET_ADX_SIDEWAYS_MAX", value: "25"},
		{name: "trend ADX is below sideways ADX", key: "SCANNER_MARKET_ADX_TREND_MIN", value: "19"},
		{name: "non-numeric threshold", key: "SCANNER_MARKET_CHOP_SIDEWAYS_MIN", value: "high"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("SCANNER_MARKET_ADX_SIDEWAYS_MAX", "")
			t.Setenv("SCANNER_MARKET_CHOP_SIDEWAYS_MIN", "")
			t.Setenv("SCANNER_MARKET_ADX_TREND_MIN", "")
			t.Setenv(test.key, test.value)
			if _, err := Load(); err == nil {
				t.Fatal("Load() expected an error for invalid market-regime thresholds")
			}
		})
	}
}
