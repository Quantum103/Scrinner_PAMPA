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
	t.Setenv("TELEGRAM_ALLOWED_USER_IDS", "")

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
	if !reflect.DeepEqual(cfg.Scanner.Timeframes, []string{"5", "15", "60"}) {
		t.Fatalf("timeframes = %#v", cfg.Scanner.Timeframes)
	}
}

func TestLoadTelegramIDs(t *testing.T) {
	t.Setenv("TELEGRAM_ALLOWED_USER_IDS", "123, 456,123")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !reflect.DeepEqual(cfg.Telegram.AllowedUsers, []int64{123, 456}) {
		t.Fatalf("allowed IDs = %#v", cfg.Telegram.AllowedUsers)
	}
}

func TestLoadRejectsInvalidTelegramID(t *testing.T) {
	t.Setenv("TELEGRAM_ALLOWED_USER_IDS", "123,not-a-number")
	if _, err := Load(); err == nil {
		t.Fatal("Load() expected an error for invalid Telegram ID")
	}
}

func TestValidateRejectsUnsupportedTimeframe(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	cfg.Scanner.Timeframes = []string{"5", "30"}
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
