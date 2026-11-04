package storage

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestValidateSettings(t *testing.T) {
	base := UserSettings{
		UserID:          1,
		RSILength:       14,
		EMALength:       9,
		InstrumentLimit: 100,
		Timeframes:      []string{"5", "15", "30", "60"},
	}
	if err := validateSettings(base); err != nil {
		t.Fatalf("valid settings rejected: %v", err)
	}
	tests := []struct {
		name string
		edit func(*UserSettings)
	}{
		{name: "zero user", edit: func(s *UserSettings) { s.UserID = 0 }},
		{name: "short RSI", edit: func(s *UserSettings) { s.RSILength = 1 }},
		{name: "long RSI", edit: func(s *UserSettings) { s.RSILength = 501 }},
		{name: "zero EMA", edit: func(s *UserSettings) { s.EMALength = 0 }},
		{name: "zero instrument limit", edit: func(s *UserSettings) { s.InstrumentLimit = 0 }},
		{name: "no timeframes", edit: func(s *UserSettings) { s.Timeframes = nil }},
		{name: "unsupported timeframe", edit: func(s *UserSettings) { s.Timeframes = []string{"1"} }},
		{name: "duplicate timeframe", edit: func(s *UserSettings) { s.Timeframes = []string{"5", "5"} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings := base
			settings.Timeframes = append([]string(nil), base.Timeframes...)
			tt.edit(&settings)
			if err := validateSettings(settings); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("validateSettings() error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestValidateSignal(t *testing.T) {
	now := time.Now().UTC()
	valid := Signal{
		Symbol:      "BTCUSDT",
		Timeframe:   "30",
		Direction:   "LONG",
		CandleStart: now,
		CandleClose: now.Add(30 * time.Minute),
		ClosePrice:  100,
		RSI:         50,
		EMARSI:      48,
		MarketRegime: "SIDEWAYS",
		MarketADX: 17.8, MarketADXReady: true,
		MarketCHOP: 65.3, MarketCHOPReady: true,
		MarketEMA: 99.5, MarketEMAReady: true,
	}
	if err := validateSignal(valid); err != nil {
		t.Fatalf("valid signal rejected: %v", err)
	}
	legacy := valid
	legacy.MarketRegime = ""
	legacy.MarketADXReady = false
	legacy.MarketCHOPReady = false
	legacy.MarketEMAReady = false
	if err := validateSignal(legacy); err != nil {
		t.Fatalf("legacy signal without market context rejected: %v", err)
	}
	tests := []struct {
		name string
		edit func(*Signal)
	}{
		{name: "unknown timeframe", edit: func(s *Signal) { s.Timeframe = "1" }},
		{name: "unknown direction", edit: func(s *Signal) { s.Direction = "BUY" }},
		{name: "open or invalid candle", edit: func(s *Signal) { s.CandleClose = s.CandleStart }},
		{name: "zero price", edit: func(s *Signal) { s.ClosePrice = 0 }},
		{name: "NaN RSI", edit: func(s *Signal) { s.RSI = math.NaN() }},
		{name: "out of range EMA", edit: func(s *Signal) { s.EMARSI = 100.1 }},
		{name: "unknown market regime", edit: func(s *Signal) { s.MarketRegime = "RANGING" }},
		{name: "market ADX above 100", edit: func(s *Signal) { s.MarketADX = 100.1 }},
		{name: "negative market CHOP", edit: func(s *Signal) { s.MarketCHOP = -0.1 }},
		{name: "zero market EMA", edit: func(s *Signal) { s.MarketEMA = 0 }},
		{name: "NaN market indicator", edit: func(s *Signal) { s.MarketADX = math.NaN() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			signal := valid
			tt.edit(&signal)
			if err := validateSignal(signal); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("validateSignal() error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestValidateScannerState(t *testing.T) {
	now := time.Now().UTC()
	state := ScannerState{
		Symbol:                   "BTCUSDT",
		Timeframe:                "5",
		LastProcessedCandleStart: now,
		LastClose:                100,
		LastRSI:                  50,
		LastEMARSI:               49,
		Initialized:              true,
	}
	if err := validateScannerState(state); err != nil {
		t.Fatalf("valid scanner state rejected: %v", err)
	}
	state.LastCrossDirection = "BUY"
	if err := validateScannerState(state); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid last cross direction error = %v", err)
	}
}
