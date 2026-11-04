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
		Timeframes:      []string{"5", "15", "60"},
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
		{name: "unsupported timeframe", edit: func(s *UserSettings) { s.Timeframes = []string{"30"} }},
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
		Timeframe:   "15",
		Direction:   "LONG",
		CandleStart: now,
		CandleClose: now.Add(15 * time.Minute),
		ClosePrice:  100,
		RSI:         50,
		EMARSI:      48,
	}
	if err := validateSignal(valid); err != nil {
		t.Fatalf("valid signal rejected: %v", err)
	}
	tests := []struct {
		name string
		edit func(*Signal)
	}{
		{name: "unknown timeframe", edit: func(s *Signal) { s.Timeframe = "30" }},
		{name: "unknown direction", edit: func(s *Signal) { s.Direction = "BUY" }},
		{name: "open or invalid candle", edit: func(s *Signal) { s.CandleClose = s.CandleStart }},
		{name: "zero price", edit: func(s *Signal) { s.ClosePrice = 0 }},
		{name: "NaN RSI", edit: func(s *Signal) { s.RSI = math.NaN() }},
		{name: "out of range EMA", edit: func(s *Signal) { s.EMARSI = 100.1 }},
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
