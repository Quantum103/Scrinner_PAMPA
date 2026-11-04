package indicators

import (
	"math"
	"testing"
)

func TestCalculateWarmupAndAlignment(t *testing.T) {
	closes := make([]float64, 30)
	for index := range closes {
		closes[index] = 100 + float64(index)
	}

	points, err := Calculate(closes, 14, 9)
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}
	if len(points) != len(closes) {
		t.Fatalf("points length = %d, want %d", len(points), len(closes))
	}
	for index := 0; index < 14; index++ {
		if points[index].RSIReady || points[index].EMAReady {
			t.Fatalf("indicator unexpectedly ready at index %d: %+v", index, points[index])
		}
	}
	if !points[14].RSIReady || points[14].EMAReady {
		t.Fatalf("RSI/EMA readiness at first RSI index is incorrect: %+v", points[14])
	}
	if !points[21].RSIReady || points[21].EMAReady {
		t.Fatalf("EMA must still warm up at index 21: %+v", points[21])
	}
	if !points[22].RSIReady || !points[22].EMAReady {
		t.Fatalf("first EMA value must be ready at index 22: %+v", points[22])
	}
	for index := 22; index < len(points); index++ {
		assertNear(t, points[index].RSI, 100, 1e-12)
		assertNear(t, points[index].EMARSI, 100, 1e-12)
	}
}

func TestCalculateFlatMarketReturnsNeutralRSI(t *testing.T) {
	closes := make([]float64, 20)
	for index := range closes {
		closes[index] = 42
	}
	points, err := Calculate(closes, 5, 3)
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}
	for index := 5; index < len(points); index++ {
		if !points[index].RSIReady {
			t.Fatalf("RSI not ready at index %d", index)
		}
		assertNear(t, points[index].RSI, 50, 1e-12)
	}
	for index := 7; index < len(points); index++ {
		if !points[index].EMAReady {
			t.Fatalf("EMA not ready at index %d", index)
		}
		assertNear(t, points[index].EMARSI, 50, 1e-12)
	}
}

func TestCalculateFallingMarketReturnsZeroRSI(t *testing.T) {
	closes := make([]float64, 20)
	for index := range closes {
		closes[index] = 100 - float64(index)
	}
	points, err := Calculate(closes, 5, 3)
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}
	for index := 5; index < len(points); index++ {
		assertNear(t, points[index].RSI, 0, 1e-12)
	}
	for index := 7; index < len(points); index++ {
		assertNear(t, points[index].EMARSI, 0, 1e-12)
	}
}

func TestCalculateUsesWilderSmoothingAndEMAOfRSI(t *testing.T) {
	closes := []float64{100, 102, 101, 103, 102, 101}
	points, err := Calculate(closes, 3, 2)
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}

	// Initial averages: gain=(2+0+2)/3, loss=(0+1+0)/3 => RSI=80.
	assertNear(t, points[3].RSI, 80, 1e-10)

	// Wilder smoothing after the next -1 move:
	// average gain=8/9, average loss=5/9 => RSI=61.5384615...
	assertNear(t, points[4].RSI, 100*8.0/13.0, 1e-10)
	if !points[4].EMAReady {
		t.Fatal("EMA(2) should be ready at index 4")
	}

	// EMA(2) is seeded by SMA(80, 61.5384615...).
	firstEMA := (80 + 100*8.0/13.0) / 2
	assertNear(t, points[4].EMARSI, firstEMA, 1e-10)

	// Another -1 move applies Wilder smoothing again, then EMA alpha=2/3.
	nextRSI := 100 * 16.0 / 35.0
	assertNear(t, points[5].RSI, nextRSI, 1e-10)
	expectedEMA := firstEMA + (2.0/3.0)*(nextRSI-firstEMA)
	assertNear(t, points[5].EMARSI, expectedEMA, 1e-10)
}

func TestCalculateInsufficientHistoryIsWarmupNotError(t *testing.T) {
	points, err := Calculate([]float64{10, 11, 12}, 14, 9)
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}
	if len(points) != 3 {
		t.Fatalf("points length = %d, want 3", len(points))
	}
	for index, point := range points {
		if point.RSIReady || point.EMAReady || point.RSI != 0 || point.EMARSI != 0 {
			t.Fatalf("point %d should be in warm-up state: %+v", index, point)
		}
	}
}

func TestCalculateEMAOneUsesFirstRSIValue(t *testing.T) {
	points, err := Calculate([]float64{100, 101, 102, 103}, 2, 1)
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}
	if !points[2].EMAReady || !points[2].RSIReady {
		t.Fatalf("expected RSI and EMA at index 2: %+v", points[2])
	}
	assertNear(t, points[2].EMARSI, points[2].RSI, 1e-12)
	assertNear(t, points[3].EMARSI, points[3].RSI, 1e-12)
}

func TestCalculateRejectsInvalidPeriods(t *testing.T) {
	tests := []struct {
		name string
		rsi  int
		ema  int
	}{
		{name: "RSI too short", rsi: 1, ema: 9},
		{name: "RSI zero", rsi: 0, ema: 9},
		{name: "EMA zero", rsi: 14, ema: 0},
		{name: "negative EMA", rsi: 14, ema: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Calculate([]float64{1, 2, 3}, test.rsi, test.ema); err == nil {
				t.Fatal("Calculate() expected invalid-period error")
			}
		})
	}
}

func TestCalculateRejectsInvalidClosePrices(t *testing.T) {
	tests := []struct {
		name string
		value float64
	}{
		{name: "zero", value: 0},
		{name: "negative", value: -1},
		{name: "NaN", value: math.NaN()},
		{name: "positive infinity", value: math.Inf(1)},
		{name: "negative infinity", value: math.Inf(-1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Calculate([]float64{100, test.value, 101}, 2, 1); err == nil {
				t.Fatal("Calculate() expected invalid-close error")
			}
		})
	}
}

func TestCalculateDoesNotModifyInput(t *testing.T) {
	closes := []float64{100, 102, 101, 103, 102, 101}
	before := append([]float64(nil), closes...)
	if _, err := Calculate(closes, 3, 2); err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}
	for index := range closes {
		if closes[index] != before[index] {
			t.Fatalf("input was modified at index %d", index)
		}
	}
}

func assertNear(t *testing.T, got, want, tolerance float64) {
	t.Helper()
	if math.Abs(got-want) > tolerance {
		t.Fatalf("got %.12f, want %.12f (tolerance %.2g)", got, want, tolerance)
	}
}
