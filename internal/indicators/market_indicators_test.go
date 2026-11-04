package indicators

import (
	"math"
	"testing"
)

func TestCalculateMarketIndicatorsWarmupAndAlignment(t *testing.T) {
	candles := trendingMarketCandles(26)
	points, err := CalculateMarketIndicators(candles, 14, 14, 50)
	if err != nil {
		t.Fatalf("CalculateMarketIndicators() error = %v", err)
	}
	if len(points) != len(candles) {
		t.Fatalf("points length = %d, want %d", len(points), len(candles))
	}

	if points[12].CHOPReady {
		t.Fatal("CHOP must not be ready before 14 candles")
	}
	if !points[13].CHOPReady {
		t.Fatalf("CHOP must be ready at index 13: %+v", points[13])
	}
	for index := range points {
		if points[index].ADXReady {
			t.Fatalf("ADX must not be ready before index 27, got ready at %d", index)
		}
		if points[index].EMAReady {
			t.Fatalf("EMA(50) must not be ready with only 26 candles, got ready at %d", index)
		}
	}
}

func TestCalculateMarketIndicatorsDetectsDirectionalTrend(t *testing.T) {
	candles := trendingMarketCandles(120)
	points, err := CalculateMarketIndicators(candles, 14, 14, 50)
	if err != nil {
		t.Fatalf("CalculateMarketIndicators() error = %v", err)
	}

	last := points[len(points)-1]
	if !last.ADXReady || !last.CHOPReady || !last.EMAReady {
		t.Fatalf("all indicators should be ready: %+v", last)
	}
	if last.ADX <= 25 {
		t.Fatalf("ADX = %.4f, want a strong directional trend above 25", last.ADX)
	}
	if last.CHOP >= 38.2 {
		t.Fatalf("CHOP = %.4f, want a low value for a clean trend", last.CHOP)
	}
	if last.EMA <= candles[0].Close || last.EMA >= candles[len(candles)-1].Close {
		t.Fatalf("EMA = %.4f, want a value inside the rising price range", last.EMA)
	}
}

func TestCalculateMarketIndicatorsDetectsSidewaysMovement(t *testing.T) {
	candles := sidewaysMarketCandles(120)
	points, err := CalculateMarketIndicators(candles, 14, 14, 50)
	if err != nil {
		t.Fatalf("CalculateMarketIndicators() error = %v", err)
	}

	last := points[len(points)-1]
	if !last.ADXReady || !last.CHOPReady || !last.EMAReady {
		t.Fatalf("all indicators should be ready: %+v", last)
	}
	if last.ADX >= 20 {
		t.Fatalf("ADX = %.4f, want a weak directional movement below 20", last.ADX)
	}
	if last.CHOP <= 61.8 {
		t.Fatalf("CHOP = %.4f, want a choppy market above 61.8", last.CHOP)
	}
}

func TestCalculateMarketIndicatorsEMAUsesSMASeedAndStandardSmoothing(t *testing.T) {
	candles := trendingMarketCandles(60)
	points, err := CalculateMarketIndicators(candles, 14, 14, 50)
	if err != nil {
		t.Fatalf("CalculateMarketIndicators() error = %v", err)
	}

	if points[48].EMAReady {
		t.Fatal("EMA(50) must not be ready before index 49")
	}
	if !points[49].EMAReady || !points[50].EMAReady {
		t.Fatalf("EMA readiness is incorrect: index49=%+v index50=%+v", points[49], points[50])
	}
	assertNear(t, points[49].EMA, 124.5, 1e-12)
	assertNear(t, points[50].EMA, 125.5, 1e-12)
}

func TestCalculateMarketIndicatorsFallingTrendHasHighADX(t *testing.T) {
	candles := make([]OHLC, 120)
	for index := range candles {
		closePrice := 220 - float64(index)
		candles[index] = OHLC{
			High: closePrice + 0.25,
			Low:  closePrice - 0.25,
			Close: closePrice,
		}
	}

	points, err := CalculateMarketIndicators(candles, 14, 14, 50)
	if err != nil {
		t.Fatalf("CalculateMarketIndicators() error = %v", err)
	}
	last := points[len(points)-1]
	if !last.ADXReady || !last.EMAReady {
		t.Fatalf("ADX and EMA should be ready: %+v", last)
	}
	if last.ADX <= 25 {
		t.Fatalf("ADX = %.4f, want a strong falling trend above 25", last.ADX)
	}
	if last.EMA <= candles[len(candles)-1].Close {
		t.Fatalf("EMA = %.4f, want EMA above the latest close in a falling trend", last.EMA)
	}
}

func TestCalculateMarketIndicatorsFlatWindowDoesNotProduceNaN(t *testing.T) {
	candles := make([]OHLC, 60)
	for index := range candles {
		candles[index] = OHLC{High: 42, Low: 42, Close: 42}
	}

	points, err := CalculateMarketIndicators(candles, 14, 14, 50)
	if err != nil {
		t.Fatalf("CalculateMarketIndicators() error = %v", err)
	}
	last := points[len(points)-1]
	if !last.ADXReady || !last.EMAReady {
		t.Fatalf("ADX and EMA should be ready on a flat series: %+v", last)
	}
	if last.CHOPReady {
		t.Fatal("CHOP must remain unavailable when the high-low range is zero")
	}
	assertNear(t, last.ADX, 0, 1e-12)
	assertNear(t, last.EMA, 42, 1e-12)
	if math.IsNaN(last.ADX) || math.IsInf(last.ADX, 0) {
		t.Fatalf("ADX must remain finite: %+v", last)
	}
}

func TestCalculateMarketIndicatorsRejectsInvalidPeriods(t *testing.T) {
	candles := trendingMarketCandles(5)
	tests := []struct {
		name      string
		adxPeriod int
		chopPeriod int
		emaPeriod int
	}{
		{name: "ADX too short", adxPeriod: 1, chopPeriod: 14, emaPeriod: 50},
		{name: "ADX too long", adxPeriod: 501, chopPeriod: 14, emaPeriod: 50},
		{name: "CHOP too short", adxPeriod: 14, chopPeriod: 1, emaPeriod: 50},
		{name: "CHOP too long", adxPeriod: 14, chopPeriod: 501, emaPeriod: 50},
		{name: "EMA zero", adxPeriod: 14, chopPeriod: 14, emaPeriod: 0},
		{name: "EMA too long", adxPeriod: 14, chopPeriod: 14, emaPeriod: 501},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := CalculateMarketIndicators(candles, test.adxPeriod, test.chopPeriod, test.emaPeriod)
			if err == nil {
				t.Fatal("CalculateMarketIndicators() expected invalid-period error")
			}
		})
	}
}

func TestCalculateMarketIndicatorsRejectsInvalidOHLC(t *testing.T) {
	valid := OHLC{High: 102, Low: 98, Close: 100}
	tests := []struct {
		name   string
		candle OHLC
	}{
		{name: "high below low", candle: OHLC{High: 98, Low: 102, Close: 100}},
		{name: "close above high", candle: OHLC{High: 102, Low: 98, Close: 103}},
		{name: "close below low", candle: OHLC{High: 102, Low: 98, Close: 97}},
		{name: "zero high", candle: OHLC{High: 0, Low: 0, Close: 0}},
		{name: "NaN close", candle: OHLC{High: 102, Low: 98, Close: math.NaN()}},
		{name: "infinite high", candle: OHLC{High: math.Inf(1), Low: 98, Close: 100}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := CalculateMarketIndicators([]OHLC{valid, test.candle}, 2, 2, 2)
			if err == nil {
				t.Fatal("CalculateMarketIndicators() expected invalid-OHLC error")
			}
		})
	}
}

func TestCalculateMarketIndicatorsDoesNotModifyInput(t *testing.T) {
	candles := trendingMarketCandles(60)
	before := append([]OHLC(nil), candles...)
	if _, err := CalculateMarketIndicators(candles, 14, 14, 50); err != nil {
		t.Fatalf("CalculateMarketIndicators() error = %v", err)
	}
	for index := range candles {
		if candles[index] != before[index] {
			t.Fatalf("input candle was modified at index %d", index)
		}
	}
}

func trendingMarketCandles(count int) []OHLC {
	candles := make([]OHLC, count)
	for index := range candles {
		closePrice := 100 + float64(index)
		candles[index] = OHLC{
			High: closePrice + 0.25,
			Low:  closePrice - 0.25,
			Close: closePrice,
		}
	}
	return candles
}

func sidewaysMarketCandles(count int) []OHLC {
	candles := make([]OHLC, count)
	for index := range candles {
		closePrice := 100.0
		if index%2 == 1 {
			closePrice = 100.2
		}
		candles[index] = OHLC{
			High: closePrice + 0.5,
			Low:  closePrice - 0.5,
			Close: closePrice,
		}
	}
	return candles
}
