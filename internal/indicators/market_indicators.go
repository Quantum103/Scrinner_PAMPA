// Package indicators calculates technical indicators for closed market candles.
package indicators

import (
	"errors"
	"fmt"
	"math"
)

// OHLC contains the price fields required by volatility and trend indicators.
type OHLC struct {
	High  float64
	Low   float64
	Close float64
}

// MarketPoint contains ADX, Choppiness Index and price EMA values aligned with
// the corresponding input candle. Ready flags remain false during warm-up.
type MarketPoint struct {
	ADX       float64
	CHOP      float64
	EMA       float64
	ADXReady  bool
	CHOPReady bool
	EMAReady  bool
}

// CalculateMarketIndicators calculates Wilder's ADX, the Choppiness Index and
// an EMA of closing prices from chronological OHLC candles.
//
// ADX first becomes available after 2*adxPeriod-1 candles. CHOP first becomes
// available after chopPeriod candles, and EMA after emaPeriod candles. An
// unavailable value stays zero with its Ready flag set to false. Insufficient
// history is a normal warm-up state, not an error.
func CalculateMarketIndicators(
	candles []OHLC,
	adxPeriod, chopPeriod, emaPeriod int,
) ([]MarketPoint, error) {
	if adxPeriod < 2 || adxPeriod > 500 {
		return nil, errors.New("ADX period must be between 2 and 500")
	}
	if chopPeriod < 2 || chopPeriod > 500 {
		return nil, errors.New("CHOP period must be between 2 and 500")
	}
	if emaPeriod < 1 || emaPeriod > 500 {
		return nil, errors.New("EMA period must be between 1 and 500")
	}

	for index, candle := range candles {
		if !isFinitePositive(candle.High) || !isFinitePositive(candle.Low) ||
			!isFinitePositive(candle.Close) {
			return nil, fmt.Errorf("OHLC prices at index %d must be finite and positive", index)
		}
		if candle.High < candle.Low {
			return nil, fmt.Errorf("high price at index %d is below low price", index)
		}
		if candle.Close < candle.Low || candle.Close > candle.High {
			return nil, fmt.Errorf("close price at index %d is outside the high-low range", index)
		}
	}

	points := make([]MarketPoint, len(candles))
	if len(candles) == 0 {
		return points, nil
	}

	trueRanges := make([]float64, len(candles))
	plusDM := make([]float64, len(candles))
	minusDM := make([]float64, len(candles))

	for index, candle := range candles {
		trueRange := candle.High - candle.Low
		if index > 0 {
			previousClose := candles[index-1].Close
			trueRange = math.Max(
				trueRange,
				math.Max(
					math.Abs(candle.High-previousClose),
					math.Abs(candle.Low-previousClose),
				),
			)

			upMove := candle.High - candles[index-1].High
			downMove := candles[index-1].Low - candle.Low
			if upMove > downMove && upMove > 0 {
				plusDM[index] = upMove
			} else if downMove > upMove && downMove > 0 {
				minusDM[index] = downMove
			}
		}
		trueRanges[index] = trueRange
	}

	calculateADX(points, trueRanges, plusDM, minusDM, adxPeriod)
	calculateCHOP(points, candles, trueRanges, chopPeriod)
	calculatePriceEMA(points, candles, emaPeriod)

	return points, nil
}

func calculateADX(
	points []MarketPoint,
	trueRanges, plusDM, minusDM []float64,
	period int,
) {
	if len(points) <= period {
		return
	}

	periodFloat := float64(period)
	var smoothedTR, smoothedPlusDM, smoothedMinusDM float64
	for index := 1; index <= period; index++ {
		smoothedTR += trueRanges[index]
		smoothedPlusDM += plusDM[index]
		smoothedMinusDM += minusDM[index]
	}

	dxSeedSum := 0.0
	dxSeedCount := 0
	adx := 0.0

	for index := period; index < len(points); index++ {
		if index > period {
			smoothedTR = smoothedTR - smoothedTR/periodFloat + trueRanges[index]
			smoothedPlusDM = smoothedPlusDM - smoothedPlusDM/periodFloat + plusDM[index]
			smoothedMinusDM = smoothedMinusDM - smoothedMinusDM/periodFloat + minusDM[index]
		}

		plusDI, minusDI := 0.0, 0.0
		if smoothedTR > 0 {
			plusDI = 100 * smoothedPlusDM / smoothedTR
			minusDI = 100 * smoothedMinusDM / smoothedTR
		}

		directionalIndex := 0.0
		directionalSum := plusDI + minusDI
		if directionalSum > 0 {
			directionalIndex = 100 * math.Abs(plusDI-minusDI) / directionalSum
		}

		if dxSeedCount < period {
			dxSeedSum += directionalIndex
			dxSeedCount++
			if dxSeedCount == period {
				adx = dxSeedSum / periodFloat
				points[index].ADX = adx
				points[index].ADXReady = true
			}
			continue
		}

		adx = (adx*(periodFloat-1) + directionalIndex) / periodFloat
		points[index].ADX = adx
		points[index].ADXReady = true
	}
}

func calculateCHOP(points []MarketPoint, candles []OHLC, trueRanges []float64, period int) {
	if len(points) < period {
		return
	}

	periodFloat := float64(period)
	rollingTR := 0.0
	logPeriod := math.Log10(periodFloat)

	for index := range candles {
		rollingTR += trueRanges[index]
		if index >= period {
			rollingTR -= trueRanges[index-period]
		}
		if index < period-1 {
			continue
		}

		start := index - period + 1
		highestHigh := candles[start].High
		lowestLow := candles[start].Low
		for windowIndex := start + 1; windowIndex <= index; windowIndex++ {
			highestHigh = math.Max(highestHigh, candles[windowIndex].High)
			lowestLow = math.Min(lowestLow, candles[windowIndex].Low)
		}

		priceRange := highestHigh - lowestLow
		if rollingTR <= 0 || priceRange <= 0 {
			// A flat window has no defined CHOP denominator.
			continue
		}

		ratio := rollingTR / priceRange
		if ratio <= 0 {
			continue
		}
		chop := 100 * math.Log10(ratio) / logPeriod
		// Floating-point rounding can move boundary values just outside the
		// theoretical 0..100 range.
		points[index].CHOP = math.Max(0, math.Min(100, chop))
		points[index].CHOPReady = true
	}
}

func calculatePriceEMA(points []MarketPoint, candles []OHLC, period int) {
	if len(candles) < period {
		return
	}

	periodFloat := float64(period)
	seedSum := 0.0
	for index := 0; index < period; index++ {
		seedSum += candles[index].Close
	}

	ema := seedSum / periodFloat
	points[period-1].EMA = ema
	points[period-1].EMAReady = true

	alpha := 2 / (periodFloat + 1)
	for index := period; index < len(candles); index++ {
		ema += alpha * (candles[index].Close - ema)
		points[index].EMA = ema
		points[index].EMAReady = true
	}
}

func isFinitePositive(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
