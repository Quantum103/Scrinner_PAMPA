// Package indicators calculates technical indicators for closed market candles.
package indicators

import (
	"errors"
	"fmt"
	"math"
)

// Point contains indicator values aligned with the corresponding input close.
// RSIReady and EMAReady distinguish calculated values from the warm-up period.
type Point struct {
	RSI      float64
	EMARSI   float64
	RSIReady bool
	EMAReady bool
}

// Calculate computes Wilder's RSI and an EMA of that RSI.
//
// The first RSI is seeded from the simple average gain/loss across rsiPeriod
// price changes. Later averages use Wilder's recursive smoothing. The EMA is
// seeded with the SMA of its first emaPeriod RSI values, then uses alpha=2/(N+1).
//
// The returned slice has the same length and candle alignment as closes. Points
// before an indicator becomes available retain zero values and a false ready flag.
// Insufficient history is not an error; it is a normal warm-up state.
func Calculate(closes []float64, rsiPeriod, emaPeriod int) ([]Point, error) {
	if rsiPeriod < 2 {
		return nil, errors.New("RSI period must be at least 2")
	}
	if emaPeriod < 1 {
		return nil, errors.New("EMA period must be positive")
	}
	for index, closePrice := range closes {
		if closePrice <= 0 || math.IsNaN(closePrice) || math.IsInf(closePrice, 0) {
			return nil, fmt.Errorf("close price at index %d must be finite and positive", index)
		}
	}

	points := make([]Point, len(closes))
	if len(closes) <= rsiPeriod {
		return points, nil
	}

	period := float64(rsiPeriod)
	var averageGain, averageLoss float64
	for index := 1; index <= rsiPeriod; index++ {
		delta := closes[index] - closes[index-1]
		if delta > 0 {
			averageGain += delta / period
		} else if delta < 0 {
			averageLoss += -delta / period
		}
	}

	emaAlpha := 2 / (float64(emaPeriod) + 1)
	emaSeedSum := 0.0
	emaSeedCount := 0
	emaReady := false
	emaValue := 0.0

	for index := rsiPeriod; index < len(closes); index++ {
		if index > rsiPeriod {
			delta := closes[index] - closes[index-1]
			gain, loss := 0.0, 0.0
			if delta > 0 {
				gain = delta
			} else if delta < 0 {
				loss = -delta
			}

			// Algebraically equivalent to Wilder's formula while avoiding
			// an intermediate multiplication by the full period.
			weight := float64(rsiPeriod-1) / period
			averageGain = averageGain*weight + gain/period
			averageLoss = averageLoss*weight + loss/period
		}

		rsi := calculateRSI(averageGain, averageLoss)
		points[index].RSI = rsi
		points[index].RSIReady = true

		if !emaReady {
			emaSeedSum += rsi
			emaSeedCount++
			if emaSeedCount == emaPeriod {
				emaValue = emaSeedSum / float64(emaPeriod)
				emaReady = true
				points[index].EMARSI = emaValue
				points[index].EMAReady = true
			}
			continue
		}

		emaValue += emaAlpha * (rsi - emaValue)
		points[index].EMARSI = emaValue
		points[index].EMAReady = true
	}

	return points, nil
}

func calculateRSI(averageGain, averageLoss float64) float64 {
	switch {
	case averageGain == 0 && averageLoss == 0:
		return 50
	case averageLoss == 0:
		return 100
	case averageGain == 0:
		return 0
	}

	// Normalization avoids overflow when the two smoothed values are large.
	scale := math.Max(averageGain, averageLoss)
	gain := averageGain / scale
	loss := averageLoss / scale
	return 100 * gain / (gain + loss)
}
