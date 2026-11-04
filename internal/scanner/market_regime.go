package scanner

import (
	"fmt"
	"strings"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	"github.com/Quantum103/Scrinner_PAMPA/internal/indicators"
	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
)

const (
	marketADXPeriod  = 14
	marketCHOPPeriod = 14
	marketEMAPeriod  = 50

	marketRegimeWindow  = 3
	marketRegimeConfirm = 2

	marketADXSidewaysMax  = 20.0
	marketCHOPSidewaysMin = 61.8
	marketADXTrendMin     = 25.0
)

const (
	marketRegimeUnknown      = "UNKNOWN"
	marketRegimeSideways     = "SIDEWAYS"
	marketRegimeUptrend      = "UPTREND"
	marketRegimeDowntrend    = "DOWNTREND"
	marketRegimeTransitional = "TRANSITIONAL"
)

type marketSnapshot struct {
	regime string

	adx      float64
	adxReady bool

	chop      float64
	chopReady bool

	ema      float64
	emaReady bool
}

// minimumMarketHistory retains enough confirmed candles for EMA(50) and for
// two of the latest three candles to have a valid EMA slope.
func minimumMarketHistory(requested int) int {
	minimum := marketEMAPeriod + marketRegimeWindow - 1
	if requested > minimum {
		return requested
	}
	return minimum
}

func calculateMarketPoints(candles []bybit.Candle) ([]indicators.MarketPoint, error) {
	ohlc := make([]indicators.OHLC, len(candles))
	for index, candle := range candles {
		ohlc[index] = indicators.OHLC{
			High:  candle.High,
			Low:   candle.Low,
			Close: candle.ClosePrice,
		}
	}
	points, err := indicators.CalculateMarketIndicators(
		ohlc,
		marketADXPeriod,
		marketCHOPPeriod,
		marketEMAPeriod,
	)
	if err != nil {
		return nil, fmt.Errorf("calculate ADX(%d), CHOP(%d), EMA(%d): %w",
			marketADXPeriod, marketCHOPPeriod, marketEMAPeriod, err)
	}
	return points, nil
}

// marketSnapshotAt classifies only information available at index. SIDEWAYS
// requires two of the latest three candles to satisfy both the ADX and CHOP
// thresholds. Trend labels require two of the latest three candles to have
// ADX above 25 and an EMA(50) slope in the same direction.
func marketSnapshotAt(points []indicators.MarketPoint, index int) marketSnapshot {
	snapshot := marketSnapshot{regime: marketRegimeUnknown}
	if index < 0 || index >= len(points) {
		return snapshot
	}

	current := points[index]
	snapshot.adx, snapshot.adxReady = current.ADX, current.ADXReady
	snapshot.chop, snapshot.chopReady = current.CHOP, current.CHOPReady
	snapshot.ema, snapshot.emaReady = current.EMA, current.EMAReady

	if index < marketRegimeWindow-1 {
		if current.ADXReady || current.CHOPReady || current.EMAReady {
			snapshot.regime = marketRegimeTransitional
		}
		return snapshot
	}

	sideways, uptrend, downtrend := 0, 0, 0
	start := index - marketRegimeWindow + 1
	for candleIndex := start; candleIndex <= index; candleIndex++ {
		point := points[candleIndex]
		if point.ADXReady && point.CHOPReady &&
			point.ADX < marketADXSidewaysMax && point.CHOP > marketCHOPSidewaysMin {
			sideways++
		}

		if candleIndex == 0 || !point.ADXReady || point.ADX <= marketADXTrendMin ||
			!point.EMAReady || !points[candleIndex-1].EMAReady {
			continue
		}
		previousEMA := points[candleIndex-1].EMA
		switch {
		case point.EMA > previousEMA:
			uptrend++
		case point.EMA < previousEMA:
			downtrend++
		}
	}

	switch {
	case sideways >= marketRegimeConfirm:
		snapshot.regime = marketRegimeSideways
	case uptrend >= marketRegimeConfirm:
		snapshot.regime = marketRegimeUptrend
	case downtrend >= marketRegimeConfirm:
		snapshot.regime = marketRegimeDowntrend
	default:
		if current.ADXReady || current.CHOPReady || current.EMAReady {
			snapshot.regime = marketRegimeTransitional
		}
	}
	return snapshot
}

func applyMarketContext(signal *storage.Signal, points []indicators.MarketPoint, index int) {
	if signal == nil {
		return
	}
	snapshot := marketSnapshotAt(points, index)
	signal.MarketRegime = snapshot.regime
	signal.MarketADX = snapshot.adx
	signal.MarketADXReady = snapshot.adxReady
	signal.MarketCHOP = snapshot.chop
	signal.MarketCHOPReady = snapshot.chopReady
	signal.MarketEMA = snapshot.ema
	signal.MarketEMAReady = snapshot.emaReady
}


// marketRegimeAllowsSignal suppresses only confirmed sideways conditions.
// Unknown and transitional markets remain eligible because they are not
// confirmed to be ranging.
func marketRegimeAllowsSignal(regime string) bool {
	return !strings.EqualFold(strings.TrimSpace(regime), marketRegimeSideways)
}
