package paper

import (
	"fmt"
	"math"
	"time"

	marketanalysis "github.com/Quantum103/Scrinner_PAMPA/internal/analysis"
)

// UpdateOrderFlow проверяет разворот потока только на готовых последовательных срезах.
// Четыре независимые группы признаков: ослабление Taker Buy, давление продаж
// (Taker Sell вместе с отрицательной Delta), снижение CVD и остановка роста цены.
func (e *Engine) UpdateOrderFlow(symbol string, snapshot marketanalysis.Snapshot, book Book, now time.Time) []Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	position := e.positions[symbol]
	if position == nil || !e.cfg.OrderFlowExitEnabled || position.ExitReason != "" {
		return nil
	}

	if snapshot.Status != marketanalysis.StatusReady ||
		!bookUsable(book, now, e.cfg.MaxBookAge) ||
		!withinOrderFlowAge(snapshot.UpdatedAt, now, e.cfg.MaxBookAge) ||
		!validOrderFlowSnapshot(snapshot) {
		resetOrderFlowConfirmation(position)
		return nil
	}

	// Не считаем один и тот же интервал несколько раз из-за частых WS-сообщений.
	if !position.LastOrderFlowObservationAt.IsZero() {
		gap := now.Sub(position.LastOrderFlowObservationAt)
		if gap < e.cfg.OrderFlowObservationInterval {
			return nil
		}
		if gap > e.cfg.OrderFlowMaxObservationGap || gap < 0 {
			resetOrderFlowConfirmation(position)
		}
	}

	if !position.HasOrderFlowObservation {
		rememberOrderFlowObservation(position, snapshot, now)
		position.UpdatedAt = now
		return nil
	}

	buyShare := *snapshot.TakerBuySharePercent
	sellShare := *snapshot.TakerSellSharePercent
	delta := *snapshot.Delta
	cvd := *snapshot.CVD
	lastPrice := *snapshot.LastPrice

	signals := 0
	weakTakerBuy := buyShare <= e.cfg.OrderFlowMaxTakerBuySharePercent ||
		position.PreviousTakerBuySharePercent-buyShare >= e.cfg.OrderFlowTakerBuyShareDropPP
	if weakTakerBuy {
		signals++
	}

	// Sell share и отрицательная Delta описывают одну группу давления продаж,
	// поэтому не считаются двумя отдельными подтверждениями.
	aggressiveSelling := sellShare >= e.cfg.OrderFlowMinTakerSellSharePercent && delta < 0
	if aggressiveSelling {
		signals++
	}

	cvdDrop := position.PreviousCVD - cvd
	cvdThreshold := math.Abs(position.PreviousCVD) * e.cfg.OrderFlowCVDMinDeclinePercent / 100
	cvdTurningDown := cvdDrop > 0 && cvdDrop+1e-12 >= cvdThreshold
	if cvdTurningDown {
		signals++
	}

	maxAllowedPrice := position.PreviousLastPrice * (1 + e.cfg.OrderFlowPriceStallMaxRisePercent/100)
	priceStalledOrFalling := lastPrice <= maxAllowedPrice
	if priceStalledOrFalling {
		signals++
	}

	if signals >= e.cfg.OrderFlowMinSignals {
		position.OrderFlowConfirmations++
	} else {
		position.OrderFlowConfirmations = 0
	}
	position.LastOrderFlowSignalCount = signals
	rememberOrderFlowObservation(position, snapshot, now)
	position.UpdatedAt = now

	if position.OrderFlowConfirmations < e.cfg.OrderFlowConsecutiveIntervals {
		return nil
	}

	reason := fmt.Sprintf(
		"подтверждённый разворот Order Flow: %d из 4 групп сигналов на %d последовательных интервалах",
		signals, position.OrderFlowConfirmations,
	)
	position.ExitReason = reason
	position.OrderFlowExitTriggered = true
	position.StopTriggered = false

	trigger := Event{
		Type: "order_flow_exit_triggered", Symbol: symbol, At: now,
		Direction: "LONG", Reason: reason,
		Price: book.Bids[0].Price, Quantity: position.RemainingQuantity,
		RemainingQuantity: position.RemainingQuantity,
		FlowSignalCount: signals, FlowConfirmations: position.OrderFlowConfirmations,
	}
	e.addEvent(trigger)

	e.applyDueFunding(position, now)
	closeEvent := e.closeLocked(position, book, now, reason)
	return []Event{trigger, closeEvent}
}

func validOrderFlowSnapshot(snapshot marketanalysis.Snapshot) bool {
	values := []*float64{
		snapshot.TakerBuySharePercent,
		snapshot.TakerSellSharePercent,
		snapshot.Delta,
		snapshot.CVD,
		snapshot.LastPrice,
	}
	for _, value := range values {
		if value == nil || math.IsNaN(*value) || math.IsInf(*value, 0) {
			return false
		}
	}
	return *snapshot.TakerBuySharePercent >= 0 && *snapshot.TakerBuySharePercent <= 100 &&
		*snapshot.TakerSellSharePercent >= 0 && *snapshot.TakerSellSharePercent <= 100 &&
		*snapshot.LastPrice > 0
}

func withinOrderFlowAge(at, now time.Time, maxAge time.Duration) bool {
	return !at.IsZero() && !now.Before(at) && now.Sub(at) <= maxAge
}

func resetOrderFlowConfirmation(position *Position) {
	position.HasOrderFlowObservation = false
	position.LastOrderFlowObservationAt = time.Time{}
	position.OrderFlowConfirmations = 0
	position.LastOrderFlowSignalCount = 0
}

func rememberOrderFlowObservation(position *Position, snapshot marketanalysis.Snapshot, now time.Time) {
	position.PreviousTakerBuySharePercent = *snapshot.TakerBuySharePercent
	position.PreviousCVD = *snapshot.CVD
	position.PreviousLastPrice = *snapshot.LastPrice
	position.LastOrderFlowObservationAt = now
	position.HasOrderFlowObservation = true
}
