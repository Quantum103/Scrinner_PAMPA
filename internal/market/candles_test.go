package market

import (
	"errors"
	"testing"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
)

func testMinuteHistory(now time.Time, count int) []bybit.Kline {
	currentStart := now.UTC().Truncate(time.Minute)
	start := currentStart.Add(-time.Duration(count) * time.Minute)
	result := make([]bybit.Kline, 0, count)
	for index := 0; index < count; index++ {
		open := 100 + float64(index)
		result = append(result, bybit.Kline{
			StartTime: start.Add(time.Duration(index) * time.Minute),
			Open: open, High: open + 2, Low: open - 1, Close: open + 1,
			Volume: float64(index + 1), Turnover: float64((index + 1) * 100),
		})
	}
	return result
}

func testLiveCandle(start time.Time, confirmed bool, turnover float64) Candle {
	return Candle{
		StartTime: start.UTC(), EndTime: start.UTC().Add(time.Minute - time.Millisecond),
		Open: 100, High: 103, Low: 99, Close: 102,
		Volume: 2, Turnover: turnover, Confirmed: confirmed,
	}
}

func TestCandleStoreLoadsTwentyCompletedCandlesAndExcludesCurrentRESTBar(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 34, 30, 0, time.UTC)
	history := testMinuteHistory(now, 20)
	history = append(history, bybit.Kline{
		StartTime: now.Truncate(time.Minute), Open: 120, High: 121, Low: 119, Close: 120,
		Volume: 5, Turnover: 600,
	})

	store := NewCandleStore(90 * time.Second)
	store.Activate("AAAUSDT")
	if err := store.LoadHistory("AAAUSDT", history, now); err != nil {
		t.Fatalf("LoadHistory() error = %v", err)
	}
	snapshot, ok := store.Snapshot("AAAUSDT", now)
	if !ok {
		t.Fatal("Snapshot() не нашёл активный символ")
	}
	if len(snapshot.History) != 20 {
		t.Fatalf("история содержит %d свечей, ожидалось 20", len(snapshot.History))
	}
	if snapshot.History[0].StartTime != history[0].StartTime || snapshot.History[19].StartTime != history[19].StartTime {
		t.Fatalf("история не отсортирована или текущая REST-свеча попала в базу: %+v", snapshot.History)
	}
	if snapshot.History[19].Turnover != 2000 || snapshot.History[19].Volume != 20 {
		t.Fatalf("volume/turnover потеряны: %+v", snapshot.History[19])
	}
	if !snapshot.HistoryReady {
		t.Fatal("20 завершённых свечей должны прогреть историческую базу")
	}
	if snapshot.Status != CandleStatusWarmingUp {
		t.Fatalf("status = %s, ожидался warming_up", snapshot.Status)
	}
}

func TestCandleStoreNeedsTwentyContinuousCompletedCandles(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 34, 30, 0, time.UTC)
	store := NewCandleStore(time.Minute)
	store.Activate("AAAUSDT")
	if err := store.LoadHistory("AAAUSDT", testMinuteHistory(now, 19), now); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := store.Snapshot("AAAUSDT", now)
	if snapshot.HistoryReady || snapshot.Status != CandleStatusWarmingUp {
		t.Fatalf("19 свечей дали готовый статус: %+v", snapshot)
	}

	store.Activate("BBBUSDT")
	gapped := testMinuteHistory(now, 20)
	gapped[10].StartTime = gapped[10].StartTime.Add(time.Minute)
	if err := store.LoadHistory("BBBUSDT", gapped, now); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = store.Snapshot("BBBUSDT", now)
	if snapshot.HistoryReady {
		t.Fatalf("история с пропуском признана готовой: %+v", snapshot)
	}
}

func TestCandleStoreUpdatesCurrentCandleAndClosesWithoutDuplicates(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 34, 30, 0, time.UTC)
	store := NewCandleStore(90 * time.Second)
	store.Activate("AAAUSDT")
	if err := store.LoadHistory("AAAUSDT", testMinuteHistory(now, 20), now); err != nil {
		t.Fatal(err)
	}

	currentStart := now.Truncate(time.Minute)
	if err := store.ApplyUpdate("AAAUSDT", testLiveCandle(currentStart, false, 50), now); err != nil {
		t.Fatalf("первое обновление текущей свечи: %v", err)
	}
	if err := store.ApplyUpdate("AAAUSDT", testLiveCandle(currentStart, false, 125), now.Add(5*time.Second)); err != nil {
		t.Fatalf("повторное обновление текущей свечи: %v", err)
	}
	snapshot, _ := store.Snapshot("AAAUSDT", now.Add(5*time.Second))
	if len(snapshot.History) != 20 || snapshot.Current == nil || snapshot.Current.Turnover != 125 {
		t.Fatalf("обновление текущей свечи создало дубль или не заменило данные: %+v", snapshot)
	}
	if snapshot.Status != CandleStatusReady || !snapshot.Fresh {
		t.Fatalf("актуальные данные с 20 свечами не стали ready: %+v", snapshot)
	}

	closed := testLiveCandle(currentStart, true, 150)
	if err := store.ApplyUpdate("AAAUSDT", closed, now.Add(10*time.Second)); err != nil {
		t.Fatalf("confirm=true: %v", err)
	}
	if err := store.ApplyUpdate("AAAUSDT", closed, now.Add(11*time.Second)); err != nil {
		t.Fatalf("дублирующий confirm=true: %v", err)
	}
	snapshot, _ = store.Snapshot("AAAUSDT", now.Add(11*time.Second))
	if len(snapshot.History) != 20 || snapshot.History[19].StartTime != currentStart ||
		snapshot.History[19].Turnover != 150 || snapshot.Current != nil {
		t.Fatalf("закрытая свеча добавлена несколько раз или не заменила самую старую: %+v", snapshot)
	}
}

func TestCandleStoreDetectsSkippedCandleAndReconnectReloadsHistory(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 34, 30, 0, time.UTC)
	store := NewCandleStore(90 * time.Second)
	store.Activate("AAAUSDT")
	if err := store.LoadHistory("AAAUSDT", testMinuteHistory(now, 20), now); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyUpdate("AAAUSDT", testLiveCandle(now.Truncate(time.Minute), false, 10), now); err != nil {
		t.Fatal(err)
	}
	nextMinute := now.Truncate(time.Minute).Add(2 * time.Minute)
	err := store.ApplyUpdate("AAAUSDT", testLiveCandle(nextMinute, false, 20), now.Add(2*time.Minute))
	if !errors.Is(err, ErrCandleGap) {
		t.Fatalf("ошибка пропущенной свечи = %v, ожидалась ErrCandleGap", err)
	}
	snapshot, _ := store.Snapshot("AAAUSDT", now.Add(2*time.Minute))
	if snapshot.HistoryReady {
		t.Fatalf("пропуск минутной свечи не инвалидировал историю: %+v", snapshot)
	}

	store.MarkDisconnected("AAAUSDT", "test disconnect")
	snapshot, _ = store.Snapshot("AAAUSDT", now.Add(2*time.Minute))
	if snapshot.Connected || snapshot.Current != nil || snapshot.HistoryReady || snapshot.Status != CandleStatusDisconnected {
		t.Fatalf("после разрыва остались пригодные текущие данные: %+v", snapshot)
	}
	reconnectedAt := now.Add(3 * time.Minute)
	if err := store.LoadHistory("AAAUSDT", testMinuteHistory(reconnectedAt, 20), reconnectedAt); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyUpdate("AAAUSDT", testLiveCandle(reconnectedAt.Truncate(time.Minute), false, 30), reconnectedAt); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = store.Snapshot("AAAUSDT", reconnectedAt)
	if snapshot.Status != CandleStatusReady {
		t.Fatalf("после восстановления истории статус = %s, ожидался ready", snapshot.Status)
	}
}

func TestCandleStoreDropsInactiveSymbolsAndBoundsHistory(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 34, 30, 0, time.UTC)
	store := NewCandleStore(time.Minute)
	store.Activate("AAAUSDT")
	history := testMinuteHistory(now, 40)
	if err := store.LoadHistory("AAAUSDT", history, now); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := store.Snapshot("AAAUSDT", now)
	if len(snapshot.History) != 20 {
		t.Fatalf("хранилище содержит %d свечей, ожидалось максимум 20", len(snapshot.History))
	}
	store.MarkInactive("AAAUSDT")
	if _, ok := store.Snapshot("AAAUSDT", now); ok {
		t.Fatal("неактивный символ остался в CandleStore")
	}
	if got := store.ActiveSymbols(); len(got) != 0 {
		t.Fatalf("ActiveSymbols() = %v, ожидался пустой список", got)
	}
}


func TestCandleStoreRejectsStaleButInternallyContinuousHistory(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 34, 30, 0, time.UTC)
	history := testMinuteHistory(now, 20)
	// Сдвигаем всю непрерывную историю на одну минуту назад:
	// 20 свечей есть, но последняя закрытая свеча уже устарела.
	for index := range history {
		history[index].StartTime = history[index].StartTime.Add(-time.Minute)
	}

	store := NewCandleStore(time.Minute)
	store.Activate("AAAUSDT")
	if err := store.LoadHistory("AAAUSDT", history, now); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := store.Snapshot("AAAUSDT", now)
	if snapshot.HistoryReady || snapshot.Status != CandleStatusWarmingUp {
		t.Fatalf("устаревшая история признана готовой: %+v", snapshot)
	}
}
