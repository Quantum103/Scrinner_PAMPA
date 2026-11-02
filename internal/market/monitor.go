package market

import (
	"sync"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
)

// MonitorSnapshot содержит последний снимок подходящих контрактов.
type MonitorSnapshot struct {
	UpdatedAt time.Time
	Items     []bybit.MarketData
}

// Monitor хранит полный REST-снимок рынка, рейтинг и CandleStore независимо от
// подробных потоков publicTrade/orderbook.
type Monitor struct {
	mu         sync.RWMutex
	updatedAt  time.Time
	items      []bybit.MarketData
	ranking    RankingSnapshot
	candleData *CandleStore
}

func NewMonitor(candleStores ...*CandleStore) *Monitor {
	monitor := &Monitor{}
	if len(candleStores) > 0 {
		monitor.candleData = candleStores[0]
	}
	return monitor
}

// Candles возвращает хранилище минутных свечей, связанное с полным Market Monitor.
func (m *Monitor) Candles() *CandleStore {
	return m.candleData
}

func (m *Monitor) UpdateMarketSnapshot(items []bybit.MarketData, updatedAt time.Time) {
	copyItems := append([]bybit.MarketData(nil), items...)
	m.mu.Lock()
	m.items = copyItems
	m.updatedAt = updatedAt
	m.mu.Unlock()
}

func (m *Monitor) Snapshot() MonitorSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return MonitorSnapshot{UpdatedAt: m.updatedAt, Items: append([]bybit.MarketData(nil), m.items...)}
}

func (m *Monitor) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.items)
}

// UpdateRanking сохраняет последний расчёт двух независимых рейтингов.
func (m *Monitor) UpdateRanking(snapshot RankingSnapshot) {
	copySnapshot := cloneRankingSnapshot(snapshot)
	m.mu.Lock()
	m.ranking = copySnapshot
	m.mu.Unlock()
}

// Ranking возвращает отдельную копию рейтинга, безопасную для чтения другими компонентами.
func (m *Monitor) Ranking() RankingSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cloneRankingSnapshot(m.ranking)
}
