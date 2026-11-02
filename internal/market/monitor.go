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

// Monitor хранит REST-состояние рынка независимо от подробных потоков сделок и стакана.
// На этом этапе компонент только принимает и публикует снимок; рейтинг появится позже.
type Monitor struct {
	mu        sync.RWMutex
	updatedAt time.Time
	items     []bybit.MarketData
}

func NewMonitor() *Monitor { return &Monitor{} }

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
