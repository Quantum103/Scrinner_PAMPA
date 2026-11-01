package stream

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"time"
)

var (
	ErrSnapshotRequired = errors.New("для стакана требуется новый snapshot")
	ErrInvalidOrderbook = errors.New("некорректное обновление стакана")
)

type PriceLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

type OrderBookSnapshot struct {
	Symbol       string       `json:"symbol"`
	Bids         []PriceLevel `json:"bids"`
	Asks         []PriceLevel `json:"asks"`
	BestBid      string       `json:"best_bid"`
	BestAsk      string       `json:"best_ask"`
	UpdateID     int64        `json:"update_id"`
	Sequence     int64        `json:"sequence"`
	ExchangeTime time.Time    `json:"exchange_time"`
	ReceivedAt   time.Time    `json:"received_at"`
	Valid        bool         `json:"valid"`
	Fresh        bool         `json:"fresh"`
}

type rawLevel [2]string

type orderBookMessage struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	TS    int64  `json:"ts"`
	Data  struct {
		Symbol string     `json:"s"`
		Bids   []rawLevel `json:"b"`
		Asks   []rawLevel `json:"a"`
		Update int64      `json:"u"`
		Seq    int64      `json:"seq"`
		CTS    int64      `json:"cts"`
	} `json:"data"`
}

type bookLevel struct {
	level PriceLevel
	price *big.Rat
}

type orderBook struct {
	bids       map[string]bookLevel
	asks       map[string]bookLevel
	updateID   int64
	sequence   int64
	exchangeAt time.Time
	receivedAt time.Time
	valid      bool
}

func newOrderBook() *orderBook {
	return &orderBook{
		bids: make(map[string]bookLevel),
		asks: make(map[string]bookLevel),
	}
}

func (b *orderBook) invalidate() {
	b.valid = false
	b.bids = make(map[string]bookLevel)
	b.asks = make(map[string]bookLevel)
	b.updateID = 0
	b.sequence = 0
	b.exchangeAt = time.Time{}
	b.receivedAt = time.Time{}
}

func (b *orderBook) apply(message orderBookMessage, depth int, maxAge time.Duration, receivedAt time.Time) error {
	if message.Data.Symbol == "" || message.TS <= 0 || message.Data.Update <= 0 {
		b.invalidate()
		return fmt.Errorf("%w: отсутствует символ, timestamp или update ID", ErrInvalidOrderbook)
	}
	if age := receivedAt.Sub(time.UnixMilli(message.TS)); age > maxAge || age < -maxAge {
		b.invalidate()
		return fmt.Errorf("%w: время сообщения вне допустимого диапазона (%s)", ErrInvalidOrderbook, age)
	}

	exchangeTime := time.UnixMilli(message.TS).UTC()
	if message.Data.CTS > 0 {
		exchangeTime = time.UnixMilli(message.Data.CTS).UTC()
	}

	switch message.Type {
	case "snapshot":
		bids, err := parseLevels(message.Data.Bids)
		if err != nil {
			b.invalidate()
			return fmt.Errorf("%w: bids: %v", ErrInvalidOrderbook, err)
		}
		asks, err := parseLevels(message.Data.Asks)
		if err != nil {
			b.invalidate()
			return fmt.Errorf("%w: asks: %v", ErrInvalidOrderbook, err)
		}
		b.bids = bids
		b.asks = asks
		b.updateID = message.Data.Update
		b.sequence = message.Data.Seq
		b.exchangeAt = exchangeTime
		b.receivedAt = receivedAt
		b.valid = true
		b.trim(depth)
		if err := b.validateTop(); err != nil {
			b.invalidate()
			return err
		}
		return nil

	case "delta":
		if !b.valid {
			return ErrSnapshotRequired
		}
		if receivedAt.Before(b.receivedAt) || receivedAt.Sub(b.receivedAt) > maxAge {
			b.invalidate()
			return ErrSnapshotRequired
		}
		if message.Data.Update == 1 {
			b.invalidate()
			return ErrSnapshotRequired
		}
		if message.Data.Update <= b.updateID {
			return nil
		}
		if message.Data.Seq > 0 && b.sequence > 0 && message.Data.Seq < b.sequence {
			b.invalidate()
			return fmt.Errorf("%w: sequence уменьшился с %d до %d", ErrInvalidOrderbook, b.sequence, message.Data.Seq)
		}
		if err := applyLevels(b.bids, message.Data.Bids); err != nil {
			b.invalidate()
			return fmt.Errorf("%w: bids: %v", ErrInvalidOrderbook, err)
		}
		if err := applyLevels(b.asks, message.Data.Asks); err != nil {
			b.invalidate()
			return fmt.Errorf("%w: asks: %v", ErrInvalidOrderbook, err)
		}
		b.updateID = message.Data.Update
		if message.Data.Seq > 0 {
			b.sequence = message.Data.Seq
		}
		b.exchangeAt = exchangeTime
		b.receivedAt = receivedAt
		b.trim(depth)
		return b.validateTop()

	default:
		return fmt.Errorf("неизвестный тип сообщения стакана %q", message.Type)
	}
}

func parseLevels(levels []rawLevel) (map[string]bookLevel, error) {
	result := make(map[string]bookLevel, len(levels))
	for _, item := range levels {
		price, size, key, err := parseLevel(item)
		if err != nil {
			return nil, err
		}
		if size <= 0 {
			return nil, fmt.Errorf("snapshot содержит неположительный размер по цене %s", item[0])
		}
		result[key] = bookLevel{level: PriceLevel{Price: item[0], Size: item[1]}, price: price}
	}
	return result, nil
}

func applyLevels(side map[string]bookLevel, levels []rawLevel) error {
	for _, item := range levels {
		price, size, key, err := parseLevel(item)
		if err != nil {
			return err
		}
		if size == 0 {
			delete(side, key)
			continue
		}
		side[key] = bookLevel{level: PriceLevel{Price: item[0], Size: item[1]}, price: price}
	}
	return nil
}

func parseLevel(item rawLevel) (*big.Rat, float64, string, error) {
	if item[0] == "" || item[1] == "" {
		return nil, 0, "", errors.New("цена или количество отсутствуют")
	}
	price, ok := new(big.Rat).SetString(item[0])
	if !ok || price.Sign() <= 0 {
		return nil, 0, "", fmt.Errorf("некорректная цена %q", item[0])
	}
	size, err := strconv.ParseFloat(item[1], 64)
	if err != nil || size < 0 || math.IsNaN(size) || math.IsInf(size, 0) {
		return nil, 0, "", fmt.Errorf("некорректное количество %q", item[1])
	}
	return price, size, price.RatString(), nil
}

func (b *orderBook) trim(depth int) {
	trimSide(b.bids, depth, true)
	trimSide(b.asks, depth, false)
}

func trimSide(side map[string]bookLevel, depth int, descending bool) {
	if len(side) <= depth {
		return
	}
	levels := make([]bookLevel, 0, len(side))
	for _, level := range side {
		levels = append(levels, level)
	}
	sort.Slice(levels, func(i, j int) bool {
		if descending {
			return levels[i].price.Cmp(levels[j].price) > 0
		}
		return levels[i].price.Cmp(levels[j].price) < 0
	})
	for _, level := range levels[depth:] {
		key, _ := new(big.Rat).SetString(level.level.Price)
		delete(side, key.RatString())
	}
}

func (b *orderBook) validateTop() error {
	if len(b.bids) == 0 || len(b.asks) == 0 {
		b.invalidate()
		return fmt.Errorf("%w: одна из сторон стакана пуста", ErrInvalidOrderbook)
	}
	bestBid, _ := b.best(true)
	bestAsk, _ := b.best(false)
	if bestBid.price.Cmp(bestAsk.price) >= 0 {
		b.invalidate()
		return fmt.Errorf("%w: пересекающийся стакан bid=%s ask=%s", ErrInvalidOrderbook, bestBid.level.Price, bestAsk.level.Price)
	}
	return nil
}

func (b *orderBook) best(bid bool) (bookLevel, bool) {
	var best bookLevel
	found := false
	for _, level := range mapForSide(b, bid) {
		if !found || (bid && level.price.Cmp(best.price) > 0) || (!bid && level.price.Cmp(best.price) < 0) {
			best = level
			found = true
		}
	}
	return best, found
}

func mapForSide(b *orderBook, bid bool) map[string]bookLevel {
	if bid {
		return b.bids
	}
	return b.asks
}

func (b *orderBook) snapshot(symbol string) OrderBookSnapshot {
	bids := sortedLevels(b.bids, true)
	asks := sortedLevels(b.asks, false)
	snapshot := OrderBookSnapshot{
		Symbol:       symbol,
		Bids:         bids,
		Asks:         asks,
		UpdateID:     b.updateID,
		Sequence:     b.sequence,
		ExchangeTime: b.exchangeAt,
		ReceivedAt:   b.receivedAt,
		Valid:        b.valid,
	}
	if len(bids) > 0 {
		snapshot.BestBid = bids[0].Price
	}
	if len(asks) > 0 {
		snapshot.BestAsk = asks[0].Price
	}
	return snapshot
}

func sortedLevels(side map[string]bookLevel, descending bool) []PriceLevel {
	levels := make([]bookLevel, 0, len(side))
	for _, level := range side {
		levels = append(levels, level)
	}
	sort.Slice(levels, func(i, j int) bool {
		if descending {
			return levels[i].price.Cmp(levels[j].price) > 0
		}
		return levels[i].price.Cmp(levels[j].price) < 0
	})
	result := make([]PriceLevel, 0, len(levels))
	for _, level := range levels {
		result = append(result, level.level)
	}
	return result
}
