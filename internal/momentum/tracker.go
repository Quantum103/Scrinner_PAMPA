package momentum

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	DefaultWindow           = 15 * time.Minute
	DefaultThresholdPercent = 5.0
	DefaultMaxReferenceAge  = 90 * time.Second
	defaultMaximumPoints    = 1024
)

type PricePoint struct {
	At    time.Time
	Price float64
}

type Signal struct {
	Symbol         string
	At             time.Time
	Price          float64
	ReferenceAt    time.Time
	ReferencePrice float64
	ChangePercent  float64
}

type symbolSeries struct {
	points     []PricePoint
	armed      bool
	hasHistory bool
}

type Tracker struct {
	mu              sync.RWMutex
	window          time.Duration
	threshold       float64
	maxReferenceAge time.Duration
	maximumPoints   int
	symbols         map[string]*symbolSeries
}

func NewTracker(window time.Duration, thresholdPercent float64, maxReferenceAge time.Duration) (*Tracker, error) {
	if window <= 0 {
		return nil, errors.New("momentum window must be positive")
	}
	if math.IsNaN(thresholdPercent) || math.IsInf(thresholdPercent, 0) || thresholdPercent <= 0 {
		return nil, errors.New("momentum threshold must be positive and finite")
	}
	if maxReferenceAge <= 0 {
		return nil, errors.New("momentum maximum reference age must be positive")
	}
	return &Tracker{
		window: window, threshold: thresholdPercent, maxReferenceAge: maxReferenceAge,
		maximumPoints: defaultMaximumPoints, symbols: make(map[string]*symbolSeries),
	}, nil
}

// Seed merges historical reference points into the live series without
// generating historical alerts. It is safe to call while WebSocket updates
// are arriving; points are sorted and duplicate seconds are coalesced.
func (t *Tracker) Seed(symbol string, points []PricePoint) error {
	symbol, err := normalizeSymbol(symbol)
	if err != nil {
		return err
	}
	validated := make([]PricePoint, 0, len(points))
	for _, point := range points {
		if err := validatePoint(point); err != nil {
			return fmt.Errorf("invalid seed point for %s: %w", symbol, err)
		}
		validated = append(validated, point)
	}
	sort.Slice(validated, func(i, j int) bool { return validated[i].At.Before(validated[j].At) })

	t.mu.Lock()
	defer t.mu.Unlock()
	series := t.seriesLocked(symbol)
	for _, point := range validated {
		series.points = insertOrReplaceSecond(series.points, point)
	}
	if len(validated) > 0 {
		series.hasHistory = true
	}
	t.pruneLocked(series, latestPointTime(series.points))
	return nil
}

func (t *Tracker) Add(symbol string, point PricePoint) (*Signal, error) {
	symbol, err := normalizeSymbol(symbol)
	if err != nil {
		return nil, err
	}
	if err := validatePoint(point); err != nil {
		return nil, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	series := t.seriesLocked(symbol)
	if len(series.points) > 0 {
		last := series.points[len(series.points)-1]
		if point.At.Before(last.At) {
			return nil, nil
		}
	}

	cutoff := point.At.Add(-t.window)
	reference, ok := latestPointAtOrBefore(series.points, cutoff)
	if ok {
		age := cutoff.Sub(reference.At)
		if age < 0 || age > t.maxReferenceAge {
			ok = false
		}
	}

	var signal *Signal
	if ok {
		change := (point.Price/reference.Price - 1) * 100
		thresholdPrice := reference.Price * (1 + t.threshold/100)
		if point.Price <= thresholdPrice {
			series.armed = true
		} else if series.armed {
			signal = &Signal{
				Symbol: symbol, At: point.At.UTC(), Price: point.Price,
				ReferenceAt: reference.At.UTC(), ReferencePrice: reference.Price,
				ChangePercent: change,
			}
			series.armed = false
		}
	}

	series.points = insertOrReplaceSecond(series.points, point)
	t.pruneLocked(series, point.At)
	return signal, nil
}

func (t *Tracker) HasSymbol(symbol string) bool {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	t.mu.RLock()
	defer t.mu.RUnlock()
	series := t.symbols[symbol]
	return series != nil && len(series.points) > 0
}

// HasHistory indicates that REST history was successfully merged for a
// symbol. Live ticks alone do not suppress a retry after a failed seed request.
func (t *Tracker) HasHistory(symbol string) bool {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	t.mu.RLock()
	defer t.mu.RUnlock()
	series := t.symbols[symbol]
	return series != nil && series.hasHistory
}

func (t *Tracker) KeepSymbols(symbols []string) {
	keep := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		symbol = strings.ToUpper(strings.TrimSpace(symbol))
		if symbol != "" {
			keep[symbol] = struct{}{}
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for symbol := range t.symbols {
		if _, exists := keep[symbol]; !exists {
			delete(t.symbols, symbol)
		}
	}
}

func (t *Tracker) seriesLocked(symbol string) *symbolSeries {
	series := t.symbols[symbol]
	if series == nil {
		series = &symbolSeries{armed: true}
		t.symbols[symbol] = series
	}
	return series
}

func (t *Tracker) pruneLocked(series *symbolSeries, now time.Time) {
	if len(series.points) <= 1 {
		return
	}
	cutoff := now.Add(-t.window - t.maxReferenceAge)
	firstUseful := sort.Search(len(series.points), func(i int) bool {
		return !series.points[i].At.Before(cutoff)
	})
	if firstUseful > 0 {
		firstUseful--
		series.points = append([]PricePoint(nil), series.points[firstUseful:]...)
	}
	if len(series.points) > t.maximumPoints {
		series.points = append([]PricePoint(nil), series.points[len(series.points)-t.maximumPoints:]...)
	}
}

func insertOrReplaceSecond(points []PricePoint, point PricePoint) []PricePoint {
	second := point.At.Unix()
	index := sort.Search(len(points), func(i int) bool { return points[i].At.Unix() >= second })
	if index < len(points) && points[index].At.Unix() == second {
		if point.At.After(points[index].At) || point.At.Equal(points[index].At) {
			points[index] = point
		}
		return points
	}
	points = append(points, PricePoint{})
	copy(points[index+1:], points[index:])
	points[index] = point
	return points
}

func latestPointAtOrBefore(points []PricePoint, cutoff time.Time) (PricePoint, bool) {
	index := sort.Search(len(points), func(i int) bool { return points[i].At.After(cutoff) })
	if index == 0 {
		return PricePoint{}, false
	}
	return points[index-1], true
}

func latestPointTime(points []PricePoint) time.Time {
	if len(points) == 0 {
		return time.Time{}
	}
	return points[len(points)-1].At
}

func normalizeSymbol(symbol string) (string, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return "", errors.New("momentum symbol is required")
	}
	for _, char := range symbol {
		if !(char >= 'A' && char <= 'Z') && !(char >= '0' && char <= '9') {
			return "", fmt.Errorf("invalid momentum symbol %q", symbol)
		}
	}
	return symbol, nil
}

func validatePoint(point PricePoint) error {
	if point.At.IsZero() {
		return errors.New("price timestamp is required")
	}
	if math.IsNaN(point.Price) || math.IsInf(point.Price, 0) || point.Price <= 0 {
		return errors.New("price must be positive and finite")
	}
	return nil
}
