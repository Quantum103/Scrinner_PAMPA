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
	MaxSupportedWindow      = 60 * time.Minute
	defaultMaximumPoints    = 4096
)

type Rule struct {
	Window          time.Duration
	ThresholdPercent float64
}

type PricePoint struct {
	At    time.Time
	Price float64
}

type Signal struct {
	Symbol          string
	At              time.Time
	Price           float64
	ReferenceAt     time.Time
	ReferencePrice  float64
	ChangePercent   float64
	Window          time.Duration
	ThresholdPercent float64
}

type symbolSeries struct {
	points     []PricePoint
	armed      map[Rule]bool
	hasHistory bool
}

type Tracker struct {
	mu              sync.RWMutex
	rules           []Rule
	maxReferenceAge time.Duration
	maximumPoints   int
	symbols         map[string]*symbolSeries
}

// NewTracker preserves the single-rule constructor used by older callers.
// New code that manages user-configured rules should use NewRuleTracker.
func NewTracker(window time.Duration, thresholdPercent float64, maxReferenceAge time.Duration) (*Tracker, error) {
	tracker, err := NewRuleTracker(maxReferenceAge)
	if err != nil {
		return nil, err
	}
	if err := tracker.SetRules([]Rule{{Window: window, ThresholdPercent: thresholdPercent}}); err != nil {
		return nil, err
	}
	return tracker, nil
}

// NewRuleTracker creates a tracker whose active rule set can be replaced at
// runtime. It retains enough price history for any supported rule, so adding a
// longer rule does not require restarting or reseeding every symbol.
func NewRuleTracker(maxReferenceAge time.Duration) (*Tracker, error) {
	if maxReferenceAge <= 0 {
		return nil, errors.New("momentum maximum reference age must be positive")
	}
	return &Tracker{
		maxReferenceAge: maxReferenceAge,
		maximumPoints:   defaultMaximumPoints,
		symbols:         make(map[string]*symbolSeries),
	}, nil
}

// SetRules replaces the active rule set without discarding ticker history.
// Each rule has an independent crossing/re-arm state. Duplicate rules are
// collapsed, and the remaining rules are processed in deterministic order.
func (t *Tracker) SetRules(rules []Rule) error {
	unique := make(map[Rule]struct{}, len(rules))
	normalized := make([]Rule, 0, len(rules))
	for _, rule := range rules {
		if err := validateRule(rule); err != nil {
			return err
		}
		if _, exists := unique[rule]; exists {
			continue
		}
		unique[rule] = struct{}{}
		normalized = append(normalized, rule)
	}
	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].Window != normalized[j].Window {
			return normalized[i].Window < normalized[j].Window
		}
		return normalized[i].ThresholdPercent < normalized[j].ThresholdPercent
	})

	t.mu.Lock()
	defer t.mu.Unlock()
	t.rules = normalized
	for _, series := range t.symbols {
		if series.armed == nil {
			series.armed = make(map[Rule]bool)
		}
		for rule := range series.armed {
			if _, active := unique[rule]; !active {
				delete(series.armed, rule)
			}
		}
		for _, rule := range normalized {
			if _, exists := series.armed[rule]; !exists {
				series.armed[rule] = true
			}
		}
	}
	return nil
}

func validateRule(rule Rule) error {
	if rule.Window < time.Minute || rule.Window > MaxSupportedWindow || rule.Window%time.Second != 0 {
		return fmt.Errorf("momentum window must be between 1 minute and %s", MaxSupportedWindow)
	}
	if math.IsNaN(rule.ThresholdPercent) || math.IsInf(rule.ThresholdPercent, 0) ||
		rule.ThresholdPercent < 0.01 || rule.ThresholdPercent > 1000 {
		return errors.New("momentum threshold must be between 0.01 and 1000 percent")
	}
	return nil
}

func (t *Tracker) RuleCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.rules)
}

// Seed merges historical reference points into the live series without
// generating alerts. Live prices that arrived while REST history was loading
// are merged rather than overwritten.
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

// Add is the backwards-compatible single-signal form. Multi-rule consumers
// should call AddAll so every matching configured rule can produce an alert.
func (t *Tracker) Add(symbol string, point PricePoint) (*Signal, error) {
	signals, err := t.AddAll(symbol, point)
	if err != nil || len(signals) == 0 {
		return nil, err
	}
	return &signals[0], nil
}

// AddAll evaluates every active rule against the same incoming ticker update.
// One rule can signal only once during a continuous above-threshold excursion;
// it re-arms independently when its own rolling return falls to or below its
// threshold.
func (t *Tracker) AddAll(symbol string, point PricePoint) ([]Signal, error) {
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

	signals := make([]Signal, 0)
	for _, rule := range t.rules {
		cutoff := point.At.Add(-rule.Window)
		reference, ok := latestPointAtOrBefore(series.points, cutoff)
		if !ok {
			continue
		}
		age := cutoff.Sub(reference.At)
		if age < 0 || age > t.maxReferenceAge {
			continue
		}

		thresholdPrice := reference.Price * (1 + rule.ThresholdPercent/100)
		if point.Price <= thresholdPrice {
			series.armed[rule] = true
			continue
		}
		if !series.armed[rule] {
			continue
		}
		change := (point.Price/reference.Price - 1) * 100
		signals = append(signals, Signal{
			Symbol: symbol, At: point.At.UTC(), Price: point.Price,
			ReferenceAt: reference.At.UTC(), ReferencePrice: reference.Price,
			ChangePercent: change, Window: rule.Window,
			ThresholdPercent: rule.ThresholdPercent,
		})
		series.armed[rule] = false
	}

	series.points = insertOrReplaceSecond(series.points, point)
	t.pruneLocked(series, point.At)
	return signals, nil
}

// Rearm makes every active rule for a symbol retryable after persistence fails.
func (t *Tracker) Rearm(symbol string) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	t.mu.Lock()
	defer t.mu.Unlock()
	if series := t.symbols[symbol]; series != nil {
		for rule := range series.armed {
			series.armed[rule] = true
		}
	}
}

// RearmRule makes only the failed rule retryable. Other signals generated by
// the same WebSocket update remain disarmed.
func (t *Tracker) RearmRule(symbol string, rule Rule) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	t.mu.Lock()
	defer t.mu.Unlock()
	if series := t.symbols[symbol]; series != nil {
		if _, exists := series.armed[rule]; exists {
			series.armed[rule] = true
		}
	}
}

func (t *Tracker) HasSymbol(symbol string) bool {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	t.mu.RLock()
	defer t.mu.RUnlock()
	series := t.symbols[symbol]
	return series != nil && len(series.points) > 0
}

// HasHistory indicates that a symbol's REST candle history was successfully
// merged. Live ticks alone do not suppress a retry after a failed seed request.
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
		series = &symbolSeries{armed: make(map[Rule]bool)}
		for _, rule := range t.rules {
			series.armed[rule] = true
		}
		t.symbols[symbol] = series
	}
	return series
}

func (t *Tracker) pruneLocked(series *symbolSeries, now time.Time) {
	if len(series.points) <= 1 {
		return
	}
	cutoff := now.Add(-MaxSupportedWindow - t.maxReferenceAge)
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
