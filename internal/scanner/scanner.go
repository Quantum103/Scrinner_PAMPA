package scanner

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	"github.com/Quantum103/Scrinner_PAMPA/internal/indicators"
	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
)

type MarketDataSource interface {
	GetServerTime(context.Context) (time.Time, error)
	GetKlinesAt(context.Context, string, string, int, time.Time) ([]bybit.Candle, error)
}

type Repository interface {
	ListActiveInstruments(context.Context, int) ([]storage.Instrument, error)
	GetScannerState(context.Context, string, string) (storage.ScannerState, error)
	InitializeScannerState(context.Context, storage.ScannerState) (storage.ScannerState, bool, error)
	CommitScannedCandle(context.Context, storage.ScannerState, *storage.Signal) (int64, bool, int64, error)
}

type ProfileRepository interface {
	ListScannerWorkloads(context.Context) ([]storage.ScannerWorkload, error)
	HasActiveScannerUsers(context.Context) (bool, error)
	GetScannerProfileState(context.Context, string, string, int, int) (storage.ScannerProfileState, error)
	InitializeScannerProfileState(context.Context, storage.ScannerProfileState) (storage.ScannerProfileState, bool, error)
	CommitScannerProfileCandle(context.Context, storage.ScannerProfileState, *storage.Signal) (int64, bool, int64, error)
}

type Settings struct {
	Timeframes      []string
	RSILength       int
	EMALength       int
	InstrumentLimit int
	HistoryCandles  int
}

type Report struct {
	Instruments      int
	PairsScanned     int
	CandlesProcessed int
	SignalsInserted  int
	DeliveriesQueued int64
}

type Scanner struct {
	source            MarketDataSource
	repository        Repository
	profileRepository ProfileRepository
	settings          Settings
	now               func() time.Time

	// Processing is serialized so REST reconciliation and WebSocket candle
	// events cannot advance the same durable cursor concurrently.
	mu      sync.Mutex
	history map[string][]bybit.Candle

	// Active profile routing is refreshed together with REST reconciliation.
	// WebSocket candle handling must not issue profile-discovery SQL per event.
	profilePairs          map[string][]storage.ScannerProfile
	hasActiveProfileUsers bool
	profileCacheReady     bool
}

func New(source MarketDataSource, repository Repository, settings Settings) (*Scanner, error) {
	if source == nil {
		return nil, errors.New("scanner market data source is required")
	}
	if repository == nil {
		return nil, errors.New("scanner repository is required")
	}
	if err := validateSettings(settings); err != nil {
		return nil, err
	}
	settings.Timeframes = append([]string(nil), settings.Timeframes...)
	profileRepository, _ := repository.(ProfileRepository)
	return &Scanner{
		source: source, repository: repository, profileRepository: profileRepository, settings: settings,
		now: time.Now, history: make(map[string][]bybit.Candle),
		profilePairs: make(map[string][]storage.ScannerProfile),
	}, nil
}

func validateSettings(settings Settings) error {
	if settings.RSILength < 2 || settings.RSILength > 500 {
		return errors.New("scanner RSI length must be between 2 and 500")
	}
	if settings.EMALength < 1 || settings.EMALength > 500 {
		return errors.New("scanner EMA length must be between 1 and 500")
	}
	if settings.InstrumentLimit < 1 || settings.InstrumentLimit > 10000 {
		return errors.New("scanner instrument limit must be between 1 and 10000")
	}
	if settings.HistoryCandles < settings.RSILength+settings.EMALength || settings.HistoryCandles > 1000 {
		return errors.New("scanner history candles must be between RSI length + EMA length and 1000")
	}
	if len(settings.Timeframes) == 0 {
		return errors.New("scanner must have at least one timeframe")
	}
	seen := make(map[string]struct{}, len(settings.Timeframes))
	for _, timeframe := range settings.Timeframes {
		if _, err := timeframeDuration(timeframe); err != nil {
			return err
		}
		if _, exists := seen[timeframe]; exists {
			return fmt.Errorf("scanner timeframe %q is duplicated", timeframe)
		}
		seen[timeframe] = struct{}{}
	}
	return nil
}

// ScanOnce reconciles every configured pair against REST using one exchange-time
// snapshot. It is used for initial history loading and periodic recovery.
func (s *Scanner) ScanOnce(ctx context.Context) (Report, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.profileRepository != nil {
		workloads, err := s.profileRepository.ListScannerWorkloads(ctx)
		if err != nil {
			return Report{}, fmt.Errorf("list personalized scanner workloads: %w", err)
		}
		hasUsers := len(workloads) > 0
		if !hasUsers {
			hasUsers, err = s.profileRepository.HasActiveScannerUsers(ctx)
			if err != nil {
				return Report{}, fmt.Errorf("check active personalized scanner users: %w", err)
			}
		}
		if err := s.refreshProfileCacheLocked(workloads, hasUsers); err != nil {
			return Report{}, fmt.Errorf("refresh personalized scanner profile cache: %w", err)
		}
		if len(workloads) > 0 {
			return s.scanProfileWorkloadsLocked(ctx, workloads)
		}
		if hasUsers {
			// Active users with an empty selected universe must not fall back to
			// the global scanner's instrument list.
			return Report{}, nil
		}
	}
	return s.scanLegacyOnceLocked(ctx)
}

// refreshProfileCacheLocked builds the immutable routing index used by the
// WebSocket hot path. It is called under s.mu before the corresponding REST
// reconciliation so new profiles have their baseline before live events run.
func (s *Scanner) refreshProfileCacheLocked(workloads []storage.ScannerWorkload, hasUsers bool) error {
	profilesByPair := make(map[string][]storage.ScannerProfile)
	for _, workload := range workloads {
		if _, err := timeframeDuration(workload.Timeframe); err != nil {
			return err
		}
		profile := storage.ScannerProfile{RSILength: workload.RSILength, EMALength: workload.EMALength}
		if profile.RSILength < 2 || profile.RSILength > 500 ||
			profile.EMALength < 1 || profile.EMALength > 500 ||
			profile.RSILength+profile.EMALength > 1000 {
			return fmt.Errorf("invalid active RSI/EMA profile: RSI=%d EMA=%d", profile.RSILength, profile.EMALength)
		}
		for _, rawSymbol := range workload.Symbols {
			symbol := strings.ToUpper(strings.TrimSpace(rawSymbol))
			if symbol == "" {
				return errors.New("personalized scanner workload contains an empty symbol")
			}
			key := historyKey(symbol, workload.Timeframe)
			profilesByPair[key] = append(profilesByPair[key], profile)
		}
	}

	// Workloads normally arrive sorted and unique from PostgreSQL. Sorting and
	// in-place deduplication preserve correctness for other callers while
	// avoiding one nested map allocation per symbol/timeframe pair.
	for key, profiles := range profilesByPair {
		sort.Slice(profiles, func(i, j int) bool {
			if profiles[i].RSILength == profiles[j].RSILength {
				return profiles[i].EMALength < profiles[j].EMALength
			}
			return profiles[i].RSILength < profiles[j].RSILength
		})
		unique := profiles[:0]
		for _, profile := range profiles {
			if len(unique) == 0 || unique[len(unique)-1] != profile {
				unique = append(unique, profile)
			}
		}
		profilesByPair[key] = unique
	}
	s.profilePairs = profilesByPair
	s.hasActiveProfileUsers = hasUsers || len(workloads) > 0
	s.profileCacheReady = true
	return nil
}

// RefreshProfileRouting updates the WebSocket routing cache without running
// REST candle reconciliation. The app calls it during its minute-level
// subscription refresh so RSI/EMA changes do not wait for the slower scan timer.
func (s *Scanner) RefreshProfileRouting(workloads []storage.ScannerWorkload, hasUsers bool) error {
	if s == nil {
		return errors.New("scanner is nil")
	}
	if s.profileRepository == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refreshProfileCacheLocked(workloads, hasUsers)
}

func (s *Scanner) scanLegacyOnceLocked(ctx context.Context) (Report, error) {
	instruments, err := s.repository.ListActiveInstruments(ctx, s.settings.InstrumentLimit)
	if err != nil {
		return Report{}, fmt.Errorf("list active Bybit instruments: %w", err)
	}
	if len(instruments) == 0 {
		return Report{}, errors.New("there are no active Bybit instruments; market catalogue has not been synchronized")
	}
	serverTime, err := s.source.GetServerTime(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("get Bybit exchange time for scanner cycle: %w", err)
	}
	if serverTime.IsZero() {
		return Report{}, errors.New("Bybit exchange returned an empty server time")
	}
	serverTime = serverTime.UTC()

	report := Report{Instruments: len(instruments)}
	var scanErrors []error
	for _, instrument := range instruments {
		symbol := strings.ToUpper(strings.TrimSpace(instrument.Symbol))
		if symbol == "" {
			scanErrors = append(scanErrors, errors.New("active instrument has an empty symbol"))
			continue
		}
		for _, timeframe := range s.settings.Timeframes {
			pairReport, err := s.scanPairLocked(ctx, symbol, timeframe, serverTime)
			if err != nil {
				scanErrors = append(scanErrors, fmt.Errorf("%s %s: %w", symbol, timeframe, err))
				continue
			}
			report.PairsScanned++
			report.CandlesProcessed += pairReport.CandlesProcessed
			report.SignalsInserted += pairReport.SignalsInserted
			report.DeliveriesQueued += pairReport.DeliveriesQueued
		}
	}
	return report, errors.Join(scanErrors...)
}

type profilePairWork struct {
	symbol    string
	timeframe string
	profiles  map[storage.ScannerProfile]struct{}
}

func (s *Scanner) scanProfileWorkloadsLocked(ctx context.Context, workloads []storage.ScannerWorkload) (Report, error) {
	pairs := make(map[string]*profilePairWork)
	instruments := make(map[string]struct{})
	for _, workload := range workloads {
		if _, err := timeframeDuration(workload.Timeframe); err != nil {
			return Report{}, err
		}
		profile := storage.ScannerProfile{RSILength: workload.RSILength, EMALength: workload.EMALength}
		if profile.RSILength < 2 || profile.RSILength > 500 ||
			profile.EMALength < 1 || profile.EMALength > 500 ||
			profile.RSILength+profile.EMALength > 1000 {
			return Report{}, fmt.Errorf("invalid active RSI/EMA profile: RSI=%d EMA=%d", profile.RSILength, profile.EMALength)
		}
		for _, rawSymbol := range workload.Symbols {
			symbol := strings.ToUpper(strings.TrimSpace(rawSymbol))
			if symbol == "" {
				return Report{}, errors.New("personalized scanner workload contains an empty symbol")
			}
			key := historyKey(symbol, workload.Timeframe)
			pair := pairs[key]
			if pair == nil {
				pair = &profilePairWork{
					symbol: symbol, timeframe: workload.Timeframe,
					profiles: make(map[storage.ScannerProfile]struct{}),
				}
				pairs[key] = pair
			}
			pair.profiles[profile] = struct{}{}
			instruments[symbol] = struct{}{}
		}
	}
	if len(pairs) == 0 {
		return Report{}, nil
	}

	serverTime, err := s.source.GetServerTime(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("get Bybit exchange time for personalized scan: %w", err)
	}
	if serverTime.IsZero() {
		return Report{}, errors.New("Bybit exchange returned an empty server time")
	}
	serverTime = serverTime.UTC()

	keys := make([]string, 0, len(pairs))
	for key := range pairs {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	report := Report{Instruments: len(instruments)}
	var scanErrors []error
	for _, key := range keys {
		pair := pairs[key]
		maxHistory := s.settings.HistoryCandles
		profiles := make([]storage.ScannerProfile, 0, len(pair.profiles))
		for profile := range pair.profiles {
			profiles = append(profiles, profile)
			if required := profile.RSILength + profile.EMALength; required > maxHistory {
				maxHistory = required
			}
		}
		sort.Slice(profiles, func(i, j int) bool {
			if profiles[i].RSILength == profiles[j].RSILength {
				return profiles[i].EMALength < profiles[j].EMALength
			}
			return profiles[i].RSILength < profiles[j].RSILength
		})

		candles, err := s.source.GetKlinesAt(ctx, pair.symbol, pair.timeframe, maxHistory, serverTime)
		if err != nil {
			scanErrors = append(scanErrors, fmt.Errorf("%s %s: load closed candles: %w", pair.symbol, pair.timeframe, err))
			continue
		}
		closed, err := normalizeCandles(candles, pair.symbol, pair.timeframe, serverTime)
		if err != nil {
			scanErrors = append(scanErrors, fmt.Errorf("%s %s: %w", pair.symbol, pair.timeframe, err))
			continue
		}
		if len(closed) > maxHistory {
			closed = closed[len(closed)-maxHistory:]
		}
		s.history[key] = closed
		report.PairsScanned++

		for _, profile := range profiles {
			pairReport, err := s.processProfilePairCandlesLocked(ctx, pair.symbol, pair.timeframe, profile, closed)
			if err != nil {
				scanErrors = append(scanErrors, fmt.Errorf("%s %s RSI(%d)/EMA(%d): %w",
					pair.symbol, pair.timeframe, profile.RSILength, profile.EMALength, err))
				continue
			}
			report.CandlesProcessed += pairReport.CandlesProcessed
			report.SignalsInserted += pairReport.SignalsInserted
			report.DeliveriesQueued += pairReport.DeliveriesQueued
		}
	}
	return report, errors.Join(scanErrors...)
}

// ProcessClosedCandle consumes a confirmed WebSocket candle. The in-memory
// history is populated by ScanOnce; on an unexpected gap it backfills that
// pair from REST before calculating indicators.
func (s *Scanner) ProcessClosedCandle(ctx context.Context, candle bybit.Candle) (Report, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.profileRepository != nil {
		if !s.profileCacheReady {
			return Report{}, errors.New("personalized scanner profile cache is not initialized; run ScanOnce before processing WebSocket candles")
		}
		symbol := strings.ToUpper(strings.TrimSpace(candle.Symbol))
		if symbol == "" {
			return Report{}, errors.New("WebSocket candle symbol is required")
		}
		if _, err := timeframeDuration(candle.Timeframe); err != nil {
			return Report{}, err
		}
		profiles := s.profilePairs[historyKey(symbol, candle.Timeframe)]
		if len(profiles) > 0 {
			return s.processProfileClosedCandleLocked(ctx, candle, profiles)
		}
		if s.hasActiveProfileUsers {
			return Report{PairsScanned: 1}, nil
		}
	}
	return s.processLegacyClosedCandleLocked(ctx, candle)
}

func (s *Scanner) processLegacyClosedCandleLocked(ctx context.Context, candle bybit.Candle) (Report, error) {
	symbol := strings.ToUpper(strings.TrimSpace(candle.Symbol))
	if symbol == "" {
		return Report{}, errors.New("WebSocket candle symbol is required")
	}
	if _, err := timeframeDuration(candle.Timeframe); err != nil {
		return Report{}, err
	}
	closed, err := normalizeCandles([]bybit.Candle{candle}, symbol, candle.Timeframe, candle.Close)
	if err != nil {
		return Report{}, fmt.Errorf("validate WebSocket candle: %w", err)
	}
	if len(closed) != 1 {
		return Report{}, errors.New("WebSocket confirmed candle was not closed")
	}
	candle = closed[0]

	key := historyKey(symbol, candle.Timeframe)
	history := s.history[key]
	if len(history) > 0 && !candle.Start.After(history[len(history)-1].Start) {
		return Report{PairsScanned: 1}, nil
	}

	needsBackfill := len(history) < s.settings.RSILength+s.settings.EMALength
	if len(history) > 0 {
		interval, _ := timeframeDuration(candle.Timeframe)
		needsBackfill = needsBackfill || candle.Start.After(history[len(history)-1].Start.Add(interval))
	}
	if needsBackfill {
		serverTime, err := s.source.GetServerTime(ctx)
		if err != nil {
			return Report{}, fmt.Errorf("get exchange time for WebSocket gap recovery: %w", err)
		}
		recovered, err := s.source.GetKlinesAt(ctx, symbol, candle.Timeframe, s.settings.HistoryCandles, serverTime)
		if err != nil {
			return Report{}, fmt.Errorf("backfill %s %s after WebSocket gap: %w", symbol, candle.Timeframe, err)
		}
		normalized, err := normalizeCandles(recovered, symbol, candle.Timeframe, serverTime)
		if err != nil {
			return Report{}, fmt.Errorf("normalize REST recovery candles: %w", err)
		}
		history = mergeCandles(normalized, []bybit.Candle{candle})
	} else {
		history = mergeCandles(history, []bybit.Candle{candle})
	}

	if len(history) > s.settings.HistoryCandles {
		history = history[len(history)-s.settings.HistoryCandles:]
	}
	pairReport, err := s.processPairCandlesLocked(ctx, symbol, candle.Timeframe, history)
	report := Report{PairsScanned: 1}
	report.CandlesProcessed = pairReport.CandlesProcessed
	report.SignalsInserted = pairReport.SignalsInserted
	report.DeliveriesQueued = pairReport.DeliveriesQueued
	return report, err
}

func (s *Scanner) processProfileClosedCandleLocked(
	ctx context.Context, candle bybit.Candle, profiles []storage.ScannerProfile,
) (Report, error) {
	symbol := strings.ToUpper(strings.TrimSpace(candle.Symbol))
	if symbol == "" {
		return Report{}, errors.New("WebSocket candle symbol is required")
	}
	if _, err := timeframeDuration(candle.Timeframe); err != nil {
		return Report{}, err
	}
	closed, err := normalizeCandles([]bybit.Candle{candle}, symbol, candle.Timeframe, candle.Close)
	if err != nil {
		return Report{}, fmt.Errorf("validate personalized WebSocket candle: %w", err)
	}
	if len(closed) != 1 {
		return Report{}, errors.New("WebSocket confirmed candle was not closed")
	}
	candle = closed[0]

	maxHistory := s.settings.HistoryCandles
	for _, profile := range profiles {
		if profile.RSILength < 2 || profile.RSILength > 500 ||
			profile.EMALength < 1 || profile.EMALength > 500 ||
			profile.RSILength+profile.EMALength > 1000 {
			return Report{}, fmt.Errorf("invalid active RSI/EMA profile: RSI=%d EMA=%d", profile.RSILength, profile.EMALength)
		}
		if required := profile.RSILength + profile.EMALength; required > maxHistory {
			maxHistory = required
		}
	}
	key := historyKey(symbol, candle.Timeframe)
	history := s.history[key]
	needsBackfill := len(history) < maxHistory
	if len(history) > 0 {
		interval, _ := timeframeDuration(candle.Timeframe)
		if candle.Start.After(history[len(history)-1].Start.Add(interval)) {
			needsBackfill = true
		}
	}
	if needsBackfill {
		serverTime, err := s.source.GetServerTime(ctx)
		if err != nil {
			return Report{}, fmt.Errorf("get exchange time for personalized WebSocket recovery: %w", err)
		}
		recovered, err := s.source.GetKlinesAt(ctx, symbol, candle.Timeframe, maxHistory, serverTime)
		if err != nil {
			return Report{}, fmt.Errorf("backfill %s %s for personalized profiles: %w", symbol, candle.Timeframe, err)
		}
		normalized, err := normalizeCandles(recovered, symbol, candle.Timeframe, serverTime)
		if err != nil {
			return Report{}, fmt.Errorf("normalize personalized REST recovery: %w", err)
		}
		history = mergeCandles(normalized, []bybit.Candle{candle})
	} else {
		history = mergeCandles(history, []bybit.Candle{candle})
	}
	if len(history) > maxHistory {
		history = history[len(history)-maxHistory:]
	}
	s.history[key] = history

	report := Report{PairsScanned: 1}
	var scanErrors []error
	seen := make(map[storage.ScannerProfile]struct{}, len(profiles))
	for _, profile := range profiles {
		if _, exists := seen[profile]; exists {
			continue
		}
		seen[profile] = struct{}{}
		pairReport, err := s.processProfilePairCandlesLocked(ctx, symbol, candle.Timeframe, profile, history)
		if err != nil {
			scanErrors = append(scanErrors, fmt.Errorf("RSI(%d)/EMA(%d): %w", profile.RSILength, profile.EMALength, err))
			continue
		}
		report.CandlesProcessed += pairReport.CandlesProcessed
		report.SignalsInserted += pairReport.SignalsInserted
		report.DeliveriesQueued += pairReport.DeliveriesQueued
	}
	return report, errors.Join(scanErrors...)
}

func (s *Scanner) processProfilePairCandlesLocked(
	ctx context.Context, symbol, timeframe string, profile storage.ScannerProfile, closed []bybit.Candle,
) (pairReport, error) {
	if len(closed) == 0 {
		return pairReport{}, nil
	}
	closes := make([]float64, len(closed))
	for index, candle := range closed {
		closes[index] = candle.ClosePrice
	}
	points, err := indicators.Calculate(closes, profile.RSILength, profile.EMALength)
	if err != nil {
		return pairReport{}, fmt.Errorf("calculate RSI/EMA RSI: %w", err)
	}

	state, err := s.profileRepository.GetScannerProfileState(
		ctx, symbol, timeframe, profile.RSILength, profile.EMALength,
	)
	if errors.Is(err, storage.ErrNotFound) {
		last := len(closed) - 1
		if !points[last].RSIReady || !points[last].EMAReady {
			return pairReport{}, fmt.Errorf("not enough closed candles to initialize RSI(%d)/EMA(%d)",
				profile.RSILength, profile.EMALength)
		}
		baseline := storage.ScannerProfileState{
			Symbol: symbol, Timeframe: timeframe,
			RSILength: profile.RSILength, EMALength: profile.EMALength,
			LastProcessedCandleStart: closed[last].Start,
			LastClose:                closed[last].ClosePrice,
			LastRSI:                  points[last].RSI,
			LastEMARSI:               points[last].EMARSI,
			Initialized:              true,
		}
		if _, _, err := s.profileRepository.InitializeScannerProfileState(ctx, baseline); err != nil {
			return pairReport{}, fmt.Errorf("initialize profile baseline: %w", err)
		}
		return pairReport{}, nil
	}
	if err != nil {
		return pairReport{}, fmt.Errorf("load profile cursor: %w", err)
	}

	var report pairReport
	for index, current := range closed {
		if !current.Start.After(state.LastProcessedCandleStart) {
			continue
		}
		point := points[index]
		if !point.RSIReady || !point.EMAReady {
			continue
		}

		next := state
		next.LastProcessedCandleStart = current.Start
		next.LastClose = current.ClosePrice
		next.LastRSI = point.RSI
		next.LastEMARSI = point.EMARSI
		next.Initialized = true

		direction := ""
		if index > 0 && contiguous(closed[index-1], current, timeframe) {
			previous := points[index-1]
			if previous.RSIReady && previous.EMAReady {
				switch {
				case previous.RSI <= previous.EMARSI && point.RSI > point.EMARSI:
					direction = "LONG"
				case previous.RSI >= previous.EMARSI && point.RSI < point.EMARSI:
					direction = "SHORT"
				}
			}
		}
		var signal *storage.Signal
		if direction != "" {
			crossAt := current.Close.UTC()
			next.LastCrossDirection = direction
			next.LastCrossAt = &crossAt
			signal = &storage.Signal{
				Symbol: symbol, Timeframe: timeframe, Direction: direction,
				CandleStart: current.Start, CandleClose: current.Close,
				ClosePrice: current.ClosePrice, RSI: point.RSI, EMARSI: point.EMARSI,
				RSILength: profile.RSILength, EMALength: profile.EMALength,
				DetectedAt: s.now().UTC(),
			}
		}
		signalID, inserted, queued, err := s.profileRepository.CommitScannerProfileCandle(ctx, next, signal)
		if errors.Is(err, storage.ErrStaleCandle) {
			state, err = s.profileRepository.GetScannerProfileState(
				ctx, symbol, timeframe, profile.RSILength, profile.EMALength,
			)
			if err != nil {
				return report, fmt.Errorf("reload profile cursor after competing update: %w", err)
			}
			continue
		}
		if err != nil {
			return report, fmt.Errorf("commit profile candle %s (signal %d): %w",
				current.Start.Format(time.RFC3339), signalID, err)
		}
		state = next
		report.CandlesProcessed++
		if inserted {
			report.SignalsInserted++
			report.DeliveriesQueued += queued
		}
	}
	return report, nil
}

type pairReport struct {
	CandlesProcessed int
	SignalsInserted  int
	DeliveriesQueued int64
}

func (s *Scanner) scanPairLocked(ctx context.Context, symbol, timeframe string, serverTime time.Time) (pairReport, error) {
	candles, err := s.source.GetKlinesAt(ctx, symbol, timeframe, s.settings.HistoryCandles, serverTime)
	if err != nil {
		return pairReport{}, fmt.Errorf("load closed candles: %w", err)
	}
	closed, err := normalizeCandles(candles, symbol, timeframe, serverTime)
	if err != nil {
		return pairReport{}, err
	}
	return s.processPairCandlesLocked(ctx, symbol, timeframe, closed)
}

func (s *Scanner) processPairCandlesLocked(ctx context.Context, symbol, timeframe string, closed []bybit.Candle) (pairReport, error) {
	if len(closed) == 0 {
		return pairReport{}, nil
	}
	closed = append([]bybit.Candle(nil), closed...)
	if len(closed) > s.settings.HistoryCandles {
		closed = closed[len(closed)-s.settings.HistoryCandles:]
	}
	s.history[historyKey(symbol, timeframe)] = closed

	closes := make([]float64, len(closed))
	for index, candle := range closed {
		closes[index] = candle.ClosePrice
	}
	points, err := indicators.Calculate(closes, s.settings.RSILength, s.settings.EMALength)
	if err != nil {
		return pairReport{}, fmt.Errorf("calculate RSI/EMA RSI: %w", err)
	}

	state, err := s.repository.GetScannerState(ctx, symbol, timeframe)
	if errors.Is(err, storage.ErrNotFound) {
		last := len(closed) - 1
		if !points[last].RSIReady || !points[last].EMAReady {
			return pairReport{}, fmt.Errorf("not enough closed candles to initialize RSI(%d)/EMA(%d)", s.settings.RSILength, s.settings.EMALength)
		}
		baseline := storage.ScannerState{
			Symbol:                   symbol,
			Timeframe:                timeframe,
			LastProcessedCandleStart: closed[last].Start,
			LastClose:                closed[last].ClosePrice,
			LastRSI:                  points[last].RSI,
			LastEMARSI:               points[last].EMARSI,
			Initialized:              true,
		}
		state, _, err = s.repository.InitializeScannerState(ctx, baseline)
		if err != nil {
			return pairReport{}, fmt.Errorf("initialize scanner baseline: %w", err)
		}
		return pairReport{}, nil
	}
	if err != nil {
		return pairReport{}, fmt.Errorf("load scanner cursor: %w", err)
	}

	var report pairReport
	for index, candle := range closed {
		if !candle.Start.After(state.LastProcessedCandleStart) {
			continue
		}
		point := points[index]
		if !point.RSIReady || !point.EMAReady {
			// If downtime exceeded the retained history window, older candles no
			// longer have enough warm-up data. Skip them and resume at the first
			// fully calculated point without generating a false crossover.
			continue
		}

		next := state
		next.LastProcessedCandleStart = candle.Start
		next.LastClose = candle.ClosePrice
		next.LastRSI = point.RSI
		next.LastEMARSI = point.EMARSI
		next.Initialized = true

		direction := ""
		if index > 0 && contiguous(closed[index-1], candle, timeframe) {
			previous := points[index-1]
			if previous.RSIReady && previous.EMAReady {
				switch {
				case previous.RSI <= previous.EMARSI && point.RSI > point.EMARSI:
					direction = "LONG"
				case previous.RSI >= previous.EMARSI && point.RSI < point.EMARSI:
					direction = "SHORT"
				}
			}
		}

		var signal *storage.Signal
		if direction != "" {
			crossAt := candle.Close.UTC()
			next.LastCrossDirection = direction
			next.LastCrossAt = &crossAt
			signal = &storage.Signal{
				Symbol:      symbol,
				RSILength:   s.settings.RSILength,
				EMALength:   s.settings.EMALength,
				Timeframe:   timeframe,
				Direction:   direction,
				CandleStart: candle.Start,
				CandleClose: candle.Close,
				ClosePrice:  candle.ClosePrice,
				RSI:         point.RSI,
				EMARSI:      point.EMARSI,
				DetectedAt:  s.now().UTC(),
			}
		}

		signalID, inserted, queued, err := s.repository.CommitScannedCandle(ctx, next, signal)
		if errors.Is(err, storage.ErrStaleCandle) {
			state, err = s.repository.GetScannerState(ctx, symbol, timeframe)
			if err != nil {
				return report, fmt.Errorf("reload scanner cursor after competing update: %w", err)
			}
			continue
		}
		if err != nil {
			return report, fmt.Errorf("commit candle %s (signal %d): %w", candle.Start.Format(time.RFC3339), signalID, err)
		}
		state = next
		report.CandlesProcessed++
		if inserted {
			report.SignalsInserted++
			report.DeliveriesQueued += queued
		}
	}
	return report, nil
}

func mergeCandles(first, second []bybit.Candle) []bybit.Candle {
	byStart := make(map[int64]bybit.Candle, len(first)+len(second))
	for _, candle := range first {
		byStart[candle.Start.UnixMilli()] = candle
	}
	for _, candle := range second {
		byStart[candle.Start.UnixMilli()] = candle
	}
	result := make([]bybit.Candle, 0, len(byStart))
	for _, candle := range byStart {
		result = append(result, candle)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Start.Before(result[j].Start) })
	return result
}

func normalizeCandles(candles []bybit.Candle, symbol, timeframe string, serverTime time.Time) ([]bybit.Candle, error) {
	interval, err := timeframeDuration(timeframe)
	if err != nil {
		return nil, err
	}
	closed := make([]bybit.Candle, 0, len(candles))
	for _, candle := range candles {
		if !strings.EqualFold(candle.Symbol, symbol) || candle.Timeframe != timeframe {
			return nil, fmt.Errorf("market source returned mismatched candle %s %s", candle.Symbol, candle.Timeframe)
		}
		if candle.Start.IsZero() || !candle.Close.After(candle.Start) ||
			!candle.Close.Equal(candle.Start.Add(interval)) {
			return nil, fmt.Errorf("market source returned invalid candle interval for %s", symbol)
		}
		if candle.Close.After(serverTime) {
			continue
		}
		if candle.ClosePrice <= 0 {
			return nil, fmt.Errorf("market source returned non-positive close for %s at %s", symbol, candle.Start)
		}
		closed = append(closed, candle)
	}
	sort.Slice(closed, func(i, j int) bool { return closed[i].Start.Before(closed[j].Start) })
	for index := 1; index < len(closed); index++ {
		if !closed[index].Start.After(closed[index-1].Start) {
			return nil, fmt.Errorf("market source returned duplicate candle timestamp for %s", symbol)
		}
	}
	return closed, nil
}

func contiguous(previous, current bybit.Candle, timeframe string) bool {
	interval, err := timeframeDuration(timeframe)
	return err == nil && current.Start.Equal(previous.Start.Add(interval))
}

func historyKey(symbol, timeframe string) string {
	return strings.ToUpper(symbol) + "/" + timeframe
}

func timeframeDuration(timeframe string) (time.Duration, error) {
	switch timeframe {
	case "5":
		return 5 * time.Minute, nil
	case "15":
		return 15 * time.Minute, nil
	case "30":
		return 30 * time.Minute, nil
	case "60":
		return time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported scanner timeframe %q", timeframe)
	}
}
