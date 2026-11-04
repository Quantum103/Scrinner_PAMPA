package scanner

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
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
	source     MarketDataSource
	repository Repository
	settings   Settings
	now        func() time.Time
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
	return &Scanner{source: source, repository: repository, settings: settings, now: time.Now}, nil
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

// ScanOnce processes the configured top instruments using one exchange-time
// snapshot. Every pair has its own durable cursor in PostgreSQL.
func (s *Scanner) ScanOnce(ctx context.Context) (Report, error) {
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
			pairReport, err := s.scanPair(ctx, symbol, timeframe, serverTime)
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

type pairReport struct {
	CandlesProcessed int
	SignalsInserted  int
	DeliveriesQueued int64
}

func (s *Scanner) scanPair(ctx context.Context, symbol, timeframe string, serverTime time.Time) (pairReport, error) {
	candles, err := s.source.GetKlinesAt(ctx, symbol, timeframe, s.settings.HistoryCandles, serverTime)
	if err != nil {
		return pairReport{}, fmt.Errorf("load closed candles: %w", err)
	}
	closed, err := normalizeCandles(candles, symbol, timeframe, serverTime)
	if err != nil {
		return pairReport{}, err
	}
	if len(closed) == 0 {
		return pairReport{}, nil
	}

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
			return report, fmt.Errorf("indicator warm-up is incomplete at candle %s", candle.Start.Format(time.RFC3339))
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

func timeframeDuration(timeframe string) (time.Duration, error) {
	switch timeframe {
	case "5":
		return 5 * time.Minute, nil
	case "15":
		return 15 * time.Minute, nil
	case "60":
		return time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported scanner timeframe %q", timeframe)
	}
}
