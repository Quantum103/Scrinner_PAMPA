package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	"github.com/Quantum103/Scrinner_PAMPA/internal/config"
	"github.com/Quantum103/Scrinner_PAMPA/internal/market"
	"github.com/Quantum103/Scrinner_PAMPA/internal/migrations"
	"github.com/Quantum103/Scrinner_PAMPA/internal/momentum"
	"github.com/Quantum103/Scrinner_PAMPA/internal/scanner"
	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
	"github.com/Quantum103/Scrinner_PAMPA/internal/telegrambot"
	"github.com/jackc/pgx/v5/pgxpool"
)

type App struct {
	pool             *pgxpool.Pool
	storage          *storage.Repository
	server           *http.Server
	logger           *slog.Logger
	config           config.Config
	marketSync       *market.Synchronizer
	signalScanner    *scanner.Scanner
	publicStream     *bybit.PublicStream
	spotRESTClient   *bybit.Client
	spotSynchronizer *market.SpotSynchronizer
	spotPublicStream *bybit.PublicSpotStream
	momentumTracker  *momentum.Tracker
	telegramBot      *telegrambot.Bot
	spotAlertEvents  chan storage.SpotMomentumAlert
}

func New(ctx context.Context, cfg config.Config, logger *slog.Logger) (*App, error) {
	if logger == nil {
		logger = slog.Default()
	}
	poolConfig, err := pgxpool.ParseConfig(cfg.Database.URL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	poolConfig.MaxConns = cfg.Database.MaxConns
	poolConfig.MinConns = cfg.Database.MinConns
	poolConfig.MaxConnIdleTime = 5 * time.Minute
	poolConfig.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	if err := migrations.Up(ctx, pool, cfg.App.MigrationsDir); err != nil {
		pool.Close()
		return nil, fmt.Errorf("run database migrations: %w", err)
	}
	repository, err := storage.New(pool)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("initialize PostgreSQL repository: %w", err)
	}
	restClient, err := bybit.NewClient(cfg.Bybit.RESTURL, nil)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("initialize Bybit REST client: %w", err)
	}
	publicStream, err := bybit.NewPublicStream(cfg.Bybit.PublicWSURL)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("initialize Bybit public WebSocket: %w", err)
	}
	marketSync, err := market.NewSynchronizer(restClient, repository)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("initialize Bybit market synchronizer: %w", err)
	}
	signalScanner, err := scanner.New(restClient, repository, scanner.Settings{
		Timeframes:            cfg.Scanner.Timeframes,
		RSILength:             cfg.Scanner.RSILength,
		EMALength:             cfg.Scanner.EMALength,
		InstrumentLimit:       cfg.Scanner.InstrumentLimit,
		HistoryCandles:        cfg.Scanner.HistoryCandles,
		MarketADXSidewaysMax:  cfg.Scanner.MarketADXSidewaysMax,
		MarketCHOPSidewaysMin: cfg.Scanner.MarketCHOPSidewaysMin,
		MarketADXTrendMin:     cfg.Scanner.MarketADXTrendMin,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("initialize RSI scanner: %w", err)
	}

	spotSynchronizer, err := market.NewSpotSynchronizer(restClient, repository)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("initialize Bybit Spot market synchronizer: %w", err)
	}
	spotPublicStream, err := bybit.NewPublicSpotStream(cfg.Bybit.SpotPublicWSURL)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("initialize Bybit Spot public WebSocket: %w", err)
	}
	momentumTracker, err := momentum.NewTracker(momentum.DefaultWindow, momentum.DefaultThresholdPercent, momentum.DefaultMaxReferenceAge)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("initialize Spot momentum tracker: %w", err)
	}

	var telegramBotInstance *telegrambot.Bot
	switch {
	case cfg.Telegram.BotToken == "":
		if cfg.Telegram.AccessPassword != "" {
			logger.Warn("Telegram bot is disabled because TELEGRAM_BOT_TOKEN is empty")
		}
	case cfg.Telegram.AccessPassword == "":
		logger.Warn("Telegram bot is disabled because TELEGRAM_ACCESS_PASSWORD is empty")
	default:
		telegramBotInstance, err = telegrambot.New(
			cfg.Telegram.BotToken, cfg.Telegram.AccessPassword, repository, logger,
		)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("initialize Telegram bot: %w", err)
		}
	}

	server := &http.Server{
		Addr:              cfg.App.HTTPAddr,
		Handler:           healthHandler(pool.Ping),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return &App{
		pool: pool, storage: repository, server: server, logger: logger,
		config: cfg, marketSync: marketSync, signalScanner: signalScanner,
		publicStream: publicStream, spotRESTClient: restClient,
		spotSynchronizer: spotSynchronizer, spotPublicStream: spotPublicStream,
		momentumTracker: momentumTracker, telegramBot: telegramBotInstance,
		spotAlertEvents: make(chan storage.SpotMomentumAlert, 1000),
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go a.runSpotMonitor(runCtx)
	go a.runSpotAlertPersistence(runCtx)
	if a.telegramBot != nil {
		go func() {
			if err := a.telegramBot.Run(runCtx); err != nil && runCtx.Err() == nil {
				a.logger.Error("Telegram bot stopped unexpectedly", "error", err)
			}
		}()
	}
	marketReady := make(chan struct{})
	marketChanged := make(chan struct{}, 1)
	go a.runMarketSync(runCtx, marketReady, marketChanged)
	go func() {
		select {
		case <-marketReady:
			go a.runScanner(runCtx)
			a.runMarketData(runCtx, marketChanged)
		case <-runCtx.Done():
			return
		}
	}()

	serverErrors := make(chan error, 1)
	go func() {
		a.logger.Info("HTTP health server started", "address", a.config.App.HTTPAddr)
		err := a.server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			serverErrors <- nil
			return
		}
		serverErrors <- err
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), a.config.App.ShutdownTimeout)
		defer shutdownCancel()
		if err := a.server.Shutdown(shutdownCtx); err != nil {
			_ = a.server.Close()
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		return nil
	case err := <-serverErrors:
		if err != nil {
			return fmt.Errorf("HTTP server: %w", err)
		}
		return nil
	}
}

const spotSubscriptionShardSize = 100
const spotHistoryCandleLimit = 20
const spotHistoryWorkers = 8

func (a *App) runSpotMonitor(ctx context.Context) {
	ticker := time.NewTicker(a.config.Spot.MarketRefreshInterval)
	defer ticker.Stop()

	seedResults := make(chan []string, 1)
	seedInFlight := make(map[string]struct{})
	activeSymbols := make([]string, 0)
	var stopStreams func()

	refresh := func() {
		syncCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		stats, err := a.spotSynchronizer.Sync(syncCtx)
		cancel()
		if err != nil {
			a.logger.Error("Bybit Spot market catalogue refresh failed", "error", err)
			return
		}

		watchlist, err := a.storage.ListSpotWatchlist(ctx)
		if err != nil {
			a.logger.Error("load Bybit Spot Watchlist failed", "error", err)
			return
		}
		symbols := make([]string, 0, len(watchlist))
		for _, item := range watchlist {
			symbols = append(symbols, strings.ToUpper(strings.TrimSpace(item.Symbol)))
		}
		sort.Strings(symbols)

		if !sameSpotSymbols(activeSymbols, symbols) {
			if stopStreams != nil {
				stopStreams()
				stopStreams = nil
			}
			a.momentumTracker.KeepSymbols(symbols)
			activeSymbols = append(activeSymbols[:0], symbols...)
			if len(symbols) > 0 {
				stopStreams = a.startSpotStreams(ctx, symbols)
			}
			a.logger.Info("Bybit Spot ticker subscriptions reconciled",
				"watchlist_symbols", len(symbols),
				"connections", (len(symbols)+spotSubscriptionShardSize-1)/spotSubscriptionShardSize)
		}

		toSeed := make([]string, 0)
		for _, symbol := range symbols {
			if !a.momentumTracker.HasHistory(symbol) {
				if _, inFlight := seedInFlight[symbol]; !inFlight {
					seedInFlight[symbol] = struct{}{}
					toSeed = append(toSeed, symbol)
				}
			}
		}
		if len(toSeed) > 0 {
			seedBatch := append([]string(nil), toSeed...)
			go func() {
				a.seedSpotHistory(ctx, seedBatch)
				select {
				case seedResults <- seedBatch:
				case <-ctx.Done():
				}
			}()
		}

		a.logger.Info("Bybit Spot market snapshot refreshed",
			"instruments", stats.Instruments,
			"watchlist", stats.Watchlist,
			"over_limit_hold", stats.OverLimitHold,
			"history_seed_pending", len(seedInFlight))
	}

	refresh()
	for {
		select {
		case <-ctx.Done():
			if stopStreams != nil {
				stopStreams()
			}
			return
		case <-ticker.C:
			refresh()
		case completed := <-seedResults:
			for _, symbol := range completed {
				delete(seedInFlight, symbol)
			}
		}
	}
}

// runSpotAlertPersistence stores each crossing and atomically creates deliveries
// for all active Telegram subscribers. Failed database writes are retried.
func (a *App) runSpotAlertPersistence(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case alert := <-a.spotAlertEvents:
			retryDelay := time.Second
			for attempt := 1; ; attempt++ {
				saveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				alertID, inserted, queued, err := a.storage.SaveSpotMomentumAlert(saveCtx, alert)
				cancel()
				if err == nil {
					if inserted {
						a.logger.Info("Spot momentum alert persisted",
							"alert_id", alertID, "symbol", alert.Symbol,
							"change_percent", alert.ChangePercent, "deliveries_queued", queued)
					}
					break
				}
				if ctx.Err() != nil {
					return
				}
				a.logger.Error("persist Spot momentum alert failed; retry scheduled",
					"symbol", alert.Symbol, "attempt", attempt, "error", err)
				if !waitForSpotPersistenceRetry(ctx, retryDelay) {
					return
				}
				retryDelay *= 2
				if retryDelay > 30*time.Second {
					retryDelay = 30 * time.Second
				}
			}
		}
	}
}

func waitForSpotPersistenceRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (a *App) startSpotStreams(parent context.Context, symbols []string) func() {
	streamCtx, cancel := context.WithCancel(parent)
	var wait sync.WaitGroup
	for start := 0; start < len(symbols); start += spotSubscriptionShardSize {
		end := start + spotSubscriptionShardSize
		if end > len(symbols) {
			end = len(symbols)
		}
		shard := append([]string(nil), symbols[start:end]...)
		wait.Add(1)
		go func() {
			defer wait.Done()
			err := a.spotPublicStream.Run(streamCtx, shard, bybit.SpotStreamHandlers{
				OnTicker: func(_ context.Context, update bybit.SpotPriceUpdate) error {
					signal, err := a.momentumTracker.Add(update.Symbol, momentum.PricePoint{
						At: update.At, Price: update.Price,
					})
					if err != nil {
						return err
					}
					if signal != nil {
						alert := storage.SpotMomentumAlert{
							Symbol: signal.Symbol, DetectedAt: signal.At, ReferenceAt: signal.ReferenceAt,
							Price: signal.Price, ReferencePrice: signal.ReferencePrice,
							ChangePercent: signal.ChangePercent,
							WindowSeconds: int(momentum.DefaultWindow / time.Second),
						}
						select {
						case a.spotAlertEvents <- alert:
						case <-streamCtx.Done():
							return streamCtx.Err()
						}
					}
					return nil
				},
				OnDisconnect: func(disconnectErr error) {
					a.logger.Warn("Bybit Spot WebSocket disconnected; reconnect scheduled",
						"symbols", len(shard), "error", disconnectErr)
				},
			})
			if err != nil && streamCtx.Err() == nil {
				a.logger.Error("Bybit Spot WebSocket stream stopped", "symbols", len(shard), "error", err)
			}
		}()
	}
	return func() {
		cancel()
		wait.Wait()
	}
}

func (a *App) seedSpotHistory(ctx context.Context, symbols []string) {
	if len(symbols) == 0 {
		return
	}
	serverTimeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	serverTime, err := a.spotRESTClient.GetServerTime(serverTimeCtx)
	cancel()
	if err != nil {
		a.logger.Warn("Spot history seed could not get Bybit server time", "symbols", len(symbols), "error", err)
		return
	}

	jobs := make(chan string)
	workerCount := spotHistoryWorkers
	if len(symbols) < workerCount {
		workerCount = len(symbols)
	}
	var wait sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for symbol := range jobs {
				requestCtx, requestCancel := context.WithTimeout(ctx, 3*time.Minute)
				candles, requestErr := a.spotRESTClient.GetSpotKlinesAt(requestCtx, symbol, spotHistoryCandleLimit, serverTime)
				requestCancel()
				if requestErr != nil {
					if ctx.Err() == nil {
						a.logger.Warn("Spot history seed failed", "symbol", symbol, "error", requestErr)
					}
					continue
				}
				points := make([]momentum.PricePoint, 0, len(candles))
				for _, candle := range candles {
					points = append(points, momentum.PricePoint{At: candle.Close, Price: candle.ClosePrice})
				}
				if len(points) == 0 {
					a.logger.Warn("Spot history seed returned no closed candles", "symbol", symbol)
					continue
				}
				if err := a.momentumTracker.Seed(symbol, points); err != nil {
					a.logger.Warn("Spot history seed was rejected", "symbol", symbol, "error", err)
					continue
				}
				a.logger.Debug("Spot momentum history seeded", "symbol", symbol, "candles", len(points))
			}
		}()
	}
	for _, symbol := range symbols {
		select {
		case <-ctx.Done():
			close(jobs)
			wait.Wait()
			return
		case jobs <- symbol:
		}
	}
	close(jobs)
	wait.Wait()
}

func sameSpotSymbols(first, second []string) bool {
	if len(first) != len(second) {
		return false
	}
	for index := range first {
		if first[index] != second[index] {
			return false
		}
	}
	return true
}

func (a *App) runMarketSync(ctx context.Context, initialSyncDone chan<- struct{}, marketChanged chan<- struct{}) {
	ticker := time.NewTicker(a.config.Scanner.RefreshInterval)
	defer ticker.Stop()

	refresh := func() {
		syncCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		stats, err := a.marketSync.Sync(syncCtx)
		if err != nil {
			a.logger.Error("Bybit market catalogue refresh failed", "error", err)
			return
		}
		a.logger.Info("Bybit market catalogue refreshed",
			"instruments", stats.Instruments,
			"with_ticker", stats.WithTicker,
		)
		select {
		case marketChanged <- struct{}{}:
		default:
		}
	}

	refresh()
	close(initialSyncDone)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}

func (a *App) runScanner(ctx context.Context) {
	ticker := time.NewTicker(a.config.Scanner.ScanInterval)
	defer ticker.Stop()

	scan := func() {
		// Allow enough time for REST rate limiting during periodic reconciliation.
		timeout := a.scannerTimeout(ctx)
		scanCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		report, err := a.signalScanner.ScanOnce(scanCtx)
		if err != nil {
			a.logger.Error("REST scanner reconciliation completed with errors",
				"error", err,
				"instruments", report.Instruments,
				"pairs_scanned", report.PairsScanned,
				"candles_processed", report.CandlesProcessed,
				"signals_inserted", report.SignalsInserted,
				"sideways_signals_filtered", report.SidewaysSignalsFiltered,
			)
			return
		}
		a.logger.Info("REST scanner reconciliation completed",
			"instruments", report.Instruments,
			"pairs_scanned", report.PairsScanned,
			"candles_processed", report.CandlesProcessed,
			"signals_inserted", report.SignalsInserted,
			"sideways_signals_filtered", report.SidewaysSignalsFiltered,
			"deliveries_queued", report.DeliveriesQueued,
		)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scan()
		}
	}
}

func (a *App) runMarketData(ctx context.Context, marketChanged <-chan struct{}) {
	// The initial catalogue refresh notification is already represented by the
	// first subscription load below. Drain only the stale startup notification.
	for {
		select {
		case <-marketChanged:
		default:
			goto start
		}
	}

start:
	subscriptionTicker := time.NewTicker(time.Minute)
	defer subscriptionTicker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return
		}
		subscriptions, err := a.loadMarketSubscriptions(ctx)
		if err != nil {
			a.logger.Error("load instruments for Bybit WebSocket subscriptions failed", "error", err)
			if !waitForMarketRetry(ctx, marketChanged) {
				return
			}
			continue
		}
		if len(subscriptions) == 0 {
			a.logger.Warn("Bybit WebSocket has no active kline subscriptions; waiting for settings refresh")
			// User settings can change without a market-catalogue notification. Check
			// again on the regular subscription ticker, but do not poll PostgreSQL
			// every five seconds while the user's selected universe is empty.
			if !waitForSubscriptionRefresh(ctx, marketChanged, subscriptionTicker.C) {
				return
			}
			continue
		}

		streamCtx, cancel := context.WithCancel(ctx)
		streamDone := make(chan error, 1)
		handlers := bybit.StreamHandlers{
			OnConnected: func(connectionCtx context.Context) error {
				reconcileCtx, reconcileCancel := context.WithTimeout(connectionCtx, a.scannerTimeout(connectionCtx))
				defer reconcileCancel()
				report, scanErr := a.signalScanner.ScanOnce(reconcileCtx)
				if scanErr != nil {
					a.logger.Error("REST backfill after Bybit WebSocket connection completed with partial errors",
						"error", scanErr, "pairs_scanned", report.PairsScanned,
						"candles_processed", report.CandlesProcessed,
						"signals_inserted", report.SignalsInserted,
						"sideways_signals_filtered", report.SidewaysSignalsFiltered)
					if report.PairsScanned == 0 {
						return scanErr
					}
				}
				a.logger.Info("REST backfill after Bybit WebSocket connection completed",
					"pairs_scanned", report.PairsScanned,
					"candles_processed", report.CandlesProcessed,
					"signals_inserted", report.SignalsInserted,
					"sideways_signals_filtered", report.SidewaysSignalsFiltered)
				return nil
			},
			OnCandle: func(candleCtx context.Context, candle bybit.Candle) error {
				report, processErr := a.signalScanner.ProcessClosedCandle(candleCtx, candle)
				if processErr != nil {
					a.logger.Error("confirmed Bybit WebSocket candle processing failed",
						"symbol", candle.Symbol, "timeframe", candle.Timeframe,
						"candle_start", candle.Start, "error", processErr)
					return processErr
				}
				if report.SignalsInserted > 0 || report.SidewaysSignalsFiltered > 0 {
					a.logger.Info("RSI crossover processed from Bybit WebSocket candle",
						"symbol", candle.Symbol, "timeframe", candle.Timeframe,
						"signals_inserted", report.SignalsInserted,
						"sideways_signals_filtered", report.SidewaysSignalsFiltered,
						"deliveries_queued", report.DeliveriesQueued)
				}
				return nil
			},
			OnDisconnect: func(disconnectErr error) {
				a.logger.Warn("Bybit public WebSocket disconnected; reconnect scheduled", "error", disconnectErr)
			},
		}
		a.logger.Info("starting Bybit public WebSocket",
			"subscriptions", len(subscriptions),
			"personalized_universe", true)
		go func() {
			streamDone <- a.publicStream.Run(streamCtx, subscriptions, handlers)
		}()

		restart := false
		for !restart {
			select {
			case <-ctx.Done():
				cancel()
				<-streamDone
				return
			case <-marketChanged:
				restart = true
				a.logger.Info("restarting Bybit WebSocket after market catalogue refresh")
			case streamErr := <-streamDone:
				cancel()
				if streamErr != nil {
					a.logger.Error("Bybit public WebSocket stopped", "error", streamErr)
				}
				if !waitForMarketRetry(ctx, marketChanged) {
					return
				}
				restart = true
			case <-subscriptionTicker.C:
				updated, loadErr := a.loadMarketSubscriptions(ctx)
				if loadErr != nil {
					a.logger.Error("refresh personalized Bybit WebSocket subscriptions failed", "error", loadErr)
					continue
				}
				if sameKlineSubscriptions(subscriptions, updated) {
					continue
				}
				a.logger.Info("restarting Bybit WebSocket because user scanner settings changed",
					"previous_subscriptions", len(subscriptions),
					"updated_subscriptions", len(updated))
				restart = true
			}
		}
		cancel()
		<-streamDone
	}
}

func (a *App) loadMarketSubscriptions(ctx context.Context) ([]bybit.KlineSubscription, error) {
	workloads, err := a.storage.ListScannerWorkloads(ctx)
	if err != nil {
		return nil, fmt.Errorf("list personalized scanner workloads: %w", err)
	}

	unique := make(map[string]bybit.KlineSubscription)
	for _, workload := range workloads {
		for _, symbol := range workload.Symbols {
			key := strings.ToUpper(strings.TrimSpace(symbol)) + "/" + workload.Timeframe
			if key == "/" {
				continue
			}
			unique[key] = bybit.KlineSubscription{
				Symbol:    strings.ToUpper(strings.TrimSpace(symbol)),
				Timeframe: workload.Timeframe,
			}
		}
	}

	// When workloads are present, at least one active user needs these
	// subscriptions. With no workloads, distinguish an empty active-user
	// universe from the no-user legacy mode.
	hasUsers := len(workloads) > 0
	if len(unique) == 0 && !hasUsers {
		hasUsers, err = a.storage.HasActiveScannerUsers(ctx)
		if err != nil {
			return nil, fmt.Errorf("check active Telegram scanner users: %w", err)
		}
	}
	if len(unique) == 0 && !hasUsers {
		instruments, err := a.storage.ListActiveInstruments(ctx, a.config.Scanner.InstrumentLimit)
		if err != nil {
			return nil, fmt.Errorf("list active Bybit instruments: %w", err)
		}
		for _, instrument := range instruments {
			for _, timeframe := range a.config.Scanner.Timeframes {
				key := strings.ToUpper(strings.TrimSpace(instrument.Symbol)) + "/" + timeframe
				unique[key] = bybit.KlineSubscription{Symbol: strings.ToUpper(strings.TrimSpace(instrument.Symbol)), Timeframe: timeframe}
			}
		}
	}
	if err := a.signalScanner.RefreshProfileRouting(workloads, hasUsers); err != nil {
		return nil, fmt.Errorf("refresh scanner profile routing: %w", err)
	}

	subscriptions := make([]bybit.KlineSubscription, 0, len(unique))
	for _, subscription := range unique {
		subscriptions = append(subscriptions, subscription)
	}
	sort.Slice(subscriptions, func(i, j int) bool {
		if subscriptions[i].Timeframe == subscriptions[j].Timeframe {
			return subscriptions[i].Symbol < subscriptions[j].Symbol
		}
		return subscriptions[i].Timeframe < subscriptions[j].Timeframe
	})
	return subscriptions, nil
}

func sameKlineSubscriptions(first, second []bybit.KlineSubscription) bool {
	if len(first) != len(second) {
		return false
	}
	for index := range first {
		if first[index] != second[index] {
			return false
		}
	}
	return true
}

func (a *App) scannerTimeout(ctx context.Context) time.Duration {
	workloads, err := a.storage.ListScannerWorkloads(ctx)
	if err == nil && len(workloads) > 0 {
		profilePairs := 0
		for _, workload := range workloads {
			profilePairs += len(workload.Symbols)
		}
		if profilePairs > 0 {
			return 2*time.Minute + time.Duration(profilePairs)*200*time.Millisecond
		}
	}
	requests := a.config.Scanner.InstrumentLimit * len(a.config.Scanner.Timeframes)
	return 2*time.Minute + time.Duration(requests)*200*time.Millisecond
}

func waitForMarketRetry(ctx context.Context, marketChanged <-chan struct{}) bool {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-marketChanged:
		return true
	case <-timer.C:
		return true
	}
}

func waitForSubscriptionRefresh(ctx context.Context, marketChanged <-chan struct{}, refresh <-chan time.Time) bool {
	select {
	case <-ctx.Done():
		return false
	case <-marketChanged:
		return true
	case <-refresh:
		return true
	}
}

// Storage exposes the PostgreSQL repository to application services.
func (a *App) Storage() *storage.Repository {
	return a.storage
}

func (a *App) Close() {
	if a.pool != nil {
		a.pool.Close()
	}
}

func healthHandler(ping func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeHealth(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := ping(ctx); err != nil {
			writeHealth(w, http.StatusServiceUnavailable, "not_ready")
			return
		}
		writeHealth(w, http.StatusOK, "ready")
	})
	return mux
}

func writeHealth(w http.ResponseWriter, status int, value string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": value})
}
