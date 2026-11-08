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
	spotSynchronizer *market.SpotSynchronizer
	spotPublicStream *bybit.PublicSpotStream
	spotRESTClient   *bybit.Client
	momentumTracker  *momentum.Tracker
	telegramBot      *telegrambot.Bot
	spotAlertSpool   *spotAlertSpool
	spotAlertEvents  chan struct{}
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
	alertSpool, err := newSpotAlertSpool(cfg.App.DataDir)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("initialize durable Spot alert spool: %w", err)
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
		Addr: cfg.App.HTTPAddr, Handler: healthHandler(pool.Ping),
		ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second,
	}
	return &App{
		pool: pool, storage: repository, server: server, logger: logger, config: cfg,
		spotSynchronizer: spotSynchronizer, spotPublicStream: spotPublicStream,
		spotRESTClient: restClient, momentumTracker: momentumTracker,
		telegramBot: telegramBotInstance, spotAlertSpool: alertSpool,
		spotAlertEvents: make(chan struct{}, 1),
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

const (
	spotSubscriptionShardSize = 100
	spotHistoryCandleLimit    = 20
	spotHistoryWorkers        = 8
)

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
			"instruments", stats.Instruments, "watchlist", stats.Watchlist,
			"over_limit_hold", stats.OverLimitHold, "history_seed_pending", len(seedInFlight))
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

func (a *App) runSpotAlertPersistence(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		alert, pending, err := a.spotAlertSpool.Peek()
		if err != nil {
			a.logger.Error("read durable Spot alert spool failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-a.spotAlertEvents:
			case <-ticker.C:
			}
			continue
		}
		if !pending {
			select {
			case <-ctx.Done():
				return
			case <-a.spotAlertEvents:
			case <-ticker.C:
			}
			continue
		}
		retryDelay := time.Second
		for attempt := 1; ; attempt++ {
			saveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			alertID, inserted, queued, saveErr := a.storage.SaveSpotMomentumAlert(saveCtx, alert)
			cancel()
			if saveErr == nil {
				if inserted {
					a.logger.Info("Spot momentum alert persisted", "alert_id", alertID,
						"symbol", alert.Symbol, "change_percent", alert.ChangePercent,
						"deliveries_queued", queued)
				}
				for {
					if ackErr := a.spotAlertSpool.Ack(alert); ackErr == nil {
						break
					} else {
						a.logger.Error("acknowledge persisted Spot alert spool record failed",
							"symbol", alert.Symbol, "error", ackErr)
					}
					if !waitForSpotPersistenceRetry(ctx, time.Second) {
						return
					}
				}
				break
			}
			if ctx.Err() != nil {
				return
			}
			a.logger.Error("persist Spot momentum alert failed; retry scheduled",
				"symbol", alert.Symbol, "attempt", attempt, "error", saveErr)
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
	var waitGroup sync.WaitGroup
	for start := 0; start < len(symbols); start += spotSubscriptionShardSize {
		end := start + spotSubscriptionShardSize
		if end > len(symbols) {
			end = len(symbols)
		}
		shard := append([]string(nil), symbols[start:end]...)
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := a.spotPublicStream.Run(streamCtx, shard, bybit.SpotStreamHandlers{
				OnTicker: func(_ context.Context, update bybit.SpotPriceUpdate) error {
					signal, err := a.momentumTracker.Add(update.Symbol, momentum.PricePoint{At: update.At, Price: update.Price})
					if err != nil {
						return err
					}
					if signal == nil {
						return nil
					}
					alert := storage.SpotMomentumAlert{
						Symbol: signal.Symbol, DetectedAt: signal.At, ReferenceAt: signal.ReferenceAt,
						Price: signal.Price, ReferencePrice: signal.ReferencePrice,
						ChangePercent: signal.ChangePercent,
						WindowSeconds: int(momentum.DefaultWindow / time.Second),
					}
					if err := a.spotAlertSpool.Append(alert); err != nil {
						a.momentumTracker.Rearm(update.Symbol)
						a.logger.Error("append Spot alert to durable spool failed", "symbol", signal.Symbol, "error", err)
						return err
					}
					select {
					case a.spotAlertEvents <- struct{}{}:
					default:
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
		waitGroup.Wait()
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
	var waitGroup sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
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
			waitGroup.Wait()
			return
		case jobs <- symbol:
		}
	}
	close(jobs)
	waitGroup.Wait()
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
