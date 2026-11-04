package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	"github.com/Quantum103/Scrinner_PAMPA/internal/config"
	"github.com/Quantum103/Scrinner_PAMPA/internal/market"
	"github.com/Quantum103/Scrinner_PAMPA/internal/migrations"
	"github.com/Quantum103/Scrinner_PAMPA/internal/scanner"
	"github.com/Quantum103/Scrinner_PAMPA/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

type App struct {
	pool          *pgxpool.Pool
	storage       *storage.Repository
	server        *http.Server
	logger        *slog.Logger
	config        config.Config
	marketSync    *market.Synchronizer
	signalScanner *scanner.Scanner
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
	marketSync, err := market.NewSynchronizer(restClient, repository)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("initialize Bybit market synchronizer: %w", err)
	}
	signalScanner, err := scanner.New(restClient, repository, scanner.Settings{
		Timeframes: cfg.Scanner.Timeframes, RSILength: cfg.Scanner.RSILength,
		EMALength: cfg.Scanner.EMALength, InstrumentLimit: cfg.Scanner.InstrumentLimit,
		HistoryCandles: cfg.Scanner.HistoryCandles,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("initialize RSI scanner: %w", err)
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
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	marketReady := make(chan struct{})
	go a.runMarketSync(runCtx, marketReady)
	go func() {
		select {
		case <-marketReady:
			a.runScanner(runCtx)
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

func (a *App) runMarketSync(ctx context.Context, initialSyncDone chan<- struct{}) {
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
		// Allow enough time for the REST rate limiter to serialize the configured pairs.
		requests := a.config.Scanner.InstrumentLimit * len(a.config.Scanner.Timeframes)
		timeout := 2*time.Minute + time.Duration(requests)*200*time.Millisecond
		scanCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		report, err := a.signalScanner.ScanOnce(scanCtx)
		if err != nil {
			a.logger.Error("RSI scanner cycle completed with errors",
				"error", err,
				"instruments", report.Instruments,
				"pairs_scanned", report.PairsScanned,
				"candles_processed", report.CandlesProcessed,
				"signals_inserted", report.SignalsInserted,
			)
			return
		}
		a.logger.Info("RSI scanner cycle completed",
			"instruments", report.Instruments,
			"pairs_scanned", report.PairsScanned,
			"candles_processed", report.CandlesProcessed,
			"signals_inserted", report.SignalsInserted,
			"deliveries_queued", report.DeliveriesQueued,
		)
	}

	scan()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scan()
		}
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
