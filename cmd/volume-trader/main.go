package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Quantum103/Scrinner_PAMPA/internal/bybit"
	"github.com/Quantum103/Scrinner_PAMPA/internal/config"
	"github.com/Quantum103/Scrinner_PAMPA/internal/market"
	marketpaper "github.com/Quantum103/Scrinner_PAMPA/internal/paper"
	"github.com/Quantum103/Scrinner_PAMPA/internal/stream"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("Некорректная конфигурация", "ошибка", err)
		os.Exit(1)
	}

	logger := newLogger(cfg.LogLevel)
	repository, err := marketpaper.OpenSQLiteRepository(cfg.Database.Path, cfg.Database.BusyTimeout)
	if err != nil {
		logger.Error("Не удалось открыть общую SQLite базу", "ошибка", err, "путь", cfg.Database.Path)
		os.Exit(1)
	}
	defer repository.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("Volume Trader запущен",
		"режим", "виртуальная торговля",
		"биржа", "Bybit",
		"категория", config.MarketCategory,
		"направление", "только LONG",
		"виртуальный_депозит_USDT", cfg.Paper.InitialDepositUSDT,
		"режим_счёта", "Paper Trading",
		"sqlite_база", cfg.Database.Path,
		"получение_рыночных_данных", cfg.MarketData.Enabled,
	)

	if !cfg.MarketData.Enabled {
		logger.Info("Получение рыночных данных отключено")
		<-ctx.Done()
		logger.Info("Volume Trader остановлен")
		return
	}

	client := bybit.NewClient(cfg.Bybit.RESTURL)
	wsConfig := stream.DefaultConfig(cfg.Bybit.PublicWSURL)
	wsConfig.Depth = cfg.WebSocket.OrderbookDepth
	wsConfig.MaxSymbolsPerConnection = cfg.WebSocket.MaxSymbolsPerConnection
	wsConfig.MaxArgsPerSubscribe = cfg.WebSocket.MaxArgsPerSubscribe
	wsConfig.MaxDataAge = cfg.WebSocket.MaxDataAge
	wsConfig.TradeBufferCapacity = cfg.WebSocket.TradeBufferCapacity
	wsConfig.SeenTradeIDCapacity = cfg.WebSocket.SeenTradeIDCapacity
	wsConfig.Analysis = cfg.Analysis
	wsConfig.Strategy = cfg.EntryStrategy
	wsConfig.Paper = cfg.PaperEngineConfig()
	wsConfig.PaperRepository = repository
	wsManager, err := stream.NewManager(wsConfig, logger)
	if err != nil {
		_ = repository.Close()
		logger.Error("Некорректная конфигурация WebSocket", "ошибка", err)
		os.Exit(1)
	}
	wsDone := make(chan struct{})
	go func() {
		defer close(wsDone)
		wsManager.Run(ctx)
	}()

	poller := market.NewPoller(
		client,
		cfg.MarketData.UpdateInterval,
		cfg.MarketData.InstrumentRefreshInterval,
		cfg.MarketData.MaxTrackedSymbols,
		wsManager,
		logger,
	)
	poller.Run(ctx)
	<-wsDone
	logger.Info("Volume Trader остановлен")
}

func newLogger(level string) *slog.Logger {
	var slogLevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		slogLevel = slog.LevelDebug
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slogLevel}))
}
