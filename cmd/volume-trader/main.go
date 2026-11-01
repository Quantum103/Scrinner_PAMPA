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
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("Некорректная конфигурация", "ошибка", err)
		os.Exit(1)
	}

	logger := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("Volume Trader запущен",
		"режим", "виртуальная торговля",
		"биржа", "Bybit",
		"категория", config.MarketCategory,
		"направление", "только LONG",
		"виртуальный_депозит_USDT", cfg.Paper.InitialDepositUSDT,
		"получение_рыночных_данных", cfg.MarketData.Enabled,
	)

	if !cfg.MarketData.Enabled {
		logger.Info("Получение рыночных данных отключено")
		<-ctx.Done()
		logger.Info("Volume Trader остановлен")
		return
	}

	client := bybit.NewClient(cfg.Bybit.RESTURL)
	poller := market.NewPoller(
		client,
		cfg.MarketData.UpdateInterval,
		cfg.MarketData.InstrumentRefreshInterval,
		cfg.MarketData.MaxTrackedSymbols,
		logger,
	)
	poller.Run(ctx)
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
