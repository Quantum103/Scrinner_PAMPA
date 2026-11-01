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
		slog.Error("Invalid configuration", "error", err)
		os.Exit(1)
	}

	logger := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("Volume Trader started",
		"mode", "paper",
		"exchange", "Bybit",
		"category", config.MarketCategory,
		"position_side", "LONG_ONLY",
		"virtual_deposit_usdt", cfg.Paper.InitialDepositUSDT,
		"market_data_enabled", cfg.MarketData.Enabled,
	)

	if !cfg.MarketData.Enabled {
		logger.Info("Public market-data polling is disabled")
		<-ctx.Done()
		logger.Info("Volume Trader stopped")
		return
	}

	client := bybit.NewClient(cfg.Bybit.RESTURL)
	poller := market.NewPoller(client, cfg.MarketData.UpdateInterval, cfg.MarketData.MaxTrackedSymbols, logger)
	poller.Run(ctx)
	logger.Info("Volume Trader stopped")
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
