package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/application"
	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil || cfg.DatabaseURL == "" {
		logger.Error("hold expirer configuration invalid")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("hold expirer database unavailable")
		os.Exit(1)
	}
	defer pool.Close()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		logger.Error("hold expirer metrics initialization failed")
		os.Exit(1)
	}
	expirer, err := application.NewHoldExpirer(bookingpostgres.New(pool), clock.RealClock{}, metrics, cfg.HoldExpirerBatchSize)
	if err != nil {
		logger.Error("hold expirer initialization failed")
		os.Exit(1)
	}

	run := func() {
		result, runErr := expirer.RunOnce(ctx)
		if runErr != nil {
			logger.Error("hold expiration pass failed")
			return
		}
		logger.Info("hold expiration pass complete", "expired_count", result.Expired)
	}
	run()
	if !cfg.HoldExpirerEnabled {
		return
	}
	ticker := clock.RealClock{}.NewTicker(cfg.HoldExpirerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			run()
		}
	}
}
