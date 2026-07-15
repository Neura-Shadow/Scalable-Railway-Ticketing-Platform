package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/application"
	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerhttp"
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
	healthServer, err := workerhttp.New(cfg.WorkerHTTPAddress, registry, pool.Ping, cfg.DatabaseTimeout)
	if err != nil {
		logger.Error("hold expirer health server invalid")
		os.Exit(1)
	}
	listener, err := net.Listen("tcp", cfg.WorkerHTTPAddress)
	if err != nil {
		logger.Error("hold expirer health listener unavailable")
		os.Exit(1)
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- healthServer.Serve(listener) }()
	defer shutdownWorkerHTTP(healthServer, cfg.ShutdownTimeout)
	expirer, err := application.NewHoldExpirer(bookingpostgres.New(pool), clock.RealClock{}, metrics, cfg.HoldExpirerBatchSize)
	if err != nil {
		logger.Error("hold expirer initialization failed")
		os.Exit(1)
	}

	run := func() {
		passContext, cancel := context.WithTimeout(ctx, cfg.WorkerPassTimeout)
		defer cancel()
		result, runErr := expirer.RunOnce(passContext)
		if runErr != nil {
			logger.Error("hold expiration pass completed with isolated failures", "expired_count", result.Expired)
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
		case serverErr := <-serverErrors:
			if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
				logger.Error("hold expirer health server failed")
			}
			return
		case <-ticker.C():
			run()
		}
	}
}

func shutdownWorkerHTTP(server *http.Server, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		_ = server.Close()
	}
}
