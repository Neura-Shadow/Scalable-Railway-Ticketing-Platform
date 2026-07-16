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

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/application"
	eventpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/publisher"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerhttp"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.LoadFor(config.ProcessOutboxWorker)
	if err != nil {
		logger.Error("outbox worker configuration invalid")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if !cfg.OutboxPublisherEnabled {
		logger.Info("outbox publisher disabled", "category", "outbox_publisher_disabled")
		return
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("outbox worker database unavailable")
		os.Exit(1)
	}
	defer pool.Close()
	store, err := eventpostgres.NewStore(pool)
	if err != nil {
		logger.Error("outbox store initialization failed")
		os.Exit(1)
	}

	var eventPublisher application.Publisher
	var redisClient *redis.Client
	readiness := workerhttp.ReadinessCheck(pool.Ping)
	switch cfg.OutboxPublisher {
	case "redis_stream":
		redisClient = redis.NewClient(&redis.Options{Addr: cfg.RedisAddress, Password: cfg.RedisPassword})
		defer func() { _ = redisClient.Close() }()
		redisReady := func(checkContext context.Context) error {
			return redisClient.Ping(checkContext).Err()
		}
		startupContext, cancelStartup := context.WithTimeout(ctx, cfg.RedisTimeout)
		startupErr := redisReady(startupContext)
		cancelStartup()
		if startupErr != nil {
			logger.Error("outbox Redis publisher unavailable")
			os.Exit(1)
		}
		eventPublisher, err = publisher.NewRedisStream(redisClient, "railway:outbox:v1")
		readiness = allReady(pool.Ping, redisReady)
	case "log":
		if cfg.Environment == config.EnvironmentProduction {
			logger.Warn("production log publisher override enabled", "category", "production_log_publisher_override")
		}
		eventPublisher, err = publisher.NewLog(logger)
	default:
		logger.Error("outbox publisher configuration invalid")
		os.Exit(1)
	}
	if err != nil {
		logger.Error("outbox publisher initialization failed")
		os.Exit(1)
	}
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		logger.Error("outbox metrics initialization failed")
		os.Exit(1)
	}
	healthServer, err := workerhttp.New(cfg.WorkerHTTPAddress, registry, readiness, outboxReadinessTimeout(cfg))
	if err != nil {
		logger.Error("outbox health server invalid")
		os.Exit(1)
	}
	listener, err := net.Listen("tcp", cfg.WorkerHTTPAddress)
	if err != nil {
		logger.Error("outbox health listener unavailable")
		os.Exit(1)
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- healthServer.Serve(listener) }()
	defer shutdownWorkerHTTP(healthServer, cfg.ShutdownTimeout)
	worker, err := application.NewWorker(store, eventPublisher, clock.RealClock{}, metrics, application.Config{
		WorkerID:          "outbox-" + uuid.NewString(),
		BatchSize:         cfg.OutboxBatchSize,
		MaxAttempts:       cfg.OutboxMaxAttempts,
		ProcessingTimeout: cfg.OutboxProcessingTimeout,
		RetryBase:         cfg.OutboxRetryBase,
		RetryMax:          cfg.OutboxRetryMax,
	})
	if err != nil {
		logger.Error("outbox worker initialization failed")
		os.Exit(1)
	}

	run := func() {
		passContext, cancel := context.WithTimeout(ctx, cfg.WorkerPassTimeout)
		defer cancel()
		result, runErr := worker.RunOnce(passContext)
		if runErr != nil {
			logger.Error("outbox pass completed with finalize failures")
			return
		}
		logger.Info("outbox pass complete", "claimed", result.Claimed, "published", result.Published, "retried", result.Retried, "dead_letter", result.DeadLetter)
	}
	run()
	ticker := clock.RealClock{}.NewTicker(cfg.OutboxPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case serverErr := <-serverErrors:
			if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
				logger.Error("outbox health server failed")
			}
			return
		case <-ticker.C():
			run()
		}
	}
}

func allReady(checks ...workerhttp.ReadinessCheck) workerhttp.ReadinessCheck {
	return func(ctx context.Context) error {
		for _, check := range checks {
			if check == nil || check(ctx) != nil {
				return errors.New("worker dependency unavailable")
			}
		}
		return nil
	}
}

func outboxReadinessTimeout(cfg config.Config) time.Duration {
	timeout := cfg.DatabaseTimeout
	if cfg.OutboxPublisher == "redis_stream" && cfg.RedisTimeout > timeout {
		timeout = cfg.RedisTimeout
	}
	return timeout
}

func shutdownWorkerHTTP(server *http.Server, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		_ = server.Close()
	}
}
