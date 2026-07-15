package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/application"
	eventpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/publisher"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil || cfg.DatabaseURL == "" {
		logger.Error("outbox worker configuration invalid")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
	if cfg.OutboxPublisher == "redis_stream" {
		redisClient = redis.NewClient(&redis.Options{Addr: cfg.RedisAddress, Password: cfg.RedisPassword})
		defer redisClient.Close()
		eventPublisher, err = publisher.NewRedisStream(redisClient, "railway:outbox:v1")
	} else {
		eventPublisher, err = publisher.NewLog(logger)
	}
	if err != nil {
		logger.Error("outbox publisher initialization failed")
		os.Exit(1)
	}
	metrics, err := platformmetrics.New(prometheus.NewRegistry())
	if err != nil {
		logger.Error("outbox metrics initialization failed")
		os.Exit(1)
	}
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
		result, runErr := worker.RunOnce(ctx)
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
		case <-ticker.C():
			run()
		}
	}
}
