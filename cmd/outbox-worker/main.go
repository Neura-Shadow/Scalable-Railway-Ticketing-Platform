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
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/redisx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerhttp"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerlane"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalworker"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

const outboxSchemaVersion = 11

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.LoadFor(config.ProcessOutboxWorker)
	if err != nil {
		logger.Error("outbox worker configuration invalid")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgresx.NewRegionalBoundedPool(ctx, cfg.DatabaseURL, cfg.ControlDatabaseMaxOpenConns, outboxRegionalSession(cfg))
	if err != nil {
		logger.Error("outbox worker database unavailable")
		os.Exit(1)
	}
	defer pool.Close()
	var physicalRuntime *physicalworker.Runtime
	if cfg.BookingShardMode == config.BookingShardModePhysical {
		physicalRuntime, err = physicalworker.NewRuntime(ctx, cfg, pool)
		if err != nil {
			logger.Error("outbox physical shard runtime unavailable")
			os.Exit(1)
		}
		defer physicalRuntime.Close()
	}
	store, err := eventpostgres.NewStore(pool)
	if err != nil {
		logger.Error("outbox store initialization failed")
		os.Exit(1)
	}

	var eventPublisher application.Publisher
	var redisClient *redis.Client
	databaseReadiness := func(checkContext context.Context) error {
		if pool == nil || cfg.ValidateFor(config.ProcessOutboxWorker) != nil {
			return errors.New("worker dependency unavailable")
		}
		if err := pool.Ping(checkContext); err != nil {
			return errors.New("worker dependency unavailable")
		}
		if !outboxPassEnabled(cfg) || postgresx.CheckRegionalReadiness(checkContext, pool, outboxRegionalSession(cfg)) != nil {
			return errors.New("worker regional authority unavailable")
		}
		var version int
		var dirty bool
		if err := pool.QueryRow(checkContext, `SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(
			&version, &dirty,
		); err != nil || version != outboxSchemaVersion || dirty {
			return errors.New("worker migration unavailable")
		}
		return nil
	}
	readinessChecks := []workerhttp.ReadinessCheck{databaseReadiness}
	if physicalRuntime != nil {
		readinessChecks = append(readinessChecks, physicalRuntime.Ready)
	}
	readiness := allReady(readinessChecks...)
	if cfg.OutboxPublisherEnabled {
		switch cfg.OutboxPublisher {
		case "redis_stream":
			redisClient = redis.NewClient(redisx.BoundedClientOptions(
				cfg.RedisAddress,
				cfg.RedisPassword,
				cfg.RedisTimeout,
			))
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
			readinessChecks = append(readinessChecks, redisReady)
			readiness = allReady(readinessChecks...)
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
	}
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.NewEventMetrics(registry)
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
	if !cfg.OutboxPublisherEnabled {
		logger.Info("outbox publisher disabled", "category", "outbox_publisher_disabled")
		select {
		case <-ctx.Done():
			return
		case serverErr := <-serverErrors:
			if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
				logger.Error("outbox health server failed")
			}
			return
		}
	}
	workerID := "outbox-" + uuid.NewString()
	worker, err := application.NewWorker(store, eventPublisher, clock.RealClock{}, metrics, application.Config{
		WorkerID:          workerID,
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
	var physicalOutbox *physicalworker.Orchestrator
	if physicalRuntime != nil {
		processor, processorErr := physicalworker.NewOutboxProcessor(eventPublisher, physicalworker.OutboxOptions{
			WorkerID:          workerID,
			MaxAttempts:       cfg.OutboxMaxAttempts,
			ProcessingTimeout: cfg.OutboxProcessingTimeout,
			RetryBase:         cfg.OutboxRetryBase,
			RetryMax:          cfg.OutboxRetryMax,
			StatementTimeout:  cfg.PhysicalShardQueryTimeout,
			LockTimeout:       cfg.PhysicalShardQueryTimeout,
			Now:               clock.RealClock{}.Now,
		})
		if processorErr != nil {
			logger.Error("outbox physical processor initialization failed")
			os.Exit(1)
		}
		physicalOutbox, err = physicalworker.New(physicalRuntime.Handles(), processor, physicalOutboxWorkerConfig(cfg, len(physicalRuntime.Handles())))
		if err != nil {
			logger.Error("outbox physical orchestrator initialization failed")
			os.Exit(1)
		}
	}

	run := func() {
		passContext, cancel := context.WithTimeout(ctx, cfg.WorkerPassTimeout)
		defer cancel()
		if !outboxPassEnabled(cfg) || postgresx.CheckRegionalReadiness(passContext, pool, outboxRegionalSession(cfg)) != nil || (physicalRuntime != nil && physicalRuntime.Ready(passContext) != nil) {
			logger.Info("outbox worker retained without regional claim authority", "deployment_role", cfg.DeploymentRole)
			return
		}
		var physical func(context.Context) (physicalworker.Result, error)
		if physicalOutbox != nil {
			physical = physicalOutbox.RunOnce
		}
		outcome, laneErr := workerlane.Run(passContext, cfg.WorkerPassTimeout,
			cfg.PhysicalWorkerShardTimeout, worker.RunOnce, physical)
		if laneErr != nil || outcome.ControlErr != nil || outcome.PhysicalErr != nil {
			logger.Error("outbox pass completed with isolated failures", "control_claimed", outcome.Control.Claimed, "physical_processed", outcome.Physical.Processed)
			return
		}
		logger.Info("outbox pass complete", "control_claimed", outcome.Control.Claimed, "control_published", outcome.Control.Published, "control_retried", outcome.Control.Retried, "control_dead_letter", outcome.Control.DeadLetter, "physical_processed", outcome.Physical.Processed)
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

func outboxPassEnabled(cfg config.Config) bool {
	return cfg.OutboxPublisherEnabled && cfg.DeploymentRole == config.DeploymentRoleActive && cfg.RegionalWritesEnabled
}

func outboxRegionalSession(cfg config.Config) postgresx.RegionalSession {
	return postgresx.RegionalSession{Region: string(cfg.DeploymentRegion), Role: string(cfg.DeploymentRole), Epoch: cfg.RegionEpoch, WritesEnabled: cfg.RegionalWritesEnabled}
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

func physicalOutboxWorkerConfig(cfg config.Config, shardCount int) physicalworker.Config {
	maxConcurrency := cfg.WorkerShardConcurrency
	if maxConcurrency > shardCount {
		maxConcurrency = shardCount
	}
	return physicalworker.Config{
		MaxConcurrency: maxConcurrency,
		PerShardLimit:  cfg.OutboxBatchSize,
		PassLimit:      cfg.OutboxBatchSize,
		ShardTimeout:   cfg.PhysicalWorkerShardTimeout,
	}
}

func shutdownWorkerHTTP(server *http.Server, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		_ = server.Close()
	}
}
