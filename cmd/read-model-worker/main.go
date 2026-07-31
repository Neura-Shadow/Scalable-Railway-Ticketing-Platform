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

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/redisx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerhttp"
	querycache "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/cache"
	queryreadmodel "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/readmodel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

const (
	readModelStream                = "railway:outbox:v1"
	readModelDLQ                   = "railway:outbox:v1:read-model:dlq"
	schemaVersion                  = 9
	projectionLagObservationPeriod = 5 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("read-model worker stopped", "reason", "runtime_failure")
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	if logger == nil {
		return errors.New("logger unavailable")
	}
	cfg, err := config.LoadFor(config.ProcessReadModelWorker)
	if err != nil {
		return errors.New("read-model worker configuration invalid")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := postgresx.NewBoundedPool(ctx, cfg.DatabaseURL, cfg.ControlDatabaseMaxOpenConns)
	if err != nil {
		return errors.New("read-model worker database unavailable")
	}
	defer pool.Close()
	redisClient := redis.NewClient(redisx.BoundedClientOptions(
		cfg.RedisAddress, cfg.RedisPassword, cfg.RedisTimeout,
	))
	defer func() { _ = redisClient.Close() }()
	projectionStore, err := queryreadmodel.NewStore(pool, clock.RealClock{})
	if err != nil {
		return errors.New("read-model store initialization failed")
	}
	impacts, err := queryreadmodel.NewPostgresImpactResolver(pool)
	if err != nil {
		return errors.New("read-model impact resolver initialization failed")
	}
	versions, err := querycache.NewSecureVersionManager(redisClient)
	if err != nil {
		return errors.New("read-model version manager initialization failed")
	}
	coordinator, err := queryreadmodel.NewEventCoordinator(projectionStore, impacts, versions)
	if err != nil {
		return errors.New("read-model event coordinator initialization failed")
	}
	registry := prometheus.NewRegistry()
	readMetrics, err := platformmetrics.NewReadModelMetrics(registry)
	if err != nil {
		return errors.New("read-model metrics initialization failed")
	}
	stopLagObserver := startProjectionLagObserver(
		ctx, projectionStore, readMetrics, cfg.DatabaseTimeout, logger,
	)
	defer stopLagObserver()
	coordinator.WithMetrics(readMetrics)
	transport, err := queryreadmodel.NewRedisStreamTransport(
		redisClient, readModelStream, cfg.ReadModelConsumerGroup, readModelDLQ,
	)
	if err != nil {
		return errors.New("read-model stream transport initialization failed")
	}
	consumerName := cfg.ReadModelConsumerName
	if consumerName == "" {
		consumerName = "read-model-" + uuid.NewString()
	}
	worker, err := queryreadmodel.NewWorker(transport, coordinator, queryreadmodel.WorkerConfig{
		ConsumerName: consumerName,
		BatchSize:    int64(cfg.ReadModelWorkerBatchSize), MaxAttempts: int64(cfg.ReadModelWorkerMaxAttempts),
		PendingIdle: cfg.ReadModelWorkerPendingIdle,
	})
	if err != nil {
		return errors.New("read-model worker initialization failed")
	}
	readiness := readModelReadiness(pool, redisClient, cfg)
	healthServer, err := workerhttp.New(cfg.WorkerHTTPAddress, registry, readiness, readModelReadinessTimeout(cfg))
	if err != nil {
		return errors.New("read-model health server invalid")
	}
	listener, err := net.Listen("tcp", cfg.WorkerHTTPAddress)
	if err != nil {
		return errors.New("read-model health listener unavailable")
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- healthServer.Serve(listener) }()
	defer shutdownWorkerHTTP(healthServer, cfg.ShutdownTimeout)
	if !cfg.ReadModelWorkerEnabled {
		logger.Info("read-model worker disabled", "category", "read_model_worker_disabled")
		return waitForShutdown(ctx, serverErrors)
	}
	runPass := func() {
		passContext, cancel := context.WithTimeout(ctx, cfg.WorkerPassTimeout)
		defer cancel()
		result, passErr := worker.RunOnce(passContext)
		if passErr != nil {
			logger.Warn("read-model pass completed with handled failures",
				"claimed", result.Claimed, "read", result.Read, "processed", result.Processed,
				"retried", result.Retried, "continued", result.Continued, "dead_lettered", result.DeadLettered)
			return
		}
		logger.Info("read-model pass complete",
			"claimed", result.Claimed, "read", result.Read, "processed", result.Processed,
			"retried", result.Retried, "continued", result.Continued, "dead_lettered", result.DeadLettered)
	}
	runPass()
	ticker := clock.RealClock{}.NewTicker(cfg.ReadModelWorkerPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case serverErr := <-serverErrors:
			if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
				return errors.New("read-model health server failed")
			}
			return nil
		case <-ticker.C():
			runPass()
		}
	}
}

func startProjectionLagObserver(
	parent context.Context,
	store *queryreadmodel.Store,
	readMetrics *platformmetrics.ReadModelMetrics,
	timeout time.Duration,
	logger *slog.Logger,
) func() {
	observerContext, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(projectionLagObservationPeriod)
		defer ticker.Stop()
		observe := func() {
			ctx, stop := context.WithTimeout(observerContext, timeout)
			defer stop()
			lag, err := store.ProjectionLag(ctx, queryreadmodel.DurableConsumerName)
			if err != nil {
				logger.Warn("read-model lag observation failed", "reason", "database")
				return
			}
			readMetrics.SetProjectionLag(lag)
		}
		observe()
		for {
			select {
			case <-observerContext.Done():
				return
			case <-ticker.C:
				observe()
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func readModelReadiness(
	pool *pgxpool.Pool,
	client redis.UniversalClient,
	cfg config.Config,
) workerhttp.ReadinessCheck {
	return func(ctx context.Context) error {
		if pool == nil || client == nil || cfg.ValidateFor(config.ProcessReadModelWorker) != nil {
			return errors.New("read-model worker dependency unavailable")
		}
		if err := pool.Ping(ctx); err != nil {
			return errors.New("read-model worker dependency unavailable")
		}
		if err := client.Ping(ctx).Err(); err != nil {
			return errors.New("read-model worker dependency unavailable")
		}
		var version int
		var dirty bool
		if err := pool.QueryRow(ctx, `SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(&version, &dirty); err != nil || version != schemaVersion || dirty {
			return errors.New("read-model worker migration unavailable")
		}
		return nil
	}
}

func readModelReadinessTimeout(cfg config.Config) time.Duration {
	if cfg.RedisTimeout > cfg.DatabaseTimeout {
		return cfg.RedisTimeout
	}
	return cfg.DatabaseTimeout
}

func waitForShutdown(ctx context.Context, serverErrors <-chan error) error {
	select {
	case <-ctx.Done():
		return nil
	case serverErr := <-serverErrors:
		if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
			return errors.New("read-model health server failed")
		}
		return nil
	}
}

func shutdownWorkerHTTP(server *http.Server, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		_ = server.Close()
	}
}
