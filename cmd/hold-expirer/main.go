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
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerhttp"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerlane"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalworker"
	shardingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/routecache"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

const holdExpirerSchemaVersion = 11

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.LoadFor(config.ProcessHoldExpirer)
	if err != nil {
		logger.Error("hold expirer configuration invalid")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgresx.NewRegionalBoundedPool(ctx, cfg.DatabaseURL, cfg.ControlDatabaseMaxOpenConns, holdExpirerRegionalSession(cfg))
	if err != nil {
		logger.Error("hold expirer database unavailable")
		os.Exit(1)
	}
	defer pool.Close()
	var physicalRuntime *physicalworker.Runtime
	if cfg.BookingShardMode == config.BookingShardModePhysical {
		physicalRuntime, err = physicalworker.NewRuntime(ctx, cfg, pool)
		if err != nil {
			logger.Error("hold expirer physical shard runtime unavailable")
			os.Exit(1)
		}
		defer physicalRuntime.Close()
	}
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.NewEventMetrics(registry)
	if err != nil {
		logger.Error("hold expirer metrics initialization failed")
		os.Exit(1)
	}
	healthServer, err := workerhttp.New(
		cfg.WorkerHTTPAddress, registry, holdExpirerReadiness(pool, cfg, physicalRuntime), cfg.DatabaseTimeout,
	)
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
	bookingStore := bookingpostgres.New(pool)
	if cfg.BookingShardMode == config.BookingShardModeSchemaPOC {
		allowedShards, parseErr := sharding.ParseShardIDs(cfg.BookingShardIDs)
		if parseErr != nil {
			logger.Error("hold expirer shard allowlist initialization failed")
			os.Exit(1)
		}
		cache, cacheErr := routecache.New(routecache.Config{
			Enabled: cfg.BookingRouteCacheEnabled, TTL: cfg.BookingRouteCacheTTL,
			MaxEntries: cfg.BookingRouteCacheMaxEntries,
		})
		if cacheErr != nil {
			logger.Error("hold expirer route cache initialization failed")
			os.Exit(1)
		}
		router, routerErr := shardingpostgres.NewRouter(
			pool,
			cache,
			shardingpostgres.WithMetrics(metrics),
			shardingpostgres.WithQueryTimeout(cfg.BookingShardQueryTimeout),
			shardingpostgres.WithAllowedShards(allowedShards...),
		)
		if routerErr != nil {
			logger.Error("hold expirer shard router initialization failed")
			os.Exit(1)
		}
		bookingStore, err = bookingpostgres.NewSharded(pool, router)
		if err != nil {
			logger.Error("hold expirer sharded store initialization failed")
			os.Exit(1)
		}
	}
	expirer, err := application.NewHoldExpirer(bookingStore, clock.RealClock{}, metrics, cfg.HoldExpirerBatchSize)
	if err != nil {
		logger.Error("hold expirer initialization failed")
		os.Exit(1)
	}
	var physicalExpirer *physicalworker.Orchestrator
	if physicalRuntime != nil {
		processor, processorErr := physicalworker.NewHoldExpirationProcessor(physicalworker.HoldExpirationOptions{
			StatementTimeout: cfg.PhysicalShardQueryTimeout,
			LockTimeout:      cfg.PhysicalShardQueryTimeout,
			Now:              clock.RealClock{}.Now,
		})
		if processorErr != nil {
			logger.Error("hold expirer physical processor initialization failed")
			os.Exit(1)
		}
		physicalExpirer, err = physicalworker.New(physicalRuntime.Handles(), processor, physicalWorkerConfig(cfg, len(physicalRuntime.Handles())))
		if err != nil {
			logger.Error("hold expirer physical orchestrator initialization failed")
			os.Exit(1)
		}
	}

	run := func() {
		passContext, cancel := context.WithTimeout(ctx, cfg.WorkerPassTimeout)
		defer cancel()
		if !holdExpirerPassEnabled(cfg) || postgresx.CheckRegionalReadiness(passContext, pool, holdExpirerRegionalSession(cfg)) != nil || (physicalRuntime != nil && physicalRuntime.Ready(passContext) != nil) {
			logger.Info("hold expirer retained without regional claim authority", "deployment_role", cfg.DeploymentRole)
			return
		}
		var physical func(context.Context) (physicalworker.Result, error)
		if physicalExpirer != nil {
			physical = physicalExpirer.RunOnce
		}
		outcome, laneErr := workerlane.Run(passContext, cfg.WorkerPassTimeout,
			cfg.PhysicalWorkerShardTimeout, expirer.RunOnce, physical)
		if laneErr != nil || outcome.ControlErr != nil || outcome.PhysicalErr != nil {
			logger.Error("hold expiration pass completed with isolated failures", "control_expired_count", outcome.Control.Expired, "physical_expired_count", outcome.Physical.Processed)
			return
		}
		logger.Info("hold expiration pass complete", "control_expired_count", outcome.Control.Expired, "physical_expired_count", outcome.Physical.Processed)
	}
	runInitialExpirationPass(cfg.HoldExpirerEnabled, run)
	if !cfg.HoldExpirerEnabled {
		logger.Info("hold expirer disabled", "category", "hold_expirer_disabled")
		if serverErr := waitForDisabledHoldExpirer(ctx, serverErrors); serverErr != nil {
			logger.Error("hold expirer health server failed")
		}
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

func holdExpirerReadiness(pool *pgxpool.Pool, cfg config.Config, physicalRuntime ...*physicalworker.Runtime) workerhttp.ReadinessCheck {
	return func(ctx context.Context) error {
		if pool == nil || cfg.ValidateFor(config.ProcessHoldExpirer) != nil {
			return errors.New("hold expirer dependency unavailable")
		}
		if err := pool.Ping(ctx); err != nil {
			return errors.New("hold expirer dependency unavailable")
		}
		if !holdExpirerPassEnabled(cfg) || postgresx.CheckRegionalReadiness(ctx, pool, holdExpirerRegionalSession(cfg)) != nil {
			return errors.New("hold expirer regional authority unavailable")
		}
		var version int
		var dirty bool
		if err := pool.QueryRow(ctx, `SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(
			&version, &dirty,
		); err != nil || version != holdExpirerSchemaVersion || dirty {
			return errors.New("hold expirer migration unavailable")
		}
		if cfg.BookingShardMode == config.BookingShardModeSchemaPOC {
			var serving int
			var incompatible int
			if err := pool.QueryRow(ctx, `
SELECT
    count(*) FILTER (WHERE enabled AND state IN ('active', 'draining')),
    count(*) FILTER (WHERE enabled AND state IN ('active', 'draining')
        AND minimum_fencing_protocol_version > $1)
FROM public.booking_shards
WHERE shard_id IN ('legacy', 'shard-0', 'shard-1')`, sharding.SupportedFencingProtocolVersion).Scan(
				&serving,
				&incompatible,
			); err != nil || serving < 1 || incompatible != 0 {
				return errors.New("hold expirer shard catalog unavailable")
			}
		}
		if cfg.BookingShardMode == config.BookingShardModePhysical {
			if len(physicalRuntime) != 1 || physicalRuntime[0] == nil || physicalRuntime[0].Ready(ctx) != nil {
				return errors.New("hold expirer physical shard migration unavailable")
			}
		}
		return nil
	}
}

func holdExpirerPassEnabled(cfg config.Config) bool {
	return cfg.HoldExpirerEnabled && cfg.DeploymentRole == config.DeploymentRoleActive && cfg.RegionalWritesEnabled
}

func holdExpirerRegionalSession(cfg config.Config) postgresx.RegionalSession {
	return postgresx.RegionalSession{Region: string(cfg.DeploymentRegion), Role: string(cfg.DeploymentRole), Epoch: cfg.RegionEpoch, WritesEnabled: cfg.RegionalWritesEnabled}
}

func physicalWorkerConfig(cfg config.Config, shardCount int) physicalworker.Config {
	maxConcurrency := cfg.WorkerShardConcurrency
	if maxConcurrency > shardCount {
		maxConcurrency = shardCount
	}
	return physicalworker.Config{
		MaxConcurrency: maxConcurrency,
		PerShardLimit:  cfg.HoldExpirerBatchSize,
		PassLimit:      cfg.HoldExpirerBatchSize,
		ShardTimeout:   cfg.PhysicalWorkerShardTimeout,
	}
}

func runInitialExpirationPass(enabled bool, run func()) {
	if enabled {
		run()
	}
}

func waitForDisabledHoldExpirer(ctx context.Context, serverErrors <-chan error) error {
	select {
	case <-ctx.Done():
		return nil
	case serverErr := <-serverErrors:
		if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
			return serverErr
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
