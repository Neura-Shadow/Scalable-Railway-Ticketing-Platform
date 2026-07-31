package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/app"
	commandphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/physical"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/reconcile"
	reconcilephysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/reconcile/physical"
	reconcilepostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/reconcile/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand"
	operatorphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand/physical"
	operatorpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerhttp"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

const controlSchemaVersion = 9

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger, os.LookupEnv); err != nil {
		logger.Error("booking command reconciler stopped", "reason", publicReason(err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger, lookup func(string) (string, bool)) error {
	if logger == nil || lookup == nil {
		return errors.New("runtime unavailable")
	}
	cfg, err := loadConfig(lookup)
	if err != nil {
		return errors.New("configuration invalid")
	}
	rootContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	controlConfig, err := pgxpool.ParseConfig(cfg.databaseURL)
	if err != nil {
		return errors.New("control postgres configuration invalid")
	}
	controlConfig.MaxConns = int32(cfg.controlMaxConns)
	controlConfig.MinConns = 0
	controlConfig.ConnConfig.ConnectTimeout = cfg.connectTimeout
	controlPool, err := pgxpool.NewWithConfig(rootContext, controlConfig)
	if err != nil {
		return errors.New("control postgres unavailable")
	}
	defer controlPool.Close()

	shardPools := make(map[string]shardphysical.Pool, 2)
	registry, err := shardphysical.NewRegistry(rootContext, shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{
			"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: cfg.shardZeroURL},
			"physical-shard-1": {ShardID: sharding.ShardPhysicalOne, DSN: cfg.shardOneURL},
		},
		MaxCount: 2,
		Limits: shardphysical.PoolLimits{
			MaxOpenConns: cfg.shardMaxConns, MaxIdleConns: cfg.shardMaxConns,
			MaxLifetime: 30 * time.Minute, MaxIdleTime: 5 * time.Minute,
			ConnectTimeout: cfg.connectTimeout, StatementTimeout: cfg.queryTimeout,
			LockTimeout: cfg.queryTimeout,
		},
	}, func(ctx context.Context, dsn string, limits shardphysical.PoolLimits) (shardphysical.Pool, error) {
		pool, openErr := shardphysical.OpenPGXPool(ctx, dsn, limits)
		if openErr == nil {
			if dsn == cfg.shardZeroURL {
				shardPools["physical-shard-0"] = pool
			} else if dsn == cfg.shardOneURL {
				shardPools["physical-shard-1"] = pool
			}
		}
		return pool, openErr
	})
	if err != nil {
		return errors.New("physical shard registry unavailable")
	}
	defer registry.Close()
	handleResolver, err := reconcilephysical.NewCatalogHandleResolver(controlPool, registry)
	if err != nil {
		return errors.New("physical shard resolver unavailable")
	}
	controlStore, err := reconcilepostgres.NewStore(controlPool)
	if err != nil {
		return errors.New("control store unavailable")
	}
	shardInspector, err := reconcilephysical.NewInspector(handleResolver)
	if err != nil {
		return errors.New("shard inspector unavailable")
	}
	metrics, err := newMetrics()
	if err != nil {
		return errors.New("metrics unavailable")
	}
	physicalMetrics, err := platformmetrics.New(metrics.registry)
	if err != nil {
		return errors.New("metrics unavailable")
	}
	service, err := reconcile.New(controlStore, shardInspector, reconcile.Options{
		WorkerID: cfg.workerID, BatchSize: cfg.batchSize, LeaseTTL: cfg.leaseTTL,
		InspectTimeout: cfg.inspectTimeout, Metrics: physicalMetrics,
	})
	if err != nil {
		return errors.New("reconciler unavailable")
	}
	operatorService, err := newOperatorRecovery(controlPool, registry, cfg)
	if err != nil {
		return errors.New("operator command reconciler unavailable")
	}
	readiness := reconcilerReadiness(controlPool, shardPools, cfg)
	healthServer, err := workerhttp.New(cfg.httpAddress, metrics.registry, readiness, cfg.readinessTimeout)
	if err != nil {
		return errors.New("health server invalid")
	}
	listener, err := net.Listen("tcp", cfg.httpAddress)
	if err != nil {
		return errors.New("health listener unavailable")
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- healthServer.Serve(listener) }()
	defer shutdownHTTP(healthServer, cfg.shutdownTimeout)

	runPass := func() {
		ctx, cancel := context.WithTimeout(rootContext, cfg.passTimeout)
		defer cancel()
		started := time.Now()
		type bookingOutcome struct {
			result reconcile.Result
			err    error
		}
		type operatorOutcome struct {
			result operatorcommand.RecoveryResult
			err    error
		}
		bookingResults := make(chan bookingOutcome, 1)
		operatorResults := make(chan operatorOutcome, 1)
		go func() {
			result, runErr := service.RunOnce(ctx)
			bookingResults <- bookingOutcome{result: result, err: runErr}
		}()
		go func() {
			result, runErr := operatorService.RunOnce(ctx)
			operatorResults <- operatorOutcome{result: result, err: runErr}
		}()
		bookingRun := <-bookingResults
		operatorRun := <-operatorResults
		result, runErr := bookingRun.result, bookingRun.err
		operatorResult, operatorErr := operatorRun.result, operatorRun.err
		combinedErr := errors.Join(runErr, operatorErr)
		metrics.record(result, combinedErr, time.Since(started))
		metrics.recordOperator(operatorResult)
		if combinedErr != nil {
			logger.Warn("booking command reconciliation pass incomplete",
				"claimed", result.Claimed, "finalized", result.Finalized, "failed", result.Failed,
				"expired", result.Expired, "deferred", result.Deferred, "failure_count", result.Failures,
				"operator_claimed", operatorResult.Claimed, "operator_finalized", operatorResult.Finalized,
				"operator_failed", operatorResult.Failed, "operator_deferred", operatorResult.Deferred,
				"operator_failures", operatorResult.Failures)
			return
		}
		logger.Info("booking command reconciliation pass complete",
			"claimed", result.Claimed, "finalized", result.Finalized, "failed", result.Failed,
			"expired", result.Expired, "deferred", result.Deferred,
			"operator_claimed", operatorResult.Claimed, "operator_finalized", operatorResult.Finalized,
			"operator_failed", operatorResult.Failed, "operator_deferred", operatorResult.Deferred)
	}
	if cfg.enabled {
		runPass()
	} else {
		logger.Info("booking command reconciler disabled", "category", "worker_disabled")
	}
	if !cfg.enabled {
		return waitForShutdown(rootContext, serverErrors)
	}
	ticker := clock.RealClock{}.NewTicker(cfg.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-rootContext.Done():
			return nil
		case serverErr := <-serverErrors:
			if serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) {
				return errors.New("health server failed")
			}
			return nil
		case <-ticker.C():
			runPass()
		}
	}
}

func newOperatorRecovery(controlPool *pgxpool.Pool, registry *shardphysical.Registry, cfg runtimeConfig) (*operatorcommand.RecoveryService, error) {
	router, err := shardphysical.NewCatalogRouter(controlPool, registry, cfg.routeTTL)
	if err != nil {
		return nil, err
	}
	store, err := operatorpostgres.NewStore(controlPool)
	if err != nil {
		return nil, err
	}
	physicalExecutor, err := commandphysical.NewExecutor(router, commandphysical.Options{MaxHoldTTL: 15 * time.Minute})
	if err != nil {
		return nil, err
	}
	fareResolver, err := app.NewPostgresOperatorCommandFareResolver(controlPool)
	if err != nil {
		return nil, err
	}
	executor, err := app.NewPhysicalOperatorCommandShardExecutor(router, fareResolver, physicalExecutor)
	if err != nil {
		return nil, err
	}
	inspector, err := operatorphysical.NewInspector(router)
	if err != nil {
		return nil, err
	}
	finalizer, err := app.NewPostgresDurableOperatorCommandFinalizer(controlPool)
	if err != nil {
		return nil, err
	}
	batchSize := cfg.batchSize
	if batchSize > operatorcommand.MaxClaimBatch {
		batchSize = operatorcommand.MaxClaimBatch
	}
	return operatorcommand.NewRecoveryService(store, executor, inspector, finalizer, operatorcommand.RecoveryOptions{
		ClaimOptions:   operatorcommand.ClaimOptions{WorkerID: cfg.workerID + ":operator", BatchSize: batchSize, LeaseTTL: cfg.leaseTTL},
		InspectTimeout: cfg.inspectTimeout,
	})
}

type runtimeConfig struct {
	databaseURL      string
	shardZeroURL     string
	shardOneURL      string
	workerID         string
	httpAddress      string
	enabled          bool
	batchSize        int
	controlMaxConns  int
	shardMaxConns    int
	leaseTTL         time.Duration
	inspectTimeout   time.Duration
	pollInterval     time.Duration
	passTimeout      time.Duration
	shutdownTimeout  time.Duration
	readinessTimeout time.Duration
	connectTimeout   time.Duration
	queryTimeout     time.Duration
	routeTTL         time.Duration
}

func loadConfig(lookup func(string) (string, bool)) (runtimeConfig, error) {
	hostname, _ := os.Hostname()
	if strings.TrimSpace(hostname) == "" {
		hostname = "booking-command-reconciler"
	}
	cfg := runtimeConfig{
		workerID: hostname, httpAddress: ":9090", enabled: true, batchSize: 100,
		controlMaxConns: 4, shardMaxConns: 2, leaseTTL: 30 * time.Second,
		inspectTimeout: 3 * time.Second, pollInterval: 2 * time.Second,
		passTimeout: 20 * time.Second, shutdownTimeout: 10 * time.Second,
		readinessTimeout: 3 * time.Second, connectTimeout: 3 * time.Second,
		queryTimeout: 2 * time.Second, routeTTL: 2 * time.Second,
	}
	cfg.databaseURL = env(lookup, "DATABASE_URL", "")
	cfg.shardZeroURL = env(lookup, "BOOKING_SHARD_0_DATABASE_URL", "")
	cfg.shardOneURL = env(lookup, "BOOKING_SHARD_1_DATABASE_URL", "")
	cfg.workerID = env(lookup, "BOOKING_COMMAND_RECONCILER_ID", cfg.workerID)
	cfg.httpAddress = env(lookup, "WORKER_HTTP_ADDRESS", cfg.httpAddress)
	var err error
	if cfg.enabled, err = envBool(lookup, "BOOKING_COMMAND_RECONCILER_ENABLED", cfg.enabled); err != nil {
		return runtimeConfig{}, err
	}
	for _, item := range []struct {
		name   string
		target *int
	}{
		{"BOOKING_COMMAND_RECONCILER_BATCH_SIZE", &cfg.batchSize},
		{"CONTROL_DATABASE_MAX_OPEN_CONNS", &cfg.controlMaxConns},
		{"PHYSICAL_SHARD_MAX_OPEN_CONNS", &cfg.shardMaxConns},
	} {
		if *item.target, err = envInt(lookup, item.name, *item.target); err != nil {
			return runtimeConfig{}, err
		}
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
	}{
		{"BOOKING_COMMAND_RECONCILER_LEASE_TTL", &cfg.leaseTTL},
		{"BOOKING_COMMAND_RECONCILER_INSPECT_TIMEOUT", &cfg.inspectTimeout},
		{"BOOKING_COMMAND_RECONCILER_POLL_INTERVAL", &cfg.pollInterval},
		{"WORKER_PASS_TIMEOUT", &cfg.passTimeout}, {"SHUTDOWN_TIMEOUT", &cfg.shutdownTimeout},
		{"DATABASE_TIMEOUT", &cfg.readinessTimeout}, {"PHYSICAL_SHARD_CONNECT_TIMEOUT", &cfg.connectTimeout},
		{"PHYSICAL_SHARD_QUERY_TIMEOUT", &cfg.queryTimeout},
		{"BOOKING_ROUTE_CACHE_TTL", &cfg.routeTTL},
	} {
		if *item.target, err = envDuration(lookup, item.name, *item.target); err != nil {
			return runtimeConfig{}, err
		}
	}
	if cfg.databaseURL == "" || cfg.shardZeroURL == "" || cfg.shardOneURL == "" ||
		cfg.shardZeroURL == cfg.shardOneURL || cfg.httpAddress == "" || cfg.batchSize < 1 || cfg.batchSize > 500 ||
		cfg.controlMaxConns < 1 || cfg.controlMaxConns > 20 || cfg.shardMaxConns < 1 || cfg.shardMaxConns > 10 ||
		cfg.leaseTTL <= 0 || cfg.leaseTTL > 5*time.Minute || cfg.inspectTimeout <= 0 ||
		cfg.inspectTimeout >= cfg.leaseTTL || cfg.inspectTimeout > 10*time.Second || cfg.pollInterval <= 0 ||
		cfg.passTimeout <= 0 || cfg.passTimeout > cfg.leaseTTL || cfg.shutdownTimeout <= 0 ||
		cfg.readinessTimeout <= 0 || cfg.readinessTimeout > 10*time.Second || cfg.connectTimeout <= 0 ||
		cfg.connectTimeout > 10*time.Second || cfg.queryTimeout <= 0 || cfg.queryTimeout > 30*time.Second ||
		cfg.routeTTL <= 0 || cfg.routeTTL > 5*time.Minute {
		return runtimeConfig{}, errors.New("bounded configuration invalid")
	}
	return cfg, nil
}

func env(lookup func(string) (string, bool), name, fallback string) string {
	if value, ok := lookup(name); ok {
		return strings.TrimSpace(value)
	}
	return fallback
}

func envBool(lookup func(string) (string, bool), name string, fallback bool) (bool, error) {
	value, ok := lookup(name)
	if !ok {
		return fallback, nil
	}
	return strconv.ParseBool(strings.TrimSpace(value))
}

func envInt(lookup func(string) (string, bool), name string, fallback int) (int, error) {
	value, ok := lookup(name)
	if !ok {
		return fallback, nil
	}
	return strconv.Atoi(strings.TrimSpace(value))
}

func envDuration(lookup func(string) (string, bool), name string, fallback time.Duration) (time.Duration, error) {
	value, ok := lookup(name)
	if !ok {
		return fallback, nil
	}
	return time.ParseDuration(strings.TrimSpace(value))
}

type workerMetrics struct {
	registry    *prometheus.Registry
	passes      *prometheus.CounterVec
	outcomes    *prometheus.CounterVec
	duration    prometheus.Histogram
	lastSuccess prometheus.Gauge
}

func newMetrics() (*workerMetrics, error) {
	metrics := &workerMetrics{
		registry: prometheus.NewRegistry(),
		passes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "booking_command_reconciliation_pass_total", Help: "Reconciliation passes by bounded result.",
		}, []string{"result"}),
		outcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "booking_command_reconciliation_outcome_total", Help: "Command reconciliation outcomes by bounded result.",
		}, []string{"outcome"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "booking_command_reconciliation_pass_duration_seconds", Help: "Reconciliation pass duration.",
		}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "booking_command_reconciliation_last_success_unixtime", Help: "Last successful pass Unix timestamp.",
		}),
	}
	if err := metrics.registry.Register(metrics.passes); err != nil {
		return nil, err
	}
	if err := metrics.registry.Register(metrics.outcomes); err != nil {
		return nil, err
	}
	if err := metrics.registry.Register(metrics.duration); err != nil {
		return nil, err
	}
	if err := metrics.registry.Register(metrics.lastSuccess); err != nil {
		return nil, err
	}
	return metrics, nil
}

func (metrics *workerMetrics) record(result reconcile.Result, err error, duration time.Duration) {
	passResult := "success"
	if err != nil {
		passResult = "failure"
	} else {
		metrics.lastSuccess.SetToCurrentTime()
	}
	metrics.passes.WithLabelValues(passResult).Inc()
	metrics.duration.Observe(duration.Seconds())
	for outcome, count := range map[string]int{
		"finalized": result.Finalized, "failed": result.Failed, "expired": result.Expired,
		"deferred": result.Deferred, "inspection_failure": result.Failures,
	} {
		metrics.outcomes.WithLabelValues(outcome).Add(float64(count))
	}
}

func (metrics *workerMetrics) recordOperator(result operatorcommand.RecoveryResult) {
	for outcome, count := range map[string]int{
		"operator_finalized": result.Finalized, "operator_deferred": result.Deferred,
		"operator_failed": result.Failed, "operator_failure": result.Failures,
	} {
		metrics.outcomes.WithLabelValues(outcome).Add(float64(count))
	}
}

func reconcilerReadiness(
	control *pgxpool.Pool,
	shards map[string]shardphysical.Pool,
	cfg runtimeConfig,
) workerhttp.ReadinessCheck {
	return func(ctx context.Context) error {
		if control == nil || len(shards) != 2 {
			return errors.New("dependency unavailable")
		}
		if err := control.Ping(ctx); err != nil {
			return errors.New("dependency unavailable")
		}
		var version int
		var dirty bool
		if err := control.QueryRow(ctx, "SELECT version, dirty FROM schema_migrations LIMIT 1").Scan(&version, &dirty); err != nil ||
			version != controlSchemaVersion || dirty {
			return errors.New("migration unavailable")
		}
		for _, shardID := range []string{"physical-shard-0", "physical-shard-1"} {
			tx, err := shards[shardID].BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
			if err != nil {
				return errors.New("dependency unavailable")
			}
			var shardVersion int
			var shardDirty bool
			err = tx.QueryRow(ctx, "SELECT version, dirty FROM public.schema_migrations LIMIT 1").Scan(&shardVersion, &shardDirty)
			if err == nil {
				err = tx.Commit(ctx)
			} else {
				_ = tx.Rollback(context.WithoutCancel(ctx))
			}
			if err != nil || shardVersion != int(shardphysical.SupportedSchemaVersion) || shardDirty {
				return errors.New("migration unavailable")
			}
		}
		if cfg.databaseURL == "" || cfg.workerID == "" {
			return errors.New("configuration unavailable")
		}
		return nil
	}
}

func waitForShutdown(ctx context.Context, serverErrors <-chan error) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return errors.New("health server failed")
		}
		return nil
	}
}

func shutdownHTTP(server *http.Server, timeout time.Duration) {
	if server == nil || timeout <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		_ = server.Close()
	}
}

func publicReason(err error) string {
	if err == nil {
		return "none"
	}
	for _, allowed := range []string{
		"runtime unavailable", "configuration invalid", "control postgres configuration invalid",
		"control postgres unavailable", "physical shard registry unavailable", "physical shard resolver unavailable",
		"control store unavailable", "shard inspector unavailable", "reconciler unavailable", "metrics unavailable",
		"operator command reconciler unavailable",
		"health server invalid", "health listener unavailable", "health server failed",
	} {
		if err.Error() == allowed {
			return allowed
		}
	}
	return "worker failure"
}
