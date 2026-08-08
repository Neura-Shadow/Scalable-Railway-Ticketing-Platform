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

	providerhttp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/httpclient"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	paymentshardpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard/postgres"
	paymentworker "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker"
	paymentworkerpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerhttp"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

const paymentControlSchemaVersion = 10

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("payment worker stopped", "reason", publicReason(err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	if logger == nil {
		return errors.New("logger unavailable")
	}
	cfg, err := config.LoadFor(config.ProcessPaymentWorker)
	if err != nil {
		return errors.New("configuration invalid")
	}
	if cfg.BookingShardMode != config.BookingShardModePhysical {
		return errors.New("physical payment shard mode required")
	}
	rootContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := postgresx.NewBoundedPool(rootContext, cfg.DatabaseURL, cfg.ControlDatabaseMaxOpenConns)
	if err != nil {
		return errors.New("control database unavailable")
	}
	defer pool.Close()

	controlStore, err := paymentworkerpostgres.New(pool)
	if err != nil {
		return errors.New("payment store initialization failed")
	}
	webhookKeys, err := cfg.ParsePaymentWebhookKeys()
	if err != nil {
		return errors.New("payment provider initialization failed")
	}
	providerClient, err := providerhttp.New(providerhttp.Config{
		BaseURL: cfg.PaymentProviderBaseURL, APIKey: cfg.PaymentProviderAPIKey,
		ConnectTimeout: cfg.PaymentProviderConnectTimeout, RequestTimeout: cfg.PaymentProviderRequestTimeout,
		MaxResponseBytes:    int64(cfg.PaymentProviderMaxResponseBytes),
		MaxWebhookBodyBytes: int64(cfg.PaymentWebhookMaxBodyBytes), WebhookKeys: webhookKeys,
		WebhookClockSkew: cfg.PaymentWebhookClockSkew, Now: time.Now,
	})
	if err != nil {
		return errors.New("payment provider initialization failed")
	}
	defer providerClient.CloseIdleConnections()

	physicalRegistry, err := newPhysicalRegistry(rootContext, cfg)
	if err != nil {
		return errors.New("physical shard registry initialization failed")
	}
	defer physicalRegistry.Close()
	physicalRouter, err := shardphysical.NewCatalogRouter(pool, physicalRegistry, cfg.BookingRouteCacheTTL)
	if err != nil {
		return errors.New("physical shard router initialization failed")
	}
	directory, err := paymentshardpostgres.NewDirectory(pool)
	if err != nil {
		return errors.New("payment directory initialization failed")
	}
	shardStore, err := paymentshardpostgres.NewStore(physicalRouter)
	if err != nil {
		return errors.New("payment shard store initialization failed")
	}
	shardGateway, err := paymentshard.NewGateway(directory, shardStore)
	if err != nil {
		return errors.New("payment shard gateway initialization failed")
	}

	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		return errors.New("metrics initialization failed")
	}
	workerID := "payment-" + uuid.NewString()
	paymentWorker, err := paymentworker.New(
		controlStore,
		paymentworker.Providers{"sandbox": providerClient},
		shardGateway,
		paymentMetrics{metrics: metrics},
		paymentworker.Config{
			WorkerID: workerID, BatchSize: cfg.PaymentWorkerBatchSize,
			MaxAttempts: cfg.PaymentWorkerMaxAttempts, LeaseTTL: cfg.PaymentWorkerLease,
			RetryBase: cfg.PaymentWorkerRetryBase, RetryMax: retryMaximum(cfg.PaymentWorkerRetryBase),
			Interval: cfg.PaymentWorkerInterval, Now: time.Now,
		},
	)
	if err != nil {
		return errors.New("payment worker initialization failed")
	}

	readiness := paymentReadiness(pool, physicalRegistry, providerClient, cfg)
	healthServer, err := workerhttp.New(cfg.WorkerHTTPAddress, registry, readiness, readinessTimeout(cfg))
	if err != nil {
		return errors.New("health server invalid")
	}
	listener, err := net.Listen("tcp", cfg.WorkerHTTPAddress)
	if err != nil {
		return errors.New("health listener unavailable")
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- healthServer.Serve(listener) }()
	defer shutdownHealthServer(healthServer, cfg.ShutdownTimeout)

	runPass := func() {
		passContext, cancel := context.WithTimeout(rootContext, cfg.WorkerPassTimeout)
		defer cancel()
		result, runErr := paymentWorker.RunOnce(passContext)
		if runErr != nil {
			logger.Error("payment pass completed with isolated failures",
				"operations_claimed", result.OperationsClaimed,
				"webhooks_claimed", result.WebhooksClaimed,
				"actions_claimed", result.ActionsClaimed,
				"failure_count", result.Failures)
			return
		}
		logger.Info("payment pass complete",
			"operations_claimed", result.OperationsClaimed,
			"operations_done", result.OperationsDone,
			"webhooks_claimed", result.WebhooksClaimed,
			"webhooks_done", result.WebhooksDone,
			"actions_claimed", result.ActionsClaimed,
			"actions_done", result.ActionsDone,
			"retried", result.Retried,
			"compensating", result.Compensating,
			"manual_review", result.ManualReview)
	}
	runPass()
	ticker := time.NewTicker(cfg.PaymentWorkerInterval)
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
		case <-ticker.C:
			runPass()
		}
	}
}

func newPhysicalRegistry(ctx context.Context, cfg config.Config) (*shardphysical.Registry, error) {
	connections := make(map[string]shardphysical.ConnectionConfig, len(cfg.PhysicalShardConnections))
	for reference, dsn := range cfg.PhysicalShardConnections {
		shardID, err := sharding.ParseShardID(reference)
		if err != nil {
			return nil, errors.New("invalid physical shard configuration")
		}
		connections[reference] = shardphysical.ConnectionConfig{ShardID: shardID, DSN: dsn}
	}
	return shardphysical.NewRegistry(ctx, shardphysical.RegistryConfig{
		Connections: connections,
		MaxCount:    cfg.PhysicalShardMaxCount,
		Limits: shardphysical.PoolLimits{
			MaxOpenConns: cfg.PhysicalShardMaxOpenConns, MaxIdleConns: cfg.PhysicalShardMaxIdleConns,
			MaxLifetime: cfg.PhysicalShardConnMaxLifetime, MaxIdleTime: cfg.PhysicalShardConnMaxIdleTime,
			ConnectTimeout:   cfg.PhysicalShardConnectTimeout,
			StatementTimeout: cfg.PhysicalShardQueryTimeout, LockTimeout: cfg.PhysicalShardQueryTimeout,
		},
	}, shardphysical.OpenPGXPool)
}

func paymentReadiness(pool *pgxpool.Pool, registry *shardphysical.Registry, providerClient *providerhttp.Client, cfg config.Config) workerhttp.ReadinessCheck {
	return func(ctx context.Context) error {
		if pool == nil || registry == nil || providerClient == nil || cfg.ValidateFor(config.ProcessPaymentWorker) != nil {
			return errors.New("worker dependency unavailable")
		}
		if err := pool.Ping(ctx); err != nil {
			return errors.New("worker dependency unavailable")
		}
		var version int
		var dirty bool
		if err := pool.QueryRow(ctx, `SELECT version,dirty FROM public.schema_migrations LIMIT 1`).Scan(&version, &dirty); err != nil || version != paymentControlSchemaVersion || dirty {
			return errors.New("worker migration unavailable")
		}
		if err := providerClient.Ready(ctx); err != nil {
			return errors.New("worker provider unavailable")
		}
		for _, rawShardID := range cfg.BookingShardIDs {
			if err := physicalShardReady(ctx, pool, registry, rawShardID); err != nil {
				return errors.New("worker physical shard unavailable")
			}
		}
		return nil
	}
}

func physicalShardReady(ctx context.Context, pool *pgxpool.Pool, registry *shardphysical.Registry, rawShardID string) error {
	var (
		catalogShardID, storageKind, connectionRef, healthState, state string
		protocolVersion, schemaVersion                                 int32
		enabled, writeEnabled                                          bool
	)
	err := pool.QueryRow(ctx, `SELECT shard_id,storage_kind,connection_ref,protocol_version,
 schema_version,enabled,write_enabled,health_state,state FROM public.booking_shards
WHERE shard_id=$1`, rawShardID).Scan(&catalogShardID, &storageKind, &connectionRef,
		&protocolVersion, &schemaVersion, &enabled, &writeEnabled, &healthState, &state)
	if err != nil || catalogShardID != rawShardID {
		return errors.New("physical shard unavailable")
	}
	shardID, err := sharding.ParseShardID(catalogShardID)
	if err != nil {
		return errors.New("physical shard unavailable")
	}
	handle, err := registry.Resolve(shardphysical.CatalogEntry{
		ShardID: shardID, StorageKind: shardphysical.StorageKind(storageKind), ConnectionRef: connectionRef,
		ProtocolVersion: protocolVersion, SchemaVersion: schemaVersion, Enabled: enabled,
		WriteEnabled: writeEnabled, HealthState: shardphysical.HealthState(healthState),
		State: shardphysical.CatalogState(state),
	})
	if err != nil {
		return errors.New("physical shard unavailable")
	}
	tx, err := handle.Pool().BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return errors.New("physical shard unavailable")
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var shardDirty bool
	if err := tx.QueryRow(ctx, `SELECT version,dirty FROM public.schema_migrations LIMIT 1`).Scan(&schemaVersion, &shardDirty); err != nil || schemaVersion != shardphysical.SupportedSchemaVersion || shardDirty {
		return errors.New("physical shard migration unavailable")
	}
	return tx.Commit(ctx)
}

func retryMaximum(base time.Duration) time.Duration {
	const maximum = time.Hour
	if base <= 0 || base >= maximum {
		return maximum
	}
	if base > maximum/32 {
		return maximum
	}
	return base * 32
}

func readinessTimeout(cfg config.Config) time.Duration {
	timeout := cfg.DatabaseTimeout
	for _, candidate := range []time.Duration{cfg.PaymentProviderRequestTimeout, cfg.PhysicalShardQueryTimeout} {
		if candidate > timeout {
			timeout = candidate
		}
	}
	if timeout <= 0 || timeout > 10*time.Second {
		return 2 * time.Second
	}
	return timeout
}

func shutdownHealthServer(server *http.Server, timeout time.Duration) {
	if server == nil || timeout <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		_ = server.Close()
	}
}

type paymentMetrics struct{ metrics *platformmetrics.Metrics }

func (metrics paymentMetrics) RecordPaymentWorker(observation paymentworker.MetricObservation) {
	if metrics.metrics == nil {
		return
	}
	switch observation.Lane {
	case "operation":
		metrics.metrics.RecordPaymentOperation(observation.Provider, observation.Operation, observation.Result, observation.Duration, observation.Uncertain)
		if observation.Result == "success" {
			recordOperationTransitions(metrics.metrics, observation.Operation)
		} else if observation.Result == "superseded" {
			metrics.metrics.RecordPaymentSagaTransition("compensating", "refunding")
		}
		if observation.Result == "manual_review" {
			metrics.metrics.RecordPaymentSagaFailure(observation.Reason, true)
		}
	case "webhook":
		metrics.metrics.RecordPaymentWebhook(observation.Provider, observation.Operation, observation.Result, observation.Duration, observation.Lag)
	case "action":
		if observation.Operation == string(paymentworker.ActionIssueTickets) {
			metrics.metrics.RecordTicketIssuance(observation.Result, observation.Reason, observation.Duration, observation.Replay)
		}
		if observation.Result == "success" {
			recordActionTransition(metrics.metrics, observation.Operation)
		} else if observation.Result == "failure" && observation.Operation == string(paymentworker.ActionIssueTickets) {
			metrics.metrics.RecordPaymentSagaFailure(observation.Reason, false)
			metrics.metrics.RecordPaymentSagaTransition("issuing_tickets", "compensating")
		} else if observation.Result == "manual_review" {
			metrics.metrics.RecordPaymentSagaFailure(observation.Reason, true)
		}
	}
}

func recordOperationTransitions(metrics *platformmetrics.Metrics, operation string) {
	switch operation {
	case "create_checkout":
		metrics.RecordPaymentSagaTransition("reservation_secured", "checkout_created")
		metrics.RecordPaymentSagaTransition("checkout_created", "awaiting_provider")
	case "authorize":
		metrics.RecordPaymentSagaTransition("awaiting_provider", "authorized")
	case "capture":
		metrics.RecordPaymentSagaTransition("capturing", "captured")
		metrics.RecordPaymentSagaTransition("captured", "issuing_tickets")
	}
}

func recordActionTransition(metrics *platformmetrics.Metrics, action string) {
	switch action {
	case string(paymentworker.ActionIssueTickets):
		metrics.RecordPaymentSagaTransition("issuing_tickets", "completed")
	case string(paymentworker.ActionMarkRefundPending):
		metrics.RecordPaymentSagaTransition("compensating", "refunding")
	case string(paymentworker.ActionCancelVoided):
		metrics.RecordPaymentSagaTransition("compensating", "compensated")
	case string(paymentworker.ActionCompensate):
		metrics.RecordPaymentSagaTransition("refunding", "compensated")
	}
}

func publicReason(err error) string {
	if err == nil {
		return "none"
	}
	switch err.Error() {
	case "logger unavailable", "configuration invalid", "physical payment shard mode required",
		"control database unavailable", "payment store initialization failed",
		"payment provider initialization failed", "physical shard registry initialization failed",
		"physical shard router initialization failed", "payment directory initialization failed",
		"payment shard store initialization failed", "payment shard gateway initialization failed",
		"metrics initialization failed", "payment worker initialization failed",
		"health server invalid", "health listener unavailable", "health server failed":
		return err.Error()
	default:
		return "payment worker failure"
	}
}
