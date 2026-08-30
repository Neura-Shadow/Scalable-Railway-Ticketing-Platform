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

	paymentprovider "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	providerhttp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/httpclient"
	providerstripe "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/stripe"
	paymentrefund "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/refund"
	refundpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/refund/postgres"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	paymentshardpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard/postgres"
	paymentworker "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker"
	paymentworkerpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerhttp"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	authoritypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

const paymentControlSchemaVersion = 11

const databasePoolObservationInterval = time.Second

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
	pool, err := postgresx.NewRegionalBoundedPool(rootContext, cfg.DatabaseURL, cfg.ControlDatabaseMaxOpenConns, regionalSession(cfg))
	if err != nil {
		return errors.New("control database unavailable")
	}
	defer pool.Close()

	deployment, err := regionalDeployment(cfg)
	if err != nil {
		return errors.New("regional deployment invalid")
	}
	controlStore, err := paymentworkerpostgres.NewWithRegionalAuthority(pool, deployment)
	if err != nil {
		return errors.New("payment store initialization failed")
	}
	providerClient, err := newPaymentProvider(cfg)
	if err != nil {
		return errors.New("payment provider initialization failed")
	}
	if err := providerClient.Descriptor().Require(paymentprovider.SagaCapabilities()); err != nil {
		return errors.New("payment provider capability contract unsupported")
	}
	providerDescriptor := providerClient.Descriptor()
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
	shardStore, err := paymentshardpostgres.NewStore(physicalRouter, paymentshardpostgres.WithRegionalAuthority(deployment))
	if err != nil {
		return errors.New("payment shard store initialization failed")
	}
	shardGateway, err := paymentshard.NewGateway(directory, shardStore, paymentshard.WithTicketCodeClaimer(controlStore))
	if err != nil {
		return errors.New("payment shard gateway initialization failed")
	}
	workerShardGateway := paymentworker.ShardGateway(shardGateway)
	if testFault := newTestTicketIssueConflict(shardGateway, os.LookupEnv); testFault != nil {
		workerShardGateway = testFault
	}

	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.NewEventMetrics(registry)
	if err != nil {
		return errors.New("metrics initialization failed")
	}
	stopDatabasePoolObserver := startDatabasePoolObserver(
		rootContext, metrics, pool, physicalRegistry, databasePoolObservationInterval,
	)
	defer stopDatabasePoolObserver()
	workerID := "payment-" + uuid.NewString()
	testCrash := newTestExternalEffectCrash(os.LookupEnv, os.Exit)
	paymentWorker, err := paymentworker.New(
		controlStore,
		paymentworker.Providers{string(cfg.PaymentProviderType): providerClient},
		workerShardGateway,
		paymentMetrics{metrics: metrics},
		paymentworker.Config{
			WorkerID: workerID, BatchSize: cfg.PaymentWorkerBatchSize,
			MaxAttempts: cfg.PaymentWorkerMaxAttempts, LeaseTTL: cfg.PaymentWorkerLease,
			RetryBase: cfg.PaymentWorkerRetryBase, RetryMax: retryMaximum(cfg.PaymentWorkerRetryBase),
			MaxUncertain: cfg.PaymentMaxUncertain,
			Interval:     cfg.PaymentWorkerInterval, Now: time.Now,
			TestAfterExternalEffect: testCrash.payment,
		},
	)
	if err != nil {
		return errors.New("payment worker initialization failed")
	}
	refundStore, err := refundpostgres.NewStore(pool, shardGateway, refundpostgres.Config{
		PartialRefundProviders: map[string]bool{providerDescriptor.Name: providerDescriptor.Capabilities.PartialRefund},
		RegionalAuthority:      &deployment,
	})
	if err != nil {
		return errors.New("ticket refund store initialization failed")
	}
	refundProcessor, err := paymentrefund.NewProcessor(
		refundStore,
		paymentrefund.Providers{string(cfg.PaymentProviderType): providerClient},
		shardGateway,
		paymentrefund.ProcessorConfig{
			WorkerID: workerID, BatchSize: cfg.PaymentWorkerBatchSize,
			MaxAttempts: cfg.PaymentWorkerMaxAttempts, LeaseTTL: cfg.PaymentWorkerLease,
			RetryBase: cfg.PaymentWorkerRetryBase, RetryMax: retryMaximum(cfg.PaymentWorkerRetryBase),
			Region: string(cfg.DeploymentRegion), RegionalEpoch: cfg.RegionEpoch, Now: time.Now,
			TestAfterExternalEffect: testCrash.refund,
		},
	)
	if err != nil {
		return errors.New("ticket refund worker initialization failed")
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
		paymentContext, cancelPayment := context.WithTimeout(rootContext, cfg.WorkerPassTimeout)
		result, runErr := paymentWorker.RunOnce(paymentContext)
		cancelPayment()
		refundStarted := time.Now()
		refundContext, cancelRefund := context.WithTimeout(rootContext, cfg.WorkerPassTimeout)
		refundResult, refundErr := refundProcessor.RunOnce(refundContext)
		cancelRefund()
		for _, observation := range refundResult.Observations {
			metrics.RecordPartialRefund(observation.Provider, observation.Result, observation.Reason, observation.Currency, 0, time.Since(refundStarted))
		}
		if runErr != nil {
			logPaymentPassFailure(logger, result, runErr)
		}
		if refundErr != nil {
			logger.Error("ticket refund pass completed with isolated failures",
				"claimed", refundResult.Claimed, "completed", refundResult.Completed,
				"retried", refundResult.Retried, "manual_review", refundResult.ManualReview,
				"failure_count", refundResult.Failures)
		}
		if runErr != nil || refundErr != nil {
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
			"manual_review", result.ManualReview,
			"ticket_refunds_claimed", refundResult.Claimed,
			"ticket_refunds_completed", refundResult.Completed)
	}
	if paymentPassEnabled(cfg) && paymentAuthorityReady(rootContext, pool, physicalRegistry, cfg) == nil {
		runPass()
	} else {
		logger.Info("payment worker retained without claim authority", "deployment_role", cfg.DeploymentRole)
	}
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
			if paymentPassEnabled(cfg) && paymentAuthorityReady(rootContext, pool, physicalRegistry, cfg) == nil {
				runPass()
			}
		}
	}
}

func logPaymentPassFailure(logger *slog.Logger, result paymentworker.Result, _ error) {
	lanes := make([]string, 0, len(result.FailureSummaries))
	reasons := make([]string, 0, len(result.FailureSummaries))
	for _, failure := range result.FailureSummaries {
		lanes = append(lanes, string(failure.Lane))
		reasons = append(reasons, string(failure.Reason))
	}
	logger.Error("payment pass completed with isolated failures",
		"operations_claimed", result.OperationsClaimed,
		"webhooks_claimed", result.WebhooksClaimed,
		"actions_claimed", result.ActionsClaimed,
		"failure_count", result.Failures,
		"failure_lanes", lanes,
		"failure_reasons", reasons)
}

type testExternalEffectCrash struct {
	point  string
	target uuid.UUID
	exit   func(int)
}

// testTicketIssueConflict is a deterministic, target-specific test hook used
// to drive the real full-refund compensation state machine. It is impossible
// to enable outside APP_ENV=test and delegates every non-target operation.
type testTicketIssueConflict struct {
	next   paymentworker.ShardGateway
	target uuid.UUID
}

func newTestTicketIssueConflict(next paymentworker.ShardGateway, lookup func(string) (string, bool)) *testTicketIssueConflict {
	if next == nil || lookup == nil {
		return nil
	}
	environment, _ := lookup("APP_ENV")
	enabled, _ := lookup("PAYMENT_WORKER_TEST_TICKET_ISSUE_CONFLICT_ENABLED")
	targetText, _ := lookup("PAYMENT_WORKER_TEST_TICKET_ISSUE_CONFLICT_TARGET_ID")
	target, err := uuid.Parse(targetText)
	if environment != "test" || enabled != "true" || err != nil || target == uuid.Nil {
		return nil
	}
	return &testTicketIssueConflict{next: next, target: target}
}

func (fault *testTicketIssueConflict) IssueTickets(ctx context.Context, command paymentshard.IssueTicketsCommand) (paymentshard.IssueTicketsReceipt, error) {
	if fault != nil && command.PaymentIntentID == fault.target {
		return paymentshard.IssueTicketsReceipt{}, paymentshard.ErrTicketClaimConflict
	}
	return fault.next.IssueTickets(ctx, command)
}

func (fault *testTicketIssueConflict) MarkRefundPending(ctx context.Context, command paymentshard.MarkRefundPendingCommand) (paymentshard.MarkRefundPendingReceipt, error) {
	return fault.next.MarkRefundPending(ctx, command)
}

func (fault *testTicketIssueConflict) CancelVoidedReservation(ctx context.Context, command paymentshard.CancelVoidedReservationCommand) (paymentshard.CancelVoidedReservationReceipt, error) {
	return fault.next.CancelVoidedReservation(ctx, command)
}

func (fault *testTicketIssueConflict) ApplyRefundCompensation(ctx context.Context, command paymentshard.ApplyRefundCompensationCommand) (paymentshard.ApplyRefundCompensationReceipt, error) {
	return fault.next.ApplyRefundCompensation(ctx, command)
}

func newTestExternalEffectCrash(lookup func(string) (string, bool), exit func(int)) *testExternalEffectCrash {
	disabled := &testExternalEffectCrash{}
	if lookup == nil || exit == nil {
		return disabled
	}
	appEnv, _ := lookup("APP_ENV")
	enabled, _ := lookup("PAYMENT_WORKER_TEST_CRASH_AFTER_EFFECT_ENABLED")
	point, _ := lookup("PAYMENT_WORKER_TEST_CRASH_AFTER_EFFECT_POINT")
	targetText, _ := lookup("PAYMENT_WORKER_TEST_CRASH_TARGET_ID")
	if appEnv != "test" || enabled != "true" {
		return disabled
	}
	allowed := map[string]bool{
		string(paymentworker.ExternalEffectCaptureCommitted):        true,
		string(paymentworker.ExternalEffectTicketsCommitted):        true,
		string(paymentworker.ExternalEffectRefundCommitted):         true,
		string(paymentworker.ExternalEffectCompensationCommitted):   true,
		string(paymentrefund.ExternalEffectProviderRefundCommitted): true,
		string(paymentrefund.ExternalEffectShardRefundCommitted):    true,
	}
	target, err := uuid.Parse(targetText)
	if !allowed[point] || err != nil || target == uuid.Nil {
		return disabled
	}
	return &testExternalEffectCrash{point: point, target: target, exit: exit}
}

func (crash *testExternalEffectCrash) trigger(point string, target uuid.UUID) {
	if crash != nil && crash.exit != nil && crash.point == point && crash.target == target {
		crash.exit(86)
	}
}

func (crash *testExternalEffectCrash) payment(point paymentworker.ExternalEffectPoint, target uuid.UUID) {
	crash.trigger(string(point), target)
}

func (crash *testExternalEffectCrash) refund(point paymentrefund.ExternalEffectPoint, target uuid.UUID) {
	crash.trigger(string(point), target)
}

func paymentPassEnabled(cfg config.Config) bool {
	return cfg.DeploymentRole == config.DeploymentRoleActive && cfg.RegionalWritesEnabled
}

func regionalSession(cfg config.Config) postgresx.RegionalSession {
	return postgresx.RegionalSession{
		Region: string(cfg.DeploymentRegion), Role: string(cfg.DeploymentRole),
		Epoch: cfg.RegionEpoch, WritesEnabled: cfg.RegionalWritesEnabled,
	}
}

func regionalDeployment(cfg config.Config) (authority.Deployment, error) {
	region, err := authority.ParseRegion(string(cfg.DeploymentRegion))
	if err != nil || cfg.RegionEpoch <= 0 {
		return authority.Deployment{}, authority.ErrInvalidDeployment
	}
	epoch, err := authority.NewEpoch(uint64(cfg.RegionEpoch))
	if err != nil {
		return authority.Deployment{}, err
	}
	return authority.NewDeployment(region, authority.Role(cfg.DeploymentRole), epoch, cfg.RegionalWritesEnabled)
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
	}, shardphysical.RegionalPGXPoolFactory(regionalSession(cfg)))
}

type paymentProviderRuntime interface {
	paymentprovider.Client
	paymentprovider.Described
	paymentprovider.RefundLookupReader
	Ready(context.Context) error
	CloseIdleConnections()
}

func newPaymentProvider(cfg config.Config) (paymentProviderRuntime, error) {
	switch cfg.PaymentProviderType {
	case config.PaymentProviderSandbox:
		return providerhttp.New(providerhttp.Config{
			BaseURL: cfg.PaymentProviderBaseURL, APIKey: cfg.PaymentProviderAPIKey,
			ConnectTimeout: cfg.PaymentProviderConnectTimeout, RequestTimeout: cfg.PaymentProviderRequestTimeout,
			MaxResponseBytes: int64(cfg.PaymentProviderMaxResponseBytes), Now: time.Now,
		})
	case config.PaymentProviderStripe:
		return providerstripe.New(providerstripe.Config{
			SecretKey: cfg.PaymentProviderAPIKey, AccountID: cfg.PaymentProviderAccountID,
			APIOrigin: cfg.PaymentProviderBaseURL, SuccessURL: cfg.PaymentProviderSuccessURL,
			CancelURL: cfg.PaymentProviderCancelURL, ConnectTimeout: cfg.PaymentProviderConnectTimeout,
			RequestTimeout:       cfg.PaymentProviderRequestTimeout,
			MaxResponseBodyBytes: int64(cfg.PaymentProviderMaxResponseBytes), Now: time.Now,
			AllowInsecureForTest: cfg.Environment == config.EnvironmentTest,
		})
	default:
		return nil, errors.New("payment provider type unsupported")
	}
}

func paymentReadiness(pool *pgxpool.Pool, registry *shardphysical.Registry, providerClient paymentProviderRuntime, cfg config.Config) workerhttp.ReadinessCheck {
	return func(ctx context.Context) error {
		if pool == nil || registry == nil || providerClient == nil || cfg.ValidateFor(config.ProcessPaymentWorker) != nil {
			return errors.New("worker dependency unavailable")
		}
		if err := pool.Ping(ctx); err != nil {
			return errors.New("worker dependency unavailable")
		}
		if err := paymentAuthorityReady(ctx, pool, registry, cfg); err != nil {
			return errors.New("worker regional authority unavailable")
		}
		var version int
		var dirty bool
		if err := pool.QueryRow(ctx, `SELECT version,dirty FROM public.schema_migrations LIMIT 1`).Scan(&version, &dirty); err != nil || version != paymentControlSchemaVersion || dirty {
			return errors.New("worker migration unavailable")
		}
		if err := providerClient.Ready(ctx); err != nil {
			return errors.New("worker provider unavailable")
		}
		return nil
	}
}

func paymentAuthorityReady(ctx context.Context, pool *pgxpool.Pool, registry *shardphysical.Registry, cfg config.Config) error {
	deployment, err := regionalDeployment(cfg)
	if err != nil || !paymentPassEnabled(cfg) || pool == nil || registry == nil {
		return errors.New("worker regional authority unavailable")
	}
	if err := authoritypostgres.CheckActiveReadiness(ctx, pool, deployment); err != nil {
		return errors.New("worker regional authority unavailable")
	}
	shardIDs := cfg.BookingShardIDs
	if cfg.DRRequiredDatabaseCount == 3 {
		shardIDs = []string{"physical-shard-0", "physical-shard-1"}
	}
	for _, rawShardID := range shardIDs {
		if err := physicalShardReady(ctx, pool, registry, rawShardID, deployment); err != nil {
			return errors.New("worker regional authority unavailable")
		}
	}
	return nil
}

func physicalShardReady(ctx context.Context, pool *pgxpool.Pool, registry *shardphysical.Registry, rawShardID string, deployment authority.Deployment) error {
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
	if err := authoritypostgres.CheckActiveReadiness(ctx, tx, deployment); err != nil {
		return errors.New("physical shard authority unavailable")
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
	case "failure":
		metrics.metrics.RecordPaymentWorkerLaneFailure(observation.Operation, observation.Reason)
	case "operation":
		metrics.metrics.RecordPaymentOperationWithReason(observation.Provider, observation.Operation, observation.Result, observation.Reason, observation.Duration, observation.Uncertain)
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
