package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	paymentprovider "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	providerhttp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/httpclient"
	providerstripe "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/stripe"
	paymentreconcile "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/reconcile"
	reconcilepostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/reconcile/postgres"
	platformconfig "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerhttp"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultBatchSize = 100
	defaultInterval  = 15 * time.Second
	defaultTimeout   = 30 * time.Second
	maxInterval      = 10 * time.Minute
	maxTimeout       = 5 * time.Minute
)

var (
	errArguments     = errors.New("invalid arguments")
	errRuntimeWiring = errors.New("payment reconciliation runtime wiring unavailable")
)

type config struct {
	Scope     paymentreconcile.Scope
	BatchSize int
	Interval  time.Duration
	Timeout   time.Duration
	Once      bool
}

type runner interface {
	ReconcileAll(context.Context, paymentreconcile.Options) (paymentreconcile.Result, error)
}

type runnerFactory func(context.Context, func(string) (string, bool), config) (runner, func(), error)

type envelope struct {
	Status         string                 `json:"status"`
	Scope          paymentreconcile.Scope `json:"scope"`
	ReadOnly       bool                   `json:"read_only"`
	RowsExamined   int                    `json:"rows_examined"`
	ShardRowsFound int                    `json:"shard_rows_found"`
	IssuedOrders   int                    `json:"issued_orders"`
	MismatchCount  int                    `json:"mismatch_count"`
	ManualReviews  int                    `json:"manual_reviews"`
	Truncated      bool                   `json:"truncated"`
	Error          string                 `json:"error,omitempty"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr, openRunner))
}

func run(parent context.Context, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer, factory runnerFactory) int {
	if parent == nil || lookup == nil || stdout == nil || stderr == nil || factory == nil {
		return 2
	}
	cfg, err := parse(args, lookup)
	if err != nil {
		_ = writeEnvelope(stdout, envelope{Status: "rejected", Scope: paymentreconcile.ScopeAll, ReadOnly: true, Error: "invalid_arguments"})
		fmt.Fprintln(stderr, "payment-reconciler: arguments rejected")
		return 2
	}
	service, closeService, err := factory(parent, lookup, cfg)
	if err != nil {
		_ = writeEnvelope(stdout, envelope{Status: "failed", Scope: cfg.Scope, ReadOnly: true, Error: "startup_failed"})
		fmt.Fprintln(stderr, "payment-reconciler: startup failed")
		return 1
	}
	defer closeService()

	execute := func() error {
		ctx, cancel := context.WithTimeout(parent, cfg.Timeout)
		defer cancel()
		result, runErr := service.ReconcileAll(ctx, paymentreconcile.Options{Scope: cfg.Scope, Limit: cfg.BatchSize})
		status := "completed"
		code := ""
		if runErr != nil {
			status = "failed"
			code = boundedError(runErr)
		}
		writeErr := writeEnvelope(stdout, envelope{
			Status: status, Scope: cfg.Scope, ReadOnly: true, RowsExamined: result.RowsExamined,
			ShardRowsFound: countShardRows(result.Reports), IssuedOrders: countIssuedOrders(result.Reports),
			MismatchCount: result.MismatchCount, ManualReviews: result.ManualReviews,
			Truncated: result.Truncated, Error: code,
		})
		return errors.Join(runErr, writeErr)
	}
	if cfg.Once {
		if err := execute(); err != nil {
			fmt.Fprintln(stderr, "payment-reconciler: pass failed")
			return 1
		}
		return 0
	}
	if err := execute(); err != nil {
		fmt.Fprintln(stderr, "payment-reconciler: pass failed")
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-parent.Done():
			return 0
		case <-ticker.C:
			if err := execute(); err != nil {
				fmt.Fprintln(stderr, "payment-reconciler: pass failed")
			}
		}
	}
}

func countShardRows(reports []paymentreconcile.Report) int {
	count := 0
	for _, report := range reports {
		if report.ShardFound {
			count++
		}
	}
	return count
}

func countIssuedOrders(reports []paymentreconcile.Report) int {
	count := 0
	for _, report := range reports {
		if report.TicketOrderFound && report.TicketOrderState == "issued" {
			count++
		}
	}
	return count
}

func parse(args []string, lookup func(string) (string, bool)) (config, error) {
	flags := flag.NewFlagSet("payment-reconciler", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	scope := flags.String("scope", envOr(lookup, "PAYMENT_RECONCILER_SCOPE", string(paymentreconcile.ScopeAll)), "bounded reconciliation scope")
	batch := flags.Int("batch-size", envInt(lookup, "PAYMENT_RECONCILER_BATCH_SIZE", defaultBatchSize), "bounded intent batch")
	interval := flags.Duration("interval", envDuration(lookup, "PAYMENT_RECONCILER_INTERVAL", defaultInterval), "delay between passes")
	timeout := flags.Duration("timeout", envDuration(lookup, "PAYMENT_RECONCILER_TIMEOUT", defaultTimeout), "per-pass timeout")
	once := flags.Bool("once", false, "run one pass")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return config{}, errArguments
	}
	cfg := config{Scope: paymentreconcile.Scope(strings.TrimSpace(*scope)), BatchSize: *batch, Interval: *interval, Timeout: *timeout, Once: *once}
	if !cfg.Scope.Valid() || cfg.BatchSize < 1 || cfg.BatchSize > paymentreconcile.MaxBatchSize || cfg.Interval <= 0 || cfg.Interval > maxInterval || cfg.Timeout <= 0 || cfg.Timeout > maxTimeout {
		return config{}, errArguments
	}
	return cfg, nil
}

func openRunner(ctx context.Context, lookup func(string) (string, bool), commandConfig config) (runner, func(), error) {
	cfg, err := platformConfig(lookup)
	if err != nil || cfg.BookingShardMode != platformconfig.BookingShardModePhysical {
		return nil, func() {}, errRuntimeWiring
	}
	control, err := postgresx.NewRegionalBoundedPool(ctx, cfg.DatabaseURL, cfg.ControlDatabaseMaxOpenConns, postgresx.RegionalSession{
		Region: string(cfg.DeploymentRegion), Role: string(cfg.DeploymentRole),
		Epoch: cfg.RegionEpoch, WritesEnabled: cfg.RegionalWritesEnabled,
	})
	if err != nil {
		return nil, func() {}, errRuntimeWiring
	}
	cleanup := []func(){control.Close}
	fail := func() (runner, func(), error) {
		for index := len(cleanup) - 1; index >= 0; index-- {
			cleanup[index]()
		}
		return nil, func() {}, errRuntimeWiring
	}
	providerClient, err := newReconciliationProvider(cfg)
	if err != nil {
		return fail()
	}
	cleanup = append(cleanup, providerClient.CloseIdleConnections)
	physicalRegistry, err := newPhysicalRegistry(ctx, cfg)
	if err != nil {
		return fail()
	}
	cleanup = append(cleanup, physicalRegistry.Close)
	router, err := shardphysical.NewCatalogRouter(control, physicalRegistry, cfg.BookingRouteCacheTTL)
	if err != nil {
		return fail()
	}
	store, err := reconcilepostgres.New(control, router)
	if err != nil {
		return fail()
	}
	reconciler, err := paymentreconcile.New(store, providerRegistry{string(cfg.PaymentProviderType): providerClient}, nil, paymentreconcile.Config{
		BatchSize: commandConfig.BatchSize, StaleAfter: cfg.PaymentProcessingGrace,
		ReviewDue: cfg.PaymentManualReviewAfter, Now: time.Now,
	})
	if err != nil {
		return fail()
	}
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.NewEventMetrics(registry)
	if err != nil {
		return fail()
	}
	durableOptions := make([]platformmetrics.DurableOperationsOption, 0, 2)
	providerCapabilities := providerClient.Descriptor().Capabilities
	durableOptions = append(durableOptions, platformmetrics.WithProviderCapabilityProfile(providerClient.Descriptor().Name, map[string]bool{
		"hosted_checkout": providerCapabilities.HostedCheckout, "authorize": providerCapabilities.Authorize,
		"capture": providerCapabilities.Capture, "void": providerCapabilities.Void,
		"full_refund": providerCapabilities.FullRefund, "partial_refund": providerCapabilities.PartialRefund,
		"payment_status_query":    providerCapabilities.PaymentStatusQuery,
		"settlement_transactions": providerCapabilities.SettlementTransactions,
		"payout_reports":          providerCapabilities.PayoutReports, "webhook_signatures": providerCapabilities.WebhookSignatures,
		"webhook_key_rotation": providerCapabilities.WebhookKeyRotation,
	}))
	for shardID, shardPool := range physicalRegistry.ReadOnlyPools() {
		metricShardID := strings.TrimPrefix(shardID.String(), "physical-")
		durableOptions = append(durableOptions, platformmetrics.WithDurableReplicationSource("booking_shard", metricShardID, shardPool))
	}
	durableMetrics, err := platformmetrics.NewDurableOperationsCollector(
		control, string(cfg.DeploymentRegion), readinessTimeout(cfg), durableOptions...,
	)
	if err != nil || registry.Register(durableMetrics) != nil {
		return fail()
	}
	authorityReadiness := readiness(control, physicalRegistry, store, providerClient, cfg)
	health, err := workerhttp.New(cfg.WorkerHTTPAddress, registry, authorityReadiness, readinessTimeout(cfg))
	if err != nil {
		return fail()
	}
	listener, err := net.Listen("tcp", cfg.WorkerHTTPAddress)
	if err != nil {
		return fail()
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- health.Serve(listener) }()
	cleanup = append(cleanup, func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := health.Shutdown(shutdownCtx); err != nil {
			_ = health.Close()
		}
	})
	closeAll := func() {
		for index := len(cleanup) - 1; index >= 0; index-- {
			cleanup[index]()
		}
	}
	return &runtimeRunner{reconciler: reconciler, serverErrors: serverErrors, metrics: metrics, authorityReadiness: authorityReadiness}, closeAll, nil
}

type runtimeRunner struct {
	reconciler         *paymentreconcile.Reconciler
	serverErrors       <-chan error
	metrics            *platformmetrics.Metrics
	authorityReadiness workerhttp.ReadinessCheck
}

func (r *runtimeRunner) ReconcileAll(ctx context.Context, options paymentreconcile.Options) (paymentreconcile.Result, error) {
	if r == nil || r.reconciler == nil {
		return paymentreconcile.Result{}, errRuntimeWiring
	}
	if r.authorityReadiness == nil || r.authorityReadiness(ctx) != nil {
		return paymentreconcile.Result{}, errRuntimeWiring
	}
	select {
	case err := <-r.serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return paymentreconcile.Result{}, errRuntimeWiring
		}
	default:
	}
	result, err := r.reconciler.ReconcileAll(ctx, options)
	metricResult := "success"
	if err != nil {
		metricResult = "failure"
	}
	if r.metrics != nil {
		r.metrics.RecordPaymentReconciliation(strings.ReplaceAll(string(options.Scope), "-", "_"), metricResult, result.MismatchCount > 0, result.RepairCount > 0)
	}
	return result, err
}

type providerRegistry map[string]paymentreconcile.StatusQuerier

func (registry providerRegistry) Provider(name string) (paymentreconcile.StatusQuerier, bool) {
	client, ok := registry[name]
	return client, ok
}

type reconciliationProvider interface {
	paymentreconcile.StatusQuerier
	paymentprovider.Described
	Ready(context.Context) error
	CloseIdleConnections()
}

func newReconciliationProvider(cfg platformconfig.Config) (reconciliationProvider, error) {
	switch cfg.PaymentProviderType {
	case platformconfig.PaymentProviderSandbox:
		return providerhttp.New(providerhttp.Config{
			BaseURL: cfg.PaymentProviderBaseURL, APIKey: cfg.PaymentProviderAPIKey,
			ConnectTimeout: cfg.PaymentProviderConnectTimeout, RequestTimeout: cfg.PaymentProviderRequestTimeout,
			MaxResponseBytes: int64(cfg.PaymentProviderMaxResponseBytes), Now: time.Now,
		})
	case platformconfig.PaymentProviderStripe:
		return providerstripe.NewStatusClient(providerstripe.Config{
			SecretKey: cfg.PaymentProviderAPIKey, AccountID: cfg.PaymentProviderAccountID,
			APIOrigin: cfg.PaymentProviderBaseURL, ConnectTimeout: cfg.PaymentProviderConnectTimeout,
			RequestTimeout:       cfg.PaymentProviderRequestTimeout,
			MaxResponseBodyBytes: int64(cfg.PaymentProviderMaxResponseBytes), Now: time.Now,
			AllowInsecureForTest: cfg.Environment == platformconfig.EnvironmentTest,
		})
	default:
		return nil, &paymentprovider.Error{Category: paymentprovider.ErrorPermanentValidation, Operation: "reconcile_provider", Message: "provider type unsupported"}
	}
}

func platformConfig(lookup func(string) (string, bool)) (platformconfig.Config, error) {
	return platformconfig.LoadFromFor(lookup, platformconfig.ProcessPaymentReconciler)
}

func newPhysicalRegistry(ctx context.Context, cfg platformconfig.Config) (*shardphysical.Registry, error) {
	connections := make(map[string]shardphysical.ConnectionConfig, len(cfg.PhysicalShardConnections))
	for reference, dsn := range cfg.PhysicalShardConnections {
		shardID, err := sharding.ParseShardID(reference)
		if err != nil {
			return nil, errRuntimeWiring
		}
		connections[reference] = shardphysical.ConnectionConfig{ShardID: shardID, DSN: dsn}
	}
	return shardphysical.NewRegistry(ctx, shardphysical.RegistryConfig{
		Connections: connections, MaxCount: cfg.PhysicalShardMaxCount,
		Limits: shardphysical.PoolLimits{
			MaxOpenConns: cfg.PhysicalShardMaxOpenConns, MaxIdleConns: cfg.PhysicalShardMaxIdleConns,
			MaxLifetime: cfg.PhysicalShardConnMaxLifetime, MaxIdleTime: cfg.PhysicalShardConnMaxIdleTime,
			ConnectTimeout: cfg.PhysicalShardConnectTimeout, StatementTimeout: cfg.PhysicalShardQueryTimeout,
			LockTimeout: cfg.PhysicalShardQueryTimeout,
		},
	}, shardphysical.RegionalPGXPoolFactory(postgresx.RegionalSession{
		Region: string(cfg.DeploymentRegion), Role: string(cfg.DeploymentRole),
		Epoch: cfg.RegionEpoch, WritesEnabled: cfg.RegionalWritesEnabled,
	}))
}

type readyStore interface{ Ready(context.Context) error }
type readyProvider interface{ Ready(context.Context) error }

func readiness(control *pgxpool.Pool, registry *shardphysical.Registry, store readyStore, provider readyProvider, cfg platformconfig.Config) workerhttp.ReadinessCheck {
	return func(ctx context.Context) error {
		if control == nil || registry == nil || store == nil || provider == nil || cfg.ValidateFor(platformconfig.ProcessPaymentReconciler) != nil {
			return errRuntimeWiring
		}
		if err := control.Ping(ctx); err != nil {
			return errRuntimeWiring
		}
		session := postgresx.RegionalSession{Region: string(cfg.DeploymentRegion), Role: string(cfg.DeploymentRole), Epoch: cfg.RegionEpoch, WritesEnabled: cfg.RegionalWritesEnabled}
		if postgresx.CheckRegionalReadiness(ctx, control, session) != nil {
			return errRuntimeWiring
		}
		if err := store.Ready(ctx); err != nil {
			return errRuntimeWiring
		}
		if err := provider.Ready(ctx); err != nil {
			return errRuntimeWiring
		}
		for _, rawShardID := range cfg.BookingShardIDs {
			if err := physicalShardReady(ctx, control, registry, rawShardID, session); err != nil {
				return errRuntimeWiring
			}
		}
		return nil
	}
}

func physicalShardReady(ctx context.Context, control *pgxpool.Pool, registry *shardphysical.Registry, rawShardID string, session postgresx.RegionalSession) error {
	var shardIDText, storageKind, connectionRef, healthState, state string
	var protocolVersion, schemaVersion int32
	var enabled, writeEnabled bool
	err := control.QueryRow(ctx, `SELECT shard_id,storage_kind,connection_ref,protocol_version,
 schema_version,enabled,write_enabled,health_state,state FROM public.booking_shards WHERE shard_id=$1`, rawShardID).Scan(
		&shardIDText, &storageKind, &connectionRef, &protocolVersion, &schemaVersion,
		&enabled, &writeEnabled, &healthState, &state,
	)
	if err != nil || shardIDText != rawShardID {
		return errRuntimeWiring
	}
	shardID, err := sharding.ParseShardID(shardIDText)
	if err != nil {
		return errRuntimeWiring
	}
	handle, err := registry.Resolve(shardphysical.CatalogEntry{
		ShardID: shardID, StorageKind: shardphysical.StorageKind(storageKind), ConnectionRef: connectionRef,
		ProtocolVersion: protocolVersion, SchemaVersion: schemaVersion, Enabled: enabled,
		WriteEnabled: writeEnabled, HealthState: shardphysical.HealthState(healthState), State: shardphysical.CatalogState(state),
	})
	if err != nil {
		return errRuntimeWiring
	}
	tx, err := handle.Pool().BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return errRuntimeWiring
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var dirty bool
	if err := tx.QueryRow(ctx, `SELECT version,dirty FROM public.schema_migrations LIMIT 1`).Scan(&schemaVersion, &dirty); err != nil || schemaVersion != shardphysical.SupportedSchemaVersion || dirty {
		return errRuntimeWiring
	}
	if postgresx.CheckRegionalReadiness(ctx, tx, session) != nil {
		return errRuntimeWiring
	}
	return tx.Commit(ctx)
}

func readinessTimeout(cfg platformconfig.Config) time.Duration {
	result := cfg.DatabaseTimeout
	for _, candidate := range []time.Duration{cfg.PaymentProviderRequestTimeout, cfg.PhysicalShardQueryTimeout} {
		if candidate > result {
			result = candidate
		}
	}
	if result <= 0 || result > 10*time.Second {
		return 2 * time.Second
	}
	return result
}

func writeEnvelope(writer io.Writer, value envelope) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(true)
	return encoder.Encode(value)
}

func boundedError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "reconciliation_failed"
	}
}

func envOr(lookup func(string) (string, bool), name, fallback string) string {
	if value, ok := lookup(name); ok && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}

func envInt(lookup func(string) (string, bool), name string, fallback int) int {
	value, err := strconv.Atoi(envOr(lookup, name, strconv.Itoa(fallback)))
	if err != nil {
		return 0
	}
	return value
}

func envDuration(lookup func(string) (string, bool), name string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(envOr(lookup, name, fallback.String()))
	if err != nil {
		return 0
	}
	return value
}
