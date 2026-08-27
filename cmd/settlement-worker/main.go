package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
	settlementpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement/postgres"
	platformconfig "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/workerhttp"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	authoritypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority/postgres"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultPageSize = 100
	defaultMaxPages = 10
	defaultInterval = 15 * time.Minute
	defaultTimeout  = 2 * time.Minute
	defaultAttempts = 8
	maxInterval     = 24 * time.Hour
	maxTimeout      = 10 * time.Minute
	maxAttempts     = 20
	defaultHTTPAddr = ":9090"
	healthTimeout   = 5 * time.Second
	shutdownTimeout = 10 * time.Second
)

var (
	errArguments     = errors.New("invalid settlement worker arguments")
	errRuntimeWiring = errors.New("settlement worker runtime wiring unavailable")
)

type config struct {
	Scope        settlement.AccountScope
	PageSize     int
	MaxPages     int
	Interval     time.Duration
	Timeout      time.Duration
	MaxAttempts  int
	LookbackDays int
	Once         bool
	Authority    authority.Deployment
}

type runner interface {
	RunOnce(context.Context, settlement.AccountScope) (settlement.ImportReport, error)
}

type runnerFactory func(context.Context, func(string) (string, bool), config) (runner, func(), error)

type envelope struct {
	Status    string `json:"status"`
	Provider  string `json:"provider"`
	Pages     int    `json:"pages"`
	Examined  int    `json:"examined"`
	Inserted  int    `json:"inserted"`
	Replayed  int    `json:"replayed"`
	Conflicts int    `json:"conflicts"`
	Completed bool   `json:"completed"`
	Bounded   bool   `json:"bounded"`
	Error     string `json:"error,omitempty"`
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
		_ = writeEnvelope(stdout, envelope{Status: "rejected", Provider: "unknown", Error: "invalid_arguments"})
		fmt.Fprintln(stderr, "settlement-worker: arguments rejected")
		return 2
	}
	backend, closeBackend, err := factory(parent, lookup, cfg)
	if err != nil {
		_ = writeEnvelope(stdout, envelope{Status: "failed", Provider: cfg.Scope.Provider, Error: "startup_failed"})
		fmt.Fprintln(stderr, "settlement-worker: startup failed")
		return 1
	}
	defer closeBackend()
	if !settlementPassEnabled(cfg.Authority) {
		if cfg.Once {
			_ = writeEnvelope(stdout, envelope{Status: "rejected", Provider: cfg.Scope.Provider, Error: "passive_region"})
			fmt.Fprintln(stderr, "settlement-worker: passive region cannot import")
			return 1
		}
		<-parent.Done()
		return 0
	}

	execute := func() error {
		ctx, cancel := context.WithTimeout(parent, cfg.Timeout)
		defer cancel()
		report, runErr := runSettlementPass(ctx, backend, cfg.Scope, cfg.MaxAttempts, waitSettlementRetry)
		status := "completed"
		if runErr != nil {
			status = "failed"
		}
		writeErr := writeEnvelope(stdout, envelope{
			Status: status, Provider: cfg.Scope.Provider, Pages: report.Pages, Examined: report.Examined,
			Inserted: report.Inserted, Replayed: report.Replayed, Conflicts: report.Conflicts,
			Completed: report.Completed, Bounded: report.Bounded, Error: boundedError(runErr),
		})
		return errors.Join(runErr, writeErr)
	}
	if cfg.Once {
		if err := execute(); err != nil {
			fmt.Fprintln(stderr, "settlement-worker: import failed")
			return 1
		}
		return 0
	}
	if err := execute(); err != nil {
		fmt.Fprintln(stderr, "settlement-worker: import failed")
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-parent.Done():
			return 0
		case <-ticker.C:
			if err := execute(); err != nil {
				fmt.Fprintln(stderr, "settlement-worker: import failed")
			}
		}
	}
}

func parse(args []string, lookup func(string) (string, bool)) (config, error) {
	validated, err := platformconfig.LoadFromFor(lookup, platformconfig.ProcessSettlementWorker)
	if err != nil {
		return config{}, errArguments
	}
	flags := flag.NewFlagSet("settlement-worker", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	pageSize := flags.Int("page-size", envInt(lookup, "SETTLEMENT_WORKER_PAGE_SIZE", defaultPageSize), "bounded records per provider page")
	maxPages := flags.Int("max-pages", envInt(lookup, "SETTLEMENT_WORKER_MAX_PAGES_PER_RUN", defaultMaxPages), "bounded provider pages per pass")
	attemptLimit := flags.Int("max-attempts", envInt(lookup, "SETTLEMENT_WORKER_MAX_ATTEMPTS", defaultAttempts), "bounded read-only provider attempts per pass")
	interval := flags.Duration("interval", envSeconds(lookup, "SETTLEMENT_WORKER_INTERVAL_SECONDS", defaultInterval), "delay between passes")
	timeout := flags.Duration("timeout", envDuration(lookup, "SETTLEMENT_WORKER_TIMEOUT", defaultTimeout), "per-pass timeout")
	once := flags.Bool("once", false, "run one bounded pass")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return config{}, errArguments
	}
	scope := settlement.AccountScope{
		Provider:  strings.ToLower(string(validated.PaymentProviderType)),
		AccountID: validated.PaymentProviderAccountID,
	}
	deployment, err := settlementDeployment(lookup)
	if err != nil {
		return config{}, errArguments
	}
	cfg := config{
		Scope: scope, PageSize: *pageSize, MaxPages: *maxPages, Interval: *interval,
		Timeout: *timeout, MaxAttempts: *attemptLimit, LookbackDays: validated.SettlementReconciliationLookbackDays,
		Once: *once, Authority: deployment,
	}
	if scope.Provider != "stripe" || !safeIdentity(scope.AccountID) || cfg.PageSize < 1 || cfg.PageSize > 100 ||
		cfg.MaxPages < 1 || cfg.MaxPages > 100 || cfg.MaxAttempts < 1 || cfg.MaxAttempts > maxAttempts || cfg.Interval <= 0 || cfg.Interval > maxInterval ||
		cfg.Timeout <= 0 || cfg.Timeout > maxTimeout || cfg.LookbackDays < 1 || cfg.LookbackDays > 366 {
		return config{}, errArguments
	}
	return cfg, nil
}

type settlementRetryWait func(context.Context, time.Duration) error

func runSettlementPass(ctx context.Context, backend runner, scope settlement.AccountScope, attempts int, wait settlementRetryWait) (settlement.ImportReport, error) {
	if ctx == nil || backend == nil || attempts < 1 || attempts > maxAttempts || wait == nil {
		return settlement.ImportReport{}, errRuntimeWiring
	}
	var report settlement.ImportReport
	for attempt := 1; attempt <= attempts; attempt++ {
		var err error
		report, err = backend.RunOnce(ctx, scope)
		if err == nil {
			return report, nil
		}
		var providerErr *provider.Error
		if !errors.As(err, &providerErr) || !providerErr.Retryable || providerErr.Uncertain || attempt == attempts {
			return report, err
		}
		delay := providerErr.RetryAfter
		if delay <= 0 {
			delay = time.Duration(1<<(attempt-1)) * 250 * time.Millisecond
		}
		if delay > 5*time.Minute {
			delay = 5 * time.Minute
		}
		if err := wait(ctx, delay); err != nil {
			return report, err
		}
	}
	return report, errRuntimeWiring
}

func waitSettlementRetry(ctx context.Context, delay time.Duration) error {
	if ctx == nil || delay <= 0 || delay > 5*time.Minute {
		return errRuntimeWiring
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func openRunner(ctx context.Context, lookup func(string) (string, bool), cfg config) (runner, func(), error) {
	databaseURL := envOr(lookup, "DATABASE_URL", "")
	if databaseURL == "" {
		return nil, func() {}, errRuntimeWiring
	}
	pool, err := postgresx.NewRegionalBoundedPool(ctx, databaseURL, 4, postgresx.RegionalSession{
		Region: cfg.Authority.Region().String(), Role: string(cfg.Authority.Role()),
		Epoch: int64(cfg.Authority.Epoch().Uint64()), WritesEnabled: cfg.Authority.WritesEnabled(),
	})
	if err != nil {
		return nil, func() {}, errRuntimeWiring
	}
	fail := func() (runner, func(), error) {
		pool.Close()
		return nil, func() {}, errRuntimeWiring
	}
	store, err := settlementpostgres.New(pool, settlementpostgres.WithRegionalAuthority(cfg.Authority))
	if err != nil {
		return fail()
	}
	readyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := store.Ready(readyCtx); err != nil {
		return fail()
	}
	source, err := newStripeSource(stripeSourceConfig{
		BaseURL:              envOr(lookup, "PAYMENT_PROVIDER_BASE_URL", ""),
		APIKey:               envOr(lookup, "PAYMENT_PROVIDER_API_KEY", ""),
		APIVersion:           envOr(lookup, "PAYMENT_PROVIDER_API_VERSION", ""),
		AccountID:            cfg.Scope.AccountID,
		ConnectTimeout:       envDuration(lookup, "PAYMENT_PROVIDER_CONNECT_TIMEOUT", 2*time.Second),
		RequestTimeout:       envDuration(lookup, "PAYMENT_PROVIDER_REQUEST_TIMEOUT", 10*time.Second),
		MaxResponseBytes:     int64(envInt(lookup, "PAYMENT_PROVIDER_MAX_RESPONSE_BYTES", 1<<20)),
		AllowInsecureForTest: allowInsecureProviderOrigin(lookup),
	})
	if err != nil {
		return fail()
	}
	detector, err := settlement.NewDetector(store, settlement.DetectorConfig{PageSize: cfg.PageSize, MaxPages: cfg.MaxPages})
	if err != nil {
		source.CloseIdleConnections()
		return fail()
	}
	backend := &leasedSettlementRunner{
		source: source, leaser: store, detector: detector,
		owner: "settlement-worker:" + uuid.NewString(), leaseDuration: cfg.Timeout + healthTimeout,
		nextDelay: cfg.Interval, pageSize: cfg.PageSize, maxPages: cfg.MaxPages,
		lookbackDays: cfg.LookbackDays, now: time.Now,
	}
	registry := prometheus.NewRegistry()
	metricSet, err := platformmetrics.NewSettlementEventMetrics(registry)
	if err != nil {
		source.CloseIdleConnections()
		return fail()
	}
	health, err := workerhttp.New(
		envOr(lookup, "WORKER_HTTP_ADDRESS", defaultHTTPAddr), registry,
		settlementReadiness(cfg.Authority, pool, store, source.reader), healthTimeout,
	)
	if err != nil {
		source.CloseIdleConnections()
		return fail()
	}
	listener, err := net.Listen("tcp", health.Addr)
	if err != nil {
		source.CloseIdleConnections()
		return fail()
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- health.Serve(listener) }()
	closeAll := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := health.Shutdown(shutdownCtx); err != nil {
			_ = health.Close()
		}
		source.CloseIdleConnections()
		pool.Close()
	}
	return &runtimeRunner{runner: backend, metrics: metricSet, serverErrors: serverErrors}, closeAll, nil
}

type detectionRunner interface {
	RunOnce(context.Context, settlement.DetectionScope) (settlement.DetectionReport, error)
}

// leasedSettlementRunner holds no database transaction across importer or
// detector work. Claim and finish are short regional-authority transactions;
// each page and the detect-only run are committed independently.
type leasedSettlementRunner struct {
	source        settlement.Source
	leaser        settlement.ImportLeaser
	detector      detectionRunner
	owner         string
	leaseDuration time.Duration
	nextDelay     time.Duration
	pageSize      int
	maxPages      int
	lookbackDays  int
	now           func() time.Time
}

func (runner *leasedSettlementRunner) RunOnce(ctx context.Context, scope settlement.AccountScope) (report settlement.ImportReport, resultErr error) {
	if runner == nil || runner.source == nil || runner.leaser == nil || runner.detector == nil || runner.now == nil ||
		runner.owner == "" || runner.leaseDuration < time.Second || runner.leaseDuration > 15*time.Minute ||
		runner.nextDelay <= 0 || runner.pageSize < 1 || runner.pageSize > 1000 || runner.maxPages < 1 || runner.maxPages > 100 ||
		runner.lookbackDays < 1 || runner.lookbackDays > 366 {
		return settlement.ImportReport{}, errRuntimeWiring
	}
	claimedStore, lease, claimed, err := runner.leaser.ClaimDue(ctx, scope, runner.owner, runner.now().UTC(), runner.leaseDuration)
	if err != nil || !claimed {
		return settlement.ImportReport{}, err
	}
	nextDelay := time.Duration(0)
	defer func() {
		if resultErr == nil {
			nextDelay = runner.nextDelay
		}
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), healthTimeout)
		defer cancel()
		resultErr = errors.Join(resultErr, runner.leaser.FinishLease(finishCtx, lease, nextDelay))
	}()

	importer, err := settlement.NewImporter(runner.source, claimedStore, settlement.ImporterConfig{
		PageSize: runner.pageSize, MaxPages: runner.maxPages,
	})
	if err != nil {
		return settlement.ImportReport{}, errRuntimeWiring
	}
	report, resultErr = importer.RunOnce(ctx, scope)
	if resultErr != nil {
		return report, resultErr
	}
	end := runner.now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
	start := end.AddDate(0, 0, -runner.lookbackDays)
	_, resultErr = runner.detector.RunOnce(ctx, settlement.DetectionScope{
		Kind: settlement.ScopePeriod, Value: start.Format("2006-01-02") + "/" + end.Format("2006-01-02"),
	})
	return report, resultErr
}

func allowInsecureProviderOrigin(lookup func(string) (string, bool)) bool {
	return lookup != nil && strings.EqualFold(strings.TrimSpace(envOr(lookup, "APP_ENV", "")), "test")
}

func settlementPassEnabled(deployment authority.Deployment) bool {
	return deployment.Role() == authority.RoleActive && deployment.WritesEnabled()
}

type readyDependency interface {
	Ready(context.Context) error
}

func settlementReadiness(deployment authority.Deployment, database authoritypostgres.QueryRower, store, provider readyDependency) workerhttp.ReadinessCheck {
	return func(ctx context.Context) error {
		if ctx == nil || database == nil || store == nil || provider == nil || deployment.Role() != authority.RoleActive || !deployment.WritesEnabled() {
			return errRuntimeWiring
		}
		if err := authoritypostgres.CheckActiveReadiness(ctx, database, deployment); err != nil {
			return errRuntimeWiring
		}
		if err := store.Ready(ctx); err != nil {
			return errRuntimeWiring
		}
		if err := provider.Ready(ctx); err != nil {
			return errRuntimeWiring
		}
		return nil
	}
}

type runtimeRunner struct {
	runner       runner
	metrics      *platformmetrics.Metrics
	serverErrors <-chan error
}

func (runtime *runtimeRunner) RunOnce(ctx context.Context, scope settlement.AccountScope) (settlement.ImportReport, error) {
	if runtime == nil || runtime.runner == nil {
		return settlement.ImportReport{}, errRuntimeWiring
	}
	if runtime.serverErrors != nil {
		select {
		case err := <-runtime.serverErrors:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				return settlement.ImportReport{}, errRuntimeWiring
			}
		default:
		}
	}
	report, err := runtime.runner.RunOnce(ctx, scope)
	if runtime.metrics != nil {
		result, reason := "success", "none"
		if err != nil {
			result, reason = "failure", settlementMetricReason(err)
		}
		runtime.metrics.RecordSettlementImportResult(scope.Provider, "settlement_batch", result, reason, report.Inserted)
	}
	return report, err
}

func settlementMetricReason(err error) string {
	var providerErr *provider.Error
	if errors.As(err, &providerErr) {
		switch providerErr.Category {
		case provider.ErrorTransport:
			return "transport"
		case provider.ErrorTimeoutUnknown:
			return "timeout"
		case provider.ErrorPermanentValidation:
			return "validation"
		case provider.ErrorAuthentication:
			return "authentication"
		case provider.ErrorUnavailable:
			return "provider_unavailable"
		case provider.ErrorRateLimited:
			return "rate_limited"
		case provider.ErrorConflict:
			return "conflict"
		case provider.ErrorInconsistentResponse:
			return "invariant_mismatch"
		}
	}
	switch {
	case errors.Is(err, settlement.ErrPayloadConflict), errors.Is(err, settlement.ErrCheckpointConflict):
		return "conflict"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "database"
	}
}

func settlementDeployment(lookup func(string) (string, bool)) (authority.Deployment, error) {
	region, err := authority.ParseRegion(envOr(lookup, "DEPLOYMENT_REGION", ""))
	if err != nil {
		return authority.Deployment{}, err
	}
	epochValue, err := strconv.ParseUint(envOr(lookup, "REGION_EPOCH", ""), 10, 64)
	if err != nil {
		return authority.Deployment{}, err
	}
	epoch, err := authority.NewEpoch(epochValue)
	if err != nil {
		return authority.Deployment{}, err
	}
	writes, err := strconv.ParseBool(envOr(lookup, "REGIONAL_WRITES_ENABLED", ""))
	if err != nil {
		return authority.Deployment{}, err
	}
	return authority.NewDeployment(region, authority.Role(envOr(lookup, "DEPLOYMENT_ROLE", "")), epoch, writes)
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
	case errors.Is(err, settlement.ErrPayloadConflict):
		return "payload_conflict"
	default:
		return "import_failed"
	}
}

func safeIdentity(value string) bool {
	if value == "" || len(value) > 200 {
		return false
	}
	for index, character := range value {
		if (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') || (index > 0 && strings.ContainsRune("._:-", character)) {
			continue
		}
		return false
	}
	return true
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

func envSeconds(lookup func(string) (string, bool), name string, fallback time.Duration) time.Duration {
	value, err := strconv.ParseInt(envOr(lookup, name, strconv.FormatInt(int64(fallback/time.Second), 10)), 10, 64)
	if err != nil || value <= 0 || value > math.MaxInt64/int64(time.Second) {
		return 0
	}
	return time.Duration(value) * time.Second
}

func envDuration(lookup func(string) (string, bool), name string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(envOr(lookup, name, fallback.String()))
	if err != nil {
		return 0
	}
	return value
}
