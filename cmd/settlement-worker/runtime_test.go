package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

func TestSettlementReadinessRequiresActiveWriterAndBothDependencies(t *testing.T) {
	t.Parallel()

	region, err := authority.ParseRegion("region-a")
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := authority.NewEpoch(7)
	if err != nil {
		t.Fatal(err)
	}
	active, err := authority.NewDeployment(region, authority.RoleActive, epoch, true)
	if err != nil {
		t.Fatal(err)
	}
	passive, err := authority.NewDeployment(region, authority.RolePassive, epoch, false)
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeReady{}
	provider := &fakeReady{}
	database := &fakeAuthorityDatabase{row: authorityReadinessRow{values: []any{"region-a", int64(7), "active", true, false}}}

	if err := settlementReadiness(passive, database, store, provider)(context.Background()); !errors.Is(err, errRuntimeWiring) {
		t.Fatalf("passive readiness error = %v, want runtime wiring error", err)
	}
	if store.calls != 0 || provider.calls != 0 {
		t.Fatalf("passive readiness touched dependencies: store=%d provider=%d", store.calls, provider.calls)
	}
	if err := settlementReadiness(active, database, store, provider)(context.Background()); err != nil {
		t.Fatalf("active readiness error = %v", err)
	}
	if store.calls != 1 || provider.calls != 1 {
		t.Fatalf("active readiness calls: store=%d provider=%d, want 1 each", store.calls, provider.calls)
	}
	if database.calls != 1 {
		t.Fatalf("active readiness authority calls = %d, want 1", database.calls)
	}
	database.row = authorityReadinessRow{values: []any{"region-a", int64(7), "active", true, true}}
	if err := settlementReadiness(active, database, store, provider)(context.Background()); !errors.Is(err, errRuntimeWiring) {
		t.Fatalf("standby readiness error = %v, want runtime wiring error", err)
	}
	if store.calls != 1 || provider.calls != 1 {
		t.Fatal("standby readiness reached dependencies after primary identity rejection")
	}
	if !settlementPassEnabled(active) || settlementPassEnabled(passive) {
		t.Fatal("settlement pass authority did not match active writer role")
	}
}

func TestSettlementRuntimeCountsOnlyNewDurableImports(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metricSet, err := metrics.NewSettlementEventMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeRunner{report: settlement.ImportReport{Examined: 4, Inserted: 3, Replayed: 1, Completed: true}}
	runtime := &runtimeRunner{runner: backend, metrics: metricSet}

	report, err := runtime.RunOnce(context.Background(), settlement.AccountScope{Provider: "stripe", AccountID: "acct_ops"})
	if err != nil || report.Examined != 4 {
		t.Fatalf("RunOnce() report=%+v err=%v", report, err)
	}
	if got := gatheredMetricValue(t, registry, "settlement_import_total", map[string]string{
		"provider": "stripe", "operation": "settlement_batch", "result": "success",
	}); got != 3 {
		t.Fatalf("settlement import metric = %v, want 3 inserted records without replay inflation", got)
	}
}

func TestSettlementRuntimePreservesProviderFailureReason(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metricSet, err := metrics.NewSettlementEventMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	providerErr := &provider.Error{Category: provider.ErrorRateLimited, Operation: "list_balance_transactions"}
	runtime := &runtimeRunner{runner: &fakeRunner{err: providerErr}, metrics: metricSet}
	if _, err := runtime.RunOnce(context.Background(), settlement.AccountScope{Provider: "stripe", AccountID: "acct_ops"}); !errors.Is(err, providerErr) {
		t.Fatalf("RunOnce error = %v", err)
	}
	if got := gatheredMetricValue(t, registry, "settlement_import_failure_total", map[string]string{
		"provider": "stripe", "operation": "settlement_batch", "reason": "rate_limited",
	}); got != 1 {
		t.Fatalf("settlement failure metric = %v, want 1", got)
	}
}

func TestSettlementHTTPProviderOriginIsTestOnly(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		value string
		want  bool
	}{
		{value: "test", want: true},
		{value: "TEST", want: true},
		{value: "development", want: false},
		{value: "production", want: false},
		{value: "", want: false},
	} {
		lookup := func(name string) (string, bool) {
			if name == "APP_ENV" {
				return test.value, test.value != ""
			}
			return "", false
		}
		if got := allowInsecureProviderOrigin(lookup); got != test.want {
			t.Fatalf("APP_ENV=%q allow=%v, want %v", test.value, got, test.want)
		}
	}
}

func TestSettlementPassRetriesBoundedReadOnlyRateLimitAndHonorsRetryAfter(t *testing.T) {
	t.Parallel()
	backend := &sequenceRunner{errors: []error{
		&provider.Error{Category: provider.ErrorRateLimited, Operation: "list_balance_transactions", Retryable: true, RetryAfter: 3 * time.Second},
		&provider.Error{Category: provider.ErrorRateLimited, Operation: "list_balance_transactions", Retryable: true},
		nil,
	}}
	var delays []time.Duration
	wait := func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	}
	report, err := runSettlementPass(context.Background(), backend, settlement.AccountScope{Provider: "stripe", AccountID: "acct_ops"}, 3, wait)
	if err != nil || report.Inserted != 1 || backend.calls != 3 {
		t.Fatalf("report=%+v err=%v calls=%d", report, err, backend.calls)
	}
	if len(delays) != 2 || delays[0] != 3*time.Second || delays[1] != 500*time.Millisecond {
		t.Fatalf("retry delays = %v", delays)
	}
}

func TestSettlementPassDoesNotRetryUncertainOrPermanentFailure(t *testing.T) {
	t.Parallel()
	for _, failure := range []*provider.Error{
		{Category: provider.ErrorTimeoutUnknown, Operation: "list_balance_transactions", Retryable: true, Uncertain: true},
		{Category: provider.ErrorAuthentication, Operation: "list_balance_transactions"},
	} {
		backend := &sequenceRunner{errors: []error{failure, nil}}
		_, err := runSettlementPass(context.Background(), backend, settlement.AccountScope{Provider: "stripe", AccountID: "acct_ops"}, 3,
			func(context.Context, time.Duration) error { t.Fatal("unexpected retry wait"); return nil })
		if !errors.Is(err, failure) || backend.calls != 1 {
			t.Fatalf("failure=%+v err=%v calls=%d", failure, err, backend.calls)
		}
	}
}

func TestLeasedSettlementRunnerAllowsOneReplicaAndExpiredLeaseTakeover(t *testing.T) {
	t.Parallel()

	scope := settlement.AccountScope{Provider: "stripe", AccountID: "acct_lease"}
	started := make(chan struct{})
	release := make(chan struct{})
	source := &blockingSettlementSource{started: started, release: release}
	leaser := newFakeImportLeaser()
	detector := &fakeDetector{}
	base := time.Date(2026, 8, 17, 1, 0, 0, 0, time.UTC)
	first := newTestLeasedRunner(source, leaser, detector, "replica:first", base)
	second := newTestLeasedRunner(source, leaser, detector, "replica:second", base)

	firstDone := make(chan error, 1)
	go func() {
		_, err := first.RunOnce(context.Background(), scope)
		firstDone <- err
	}()
	<-started

	if report, err := second.RunOnce(context.Background(), scope); err != nil || report.Pages != 0 {
		t.Fatalf("second replica while lease active = (%+v, %v)", report, err)
	}
	if source.Calls() != 1 {
		t.Fatalf("provider calls while lease active = %d, want exactly one", source.Calls())
	}

	second.now = func() time.Time { return base.Add(2 * time.Minute) }
	if _, err := second.RunOnce(context.Background(), scope); err != nil {
		t.Fatalf("expired lease takeover failed: %v", err)
	}
	close(release)
	if err := <-firstDone; !errors.Is(err, settlement.ErrCheckpointConflict) || !errors.Is(err, settlement.ErrImportLeaseLost) {
		t.Fatalf("stale replica error = %v, want checkpoint conflict plus lost lease", err)
	}
	if source.Calls() != 2 {
		t.Fatalf("provider calls after takeover = %d, want one call per valid owner", source.Calls())
	}
	if report, err := second.RunOnce(context.Background(), scope); err != nil || report.Pages != 0 || source.Calls() != 2 {
		t.Fatalf("completed account before next due time = (%+v, %v), provider calls=%d", report, err, source.Calls())
	}
}

func TestLeasedSettlementRunnerPersistsImportBeforeIsolatedDetectionFailure(t *testing.T) {
	t.Parallel()

	scope := settlement.AccountScope{Provider: "stripe", AccountID: "acct_detection"}
	at := time.Date(2026, 8, 17, 2, 0, 0, 0, time.UTC)
	source := settlement.NewMemorySource(map[string]settlement.Page{"": {
		Records: []settlement.Record{{
			Kind: settlement.RecordBalanceTransaction, ProviderID: "txn_detection", Operation: settlement.OperationCapture,
			GrossMinor: 1000, FeeMinor: 30, NetMinor: 970, Currency: "TWD", CreatedAt: at,
		}},
		NextCursor: "completed", Done: true,
	}})
	leaser := newFakeImportLeaser()
	detectorFailure := errors.New("bounded detector unavailable")
	detector := &fakeDetector{errors: []error{detectorFailure, nil}}
	runner := newTestLeasedRunner(source, leaser, detector, "replica:detection", at)

	report, err := runner.RunOnce(context.Background(), scope)
	if !errors.Is(err, detectorFailure) || report.Inserted != 1 {
		t.Fatalf("first pass report=%+v err=%v", report, err)
	}
	stored, found, err := leaser.store.Record(context.Background(), scope, settlement.RecordBalanceTransaction, "txn_detection")
	if err != nil || !found || stored.NetMinor != 970 {
		t.Fatalf("import was rolled back by detector failure: record=%+v found=%v err=%v", stored, found, err)
	}

	report, err = runner.RunOnce(context.Background(), scope)
	if err != nil || !report.Completed || detector.calls != 2 {
		t.Fatalf("detection retry report=%+v err=%v calls=%d", report, err, detector.calls)
	}
	if detector.scopes[0].Kind != settlement.ScopePeriod || detector.scopes[0].Value != "2026-07-19/2026-08-18" {
		t.Fatalf("bounded reconciliation scope = %+v", detector.scopes[0])
	}
}

type fakeReady struct {
	calls int
	err   error
}

type fakeAuthorityDatabase struct {
	row   pgx.Row
	calls int
}

func (database *fakeAuthorityDatabase) QueryRow(context.Context, string, ...any) pgx.Row {
	database.calls++
	return database.row
}

type authorityReadinessRow struct{ values []any }

func (row authorityReadinessRow) Scan(destinations ...any) error {
	if len(destinations) != len(row.values) {
		return errors.New("authority readiness scan mismatch")
	}
	for index, value := range row.values {
		switch destination := destinations[index].(type) {
		case *string:
			*destination = value.(string)
		case *int64:
			*destination = value.(int64)
		case *bool:
			*destination = value.(bool)
		default:
			return errors.New("authority readiness scan destination unsupported")
		}
	}
	return nil
}

type sequenceRunner struct {
	errors []error
	calls  int
}

type fakeDetector struct {
	mu     sync.Mutex
	errors []error
	calls  int
	scopes []settlement.DetectionScope
}

func (detector *fakeDetector) RunOnce(_ context.Context, scope settlement.DetectionScope) (settlement.DetectionReport, error) {
	detector.mu.Lock()
	defer detector.mu.Unlock()
	index := detector.calls
	detector.calls++
	detector.scopes = append(detector.scopes, scope)
	if index < len(detector.errors) && detector.errors[index] != nil {
		return settlement.DetectionReport{}, detector.errors[index]
	}
	return settlement.DetectionReport{Completed: true}, nil
}

type fakeImportLeaser struct {
	mu          sync.Mutex
	store       *settlement.MemoryImportStore
	active      settlement.ImportLease
	nextAttempt time.Time
	lastNow     time.Time
}

func newFakeImportLeaser() *fakeImportLeaser {
	return &fakeImportLeaser{store: settlement.NewMemoryImportStore()}
}

func (leaser *fakeImportLeaser) ClaimDue(_ context.Context, scope settlement.AccountScope, owner string, now time.Time, duration time.Duration) (settlement.ImportStore, settlement.ImportLease, bool, error) {
	leaser.mu.Lock()
	defer leaser.mu.Unlock()
	if !leaser.nextAttempt.IsZero() && now.Before(leaser.nextAttempt) {
		return nil, settlement.ImportLease{}, false, nil
	}
	if leaser.active.Token != uuid.Nil && now.Before(leaser.active.LeaseUntil) {
		return nil, settlement.ImportLease{}, false, nil
	}
	cursor, err := leaser.store.Checkpoint(context.Background(), scope)
	if err != nil {
		return nil, settlement.ImportLease{}, false, err
	}
	lease := settlement.ImportLease{Scope: scope, Owner: owner, Token: uuid.New(), Cursor: cursor, LeaseUntil: now.Add(duration)}
	leaser.active = lease
	leaser.lastNow = now
	return leaser.store, lease, true, nil
}

func (leaser *fakeImportLeaser) FinishLease(_ context.Context, lease settlement.ImportLease, nextDelay time.Duration) error {
	leaser.mu.Lock()
	defer leaser.mu.Unlock()
	if leaser.active.Token != lease.Token {
		return settlement.ErrImportLeaseLost
	}
	leaser.active = settlement.ImportLease{}
	leaser.nextAttempt = leaser.lastNow.Add(nextDelay)
	return nil
}

type blockingSettlementSource struct {
	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
	calls   int
}

func (source *blockingSettlementSource) ListPage(ctx context.Context, _ settlement.AccountScope, cursor string, _ int) (settlement.Page, error) {
	source.mu.Lock()
	source.calls++
	call := source.calls
	source.mu.Unlock()
	if call == 1 {
		close(source.started)
		select {
		case <-ctx.Done():
			return settlement.Page{}, ctx.Err()
		case <-source.release:
		}
	}
	return settlement.Page{
		Records: []settlement.Record{{
			Kind: settlement.RecordBalanceTransaction, ProviderID: "txn_lease", Operation: settlement.OperationCapture,
			GrossMinor: 100, NetMinor: 100, Currency: "TWD", CreatedAt: time.Date(2026, 8, 17, 1, 0, 0, 0, time.UTC),
		}},
		NextCursor: cursor + "done", Done: true,
	}, nil
}

func (source *blockingSettlementSource) Calls() int {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.calls
}

func newTestLeasedRunner(source settlement.Source, leaser settlement.ImportLeaser, detector detectionRunner, owner string, now time.Time) *leasedSettlementRunner {
	return &leasedSettlementRunner{
		source: source, leaser: leaser, detector: detector, owner: owner,
		leaseDuration: time.Minute, nextDelay: time.Minute, pageSize: 10, maxPages: 2,
		lookbackDays: 30, now: func() time.Time { return now },
	}
}

func (runner *sequenceRunner) RunOnce(context.Context, settlement.AccountScope) (settlement.ImportReport, error) {
	index := runner.calls
	runner.calls++
	if index >= len(runner.errors) {
		return settlement.ImportReport{}, errors.New("unexpected settlement attempt")
	}
	if runner.errors[index] != nil {
		return settlement.ImportReport{}, runner.errors[index]
	}
	return settlement.ImportReport{Inserted: 1, Examined: 1, Completed: true}, nil
}

func (ready *fakeReady) Ready(context.Context) error {
	ready.calls++
	return ready.err
}

func gatheredMetricValue(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			matched := len(metric.Label) == len(labels)
			for _, label := range metric.Label {
				if labels[label.GetName()] != label.GetValue() {
					matched = false
				}
			}
			if matched {
				return metric.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("metric %s with labels %v not found", name, labels)
	return 0
}
