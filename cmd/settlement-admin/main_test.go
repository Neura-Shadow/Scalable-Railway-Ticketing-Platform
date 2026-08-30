package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
	settlementpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement/postgres"
)

func TestReconcilePaymentRunsBoundedDetectorAndSanitizesOutput(t *testing.T) {
	t.Parallel()

	backend := &fakeAdminBackend{report: settlement.DetectionReport{
		Pages: 2, Examined: 3, Completed: true,
		Findings: []settlement.Finding{
			{Correlation: "payment-secret", Kind: settlement.EvidenceLedger, Reason: settlement.FindingAmount},
			{Correlation: "payment-secret", Kind: settlement.EvidenceLedger, Reason: settlement.FindingFee},
		},
	}}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"reconcile-payment", "--payment", "payment-secret", "--page-size", "7", "--max-pages", "3",
	}, adminEnv, &stdout, &stderr, func(context.Context, func(string) (string, bool), commandConfig) (adminBackend, func(), error) {
		return backend, func() {}, nil
	})
	if code != 0 || backend.scope.Kind != settlement.ScopePayment || backend.scope.Value != "payment-secret" {
		t.Fatalf("code=%d scope=%+v stderr=%q", code, backend.scope, stderr.String())
	}
	for _, fragment := range []string{
		`"status":"completed"`, `"command":"reconcile-payment"`, `"read_only":true`,
		`"financial_mutation":false`, `"pages":2`, `"finding_count":2`, `"amount":1`, `"fee":1`,
		`"completed":true`, `"bounded":false`,
	} {
		if !strings.Contains(stdout.String(), fragment) {
			t.Fatalf("stdout=%q missing %q", stdout.String(), fragment)
		}
	}
	if strings.Contains(stdout.String(), "payment-secret") {
		t.Fatalf("output disclosed correlation: %q", stdout.String())
	}
}

func TestBoundedDetectionOutputPreservesFalseCompletedBoolean(t *testing.T) {
	t.Parallel()

	backend := &fakeAdminBackend{report: settlement.DetectionReport{Pages: 3, Examined: 21, Bounded: true}}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"reconcile-payment", "--payment", "pay_1", "--page-size", "7", "--max-pages", "3",
	}, adminEnv, &stdout, &stderr, func(context.Context, func(string) (string, bool), commandConfig) (adminBackend, func(), error) {
		return backend, func() {}, nil
	})
	if code != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	for _, fragment := range []string{`"completed":false`, `"bounded":true`} {
		if !strings.Contains(stdout.String(), fragment) {
			t.Fatalf("stdout=%q missing %q", stdout.String(), fragment)
		}
	}
}

func TestInspectAndExportCommandsMapToDetectOnlyScopes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		args []string
		kind settlement.DetectionScopeKind
		want string
	}{
		{[]string{"inspect-batch", "--batch", "set_1"}, settlement.ScopeSettlement, "set_1"},
		{[]string{"inspect-payout", "--payout", "po_1"}, settlement.ScopePayout, "po_1"},
		{[]string{"inspect-transaction", "--transaction", "txn_1"}, settlement.ScopePayment, "txn_1"},
		{[]string{"reconcile-period", "--from", "2026-08-01", "--to", "2026-08-11"}, settlement.ScopePeriod, "2026-08-01/2026-08-11"},
		{[]string{"reconcile-payout", "--payout", "po_2"}, settlement.ScopePayout, "po_2"},
		{[]string{"export-sanitized-report", "--scope", "settlement", "--value", "set_2"}, settlement.ScopeSettlement, "set_2"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.args[0], func(t *testing.T) {
			t.Parallel()
			backend := &fakeAdminBackend{report: settlement.DetectionReport{Completed: true}}
			var stdout, stderr bytes.Buffer
			code := run(context.Background(), test.args, adminEnv, &stdout, &stderr,
				func(context.Context, func(string) (string, bool), commandConfig) (adminBackend, func(), error) {
					return backend, func() {}, nil
				})
			if code != 0 || backend.scope.Kind != test.kind || backend.scope.Value != test.want ||
				strings.Contains(stdout.String(), test.want) {
				t.Fatalf("code=%d scope=%+v stdout=%q stderr=%q", code, backend.scope, stdout.String(), stderr.String())
			}
		})
	}
}

func TestMarkReviewedAppendsHashedEvidenceWithoutFinancialMutation(t *testing.T) {
	t.Parallel()

	backend := &fakeAdminBackend{}
	runID := "0198a9d3-c042-7145-b691-8a3b31ba7aad"
	evidenceHash := strings.Repeat("a1", 32)
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"mark-reviewed", "--run", runID, "--reviewer", "operator:oncall",
		"--disposition", "investigating", "--evidence-hash", evidenceHash,
	}, adminEnv, &stdout, &stderr, func(context.Context, func(string) (string, bool), commandConfig) (adminBackend, func(), error) {
		return backend, func() {}, nil
	})
	if code != 0 || backend.review.RunID.String() != runID || backend.review.ReviewerID != "operator:oncall" ||
		backend.review.Disposition != settlementpostgres.ReviewInvestigating || backend.review.ID.String() == "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("code=%d review=%+v stderr=%q", code, backend.review, stderr.String())
	}
	for _, fragment := range []string{`"status":"completed"`, `"command":"mark-reviewed"`, `"append_only":true`, `"financial_mutation":false`} {
		if !strings.Contains(stdout.String(), fragment) {
			t.Fatalf("stdout=%q missing %q", stdout.String(), fragment)
		}
	}
	for _, secret := range []string{runID, "operator:oncall", evidenceHash} {
		if strings.Contains(stdout.String(), secret) || strings.Contains(stderr.String(), secret) {
			t.Fatalf("output disclosed review input %q: stdout=%q stderr=%q", secret, stdout.String(), stderr.String())
		}
	}
	for _, detectionField := range []string{`"completed":`, `"bounded":`} {
		if strings.Contains(stdout.String(), detectionField) {
			t.Fatalf("review output included detection-only field %q: %q", detectionField, stdout.String())
		}
	}
}

func TestRunRejectsUnboundedOrDestructiveInputAndRedactsFailures(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"repair", "--payment", "pay_1"}, adminEnv, &stdout, &stderr, openAdminBackend); code != 2 {
		t.Fatalf("destructive command code=%d stdout=%q", code, stdout.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"reconcile-payment", "--payment", "pay_1", "--page-size", "1001"}, adminEnv, &stdout, &stderr, openAdminBackend); code != 2 {
		t.Fatalf("unbounded command code=%d stdout=%q", code, stdout.String())
	}
	stdout.Reset()
	stderr.Reset()
	backend := &fakeAdminBackend{err: errors.New("postgres://secret@db raw-provider-report")}
	code := run(context.Background(), []string{"reconcile-payment", "--payment", "pay_1"}, adminEnv, &stdout, &stderr,
		func(context.Context, func(string) (string, bool), commandConfig) (adminBackend, func(), error) {
			return backend, func() {}, nil
		})
	if code != 1 || strings.Contains(stdout.String(), "secret") || strings.Contains(stderr.String(), "secret") ||
		!strings.Contains(stdout.String(), `"error":"detection_failed"`) {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

type fakeAdminBackend struct {
	report settlement.DetectionReport
	err    error
	scope  settlement.DetectionScope
	review settlementpostgres.Review
}

func (backend *fakeAdminBackend) RunOnce(_ context.Context, scope settlement.DetectionScope) (settlement.DetectionReport, error) {
	backend.scope = scope
	return backend.report, backend.err
}

func (backend *fakeAdminBackend) AppendReview(_ context.Context, review settlementpostgres.Review) error {
	backend.review = review
	return backend.err
}

func adminEnv(name string) (string, bool) {
	values := map[string]string{
		"DATABASE_URL": "postgres://settlement@db/railway", "DEPLOYMENT_REGION": "region-a",
		"DEPLOYMENT_ROLE": "active", "REGION_EPOCH": "1", "REGIONAL_WRITES_ENABLED": "true",
	}
	value, ok := values[name]
	return value, ok
}
