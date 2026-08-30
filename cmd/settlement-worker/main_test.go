package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
)

func TestRunOnceUsesBoundedScopeAndSanitizedOutput(t *testing.T) {
	t.Parallel()

	backend := &fakeRunner{report: settlement.ImportReport{Pages: 2, Examined: 3, Inserted: 2, Replayed: 1, Completed: true}}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"--once", "--page-size", "7", "--max-pages", "3"}, workerEnv, &stdout, &stderr,
		func(context.Context, func(string) (string, bool), config) (runner, func(), error) {
			return backend, func() {}, nil
		})
	if code != 0 || backend.scope.Provider != "stripe" || backend.scope.AccountID != "acct_ops" {
		t.Fatalf("code=%d scope=%+v stderr=%q", code, backend.scope, stderr.String())
	}
	for _, fragment := range []string{`"status":"completed"`, `"pages":2`, `"inserted":2`, `"replayed":1`} {
		if !strings.Contains(stdout.String(), fragment) {
			t.Fatalf("stdout=%q missing %q", stdout.String(), fragment)
		}
	}
	if strings.Contains(stdout.String(), "acct_ops") {
		t.Fatalf("output disclosed provider account: %q", stdout.String())
	}
}

func TestRunRejectsUnboundedInputAndRedactsRuntimeFailure(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"--once", "--page-size", "1001"}, workerEnv, &stdout, &stderr, openRunner); code != 2 {
		t.Fatalf("unbounded code=%d stdout=%q", code, stdout.String())
	}
	stdout.Reset()
	stderr.Reset()
	backend := &fakeRunner{err: errors.New("postgres://secret@db provider-body")}
	code := run(context.Background(), []string{"--once"}, workerEnv, &stdout, &stderr,
		func(context.Context, func(string) (string, bool), config) (runner, func(), error) {
			return backend, func() {}, nil
		})
	if code != 1 || strings.Contains(stdout.String(), "secret") || strings.Contains(stderr.String(), "secret") ||
		!strings.Contains(stdout.String(), `"error":"import_failed"`) {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestEnvSecondsRejectsDurationOverflow(t *testing.T) {
	t.Parallel()

	lookup := func(string) (string, bool) { return "9223372036854775807", true }
	if got := envSeconds(lookup, "SETTLEMENT_WORKER_INTERVAL_SECONDS", time.Minute); got != 0 {
		t.Fatalf("envSeconds() = %v, want fail-closed zero", got)
	}
}

type fakeRunner struct {
	report settlement.ImportReport
	err    error
	scope  settlement.AccountScope
}

func (runner *fakeRunner) RunOnce(_ context.Context, scope settlement.AccountScope) (settlement.ImportReport, error) {
	runner.scope = scope
	return runner.report, runner.err
}

func workerEnv(name string) (string, bool) {
	values := map[string]string{
		"APP_ENV":                      "test",
		"DATABASE_URL":                 "postgres://settlement@db/railway",
		"PAYMENT_ENABLED":              "true",
		"PAYMENT_PROVIDER_TYPE":        "stripe",
		"PAYMENT_PROVIDER_ACCOUNT_ID":  "acct_ops",
		"PAYMENT_PROVIDER_BASE_URL":    "https://api.stripe.com",
		"PAYMENT_PROVIDER_API_KEY":     "rk_test_settlement_not_output",
		"PAYMENT_PROVIDER_API_VERSION": "2026-07-29.dahlia",
		"SETTLEMENT_WORKER_ENABLED":    "true",
		"DEPLOYMENT_REGION":            "region-a",
		"DEPLOYMENT_ROLE":              "active",
		"REGION_EPOCH":                 "1",
		"REGIONAL_WRITES_ENABLED":      "true",
	}
	value, ok := values[name]
	return value, ok
}
