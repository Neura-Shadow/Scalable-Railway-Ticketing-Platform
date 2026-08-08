package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	paymentreconcile "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/reconcile"
)

func TestRunOnceUsesDetectOnlyBoundedOptions(t *testing.T) {
	backend := &fakeRunner{result: paymentreconcile.Result{RowsExamined: 3, MismatchCount: 1, ManualReviews: 1}}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"--once", "--scope", "payment-tickets", "--batch-size", "7"}, noEnv, &stdout, &stderr,
		func(context.Context, func(string) (string, bool), config) (runner, func(), error) {
			return backend, func() {}, nil
		})
	if code != 0 || backend.options.Scope != paymentreconcile.ScopeTickets || backend.options.Limit != 7 {
		t.Fatalf("code=%d options=%+v stderr=%q", code, backend.options, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"read_only":true`) || !strings.Contains(stdout.String(), `"mismatch_count":1`) {
		t.Fatalf("stdout=%s", stdout.String())
	}
}

func TestRunRejectsUnboundedConfiguration(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"--once", "--batch-size", "1001"}, noEnv, &stdout, &stderr, openRunner)
	if code != 2 || !strings.Contains(stdout.String(), `"error":"invalid_arguments"`) {
		t.Fatalf("code=%d stdout=%q", code, stdout.String())
	}
}

func TestRunRedactsBackendFailure(t *testing.T) {
	backend := &fakeRunner{err: errors.New("postgres://secret@physical-shard")}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"--once"}, noEnv, &stdout, &stderr,
		func(context.Context, func(string) (string, bool), config) (runner, func(), error) {
			return backend, func() {}, nil
		})
	if code != 1 || strings.Contains(stdout.String(), "secret") || strings.Contains(stderr.String(), "secret") || !strings.Contains(stdout.String(), "reconciliation_failed") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

type fakeRunner struct {
	result  paymentreconcile.Result
	err     error
	options paymentreconcile.Options
}

func (r *fakeRunner) ReconcileAll(_ context.Context, options paymentreconcile.Options) (paymentreconcile.Result, error) {
	r.options = options
	return r.result, r.err
}
func noEnv(string) (string, bool) { return "", false }
