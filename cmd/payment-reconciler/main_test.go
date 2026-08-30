package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	providerhttp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/httpclient"
	providerstripe "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/stripe"
	paymentreconcile "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/reconcile"
	platformconfig "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
)

func TestRunOnceUsesDetectOnlyBoundedOptions(t *testing.T) {
	backend := &fakeRunner{result: paymentreconcile.Result{
		RowsExamined: 3, MismatchCount: 1, ManualReviews: 1,
		Reports: []paymentreconcile.Report{
			{ShardFound: true, TicketOrderFound: true, TicketOrderState: "issued"},
			{ShardFound: true, TicketOrderFound: true, TicketOrderState: "refunded"},
			{},
		},
	}}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"--once", "--scope", "payment-tickets", "--batch-size", "7"}, noEnv, &stdout, &stderr,
		func(context.Context, func(string) (string, bool), config) (runner, func(), error) {
			return backend, func() {}, nil
		})
	if code != 0 || backend.options.Scope != paymentreconcile.ScopeTickets || backend.options.Limit != 7 {
		t.Fatalf("code=%d options=%+v stderr=%q", code, backend.options, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"read_only":true`) ||
		!strings.Contains(stdout.String(), `"shard_rows_found":2`) ||
		!strings.Contains(stdout.String(), `"issued_orders":1`) ||
		!strings.Contains(stdout.String(), `"mismatch_count":1`) {
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

func TestNewReconciliationProviderSelectsReadSideAdapter(t *testing.T) {
	t.Parallel()

	sandboxConfig := platformconfig.Defaults()
	sandboxConfig.PaymentProviderType = platformconfig.PaymentProviderSandbox
	sandboxConfig.PaymentProviderBaseURL = "https://sandbox.example"
	sandboxConfig.PaymentProviderAPIKey = "sandbox-contract-key"
	sandbox, err := newReconciliationProvider(sandboxConfig)
	if err != nil {
		t.Fatalf("sandbox: %v", err)
	}
	defer sandbox.CloseIdleConnections()
	if _, ok := sandbox.(*providerhttp.Client); !ok {
		t.Fatalf("sandbox adapter = %T", sandbox)
	}

	stripeConfig := platformconfig.Defaults()
	stripeConfig.PaymentProviderType = platformconfig.PaymentProviderStripe
	stripeConfig.PaymentProviderBaseURL = "https://api.stripe.com"
	stripeConfig.PaymentProviderAPIKey = "rk_test_contract"
	stripeConfig.PaymentProviderAccountID = "acct_contract"
	stripe, err := newReconciliationProvider(stripeConfig)
	if err != nil {
		t.Fatalf("stripe: %v", err)
	}
	defer stripe.CloseIdleConnections()
	if _, ok := stripe.(*providerstripe.Client); !ok {
		t.Fatalf("stripe adapter = %T", stripe)
	}
}

func TestNewReconciliationProviderAllowsHTTPStripeContractOnlyInTest(t *testing.T) {
	t.Parallel()
	cfg := platformconfig.Defaults()
	cfg.PaymentProviderType = platformconfig.PaymentProviderStripe
	cfg.PaymentProviderBaseURL = "http://stripe-contract.test"
	cfg.PaymentProviderAPIKey = "rk_test_contract"
	cfg.PaymentProviderAccountID = "acct_contract"

	cfg.Environment = platformconfig.EnvironmentProduction
	if provider, err := newReconciliationProvider(cfg); err == nil {
		provider.CloseIdleConnections()
		t.Fatal("production accepted an insecure Stripe origin")
	}
	cfg.Environment = platformconfig.EnvironmentTest
	provider, err := newReconciliationProvider(cfg)
	if err != nil {
		t.Fatalf("test contract rejected: %v", err)
	}
	provider.CloseIdleConnections()
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
