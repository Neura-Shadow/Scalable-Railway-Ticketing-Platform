package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	providerhttp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/httpclient"
	providerstripe "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/stripe"
	paymentrefund "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/refund"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	paymentworker "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestExternalEffectCrashIsExactAndTestOnly(t *testing.T) {
	t.Parallel()
	target := uuid.New()
	base := map[string]string{
		"APP_ENV": "test",
		"PAYMENT_WORKER_TEST_CRASH_AFTER_EFFECT_ENABLED": "true",
		"PAYMENT_WORKER_TEST_CRASH_AFTER_EFFECT_POINT":   string(paymentworker.ExternalEffectCaptureCommitted),
		"PAYMENT_WORKER_TEST_CRASH_TARGET_ID":            target.String(),
	}
	lookup := func(key string) (string, bool) { value, ok := base[key]; return value, ok }
	exitCode := 0
	hook := newTestExternalEffectCrash(lookup, func(code int) { exitCode = code })
	hook.payment(paymentworker.ExternalEffectCaptureCommitted, uuid.New())
	hook.refund(paymentrefund.ExternalEffectProviderRefundCommitted, target)
	if exitCode != 0 {
		t.Fatalf("non-matching crash barrier exited with %d", exitCode)
	}
	hook.payment(paymentworker.ExternalEffectCaptureCommitted, target)
	if exitCode != 86 {
		t.Fatalf("matching crash barrier exit = %d", exitCode)
	}

	base["APP_ENV"] = "production"
	exitCode = 0
	hook = newTestExternalEffectCrash(lookup, func(code int) { exitCode = code })
	hook.payment(paymentworker.ExternalEffectCaptureCommitted, target)
	if exitCode != 0 {
		t.Fatalf("production environment enabled test crash barrier")
	}
}

func TestExternalEffectCrashAllowsExactSixApplicationBoundaries(t *testing.T) {
	t.Parallel()
	target := uuid.New()
	points := []string{
		string(paymentworker.ExternalEffectCaptureCommitted),
		string(paymentworker.ExternalEffectTicketsCommitted),
		string(paymentworker.ExternalEffectRefundCommitted),
		string(paymentworker.ExternalEffectCompensationCommitted),
		string(paymentrefund.ExternalEffectProviderRefundCommitted),
		string(paymentrefund.ExternalEffectShardRefundCommitted),
	}
	for _, point := range points {
		point := point
		t.Run(point, func(t *testing.T) {
			exitCode := 0
			values := map[string]string{
				"APP_ENV": "test",
				"PAYMENT_WORKER_TEST_CRASH_AFTER_EFFECT_ENABLED": "true",
				"PAYMENT_WORKER_TEST_CRASH_AFTER_EFFECT_POINT":   point,
				"PAYMENT_WORKER_TEST_CRASH_TARGET_ID":            target.String(),
			}
			lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
			hook := newTestExternalEffectCrash(lookup, func(code int) { exitCode = code })
			if point == string(paymentrefund.ExternalEffectProviderRefundCommitted) ||
				point == string(paymentrefund.ExternalEffectShardRefundCommitted) {
				hook.refund(paymentrefund.ExternalEffectPoint(point), target)
			} else {
				hook.payment(paymentworker.ExternalEffectPoint(point), target)
			}
			if exitCode != 86 {
				t.Fatalf("exact application boundary %q exit = %d", point, exitCode)
			}
		})
	}
}

func TestTicketIssueConflictIsTargetSpecificAndTestOnly(t *testing.T) {
	t.Parallel()
	target := uuid.New()
	values := map[string]string{
		"APP_ENV": "test",
		"PAYMENT_WORKER_TEST_TICKET_ISSUE_CONFLICT_ENABLED":   "true",
		"PAYMENT_WORKER_TEST_TICKET_ISSUE_CONFLICT_TARGET_ID": target.String(),
	}
	lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	next := &paymentWorkerShardFake{}
	fault := newTestTicketIssueConflict(next, lookup)
	if fault == nil {
		t.Fatal("test-gated ticket conflict was not enabled")
	}
	if _, err := fault.IssueTickets(context.Background(), paymentshard.IssueTicketsCommand{PaymentIntentID: target}); !errors.Is(err, paymentshard.ErrTicketClaimConflict) {
		t.Fatalf("target fault = %v", err)
	}
	if _, err := fault.IssueTickets(context.Background(), paymentshard.IssueTicketsCommand{PaymentIntentID: uuid.New()}); err != nil || next.issueCalls != 1 {
		t.Fatalf("non-target delegation err=%v calls=%d", err, next.issueCalls)
	}
	values["APP_ENV"] = "production"
	if newTestTicketIssueConflict(next, lookup) != nil {
		t.Fatal("production enabled ticket conflict hook")
	}
}

type paymentWorkerShardFake struct{ issueCalls int }

func (fake *paymentWorkerShardFake) IssueTickets(context.Context, paymentshard.IssueTicketsCommand) (paymentshard.IssueTicketsReceipt, error) {
	fake.issueCalls++
	return paymentshard.IssueTicketsReceipt{}, nil
}
func (*paymentWorkerShardFake) MarkRefundPending(context.Context, paymentshard.MarkRefundPendingCommand) (paymentshard.MarkRefundPendingReceipt, error) {
	return paymentshard.MarkRefundPendingReceipt{}, nil
}
func (*paymentWorkerShardFake) CancelVoidedReservation(context.Context, paymentshard.CancelVoidedReservationCommand) (paymentshard.CancelVoidedReservationReceipt, error) {
	return paymentshard.CancelVoidedReservationReceipt{}, nil
}
func (*paymentWorkerShardFake) ApplyRefundCompensation(context.Context, paymentshard.ApplyRefundCompensationCommand) (paymentshard.ApplyRefundCompensationReceipt, error) {
	return paymentshard.ApplyRefundCompensationReceipt{}, nil
}

func TestRetryMaximumIsBounded(t *testing.T) {
	t.Parallel()
	if got := retryMaximum(time.Second); got != 32*time.Second {
		t.Fatalf("retryMaximum(1s) = %s", got)
	}
	if got := retryMaximum(5 * time.Minute); got != time.Hour {
		t.Fatalf("retryMaximum(5m) = %s", got)
	}
}

func TestReadinessTimeoutCoversProviderAndShard(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.DatabaseTimeout = time.Second
	cfg.PaymentProviderRequestTimeout = 3 * time.Second
	cfg.PhysicalShardQueryTimeout = 2 * time.Second
	if got := readinessTimeout(cfg); got != 3*time.Second {
		t.Fatalf("readinessTimeout() = %s", got)
	}
}

func TestPublicReasonDoesNotExposeUnexpectedErrorText(t *testing.T) {
	t.Parallel()
	secret := "postgres://sentinel-secret"
	got := publicReason(errors.New(secret))
	if got != "payment worker failure" || strings.Contains(got, secret) {
		t.Fatalf("publicReason() = %q", got)
	}
}

func TestPaymentWorkerControlSchemaVersionMatchesLatestMigration(t *testing.T) {
	t.Parallel()
	if paymentControlSchemaVersion != 11 {
		t.Fatalf("paymentControlSchemaVersion = %d, want 11", paymentControlSchemaVersion)
	}
}

func TestNewPaymentProviderSelectsConfiguredAdapter(t *testing.T) {
	t.Parallel()

	sandboxConfig := config.Defaults()
	sandboxConfig.PaymentProviderType = config.PaymentProviderSandbox
	sandboxConfig.PaymentProviderBaseURL = "https://sandbox.example"
	sandboxConfig.PaymentProviderAPIKey = "sandbox-contract-key"
	sandbox, err := newPaymentProvider(sandboxConfig)
	if err != nil {
		t.Fatalf("newPaymentProvider(sandbox): %v", err)
	}
	defer sandbox.CloseIdleConnections()
	if _, ok := sandbox.(*providerhttp.Client); !ok {
		t.Fatalf("sandbox adapter = %T", sandbox)
	}
	if _, ok := any(sandbox).(interface {
		LookupRefund(context.Context, provider.RefundLookupRequest) (provider.RefundLookupResult, error)
	}); !ok {
		t.Fatalf("sandbox adapter cannot compose the partial-refund recovery processor: %T", sandbox)
	}

	stripeConfig := config.Defaults()
	stripeConfig.PaymentProviderType = config.PaymentProviderStripe
	stripeConfig.PaymentProviderBaseURL = "https://api.stripe.com"
	stripeConfig.PaymentProviderAPIKey = "sk_test_contract"
	stripeConfig.PaymentProviderAccountID = "acct_contract"
	stripeConfig.PaymentProviderSuccessURL = "https://merchant.example/payments/success"
	stripeConfig.PaymentProviderCancelURL = "https://merchant.example/payments/cancel"
	stripe, err := newPaymentProvider(stripeConfig)
	if err != nil {
		t.Fatalf("newPaymentProvider(stripe): %v", err)
	}
	defer stripe.CloseIdleConnections()
	if _, ok := stripe.(*providerstripe.Client); !ok {
		t.Fatalf("stripe adapter = %T", stripe)
	}
}

func TestNewPaymentProviderRejectsUnsupportedType(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.PaymentProviderType = config.PaymentProviderDisabled
	if _, err := newPaymentProvider(cfg); err == nil {
		t.Fatal("newPaymentProvider(disabled) succeeded")
	}
}

func TestNewPaymentProviderAllowsHTTPStripeContractOnlyInTest(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.PaymentProviderType = config.PaymentProviderStripe
	cfg.PaymentProviderBaseURL = "http://stripe-contract.test"
	cfg.PaymentProviderAPIKey = "sk_test_contract"
	cfg.PaymentProviderAccountID = "acct_contract"
	cfg.PaymentProviderSuccessURL = "https://merchant.example/payments/success"
	cfg.PaymentProviderCancelURL = "https://merchant.example/payments/cancel"

	cfg.Environment = config.EnvironmentProduction
	if provider, err := newPaymentProvider(cfg); err == nil {
		provider.CloseIdleConnections()
		t.Fatal("production accepted an insecure Stripe origin")
	}
	cfg.Environment = config.EnvironmentTest
	provider, err := newPaymentProvider(cfg)
	if err != nil {
		t.Fatalf("test contract rejected: %v", err)
	}
	provider.CloseIdleConnections()
}

func TestPaymentMetricsAdapterUsesMeasuredValuesAndDurableUncertainty(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	adapter := paymentMetrics{metrics: metrics}
	adapter.RecordPaymentWorker(paymentworker.MetricObservation{
		Lane: "operation", Provider: "sandbox", Operation: "capture", Result: "retry",
		Reason: "provider_outcome_unknown", Duration: 125 * time.Millisecond, Uncertain: true,
	})
	adapter.RecordPaymentWorker(paymentworker.MetricObservation{
		Lane: "webhook", Provider: "sandbox", Operation: "payment.captured", Result: "success",
		Duration: 75 * time.Millisecond, Lag: 3 * time.Second,
	})
	adapter.RecordPaymentWorker(paymentworker.MetricObservation{
		Lane: "action", Provider: "sandbox", Operation: string(paymentworker.ActionIssueTickets),
		Result: "success", Reason: "none", Duration: 250 * time.Millisecond,
	})

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	assertHistogramSum(t, families, "payment_operation_duration_seconds", 0.125)
	assertHistogramSum(t, families, "payment_webhook_processing_duration_seconds", 0.075)
	assertHistogramSum(t, families, "payment_webhook_lag_seconds", 3)
	assertHistogramSum(t, families, "ticket_issuance_duration_seconds", 0.25)
	assertCounterValue(t, families, "payment_operation_uncertain_total", 1)
	assertCounterValue(t, families, "payment_saga_transition_total", 1)
}

func assertHistogramSum(t *testing.T, families []*dto.MetricFamily, name string, want float64) {
	t.Helper()
	for _, family := range families {
		if family.GetName() == name {
			if got := family.Metric[0].GetHistogram().GetSampleSum(); got != want {
				t.Fatalf("%s sum = %v, want %v", name, got, want)
			}
			return
		}
	}
	t.Fatalf("metric family %s not found", name)
}

func assertCounterValue(t *testing.T, families []*dto.MetricFamily, name string, want float64) {
	t.Helper()
	for _, family := range families {
		if family.GetName() == name {
			var got float64
			for _, metric := range family.Metric {
				got += metric.GetCounter().GetValue()
			}
			if got != want {
				t.Fatalf("%s total = %v, want %v", name, got, want)
			}
			return
		}
	}
	t.Fatalf("metric family %s not found", name)
}
