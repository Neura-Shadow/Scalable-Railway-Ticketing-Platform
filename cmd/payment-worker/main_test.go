package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	paymentworker "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

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
	if paymentControlSchemaVersion != 10 {
		t.Fatalf("paymentControlSchemaVersion = %d, want 10", paymentControlSchemaVersion)
	}
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
