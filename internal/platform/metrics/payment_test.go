package metrics_test

import (
	"testing"
	"time"

	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func TestPaymentMetricsExposeRequiredBoundedFamilies(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordPaymentIntent("captured", "success", time.Second)
	metrics.RecordPaymentSagaTransition("captured", "issuing_tickets")
	metrics.RecordPaymentSagaFailure("timeout_unknown", true)
	metrics.RecordPaymentOperation("sandbox", "capture", "uncertain", time.Second, true)
	metrics.RecordPaymentOperation("sandbox", "void", "success", time.Second, false)
	metrics.RecordPaymentOperation("sandbox", "refund", "success", time.Second, false)
	metrics.RecordPaymentWebhook("sandbox", "payment.captured", "duplicate", time.Second, 2*time.Second)
	metrics.RecordPaymentWebhook("sandbox", "payment.captured", "conflict", time.Second, 2*time.Second)
	metrics.RecordPaymentWebhookInvalid("sandbox")
	metrics.RecordTicketIssuance("failure", "shard_unavailable", time.Second, true)
	metrics.RecordPaymentReconciliation("payment_all", "failure", true, false)
	metrics.RecordPaymentReconciliation("payment_all", "success", false, true)

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, len(families))
	for _, family := range families {
		seen[family.GetName()] = true
	}
	for _, name := range []string{
		"payment_intent_total", "payment_intent_duration_seconds", "payment_saga_transition_total",
		"payment_saga_failure_total", "payment_saga_manual_review_total", "payment_operation_total",
		"payment_operation_duration_seconds", "payment_operation_uncertain_total", "payment_capture_total",
		"payment_void_total", "payment_refund_total", "payment_webhook_total", "payment_webhook_duplicate_total",
		"payment_webhook_invalid_signature_total", "payment_webhook_conflict_total",
		"payment_webhook_processing_duration_seconds", "payment_webhook_lag_seconds", "ticket_issuance_total",
		"ticket_issuance_failure_total", "ticket_issuance_duration_seconds", "ticket_issuance_replay_total",
		"payment_reconciliation_total", "payment_reconciliation_mismatch_total", "payment_reconciliation_repair_total",
	} {
		if !seen[name] {
			t.Fatalf("required payment metric %q was not gathered", name)
		}
	}
}

func TestPaymentMetricLabelsCollapseCallerControlledValues(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordPaymentOperation("provider-user-123", "operation-user-456", "result-user-789", 0, true)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "payment_operation_total" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetValue() != "unknown" {
					t.Fatalf("unbounded label survived normalization: %s=%q", label.GetName(), label.GetValue())
				}
			}
		}
		return
	}
	t.Fatal("payment_operation_total was not gathered")
}

func TestPaymentMetricDurationsAreNonNegativeAndCapped(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordPaymentWebhook("sandbox", "payment.captured", "success", -time.Second, 90*24*time.Hour)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		switch family.GetName() {
		case "payment_webhook_processing_duration_seconds":
			if got := family.Metric[0].GetHistogram().GetSampleSum(); got != 0 {
				t.Fatalf("negative duration sample sum = %v", got)
			}
		case "payment_webhook_lag_seconds":
			if got, want := family.Metric[0].GetHistogram().GetSampleSum(), (30 * 24 * time.Hour).Seconds(); got != want {
				t.Fatalf("lag sample sum = %v, want %v", got, want)
			}
		}
	}
}

func TestPaymentOperationPreservesBoundedProviderFailureReason(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordPaymentOperationWithReason("stripe", "capture", "failure", "rate_limited", time.Second, false)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "provider_adapter_error_total" {
			continue
		}
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["provider"] == "stripe" && labels["operation"] == "capture" && labels["reason"] == "rate_limited" && metric.GetCounter().GetValue() == 1 {
				return
			}
		}
	}
	t.Fatal("actual bounded provider reason was not gathered")
}
