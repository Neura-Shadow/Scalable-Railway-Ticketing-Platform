package metrics_test

import (
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker"
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

func TestPaymentWorkerLaneFailureMetricUsesBoundedLabels(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordPaymentWorkerLaneFailure("claim_actions", "store_unavailable")
	metrics.RecordPaymentWorkerLaneFailure("postgres://user:secret@example", "synthetic-password-never-log")
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "payment_worker_lane_failure_total" {
			continue
		}
		seen := map[string]bool{}
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			seen[labels["lane"]+"/"+labels["reason"]] = metric.GetCounter().GetValue() == 1
		}
		if !seen["claim_actions/store_unavailable"] || !seen["unknown/unknown"] {
			t.Fatalf("bounded lane failure labels = %#v", seen)
		}
		return
	}
	t.Fatal("payment_worker_lane_failure_total was not gathered")
}

func TestPaymentWorkerLaneFailureMetricAcceptsEveryWorkerEnum(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	lanes := []worker.FailureLane{
		worker.FailureLaneClaimOperations, worker.FailureLaneProcessOperation,
		worker.FailureLaneClaimWebhooks, worker.FailureLaneProcessWebhook,
		worker.FailureLaneClaimActions, worker.FailureLaneProcessAction,
	}
	reasons := []worker.FailureReason{
		worker.FailureReasonStoreUnavailable, worker.FailureReasonLeaseLost,
		worker.FailureReasonInvalidClaim, worker.FailureReasonProviderUnavailable,
		worker.FailureReasonProviderOutcomeUnknown, worker.FailureReasonDatabaseFinalizeFailed,
		worker.FailureReasonShardUnavailable, worker.FailureReasonRegionalAuthorityUnavailable,
		worker.FailureReasonConstraintRejected, worker.FailureReasonTimeout, worker.FailureReasonUnknown,
	}
	for _, lane := range lanes {
		metrics.RecordPaymentWorkerLaneFailure(string(lane), string(worker.FailureReasonUnknown))
	}
	for _, reason := range reasons {
		metrics.RecordPaymentWorkerLaneFailure(string(worker.FailureLaneClaimActions), string(reason))
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seenLanes, seenReasons := map[string]bool{}, map[string]bool{}
	for _, family := range families {
		if family.GetName() != "payment_worker_lane_failure_total" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				switch label.GetName() {
				case "lane":
					seenLanes[label.GetValue()] = true
				case "reason":
					seenReasons[label.GetValue()] = true
				}
			}
		}
	}
	for _, lane := range lanes {
		if !seenLanes[string(lane)] {
			t.Fatalf("worker failure lane %q collapsed in metrics", lane)
		}
	}
	for _, reason := range reasons {
		if !seenReasons[string(reason)] {
			t.Fatalf("worker failure reason %q collapsed in metrics", reason)
		}
	}
}
