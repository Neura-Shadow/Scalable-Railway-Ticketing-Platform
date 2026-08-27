package metrics_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	platformmetrics "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var requiredM7MetricFamilies = []string{
	"provider_adapter_request_total", "provider_adapter_request_duration_seconds", "provider_adapter_error_total", "provider_capability_failure_total",
	"financial_ledger_transaction_total", "financial_ledger_imbalance_total", "financial_ledger_reversal_total",
	"settlement_import_total", "settlement_import_failure_total", "settlement_reconciliation_total", "settlement_reconciliation_mismatch_total", "payout_reconciliation_mismatch_total", "settlement_lag_seconds",
	"partial_refund_total", "partial_refund_failure_total", "partial_refund_duration_seconds", "partial_refund_ticket_total",
	"webhook_ack_total", "webhook_ack_failure_total", "webhook_durable_commit_duration_seconds", "webhook_key_rotation_failure_total",
	"regional_active_epoch", "regional_failover_total", "regional_failover_duration_seconds", "regional_failback_total", "regional_failback_duration_seconds", "regional_write_rejected_total", "regional_replication_lag_bytes", "regional_replication_lag_seconds", "regional_last_replay_timestamp_seconds", "regional_rpo_observed_seconds", "regional_rto_observed_seconds",
	"backup_total", "backup_failure_total", "backup_age_seconds", "backup_restore_test_age_seconds", "backup_restore_duration_seconds", "backup_checksum_failure_total",
}

func assertMetricFamilies(t *testing.T, registry *prometheus.Registry, names ...string) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, len(families))
	for _, family := range families {
		seen[family.GetName()] = true
	}
	sort.Strings(names)
	for _, name := range names {
		if !seen[name] {
			t.Errorf("required Milestone 7 metric %q was not gathered", name)
		}
	}
}

func TestProviderOperationsExposeExactMetricFamilies(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordProviderAdapter("stripe", "capture", "success", "none", time.Second)
	metrics.RecordProviderCapabilityFailure("stripe", "partial_refund", "unsupported")

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, len(families))
	for _, family := range families {
		seen[family.GetName()] = true
	}
	for _, name := range []string{
		"provider_adapter_request_total",
		"provider_adapter_request_duration_seconds",
		"provider_adapter_error_total",
		"provider_capability_failure_total",
	} {
		if !seen[name] {
			t.Fatalf("required Milestone 7 metric %q was not gathered", name)
		}
	}
}

func TestPaymentOperationCompatibilityRecorderFeedsProviderAdapterMetrics(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordPaymentOperation("stripe", "capture", "uncertain", time.Second, true)

	assertMetricFamilies(t, registry,
		"provider_adapter_request_total",
		"provider_adapter_request_duration_seconds",
		"provider_adapter_error_total",
	)
}

func TestFinancialLedgerExposesExactMetricFamilies(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordFinancialLedgerTransaction("refund", "TWD", "success", true, true)

	assertMetricFamilies(t, registry,
		"financial_ledger_transaction_total",
		"financial_ledger_imbalance_total",
		"financial_ledger_reversal_total",
	)
}

func TestSettlementOperationsExposeExactMetricFamilies(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordSettlementImport("stripe", "payout", "failure", 2)
	metrics.RecordSettlementReconciliation("stripe", "payout", "TWD", "failure", "amount", true, true, 5*time.Second)

	assertMetricFamilies(t, registry,
		"settlement_import_total",
		"settlement_import_failure_total",
		"settlement_reconciliation_total",
		"settlement_reconciliation_mismatch_total",
		"payout_reconciliation_mismatch_total",
		"settlement_lag_seconds",
	)
}

func TestSettlementImportFailureCountsAttemptWhenNoRecordsWereImported(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordSettlementImport("stripe", "payout", "failure", 0)

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "settlement_import_failure_total" {
			if got := family.Metric[0].GetCounter().GetValue(); got != 1 {
				t.Fatalf("settlement import failure count = %v, want 1", got)
			}
			return
		}
	}
	t.Fatal("settlement_import_failure_total was not gathered")
}

func TestPartialRefundExposesExactMetricFamilies(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordPartialRefund("stripe", "failure", "manual_review", "TWD", 2, time.Second)

	assertMetricFamilies(t, registry,
		"partial_refund_total",
		"partial_refund_failure_total",
		"partial_refund_duration_seconds",
		"partial_refund_ticket_total",
	)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "partial_refund_ticket_total" {
			continue
		}
		metric := family.Metric[0]
		labels := map[string]string{}
		for _, label := range metric.Label {
			labels[label.GetName()] = label.GetValue()
		}
		if metric.GetCounter().GetValue() != 2 || labels["provider"] != "stripe" || labels["currency"] != "twd" || labels["result"] != "failure" {
			t.Fatalf("partial refund ticket metric = value %v labels %v", metric.GetCounter().GetValue(), labels)
		}
		return
	}
	t.Fatal("partial_refund_ticket_total was not gathered")
}

func TestPartialRefundReplayDoesNotCreateZeroOnlyTicketSeries(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordPartialRefund("stripe", "duplicate", "duplicate", "TWD", 0, time.Second)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "partial_refund_ticket_total" {
			t.Fatal("replay created a zero-only partial-refund ticket series")
		}
	}
}

func TestWebhookOperationsExposeExactMetricFamilies(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordWebhookAck("stripe", "failure", "commit", time.Second)
	metrics.RecordWebhookKeyRotationFailure("stripe", "key_rotation")

	assertMetricFamilies(t, registry,
		"webhook_ack_total",
		"webhook_ack_failure_total",
		"webhook_durable_commit_duration_seconds",
		"webhook_key_rotation_failure_total",
	)
}

func TestRegionalOperationsExposeExactMetricFamilies(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordRegionalEpoch("region-b", 2)
	metrics.RecordRegionalFailover("region-b", "target_active", "success", time.Minute)
	metrics.RecordRegionalFailback("region-a", "target_active", "success", 2*time.Minute)
	metrics.RecordRegionalWriteRejected("region-a", "booking_shard", "shard-0", "stale_epoch")
	metrics.RecordRegionalReplication("region-b", "control", "none", 64, 2*time.Second, time.Unix(1_786_400_000, 0))
	metrics.RecordRegionalRPO("region-b", "control", "none", 3*time.Second)
	metrics.RecordRegionalRTO("region-b", "rto_recorded", "success", time.Minute)

	assertMetricFamilies(t, registry,
		"regional_active_epoch",
		"regional_failover_total",
		"regional_failover_duration_seconds",
		"regional_failback_total",
		"regional_failback_duration_seconds",
		"regional_write_rejected_total",
		"regional_replication_lag_bytes",
		"regional_replication_lag_seconds",
		"regional_last_replay_timestamp_seconds",
		"regional_rpo_observed_seconds",
		"regional_rto_observed_seconds",
	)
}

func TestBackupOperationsExposeExactMetricFamilies(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordBackupOperation("booking_shard", "shard-0", "failure", "checksum", time.Hour, true)
	metrics.RecordBackupRestoreTest("booking_shard", "shard-0", "success", "none", 24*time.Hour, time.Minute)

	assertMetricFamilies(t, registry,
		"backup_total",
		"backup_failure_total",
		"backup_age_seconds",
		"backup_restore_test_age_seconds",
		"backup_restore_duration_seconds",
		"backup_checksum_failure_total",
	)
}

func TestMilestone7MetricLabelsCollapseIdentifiersAndTopologyValues(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordSettlementImport("acct_customer_123", "record_456", "value_789", 1)
	metrics.RecordRegionalRecovery("incident-uuid", "host.example/wal-position", "operator-id", time.Second)
	metrics.RecordObservedRecoveryPoint("postgres://secret@host", "10.0.0.7", 1, time.Second)

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetValue() != "unknown" {
					t.Fatalf("caller-controlled label survived normalization: %s=%q", label.GetName(), label.GetValue())
				}
			}
		}
	}
}

func TestMilestone7CallerControlledValuesHaveBoundedCardinality(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics, err := platformmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 100; index++ {
		identifier := fmt.Sprintf("customer-controlled-id-%d.example/wal/%d", index, index)
		metrics.RecordProviderAdapter(identifier, identifier, identifier, identifier, time.Second)
		metrics.RecordProviderCapabilityFailure(identifier, identifier, identifier)
		metrics.RecordFinancialLedgerTransaction(identifier, identifier, identifier, true, true)
		metrics.RecordSettlementImport(identifier, identifier, identifier, 1)
		metrics.RecordSettlementReconciliation(identifier, identifier, identifier, identifier, identifier, true, true, time.Second)
		metrics.RecordPartialRefund(identifier, identifier, identifier, identifier, 1, time.Second)
		metrics.RecordWebhookAck(identifier, identifier, identifier, time.Second)
		metrics.RecordWebhookKeyRotationFailure(identifier, identifier)
		metrics.RecordRegionalEpoch(identifier, uint64(index))
		metrics.RecordRegionalFailover(identifier, identifier, identifier, time.Second)
		metrics.RecordRegionalFailback(identifier, identifier, identifier, time.Second)
		metrics.RecordRegionalWriteRejected(identifier, identifier, identifier, identifier)
		metrics.RecordRegionalReplication(identifier, identifier, identifier, int64(index), time.Second, time.Unix(int64(index+1), 0))
		metrics.RecordRegionalRPO(identifier, identifier, identifier, time.Second)
		metrics.RecordRegionalRTO(identifier, identifier, identifier, time.Second)
		metrics.RecordBackupOperation(identifier, identifier, identifier, identifier, time.Second, true)
		metrics.RecordBackupRestoreTest(identifier, identifier, identifier, identifier, time.Second, time.Second)
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	required := make(map[string]struct{}, len(requiredM7MetricFamilies))
	for _, name := range requiredM7MetricFamilies {
		required[name] = struct{}{}
	}
	for _, family := range families {
		if _, ok := required[family.GetName()]; !ok {
			continue
		}
		delete(required, family.GetName())
		if got := len(family.Metric); got != 1 {
			t.Errorf("%s produced %d series from arbitrary identifiers; want 1 bounded fallback series", family.GetName(), got)
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "le" {
					continue
				}
				if strings.Contains(label.GetValue(), "customer-controlled") || label.GetValue() != "unknown" {
					t.Errorf("%s retained caller-controlled label %s=%q", family.GetName(), label.GetName(), label.GetValue())
				}
			}
		}
	}
	for name := range required {
		t.Errorf("required Milestone 7 family %q was not emitted", name)
	}
}
