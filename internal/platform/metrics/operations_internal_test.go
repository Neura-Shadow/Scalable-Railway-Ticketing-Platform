package metrics

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type durableOperationsSourceFake struct {
	mu       sync.Mutex
	snapshot durableOperationsSnapshot
	err      error
	calls    int
}

func (fake *durableOperationsSourceFake) Snapshot(context.Context) (durableOperationsSnapshot, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls++
	return fake.snapshot, fake.err
}

func TestDurableOperationsCollectorGathersStableSnapshotWithoutReplayInflation(t *testing.T) {
	t.Parallel()
	source := &durableOperationsSourceFake{snapshot: durableOperationsSnapshot{
		counters: []durableCounter{
			{"provider_capability_failure_total", []string{"stripe", "partial_refund", "unsupported"}, 1},
			{"financial_ledger_transaction_total", []string{"capture", "twd", "success"}, 3},
			{"financial_ledger_imbalance_total", []string{"settlement", "unknown"}, 1},
			{"financial_ledger_reversal_total", []string{"reversal", "twd", "success"}, 1},
			{"settlement_import_total", []string{"stripe", "payout", "success"}, 4},
			{"settlement_import_failure_total", []string{"stripe", "payout", "conflict"}, 1},
			{"settlement_reconciliation_total", []string{"stripe", "manual_review"}, 1},
			{"settlement_reconciliation_mismatch_total", []string{"stripe", "payout", "amount"}, 1},
			{"payout_reconciliation_mismatch_total", []string{"stripe", "twd", "amount"}, 1},
			{"webhook_key_rotation_failure_total", []string{"stripe", "key_rotation"}, 1},
			{"regional_failover_total", []string{"region-b", "target_active", "success"}, 1},
			{"regional_failback_total", []string{"region-a", "target_active", "success"}, 1},
			{"backup_total", []string{"control", "none", "success"}, 2},
			{"backup_failure_total", []string{"control", "none", "checksum"}, 1},
			{"backup_checksum_failure_total", []string{"control", "none", "checksum"}, 1},
		},
		gauges: []durableGauge{
			{"regional_active_epoch", []string{"region-b"}, 2},
			{"regional_replication_lag_bytes", []string{"region-b", "control", "none"}, 64},
			{"regional_replication_lag_seconds", []string{"region-b", "control", "none"}, 2},
			{"regional_last_replay_timestamp_seconds", []string{"region-b", "control", "none"}, 1786400000},
			{"regional_rpo_observed_seconds", []string{"region-b", "control", "none"}, 3},
			{"backup_age_seconds", []string{"control", "none"}, 60},
			{"backup_restore_test_age_seconds", []string{"control", "none"}, 120},
		},
		histograms: []durableHistogram{
			{"settlement_lag_seconds", []string{"stripe", "payout"}, 1, 5},
			{"regional_failover_duration_seconds", []string{"region-b", "target_active", "success"}, 1, 10},
			{"regional_failback_duration_seconds", []string{"region-a", "target_active", "success"}, 1, 11},
			{"regional_rto_observed_seconds", []string{"region-b", "rto_recorded", "success"}, 1, 30},
			{"backup_restore_duration_seconds", []string{"control", "none", "success"}, 1, 20},
		},
	}}
	collector, err := newDurableOperationsCollector(source, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewRegistry()
	if _, err := NewEventMetrics(registry); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(collector); err != nil {
		t.Fatal(err)
	}
	first, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if metricFamilyCounter(first, "financial_ledger_transaction_total") != 3 ||
		metricFamilyCounter(second, "financial_ledger_transaction_total") != 3 {
		t.Fatal("repeated scrape inflated durable ledger counter")
	}
	const readers = 20
	var group sync.WaitGroup
	for range readers {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, gatherErr := registry.Gather(); gatherErr != nil {
				t.Errorf("concurrent gather: %v", gatherErr)
			}
		}()
	}
	group.Wait()
}

func metricFamilyCounter(families []*dto.MetricFamily, name string) float64 {
	for _, family := range families {
		if family.GetName() == name && len(family.Metric) == 1 {
			return family.Metric[0].GetCounter().GetValue()
		}
	}
	return -1
}

var (
	descriptorNamePattern   = regexp.MustCompile(`fqName: "([^"]+)"`)
	descriptorLabelsPattern = regexp.MustCompile(`variableLabels: \{([^}]*)\}`)
)

func TestMilestone7MetricDescriptorsUseOnlyAllowedLabelNames(t *testing.T) {
	t.Parallel()
	allowed := set("provider", "operation", "result", "reason", "capability", "currency", "region", "database_role", "shard_id", "phase")
	required := set(
		"provider_adapter_request_total", "provider_adapter_request_duration_seconds", "provider_adapter_error_total", "provider_capability_failure_total",
		"financial_ledger_transaction_total", "financial_ledger_imbalance_total", "financial_ledger_reversal_total",
		"settlement_import_total", "settlement_import_failure_total", "settlement_reconciliation_total", "settlement_reconciliation_mismatch_total", "payout_reconciliation_mismatch_total", "settlement_lag_seconds",
		"partial_refund_total", "partial_refund_failure_total", "partial_refund_duration_seconds", "partial_refund_ticket_total",
		"webhook_ack_total", "webhook_ack_failure_total", "webhook_durable_commit_duration_seconds", "webhook_key_rotation_failure_total",
		"regional_active_epoch", "regional_failover_total", "regional_failover_duration_seconds", "regional_failback_total", "regional_failback_duration_seconds", "regional_write_rejected_total", "regional_replication_lag_bytes", "regional_replication_lag_seconds", "regional_last_replay_timestamp_seconds", "regional_rpo_observed_seconds", "regional_rto_observed_seconds",
		"backup_total", "backup_failure_total", "backup_age_seconds", "backup_restore_test_age_seconds", "backup_restore_duration_seconds", "backup_checksum_failure_total",
	)

	descriptions := make(chan *prometheus.Desc, len(required))
	for _, collector := range newOperationsMetrics().collectors() {
		collector.Describe(descriptions)
	}
	close(descriptions)

	seen := make(map[string]struct{}, len(required))
	for description := range descriptions {
		text := description.String()
		nameMatch := descriptorNamePattern.FindStringSubmatch(text)
		if len(nameMatch) != 2 {
			t.Fatalf("could not parse metric descriptor name: %s", text)
		}
		name := nameMatch[1]
		if _, ok := required[name]; !ok {
			t.Errorf("unexpected Milestone 7 metric descriptor %q", name)
			continue
		}
		seen[name] = struct{}{}

		labelsMatch := descriptorLabelsPattern.FindStringSubmatch(text)
		if len(labelsMatch) != 2 || strings.TrimSpace(labelsMatch[1]) == "" {
			continue
		}
		for _, label := range strings.Split(labelsMatch[1], ",") {
			label = strings.TrimSpace(label)
			if _, ok := allowed[label]; !ok {
				t.Errorf("metric %q uses disallowed label name %q", name, label)
			}
		}
	}
	for name := range required {
		if _, ok := seen[name]; !ok {
			t.Errorf("required Milestone 7 descriptor %q is missing", name)
		}
	}
}
