package metrics_test

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestPhysicalMetricsExposeRequiredFamiliesWithoutUnboundedLabels(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	recorder, err := metrics.New(registry)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	malicious := "postgres://user:secret@host/train-run-id"
	recorder.RecordBookingCommand(malicious, malicious, malicious)
	recorder.RecordBookingCommandRecovery(malicious, malicious)
	recorder.RecordBookingCommandFinalizeFailure(malicious)
	recorder.RecordBookingQuotaLease(malicious, malicious, malicious)
	recorder.RecordBookingDirectoryRepair(malicious, malicious)
	recorder.RecordPhysicalShardRoute(malicious, malicious, malicious, malicious, malicious, time.Second)
	recorder.RecordPhysicalShardRouteRefresh(malicious, malicious, malicious)
	recorder.RecordPhysicalShardUnavailable(malicious, malicious, malicious)
	recorder.RecordPhysicalShardFenceRejected(malicious, malicious, malicious)
	recorder.RecordPhysicalShardPoolFailure(malicious, malicious)
	recorder.RecordPhysicalMigration(malicious, malicious, malicious, malicious)
	recorder.AddPhysicalBaseCopyRows(malicious, 2, time.Second, malicious)
	recorder.SetPhysicalJournalLag(malicious, 3)
	recorder.RecordPhysicalJournalReplay(malicious, malicious, malicious)
	recorder.RecordPhysicalValidationFailure(malicious, malicious)
	recorder.ObservePhysicalWritePause(malicious, malicious, time.Second)
	recorder.RecordPhysicalCutover(malicious, malicious, malicious)
	recorder.RecordPhysicalRollback(malicious, malicious, malicious)
	recorder.RecordPhysicalReverse(malicious, malicious, malicious)
	recorder.AddPhysicalReconciliationMismatches(malicious, malicious, 1)

	want := []string{
		"booking_command_total", "booking_command_recovery_total",
		"booking_command_finalize_failure_total", "booking_quota_lease_total",
		"booking_directory_repair_total", "physical_shard_route_total",
		"physical_shard_route_refresh_total", "physical_shard_unavailable_total",
		"physical_shard_fence_rejected_total", "physical_shard_request_duration_seconds",
		"physical_shard_pool_failure_total", "physical_migration_total",
		"physical_migration_base_copy_rows_total", "physical_migration_base_copy_duration_seconds",
		"physical_migration_journal_lag", "physical_migration_journal_replay_total",
		"physical_migration_validation_failure_total", "physical_migration_write_pause_seconds",
		"physical_migration_cutover_total", "physical_migration_rollback_total",
		"physical_migration_reverse_total", "physical_shard_reconciliation_mismatch_total",
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	seen := make(map[string]bool, len(want))
	for _, family := range families {
		for _, name := range want {
			if family.GetName() == name {
				seen[name] = true
			}
		}
		for _, sample := range family.GetMetric() {
			for _, label := range sample.GetLabel() {
				if strings.Contains(label.GetValue(), malicious) || strings.Contains(label.GetValue(), "postgres://") {
					t.Fatalf("metric %s leaked unbounded label %q", family.GetName(), label.GetValue())
				}
			}
		}
	}
	for _, name := range want {
		if !seen[name] {
			t.Errorf("metric family %s was not exposed", name)
		}
	}

	response := httptest.NewRecorder()
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if strings.Contains(response.Body.String(), malicious) || strings.Contains(response.Body.String(), "postgres://") {
		t.Fatal("Prometheus exposition leaked an unbounded physical label")
	}
}

func TestPhysicalBookingCommandOperationsRemainBoundedAndDistinct(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	recorder, err := metrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"create", "confirm", "cancel"} {
		recorder.RecordBookingCommand(operation, "success", "none")
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	operations := make(map[string]bool, 3)
	for _, family := range families {
		if family.GetName() != "booking_command_total" {
			continue
		}
		for _, sample := range family.GetMetric() {
			for _, label := range sample.GetLabel() {
				if label.GetName() == "operation" {
					operations[label.GetValue()] = true
				}
			}
		}
	}
	for _, operation := range []string{"create", "confirm", "cancel"} {
		if !operations[operation] {
			t.Errorf("booking command operation %q was not preserved: %v", operation, operations)
		}
	}
	if operations["unknown"] {
		t.Fatalf("bounded booking operations collapsed to unknown: %v", operations)
	}
}
