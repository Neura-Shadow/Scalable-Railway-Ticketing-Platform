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

func TestDatabasePoolSnapshotExposesBoundedPressureAndPeak(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	recorder, err := metrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	recorder.RecordDatabasePoolSnapshot("control", "none", metrics.DatabasePoolSnapshot{
		TotalConnections: 8, AcquiredConnections: 5, IdleConnections: 3,
		MaxConnections: 12, AcquireCount: 21, AcquireDuration: 250 * time.Millisecond,
		EmptyAcquireCount: 2, CancelledAcquireCount: 1,
	})
	recorder.RecordDatabasePoolSnapshot("control", "none", metrics.DatabasePoolSnapshot{
		TotalConnections: 7, AcquiredConnections: 3, IdleConnections: 4,
		MaxConnections: 12, AcquireCount: 22, AcquireDuration: 300 * time.Millisecond,
		EmptyAcquireCount: 2, CancelledAcquireCount: 1,
	})

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{
		"database_pool_acquired_connections":      3,
		"database_pool_idle_connections":          4,
		"database_pool_total_connections":         7,
		"database_pool_max_connections":           12,
		"database_pool_acquire_total":             22,
		"database_pool_acquire_duration_seconds":  0.3,
		"database_pool_empty_acquire_total":       2,
		"database_pool_cancelled_acquire_total":   1,
		"database_pool_peak_acquired_connections": 5,
	}
	for _, family := range families {
		name := family.GetName()
		value, required := want[name]
		if !required {
			continue
		}
		if len(family.GetMetric()) != 1 {
			t.Fatalf("%s sample count = %d, want 1", name, len(family.GetMetric()))
		}
		metric := family.GetMetric()[0]
		got := metric.GetGauge().GetValue()
		if got != value {
			t.Fatalf("%s = %v, want %v", name, got, value)
		}
		delete(want, name)
	}
	if len(want) != 0 {
		t.Fatalf("missing database pool metrics: %v", want)
	}
}
