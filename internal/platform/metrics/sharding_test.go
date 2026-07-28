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

func TestShardingMetricsExposeRequiredFamiliesWithBoundedLabels(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	recorder, err := metrics.New(registry)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	malicious := "booking_shard_0;drop schema public--postgres://secret@db/reservation-id"
	recorder.RecordShardRoute(malicious, malicious, malicious, malicious, time.Second)
	recorder.RecordShardRouteCache(malicious, malicious, malicious)
	recorder.RecordShardRouteRefresh(malicious, malicious, malicious)
	recorder.RecordShardAssignmentStale(malicious, malicious)
	recorder.RecordShardWriteFenceRejected(malicious, malicious, malicious)
	recorder.RecordShardUnavailable(malicious, malicious, malicious)
	recorder.RecordShardFanout(malicious, malicious, true, time.Second)
	recorder.RecordShardMigration(malicious, malicious, malicious, malicious, 7, time.Second)
	recorder.RecordShardMigration("validating", "failure", "validation", "shard-0", 0, time.Second)
	recorder.RecordShardCutover(malicious, malicious, malicious)
	recorder.RecordShardRollback(malicious, malicious, malicious)
	recorder.AddShardReconciliationMismatches(malicious, malicious, 3)

	response := httptest.NewRecorder()
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(
		response,
		httptest.NewRequest("GET", "/metrics", nil),
	)
	exposition := response.Body.String()
	if strings.Contains(exposition, malicious) || strings.Contains(exposition, "booking_shard_0") || strings.Contains(exposition, "postgres://") {
		t.Fatalf("sharding metrics leaked unbounded topology or caller data: %s", exposition)
	}

	want := map[string]struct{}{}
	for _, name := range []string{
		"shard_route_total",
		"shard_route_cache_total",
		"shard_route_refresh_total",
		"shard_assignment_stale_total",
		"shard_write_fence_rejected_total",
		"shard_request_duration_seconds",
		"shard_unavailable_total",
		"shard_fanout_total",
		"shard_fanout_partial_total",
		"shard_migration_total",
		"shard_migration_duration_seconds",
		"shard_migration_rows_copied_total",
		"shard_migration_validation_failure_total",
		"shard_cutover_total",
		"shard_rollback_total",
		"shard_reconciliation_mismatch_total",
	} {
		want[name] = struct{}{}
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	seen := make(map[string]struct{}, len(want))
	for _, family := range families {
		if _, ok := want[family.GetName()]; !ok {
			continue
		}
		seen[family.GetName()] = struct{}{}
		for _, sample := range family.GetMetric() {
			for _, label := range sample.GetLabel() {
				if strings.Contains(label.GetValue(), malicious) || strings.Contains(label.GetValue(), "booking_shard_0") {
					t.Fatalf("metric %s label %s leaked unbounded value %q", family.GetName(), label.GetName(), label.GetValue())
				}
			}
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("sharding metric families seen = %d, want %d", len(seen), len(want))
	}
}

func TestNormalizeShardIDRetainsOnlyFixedLogicalIDs(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"legacy", "shard-0", "shard-1"} {
		if got := metrics.NormalizeShardID(id); got != id {
			t.Fatalf("NormalizeShardID(%q) = %q", id, got)
		}
	}
	if got := metrics.NormalizeShardID(" SHARD-0 "); got != "shard-0" {
		t.Fatalf("NormalizeShardID(canonicalizable) = %q", got)
	}
	for _, id := range []string{"booking_shard_0", "shard-2", ""} {
		if got := metrics.NormalizeShardID(id); got != "unknown" {
			t.Fatalf("NormalizeShardID(%q) = %q, want unknown", id, got)
		}
	}
}
