package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	allowedShardIDs        = set("legacy", "shard-0", "shard-1")
	allowedShardOperations = set(
		"resolve", "refresh", "read", "write", "create", "confirm", "cancel",
		"expire", "reconcile", "fanout", "copy", "validate", "cutover", "rollback",
	)
	allowedShardResults = set(
		"success", "failure", "hit", "miss", "expired", "stale", "rejected",
		"complete", "partial", "unavailable", "skipped",
	)
	allowedShardReasons = set(
		"none", "cache_hit", "cache_miss", "cache_expired", "stale_generation",
		"wrong_shard", "write_disabled", "shard_disabled", "catalog", "timeout",
		"migration", "validation", "partial", "database", "not_found", "cancelled",
	)
	allowedShardPhases = set(
		"planned", "draining", "copying", "validating", "cutover_ready",
		"cutting_over", "rollback_window", "completed", "failed", "rolled_back",
	)
)

type shardingMetrics struct {
	route               *prometheus.CounterVec
	routeCache          *prometheus.CounterVec
	routeRefresh        *prometheus.CounterVec
	assignmentStale     *prometheus.CounterVec
	writeFenceRejected  *prometheus.CounterVec
	requestDuration     *prometheus.HistogramVec
	unavailable         *prometheus.CounterVec
	fanout              *prometheus.CounterVec
	fanoutPartial       *prometheus.CounterVec
	migration           *prometheus.CounterVec
	migrationDuration   *prometheus.HistogramVec
	migrationRowsCopied *prometheus.CounterVec
	validationFailure   *prometheus.CounterVec
	cutover             *prometheus.CounterVec
	rollback            *prometheus.CounterVec
	reconciliation      *prometheus.CounterVec
}

func newShardingMetrics() *shardingMetrics {
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	}
	histogram := func(name, help string, labels ...string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: name, Help: help, Buckets: prometheus.DefBuckets,
		}, labels)
	}
	return &shardingMetrics{
		route:               counter("shard_route_total", "Authoritative shard route resolutions.", "operation", "result", "reason", "shard_id"),
		routeCache:          counter("shard_route_cache_total", "Bounded process-local shard route cache lookups.", "result", "reason", "shard_id"),
		routeRefresh:        counter("shard_route_refresh_total", "Authoritative shard route refreshes.", "operation", "result", "shard_id"),
		assignmentStale:     counter("shard_assignment_stale_total", "Database-rejected stale shard assignments.", "operation", "shard_id"),
		writeFenceRejected:  counter("shard_write_fence_rejected_total", "Shard writes rejected by database fencing.", "operation", "reason", "shard_id"),
		requestDuration:     histogram("shard_request_duration_seconds", "Routed shard request duration.", "operation", "result", "shard_id"),
		unavailable:         counter("shard_unavailable_total", "Routed shard operations rejected as unavailable.", "operation", "reason", "shard_id"),
		fanout:              counter("shard_fanout_total", "Bounded operator shard fanout operations.", "operation", "result"),
		fanoutPartial:       counter("shard_fanout_partial_total", "Bounded operator fanout partial results.", "operation", "reason"),
		migration:           counter("shard_migration_total", "Shard migration transitions and operations.", "phase", "result", "reason", "shard_id"),
		migrationDuration:   histogram("shard_migration_duration_seconds", "Shard migration phase duration.", "phase", "result", "shard_id"),
		migrationRowsCopied: counter("shard_migration_rows_copied_total", "Rows copied by bounded shard migration phases.", "phase", "shard_id"),
		validationFailure:   counter("shard_migration_validation_failure_total", "Shard migration validation failures.", "reason", "shard_id"),
		cutover:             counter("shard_cutover_total", "Shard cutover attempts.", "result", "reason", "shard_id"),
		rollback:            counter("shard_rollback_total", "Shard rollback attempts.", "result", "reason", "shard_id"),
		reconciliation:      counter("shard_reconciliation_mismatch_total", "Detected shard reconciliation mismatches.", "reason", "shard_id"),
	}
}

func (m *shardingMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.route, m.routeCache, m.routeRefresh, m.assignmentStale,
		m.writeFenceRejected, m.requestDuration, m.unavailable,
		m.fanout, m.fanoutPartial, m.migration, m.migrationDuration,
		m.migrationRowsCopied, m.validationFailure, m.cutover, m.rollback,
		m.reconciliation,
	}
}

// NormalizeShardID allows only logical shard IDs from the fixed topology.
// Database schema names and arbitrary configuration values collapse to unknown.
func NormalizeShardID(shardID string) string {
	return normalize(shardID, allowedShardIDs, "unknown")
}

func normalizeShardOperation(operation string) string {
	return normalize(operation, allowedShardOperations, "unknown")
}

func normalizeShardResult(result string) string {
	return normalize(result, allowedShardResults, "unknown")
}

func normalizeShardReason(reason string) string {
	if reason == "" {
		return "none"
	}
	return normalize(reason, allowedShardReasons, "unknown")
}

func normalizeShardPhase(phase string) string {
	return normalize(phase, allowedShardPhases, "unknown")
}

func (m *Metrics) RecordShardRoute(operation, result, reason, shardID string, duration time.Duration) {
	operation = normalizeShardOperation(operation)
	result = normalizeShardResult(result)
	shardID = NormalizeShardID(shardID)
	m.sharding.route.WithLabelValues(operation, result, normalizeShardReason(reason), shardID).Inc()
	m.sharding.requestDuration.WithLabelValues(operation, result, shardID).Observe(nonNegativeSeconds(duration))
}

func (m *Metrics) RecordShardRouteCache(result, reason, shardID string) {
	m.sharding.routeCache.WithLabelValues(normalizeShardResult(result), normalizeShardReason(reason), NormalizeShardID(shardID)).Inc()
}

func (m *Metrics) RecordShardRouteRefresh(operation, result, shardID string) {
	m.sharding.routeRefresh.WithLabelValues(normalizeShardOperation(operation), normalizeShardResult(result), NormalizeShardID(shardID)).Inc()
}

func (m *Metrics) RecordShardAssignmentStale(operation, shardID string) {
	m.sharding.assignmentStale.WithLabelValues(normalizeShardOperation(operation), NormalizeShardID(shardID)).Inc()
}

func (m *Metrics) RecordShardWriteFenceRejected(operation, reason, shardID string) {
	m.sharding.writeFenceRejected.WithLabelValues(normalizeShardOperation(operation), normalizeShardReason(reason), NormalizeShardID(shardID)).Inc()
}

func (m *Metrics) RecordShardUnavailable(operation, reason, shardID string) {
	m.sharding.unavailable.WithLabelValues(normalizeShardOperation(operation), normalizeShardReason(reason), NormalizeShardID(shardID)).Inc()
}

func (m *Metrics) RecordShardFanout(operation, result string, partial bool, duration time.Duration) {
	operation = normalizeShardOperation(operation)
	result = normalizeShardResult(result)
	m.sharding.fanout.WithLabelValues(operation, result).Inc()
	m.sharding.requestDuration.WithLabelValues(operation, result, "unknown").Observe(nonNegativeSeconds(duration))
	if partial {
		m.sharding.fanoutPartial.WithLabelValues(operation, "partial").Inc()
	}
}

func (m *Metrics) RecordShardMigration(phase, result, reason, shardID string, rows int64, duration time.Duration) {
	phase = normalizeShardPhase(phase)
	result = normalizeShardResult(result)
	reason = normalizeShardReason(reason)
	shardID = NormalizeShardID(shardID)
	m.sharding.migration.WithLabelValues(phase, result, reason, shardID).Inc()
	m.sharding.migrationDuration.WithLabelValues(phase, result, shardID).Observe(nonNegativeSeconds(duration))
	if rows > 0 {
		m.sharding.migrationRowsCopied.WithLabelValues(phase, shardID).Add(float64(rows))
	}
	if phase == "validating" && result == "failure" {
		m.sharding.validationFailure.WithLabelValues(reason, shardID).Inc()
	}
}

func (m *Metrics) RecordShardCutover(result, reason, shardID string) {
	m.sharding.cutover.WithLabelValues(normalizeShardResult(result), normalizeShardReason(reason), NormalizeShardID(shardID)).Inc()
}

func (m *Metrics) RecordShardRollback(result, reason, shardID string) {
	m.sharding.rollback.WithLabelValues(normalizeShardResult(result), normalizeShardReason(reason), NormalizeShardID(shardID)).Inc()
}

func (m *Metrics) AddShardReconciliationMismatches(reason, shardID string, count int) {
	if count <= 0 {
		return
	}
	m.sharding.reconciliation.WithLabelValues(normalizeShardReason(reason), NormalizeShardID(shardID)).Add(float64(count))
}
