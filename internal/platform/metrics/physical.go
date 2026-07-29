package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	allowedPhysicalShardIDs   = set("physical-shard-0", "physical-shard-1")
	allowedStorageKinds       = set("legacy_schema", "logical_schema", "postgres")
	allowedPhysicalOperations = set(
		"create", "finalize", "recover", "release", "repair", "resolve",
		"refresh", "read", "write", "copy", "replay", "validate",
		"cutover", "rollback", "reverse", "reconcile", "expire", "publish",
	)
	allowedPhysicalResults = set(
		"success", "failure", "conflict", "duplicate", "rejected", "deferred",
		"complete", "partial", "unavailable", "skipped", "pending", "committed",
	)
	allowedPhysicalReasons = set(
		"none", "database", "timeout", "catalog", "schema", "protocol",
		"unknown_connection_ref", "pool_budget", "wrong_shard", "stale_generation",
		"write_disabled", "migration", "validation", "quota", "receipt",
		"directory", "lease_expired", "target_write", "retention", "partial",
	)
	allowedPhysicalPhases = set(
		"planned", "preparing_target", "capture_enabled", "base_copying",
		"catching_up", "validating_online", "draining", "source_fenced",
		"final_catchup", "final_validating", "target_enabled",
		"switching_assignment", "rollback_window", "completed", "failed",
		"rolled_back", "reverse", "cleanup",
	)
)

type physicalMetrics struct {
	bookingCommand         *prometheus.CounterVec
	bookingRecovery        *prometheus.CounterVec
	bookingFinalizeFailure *prometheus.CounterVec
	quotaLease             *prometheus.CounterVec
	directoryRepair        *prometheus.CounterVec
	route                  *prometheus.CounterVec
	routeRefresh           *prometheus.CounterVec
	unavailable            *prometheus.CounterVec
	fenceRejected          *prometheus.CounterVec
	requestDuration        *prometheus.HistogramVec
	poolFailure            *prometheus.CounterVec
	migration              *prometheus.CounterVec
	baseCopyRows           *prometheus.CounterVec
	baseCopyDuration       *prometheus.HistogramVec
	journalLag             *prometheus.GaugeVec
	journalReplay          *prometheus.CounterVec
	validationFailure      *prometheus.CounterVec
	writePause             *prometheus.HistogramVec
	cutover                *prometheus.CounterVec
	rollback               *prometheus.CounterVec
	reverse                *prometheus.CounterVec
	reconciliation         *prometheus.CounterVec
}

func newPhysicalMetrics() *physicalMetrics {
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	}
	histogram := func(name, help string, labels ...string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: prometheus.DefBuckets}, labels)
	}
	return &physicalMetrics{
		bookingCommand:         counter("booking_command_total", "Durable physical booking commands.", "operation", "result", "reason"),
		bookingRecovery:        counter("booking_command_recovery_total", "Booking command recovery outcomes.", "result", "reason"),
		bookingFinalizeFailure: counter("booking_command_finalize_failure_total", "Deferred booking command finalizations.", "reason"),
		quotaLease:             counter("booking_quota_lease_total", "Conservative global booking quota leases.", "operation", "result", "reason"),
		directoryRepair:        counter("booking_directory_repair_total", "Reservation directory repair outcomes.", "result", "reason"),
		route:                  counter("physical_shard_route_total", "Physical shard route resolutions.", "operation", "result", "reason", "shard_id", "storage_kind"),
		routeRefresh:           counter("physical_shard_route_refresh_total", "Physical shard route refreshes.", "result", "reason", "shard_id"),
		unavailable:            counter("physical_shard_unavailable_total", "Bounded physical shard unavailability.", "operation", "reason", "shard_id"),
		fenceRejected:          counter("physical_shard_fence_rejected_total", "Database-local physical write fence rejections.", "operation", "reason", "shard_id"),
		requestDuration:        histogram("physical_shard_request_duration_seconds", "Physical shard request duration.", "operation", "result", "shard_id"),
		poolFailure:            counter("physical_shard_pool_failure_total", "Bounded physical shard pool failures.", "reason", "shard_id"),
		migration:              counter("physical_migration_total", "Physical migration operations and transitions.", "phase", "result", "reason", "shard_id"),
		baseCopyRows:           counter("physical_migration_base_copy_rows_total", "Rows copied by the bounded physical base copy.", "shard_id"),
		baseCopyDuration:       histogram("physical_migration_base_copy_duration_seconds", "Bounded physical base-copy duration.", "result", "shard_id"),
		journalLag:             prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "physical_migration_journal_lag", Help: "Current bounded physical migration journal lag."}, []string{"shard_id"}),
		journalReplay:          counter("physical_migration_journal_replay_total", "Physical mutation-journal replay outcomes.", "result", "reason", "shard_id"),
		validationFailure:      counter("physical_migration_validation_failure_total", "Physical migration validation failures.", "reason", "shard_id"),
		writePause:             histogram("physical_migration_write_pause_seconds", "Measured bounded final write pause.", "result", "shard_id"),
		cutover:                counter("physical_migration_cutover_total", "Physical migration cutover outcomes.", "result", "reason", "shard_id"),
		rollback:               counter("physical_migration_rollback_total", "Physical migration rollback outcomes.", "result", "reason", "shard_id"),
		reverse:                counter("physical_migration_reverse_total", "Physical reverse-migration outcomes.", "result", "reason", "shard_id"),
		reconciliation:         counter("physical_shard_reconciliation_mismatch_total", "Detected physical shard reconciliation mismatches.", "reason", "shard_id"),
	}
}

func (m *physicalMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.bookingCommand, m.bookingRecovery, m.bookingFinalizeFailure, m.quotaLease,
		m.directoryRepair, m.route, m.routeRefresh, m.unavailable, m.fenceRejected,
		m.requestDuration, m.poolFailure, m.migration, m.baseCopyRows,
		m.baseCopyDuration, m.journalLag, m.journalReplay, m.validationFailure,
		m.writePause, m.cutover, m.rollback, m.reverse, m.reconciliation,
	}
}

func normalizePhysicalShardID(value string) string {
	return normalize(value, allowedPhysicalShardIDs, "unknown")
}
func normalizeStorageKind(value string) string {
	return normalize(value, allowedStorageKinds, "unknown")
}
func normalizePhysicalOperation(value string) string {
	return normalize(value, allowedPhysicalOperations, "unknown")
}
func normalizePhysicalResult(value string) string {
	return normalize(value, allowedPhysicalResults, "unknown")
}
func normalizePhysicalReason(value string) string {
	if value == "" {
		return "none"
	}
	return normalize(value, allowedPhysicalReasons, "unknown")
}
func normalizePhysicalPhase(value string) string {
	return normalize(value, allowedPhysicalPhases, "unknown")
}

func (m *Metrics) RecordBookingCommand(operation, result, reason string) {
	m.physical.bookingCommand.WithLabelValues(normalizePhysicalOperation(operation), normalizePhysicalResult(result), normalizePhysicalReason(reason)).Inc()
}

func (m *Metrics) RecordBookingCommandRecovery(result, reason string) {
	m.physical.bookingRecovery.WithLabelValues(normalizePhysicalResult(result), normalizePhysicalReason(reason)).Inc()
}

func (m *Metrics) RecordBookingCommandFinalizeFailure(reason string) {
	m.physical.bookingFinalizeFailure.WithLabelValues(normalizePhysicalReason(reason)).Inc()
}

func (m *Metrics) RecordBookingQuotaLease(operation, result, reason string) {
	m.physical.quotaLease.WithLabelValues(normalizePhysicalOperation(operation), normalizePhysicalResult(result), normalizePhysicalReason(reason)).Inc()
}

func (m *Metrics) RecordBookingDirectoryRepair(result, reason string) {
	m.physical.directoryRepair.WithLabelValues(normalizePhysicalResult(result), normalizePhysicalReason(reason)).Inc()
}

func (m *Metrics) RecordPhysicalShardRoute(operation, result, reason, shardID, storageKind string, duration time.Duration) {
	operation = normalizePhysicalOperation(operation)
	result = normalizePhysicalResult(result)
	shardID = normalizePhysicalShardID(shardID)
	m.physical.route.WithLabelValues(operation, result, normalizePhysicalReason(reason), shardID, normalizeStorageKind(storageKind)).Inc()
	m.physical.requestDuration.WithLabelValues(operation, result, shardID).Observe(nonNegativeSeconds(duration))
}

func (m *Metrics) RecordPhysicalShardRouteRefresh(result, reason, shardID string) {
	m.physical.routeRefresh.WithLabelValues(normalizePhysicalResult(result), normalizePhysicalReason(reason), normalizePhysicalShardID(shardID)).Inc()
}

func (m *Metrics) RecordPhysicalShardUnavailable(operation, reason, shardID string) {
	m.physical.unavailable.WithLabelValues(normalizePhysicalOperation(operation), normalizePhysicalReason(reason), normalizePhysicalShardID(shardID)).Inc()
}

func (m *Metrics) RecordPhysicalShardFenceRejected(operation, reason, shardID string) {
	m.physical.fenceRejected.WithLabelValues(normalizePhysicalOperation(operation), normalizePhysicalReason(reason), normalizePhysicalShardID(shardID)).Inc()
}

func (m *Metrics) RecordPhysicalShardPoolFailure(reason, shardID string) {
	m.physical.poolFailure.WithLabelValues(normalizePhysicalReason(reason), normalizePhysicalShardID(shardID)).Inc()
}

func (m *Metrics) RecordPhysicalMigration(phase, result, reason, shardID string) {
	m.physical.migration.WithLabelValues(normalizePhysicalPhase(phase), normalizePhysicalResult(result), normalizePhysicalReason(reason), normalizePhysicalShardID(shardID)).Inc()
}

func (m *Metrics) AddPhysicalBaseCopyRows(shardID string, rows int64, duration time.Duration, result string) {
	shardID = normalizePhysicalShardID(shardID)
	if rows > 0 {
		m.physical.baseCopyRows.WithLabelValues(shardID).Add(float64(rows))
	}
	m.physical.baseCopyDuration.WithLabelValues(normalizePhysicalResult(result), shardID).Observe(nonNegativeSeconds(duration))
}

func (m *Metrics) SetPhysicalJournalLag(shardID string, lag int64) {
	if lag < 0 {
		lag = 0
	}
	m.physical.journalLag.WithLabelValues(normalizePhysicalShardID(shardID)).Set(float64(lag))
}

func (m *Metrics) RecordPhysicalJournalReplay(result, reason, shardID string) {
	m.physical.journalReplay.WithLabelValues(normalizePhysicalResult(result), normalizePhysicalReason(reason), normalizePhysicalShardID(shardID)).Inc()
}

func (m *Metrics) RecordPhysicalValidationFailure(reason, shardID string) {
	m.physical.validationFailure.WithLabelValues(normalizePhysicalReason(reason), normalizePhysicalShardID(shardID)).Inc()
}

func (m *Metrics) ObservePhysicalWritePause(result, shardID string, duration time.Duration) {
	m.physical.writePause.WithLabelValues(normalizePhysicalResult(result), normalizePhysicalShardID(shardID)).Observe(nonNegativeSeconds(duration))
}

func (m *Metrics) RecordPhysicalCutover(result, reason, shardID string) {
	m.physical.cutover.WithLabelValues(normalizePhysicalResult(result), normalizePhysicalReason(reason), normalizePhysicalShardID(shardID)).Inc()
}

func (m *Metrics) RecordPhysicalRollback(result, reason, shardID string) {
	m.physical.rollback.WithLabelValues(normalizePhysicalResult(result), normalizePhysicalReason(reason), normalizePhysicalShardID(shardID)).Inc()
}

func (m *Metrics) RecordPhysicalReverse(result, reason, shardID string) {
	m.physical.reverse.WithLabelValues(normalizePhysicalResult(result), normalizePhysicalReason(reason), normalizePhysicalShardID(shardID)).Inc()
}

func (m *Metrics) AddPhysicalReconciliationMismatches(reason, shardID string, count int) {
	if count > 0 {
		m.physical.reconciliation.WithLabelValues(normalizePhysicalReason(reason), normalizePhysicalShardID(shardID)).Add(float64(count))
	}
}
