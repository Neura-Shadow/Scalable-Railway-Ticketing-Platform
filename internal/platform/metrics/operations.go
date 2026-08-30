package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	allowedM7Providers      = set("sandbox", "stripe")
	allowedM7Operations     = set("create_checkout", "query_status", "authorize", "capture", "void", "refund", "import", "reconcile", "payout", "partial_refund", "acknowledge", "rotate_key", "failover", "failback", "backup", "restore", "verify")
	allowedM7Reasons        = set("none", "invalid_request", "transport", "timeout", "authentication", "provider_unavailable", "rate_limited", "unsupported", "validation", "conflict", "uncertain", "database", "invariant_mismatch", "ledger_imbalance", "missing", "unexpected", "amount", "currency", "fee", "duplicate", "age", "event_conflict", "payout_lifecycle", "manual_review", "commit", "key_rotation", "signature", "passive", "recovery", "fenced", "stale_epoch", "future_epoch", "region_mismatch", "writes_disabled", "timeline", "replication_lag", "restore", "checksum")
	allowedCapabilities     = set("hosted_checkout", "authorize", "capture", "void", "full_refund", "partial_refund", "payment_status_query", "settlement_transactions", "payout_reports", "webhook_signatures", "webhook_key_rotation")
	allowedLedgerPurposes   = set("capture", "ticket_issuance", "refund", "provider_fee", "settlement", "payout", "reversal")
	allowedCurrencies       = set("twd", "usd")
	allowedM7Results        = set("success", "failure", "conflict", "duplicate", "uncertain", "manual_review", "retry", "skipped", "accepted", "rejected")
	allowedWebhookKeyStates = set("none", "accepted", "primary", "retired")
	allowedWebhookKeyStores = set("hot", "archive")
	allowedSettlementKinds  = set(
		"balance_transaction", "settlement_batch", "settlement_line", "payout", "payout_line",
	)
	allowedSettlementReasons = set("missing", "unexpected", "amount", "currency", "fee", "duplicate", "age", "event_conflict", "ledger_imbalance", "payout_lifecycle")
	allowedRegions           = set("region-a", "region-b")
	allowedM7DatabaseRoles   = set("control", "booking_shard", "backup_metadata", "all")
	allowedM7ShardIDs        = set("none", "shard-0", "shard-1", "all")
	allowedLegacyDatabases   = set("control", "shard-0", "shard-1")
	allowedRegionalReasons   = set("none", "passive", "recovery", "fenced", "stale_epoch", "future_epoch", "region_mismatch", "writes_disabled", "database", "timeline")
	allowedRecoveryKinds     = set("failover", "failback")
	allowedRecoveryPhases    = set(
		"planned", "external_fencing_verified", "positions_recorded", "passive_readiness_removed",
		"control_promoted", "shard_0_promoted", "shard_1_promoted", "roles_and_timelines_verified",
		"epoch_allocated", "control_recovery_installed", "shard_authorities_installed", "recovery_apis_started",
		"reconciled", "payment_workers_enabled", "settlement_workers_enabled", "ingress_switched",
		"customer_writes_configured", "target_active", "rto_recorded", "rpo_recorded", "source_retained_fenced",
	)
)

type operationsMetrics struct {
	providerAdapterRequest    *prometheus.CounterVec
	providerAdapterDuration   *prometheus.HistogramVec
	providerAdapterError      *prometheus.CounterVec
	providerCapabilityFailure *prometheus.CounterVec
	ledgerTransaction         *prometheus.CounterVec
	ledgerImbalance           *prometheus.CounterVec
	ledgerReversal            *prometheus.CounterVec
	settlementImport          *prometheus.CounterVec
	settlementImportFailure   *prometheus.CounterVec
	settlementReconciliation  *prometheus.CounterVec
	settlementMismatch        *prometheus.CounterVec
	payoutMismatch            *prometheus.CounterVec
	settlementLag             *prometheus.HistogramVec
	partialRefund             *prometheus.CounterVec
	partialRefundFailure      *prometheus.CounterVec
	partialRefundDuration     *prometheus.HistogramVec
	partialRefundTicket       *prometheus.CounterVec
	webhookAck                *prometheus.CounterVec
	webhookAckFailure         *prometheus.CounterVec
	webhookCommitDuration     *prometheus.HistogramVec
	webhookKeyRotationFailure *prometheus.CounterVec
	regionalActiveEpoch       *prometheus.GaugeVec
	regionalFailover          *prometheus.CounterVec
	regionalFailoverDuration  *prometheus.HistogramVec
	regionalFailback          *prometheus.CounterVec
	regionalFailbackDuration  *prometheus.HistogramVec
	regionalWriteRejected     *prometheus.CounterVec
	regionalReplicationBytes  *prometheus.GaugeVec
	regionalReplicationLag    *prometheus.GaugeVec
	regionalLastReplay        *prometheus.GaugeVec
	regionalRPO               *prometheus.GaugeVec
	regionalRTO               *prometheus.HistogramVec
	backup                    *prometheus.CounterVec
	backupFailure             *prometheus.CounterVec
	backupAge                 *prometheus.GaugeVec
	backupRestoreTestAge      *prometheus.GaugeVec
	backupChecksumFailure     *prometheus.CounterVec
	restoreValidationDuration *prometheus.HistogramVec
}

func newOperationsMetrics() *operationsMetrics {
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	}
	histogram := func(name, help string, labels ...string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: prometheus.DefBuckets}, labels)
	}
	return &operationsMetrics{
		providerAdapterRequest:    counter("provider_adapter_request_total", "Provider-adapter requests.", "provider", "operation", "result"),
		providerAdapterDuration:   histogram("provider_adapter_request_duration_seconds", "Provider-adapter request duration.", "provider", "operation", "result"),
		providerAdapterError:      counter("provider_adapter_error_total", "Provider-adapter request failures.", "provider", "operation", "reason"),
		providerCapabilityFailure: counter("provider_capability_failure_total", "Required provider capabilities reported unavailable.", "provider", "capability", "reason"),
		ledgerTransaction:         counter("financial_ledger_transaction_total", "Operational ledger transaction outcomes.", "operation", "currency", "result"),
		ledgerImbalance:           counter("financial_ledger_imbalance_total", "Rejected or detected operational ledger imbalances.", "operation", "currency"),
		ledgerReversal:            counter("financial_ledger_reversal_total", "Operational ledger reversals.", "operation", "currency", "result"),
		settlementImport:          counter("settlement_import_total", "Normalized settlement records processed.", "provider", "operation", "result"),
		settlementImportFailure:   counter("settlement_import_failure_total", "Settlement records that failed import.", "provider", "operation", "reason"),
		settlementReconciliation:  counter("settlement_reconciliation_total", "Detect-only settlement reconciliation outcomes.", "provider", "result"),
		settlementMismatch:        counter("settlement_reconciliation_mismatch_total", "Detect-only settlement mismatches.", "provider", "operation", "reason"),
		payoutMismatch:            counter("payout_reconciliation_mismatch_total", "Detect-only payout reconciliation mismatches.", "provider", "currency", "reason"),
		settlementLag:             histogram("settlement_lag_seconds", "Provider settlement evidence lag.", "provider", "operation"),
		partialRefund:             counter("partial_refund_total", "Whole-ticket partial-refund outcomes.", "provider", "result", "currency"),
		partialRefundFailure:      counter("partial_refund_failure_total", "Whole-ticket partial-refund failures.", "provider", "reason"),
		partialRefundDuration:     histogram("partial_refund_duration_seconds", "Whole-ticket partial-refund duration.", "provider", "result"),
		partialRefundTicket:       counter("partial_refund_ticket_total", "Tickets selected by whole-ticket partial refunds.", "provider", "result", "currency"),
		webhookAck:                counter("webhook_ack_total", "Production webhook acknowledgement outcomes.", "provider", "result"),
		webhookAckFailure:         counter("webhook_ack_failure_total", "Production webhook acknowledgement failures.", "provider", "reason"),
		webhookCommitDuration:     histogram("webhook_durable_commit_duration_seconds", "Duration until production webhook evidence is durably committed.", "provider", "result"),
		webhookKeyRotationFailure: counter("webhook_key_rotation_failure_total", "Webhook verification-key rotation failures.", "provider", "reason"),
		regionalActiveEpoch:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "regional_active_epoch", Help: "Current bounded regional write-authority epoch."}, []string{"region"}),
		regionalFailover:          counter("regional_failover_total", "Regional failover outcomes.", "region", "phase", "result"),
		regionalFailoverDuration:  histogram("regional_failover_duration_seconds", "Regional failover phase duration.", "region", "phase", "result"),
		regionalFailback:          counter("regional_failback_total", "Regional failback outcomes.", "region", "phase", "result"),
		regionalFailbackDuration:  histogram("regional_failback_duration_seconds", "Regional failback phase duration.", "region", "phase", "result"),
		regionalWriteRejected:     counter("regional_write_rejected_total", "Writes rejected by regional authority.", "region", "database_role", "shard_id", "reason"),
		regionalReplicationBytes:  prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "regional_replication_lag_bytes", Help: "Current bounded PostgreSQL replication lag in bytes."}, []string{"region", "database_role", "shard_id"}),
		regionalReplicationLag:    prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "regional_replication_lag_seconds", Help: "Current bounded PostgreSQL replication lag in seconds."}, []string{"region", "database_role", "shard_id"}),
		regionalLastReplay:        prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "regional_last_replay_timestamp_seconds", Help: "Last observed standby replay timestamp."}, []string{"region", "database_role", "shard_id"}),
		regionalRPO:               prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "regional_rpo_observed_seconds", Help: "Observed regional recovery point objective window."}, []string{"region", "database_role", "shard_id"}),
		regionalRTO:               histogram("regional_rto_observed_seconds", "Observed regional recovery time.", "region", "phase", "result"),
		backup:                    counter("backup_total", "Backup operation outcomes.", "database_role", "shard_id", "result"),
		backupFailure:             counter("backup_failure_total", "Backup operation failures.", "database_role", "shard_id", "reason"),
		backupAge:                 prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "backup_age_seconds", Help: "Age of the latest observed backup."}, []string{"database_role", "shard_id"}),
		backupRestoreTestAge:      prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "backup_restore_test_age_seconds", Help: "Age of the latest isolated restore test."}, []string{"database_role", "shard_id"}),
		backupChecksumFailure:     counter("backup_checksum_failure_total", "Backup checksum failures.", "database_role", "shard_id", "reason"),
		restoreValidationDuration: histogram("backup_restore_duration_seconds", "Isolated backup restore validation duration.", "database_role", "shard_id", "result"),
	}
}

func (m *operationsMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.providerAdapterRequest, m.providerAdapterDuration, m.providerAdapterError, m.providerCapabilityFailure,
		m.ledgerTransaction, m.ledgerImbalance, m.ledgerReversal,
		m.settlementImport, m.settlementImportFailure, m.settlementReconciliation, m.settlementMismatch,
		m.payoutMismatch, m.settlementLag, m.partialRefund, m.partialRefundFailure,
		m.partialRefundDuration, m.partialRefundTicket,
		m.webhookAck, m.webhookAckFailure, m.webhookCommitDuration, m.webhookKeyRotationFailure,
		m.regionalActiveEpoch, m.regionalFailover, m.regionalFailoverDuration,
		m.regionalFailback, m.regionalFailbackDuration, m.regionalWriteRejected,
		m.regionalReplicationBytes, m.regionalReplicationLag, m.regionalLastReplay,
		m.regionalRPO, m.regionalRTO, m.backup, m.backupFailure, m.backupAge,
		m.backupRestoreTestAge, m.backupChecksumFailure, m.restoreValidationDuration,
	}
}

func (m *operationsMetrics) eventCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.providerAdapterRequest, m.providerAdapterDuration, m.providerAdapterError,
		m.partialRefund, m.partialRefundFailure, m.partialRefundDuration, m.partialRefundTicket,
		m.webhookAck, m.webhookAckFailure, m.webhookCommitDuration,
		m.regionalWriteRejected,
	}
}

func (m *operationsMetrics) settlementEventCollectors() []prometheus.Collector {
	collectors := m.eventCollectors()
	return append(collectors, m.settlementImport, m.settlementImportFailure)
}

func (m *Metrics) RecordProviderCapability(provider, capability, result string) {
	if normalize(result, allowedM7Results, "unknown") != "success" {
		m.RecordProviderCapabilityFailure(provider, capability, result)
	}
}

func (m *Metrics) RecordProviderAdapter(provider, operation, result, reason string, duration time.Duration) {
	provider = normalize(provider, allowedM7Providers, "unknown")
	operation = normalize(operation, allowedM7Operations, "unknown")
	result = normalize(result, allowedM7Results, "unknown")
	reason = normalize(reason, allowedM7Reasons, "unknown")
	m.operations.providerAdapterRequest.WithLabelValues(provider, operation, result).Inc()
	m.operations.providerAdapterDuration.WithLabelValues(provider, operation, result).Observe(boundedPaymentSeconds(duration))
	errorMetric := m.operations.providerAdapterError.WithLabelValues(provider, operation, reason)
	errorMetric.Add(0)
	if result == "failure" || result == "uncertain" || result == "rejected" {
		errorMetric.Inc()
	}
}

func (m *Metrics) RecordProviderCapabilityFailure(provider, capability, reason string) {
	m.operations.providerCapabilityFailure.WithLabelValues(
		normalize(provider, allowedM7Providers, "unknown"),
		normalize(capability, allowedCapabilities, "unknown"),
		normalize(reason, allowedM7Reasons, "unknown"),
	).Inc()
}

func (m *Metrics) RecordFinancialLedger(purpose, currency, result string, imbalance bool) {
	m.RecordFinancialLedgerTransaction(purpose, currency, result, imbalance, false)
}

func (m *Metrics) RecordFinancialLedgerTransaction(operation, currency, result string, imbalance, reversal bool) {
	operation = normalize(operation, allowedLedgerPurposes, "unknown")
	currency = normalize(currency, allowedCurrencies, "unknown")
	result = normalize(result, allowedM7Results, "unknown")
	m.operations.ledgerTransaction.WithLabelValues(operation, currency, result).Inc()
	metric := m.operations.ledgerImbalance.WithLabelValues(operation, currency)
	metric.Add(0)
	if imbalance {
		metric.Inc()
	}
	reversalMetric := m.operations.ledgerReversal.WithLabelValues(operation, currency, result)
	reversalMetric.Add(0)
	if reversal {
		reversalMetric.Inc()
	}
}

func (m *Metrics) RecordSettlementImport(provider, kind, result string, records int) {
	reason := "none"
	if result != "success" {
		reason = result
	}
	m.RecordSettlementImportResult(provider, kind, result, reason, records)
}

func (m *Metrics) RecordSettlementImportResult(provider, kind, result, reason string, records int) {
	if records < 0 {
		records = 0
	} else if records > 1_000_000 {
		records = 1_000_000
	}
	provider = normalize(provider, allowedM7Providers, "unknown")
	operation := normalize(kind, allowedSettlementKinds, "unknown")
	result = normalize(result, allowedM7Results, "unknown")
	reason = normalize(reason, allowedM7Reasons, "unknown")
	m.operations.settlementImport.WithLabelValues(provider, operation, result).Add(float64(records))
	failure := m.operations.settlementImportFailure.WithLabelValues(provider, operation, reason)
	failure.Add(0)
	if result == "failure" || result == "rejected" {
		failure.Inc()
	}
}

func (m *Metrics) RecordSettlementMismatch(kind, reason string) {
	m.operations.settlementMismatch.WithLabelValues("unknown", normalize(kind, allowedSettlementKinds, "unknown"), normalize(reason, allowedSettlementReasons, "unknown")).Inc()
}

func (m *Metrics) RecordSettlementReconciliation(provider, operation, currency, result, reason string, mismatch, payoutMismatch bool, lag time.Duration) {
	provider = normalize(provider, allowedM7Providers, "unknown")
	operation = normalize(operation, allowedSettlementKinds, "unknown")
	currency = normalize(currency, allowedCurrencies, "unknown")
	result = normalize(result, allowedM7Results, "unknown")
	reason = normalize(reason, allowedSettlementReasons, "unknown")
	m.operations.settlementReconciliation.WithLabelValues(provider, result).Inc()
	mismatchMetric := m.operations.settlementMismatch.WithLabelValues(provider, operation, reason)
	mismatchMetric.Add(0)
	if mismatch {
		mismatchMetric.Inc()
	}
	payoutMetric := m.operations.payoutMismatch.WithLabelValues(provider, currency, reason)
	payoutMetric.Add(0)
	if payoutMismatch {
		payoutMetric.Inc()
	}
	m.operations.settlementLag.WithLabelValues(provider, operation).Observe(boundedPaymentSeconds(lag))
}

func (m *Metrics) RecordTicketPartialRefund(result string, duration time.Duration, manualReview bool) {
	reason := "none"
	if manualReview {
		reason = "manual_review"
	}
	m.RecordPartialRefund("unknown", result, reason, "unknown", 0, duration)
}

func (m *Metrics) RecordPartialRefund(provider, result, reason, currency string, tickets int, duration time.Duration) {
	provider = normalize(provider, allowedM7Providers, "unknown")
	result = normalize(result, allowedM7Results, "unknown")
	reason = normalize(reason, allowedM7Reasons, "unknown")
	currency = normalize(currency, allowedCurrencies, "unknown")
	if tickets < 0 {
		tickets = 0
	} else if tickets > 1_000_000 {
		tickets = 1_000_000
	}
	m.operations.partialRefund.WithLabelValues(provider, result, currency).Inc()
	failure := m.operations.partialRefundFailure.WithLabelValues(provider, reason)
	failure.Add(0)
	if result == "failure" || result == "rejected" || result == "manual_review" {
		failure.Inc()
	}
	m.operations.partialRefundDuration.WithLabelValues(provider, result).Observe(boundedPaymentSeconds(duration))
	if tickets > 0 {
		m.operations.partialRefundTicket.WithLabelValues(provider, result, currency).Add(float64(tickets))
	}
}

func (m *Metrics) RecordWebhookKeyRotation(provider, result string) {
	provider = normalize(provider, allowedM7Providers, "unknown")
	result = normalize(result, allowedM7Results, "unknown")
	metric := m.operations.webhookKeyRotationFailure.WithLabelValues(provider, normalize(result, allowedM7Reasons, "unknown"))
	metric.Add(0)
	if result != "success" {
		metric.Inc()
	}
}

func (m *Metrics) RecordWebhookAck(provider, result, reason string, commitDuration time.Duration) {
	provider = normalize(provider, allowedM7Providers, "unknown")
	result = normalize(result, allowedM7Results, "unknown")
	reason = normalize(reason, allowedM7Reasons, "unknown")
	m.operations.webhookAck.WithLabelValues(provider, result).Inc()
	failure := m.operations.webhookAckFailure.WithLabelValues(provider, reason)
	failure.Add(0)
	if result == "failure" || result == "rejected" {
		failure.Inc()
	}
	m.operations.webhookCommitDuration.WithLabelValues(provider, result).Observe(boundedPaymentSeconds(commitDuration))
}

func (m *Metrics) RecordWebhookKeyRotationFailure(provider, reason string) {
	m.operations.webhookKeyRotationFailure.WithLabelValues(
		normalize(provider, allowedM7Providers, "unknown"),
		normalize(reason, allowedM7Reasons, "unknown"),
	).Inc()
}

func (m *Metrics) RecordRegionalWrite(region, result, reason string) {
	region = normalize(region, allowedRegions, "unknown")
	result = normalize(result, allowedM7Results, "unknown")
	reason = normalize(reason, allowedRegionalReasons, "unknown")
	metric := m.operations.regionalWriteRejected.WithLabelValues(region, "unknown", "unknown", reason)
	metric.Add(0)
	if result == "failure" || result == "rejected" {
		metric.Inc()
	}
}

func (m *Metrics) RecordReplicationLag(database, region string, lag time.Duration) {
	databaseRole, shardID := legacyDatabaseLabels(database)
	m.RecordRegionalReplication(region, databaseRole, shardID, 0, lag, time.Time{})
}

func (m *Metrics) RecordRegionalRecovery(operation, phase, result string, duration time.Duration) {
	switch normalize(operation, allowedRecoveryKinds, "unknown") {
	case "failover":
		m.RecordRegionalFailover("unknown", phase, result, duration)
	case "failback":
		m.RecordRegionalFailback("unknown", phase, result, duration)
	}
}

func (m *Metrics) RecordObservedRecoveryPoint(database, region string, _ int64, window time.Duration) {
	databaseRole, shardID := legacyDatabaseLabels(database)
	m.RecordRegionalRPO(region, databaseRole, shardID, window)
}

func (m *Metrics) RecordRegionalEpoch(region string, epoch uint64) {
	const largestExactFloatInteger = uint64(1 << 53)
	if epoch > largestExactFloatInteger {
		epoch = largestExactFloatInteger
	}
	m.operations.regionalActiveEpoch.WithLabelValues(normalize(region, allowedRegions, "unknown")).Set(float64(epoch))
}

func (m *Metrics) RecordRegionalFailover(region, phase, result string, duration time.Duration) {
	region, phase, result = normalizeRecoveryLabels(region, phase, result)
	m.operations.regionalFailover.WithLabelValues(region, phase, result).Inc()
	m.operations.regionalFailoverDuration.WithLabelValues(region, phase, result).Observe(boundedPaymentSeconds(duration))
}

func (m *Metrics) RecordRegionalFailback(region, phase, result string, duration time.Duration) {
	region, phase, result = normalizeRecoveryLabels(region, phase, result)
	m.operations.regionalFailback.WithLabelValues(region, phase, result).Inc()
	m.operations.regionalFailbackDuration.WithLabelValues(region, phase, result).Observe(boundedPaymentSeconds(duration))
}

func (m *Metrics) RecordRegionalWriteRejected(region, databaseRole, shardID, reason string) {
	m.operations.regionalWriteRejected.WithLabelValues(
		normalize(region, allowedRegions, "unknown"),
		normalize(databaseRole, allowedM7DatabaseRoles, "unknown"),
		normalize(shardID, allowedM7ShardIDs, "unknown"),
		normalize(reason, allowedRegionalReasons, "unknown"),
	).Inc()
}

func (m *Metrics) RecordRegionalReplication(region, databaseRole, shardID string, lagBytes int64, lag time.Duration, replayAt time.Time) {
	if lagBytes < 0 {
		lagBytes = 0
	} else if lagBytes > 1_000_000_000_000_000_000 {
		lagBytes = 1_000_000_000_000_000_000
	}
	region = normalize(region, allowedRegions, "unknown")
	databaseRole = normalize(databaseRole, allowedM7DatabaseRoles, "unknown")
	shardID = normalize(shardID, allowedM7ShardIDs, "unknown")
	m.operations.regionalReplicationBytes.WithLabelValues(region, databaseRole, shardID).Set(float64(lagBytes))
	m.operations.regionalReplicationLag.WithLabelValues(region, databaseRole, shardID).Set(boundedPaymentSeconds(lag))
	replaySeconds := float64(0)
	if !replayAt.IsZero() && replayAt.Unix() > 0 {
		replaySeconds = float64(replayAt.Unix())
	}
	m.operations.regionalLastReplay.WithLabelValues(region, databaseRole, shardID).Set(replaySeconds)
}

func (m *Metrics) RecordRegionalRPO(region, databaseRole, shardID string, observed time.Duration) {
	m.operations.regionalRPO.WithLabelValues(
		normalize(region, allowedRegions, "unknown"),
		normalize(databaseRole, allowedM7DatabaseRoles, "unknown"),
		normalize(shardID, allowedM7ShardIDs, "unknown"),
	).Set(boundedPaymentSeconds(observed))
}

func (m *Metrics) RecordRegionalRTO(region, phase, result string, observed time.Duration) {
	region, phase, result = normalizeRecoveryLabels(region, phase, result)
	m.operations.regionalRTO.WithLabelValues(region, phase, result).Observe(boundedPaymentSeconds(observed))
}

func normalizeRecoveryLabels(region, phase, result string) (string, string, string) {
	return normalize(region, allowedRegions, "unknown"),
		normalize(phase, allowedRecoveryPhases, "unknown"),
		normalize(result, allowedM7Results, "unknown")
}

func legacyDatabaseLabels(database string) (string, string) {
	switch normalize(database, allowedLegacyDatabases, "unknown") {
	case "control":
		return "control", "none"
	case "shard-0":
		return "booking_shard", "shard-0"
	case "shard-1":
		return "booking_shard", "shard-1"
	default:
		return "unknown", "unknown"
	}
}

func (m *Metrics) RecordBackupVerification(database, result string, checksumFailure bool) {
	databaseRole, shardID := legacyDatabaseLabels(database)
	reason := "none"
	if checksumFailure {
		reason = "checksum"
	}
	m.RecordBackupOperation(databaseRole, shardID, result, reason, 0, checksumFailure)
}

func (m *Metrics) RecordRestoreValidation(database, result string, duration time.Duration) {
	databaseRole, shardID := legacyDatabaseLabels(database)
	m.RecordBackupRestoreTest(databaseRole, shardID, result, "none", 0, duration)
}

func (m *Metrics) RecordBackupOperation(databaseRole, shardID, result, reason string, age time.Duration, checksumFailure bool) {
	databaseRole = normalize(databaseRole, allowedM7DatabaseRoles, "unknown")
	shardID = normalize(shardID, allowedM7ShardIDs, "unknown")
	result = normalize(result, allowedM7Results, "unknown")
	reason = normalize(reason, allowedM7Reasons, "unknown")
	m.operations.backup.WithLabelValues(databaseRole, shardID, result).Inc()
	failure := m.operations.backupFailure.WithLabelValues(databaseRole, shardID, reason)
	failure.Add(0)
	if result == "failure" || result == "rejected" {
		failure.Inc()
	}
	m.operations.backupAge.WithLabelValues(databaseRole, shardID).Set(boundedPaymentSeconds(age))
	checksum := m.operations.backupChecksumFailure.WithLabelValues(databaseRole, shardID, reason)
	checksum.Add(0)
	if checksumFailure {
		checksum.Inc()
	}
}

func (m *Metrics) RecordBackupRestoreTest(databaseRole, shardID, result, reason string, testAge, duration time.Duration) {
	databaseRole = normalize(databaseRole, allowedM7DatabaseRoles, "unknown")
	shardID = normalize(shardID, allowedM7ShardIDs, "unknown")
	result = normalize(result, allowedM7Results, "unknown")
	reason = normalize(reason, allowedM7Reasons, "unknown")
	m.operations.backupRestoreTestAge.WithLabelValues(databaseRole, shardID).Set(boundedPaymentSeconds(testAge))
	m.operations.restoreValidationDuration.WithLabelValues(databaseRole, shardID, result).Observe(boundedPaymentSeconds(duration))
	failure := m.operations.backupFailure.WithLabelValues(databaseRole, shardID, reason)
	failure.Add(0)
	if result == "failure" || result == "rejected" {
		failure.Inc()
	}
}
