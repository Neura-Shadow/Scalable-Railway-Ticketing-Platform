package metrics

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

// DurableOperationsCollector reconstructs cross-process operational metrics
// from immutable or append-only M7 evidence. It never mutates the database and
// therefore remains safe in passive and recovery deployments.
type DurableOperationsCollector struct {
	source  durableOperationsSource
	timeout time.Duration
	descs   map[string]*prometheus.Desc
}

type durableOperationsSource interface {
	Snapshot(context.Context) (durableOperationsSnapshot, error)
}

type durableCounter struct {
	name   string
	labels []string
	value  float64
}

type durableGauge = durableCounter

type durableHistogram struct {
	name   string
	labels []string
	count  uint64
	sum    float64
}

type durableOperationsSnapshot struct {
	counters   []durableCounter
	gauges     []durableGauge
	histograms []durableHistogram
}

type durableOperationsBeginner interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type postgresDurableOperationsSource struct {
	db                 durableOperationsBeginner
	region             string
	replicas           []durableReplicationSource
	capabilityFailures []durableCounter
}

type durableReplicationSource struct {
	db           durableOperationsBeginner
	databaseRole string
	shardID      string
}

type DurableOperationsOption func(*postgresDurableOperationsSource) error

func WithProviderCapabilityProfile(provider string, capabilities map[string]bool) DurableOperationsOption {
	return func(source *postgresDurableOperationsSource) error {
		provider = normalize(provider, allowedM7Providers, "unknown")
		if source == nil || provider == "unknown" || len(capabilities) == 0 || len(capabilities) > len(allowedCapabilities) {
			return errors.New("metrics: invalid provider capability profile")
		}
		for capability, supported := range capabilities {
			capability = normalize(capability, allowedCapabilities, "unknown")
			if capability == "unknown" {
				return errors.New("metrics: invalid provider capability")
			}
			if !supported {
				source.capabilityFailures = append(source.capabilityFailures, durableCounter{
					"provider_capability_failure_total", []string{provider, capability, "unsupported"}, 1,
				})
			}
		}
		return nil
	}
}

func WithDurableReplicationSource(databaseRole, shardID string, db durableOperationsBeginner) DurableOperationsOption {
	return func(source *postgresDurableOperationsSource) error {
		if source == nil || db == nil {
			return errors.New("metrics: invalid replication source")
		}
		databaseRole = normalize(databaseRole, allowedM7DatabaseRoles, "unknown")
		shardID = normalize(shardID, allowedM7ShardIDs, "unknown")
		if databaseRole == "unknown" || shardID == "unknown" {
			return errors.New("metrics: invalid replication labels")
		}
		source.replicas = append(source.replicas, durableReplicationSource{db: db, databaseRole: databaseRole, shardID: shardID})
		return nil
	}
}

func NewDurableOperationsCollector(db durableOperationsBeginner, region string, timeout time.Duration, options ...DurableOperationsOption) (*DurableOperationsCollector, error) {
	if db == nil || timeout <= 0 || timeout > 30*time.Second {
		return nil, errors.New("metrics: invalid durable operations collector")
	}
	region = normalize(region, allowedRegions, "unknown")
	source := &postgresDurableOperationsSource{db: db, region: region, replicas: []durableReplicationSource{{db: db, databaseRole: "control", shardID: "none"}}}
	for _, apply := range options {
		if apply == nil || apply(source) != nil {
			return nil, errors.New("metrics: invalid durable operations option")
		}
	}
	return newDurableOperationsCollector(source, timeout)
}

func newDurableOperationsCollector(source durableOperationsSource, timeout time.Duration) (*DurableOperationsCollector, error) {
	if source == nil || timeout <= 0 || timeout > 30*time.Second {
		return nil, errors.New("metrics: invalid durable operations collector")
	}
	descs := map[string]*prometheus.Desc{}
	add := func(name, help string, labels ...string) { descs[name] = prometheus.NewDesc(name, help, labels, nil) }
	add("financial_ledger_transaction_total", "Operational ledger transaction outcomes.", "operation", "currency", "result")
	add("provider_capability_failure_total", "Required provider capabilities reported unavailable.", "provider", "capability", "reason")
	add("financial_ledger_imbalance_total", "Rejected or detected operational ledger imbalances.", "operation", "currency")
	add("financial_ledger_reversal_total", "Operational ledger reversals.", "operation", "currency", "result")
	add("settlement_import_total", "Normalized settlement records processed.", "provider", "operation", "result")
	add("settlement_import_failure_total", "Settlement records that failed import.", "provider", "operation", "reason")
	add("settlement_reconciliation_total", "Detect-only settlement reconciliation outcomes.", "provider", "result")
	add("settlement_reconciliation_mismatch_total", "Detect-only settlement mismatches.", "provider", "operation", "reason")
	add("payout_reconciliation_mismatch_total", "Detect-only payout reconciliation mismatches.", "provider", "currency", "reason")
	add("settlement_lag_seconds", "Provider settlement evidence lag.", "provider", "operation")
	add("webhook_key_rotation_failure_total", "Webhook verification-key rotation failures.", "provider", "reason")
	add("webhook_key_version_count", "Current durable webhook-key versions by bounded lifecycle state and store.", "provider", "state", "store")
	add("webhook_key_rotation_total", "Immutable webhook-key lifecycle transitions.", "provider", "from_state", "to_state", "result")
	add("regional_active_epoch", "Current bounded regional write-authority epoch.", "region")
	add("regional_failover_total", "Regional failover outcomes.", "region", "phase", "result")
	add("regional_failover_duration_seconds", "Regional failover phase duration.", "region", "phase", "result")
	add("regional_failback_total", "Regional failback outcomes.", "region", "phase", "result")
	add("regional_failback_duration_seconds", "Regional failback phase duration.", "region", "phase", "result")
	add("regional_replication_lag_bytes", "Current bounded PostgreSQL replication lag in bytes.", "region", "database_role", "shard_id")
	add("regional_replication_lag_seconds", "Current bounded PostgreSQL replication lag in seconds.", "region", "database_role", "shard_id")
	add("regional_last_replay_timestamp_seconds", "Last observed standby replay timestamp.", "region", "database_role", "shard_id")
	add("regional_rpo_observed_seconds", "Observed regional recovery point objective window.", "region", "database_role", "shard_id")
	add("regional_rto_observed_seconds", "Observed regional recovery time.", "region", "phase", "result")
	add("backup_total", "Backup operation outcomes.", "database_role", "shard_id", "result")
	add("backup_failure_total", "Backup operation failures.", "database_role", "shard_id", "reason")
	add("backup_age_seconds", "Age of the latest observed backup.", "database_role", "shard_id")
	add("backup_restore_test_age_seconds", "Age of the latest isolated restore test.", "database_role", "shard_id")
	add("backup_checksum_failure_total", "Backup checksum failures.", "database_role", "shard_id", "reason")
	add("backup_restore_duration_seconds", "Isolated backup restore validation duration.", "database_role", "shard_id", "result")
	return &DurableOperationsCollector{source: source, timeout: timeout, descs: descs}, nil
}

func (collector *DurableOperationsCollector) Describe(output chan<- *prometheus.Desc) {
	for _, description := range collector.descs {
		output <- description
	}
}

func (collector *DurableOperationsCollector) Collect(output chan<- prometheus.Metric) {
	if collector == nil || collector.source == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), collector.timeout)
	defer cancel()
	snapshot, err := collector.source.Snapshot(ctx)
	if err != nil {
		return
	}
	for _, sample := range snapshot.counters {
		if description := collector.descs[sample.name]; description != nil && validMetricValue(sample.value) {
			output <- prometheus.MustNewConstMetric(description, prometheus.CounterValue, sample.value, sample.labels...)
		}
	}
	for _, sample := range snapshot.gauges {
		if description := collector.descs[sample.name]; description != nil && validMetricValue(sample.value) {
			output <- prometheus.MustNewConstMetric(description, prometheus.GaugeValue, sample.value, sample.labels...)
		}
	}
	for _, sample := range snapshot.histograms {
		if description := collector.descs[sample.name]; description != nil && validMetricValue(sample.sum) {
			output <- prometheus.MustNewConstHistogram(description, sample.count, sample.sum, nil, sample.labels...)
		}
	}
}

func validMetricValue(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func (source *postgresDurableOperationsSource) Snapshot(ctx context.Context) (durableOperationsSnapshot, error) {
	tx, err := source.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return durableOperationsSnapshot{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var snapshot durableOperationsSnapshot
	for _, load := range []func(context.Context, pgx.Tx, *durableOperationsSnapshot) error{
		loadLedgerMetrics, loadSettlementMetrics, loadRegionalMetrics, loadBackupMetrics,
	} {
		if err := load(ctx, tx, &snapshot); err != nil {
			return durableOperationsSnapshot{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return durableOperationsSnapshot{}, err
	}
	for _, replica := range source.replicas {
		replicaTx, beginErr := replica.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if beginErr != nil {
			continue
		}
		loadErr := loadReplicationMetrics(ctx, replicaTx, source.region, replica.databaseRole, replica.shardID, &snapshot)
		if loadErr == nil {
			loadErr = replicaTx.Commit(ctx)
		}
		if loadErr != nil {
			_ = replicaTx.Rollback(context.WithoutCancel(ctx))
		}
	}
	snapshot.counters = append(snapshot.counters, source.capabilityFailures...)
	return snapshot, nil
}

func loadLedgerMetrics(ctx context.Context, tx pgx.Tx, snapshot *durableOperationsSnapshot) error {
	rows, err := tx.Query(ctx, `SELECT purpose,lower(currency),count(*)::float8
FROM public.financial_ledger_transactions GROUP BY purpose,currency ORDER BY purpose,currency`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var operation, currency string
		var count float64
		if err := rows.Scan(&operation, &currency, &count); err != nil {
			rows.Close()
			return err
		}
		snapshot.counters = append(snapshot.counters, durableCounter{"financial_ledger_transaction_total", []string{normalize(operation, allowedLedgerPurposes, "unknown"), normalize(currency, allowedCurrencies, "unknown"), "success"}, count})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `SELECT lower(original.currency),count(*)::float8
FROM public.financial_ledger_reversals AS reversal
JOIN public.financial_ledger_transactions AS original ON original.transaction_id=reversal.original_transaction_id
GROUP BY original.currency ORDER BY original.currency`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var currency string
		var count float64
		if err := rows.Scan(&currency, &count); err != nil {
			rows.Close()
			return err
		}
		snapshot.counters = append(snapshot.counters, durableCounter{"financial_ledger_reversal_total", []string{"reversal", normalize(currency, allowedCurrencies, "unknown"), "success"}, count})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `SELECT count(*)::float8 FROM public.settlement_reconciliation_mismatches WHERE reason='ledger_imbalance'`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var count float64
		if err := rows.Scan(&count); err != nil {
			rows.Close()
			return err
		}
		if count > 0 {
			snapshot.counters = append(snapshot.counters, durableCounter{"financial_ledger_imbalance_total", []string{"settlement", "unknown"}, count})
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	return nil
}

func loadSettlementMetrics(ctx context.Context, tx pgx.Tx, snapshot *durableOperationsSnapshot) error {
	rows, err := tx.Query(ctx, `WITH evidence(provider,kind,created_at,currency,correlation) AS (
 SELECT provider,'balance_transaction',provider_created_at,currency,COALESCE(payment_correlation,provider_record_id) FROM public.provider_balance_transactions UNION ALL
 SELECT provider,'settlement_batch',provider_created_at,currency,COALESCE(payment_correlation,provider_record_id) FROM public.provider_settlement_batches UNION ALL
 SELECT provider,'settlement_line',provider_created_at,currency,COALESCE(payment_correlation,provider_record_id) FROM public.provider_settlement_lines UNION ALL
 SELECT provider,'payout',provider_created_at,currency,COALESCE(provider_payout_id,provider_record_id) FROM public.provider_payouts UNION ALL
 SELECT provider,'payout_line',provider_created_at,currency,COALESCE(provider_payout_id,provider_record_id) FROM public.provider_payout_lines
) SELECT provider,kind,count(*)::float8,max(EXTRACT(EPOCH FROM clock_timestamp()-created_at))::float8
FROM evidence GROUP BY provider,kind ORDER BY provider,kind`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var provider, operation string
		var count, lag float64
		if err := rows.Scan(&provider, &operation, &count, &lag); err != nil {
			rows.Close()
			return err
		}
		provider, operation = normalize(provider, allowedM7Providers, "unknown"), normalize(operation, allowedSettlementKinds, "unknown")
		snapshot.counters = append(snapshot.counters, durableCounter{"settlement_import_total", []string{provider, operation, "success"}, count})
		snapshot.histograms = append(snapshot.histograms, durableHistogram{"settlement_lag_seconds", []string{provider, operation}, 1, maxZero(lag)})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `SELECT provider,record_kind,count(*)::float8 FROM public.provider_settlement_import_conflicts GROUP BY provider,record_kind ORDER BY provider,record_kind`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var provider, operation string
		var count float64
		if err := rows.Scan(&provider, &operation, &count); err != nil {
			rows.Close()
			return err
		}
		snapshot.counters = append(snapshot.counters, durableCounter{"settlement_import_failure_total", []string{normalize(provider, allowedM7Providers, "unknown"), normalize(operation, allowedSettlementKinds, "unknown"), "conflict"}, count})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `WITH evidence AS (
 SELECT provider,currency,'balance_transaction'::text record_kind,provider_record_id,
        COALESCE(payment_correlation,provider_record_id) correlation,provider_settlement_id,provider_payout_id,provider_created_at
 FROM public.provider_balance_transactions UNION ALL
 SELECT provider,currency,'settlement_batch',provider_record_id,COALESCE(payment_correlation,provider_record_id),provider_settlement_id,provider_payout_id,provider_created_at FROM public.provider_settlement_batches UNION ALL
 SELECT provider,currency,'settlement_line',provider_record_id,COALESCE(payment_correlation,provider_record_id),provider_settlement_id,provider_payout_id,provider_created_at FROM public.provider_settlement_lines UNION ALL
 SELECT provider,currency,'payout',provider_record_id,COALESCE(provider_payout_id,provider_record_id),provider_settlement_id,provider_payout_id,provider_created_at FROM public.provider_payouts UNION ALL
 SELECT provider,currency,'payout_line',provider_record_id,COALESCE(provider_payout_id,provider_record_id),provider_settlement_id,provider_payout_id,provider_created_at FROM public.provider_payout_lines
), observed AS (
 SELECT run.run_id,run.finding_count,COALESCE(CASE WHEN count(DISTINCT evidence.provider)=1 THEN min(evidence.provider) END,'unknown') provider
 FROM public.settlement_reconciliation_runs run
 LEFT JOIN evidence ON
      (run.scope_type='payment' AND evidence.correlation=run.scope_value)
   OR (run.scope_type='settlement' AND (evidence.provider_settlement_id=run.scope_value OR (evidence.record_kind='settlement_batch' AND evidence.provider_record_id=run.scope_value)))
   OR (run.scope_type='payout' AND (evidence.provider_payout_id=run.scope_value OR (evidence.record_kind='payout' AND evidence.provider_record_id=run.scope_value)))
	OR CASE WHEN run.scope_type='period' THEN
	     evidence.provider_created_at >= split_part(run.scope_value,'/',1)::date
	 AND evidence.provider_created_at < split_part(run.scope_value,'/',2)::date
	   ELSE false END
 GROUP BY run.run_id,run.finding_count
) SELECT provider,CASE WHEN finding_count=0 THEN 'success' ELSE 'manual_review' END,count(*)::float8 FROM observed GROUP BY provider,2 ORDER BY provider,2`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var provider, result string
		var count float64
		if err := rows.Scan(&provider, &result, &count); err != nil {
			rows.Close()
			return err
		}
		snapshot.counters = append(snapshot.counters, durableCounter{"settlement_reconciliation_total", []string{normalize(provider, allowedM7Providers, "unknown"), normalize(result, allowedM7Results, "unknown")}, count})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `WITH evidence AS (
 SELECT provider,currency,'balance_transaction'::text record_kind,provider_record_id,
        COALESCE(payment_correlation,provider_record_id) correlation,provider_settlement_id,provider_payout_id,provider_created_at
 FROM public.provider_balance_transactions UNION ALL
 SELECT provider,currency,'settlement_batch',provider_record_id,COALESCE(payment_correlation,provider_record_id),provider_settlement_id,provider_payout_id,provider_created_at FROM public.provider_settlement_batches UNION ALL
 SELECT provider,currency,'settlement_line',provider_record_id,COALESCE(payment_correlation,provider_record_id),provider_settlement_id,provider_payout_id,provider_created_at FROM public.provider_settlement_lines UNION ALL
 SELECT provider,currency,'payout',provider_record_id,COALESCE(provider_payout_id,provider_record_id),provider_settlement_id,provider_payout_id,provider_created_at FROM public.provider_payouts UNION ALL
 SELECT provider,currency,'payout_line',provider_record_id,COALESCE(provider_payout_id,provider_record_id),provider_settlement_id,provider_payout_id,provider_created_at FROM public.provider_payout_lines
), run_provider AS (
 SELECT run.run_id,CASE WHEN count(DISTINCT evidence.provider)=1 THEN min(evidence.provider) END provider
 FROM public.settlement_reconciliation_runs run
 LEFT JOIN evidence ON
      (run.scope_type='payment' AND evidence.correlation=run.scope_value)
   OR (run.scope_type='settlement' AND (evidence.provider_settlement_id=run.scope_value OR (evidence.record_kind='settlement_batch' AND evidence.provider_record_id=run.scope_value)))
   OR (run.scope_type='payout' AND (evidence.provider_payout_id=run.scope_value OR (evidence.record_kind='payout' AND evidence.provider_record_id=run.scope_value)))
	OR CASE WHEN run.scope_type='period' THEN
	     evidence.provider_created_at >= split_part(run.scope_value,'/',1)::date
	 AND evidence.provider_created_at < split_part(run.scope_value,'/',2)::date
	   ELSE false END
 GROUP BY run.run_id
), attributed AS (
	SELECT mismatch.run_id,mismatch.finding_index,mismatch.evidence_kind,mismatch.reason,
	       COALESCE(run_provider.provider,CASE WHEN count(DISTINCT evidence.provider)=1 THEN min(evidence.provider) END,'unknown') provider,
	       COALESCE(CASE
	         WHEN mismatch.evidence_kind='payout' AND count(DISTINCT evidence.currency) FILTER(WHERE evidence.record_kind='payout')=1
	           THEN lower(min(evidence.currency) FILTER(WHERE evidence.record_kind='payout'))
	         WHEN mismatch.evidence_kind<>'payout' AND count(DISTINCT evidence.currency)=1 THEN lower(min(evidence.currency))
	       END,'unknown') currency
 FROM public.settlement_reconciliation_mismatches mismatch
 JOIN run_provider USING(run_id)
 LEFT JOIN evidence ON evidence.correlation=mismatch.correlation
                    OR evidence.provider_record_id=mismatch.correlation
                    OR 'payout:'||COALESCE(evidence.provider_payout_id,evidence.provider_record_id)=mismatch.correlation
 GROUP BY mismatch.run_id,mismatch.finding_index,mismatch.evidence_kind,mismatch.reason,run_provider.provider
) SELECT provider,evidence_kind,reason,currency,count(*)::float8
FROM attributed GROUP BY provider,evidence_kind,reason,currency ORDER BY provider,evidence_kind,reason,currency`)
	if err != nil {
		return err
	}
	type mismatchMetricKey struct {
		provider  string
		dimension string
		reason    string
	}
	settlementMismatchCounts := make(map[mismatchMetricKey]float64)
	payoutMismatchCounts := make(map[mismatchMetricKey]float64)
	for rows.Next() {
		var provider, kind, reason, currency string
		var count float64
		if err := rows.Scan(&provider, &kind, &reason, &currency, &count); err != nil {
			rows.Close()
			return err
		}
		provider, reason = normalize(provider, allowedM7Providers, "unknown"), normalize(reason, allowedSettlementReasons, "unknown")
		operation := "balance_transaction"
		if kind == "settlement" {
			operation = "settlement_line"
		}
		if kind == "payout" {
			operation = "payout"
		}
		settlementMismatchCounts[mismatchMetricKey{provider: provider, dimension: operation, reason: reason}] += count
		if kind == "payout" {
			payoutMismatchCounts[mismatchMetricKey{provider: provider, dimension: normalize(currency, allowedCurrencies, "unknown"), reason: reason}] += count
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	appendMismatchCounters := func(name string, counts map[mismatchMetricKey]float64) {
		keys := make([]mismatchMetricKey, 0, len(counts))
		for key := range counts {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].provider != keys[j].provider {
				return keys[i].provider < keys[j].provider
			}
			if keys[i].dimension != keys[j].dimension {
				return keys[i].dimension < keys[j].dimension
			}
			return keys[i].reason < keys[j].reason
		})
		for _, key := range keys {
			snapshot.counters = append(snapshot.counters, durableCounter{name, []string{key.provider, key.dimension, key.reason}, counts[key]})
		}
	}
	appendMismatchCounters("settlement_reconciliation_mismatch_total", settlementMismatchCounts)
	appendMismatchCounters("payout_reconciliation_mismatch_total", payoutMismatchCounts)
	rows, err = tx.Query(ctx, `WITH status AS (
 SELECT capability.provider,capability.provider_account_id,
        count(*) FILTER(WHERE key.state='primary') primary_count,
        count(*) FILTER(WHERE key.state<>'retired') active_count,
        count(*) FILTER(WHERE key.state='accepted' AND key.retirement_not_before<=clock_timestamp()) overdue_count
 FROM public.payment_provider_capabilities capability
 LEFT JOIN public.payment_webhook_key_versions key
   ON key.provider=capability.provider AND key.provider_account_id=capability.provider_account_id
 WHERE capability.webhook_key_rotation
 GROUP BY capability.provider,capability.provider_account_id
), anomalies AS (
 SELECT provider,'missing'::text reason FROM status WHERE primary_count=0
 UNION ALL SELECT provider,'duplicate' FROM status WHERE primary_count>1
 UNION ALL SELECT provider,'key_rotation' FROM status WHERE active_count NOT BETWEEN 1 AND 2
 UNION ALL SELECT provider,'age' FROM status WHERE overdue_count>0
) SELECT provider,reason,count(*)::float8 FROM anomalies GROUP BY provider,reason ORDER BY provider,reason`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var provider, reason string
		var failures float64
		if err := rows.Scan(&provider, &reason, &failures); err != nil {
			rows.Close()
			return err
		}
		snapshot.counters = append(snapshot.counters, durableCounter{"webhook_key_rotation_failure_total", []string{normalize(provider, allowedM7Providers, "unknown"), normalize(reason, allowedM7Reasons, "unknown")}, failures})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `SELECT provider,state,store,count(*)::float8 FROM (
 SELECT provider,state,'hot'::text store FROM public.payment_webhook_key_versions
 UNION ALL
 SELECT provider,state,'archive' FROM public.payment_webhook_key_version_archive
) versions GROUP BY provider,state,store ORDER BY provider,state,store`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var provider, state, store string
		var count float64
		if err := rows.Scan(&provider, &state, &store, &count); err != nil {
			rows.Close()
			return err
		}
		snapshot.gauges = append(snapshot.gauges, durableGauge{"webhook_key_version_count", []string{
			normalize(provider, allowedM7Providers, "unknown"), normalize(state, allowedWebhookKeyStates, "unknown"), normalize(store, allowedWebhookKeyStores, "unknown"),
		}, count})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `SELECT provider,COALESCE(from_state,'none'),to_state,
 CASE WHEN result='committed' THEN 'success' ELSE 'failure' END,count(*)::float8
FROM public.payment_webhook_key_rotation_audit
GROUP BY provider,COALESCE(from_state,'none'),to_state,CASE WHEN result='committed' THEN 'success' ELSE 'failure' END
ORDER BY provider,COALESCE(from_state,'none'),to_state`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var provider, fromState, toState, result string
		var count float64
		if err := rows.Scan(&provider, &fromState, &toState, &result, &count); err != nil {
			rows.Close()
			return err
		}
		snapshot.counters = append(snapshot.counters, durableCounter{"webhook_key_rotation_total", []string{
			normalize(provider, allowedM7Providers, "unknown"), normalize(fromState, allowedWebhookKeyStates, "unknown"),
			normalize(toState, allowedWebhookKeyStates, "unknown"), normalize(result, allowedM7Results, "unknown"),
		}, count})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	return nil
}

func loadRegionalMetrics(ctx context.Context, tx pgx.Tx, snapshot *durableOperationsSnapshot) error {
	rows, err := tx.Query(ctx, `SELECT region,epoch::float8 FROM public.regional_write_authority WHERE singleton`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var region string
		var epoch float64
		if err := rows.Scan(&region, &epoch); err != nil {
			rows.Close()
			return err
		}
		snapshot.gauges = append(snapshot.gauges, durableGauge{"regional_active_epoch", []string{normalize(region, allowedRegions, "unknown")}, epoch})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `SELECT operation_kind,target_region,phase.key,
 EXTRACT(EPOCH FROM phase.value::timestamptz-LAG(phase.value::timestamptz) OVER(PARTITION BY operation_id ORDER BY phase.value::timestamptz))::float8
FROM public.regional_failover_operations CROSS JOIN LATERAL jsonb_each_text(phase_timestamps) phase ORDER BY operation_id,phase.value`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var kind, region, phase string
		var duration *float64
		if err := rows.Scan(&kind, &region, &phase, &duration); err != nil {
			rows.Close()
			return err
		}
		region, phase = normalize(region, allowedRegions, "unknown"), normalize(phase, allowedRecoveryPhases, "unknown")
		name, durationName := "regional_failover_total", "regional_failover_duration_seconds"
		if kind == "failback" {
			name, durationName = "regional_failback_total", "regional_failback_duration_seconds"
		}
		snapshot.counters = append(snapshot.counters, durableCounter{name, []string{region, phase, "success"}, 1})
		if duration != nil {
			snapshot.histograms = append(snapshot.histograms, durableHistogram{durationName, []string{region, phase, "success"}, 1, maxZero(*duration)})
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `SELECT target_region,(checkpoint->>'rto_ms')::float8/1000,
 (checkpoint#>>'{rpo,control,window_ms}')::float8/1000,(checkpoint#>>'{rpo,shard_0,window_ms}')::float8/1000,(checkpoint#>>'{rpo,shard_1,window_ms}')::float8/1000
FROM public.regional_failover_operations WHERE checkpoint ? 'rto_ms' AND checkpoint ? 'rpo' ORDER BY updated_at DESC LIMIT 1`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var region string
		var rto, control, shard0, shard1 float64
		if err := rows.Scan(&region, &rto, &control, &shard0, &shard1); err != nil {
			rows.Close()
			return err
		}
		region = normalize(region, allowedRegions, "unknown")
		snapshot.histograms = append(snapshot.histograms, durableHistogram{"regional_rto_observed_seconds", []string{region, "rto_recorded", "success"}, 1, maxZero(rto)})
		for _, value := range []struct {
			role, shard string
			seconds     float64
		}{{"control", "none", control}, {"booking_shard", "shard-0", shard0}, {"booking_shard", "shard-1", shard1}} {
			snapshot.gauges = append(snapshot.gauges, durableGauge{"regional_rpo_observed_seconds", []string{region, value.role, value.shard}, maxZero(value.seconds)})
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	return nil
}

func loadReplicationMetrics(ctx context.Context, tx pgx.Tx, region, databaseRole, shardID string, snapshot *durableOperationsSnapshot) error {
	rows, err := tx.Query(ctx, replicationMetricsSQL)
	if err != nil {
		return err
	}
	for rows.Next() {
		var bytes, lag, replay float64
		if err := rows.Scan(&bytes, &lag, &replay); err != nil {
			rows.Close()
			return err
		}
		labels := []string{region, databaseRole, shardID}
		snapshot.gauges = append(snapshot.gauges, durableGauge{"regional_replication_lag_bytes", labels, bytes}, durableGauge{"regional_replication_lag_seconds", labels, lag}, durableGauge{"regional_last_replay_timestamp_seconds", labels, replay})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	return nil
}

const replicationMetricsSQL = `WITH observation AS (
 SELECT pg_last_wal_receive_lsn() AS receive_lsn,
        pg_last_wal_replay_lsn() AS replay_lsn,
        pg_last_xact_replay_timestamp() AS replayed_at
 WHERE pg_is_in_recovery()
)
SELECT GREATEST(pg_wal_lsn_diff(receive_lsn,replay_lsn),0)::float8,
 CASE WHEN receive_lsn=replay_lsn THEN 0::float8
      ELSE GREATEST(EXTRACT(EPOCH FROM clock_timestamp()-replayed_at),0)::float8 END,
 EXTRACT(EPOCH FROM replayed_at)::float8
FROM observation
WHERE receive_lsn IS NOT NULL AND replay_lsn IS NOT NULL AND replayed_at IS NOT NULL`

func loadBackupMetrics(ctx context.Context, tx pgx.Tx, snapshot *durableOperationsSnapshot) error {
	rows, err := tx.Query(ctx, `SELECT database_id,count(*)::float8,GREATEST(EXTRACT(EPOCH FROM clock_timestamp()-max(created_at)),0)::float8 FROM public.backup_artifacts GROUP BY database_id ORDER BY database_id`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var db string
		var count, age float64
		if err := rows.Scan(&db, &count, &age); err != nil {
			rows.Close()
			return err
		}
		role, shard := legacyDatabaseLabels(db)
		snapshot.counters = append(snapshot.counters, durableCounter{"backup_total", []string{role, shard, "success"}, count})
		snapshot.gauges = append(snapshot.gauges, durableGauge{"backup_age_seconds", []string{role, shard}, age})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `SELECT artifact.database_id,verification.state,COALESCE(verification.bounded_error_category,'none'),count(*)::float8
FROM public.backup_verifications verification JOIN public.backup_artifacts artifact USING(backup_id) GROUP BY artifact.database_id,verification.state,verification.bounded_error_category ORDER BY artifact.database_id,verification.state`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var db, state, reason string
		var count float64
		if err := rows.Scan(&db, &state, &reason, &count); err != nil {
			rows.Close()
			return err
		}
		if state == "failed" {
			role, shard := legacyDatabaseLabels(db)
			reason = normalize(reason, allowedM7Reasons, "unknown")
			snapshot.counters = append(snapshot.counters, durableCounter{"backup_failure_total", []string{role, shard, reason}, count})
			if reason == "checksum" {
				snapshot.counters = append(snapshot.counters, durableCounter{"backup_checksum_failure_total", []string{role, shard, "checksum"}, count})
			}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `SELECT database_id,state,count(*)::bigint,COALESCE(sum(EXTRACT(EPOCH FROM completed_at-started_at)),0)::float8,GREATEST(EXTRACT(EPOCH FROM clock_timestamp()-max(completed_at)),0)::float8
FROM public.restore_validations WHERE completed_at IS NOT NULL GROUP BY database_id,state ORDER BY database_id,state`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var db, state string
		var count uint64
		var sum, age float64
		if err := rows.Scan(&db, &state, &count, &sum, &age); err != nil {
			rows.Close()
			return err
		}
		role, shard := legacyDatabaseLabels(db)
		result := "failure"
		if state == "passed" {
			result = "success"
		}
		snapshot.histograms = append(snapshot.histograms, durableHistogram{"backup_restore_duration_seconds", []string{role, shard, result}, count, maxZero(sum)})
		snapshot.gauges = append(snapshot.gauges, durableGauge{"backup_restore_test_age_seconds", []string{role, shard}, maxZero(age)})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	return nil
}

func maxZero(value float64) float64 {
	if value < 0 {
		return 0
	}
	return value
}
