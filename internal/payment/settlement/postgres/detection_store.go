package postgres

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const detectionPageSQL = `
WITH evidence AS (
 SELECT provider,provider_account_id,'balance_transaction'::text AS record_kind,
        provider_record_id,payment_correlation,operation_type,gross_minor,fee_minor,net_minor,
        currency,provider_created_at,provider_settlement_id,provider_payout_id,payout_status,imported_at
 FROM public.provider_balance_transactions
 WHERE imported_at <= $7
 UNION ALL
 SELECT provider,provider_account_id,'settlement_batch',provider_record_id,
        payment_correlation,operation_type,gross_minor,fee_minor,net_minor,currency,
        provider_created_at,provider_settlement_id,provider_payout_id,payout_status,imported_at
 FROM public.provider_settlement_batches
 WHERE imported_at <= $7
 UNION ALL
 SELECT provider,provider_account_id,'settlement_line',provider_record_id,
        payment_correlation,operation_type,gross_minor,fee_minor,net_minor,currency,
        provider_created_at,provider_settlement_id,provider_payout_id,payout_status,imported_at
 FROM public.provider_settlement_lines
 WHERE imported_at <= $7
 UNION ALL
 SELECT provider,provider_account_id,'payout',provider_record_id,
        payment_correlation,operation_type,gross_minor,fee_minor,net_minor,currency,
        provider_created_at,provider_settlement_id,provider_payout_id,payout_status,imported_at
 FROM public.provider_payouts
 WHERE imported_at <= $7
 UNION ALL
 SELECT provider,provider_account_id,'payout_line',provider_record_id,
        payment_correlation,operation_type,gross_minor,fee_minor,net_minor,currency,
        provider_created_at,provider_settlement_id,provider_payout_id,payout_status,imported_at
 FROM public.provider_payout_lines
 WHERE imported_at <= $7
), local_operations AS (
 SELECT operation.provider,operation.operation_id,operation.provider_operation_id,
        intent.provider_payment_id,operation.operation_type,operation.amount_minor,
        operation.currency,operation.completed_at,
        CASE operation.operation_type
          WHEN 'capture' THEN 'capture:'||operation.operation_id::text
          WHEN 'refund' THEN 'refund:'||operation.operation_id::text
        END AS ledger_event_id
 FROM public.payment_operations AS operation
 JOIN public.payment_intents AS intent
   ON intent.payment_intent_id=operation.payment_intent_id
  AND intent.provider=operation.provider
 WHERE operation.state='succeeded'
   AND operation.operation_type IN ('capture','refund')
   AND operation.completed_at <= $7
 UNION ALL
 SELECT operation.provider,operation.refund_operation_id,
        operation.provider_refund_id,operation.provider_payment_id,
        'refund',operation.amount_minor,operation.currency,operation.completed_at,
        'partial_refund:'||operation.refund_operation_id::text
 FROM public.ticket_refund_operations AS operation
 WHERE operation.state='succeeded'
   AND operation.completed_at <= $7
), scoped AS (
 SELECT evidence.*,
        count(*) OVER(PARTITION BY provider,provider_account_id,record_kind,provider_record_id,COALESCE(payout_status,'')) AS duplicate_count,
        EXISTS(
          SELECT 1 FROM public.provider_settlement_import_conflicts AS conflict
          WHERE conflict.provider=evidence.provider
            AND conflict.provider_account_id=evidence.provider_account_id
            AND conflict.record_kind=evidence.record_kind
            AND conflict.provider_record_id=evidence.provider_record_id
            AND conflict.detected_at <= $7
        ) AS event_conflict
 FROM evidence
 WHERE CASE $1
   WHEN 'payment' THEN payment_correlation=$2
   WHEN 'settlement' THEN provider_settlement_id=$2 OR (record_kind='settlement_batch' AND provider_record_id=$2)
   WHEN 'payout' THEN provider_payout_id=$2 OR (record_kind='payout' AND provider_record_id=$2)
   WHEN 'period' THEN provider_created_at >= $5 AND provider_created_at < $6
   ELSE false
 END
), enriched AS (
 SELECT scoped.*,
        operation.operation_id IS NOT NULL AS operation_found,
        COALESCE(operation.amount_minor,0) AS operation_amount,
        COALESCE(operation.currency,'') AS operation_currency,
        COALESCE(ledger.transaction_count,0) AS ledger_count,
        COALESCE(ledger.debit_total,0) AS ledger_amount,
        COALESCE(ledger.fee_total,0) AS ledger_fee,
        COALESCE(ledger.currency,'') AS ledger_currency,
        COALESCE(ledger.balanced,false) AS ledger_balanced
 FROM scoped
 LEFT JOIN LATERAL (
   SELECT candidate.operation_id,candidate.amount_minor,candidate.currency,
          candidate.ledger_event_id
   FROM local_operations AS candidate
   WHERE scoped.payment_correlation IS NOT NULL AND (
       candidate.operation_id::text=scoped.payment_correlation
       OR candidate.provider_operation_id=scoped.payment_correlation
       OR (scoped.operation_type='capture' AND candidate.provider_payment_id=scoped.payment_correlation)
   )
     AND candidate.provider=scoped.provider
     AND candidate.operation_type=scoped.operation_type
   ORDER BY CASE WHEN candidate.provider_operation_id=scoped.payment_correlation THEN 0 ELSE 1 END,
            candidate.completed_at DESC,candidate.operation_id
   LIMIT 1
 ) AS operation ON true
 LEFT JOIN LATERAL (
   SELECT count(*)::bigint AS transaction_count,
          LEAST(COALESCE(sum(summary.effect_total),0),9223372036854775807)::bigint AS debit_total,
          LEAST(COALESCE(sum(summary.fee_total),0),9223372036854775807)::bigint AS fee_total,
          min(summary.currency) AS currency,
          bool_and(summary.debit_total=summary.credit_total) AS balanced
   FROM (
     SELECT ledger_tx.transaction_id,ledger_tx.currency,
            sum(CASE WHEN posting.side='debit' THEN posting.amount_minor ELSE 0 END)::bigint AS debit_total,
            sum(CASE WHEN posting.side='credit' THEN posting.amount_minor ELSE 0 END)::bigint AS credit_total,
            sum(CASE
              WHEN ledger_tx.purpose='capture' AND posting.account_code='provider_receivable' AND posting.side='debit' THEN posting.amount_minor
              WHEN ledger_tx.purpose='refund' AND posting.account_code='provider_refund_receivable' AND posting.side='credit' THEN posting.amount_minor
              WHEN ledger_tx.purpose='payout' AND posting.account_code='settlement_cash' AND posting.side='debit' THEN posting.amount_minor
              ELSE 0 END)::bigint AS effect_total,
            sum(CASE WHEN posting.account_code='provider_fee_expense' THEN posting.amount_minor ELSE 0 END)::bigint AS fee_total
     FROM public.financial_ledger_transactions AS ledger_tx
     JOIN public.financial_ledger_postings AS posting
       ON posting.transaction_id=ledger_tx.transaction_id
     WHERE ledger_tx.event_id=operation.ledger_event_id
        OR (ledger_tx.correlation=CASE WHEN scoped.record_kind='payout'
                                      THEN COALESCE(scoped.provider_payout_id,scoped.provider_record_id)
                                      ELSE scoped.provider_record_id END
            AND ledger_tx.purpose IN ('provider_fee','settlement','payout'))
     GROUP BY ledger_tx.transaction_id,ledger_tx.currency
   ) AS summary
 ) AS ledger ON true
), local_missing AS (
 SELECT COALESCE(operation.provider_operation_id,operation.operation_id::text) AS correlation,
        'provider'::text AS evidence_kind,
        true AS expected_present,operation.amount_minor AS expected_amount,
        0::bigint AS expected_fee,operation.currency AS expected_currency,
        false AS observed_present,0::bigint AS observed_amount,
        0::bigint AS observed_fee,''::text AS observed_currency,
        0::integer AS duplicate_count,
        operation.completed_at < clock_timestamp()-interval '7 days' AS aged,
        false AS event_conflict,false AS balance_checked,true AS ledger_balanced,
        operation.provider,''::text AS provider_account_id,
        'local_operation'::text AS record_kind,operation.operation_id::text AS provider_record_id
 FROM local_operations AS operation
 WHERE CASE $1
   WHEN 'payment' THEN $2 IN (
     operation.operation_id::text,
     COALESCE(operation.provider_operation_id,''),
     operation.provider_payment_id
   )
   WHEN 'period' THEN operation.completed_at >= $5 AND operation.completed_at < $6
   ELSE false
 END
 AND NOT EXISTS (
   SELECT 1 FROM evidence
   WHERE evidence.provider=operation.provider
     AND evidence.operation_type=operation.operation_type
     AND (
       operation.operation_id::text=evidence.payment_correlation
       OR operation.provider_operation_id=evidence.payment_correlation
       OR (operation.operation_type='capture' AND operation.provider_payment_id=evidence.payment_correlation)
     )
 )
), payout_lifecycle AS (
 SELECT scoped.*,
        row_number() OVER (
          PARTITION BY provider,provider_account_id,
                       COALESCE(provider_payout_id,provider_record_id)
		  ORDER BY imported_at DESC,provider_created_at DESC,payout_status DESC,provider_record_id DESC
        ) AS lifecycle_rank
 FROM scoped
 WHERE record_kind='payout'
), payout_totals AS (
 SELECT 'payout:'||COALESCE(payout.provider_payout_id,payout.provider_record_id) AS correlation,
        'payout'::text AS evidence_kind,
        true AS expected_present,
        CASE WHEN payout.gross_minor < 0 THEN -payout.gross_minor ELSE payout.gross_minor END AS expected_amount,
        payout.fee_minor AS expected_fee,payout.currency AS expected_currency,
        aggregate.evidence_count>0 AS observed_present,
        aggregate.amount_minor AS observed_amount,
        aggregate.fee_minor AS observed_fee,
        aggregate.currency AS observed_currency,
        CASE WHEN aggregate.currency_count>1 THEN 2 ELSE 1 END AS duplicate_count,
        false AS aged,payout.event_conflict,false AS balance_checked,true AS ledger_balanced,
        false AS payout_lifecycle_invalid,
        payout.provider,payout.provider_account_id,payout.record_kind,payout.provider_record_id
 FROM payout_lifecycle AS payout
 LEFT JOIN LATERAL (
   SELECT count(*)::bigint AS evidence_count,
          LEAST(COALESCE(sum(abs(line.net_minor::numeric)),0),9223372036854775807)::bigint AS amount_minor,
          LEAST(COALESCE(sum(line.fee_minor::numeric),0),9223372036854775807)::bigint AS fee_minor,
          COALESCE(min(line.currency),'') AS currency,
          count(DISTINCT line.currency) AS currency_count
   FROM (
     SELECT candidate.*,
            CASE candidate.record_kind
              WHEN 'payout_line' THEN 1
              WHEN 'settlement_line' THEN 2
              ELSE 3
            END AS priority,
            min(CASE candidate.record_kind
                  WHEN 'payout_line' THEN 1
                  WHEN 'settlement_line' THEN 2
                  ELSE 3
                END) OVER () AS selected_priority
     FROM evidence AS candidate
     WHERE candidate.provider=payout.provider
       AND candidate.provider_account_id=payout.provider_account_id
       AND candidate.provider_payout_id=COALESCE(payout.provider_payout_id,payout.provider_record_id)
       AND candidate.record_kind IN ('balance_transaction','settlement_line','payout_line')
   ) AS line
   WHERE line.priority=line.selected_priority
 ) AS aggregate ON true
 WHERE payout.lifecycle_rank=1 AND payout.payout_status='paid'
), payout_lifecycle_checks AS (
 SELECT 'payout:'||COALESCE(payout.provider_payout_id,payout.provider_record_id) AS correlation,
        'payout'::text AS evidence_kind,
        true AS expected_present,0::bigint AS expected_amount,0::bigint AS expected_fee,
        payout.currency AS expected_currency,
        true AS observed_present,0::bigint AS observed_amount,0::bigint AS observed_fee,
        payout.currency AS observed_currency,payout.duplicate_count::integer,
        (payout.payout_status IN ('pending','in_transit') AND payout.provider_created_at < clock_timestamp()-interval '7 days') AS aged,
        payout.event_conflict,false AS balance_checked,true AS ledger_balanced,
        payout.payout_status NOT IN ('pending','in_transit','paid') AS payout_lifecycle_invalid,
        payout.provider,payout.provider_account_id,payout.record_kind,payout.provider_record_id
 FROM payout_lifecycle AS payout
 WHERE payout.lifecycle_rank=1
), comparisons AS (
 SELECT COALESCE(payment_correlation,provider_record_id) AS correlation,
        'payment_operation'::text AS evidence_kind,
        true AS expected_present,
        CASE WHEN gross_minor < 0 THEN -gross_minor ELSE gross_minor END AS expected_amount,
        0::bigint AS expected_fee,currency AS expected_currency,
        operation_found AS observed_present,operation_amount AS observed_amount,
        0::bigint AS observed_fee,operation_currency AS observed_currency,
        duplicate_count::integer,
        (provider_created_at < clock_timestamp()-interval '7 days' AND NOT operation_found) AS aged,
        event_conflict,false AS balance_checked,true AS ledger_balanced,false AS payout_lifecycle_invalid,
        provider,provider_account_id,record_kind,provider_record_id,'payment_operation'::text AS comparison_class
 FROM enriched
 WHERE payment_correlation IS NOT NULL AND operation_type IN ('capture','refund')
 UNION ALL
 SELECT COALESCE(payment_correlation,provider_record_id),'ledger',true,
        CASE WHEN operation_type IN ('capture','refund')
             THEN CASE WHEN gross_minor < 0 THEN -gross_minor ELSE gross_minor END
             ELSE CASE WHEN net_minor < 0 THEN -net_minor ELSE net_minor END END,
        fee_minor,currency,(ledger_count>0),ledger_amount,ledger_fee,ledger_currency,
        duplicate_count::integer,
        (provider_created_at < clock_timestamp()-interval '7 days' AND ledger_count=0),
        event_conflict,(ledger_count>0),ledger_balanced,false,
        provider,provider_account_id,record_kind,provider_record_id,'ledger'
 FROM enriched
 WHERE NOT (record_kind='payout' AND COALESCE(payout_status,'')<>'paid')
 UNION ALL
 SELECT correlation,evidence_kind,expected_present,expected_amount,expected_fee,
        expected_currency,observed_present,observed_amount,observed_fee,
        observed_currency,duplicate_count,aged,event_conflict,balance_checked,
        ledger_balanced,false,provider,provider_account_id,record_kind,provider_record_id,'provider'
 FROM local_missing
 UNION ALL
 SELECT correlation,evidence_kind,expected_present,expected_amount,expected_fee,
        expected_currency,observed_present,observed_amount,observed_fee,
        observed_currency,duplicate_count,aged,event_conflict,balance_checked,
        ledger_balanced,payout_lifecycle_invalid,provider,provider_account_id,record_kind,provider_record_id,'payout_total'
 FROM payout_totals
 UNION ALL
 SELECT correlation,evidence_kind,expected_present,expected_amount,expected_fee,
        expected_currency,observed_present,observed_amount,observed_fee,
        observed_currency,duplicate_count,aged,event_conflict,balance_checked,
        ledger_balanced,payout_lifecycle_invalid,provider,provider_account_id,record_kind,provider_record_id,'payout_lifecycle'
 FROM payout_lifecycle_checks
)
SELECT correlation,evidence_kind,expected_present,expected_amount,expected_fee,
       expected_currency,observed_present,observed_amount,observed_fee,
       observed_currency,duplicate_count,aged,event_conflict,balance_checked,
       ledger_balanced,payout_lifecycle_invalid
FROM comparisons
ORDER BY provider,provider_account_id,record_kind,provider_record_id,evidence_kind,comparison_class,correlation
OFFSET $3 LIMIT $4`

const insertDetectionRunSQL = `
INSERT INTO public.settlement_reconciliation_runs(
 run_id,scope_type,scope_value,started_at,completed_at,pages,examined,
 completed,bounded,finding_count,created_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,clock_timestamp())`

const insertMismatchSQL = `
INSERT INTO public.settlement_reconciliation_mismatches(
 run_id,finding_index,correlation,evidence_kind,reason,created_at
) VALUES($1,$2,$3,$4,$5,clock_timestamp())`

func (store *Store) ReadDetectionPage(ctx context.Context, scope settlement.DetectionScope, cursor string, limit int) (settlement.DetectionPage, error) {
	if store == nil || store.db == nil || !validDetectionScope(scope) || limit <= 0 || limit > 1000 {
		return settlement.DetectionPage{}, settlement.ErrInvalidDetectionScope
	}
	offset, asOf, err := parseDetectionCursor(cursor, time.Now().UTC())
	if err != nil {
		return settlement.DetectionPage{}, settlement.ErrDetectionCursor
	}
	periodStart, periodEnd, err := detectionPeriod(scope)
	if err != nil {
		return settlement.DetectionPage{}, err
	}
	rows, err := store.db.Query(ctx, detectionPageSQL,
		scope.Kind, scope.Value, offset, limit+1, periodStart, periodEnd, asOf,
	)
	if err != nil {
		return settlement.DetectionPage{}, err
	}
	defer rows.Close()
	comparisons := make([]settlement.Comparison, 0, limit+1)
	for rows.Next() {
		var comparison settlement.Comparison
		var kind string
		if err := rows.Scan(
			&comparison.Correlation, &kind,
			&comparison.Expected.Present, &comparison.Expected.AmountMinor,
			&comparison.Expected.FeeMinor, &comparison.Expected.Currency,
			&comparison.Observed.Present, &comparison.Observed.AmountMinor,
			&comparison.Observed.FeeMinor, &comparison.Observed.Currency,
			&comparison.DuplicateCount, &comparison.Aged, &comparison.EventConflict,
			&comparison.BalanceChecked, &comparison.LedgerBalanced, &comparison.PayoutLifecycleInvalid,
		); err != nil {
			return settlement.DetectionPage{}, err
		}
		comparison.Kind = settlement.EvidenceKind(kind)
		comparisons = append(comparisons, comparison)
	}
	if err := rows.Err(); err != nil {
		return settlement.DetectionPage{}, err
	}
	done := len(comparisons) <= limit
	if !done {
		comparisons = comparisons[:limit]
	}
	nextCursor := cursor
	if len(comparisons) > 0 {
		nextCursor = formatDetectionCursor(asOf, offset+len(comparisons))
	}
	return settlement.DetectionPage{Comparisons: comparisons, NextCursor: nextCursor, Done: done}, nil
}

func (store *Store) AppendDetectionRun(ctx context.Context, run settlement.DetectionRun) error {
	if store == nil || store.db == nil || store.writer == nil || !validDetectionRun(run) {
		return ErrInvalidStore
	}
	return store.writer.Write(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, insertDetectionRunSQL,
			run.ID, run.Scope.Kind, run.Scope.Value, run.StartedAt.UTC(), run.CompletedAt.UTC(),
			run.Pages, run.Examined, run.Completed, run.Bounded, len(run.Findings),
		); err != nil {
			return err
		}
		for index, finding := range run.Findings {
			if _, err := tx.Exec(ctx, insertMismatchSQL,
				run.ID, index, finding.Correlation, finding.Kind, finding.Reason,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func (store *Store) Ready(ctx context.Context) error {
	if store == nil || store.db == nil {
		return ErrInvalidStore
	}
	const query = `SELECT
	to_regclass('public.financial_ledger_accounts') IS NOT NULL AND
	to_regclass('public.financial_ledger_transactions') IS NOT NULL AND
	to_regclass('public.financial_ledger_postings') IS NOT NULL AND
	to_regclass('public.financial_ledger_reversals') IS NOT NULL AND
	to_regclass('public.provider_balance_transactions') IS NOT NULL AND
	to_regclass('public.provider_settlement_batches') IS NOT NULL AND
	to_regclass('public.provider_settlement_lines') IS NOT NULL AND
	to_regclass('public.provider_payouts') IS NOT NULL AND
	to_regclass('public.provider_payout_lines') IS NOT NULL AND
	to_regclass('public.provider_settlement_import_checkpoints') IS NOT NULL AND
	to_regclass('public.provider_settlement_import_conflicts') IS NOT NULL AND
	to_regclass('public.settlement_reconciliation_runs') IS NOT NULL AND
	to_regclass('public.settlement_reconciliation_mismatches') IS NOT NULL AND
	to_regclass('public.settlement_reconciliation_reviews') IS NOT NULL`
	var ready bool
	if err := store.db.QueryRow(ctx, query).Scan(&ready); err != nil {
		return err
	}
	if !ready {
		return ErrInvalidStore
	}
	return nil
}

func validDetectionScope(scope settlement.DetectionScope) bool {
	if !scope.Kind.Valid() {
		return false
	}
	if scope.Kind == settlement.ScopePeriod {
		_, _, err := detectionPeriod(scope)
		return err == nil
	}
	return identityPattern.MatchString(scope.Value)
}

func detectionPeriod(scope settlement.DetectionScope) (any, any, error) {
	if scope.Kind != settlement.ScopePeriod {
		return nil, nil, nil
	}
	parts := strings.Split(scope.Value, "/")
	if len(parts) != 2 {
		return nil, nil, settlement.ErrInvalidDetectionScope
	}
	start, err := time.Parse("2006-01-02", parts[0])
	if err != nil {
		return nil, nil, settlement.ErrInvalidDetectionScope
	}
	end, err := time.Parse("2006-01-02", parts[1])
	if err != nil || !end.After(start) || end.Sub(start) > 366*24*time.Hour {
		return nil, nil, settlement.ErrInvalidDetectionScope
	}
	return start.UTC(), end.UTC(), nil
}

func parseDetectionCursor(cursor string, now time.Time) (int, time.Time, error) {
	if cursor == "" {
		return 0, now.UTC(), nil
	}
	parts := strings.Split(cursor, "|")
	if len(parts) != 2 {
		return 0, time.Time{}, settlement.ErrDetectionCursor
	}
	asOf, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil || asOf.Before(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)) || asOf.After(now.UTC().Add(time.Minute)) {
		return 0, time.Time{}, settlement.ErrDetectionCursor
	}
	offset, err := strconv.Atoi(parts[1])
	if err != nil || offset < 0 || offset > 1_000_000 {
		return 0, time.Time{}, settlement.ErrDetectionCursor
	}
	return offset, asOf.UTC(), nil
}

func formatDetectionCursor(asOf time.Time, offset int) string {
	return asOf.UTC().Format(time.RFC3339Nano) + "|" + strconv.Itoa(offset)
}

func validDetectionRun(run settlement.DetectionRun) bool {
	if run.ID == uuid.Nil || !validDetectionScope(run.Scope) || run.StartedAt.IsZero() ||
		run.CompletedAt.Before(run.StartedAt) || run.Pages < 0 || run.Examined < 0 ||
		len(run.Findings) > run.Examined*9 {
		return false
	}
	for _, finding := range run.Findings {
		if !identityPattern.MatchString(finding.Correlation) || !finding.Kind.Valid() || !validReason(finding.Reason) {
			return false
		}
	}
	return true
}

func validReason(reason settlement.FindingReason) bool {
	switch reason {
	case settlement.FindingMissing, settlement.FindingUnexpected, settlement.FindingAmount,
		settlement.FindingCurrency, settlement.FindingFee, settlement.FindingDuplicate,
		settlement.FindingAge, settlement.FindingEventConflict, settlement.FindingImbalance,
		settlement.FindingPayoutLifecycle:
		return true
	default:
		return false
	}
}

var _ settlement.DetectionStore = (*Store)(nil)
