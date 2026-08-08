// Package postgres implements bounded reconciliation observations against the
// payment control plane and the one currently assigned physical booking shard.
package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	paymentreconcile "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/reconcile"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type controlDB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type shardResolver interface {
	Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error)
}

type Store struct {
	control  controlDB
	resolver shardResolver
}

func New(control controlDB, resolver shardResolver) (*Store, error) {
	if control == nil || resolver == nil {
		return nil, paymentreconcile.ErrInvalidConfiguration
	}
	return &Store{control: control, resolver: resolver}, nil
}

func (s *Store) CandidateIntentIDs(ctx context.Context, scope paymentreconcile.Scope, staleBefore time.Time, limit int) ([]uuid.UUID, bool, error) {
	if s == nil || s.control == nil || ctx == nil || !scope.Valid() || staleBefore.IsZero() || limit < 1 || limit > paymentreconcile.MaxBatchSize {
		return nil, false, paymentreconcile.ErrInvalidRequest
	}
	rows, err := s.control.Query(ctx, candidateIntentSQL, string(scope), staleBefore.UTC(), limit+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	ids := make([]uuid.UUID, 0, limit+1)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, false, err
		}
		if id == uuid.Nil {
			return nil, false, errors.New("invalid payment reconciliation candidate")
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	truncated := len(ids) > limit
	if truncated {
		ids = ids[:limit]
	}
	return ids, truncated, nil
}

const candidateIntentSQL = `
WITH candidate_changes AS (
    SELECT intent.payment_intent_id,
           GREATEST(
               intent.updated_at,
               COALESCE(saga.updated_at, intent.updated_at),
               CASE WHEN $1 IN ('payment-operations','payment-provider','payment-all')
                    THEN COALESCE(operation.last_updated_at, intent.updated_at)
                    ELSE intent.updated_at END,
               CASE WHEN $1 IN ('payment-webhooks','payment-all')
                    THEN COALESCE(webhook.last_received_at, intent.updated_at)
                    ELSE intent.updated_at END
           ) AS last_changed_at
    FROM public.payment_intents AS intent
    LEFT JOIN LATERAL (
        SELECT max(updated_at) AS updated_at
        FROM public.payment_sagas
        WHERE payment_intent_id=intent.payment_intent_id
    ) AS saga ON true
    LEFT JOIN LATERAL (
        SELECT max(updated_at) AS last_updated_at
        FROM public.payment_operations
        WHERE payment_intent_id=intent.payment_intent_id
    ) AS operation ON true
    LEFT JOIN LATERAL (
        SELECT max(received_at) AS last_received_at
        FROM public.payment_webhook_inbox
        WHERE provider=intent.provider
          AND provider_payment_id=intent.provider_payment_id
    ) AS webhook ON true
)
SELECT changed.payment_intent_id
FROM candidate_changes AS changed
WHERE changed.last_changed_at <= $2
  AND (
    EXISTS (
      SELECT 1 FROM public.payment_intents AS recovering
      WHERE recovering.payment_intent_id=changed.payment_intent_id
        AND recovering.state IN (
          'reservation_securing','checkout_pending','authorization_pending',
          'capture_pending','captured','ticket_issue_pending','void_pending',
          'refund_pending','manual_review'
        )
    )
    OR NOT EXISTS (
      SELECT 1
      FROM public.payment_reconciliation_checkpoints AS checkpoint
      WHERE checkpoint.scope=$1
        AND checkpoint.payment_intent_id=changed.payment_intent_id
        AND checkpoint.state IN ('passed','mismatch','partial')
        AND checkpoint.completed_at >= changed.last_changed_at
    )
  )
ORDER BY changed.last_changed_at,changed.payment_intent_id
LIMIT $3`

func (s *Store) LoadControlSnapshot(ctx context.Context, intentID uuid.UUID) (paymentreconcile.ControlSnapshot, error) {
	if s == nil || s.control == nil || ctx == nil || intentID == uuid.Nil {
		return paymentreconcile.ControlSnapshot{}, paymentreconcile.ErrInvalidRequest
	}
	var (
		snapshot              paymentreconcile.ControlSnapshot
		fingerprint           []byte
		sagaIDText            string
		activeIntents         int64
		activeSagas           int64
		duplicateEvents       int64
		conflicts             int64
		openReviews           int64
		activeReconciliations int64
	)
	err := s.control.QueryRow(ctx, controlSnapshotSQL, intentID).Scan(
		&snapshot.Intent.ID, &snapshot.Intent.ReservationID, &snapshot.Intent.TrainRunID,
		&snapshot.Intent.Provider, &snapshot.Intent.ProviderPaymentID, &snapshot.Intent.State,
		&snapshot.Intent.AmountMinor, &snapshot.Intent.Currency, &fingerprint,
		&activeIntents, &sagaIDText, &snapshot.Saga.State,
		&activeSagas, &duplicateEvents, &conflicts, &openReviews,
		&activeReconciliations,
	)
	if err != nil {
		return paymentreconcile.ControlSnapshot{}, err
	}
	if len(fingerprint) != sha256.Size {
		return paymentreconcile.ControlSnapshot{}, errors.New("invalid payment fingerprint")
	}
	copy(snapshot.Intent.Fingerprint[:], fingerprint)
	snapshot.Intent.ActiveForReservation = boundedCount(activeIntents)
	snapshot.Saga.ActiveCount = boundedCount(activeSagas)
	if sagaIDText != "" {
		snapshot.Saga.ID, err = uuid.Parse(sagaIDText)
		if err != nil || snapshot.Saga.ID == uuid.Nil {
			return paymentreconcile.ControlSnapshot{}, errors.New("invalid payment saga identity")
		}
	}
	snapshot.DuplicateProviderEventIDs = boundedCount(duplicateEvents)
	snapshot.ProviderEventHashConflicts = boundedCount(conflicts)
	snapshot.OpenManualReviewCases = boundedCount(openReviews)
	snapshot.ActiveReconciliationCases = boundedCount(activeReconciliations)

	rows, err := s.control.Query(ctx, `
SELECT operation_id,operation_type,state,COALESCE(provider_operation_id,''),amount_minor,currency
FROM public.payment_operations
WHERE payment_intent_id=$1
ORDER BY created_at,operation_id
LIMIT 1001`, intentID)
	if err != nil {
		return paymentreconcile.ControlSnapshot{}, err
	}
	defer rows.Close()
	for rows.Next() {
		if len(snapshot.Operations) == 1000 {
			return paymentreconcile.ControlSnapshot{}, errors.New("payment operation observation exceeded bound")
		}
		var operation paymentreconcile.Operation
		if err := rows.Scan(&operation.ID, &operation.Type, &operation.State, &operation.ProviderOperationID, &operation.AmountMinor, &operation.Currency); err != nil {
			return paymentreconcile.ControlSnapshot{}, err
		}
		snapshot.Operations = append(snapshot.Operations, operation)
	}
	if err := rows.Err(); err != nil {
		return paymentreconcile.ControlSnapshot{}, err
	}
	return snapshot, nil
}

const controlSnapshotSQL = `
SELECT intent.payment_intent_id,intent.reservation_id,intent.train_run_id,
       intent.provider,COALESCE(intent.provider_payment_id,''),intent.state,
       intent.amount_minor,intent.currency,intent.request_fingerprint,
       (SELECT count(*) FROM public.payment_intents AS active
        WHERE active.reservation_id=intent.reservation_id
          AND active.state NOT IN ('completed','voided','refunded','cancelled','failed','expired')),
       COALESCE((SELECT saga_id::text FROM public.payment_sagas
                 WHERE payment_intent_id=intent.payment_intent_id
                 ORDER BY created_at,saga_id LIMIT 1),''),
       COALESCE((SELECT state FROM public.payment_sagas
                 WHERE payment_intent_id=intent.payment_intent_id
                 ORDER BY created_at,saga_id LIMIT 1),''),
       (SELECT count(*) FROM public.payment_sagas
        WHERE payment_intent_id=intent.payment_intent_id
          AND state NOT IN ('completed','compensated','failed')),
       (SELECT count(*) FROM (
            SELECT provider,provider_event_id FROM public.payment_webhook_inbox
            WHERE provider=intent.provider
              AND provider_payment_id=intent.provider_payment_id
            GROUP BY provider,provider_event_id HAVING count(*) > 1
        ) AS duplicates),
       (SELECT count(*) FROM public.payment_provider_event_conflicts AS conflict
        JOIN public.payment_webhook_inbox AS inbox
          ON inbox.provider=conflict.provider
         AND inbox.provider_event_id=conflict.provider_event_id
         AND inbox.payload_hash=conflict.canonical_payload_hash
        WHERE inbox.provider=intent.provider
          AND inbox.provider_payment_id=intent.provider_payment_id
          AND conflict.state IN ('open','investigating')),
       (SELECT count(*) FROM public.payment_manual_review_cases
        WHERE payment_intent_id=intent.payment_intent_id
          AND state IN ('open','assigned','investigating')),
       (SELECT count(*) FROM public.payment_reconciliation_checkpoints
        WHERE payment_intent_id=intent.payment_intent_id
          AND state IN ('pending','running'))
FROM public.payment_intents AS intent
WHERE intent.payment_intent_id=$1`

func (s *Store) LoadShardSnapshot(ctx context.Context, intentID uuid.UUID) (snapshot paymentreconcile.ShardSnapshot, returnErr error) {
	if s == nil || s.control == nil || s.resolver == nil || ctx == nil || intentID == uuid.Nil {
		return snapshot, paymentreconcile.ErrInvalidRequest
	}
	var trainRunID uuid.UUID
	if err := s.control.QueryRow(ctx, `SELECT train_run_id FROM public.payment_intents WHERE payment_intent_id=$1`, intentID).Scan(&trainRunID); err != nil {
		return snapshot, err
	}
	resolution, err := s.resolver.Resolve(ctx, trainRunID, false)
	if err != nil {
		return snapshot, err
	}
	snapshot.DirectoryResolved = true
	tx, err := resolution.Handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return snapshot, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	generation := resolution.Route.Generation().Int64()
	err = tx.QueryRow(ctx, `
SELECT status,total_amount_minor,currency
FROM public.reservations
WHERE payment_intent_id=$1 AND train_run_id=$2 AND assignment_generation=$3`, intentID, trainRunID, generation).Scan(
		&snapshot.ReservationState, &snapshot.ReservationAmountMinor, &snapshot.ReservationCurrency,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return snapshot, err
		}
		return snapshot, nil
	}
	if err != nil {
		return snapshot, err
	}
	snapshot.Found = true
	err = tx.QueryRow(ctx, `
SELECT id,status,total_amount_minor,currency
FROM public.ticket_orders
WHERE payment_intent_id=$1 AND train_run_id=$2 AND assignment_generation=$3`, intentID, trainRunID, generation).Scan(
		&snapshot.TicketOrderID, &snapshot.TicketOrderState, &snapshot.TicketOrderAmountMinor, &snapshot.TicketOrderCurrency,
	)
	if err == nil {
		snapshot.TicketOrderFound = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return snapshot, err
	}
	var issuanceIntent uuid.UUID
	err = tx.QueryRow(ctx, `
SELECT payment_intent_id
FROM public.ticket_issuance_receipts
WHERE payment_intent_id=$1 AND train_run_id=$2 AND assignment_generation=$3`, intentID, trainRunID, generation).Scan(&issuanceIntent)
	if err == nil {
		snapshot.IssuanceReceiptFound = true
		snapshot.IssuancePaymentIntentID = issuanceIntent
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return snapshot, err
	}
	var receiptFingerprint []byte
	err = tx.QueryRow(ctx, `
SELECT request_fingerprint
FROM public.payment_command_receipts
WHERE payment_intent_id=$1 AND operation='reservation.payment_begin' AND status='succeeded'`, intentID).Scan(&receiptFingerprint)
	if err == nil {
		if len(receiptFingerprint) != sha256.Size {
			return snapshot, errors.New("invalid shard payment fingerprint")
		}
		copy(snapshot.ReceiptFingerprint[:], receiptFingerprint)
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return snapshot, err
	}
	if snapshot.TicketOrderFound {
		if err := tx.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE status='active'),
       count(*) FILTER (WHERE status='refund_pending'),
       count(*) FILTER (WHERE status='cancelled'),
       count(*)-count(DISTINCT ticket_code)
FROM public.tickets
WHERE ticket_order_id=$1 AND train_run_id=$2 AND assignment_generation=$3`, snapshot.TicketOrderID, trainRunID, generation).Scan(
			&snapshot.ActiveTicketCount, &snapshot.RefundPendingTicketCount,
			&snapshot.CancelledTicketCount, &snapshot.DuplicateTicketCodeCount,
		); err != nil {
			return snapshot, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

func (s *Store) StartCheckpoint(ctx context.Context, scope paymentreconcile.Scope, intentID uuid.UUID, repair bool, now time.Time) (paymentreconcile.Checkpoint, error) {
	if s == nil || s.control == nil || ctx == nil || !scope.Valid() || now.IsZero() {
		return paymentreconcile.Checkpoint{}, paymentreconcile.ErrInvalidRequest
	}
	tx, err := s.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return paymentreconcile.Checkpoint{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	checkpoint := paymentreconcile.Checkpoint{ID: uuid.New(), Scope: scope, PaymentIntentID: intentID, Repair: repair, StartedAt: now.UTC()}
	mode := "detect_only"
	if repair {
		mode = "safe_repair"
	}
	var nullableIntent any
	if intentID != uuid.Nil {
		nullableIntent = intentID
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.payment_reconciliation_checkpoints(
 reconciliation_id,scope,payment_intent_id,mode,next_attempt_at
) VALUES($1,$2,$3,$4,$5)`, checkpoint.ID, string(scope), nullableIntent, mode, now.UTC()); err != nil {
		return paymentreconcile.Checkpoint{}, err
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.payment_reconciliation_checkpoints
SET state='running',attempts=1,lease_owner=$2,lease_until=$3,started_at=$4
WHERE reconciliation_id=$1 AND state='pending'`, checkpoint.ID, "reconciler:"+checkpoint.ID.String(), now.Add(5*time.Minute).UTC(), now.UTC()); err != nil {
		return paymentreconcile.Checkpoint{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentreconcile.Checkpoint{}, err
	}
	return checkpoint, nil
}

func (s *Store) FinishCheckpoint(ctx context.Context, checkpoint paymentreconcile.Checkpoint, result paymentreconcile.CheckpointResult) error {
	if s == nil || s.control == nil || ctx == nil || checkpoint.ID == uuid.Nil || result.CompletedAt.IsZero() || result.RowsExamined < 0 || result.MismatchCount < 0 || result.RepairCount < 0 || result.MismatchCount > result.RowsExamined || result.RepairCount > result.MismatchCount {
		return paymentreconcile.ErrInvalidRequest
	}
	state := "passed"
	if result.Failed {
		state = "failed"
	} else if result.Truncated {
		state = "partial"
	} else if result.MismatchCount > 0 {
		state = "mismatch"
	}
	var category any
	if result.Failed {
		category = boundedCategory(result.ErrorCategory)
	}
	tag, err := s.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer func() { _ = tag.Rollback(context.WithoutCancel(ctx)) }()
	command, err := tag.Exec(ctx, `
UPDATE public.payment_reconciliation_checkpoints
SET state=$2,rows_examined=$3,mismatch_count=$4,repair_count=$5,truncated=$6,
    bounded_error_category=$7,lease_owner=NULL,lease_until=NULL,completed_at=$8
WHERE reconciliation_id=$1 AND state='running'`, checkpoint.ID, state, result.RowsExamined,
		result.MismatchCount, result.RepairCount, result.Truncated, category, result.CompletedAt.UTC())
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return errors.New("payment reconciliation checkpoint lease lost")
	}
	return tag.Commit(ctx)
}

func (s *Store) EscalateManualReview(ctx context.Context, reconciliationID, intentID uuid.UUID, reason string, due time.Time) (bool, error) {
	if s == nil || s.control == nil || ctx == nil || reconciliationID == uuid.Nil || intentID == uuid.Nil || boundedCategory(reason) != reason || due.IsZero() {
		return false, paymentreconcile.ErrInvalidRequest
	}
	command, err := s.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer func() { _ = command.Rollback(context.WithoutCancel(ctx)) }()
	tag, err := command.Exec(ctx, `
INSERT INTO public.payment_manual_review_cases(
 review_case_id,payment_intent_id,reconciliation_id,reason_category,review_due_at
) VALUES($1,$2,$3,$4,$5)
ON CONFLICT (payment_intent_id,reason_category)
 WHERE payment_intent_id IS NOT NULL AND state IN ('open','assigned','investigating')
DO NOTHING`, uuid.New(), intentID, reconciliationID, reason, due.UTC())
	if err != nil {
		return false, err
	}
	if err := command.Commit(ctx); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func boundedCount(value int64) int {
	if value < 0 {
		return 0
	}
	maximum := int64(^uint(0) >> 1)
	if value > maximum {
		return int(maximum)
	}
	return int(value)
}

func boundedCategory(value string) string {
	if value == "" {
		return "reconciliation_failed"
	}
	for index, character := range value {
		if index >= 64 || (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' {
			return "reconciliation_failed"
		}
	}
	return value
}

func (s *Store) Ready(ctx context.Context) error {
	if s == nil || s.control == nil || s.resolver == nil || ctx == nil {
		return paymentreconcile.ErrInvalidConfiguration
	}
	var version int
	var dirty bool
	if err := s.control.QueryRow(ctx, `SELECT version,dirty FROM public.schema_migrations LIMIT 1`).Scan(&version, &dirty); err != nil || version != 10 || dirty {
		return errors.New("payment reconciliation control schema unavailable")
	}
	return nil
}

var _ paymentreconcile.Store = (*Store)(nil)
