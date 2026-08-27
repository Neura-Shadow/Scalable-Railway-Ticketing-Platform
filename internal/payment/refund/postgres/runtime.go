package postgres

import (
	"context"
	"encoding/hex"
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger"
	ledgerpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/refund"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const claimRefundWorkSQL = `
WITH candidates AS (
 SELECT refund_saga_id
 FROM public.ticket_refund_sagas
 WHERE state IN('created','validating','refund_pending','provider_uncertain','refund_succeeded','shard_compensating')
   AND next_attempt_at <= clock_timestamp()
   AND (lease_until IS NULL OR lease_until <= clock_timestamp())
 ORDER BY next_attempt_at,updated_at,refund_saga_id
 LIMIT $1
 FOR UPDATE SKIP LOCKED
)
UPDATE public.ticket_refund_sagas AS saga
SET lease_owner=$2,lease_until=clock_timestamp()+$3::interval,updated_at=clock_timestamp()
FROM candidates
WHERE saga.refund_saga_id=candidates.refund_saga_id
RETURNING saga.refund_saga_id`

const recoverExpiredProviderRefundSQL = `
WITH expired AS MATERIALIZED (
 SELECT saga.refund_saga_id,saga.refund_request_id,operation.refund_operation_id
 FROM public.ticket_refund_sagas AS saga
 JOIN public.ticket_refund_operations AS operation
   ON operation.refund_request_id=saga.refund_request_id
 WHERE saga.current_step='refund_provider'
   AND saga.state='refund_pending'
   AND operation.state='processing'
   AND saga.lease_until IS NOT NULL
   AND saga.lease_until <= clock_timestamp()
 ORDER BY saga.lease_until,saga.refund_saga_id
 LIMIT $1
 FOR UPDATE OF saga,operation SKIP LOCKED
), recovered_operations AS (
 UPDATE public.ticket_refund_operations AS operation
 SET state='uncertain',bounded_error_category='worker_lease_expired',
     lease_owner=NULL,lease_until=NULL,next_attempt_at=clock_timestamp(),updated_at=clock_timestamp()
 FROM expired
 WHERE operation.refund_operation_id=expired.refund_operation_id
   AND operation.state='processing'
 RETURNING expired.refund_request_id
), recovered_requests AS (
 UPDATE public.ticket_refund_requests AS request
 SET state='provider_uncertain',updated_at=clock_timestamp()
 FROM recovered_operations
 WHERE request.refund_request_id=recovered_operations.refund_request_id
 RETURNING request.refund_request_id
)
UPDATE public.ticket_refund_sagas AS saga
SET current_step='query_provider',state='provider_uncertain',
    bounded_error_category='worker_lease_expired',lease_owner=NULL,lease_until=NULL,
    next_attempt_at=clock_timestamp(),updated_at=clock_timestamp()
FROM expired
WHERE saga.refund_saga_id=expired.refund_saga_id
  AND EXISTS (
    SELECT 1 FROM recovered_requests
    WHERE recovered_requests.refund_request_id=saga.refund_request_id
  )`

const (
	refundProviderAttemptStates = "('pending','failed_retryable')"
	queryProviderAttemptStates  = "('uncertain','processing')"
)

func (store *Store) Claim(ctx context.Context, claim refund.RefundClaim) ([]refund.RefundWork, error) {
	if store == nil || store.db == nil || ctx == nil || claim.WorkerID == "" || claim.Limit <= 0 || claim.Limit > 100 || claim.LeaseTTL <= 0 {
		return nil, refund.ErrInvalidProcessorState
	}
	tx, err := store.beginControlTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// A provider refund may have committed before a worker died. Recover that
	// expired side-effect window to query-only state before any worker can
	// claim it again. The operation, request, saga, and subsequent claim share
	// this transaction, so no refund_provider claim can observe a partial
	// recovery transition.
	if _, err := tx.Exec(ctx, recoverExpiredProviderRefundSQL, claim.Limit); err != nil {
		return nil, err
	}
	// Lease ownership is based exclusively on PostgreSQL time. Replica wall
	// clock skew must not let a fast worker reclaim another live worker's
	// external-refund lease.
	rows, err := tx.Query(ctx, claimRefundWorkSQL, claim.Limit, claim.WorkerID, claim.LeaseTTL.String())
	if err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0, claim.Limit)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	work := make([]refund.RefundWork, 0, len(ids))
	for _, sagaID := range ids {
		item, err := loadRefundWork(ctx, tx, sagaID, claim.WorkerID)
		if err != nil {
			return nil, err
		}
		work = append(work, item)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return work, nil
}

func loadRefundWork(ctx context.Context, tx pgx.Tx, sagaID uuid.UUID, workerID string) (refund.RefundWork, error) {
	var work refund.RefundWork
	var requestFingerprint, responseFingerprint []byte
	var providerRefundID pgtype.Text
	var prepareReceiptID pgtype.UUID
	var abortReason pgtype.Text
	var operationState string
	err := tx.QueryRow(ctx, `
SELECT saga.refund_saga_id,saga.refund_request_id,operation.refund_operation_id,
       request.payment_intent_id,request.reservation_id,request.ticket_order_id,
       request.train_run_id,request.owner_user_id,request.provider,
       operation.provider_payment_id,operation.provider_refund_id,
       request.amount_minor,operation.captured_total_minor,
       CASE WHEN operation.state='succeeded'
            THEN operation.refunded_total_minor-request.amount_minor
            ELSE operation.refunded_total_minor END,
       request.currency,saga.current_step,saga.attempts,saga.prepare_attempts,operation.attempts,
       saga.lease_owner,request.request_fingerprint,operation.response_fingerprint,
       operation.state,operation.bounded_error_category,request.created_at,request.eligibility_cutoff_at,binding.receipt_id
FROM public.ticket_refund_sagas AS saga
JOIN public.ticket_refund_requests AS request
  ON request.refund_request_id=saga.refund_request_id
JOIN public.ticket_refund_operations AS operation
  ON operation.refund_request_id=request.refund_request_id
LEFT JOIN public.ticket_refund_prepare_bindings AS binding
  ON binding.refund_request_id=request.refund_request_id
WHERE saga.refund_saga_id=$1 AND saga.lease_owner=$2`, sagaID, workerID).Scan(
		&work.SagaID, &work.RequestID, &work.OperationID, &work.PaymentIntentID,
		&work.ReservationID, &work.TicketOrderID, &work.TrainRunID, &work.OwnerID,
		&work.Provider, &work.ProviderPaymentID, &providerRefundID, &work.AmountMinor,
		&work.CapturedMinor, &work.RefundedBeforeMinor, &work.Currency, &work.Step,
		&work.SagaAttempts, &work.PrepareAttempts, &work.OperationAttempts, &work.LeaseOwner,
		&requestFingerprint, &responseFingerprint, &operationState, &abortReason, &work.RequestedAt, &work.EligibilityCutoffAt, &prepareReceiptID,
	)
	if err != nil || len(requestFingerprint) != 32 || work.CapturedMinor <= 0 || work.RefundedBeforeMinor < 0 {
		if err != nil {
			return refund.RefundWork{}, err
		}
		return refund.RefundWork{}, refund.ErrInvalidProcessorState
	}
	copy(work.RequestFingerprint[:], requestFingerprint)
	if len(responseFingerprint) > 0 {
		if len(responseFingerprint) != 32 {
			return refund.RefundWork{}, refund.ErrInvalidProcessorState
		}
		copy(work.ResponseFingerprint[:], responseFingerprint)
	}
	if providerRefundID.Valid {
		work.ProviderRefundID = providerRefundID.String
	}
	if prepareReceiptID.Valid {
		work.PrepareReceiptID = uuid.UUID(prepareReceiptID.Bytes)
	}
	if abortReason.Valid {
		work.AbortReason = abortReason.String
	}
	if work.Step == refund.StepCompensateShard && operationState != "succeeded" {
		return refund.RefundWork{}, refund.ErrInvalidProcessorState
	}
	rows, err := tx.Query(ctx, `SELECT ticket_id FROM public.ticket_refund_request_items
WHERE refund_request_id=$1 ORDER BY ticket_id`, work.RequestID)
	if err != nil {
		return refund.RefundWork{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var ticketID uuid.UUID
		if err := rows.Scan(&ticketID); err != nil {
			return refund.RefundWork{}, err
		}
		work.TicketIDs = append(work.TicketIDs, ticketID)
	}
	if rows.Err() != nil || len(work.TicketIDs) == 0 {
		return refund.RefundWork{}, refund.ErrInvalidProcessorState
	}
	return work, nil
}

func (store *Store) AdvanceValidation(ctx context.Context, work refund.RefundWork, receipt paymentshard.SelectedTicketRefundPrepareReceipt, now time.Time) error {
	tx, err := store.runtimeTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `INSERT INTO public.ticket_refund_prepare_bindings(
 receipt_id,refund_request_id,refund_operation_id,command_id,train_run_id,
 assignment_generation,request_fingerprint,amount_minor,currency,selected_ticket_count,prepared_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (refund_request_id) DO NOTHING`, receipt.ReceiptID, work.RequestID, work.OperationID,
		receipt.CommandID, receipt.TrainRunID, receipt.AssignmentGeneration, receipt.RequestFingerprint[:],
		receipt.AmountMinor, receipt.Currency, receipt.SelectedTicketCount, receipt.PreparedAt.UTC()); err != nil {
		return err
	}
	var boundID uuid.UUID
	var boundFingerprint []byte
	var boundTrain uuid.UUID
	var boundGeneration int64
	var boundAmount int64
	var boundCurrency string
	var boundCount int
	var boundPreparedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT receipt_id,train_run_id,assignment_generation,request_fingerprint,
 amount_minor,currency,selected_ticket_count,prepared_at FROM public.ticket_refund_prepare_bindings
WHERE refund_request_id=$1 AND refund_operation_id=$2 AND command_id=$3`, work.RequestID, work.OperationID, receipt.CommandID).Scan(
		&boundID, &boundTrain, &boundGeneration, &boundFingerprint, &boundAmount, &boundCurrency, &boundCount, &boundPreparedAt); err != nil ||
		boundID != receipt.ReceiptID || boundTrain != work.TrainRunID || boundGeneration != int64(receipt.AssignmentGeneration) ||
		len(boundFingerprint) != 32 || !equalHash(boundFingerprint, work.RequestFingerprint) || boundAmount != work.AmountMinor ||
		boundCurrency != work.Currency || boundCount != len(work.TicketIDs) || !boundPreparedAt.Equal(receipt.PreparedAt.UTC()) {
		return refund.ErrInvalidProcessorTransition
	}
	if err := updateOne(ctx, tx, `UPDATE public.ticket_refund_requests
SET state='refund_pending',updated_at=$3
WHERE refund_request_id=$1 AND state='created'
  AND EXISTS(SELECT 1 FROM public.ticket_refund_sagas
             WHERE refund_request_id=$1 AND lease_owner=$2 AND current_step='validate')`,
		work.RequestID, work.LeaseOwner, now.UTC()); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE public.ticket_refund_request_items
SET state='refund_pending',updated_at=$2
WHERE refund_request_id=$1 AND state='selected'`, work.RequestID, now.UTC()); err != nil {
		return err
	}
	if err := releaseSaga(ctx, tx, work, "refund_provider", "refund_pending", nil, now, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *Store) MarkPrepareRetry(ctx context.Context, work refund.RefundWork, category string, next, now time.Time) error {
	if category == "" || next.Before(now) {
		return refund.ErrInvalidProcessorTransition
	}
	tag, err := store.dbExec(ctx, `UPDATE public.ticket_refund_sagas
SET state='validating',prepare_attempts=prepare_attempts+1,next_attempt_at=$4,
    bounded_error_category=$5,lease_owner=NULL,lease_until=NULL,updated_at=$6
WHERE refund_saga_id=$1 AND refund_request_id=$2 AND lease_owner=$3
  AND current_step='validate' AND state IN('created','validating')`,
		work.SagaID, work.RequestID, work.LeaseOwner, next.UTC(), category, now.UTC())
	if err != nil || tag != 1 {
		return refund.ErrInvalidProcessorTransition
	}
	return nil
}

func (store *Store) MarkPreparationFailed(ctx context.Context, work refund.RefundWork, receipt paymentshard.SelectedTicketRefundReleaseReceipt, category string, now time.Time) error {
	if receipt.PrepareReceiptID == uuid.Nil || receipt.PrepareReceiptID != work.PrepareReceiptID ||
		receipt.RefundRequestID != work.RequestID || receipt.RefundOperationID != work.OperationID ||
		receipt.TrainRunID != work.TrainRunID || receipt.RequestFingerprint != work.RequestFingerprint ||
		receipt.ReleasedTicketCount != len(work.TicketIDs) || receipt.ReleasedAt.IsZero() || category == "" {
		return refund.ErrInvalidProcessorTransition
	}
	tx, err := store.runtimeTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var boundFingerprint []byte
	var boundReceipt uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT receipt_id,request_fingerprint
FROM public.ticket_refund_prepare_bindings
WHERE refund_request_id=$1 AND refund_operation_id=$2 FOR SHARE`, work.RequestID, work.OperationID).Scan(
		&boundReceipt, &boundFingerprint); err != nil || boundReceipt != receipt.PrepareReceiptID || !equalHash(boundFingerprint, work.RequestFingerprint) {
		return refund.ErrInvalidProcessorTransition
	}
	var operationState string
	var providerRefundID pgtype.Text
	if err := tx.QueryRow(ctx, `SELECT state,provider_refund_id FROM public.ticket_refund_operations
WHERE refund_operation_id=$1 AND refund_request_id=$2 FOR SHARE`, work.OperationID, work.RequestID).Scan(
		&operationState, &providerRefundID); err != nil || operationState != "failed_permanent" || providerRefundID.Valid {
		return refund.ErrInvalidProcessorTransition
	}
	if err := updateOne(ctx, tx, `UPDATE public.ticket_refund_requests
SET state='failed',updated_at=$2,completed_at=$2
WHERE refund_request_id=$1 AND state='refund_pending'`, work.RequestID, now.UTC()); err != nil {
		return err
	}
	if tag, err := tx.Exec(ctx, `UPDATE public.ticket_refund_request_items
SET state='failed',updated_at=$2 WHERE refund_request_id=$1 AND state='refund_pending'`, work.RequestID, now.UTC()); err != nil || tag.RowsAffected() != int64(len(work.TicketIDs)) {
		return refund.ErrInvalidProcessorTransition
	}
	if err := updateOne(ctx, tx, `UPDATE public.ticket_refund_sagas
SET current_step='complete',state='failed',bounded_error_category=$4,
    lease_owner=NULL,lease_until=NULL,next_attempt_at=$5,updated_at=$5
WHERE refund_saga_id=$1 AND refund_request_id=$2 AND lease_owner=$3
  AND current_step='release_prepared'`, work.SagaID, work.RequestID, work.LeaseOwner, category, now.UTC()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *Store) BeginPreparationAbort(ctx context.Context, work refund.RefundWork, category string, now time.Time) error {
	if category == "" {
		return refund.ErrInvalidProcessorTransition
	}
	tx, err := store.runtimeTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := updateOne(ctx, tx, `UPDATE public.ticket_refund_operations
SET state='failed_permanent',bounded_error_category=$4,lease_owner=NULL,lease_until=NULL,
    updated_at=$5,completed_at=$5
WHERE refund_operation_id=$1 AND refund_request_id=$2
  AND state IN('pending','processing','failed_retryable') AND provider_refund_id IS NULL
  AND EXISTS(SELECT 1 FROM public.ticket_refund_sagas
             WHERE refund_saga_id=$3 AND lease_owner=$6 AND current_step='refund_provider')`,
		work.OperationID, work.RequestID, work.SagaID, category, now.UTC(), work.LeaseOwner); err != nil {
		return err
	}
	if err := updateOne(ctx, tx, `UPDATE public.ticket_refund_sagas
SET current_step='release_prepared',state='refund_pending',bounded_error_category=$4,updated_at=$5
WHERE refund_saga_id=$1 AND refund_request_id=$2 AND lease_owner=$3
  AND current_step='refund_provider'`, work.SagaID, work.RequestID, work.LeaseOwner, category, now.UTC()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *Store) MarkReleaseRetry(ctx context.Context, work refund.RefundWork, category string, next, now time.Time) error {
	if category == "" || next.Before(now) {
		return refund.ErrInvalidProcessorTransition
	}
	tag, err := store.dbExec(ctx, `UPDATE public.ticket_refund_sagas
SET attempts=attempts+1,state='refund_pending',next_attempt_at=$4,
    bounded_error_category=$5,lease_owner=NULL,lease_until=NULL,updated_at=$6
WHERE refund_saga_id=$1 AND refund_request_id=$2 AND lease_owner=$3
  AND current_step='release_prepared'`, work.SagaID, work.RequestID, work.LeaseOwner,
		next.UTC(), category, now.UTC())
	if err != nil || tag != 1 {
		return refund.ErrInvalidProcessorTransition
	}
	return nil
}

func equalHash(value []byte, expected refund.Hash) bool {
	if len(value) != len(expected) {
		return false
	}
	for index := range value {
		if value[index] != expected[index] {
			return false
		}
	}
	return true
}

func (store *Store) BeginProviderAttempt(ctx context.Context, work refund.RefundWork, now time.Time) error {
	stateCondition := refundProviderAttemptStates
	if work.Step == refund.StepQueryProvider {
		stateCondition = queryProviderAttemptStates
	}
	query := `UPDATE public.ticket_refund_operations AS operation
SET state='processing',attempts=attempts+1,lease_owner=$3,
    lease_until=saga.lease_until,updated_at=$4
FROM public.ticket_refund_sagas AS saga
WHERE operation.refund_operation_id=$1
  AND saga.refund_request_id=operation.refund_request_id
  AND saga.refund_saga_id=$2 AND saga.lease_owner=$3
  AND operation.state IN` + stateCondition
	tag, err := store.dbExec(ctx, query, work.OperationID, work.SagaID, work.LeaseOwner, now.UTC())
	if err != nil || tag != 1 {
		return refund.ErrInvalidProcessorTransition
	}
	return nil
}

func (store *Store) MarkProviderUncertain(ctx context.Context, work refund.RefundWork, category string, now time.Time) error {
	return store.providerTransition(ctx, work, providerTransition{
		operationState: "uncertain", requestState: "provider_uncertain", sagaState: "provider_uncertain",
		step: "query_provider", category: category, next: now,
	}, now)
}

func (store *Store) MarkProviderRetry(ctx context.Context, work refund.RefundWork, category string, next time.Time) error {
	operationState, requestState, sagaState := "failed_retryable", "refund_pending", "refund_pending"
	if work.Step == refund.StepQueryProvider {
		operationState, requestState, sagaState = "uncertain", "provider_uncertain", "provider_uncertain"
	}
	return store.providerTransition(ctx, work, providerTransition{
		operationState: operationState, requestState: requestState, sagaState: sagaState,
		step: string(work.Step), category: category, next: next,
	}, time.Now().UTC())
}

func (store *Store) MarkProviderNotApplied(ctx context.Context, work refund.RefundWork, fingerprint refund.Hash, now time.Time) error {
	tx, err := store.runtimeTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := updateOne(ctx, tx, `UPDATE public.ticket_refund_operations
SET state='pending',response_fingerprint=$4,bounded_error_category=NULL,
    lease_owner=NULL,lease_until=NULL,next_attempt_at=$5,updated_at=$5
WHERE refund_operation_id=$1 AND refund_request_id=$2 AND lease_owner=$3`,
		work.OperationID, work.RequestID, work.LeaseOwner, fingerprint[:], now.UTC()); err != nil {
		return err
	}
	if err := updateRequestState(ctx, tx, work.RequestID, "refund_pending", now); err != nil {
		return err
	}
	if err := releaseSaga(ctx, tx, work, "refund_provider", "refund_pending", nil, now, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *Store) MarkProviderSucceeded(ctx context.Context, work refund.RefundWork, evidence refund.ProviderRefundEvidence, now time.Time) error {
	if evidence.CapturedMinor != work.CapturedMinor || evidence.Fingerprint == (refund.Hash{}) {
		return refund.ErrInvalidProcessorTransition
	}
	tx, err := store.runtimeTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// The intent row is the serialization point for every partial-refund
	// operation on one capture. Provider calls happen before this transaction,
	// so finalization must derive, rather than trust, a work-item snapshot of the
	// cumulative succeeded total.
	var capturedMinor int64
	var currency, intentState string
	if err := tx.QueryRow(ctx, `SELECT amount_minor,currency,state
FROM public.payment_intents
WHERE payment_intent_id=$1 FOR UPDATE`, work.PaymentIntentID).Scan(&capturedMinor, &currency, &intentState); err != nil ||
		capturedMinor != work.CapturedMinor || currency != work.Currency || (intentState != "completed" && intentState != "refunded") {
		return refund.ErrInvalidProcessorTransition
	}
	var succeededMinor int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(sum(operation.amount_minor),0)::bigint
FROM public.ticket_refund_operations AS operation
JOIN public.ticket_refund_requests AS request
  ON request.refund_request_id=operation.refund_request_id
WHERE request.payment_intent_id=$1
  AND operation.refund_operation_id<>$2
  AND operation.state='succeeded'`, work.PaymentIntentID, work.OperationID).Scan(&succeededMinor); err != nil ||
		succeededMinor < 0 || work.AmountMinor <= 0 || succeededMinor > capturedMinor-work.AmountMinor {
		return refund.ErrInvalidProcessorTransition
	}
	cumulativeRefundedMinor := succeededMinor + work.AmountMinor
	var providerRefundID any
	if evidence.ProviderRefundID != "" {
		providerRefundID = evidence.ProviderRefundID
	}
	if err := updateOne(ctx, tx, `UPDATE public.ticket_refund_operations
SET state='succeeded',provider_refund_id=COALESCE($4,provider_refund_id),
    captured_total_minor=$5,refunded_total_minor=$6,response_fingerprint=$7,
    bounded_error_category=NULL,lease_owner=NULL,lease_until=NULL,
    updated_at=$8,completed_at=$8
WHERE refund_operation_id=$1 AND refund_request_id=$2 AND lease_owner=$3
  AND state='processing'`, work.OperationID, work.RequestID, work.LeaseOwner,
		providerRefundID, capturedMinor, cumulativeRefundedMinor, evidence.Fingerprint[:], now.UTC()); err != nil {
		return err
	}
	entry, err := ledger.PrepareAppend(ledger.AppendRequest{
		EventID: "partial_refund:" + work.OperationID.String(), Correlation: "payment:" + work.PaymentIntentID.String(),
		Purpose: ledger.PurposeRefund, Currency: work.Currency,
		Postings: []ledger.Posting{
			{Account: ledger.AccountTicketSales, Side: ledger.Debit, AmountMinor: work.AmountMinor, Currency: work.Currency},
			{Account: ledger.AccountProviderRefundReceivable, Side: ledger.Credit, AmountMinor: work.AmountMinor, Currency: work.Currency},
		},
	}, now.UTC())
	if err != nil {
		return refund.ErrInvalidProcessorTransition
	}
	if _, _, err := ledgerpostgres.AppendInTx(ctx, tx, entry); err != nil {
		return refund.ErrInvalidProcessorTransition
	}
	if err := updateRequestState(ctx, tx, work.RequestID, "refund_succeeded", now); err != nil {
		return err
	}
	if err := releaseSaga(ctx, tx, work, "compensate_shard", "refund_succeeded", nil, now, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *Store) BeginShardAttempt(ctx context.Context, work refund.RefundWork, now time.Time) error {
	tag, err := store.dbExec(ctx, `UPDATE public.ticket_refund_sagas
SET attempts=attempts+1,state='shard_compensating',updated_at=$4
WHERE refund_saga_id=$1 AND refund_request_id=$2 AND lease_owner=$3
  AND current_step='compensate_shard'
  AND state IN('refund_succeeded','shard_compensating')`,
		work.SagaID, work.RequestID, work.LeaseOwner, now.UTC())
	if err != nil || tag != 1 {
		return refund.ErrInvalidProcessorTransition
	}
	return nil
}

func (store *Store) MarkShardRetry(ctx context.Context, work refund.RefundWork, category string, next time.Time) error {
	tx, err := store.runtimeTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := updateRequestState(ctx, tx, work.RequestID, "shard_compensating", next); err != nil {
		return err
	}
	if err := releaseSaga(ctx, tx, work, "compensate_shard", "shard_compensating", &category, next, next); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *Store) MarkShardSucceeded(ctx context.Context, work refund.RefundWork, receipt paymentshard.SelectedTicketRefundReceipt, now time.Time) error {
	if receipt.CommandID == uuid.Nil || receipt.RefundRequestID != work.RequestID || receipt.RefundOperationID != work.OperationID {
		return refund.ErrInvalidProcessorTransition
	}
	tx, err := store.runtimeTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := updateRequestState(ctx, tx, work.RequestID, "shard_compensating", now); err != nil {
		return err
	}
	if err := releaseSaga(ctx, tx, work, "finalize", "shard_compensating", nil, now, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *Store) Finalize(ctx context.Context, work refund.RefundWork, now time.Time) error {
	tx, err := store.runtimeTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var locatorState string
	if err := tx.QueryRow(ctx, `SELECT status FROM public.ticket_order_shard_locators
WHERE ticket_order_id=$1 AND owner_user_id=$2 FOR UPDATE`, work.TicketOrderID, work.OwnerID).Scan(&locatorState); err != nil || locatorState != "confirmed" {
		return refund.ErrSnapshotConflict
	}
	var totalTickets, refundedTickets int
	if err := tx.QueryRow(ctx, `SELECT count(*)::integer
FROM public.ticket_shard_locators WHERE ticket_order_id=$1`, work.TicketOrderID).Scan(&totalTickets); err != nil || totalTickets < 1 {
		return refund.ErrSnapshotConflict
	}
	if err := tx.QueryRow(ctx, `
SELECT count(DISTINCT item.ticket_id)::integer
FROM public.ticket_refund_request_items AS item
JOIN public.ticket_refund_requests AS request
  ON request.refund_request_id=item.refund_request_id
WHERE request.ticket_order_id=$1
  AND (request.state='completed' OR request.refund_request_id=$2)`,
		work.TicketOrderID, work.RequestID).Scan(&refundedTickets); err != nil || refundedTickets < len(work.TicketIDs) || refundedTickets > totalTickets {
		return refund.ErrSnapshotConflict
	}
	if _, err := tx.Exec(ctx, `UPDATE public.ticket_refund_request_items
SET state='refunded',updated_at=$2
WHERE refund_request_id=$1 AND state='refund_pending'`, work.RequestID, now.UTC()); err != nil {
		return err
	}
	if err := updateOne(ctx, tx, `UPDATE public.ticket_refund_requests
SET state='completed',updated_at=$3,completed_at=$3
WHERE refund_request_id=$1 AND state='shard_compensating'
  AND EXISTS(SELECT 1 FROM public.ticket_refund_sagas
             WHERE refund_request_id=$1 AND lease_owner=$2 AND current_step='finalize')`,
		work.RequestID, work.LeaseOwner, now.UTC()); err != nil {
		return err
	}
	if refundedTickets == totalTickets {
		if err := updateOne(ctx, tx, `UPDATE public.ticket_order_shard_locators
SET status='cancelled',updated_at=$2
WHERE ticket_order_id=$1 AND status='confirmed'`, work.TicketOrderID, now.UTC()); err != nil {
			return err
		}
	}
	if err := updateOne(ctx, tx, `UPDATE public.ticket_refund_sagas
SET current_step='complete',state='completed',lease_owner=NULL,lease_until=NULL,
    bounded_error_category=NULL,next_attempt_at=$4,updated_at=$4,completed_at=$4
WHERE refund_saga_id=$1 AND refund_request_id=$2 AND lease_owner=$3
  AND current_step='finalize'`, work.SagaID, work.RequestID, work.LeaseOwner, now.UTC()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *Store) MarkManualReview(ctx context.Context, work refund.RefundWork, reason string, evidence refund.Hash, now time.Time) error {
	if reason == "" || len(reason) > 64 || evidence == (refund.Hash{}) {
		return refund.ErrInvalidProcessorTransition
	}
	tx, err := store.runtimeTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	reviewID := uuid.NewSHA1(work.RequestID, []byte(reason+":"+hex.EncodeToString(evidence[:])))
	if _, err := tx.Exec(ctx, `INSERT INTO public.ticket_refund_manual_reviews(
 review_id,refund_request_id,reason_category,state,evidence_fingerprint,created_at
) VALUES($1,$2,$3,'open',$4,$5)
ON CONFLICT(refund_request_id,reason_category,evidence_fingerprint) DO NOTHING`,
		reviewID, work.RequestID, reason, evidence[:], now.UTC()); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE public.ticket_refund_operations
SET state='manual_review',response_fingerprint=COALESCE(response_fingerprint,$3),
    bounded_error_category=$4,lease_owner=NULL,lease_until=NULL,updated_at=$5,
    completed_at=NULL
WHERE refund_operation_id=$1 AND refund_request_id=$2
  AND state NOT IN('succeeded','failed_permanent')`,
		work.OperationID, work.RequestID, evidence[:], reason, now.UTC()); err != nil {
		return err
	}
	if err := updateOne(ctx, tx, `UPDATE public.ticket_refund_requests
SET state='manual_review',updated_at=$3,completed_at=NULL
WHERE refund_request_id=$1
  AND EXISTS(SELECT 1 FROM public.ticket_refund_sagas
             WHERE refund_request_id=$1 AND lease_owner=$2)`, work.RequestID, work.LeaseOwner, now.UTC()); err != nil {
		return err
	}
	if err := updateOne(ctx, tx, `UPDATE public.ticket_refund_sagas
SET state='manual_review',lease_owner=NULL,lease_until=NULL,
    bounded_error_category=$4,updated_at=$5,completed_at=NULL
WHERE refund_saga_id=$1 AND refund_request_id=$2 AND lease_owner=$3`,
		work.SagaID, work.RequestID, work.LeaseOwner, reason, now.UTC()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type providerTransition struct {
	operationState, requestState, sagaState, step, category string
	next                                                    time.Time
}

func (store *Store) providerTransition(ctx context.Context, work refund.RefundWork, transition providerTransition, now time.Time) error {
	tx, err := store.runtimeTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := updateOne(ctx, tx, `UPDATE public.ticket_refund_operations
SET state=$4,bounded_error_category=$5,lease_owner=NULL,lease_until=NULL,
    next_attempt_at=$6,updated_at=$7
WHERE refund_operation_id=$1 AND refund_request_id=$2 AND lease_owner=$3
  AND state='processing'`, work.OperationID, work.RequestID, work.LeaseOwner,
		transition.operationState, transition.category, transition.next.UTC(), now.UTC()); err != nil {
		return err
	}
	if err := updateRequestState(ctx, tx, work.RequestID, transition.requestState, now); err != nil {
		return err
	}
	category := transition.category
	if err := releaseSaga(ctx, tx, work, transition.step, transition.sagaState, &category, transition.next, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *Store) runtimeTx(ctx context.Context) (pgx.Tx, error) {
	if store == nil || store.db == nil || ctx == nil {
		return nil, refund.ErrInvalidService
	}
	return store.beginControlTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
}

func (store *Store) dbExec(ctx context.Context, query string, arguments ...any) (int64, error) {
	tx, err := store.runtimeTx(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	tag, err := tx.Exec(ctx, query, arguments...)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func updateRequestState(ctx context.Context, tx pgx.Tx, requestID uuid.UUID, state string, now time.Time) error {
	return updateOne(ctx, tx, `UPDATE public.ticket_refund_requests
SET state=$2,updated_at=$3 WHERE refund_request_id=$1
  AND state NOT IN('completed','failed','manual_review')`, requestID, state, now.UTC())
}

func releaseSaga(ctx context.Context, tx pgx.Tx, work refund.RefundWork, step, state string, category *string, next, now time.Time) error {
	return updateOne(ctx, tx, `UPDATE public.ticket_refund_sagas
SET current_step=$4,state=$5,lease_owner=NULL,lease_until=NULL,
    bounded_error_category=$6,next_attempt_at=$7,updated_at=$8
WHERE refund_saga_id=$1 AND refund_request_id=$2 AND lease_owner=$3`,
		work.SagaID, work.RequestID, work.LeaseOwner, step, state, category, next.UTC(), now.UTC())
}

func updateOne(ctx context.Context, tx pgx.Tx, query string, arguments ...any) error {
	tag, err := tx.Exec(ctx, query, arguments...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return refund.ErrInvalidProcessorTransition
	}
	return nil
}

var _ refund.RuntimeStore = (*Store)(nil)

// Ensure pgx no-row errors remain distinguishable when runtime queries are
// later split into narrower adapters.
var _ = errors.Is
