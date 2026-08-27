package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strconv"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var leaseOwnerPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

const awaitingCustomerRecoveryInterval = 5 * time.Minute

var statusRecoveryNamespace = uuid.MustParse("18995477-863a-545a-8ca6-431c4999f537")

func validClaimOptions(options worker.ClaimOptions) bool {
	return leaseOwnerPattern.MatchString(options.WorkerID) && options.BatchSize > 0 &&
		options.BatchSize <= 1000 && options.MaxAttempts > 0 && options.MaxAttempts <= 1000 &&
		options.LeaseTTL > 0 && !options.Now.IsZero()
}

func (store *Store) ClaimOperations(ctx context.Context, options worker.ClaimOptions) ([]worker.OperationClaim, error) {
	if !validClaimOptions(options) {
		return nil, worker.ErrStoreUnavailable
	}
	tx, err := store.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(ctx, tx)

	// A crashed claimed row never crossed the provider boundary; a crashed
	// in-flight row may have, so it becomes uncertain and is query-only.
	if _, err = tx.Exec(ctx, `
UPDATE public.payment_operations
SET state=CASE WHEN state='claimed' THEN 'pending' ELSE 'uncertain' END,
    lease_owner=NULL,lease_until=NULL,next_attempt_at=$1,
    bounded_error_category=CASE WHEN state='in_flight' THEN 'worker_lease_expired' ELSE NULL END
WHERE state IN ('claimed','in_flight') AND lease_until<$1`, options.Now); err != nil {
		return nil, worker.ErrStoreUnavailable
	}
	if _, err = tx.Exec(ctx, `
UPDATE public.payment_operations
SET state='pending',bounded_error_category=NULL
WHERE state='failed_retryable' AND next_attempt_at<=$1 AND attempts<$2`, options.Now, options.MaxAttempts); err != nil {
		return nil, worker.ErrStoreUnavailable
	}
	if err := queueStaleAwaitingCustomerQueries(ctx, tx, options); err != nil {
		return nil, err
	}

	claims := make([]worker.OperationClaim, 0, options.BatchSize)
	rows, err := tx.Query(ctx, `
WITH candidates AS (
 SELECT operation.operation_id
 FROM public.payment_operations AS operation
 JOIN public.payment_intents AS intent ON intent.payment_intent_id=operation.payment_intent_id
 JOIN public.payment_sagas AS saga ON saga.payment_intent_id=operation.payment_intent_id
 WHERE operation.state='pending' AND operation.next_attempt_at<=$1
   AND (operation.lease_until IS NULL OR operation.lease_until<$1)
   AND (
     operation.operation_type='query_status'
     OR (operation.operation_type='create_checkout' AND saga.state='reservation_secured' AND saga.current_step='create_checkout')
     OR (operation.operation_type='authorize' AND saga.state='awaiting_provider' AND saga.current_step='await_provider')
     OR (operation.operation_type='capture' AND saga.state='authorized' AND saga.current_step='capture')
     OR (operation.operation_type='void' AND saga.state='compensating' AND saga.current_step='void')
     OR (operation.operation_type='refund' AND saga.state='refunding' AND saga.current_step='refund')
   )
 ORDER BY operation.next_attempt_at,operation.updated_at,operation.operation_id
 FOR UPDATE OF operation SKIP LOCKED LIMIT $2
), claimed AS (
 UPDATE public.payment_operations AS operation
 SET state='claimed',attempts=attempts+1,lease_owner=$3,
     lease_until=$1+($4::bigint*interval '1 millisecond')
 FROM candidates WHERE operation.operation_id=candidates.operation_id
 RETURNING operation.operation_id,operation.attempts,operation.lease_owner,operation.lease_until
)
SELECT operation.operation_id,operation.payment_intent_id,intent.reservation_id,
       intent.train_run_id,intent.owner_user_id,operation.provider,
       operation.operation_type,'pending',COALESCE(intent.provider_payment_id,''),
       COALESCE(intent.hosted_session_ref,''),operation.provider_idempotency_key_hash,
       operation.amount_minor,operation.currency,claimed.attempts,operation.created_at,
       claimed.lease_owner,claimed.lease_until
FROM claimed
JOIN public.payment_operations AS operation USING(operation_id)
JOIN public.payment_intents AS intent ON intent.payment_intent_id=operation.payment_intent_id
ORDER BY operation.next_attempt_at,operation.updated_at,operation.operation_id`,
		options.Now, options.BatchSize, options.WorkerID, options.LeaseTTL.Milliseconds())
	if err != nil {
		return nil, worker.ErrStoreUnavailable
	}
	claims, err = scanOperationClaims(rows, claims)
	if err != nil {
		return nil, err
	}

	remaining := options.BatchSize - len(claims)
	if remaining > 0 {
		// Uncertain rows are coordinated by the saga lease. The provider call is
		// read-only; compare-and-set finalization makes replica overlap harmless.
		rows, err = tx.Query(ctx, `
WITH uncertain AS (
 SELECT saga.saga_id,operation.operation_id
 FROM public.payment_sagas AS saga
 JOIN LATERAL (
   SELECT candidate.operation_id
   FROM public.payment_operations AS candidate
   WHERE candidate.payment_intent_id=saga.payment_intent_id
     AND candidate.state='uncertain' AND candidate.next_attempt_at<=$1
   ORDER BY candidate.updated_at,candidate.operation_id LIMIT 1
 ) AS operation ON true
 WHERE saga.lease_until IS NULL OR saga.lease_until<$1
 ORDER BY saga.updated_at,saga.saga_id
 FOR UPDATE OF saga SKIP LOCKED
), selected AS (
 SELECT saga_id,operation_id FROM uncertain ORDER BY operation_id LIMIT $2
), leased AS (
 UPDATE public.payment_sagas AS saga
 SET lease_owner=$3,lease_until=$1+($4::bigint*interval '1 millisecond'),attempts=attempts+1
 FROM selected WHERE saga.saga_id=selected.saga_id
 RETURNING saga.saga_id,selected.operation_id,saga.lease_owner,saga.lease_until
)
SELECT operation.operation_id,operation.payment_intent_id,intent.reservation_id,
       intent.train_run_id,intent.owner_user_id,operation.provider,
       operation.operation_type,'uncertain',COALESCE(intent.provider_payment_id,''),
       COALESCE(intent.hosted_session_ref,''),operation.provider_idempotency_key_hash,
       operation.amount_minor,operation.currency,operation.attempts,operation.created_at,
       leased.lease_owner,leased.lease_until
FROM leased
JOIN public.payment_sagas AS saga USING(saga_id)
JOIN public.payment_operations AS operation ON operation.operation_id=leased.operation_id
JOIN public.payment_intents AS intent ON intent.payment_intent_id=operation.payment_intent_id
ORDER BY operation.updated_at,operation.operation_id`,
			options.Now, remaining, options.WorkerID, options.LeaseTTL.Milliseconds())
		if err != nil {
			return nil, worker.ErrStoreUnavailable
		}
		claims, err = scanOperationClaims(rows, claims)
		if err != nil {
			return nil, err
		}
	}
	if err := commit(ctx, tx); err != nil {
		return nil, err
	}
	return claims, nil
}

func queueStaleAwaitingCustomerQueries(ctx context.Context, tx pgx.Tx, options worker.ClaimOptions) error {
	rows, err := tx.Query(ctx, `
SELECT intent.payment_intent_id,intent.provider,intent.amount_minor,intent.currency,intent.updated_at
FROM public.payment_intents AS intent
JOIN public.payment_sagas AS saga ON saga.payment_intent_id=intent.payment_intent_id
WHERE intent.state='awaiting_customer'
  AND intent.provider_payment_id IS NOT NULL
  AND intent.updated_at<=$1::timestamptz-($2::bigint*interval '1 millisecond')
  AND saga.state='awaiting_provider' AND saga.current_step='await_provider'
  AND NOT EXISTS (
    SELECT 1 FROM public.payment_operations AS operation
    WHERE operation.payment_intent_id=intent.payment_intent_id
      AND operation.operation_type='query_status'
      AND operation.state IN ('pending','claimed','in_flight','failed_retryable','uncertain')
  )
ORDER BY intent.updated_at,intent.payment_intent_id
FOR UPDATE OF intent SKIP LOCKED
LIMIT $3`, options.Now, awaitingCustomerRecoveryInterval.Milliseconds(), options.BatchSize)
	if err != nil {
		return worker.ErrStoreUnavailable
	}
	defer rows.Close()
	cycle := options.Now.Truncate(awaitingCustomerRecoveryInterval).Unix()
	for rows.Next() {
		var (
			intentID               uuid.UUID
			providerName, currency string
			amount                 int64
			updatedAt              time.Time
		)
		if err := rows.Scan(&intentID, &providerName, &amount, &currency, &updatedAt); err != nil ||
			intentID == uuid.Nil || providerName == "" || amount <= 0 || len(currency) != 3 || updatedAt.IsZero() {
			return worker.ErrStoreUnavailable
		}
		cycleText := strconv.FormatInt(cycle, 10)
		operationID := uuid.NewSHA1(statusRecoveryNamespace, []byte(intentID.String()+":"+cycleText))
		keyHash := sha256.Sum256([]byte("payment-provider-status-recovery-v1:" + intentID.String() + ":" + cycleText))
		if _, err := tx.Exec(ctx, `
INSERT INTO public.payment_operations (
 operation_id,payment_intent_id,provider,operation_type,
 provider_idempotency_key_hash,amount_minor,currency
) VALUES($1,$2,$3,'query_status',$4,$5,$6)
ON CONFLICT DO NOTHING`, operationID, intentID, providerName, keyHash[:], amount, currency); err != nil {
			return worker.ErrStoreUnavailable
		}
	}
	if rows.Err() != nil {
		return worker.ErrStoreUnavailable
	}
	return nil
}

func scanOperationClaims(rows pgx.Rows, claims []worker.OperationClaim) ([]worker.OperationClaim, error) {
	defer rows.Close()
	for rows.Next() {
		var claim worker.OperationClaim
		var rawType, rawState string
		var keyHash []byte
		if err := rows.Scan(
			&claim.OperationID, &claim.PaymentIntentID, &claim.ReservationID,
			&claim.TrainRunID, &claim.OwnerID, &claim.Provider, &rawType, &rawState,
			&claim.ProviderPaymentID, &claim.HostedSessionReference, &keyHash,
			&claim.AmountMinor, &claim.Currency, &claim.Attempts, &claim.CreatedAt,
			&claim.LeaseOwner, &claim.LeaseUntil,
		); err != nil || len(keyHash) != sha256.Size {
			return nil, worker.ErrStoreUnavailable
		}
		claim.Type = domain.OperationType(rawType)
		claim.PreviousState = domain.OperationState(rawState)
		claim.ProviderIdempotencyKey = hex.EncodeToString(keyHash)
		claims = append(claims, claim)
	}
	if rows.Err() != nil {
		return nil, worker.ErrStoreUnavailable
	}
	return claims, nil
}

func (store *Store) ClaimWebhooks(ctx context.Context, options worker.ClaimOptions) ([]worker.WebhookClaim, error) {
	if !validClaimOptions(options) {
		return nil, worker.ErrStoreUnavailable
	}
	tx, err := store.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(ctx, tx)
	if _, err = tx.Exec(ctx, `
UPDATE public.payment_webhook_inbox
SET state='failed_retryable',lease_owner=NULL,lease_until=NULL,
    bounded_error_category='worker_lease_expired',next_attempt_at=$1
WHERE state='processing' AND lease_until<$1`, options.Now); err != nil {
		return nil, worker.ErrStoreUnavailable
	}
	rows, err := tx.Query(ctx, `
WITH candidates AS (
 SELECT inbox_id FROM public.payment_webhook_inbox
 WHERE state IN ('received','failed_retryable') AND next_attempt_at<=$1
 ORDER BY next_attempt_at,received_at,inbox_id
 FOR UPDATE SKIP LOCKED LIMIT $2
), claimed AS (
 UPDATE public.payment_webhook_inbox AS inbox
 SET state='processing',attempts=attempts+1,lease_owner=$3,
     lease_until=$1+($4::bigint*interval '1 millisecond'),bounded_error_category=NULL
 FROM candidates WHERE inbox.inbox_id=candidates.inbox_id
 RETURNING inbox.*
)
SELECT inbox_id,provider,provider_event_id,event_type,
	   COALESCE(provider_payment_id,''),event_created_at,attempts,lease_owner,lease_until
FROM claimed ORDER BY next_attempt_at,received_at,inbox_id`,
		options.Now, options.BatchSize, options.WorkerID, options.LeaseTTL.Milliseconds())
	if err != nil {
		return nil, worker.ErrStoreUnavailable
	}
	claims := make([]worker.WebhookClaim, 0, options.BatchSize)
	for rows.Next() {
		var claim worker.WebhookClaim
		var eventType string
		if err := rows.Scan(&claim.InboxID, &claim.Provider, &claim.ProviderEventID,
			&eventType, &claim.ProviderPaymentID, &claim.EventCreatedAt, &claim.Attempts,
			&claim.LeaseOwner, &claim.LeaseUntil); err != nil {
			return nil, worker.ErrStoreUnavailable
		}
		claim.EventType = provider.EventType(eventType)
		claims = append(claims, claim)
	}
	if rows.Err() != nil {
		rows.Close()
		return nil, worker.ErrStoreUnavailable
	}
	rows.Close()
	if err := commit(ctx, tx); err != nil {
		return nil, err
	}
	return claims, nil
}

func (store *Store) ClaimActions(ctx context.Context, options worker.ClaimOptions) ([]worker.ActionClaim, error) {
	if !validClaimOptions(options) {
		return nil, worker.ErrStoreUnavailable
	}
	tx, err := store.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(ctx, tx)
	if _, err = tx.Exec(ctx, `
INSERT INTO public.payment_saga_actions (
 action_id,saga_id,payment_intent_id,action_type
)
SELECT gen_random_uuid(),saga.saga_id,saga.payment_intent_id,
 CASE
  WHEN saga.state='issuing_tickets' THEN 'issue_tickets'
  WHEN saga.state='compensating' AND saga.current_step='refund' THEN 'mark_refund_pending'
  WHEN saga.state='compensating' AND saga.current_step='compensate' THEN 'cancel_voided_reservation'
  WHEN saga.state='refunding' THEN 'compensate'
 END
FROM public.payment_sagas AS saga
WHERE (saga.state='issuing_tickets' AND saga.current_step='issue_tickets')
   OR (saga.state='compensating' AND saga.current_step IN ('refund','compensate'))
   OR (saga.state='refunding' AND saga.current_step='compensate')
ON CONFLICT DO NOTHING`); err != nil {
		return nil, worker.ErrStoreUnavailable
	}
	// Expired action leases are safe to reclaim because all shard commands use
	// deterministic command IDs and shard-local durable receipts. Provider and
	// saga attempt counters are deliberately untouched. A crash on the configured
	// final attempt receives exactly one receipt-recovery claim: that claim can
	// replay only the deterministic shard command, never a provider mutation. If
	// the recovery claim itself crashes, fail closed into durable manual review.
	if _, err = tx.Exec(ctx, `
WITH exhausted AS (
 UPDATE public.payment_saga_actions AS action
 SET state='failed_permanent',lease_owner=NULL,lease_until=NULL,
     bounded_error_category='action_recovery_exhausted',completed_at=clock_timestamp()
 WHERE action.state='processing' AND action.lease_until<$1 AND action.attempts>$2
 RETURNING action.saga_id
), moved_sagas AS (
 UPDATE public.payment_sagas AS saga
 SET state='manual_review',lease_owner=NULL,lease_until=NULL,
     bounded_error_category='action_recovery_exhausted',next_attempt_at=clock_timestamp()
 FROM exhausted
 WHERE saga.saga_id=exhausted.saga_id
   AND saga.state NOT IN ('completed','compensated','failed','manual_review')
 RETURNING saga.payment_intent_id
), moved_intents AS (
 UPDATE public.payment_intents AS intent
 SET state='manual_review'
 FROM moved_sagas
 WHERE intent.payment_intent_id=moved_sagas.payment_intent_id
   AND intent.state NOT IN ('completed','voided','refunded','cancelled','failed','expired','manual_review')
 RETURNING intent.payment_intent_id
)
INSERT INTO public.payment_manual_review_cases (
 review_case_id,payment_intent_id,reason_category,review_due_at
)
SELECT gen_random_uuid(),moved_sagas.payment_intent_id,
       'action_recovery_exhausted',clock_timestamp()+interval '24 hours'
FROM moved_sagas
ON CONFLICT(payment_intent_id,reason_category)
WHERE payment_intent_id IS NOT NULL AND state IN ('open','assigned','investigating')
DO NOTHING`, options.Now, options.MaxAttempts); err != nil {
		return nil, worker.ErrStoreUnavailable
	}
	if _, err = tx.Exec(ctx, `
UPDATE public.payment_saga_actions
SET state='failed_retryable',lease_owner=NULL,lease_until=NULL,
    bounded_error_category=CASE WHEN attempts=$2
      THEN 'worker_lease_expired_final_attempt'
      ELSE 'worker_lease_expired' END,
    next_attempt_at=$1
WHERE state='processing' AND lease_until<$1 AND attempts<=$2`, options.Now, options.MaxAttempts); err != nil {
		return nil, worker.ErrStoreUnavailable
	}
	rows, err := tx.Query(ctx, `
WITH candidates AS (
 SELECT action.action_id
 FROM public.payment_saga_actions AS action
 JOIN public.payment_sagas AS saga ON saga.saga_id=action.saga_id
 WHERE action.state IN ('pending','failed_retryable')
   AND action.next_attempt_at<=$1 AND (
     action.attempts<$5 OR (
       action.attempts=$5
       AND action.bounded_error_category='worker_lease_expired_final_attempt'
     )
   )
   AND (action.lease_until IS NULL OR action.lease_until<$1)
   AND (saga.lease_until IS NULL OR saga.lease_until<$1)
   AND (
     (action.action_type='issue_tickets' AND saga.state='issuing_tickets' AND saga.current_step='issue_tickets')
     OR (action.action_type='mark_refund_pending' AND saga.state='compensating' AND saga.current_step='refund')
     OR (action.action_type='cancel_voided_reservation' AND saga.state='compensating' AND saga.current_step='compensate')
     OR (action.action_type='compensate' AND saga.state='refunding' AND saga.current_step='compensate')
   )
 ORDER BY action.next_attempt_at,action.updated_at,action.action_id
 FOR UPDATE OF action,saga SKIP LOCKED LIMIT $2
), claimed AS (
 UPDATE public.payment_saga_actions AS action
 SET state='processing',attempts=action.attempts+1,lease_owner=$3,
     lease_until=$1+($4::bigint*interval '1 millisecond'),bounded_error_category=NULL
 FROM candidates WHERE action.action_id=candidates.action_id
 RETURNING action.*
)
SELECT action.action_id,saga.saga_id,saga.state,saga.current_step,
       action.attempts,action.lease_owner,action.lease_until,
       intent.payment_intent_id,intent.reservation_id,intent.train_run_id,intent.owner_user_id,
       intent.amount_minor,intent.currency,intent.provider,
       capture.operation_id,capture.response_fingerprint,
       void_operation.operation_id,void_operation.response_fingerprint,void_operation.completed_at,
       refund.operation_id,refund.response_fingerprint,refund.completed_at
FROM claimed AS action
JOIN public.payment_sagas AS saga ON saga.saga_id=action.saga_id
JOIN public.payment_intents AS intent ON intent.payment_intent_id=saga.payment_intent_id
LEFT JOIN public.payment_operations AS capture ON capture.payment_intent_id=intent.payment_intent_id
 AND capture.operation_type='capture' AND capture.state='succeeded'
LEFT JOIN public.payment_operations AS void_operation ON void_operation.payment_intent_id=intent.payment_intent_id
 AND void_operation.operation_type='void' AND void_operation.state='succeeded'
LEFT JOIN public.payment_operations AS refund ON refund.payment_intent_id=intent.payment_intent_id
 AND refund.operation_type='refund' AND refund.state='succeeded'
ORDER BY action.next_attempt_at,action.updated_at,action.action_id`,
		options.Now, options.BatchSize, options.WorkerID, options.LeaseTTL.Milliseconds(), options.MaxAttempts)
	if err != nil {
		return nil, worker.ErrStoreUnavailable
	}
	claims := make([]worker.ActionClaim, 0, options.BatchSize)
	for rows.Next() {
		var (
			claim                                        worker.ActionClaim
			state, step                                  string
			intentID, reservationID, trainRunID, ownerID uuid.UUID
			amount                                       int64
			currency                                     string
			captureID, voidID, refundID                  pgtype.UUID
			captureProof, voidProof, refundProof         []byte
			voidedAt, refundedAt                         pgtype.Timestamptz
		)
		if err := rows.Scan(&claim.ActionID, &claim.SagaID, &state, &step, &claim.Attempts,
			&claim.LeaseOwner, &claim.LeaseUntil, &intentID, &reservationID,
			&trainRunID, &ownerID, &amount, &currency, &claim.Provider, &captureID, &captureProof,
			&voidID, &voidProof, &voidedAt,
			&refundID, &refundProof, &refundedAt); err != nil {
			return nil, worker.ErrStoreUnavailable
		}
		claim, err = buildActionClaim(claim, state, step, intentID, reservationID,
			trainRunID, ownerID, amount, currency, captureID, captureProof,
			voidID, voidProof, voidedAt,
			refundID, refundProof, refundedAt)
		if err != nil {
			return nil, err
		}
		claims = append(claims, claim)
	}
	if rows.Err() != nil {
		rows.Close()
		return nil, worker.ErrStoreUnavailable
	}
	rows.Close()
	if err := commit(ctx, tx); err != nil {
		return nil, err
	}
	return claims, nil
}

func buildActionClaim(
	claim worker.ActionClaim, state, step string,
	intentID, reservationID, trainRunID, ownerID uuid.UUID,
	amount int64, currency string, captureID pgtype.UUID, captureProof []byte,
	voidID pgtype.UUID, voidProof []byte, voidedAt pgtype.Timestamptz,
	refundID pgtype.UUID, refundProof []byte, refundedAt pgtype.Timestamptz,
) (worker.ActionClaim, error) {
	switch {
	case state == "issuing_tickets" && step == "issue_tickets" && captureID.Valid && len(captureProof) == sha256.Size:
		claim.Type = worker.ActionIssueTickets
		copy(claim.Issue.CaptureProofHash[:], captureProof)
		claim.Issue.CommandID = deterministicID(claim.SagaID, "issue_tickets_command")
		claim.Issue.IssuanceID = shard.DeterministicIssuanceID(claim.SagaID)
		claim.Issue.PaymentIntentID = intentID
		claim.Issue.PaymentOperationID = captureID.Bytes
		claim.Issue.ReservationID = reservationID
		claim.Issue.TrainRunID = trainRunID
		claim.Issue.OwnerID = ownerID
		claim.Issue.AmountMinor = amount
		claim.Issue.Currency = currency
		claim.Issue.RequestFingerprint = actionFingerprint(claim.SagaID, claim.Type, captureProof)
	case state == "compensating" && step == "refund" && captureID.Valid && len(captureProof) == sha256.Size:
		claim.Type = worker.ActionMarkRefundPending
		copy(claim.MarkRefund.CaptureProofHash[:], captureProof)
		claim.MarkRefund.CommandID = deterministicID(claim.SagaID, "mark_refund_pending_command")
		claim.MarkRefund.PaymentIntentID = intentID
		claim.MarkRefund.ReservationID = reservationID
		claim.MarkRefund.TrainRunID = trainRunID
		claim.MarkRefund.OwnerID = ownerID
		claim.MarkRefund.AmountMinor = amount
		claim.MarkRefund.Currency = currency
		claim.MarkRefund.RequestFingerprint = actionFingerprint(claim.SagaID, claim.Type, captureProof)
	case state == "compensating" && step == "compensate" && voidID.Valid && len(voidProof) == sha256.Size && voidedAt.Valid:
		claim.Type = worker.ActionCancelVoided
		copy(claim.CancelVoided.VoidProofHash[:], voidProof)
		claim.CancelVoided.CommandID = deterministicID(claim.SagaID, "void_cancellation_command")
		claim.CancelVoided.VoidOperationID = voidID.Bytes
		claim.CancelVoided.PaymentIntentID = intentID
		claim.CancelVoided.ReservationID = reservationID
		claim.CancelVoided.TrainRunID = trainRunID
		claim.CancelVoided.OwnerID = ownerID
		claim.CancelVoided.AmountMinor = amount
		claim.CancelVoided.Currency = currency
		claim.CancelVoided.VoidedAt = voidedAt.Time.UTC()
		claim.CancelVoided.RequestFingerprint = shard.VoidCancellationFingerprint(claim.CancelVoided)
	case state == "refunding" && step == "compensate" && refundID.Valid && len(refundProof) == sha256.Size && refundedAt.Valid:
		claim.Type = worker.ActionCompensate
		copy(claim.Compensation.RefundProofHash[:], refundProof)
		claim.Compensation.CommandID = deterministicID(claim.SagaID, "refund_compensation_command")
		claim.Compensation.CompensationID = deterministicID(claim.SagaID, "refund_compensation")
		claim.Compensation.RefundOperationID = refundID.Bytes
		claim.Compensation.PaymentIntentID = intentID
		claim.Compensation.ReservationID = reservationID
		claim.Compensation.TrainRunID = trainRunID
		claim.Compensation.OwnerID = ownerID
		claim.Compensation.AmountMinor = amount
		claim.Compensation.Currency = currency
		claim.Compensation.RefundedAt = refundedAt.Time.UTC()
		claim.Compensation.RequestFingerprint = actionFingerprint(claim.SagaID, claim.Type, refundProof)
	default:
		return worker.ActionClaim{}, worker.ErrStoreUnavailable
	}
	return claim, nil
}

var actionNamespace = uuid.MustParse("8fd46050-e41f-5b3c-876c-77d4f4fa2570")

func deterministicID(sagaID uuid.UUID, purpose string) uuid.UUID {
	return uuid.NewSHA1(actionNamespace, []byte(sagaID.String()+":"+purpose))
}

func actionFingerprint(sagaID uuid.UUID, action worker.ActionType, proof []byte) [sha256.Size]byte {
	value := append([]byte("payment-action-v1:"+sagaID.String()+":"+string(action)+":"), proof...)
	return sha256.Sum256(value)
}
