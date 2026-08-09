package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var providerIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func (store *Store) BeginOperation(ctx context.Context, claim worker.OperationClaim) error {
	if claim.OperationID == uuid.Nil || claim.LeaseOwner == "" || claim.PreviousState == domain.OperationUncertain {
		return worker.ErrLeaseLost
	}
	tx, err := store.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_operations
SET state='in_flight'
WHERE operation_id=$1 AND state='claimed' AND lease_owner=$2
  AND lease_until>=clock_timestamp()`, claim.OperationID, claim.LeaseOwner)); err != nil {
		return err
	}
	switch claim.Type {
	case domain.OperationAuthorize:
		if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_intents SET state='authorization_pending'
WHERE payment_intent_id=$1 AND state='awaiting_customer'`, claim.PaymentIntentID)); err != nil {
			return err
		}
	case domain.OperationCapture:
		if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_intents SET state='capture_pending'
WHERE payment_intent_id=$1 AND state='authorized'`, claim.PaymentIntentID)); err != nil {
			return err
		}
		if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_sagas SET state='capturing',current_step='capture'
WHERE payment_intent_id=$1 AND state='authorized' AND lease_owner IS NULL`, claim.PaymentIntentID)); err != nil {
			return err
		}
	}
	return commit(ctx, tx)
}

func (store *Store) CompleteOperation(ctx context.Context, claim worker.OperationClaim, evidence worker.OperationEvidence) error {
	if claim.OperationID == uuid.Nil || claim.PaymentIntentID == uuid.Nil ||
		evidence.Disposition != worker.DispositionApplied || evidence.ResponseFingerprint == [sha256.Size]byte{} ||
		!validProviderEvidence(claim, evidence) {
		return worker.ErrStoreUnavailable
	}
	tx, err := store.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	providerOperationID := evidence.ProviderOperationID
	if providerOperationID == "" && claim.Type != domain.OperationQueryStatus {
		providerOperationID = confirmedOperationID(evidence.ProviderPaymentID, claim.Type)
	}
	if claim.PreviousState == domain.OperationUncertain {
		if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_operations AS operation
SET state='succeeded',provider_operation_id=NULLIF($3,''),
    normalized_provider_state=$4,response_fingerprint=$5,
    bounded_error_category=NULL,completed_at=clock_timestamp(),next_attempt_at=clock_timestamp()
WHERE operation.operation_id=$1 AND operation.state='uncertain'
  AND EXISTS (
    SELECT 1 FROM public.payment_sagas AS saga
    WHERE saga.payment_intent_id=operation.payment_intent_id
      AND saga.lease_owner=$2 AND saga.lease_until>=clock_timestamp()
  )`, claim.OperationID, claim.LeaseOwner, providerOperationID,
			string(evidence.Status), evidence.ResponseFingerprint[:])); err != nil {
			return err
		}
		if err := releaseSagaLease(ctx, tx, claim.PaymentIntentID, claim.LeaseOwner); err != nil {
			return err
		}
	} else if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_operations
SET state='succeeded',provider_operation_id=NULLIF($3,''),
    normalized_provider_state=$4,response_fingerprint=$5,
    bounded_error_category=NULL,lease_owner=NULL,lease_until=NULL,
    completed_at=clock_timestamp(),next_attempt_at=clock_timestamp()
WHERE operation_id=$1 AND state='in_flight' AND lease_owner=$2
  AND lease_until>=clock_timestamp()`, claim.OperationID, claim.LeaseOwner,
		providerOperationID, string(evidence.Status), evidence.ResponseFingerprint[:])); err != nil {
		return err
	}
	if err := applyOperationSuccess(ctx, tx, claim, evidence); err != nil {
		return err
	}
	return commit(ctx, tx)
}

// SupersedeVoidWithRefund handles the only safe non-void terminal result of an
// uncertain void: current provider evidence proves a full capture and no
// refund. The void conflict, capture ledger, intent/saga pivot, and stable full
// refund operation are committed atomically before provider I/O resumes.
func (store *Store) SupersedeVoidWithRefund(ctx context.Context, claim worker.OperationClaim, evidence worker.OperationEvidence) error {
	if claim.OperationID == uuid.Nil || claim.PaymentIntentID == uuid.Nil || claim.LeaseOwner == "" ||
		claim.Type != domain.OperationVoid || claim.PreviousState != domain.OperationUncertain ||
		evidence.Status != provider.StatusCaptured || !providerIdentifierPattern.MatchString(evidence.ProviderPaymentID) ||
		claim.AmountMinor <= 0 ||
		evidence.AmountMinor != claim.AmountMinor || evidence.Currency != claim.Currency ||
		evidence.CapturedMinor != claim.AmountMinor || evidence.RefundedMinor != 0 ||
		evidence.ResponseFingerprint == [sha256.Size]byte{} {
		return worker.ErrStoreUnavailable
	}
	if evidence.ProviderOperationID != "" && !providerIdentifierPattern.MatchString(evidence.ProviderOperationID) {
		return worker.ErrStoreUnavailable
	}
	tx, err := store.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var intentState, sagaState, sagaStep, sagaOwner string
	if err := tx.QueryRow(ctx, `
SELECT intent.state,saga.state,saga.current_step,COALESCE(saga.lease_owner,'')
FROM public.payment_intents AS intent
JOIN public.payment_sagas AS saga ON saga.payment_intent_id=intent.payment_intent_id
WHERE intent.payment_intent_id=$1
FOR UPDATE OF intent,saga`, claim.PaymentIntentID).Scan(&intentState, &sagaState, &sagaStep, &sagaOwner); err != nil ||
		intentState != "void_pending" || sagaState != "compensating" || sagaStep != "void" || sagaOwner != claim.LeaseOwner {
		return worker.ErrLeaseLost
	}
	if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_operations
SET state='failed_permanent',normalized_provider_state='captured',response_fingerprint=$3,
 bounded_error_category='superseded_by_capture',lease_owner=NULL,lease_until=NULL,
 completed_at=clock_timestamp()
WHERE operation_id=$1 AND payment_intent_id=$2 AND operation_type='void' AND state='uncertain'`,
		claim.OperationID, claim.PaymentIntentID, evidence.ResponseFingerprint[:])); err != nil {
		return err
	}
	if err := convergeCapturedCancellation(ctx, tx, claim.PaymentIntentID, claim.Provider,
		claim.AmountMinor, claim.Currency, claim.LeaseOwner); err != nil {
		return err
	}
	return commit(ctx, tx)
}

func validProviderEvidence(claim worker.OperationClaim, evidence worker.OperationEvidence) bool {
	if evidence.ProviderPaymentID == "" || !providerIdentifierPattern.MatchString(evidence.ProviderPaymentID) ||
		evidence.AmountMinor != claim.AmountMinor || evidence.Currency != claim.Currency {
		return false
	}
	if evidence.ProviderOperationID != "" && !providerIdentifierPattern.MatchString(evidence.ProviderOperationID) {
		return false
	}
	if claim.Type == domain.OperationVoid && (evidence.CapturedMinor != 0 || evidence.RefundedMinor != 0) {
		return false
	}
	return true
}

func confirmedOperationID(paymentID string, kind domain.OperationType) string {
	value := paymentID + ".confirmed." + string(kind)
	if len(value) <= 128 && providerIdentifierPattern.MatchString(value) {
		return value
	}
	digest := sha256.Sum256([]byte(value))
	return "confirmed." + hex.EncodeToString(digest[:16])
}

func applyOperationSuccess(ctx context.Context, tx pgx.Tx, claim worker.OperationClaim, evidence worker.OperationEvidence) error {
	switch claim.Type {
	case domain.OperationCreateCheckout:
		if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_intents
SET provider_payment_id=$2,hosted_session_ref=NULLIF($3,''),state='awaiting_customer'
WHERE payment_intent_id=$1 AND state='checkout_pending'`, claim.PaymentIntentID,
			evidence.ProviderPaymentID, evidence.HostedSessionRef)); err != nil {
			return err
		}
		if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_sagas SET state='checkout_created'
WHERE payment_intent_id=$1 AND state='reservation_secured'`, claim.PaymentIntentID)); err != nil {
			return err
		}
		return oneRow(tx.Exec(ctx, `
UPDATE public.payment_sagas SET state='awaiting_provider',current_step='await_provider'
WHERE payment_intent_id=$1 AND state='checkout_created'`, claim.PaymentIntentID))
	case domain.OperationAuthorize:
		if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_intents SET state='authorized'
WHERE payment_intent_id=$1 AND state='authorization_pending'`, claim.PaymentIntentID)); err != nil {
			return err
		}
		if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_sagas SET state='authorized',current_step='capture'
WHERE payment_intent_id=$1 AND state='awaiting_provider'`, claim.PaymentIntentID)); err != nil {
			return err
		}
		return insertOperation(ctx, tx, claim.PaymentIntentID, claim.Provider, domain.OperationCapture, claim.AmountMinor, claim.Currency)
	case domain.OperationCapture:
		if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_intents SET state='captured'
WHERE payment_intent_id=$1 AND state='capture_pending'`, claim.PaymentIntentID)); err != nil {
			return err
		}
		if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_intents SET state='ticket_issue_pending'
WHERE payment_intent_id=$1 AND state='captured'`, claim.PaymentIntentID)); err != nil {
			return err
		}
		if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_sagas SET state='captured'
WHERE payment_intent_id=$1 AND state='capturing'`, claim.PaymentIntentID)); err != nil {
			return err
		}
		return oneRow(tx.Exec(ctx, `
UPDATE public.payment_sagas SET state='issuing_tickets',current_step='issue_tickets'
WHERE payment_intent_id=$1 AND state='captured'`, claim.PaymentIntentID))
	case domain.OperationVoid:
		if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_intents SET state='voided',completed_at=clock_timestamp()
WHERE payment_intent_id=$1 AND state='void_pending'`, claim.PaymentIntentID)); err != nil {
			return err
		}
		return oneRow(tx.Exec(ctx, `
UPDATE public.payment_sagas
SET current_step='compensate',bounded_error_category=NULL,next_attempt_at=clock_timestamp(),
    lease_owner=NULL,lease_until=NULL
WHERE payment_intent_id=$1 AND state='compensating'`, claim.PaymentIntentID))
	case domain.OperationRefund:
		if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_intents SET state='refunded',completed_at=COALESCE(completed_at,clock_timestamp())
WHERE payment_intent_id=$1 AND state='refund_pending'`, claim.PaymentIntentID)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
UPDATE public.payment_sagas SET current_step='compensate',next_attempt_at=clock_timestamp()
WHERE payment_intent_id=$1 AND state='refunding'`, claim.PaymentIntentID)
		return err
	case domain.OperationQueryStatus:
		return nil
	default:
		return worker.ErrStoreUnavailable
	}
}

func insertOperation(ctx context.Context, tx pgx.Tx, intentID uuid.UUID, providerName string, kind domain.OperationType, amount int64, currency string) error {
	operationID := deterministicID(intentID, "provider_operation:"+string(kind))
	keyHash := sha256.Sum256([]byte("payment-provider-v1:" + intentID.String() + ":" + string(kind)))
	_, err := tx.Exec(ctx, `
INSERT INTO public.payment_operations (
 operation_id,payment_intent_id,provider,operation_type,
 provider_idempotency_key_hash,amount_minor,currency
) VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT DO NOTHING`, operationID, intentID, providerName, string(kind), keyHash[:], amount, currency)
	if err != nil {
		return worker.ErrStoreUnavailable
	}
	return nil
}

func (store *Store) FailOperation(ctx context.Context, claim worker.OperationClaim, failure worker.Failure) error {
	if claim.OperationID == uuid.Nil || claim.PaymentIntentID == uuid.Nil || claim.LeaseOwner == "" || failure.Category == "" {
		return worker.ErrStoreUnavailable
	}
	tx, err := store.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	currentState := "in_flight"
	if claim.PreviousState == domain.OperationUncertain {
		currentState = "uncertain"
	}
	if failure.ManualReview {
		if err := finalizeOperationFailure(ctx, tx, claim, currentState, "failed_permanent", failure); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
UPDATE public.payment_intents SET state='manual_review'
WHERE payment_intent_id=$1 AND state NOT IN ('completed','voided','refunded','cancelled','failed','expired','manual_review')`, claim.PaymentIntentID); err != nil {
			return worker.ErrStoreUnavailable
		}
		if _, err := tx.Exec(ctx, `
UPDATE public.payment_sagas SET state='manual_review',lease_owner=NULL,lease_until=NULL,
 bounded_error_category=$2,next_attempt_at=clock_timestamp()
WHERE payment_intent_id=$1 AND state NOT IN ('completed','compensated','failed','manual_review')`, claim.PaymentIntentID, failure.Category); err != nil {
			return worker.ErrStoreUnavailable
		}
		if err := insertReview(ctx, tx, claim.PaymentIntentID, claim.OperationID, uuid.Nil, failure.Category); err != nil {
			return err
		}
	} else {
		nextState := "failed_retryable"
		if failure.Uncertain || currentState == "uncertain" && failure.Category == "provider_outcome_unknown" {
			nextState = "uncertain"
		}
		if err := finalizeOperationFailure(ctx, tx, claim, currentState, nextState, failure); err != nil {
			return err
		}
		if claim.PreviousState == domain.OperationUncertain {
			if err := releaseSagaLease(ctx, tx, claim.PaymentIntentID, claim.LeaseOwner); err != nil {
				return err
			}
		}
	}
	return commit(ctx, tx)
}

func finalizeOperationFailure(ctx context.Context, tx pgx.Tx, claim worker.OperationClaim, currentState, nextState string, failure worker.Failure) error {
	query := `
UPDATE public.payment_operations AS operation
SET state=$4,bounded_error_category=$5,next_attempt_at=$6,
    lease_owner=NULL,lease_until=NULL,
    completed_at=CASE WHEN $4='failed_permanent' THEN clock_timestamp() ELSE NULL END
WHERE operation.operation_id=$1 AND operation.state=$3 AND (
 (operation.lease_owner=$2 AND operation.lease_until>=clock_timestamp())
 OR ($3='uncertain' AND EXISTS (
   SELECT 1 FROM public.payment_sagas AS saga
   WHERE saga.payment_intent_id=operation.payment_intent_id
     AND saga.lease_owner=$2 AND saga.lease_until>=clock_timestamp()
 ))
)`
	return oneRow(tx.Exec(ctx, query, claim.OperationID, claim.LeaseOwner, currentState,
		nextState, failure.Category, failure.NextAttemptAt))
}

func releaseSagaLease(ctx context.Context, tx pgx.Tx, intentID uuid.UUID, owner string) error {
	return oneRow(tx.Exec(ctx, `
UPDATE public.payment_sagas SET lease_owner=NULL,lease_until=NULL
WHERE payment_intent_id=$1 AND lease_owner=$2`, intentID, owner))
}

func insertReview(ctx context.Context, tx pgx.Tx, intentID, operationID, inboxID uuid.UUID, reason string) error {
	reviewID := deterministicID(intentID, "manual_review:"+reason)
	var operationValue, inboxValue any
	if operationID != uuid.Nil {
		operationValue = operationID
	}
	if inboxID != uuid.Nil {
		inboxValue = inboxID
	}
	_, err := tx.Exec(ctx, `
INSERT INTO public.payment_manual_review_cases (
 review_case_id,payment_intent_id,operation_id,inbox_id,reason_category,review_due_at
) VALUES($1,$2,$3,$4,$5,clock_timestamp()+interval '24 hours')
ON CONFLICT DO NOTHING`, reviewID, intentID, operationValue, inboxValue, reason)
	if err != nil {
		return worker.ErrStoreUnavailable
	}
	return nil
}

func (store *Store) IgnoreWebhook(ctx context.Context, claim worker.WebhookClaim) error {
	tx, err := store.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_webhook_inbox
SET state='ignored',lease_owner=NULL,lease_until=NULL,processed_at=clock_timestamp()
WHERE inbox_id=$1 AND state='processing' AND lease_owner=$2
 AND lease_until>=clock_timestamp()`, claim.InboxID, claim.LeaseOwner)); err != nil {
		return err
	}
	return commit(ctx, tx)
}

func (store *Store) CompleteWebhook(ctx context.Context, claim worker.WebhookClaim, evidence worker.WebhookEvidence) error {
	if claim.InboxID == uuid.Nil || claim.ProviderPaymentID == "" || claim.LeaseOwner == "" ||
		evidence.AmountMinor < 0 || len(evidence.Currency) != 3 {
		return worker.ErrStoreUnavailable
	}
	tx, err := store.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var intentID, sagaID uuid.UUID
	var amount int64
	var currency, providerName, intentState string
	err = tx.QueryRow(ctx, `
SELECT intent.payment_intent_id,saga.saga_id,intent.amount_minor,intent.currency,intent.provider,intent.state
FROM public.payment_intents AS intent
JOIN public.payment_sagas AS saga ON saga.payment_intent_id=intent.payment_intent_id
WHERE intent.provider=$1 AND intent.provider_payment_id=$2
FOR UPDATE OF intent,saga`, claim.Provider, claim.ProviderPaymentID).Scan(
		&intentID, &sagaID, &amount, &currency, &providerName, &intentState)
	_ = sagaID
	if errors.Is(err, pgx.ErrNoRows) {
		return worker.ErrStoreUnavailable
	}
	if err != nil || amount != evidence.AmountMinor || currency != evidence.Currency ||
		evidence.RefundedMinor < 0 || evidence.CapturedMinor < evidence.RefundedMinor || evidence.CapturedMinor > amount {
		return worker.ErrStoreUnavailable
	}
	if evidence.Status == provider.StatusRefunded && intentState != "refund_pending" && intentState != "refunded" && intentState != "cancelled" {
		return worker.ErrStoreUnavailable
	}
	if evidence.Status == provider.StatusVoided && intentState != "void_pending" && intentState != "voided" && intentState != "cancelled" {
		return worker.ErrStoreUnavailable
	}
	if err := applyProviderConfirmation(ctx, tx, intentID, providerName, intentState, amount, currency, evidence); err != nil {
		return err
	}
	if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_webhook_inbox
SET state='processed',lease_owner=NULL,lease_until=NULL,
    bounded_error_category=NULL,processed_at=clock_timestamp()
WHERE inbox_id=$1 AND state='processing' AND lease_owner=$2
 AND lease_until>=clock_timestamp()`, claim.InboxID, claim.LeaseOwner)); err != nil {
		return err
	}
	return commit(ctx, tx)
}

func applyProviderConfirmation(ctx context.Context, tx pgx.Tx, intentID uuid.UUID, providerName, intentState string, amount int64, currency string, evidence worker.WebhookEvidence) error {
	switch evidence.Status {
	case provider.StatusCreated, provider.StatusRequiresCustomerAction:
		return nil
	case provider.StatusAuthorized:
		if err := advanceAuthorization(ctx, tx, intentID); err != nil {
			return err
		}
		return insertOperation(ctx, tx, intentID, providerName, domain.OperationCapture, amount, currency)
	case provider.StatusCaptured:
		if evidence.CapturedMinor != amount || evidence.RefundedMinor != 0 {
			return worker.ErrStoreUnavailable
		}
		if intentState == "void_pending" {
			if amount <= 0 {
				return worker.ErrStoreUnavailable
			}
			fingerprint := sha256.Sum256([]byte("provider-confirmation-v1:" + intentID.String() + ":capture:captured"))
			if err := oneRow(tx.Exec(ctx, `
UPDATE public.payment_operations
SET state=CASE WHEN state IN('pending','claimed','failed_retryable') THEN 'cancelled' ELSE 'failed_permanent' END,
 normalized_provider_state='captured',response_fingerprint=$2,
 bounded_error_category='superseded_by_capture',lease_owner=NULL,lease_until=NULL,
 completed_at=clock_timestamp()
WHERE payment_intent_id=$1 AND operation_type='void'
 AND state IN('pending','claimed','in_flight','failed_retryable','uncertain')`, intentID, fingerprint[:])); err != nil {
				return err
			}
			return convergeCapturedCancellation(ctx, tx, intentID, providerName, amount, currency, "")
		}
		if err := advanceAuthorization(ctx, tx, intentID); err != nil {
			return err
		}
		if err := confirmOperation(ctx, tx, intentID, providerName, domain.OperationCapture, amount, currency, evidence.Status); err != nil {
			return err
		}
		return advanceCapture(ctx, tx, intentID)
	case provider.StatusRefunded:
		if evidence.CapturedMinor != amount || evidence.RefundedMinor != amount {
			return worker.ErrStoreUnavailable
		}
		if err := confirmOperation(ctx, tx, intentID, providerName, domain.OperationRefund, amount, currency, evidence.Status); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
UPDATE public.payment_intents SET state='refunded',completed_at=COALESCE(completed_at,clock_timestamp())
WHERE payment_intent_id=$1 AND state='refund_pending'`, intentID)
		if err != nil {
			return worker.ErrStoreUnavailable
		}
		_, err = tx.Exec(ctx, `UPDATE public.payment_sagas SET state='refunding',current_step='compensate',next_attempt_at=clock_timestamp()
WHERE payment_intent_id=$1 AND state='compensating'`, intentID)
		return err
	case provider.StatusVoided:
		if evidence.CapturedMinor != 0 || evidence.RefundedMinor != 0 {
			return worker.ErrStoreUnavailable
		}
		if err := confirmOperation(ctx, tx, intentID, providerName, domain.OperationVoid, amount, currency, evidence.Status); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE public.payment_intents
SET state='voided',completed_at=COALESCE(completed_at,clock_timestamp())
WHERE payment_intent_id=$1 AND state='void_pending'`, intentID); err != nil {
			return worker.ErrStoreUnavailable
		}
		_, err := tx.Exec(ctx, `UPDATE public.payment_sagas
SET current_step='compensate',bounded_error_category=NULL,next_attempt_at=clock_timestamp(),
 lease_owner=NULL,lease_until=NULL
WHERE payment_intent_id=$1 AND state='compensating'`, intentID)
		if err != nil {
			return worker.ErrStoreUnavailable
		}
		return nil
	case provider.StatusUnknown, provider.StatusFailed, provider.StatusCancelled:
		return worker.ErrStoreUnavailable
	default:
		return worker.ErrStoreUnavailable
	}
}

func convergeCapturedCancellation(ctx context.Context, tx pgx.Tx, intentID uuid.UUID, providerName string, amount int64, currency, leaseOwner string) error {
	if err := confirmOperation(ctx, tx, intentID, providerName, domain.OperationCapture, amount, currency, provider.StatusCaptured); err != nil {
		return err
	}
	// The v10 transition guard intentionally routes this exceptional edge
	// through manual_review, but both updates are inside this transaction, so no
	// observer or worker can claim the transient state.
	if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_intents SET state='manual_review'
WHERE payment_intent_id=$1 AND state='void_pending'`, intentID)); err != nil {
		return err
	}
	if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_intents SET state='refund_pending'
WHERE payment_intent_id=$1 AND state='manual_review'`, intentID)); err != nil {
		return err
	}
	if leaseOwner == "" {
		if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_sagas
SET state='refunding',current_step='refund',lease_owner=NULL,lease_until=NULL,
 bounded_error_category=NULL,next_attempt_at=clock_timestamp()
WHERE payment_intent_id=$1 AND state='compensating' AND current_step='void'`, intentID)); err != nil {
			return err
		}
	} else if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_sagas
SET state='refunding',current_step='refund',lease_owner=NULL,lease_until=NULL,
 bounded_error_category=NULL,next_attempt_at=clock_timestamp()
WHERE payment_intent_id=$1 AND state='compensating' AND lease_owner=$2
 AND lease_until>=clock_timestamp()`, intentID, leaseOwner)); err != nil {
		return err
	}
	return insertOperation(ctx, tx, intentID, providerName, domain.OperationRefund, amount, currency)
}

func advanceAuthorization(ctx context.Context, tx pgx.Tx, intentID uuid.UUID) error {
	// Each statement follows one database-enforced edge. Zero affected rows are
	// accepted for stale events that cannot regress a later local state.
	statements := []string{
		`UPDATE public.payment_intents SET state='authorization_pending' WHERE payment_intent_id=$1 AND state='awaiting_customer'`,
		`UPDATE public.payment_intents SET state='authorized' WHERE payment_intent_id=$1 AND state='authorization_pending'`,
		`UPDATE public.payment_sagas SET state='authorized',current_step='capture' WHERE payment_intent_id=$1 AND state='awaiting_provider'`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement, intentID); err != nil {
			return worker.ErrStoreUnavailable
		}
	}
	return nil
}

func advanceCapture(ctx context.Context, tx pgx.Tx, intentID uuid.UUID) error {
	statements := []string{
		`UPDATE public.payment_intents SET state='capture_pending' WHERE payment_intent_id=$1 AND state='authorized'`,
		`UPDATE public.payment_intents SET state='captured' WHERE payment_intent_id=$1 AND state='capture_pending'`,
		`UPDATE public.payment_intents SET state='ticket_issue_pending' WHERE payment_intent_id=$1 AND state='captured'`,
		`UPDATE public.payment_sagas SET state='capturing',current_step='capture' WHERE payment_intent_id=$1 AND state='authorized'`,
		`UPDATE public.payment_sagas SET state='captured' WHERE payment_intent_id=$1 AND state='capturing'`,
		`UPDATE public.payment_sagas SET state='issuing_tickets',current_step='issue_tickets',next_attempt_at=clock_timestamp() WHERE payment_intent_id=$1 AND state='captured'`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement, intentID); err != nil {
			return worker.ErrStoreUnavailable
		}
	}
	return nil
}

func confirmOperation(ctx context.Context, tx pgx.Tx, intentID uuid.UUID, providerName string, kind domain.OperationType, amount int64, currency string, status provider.Status) error {
	if err := insertOperation(ctx, tx, intentID, providerName, kind, amount, currency); err != nil {
		return err
	}
	var operationID uuid.UUID
	var state string
	if err := tx.QueryRow(ctx, `SELECT operation_id,state FROM public.payment_operations
WHERE payment_intent_id=$1 AND operation_type=$2 FOR UPDATE`, intentID, string(kind)).Scan(&operationID, &state); err != nil {
		return worker.ErrStoreUnavailable
	}
	leaseOwner := "webhook-confirmation"
	switch state {
	case "succeeded":
		return nil
	case "failed_retryable":
		if _, err := tx.Exec(ctx, `UPDATE public.payment_operations SET state='pending',bounded_error_category=NULL
WHERE operation_id=$1 AND state='failed_retryable'`, operationID); err != nil {
			return worker.ErrStoreUnavailable
		}
		state = "pending"
	case "failed_permanent", "cancelled":
		return worker.ErrStoreUnavailable
	}
	if state == "pending" {
		if _, err := tx.Exec(ctx, `UPDATE public.payment_operations
SET state='claimed',attempts=attempts+1,lease_owner=$2,lease_until=clock_timestamp()+interval '30 seconds'
WHERE operation_id=$1 AND state='pending'`, operationID, leaseOwner); err != nil {
			return worker.ErrStoreUnavailable
		}
		state = "claimed"
	}
	if state == "claimed" {
		if _, err := tx.Exec(ctx, `UPDATE public.payment_operations SET state='in_flight'
WHERE operation_id=$1 AND state='claimed'`, operationID); err != nil {
			return worker.ErrStoreUnavailable
		}
	}
	fingerprint := sha256.Sum256([]byte("provider-confirmation-v1:" + intentID.String() + ":" + string(kind) + ":" + string(status)))
	providerOperationID := confirmedOperationID("payment."+intentID.String(), kind)
	_, err := tx.Exec(ctx, `UPDATE public.payment_operations
SET state='succeeded',provider_operation_id=$2,normalized_provider_state=$3,
 response_fingerprint=$4,bounded_error_category=NULL,lease_owner=NULL,lease_until=NULL,
 completed_at=clock_timestamp()
WHERE operation_id=$1 AND state IN ('in_flight','uncertain')`, operationID,
		providerOperationID, string(status), fingerprint[:])
	if err != nil {
		return worker.ErrStoreUnavailable
	}
	return nil
}

func (store *Store) FailWebhook(ctx context.Context, claim worker.WebhookClaim, failure worker.Failure) error {
	if claim.InboxID == uuid.Nil || claim.LeaseOwner == "" || failure.Category == "" {
		return worker.ErrStoreUnavailable
	}
	tx, err := store.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	nextState := "failed_retryable"
	if failure.ManualReview {
		nextState = "failed_permanent"
	}
	if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_webhook_inbox
SET state=$3,lease_owner=NULL,lease_until=NULL,bounded_error_category=$4,next_attempt_at=$5,
 processed_at=CASE WHEN $3='failed_permanent' THEN clock_timestamp() ELSE NULL END
WHERE inbox_id=$1 AND state='processing' AND lease_owner=$2
 AND lease_until>=clock_timestamp()`, claim.InboxID, claim.LeaseOwner,
		nextState, failure.Category, failure.NextAttemptAt)); err != nil {
		return err
	}
	if failure.ManualReview {
		var intentID uuid.UUID
		err := tx.QueryRow(ctx, `SELECT payment_intent_id FROM public.payment_intents
WHERE provider=$1 AND provider_payment_id=$2`, claim.Provider, claim.ProviderPaymentID).Scan(&intentID)
		if err == nil {
			if err := insertReview(ctx, tx, intentID, uuid.Nil, claim.InboxID, failure.Category); err != nil {
				return err
			}
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return worker.ErrStoreUnavailable
		}
	}
	return commit(ctx, tx)
}

func (store *Store) CompleteAction(ctx context.Context, claim worker.ActionClaim, evidence worker.ActionEvidence) error {
	if claim.SagaID == uuid.Nil || claim.LeaseOwner == "" {
		return worker.ErrStoreUnavailable
	}
	tx, err := store.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	switch claim.Type {
	case worker.ActionIssueTickets:
		if err := writeTicketLocators(ctx, tx, claim, evidence.Issue); err != nil {
			return err
		}
		if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_intents AS intent
SET state='completed',completed_at=clock_timestamp()
FROM public.payment_sagas AS saga
WHERE saga.saga_id=$1 AND saga.payment_intent_id=intent.payment_intent_id
 AND saga.lease_owner=$2 AND saga.lease_until>=clock_timestamp()
 AND intent.state='ticket_issue_pending'`, claim.SagaID, claim.LeaseOwner)); err != nil {
			return err
		}
		if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_sagas
SET state='completed',current_step='complete',completed_at=clock_timestamp(),
 lease_owner=NULL,lease_until=NULL,bounded_error_category=NULL
WHERE saga_id=$1 AND state='issuing_tickets' AND lease_owner=$2
 AND lease_until>=clock_timestamp()`, claim.SagaID, claim.LeaseOwner)); err != nil {
			return err
		}
	case worker.ActionMarkRefundPending:
		if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_sagas
SET state='refunding',current_step='refund',lease_owner=NULL,lease_until=NULL,
 bounded_error_category=NULL,next_attempt_at=clock_timestamp()
WHERE saga_id=$1 AND state='compensating' AND current_step='refund'
 AND lease_owner=$2 AND lease_until>=clock_timestamp()`, claim.SagaID, claim.LeaseOwner)); err != nil {
			return err
		}
	case worker.ActionCancelVoided:
		if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_intents AS intent
SET state='cancelled'
FROM public.payment_sagas AS saga
WHERE saga.saga_id=$1 AND saga.payment_intent_id=intent.payment_intent_id
 AND saga.lease_owner=$2 AND saga.lease_until>=clock_timestamp()
 AND intent.state='voided'`, claim.SagaID, claim.LeaseOwner)); err != nil {
			return err
		}
		if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_sagas
SET state='compensated',current_step='complete',completed_at=clock_timestamp(),
 lease_owner=NULL,lease_until=NULL,bounded_error_category=NULL
WHERE saga_id=$1 AND state='compensating' AND current_step='compensate'
 AND lease_owner=$2 AND lease_until>=clock_timestamp()`, claim.SagaID, claim.LeaseOwner)); err != nil {
			return err
		}
	case worker.ActionCompensate:
		tag, err := tx.Exec(ctx, `UPDATE public.ticket_order_shard_locators
SET status='cancelled'
WHERE ticket_order_id=$1 AND reservation_id=$2 AND owner_user_id=$3`,
			evidence.Compensation.TicketOrderID, claim.Compensation.ReservationID, claim.Compensation.OwnerID)
		expectedLocators := int64(0)
		if evidence.Compensation.CancelledTicketCount > 0 {
			expectedLocators = 1
		}
		if err != nil || tag.RowsAffected() != expectedLocators {
			return worker.ErrStoreUnavailable
		}
		if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_intents AS intent
SET state='cancelled'
FROM public.payment_sagas AS saga
WHERE saga.saga_id=$1 AND saga.payment_intent_id=intent.payment_intent_id
 AND saga.lease_owner=$2 AND saga.lease_until>=clock_timestamp()
 AND intent.state='refunded'`, claim.SagaID, claim.LeaseOwner)); err != nil {
			return err
		}
		if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_sagas
SET state='compensated',current_step='complete',completed_at=clock_timestamp(),
 lease_owner=NULL,lease_until=NULL,bounded_error_category=NULL
WHERE saga_id=$1 AND state='refunding' AND current_step='compensate'
 AND lease_owner=$2 AND lease_until>=clock_timestamp()`, claim.SagaID, claim.LeaseOwner)); err != nil {
			return err
		}
	default:
		return worker.ErrStoreUnavailable
	}
	return commit(ctx, tx)
}

func writeTicketLocators(ctx context.Context, tx pgx.Tx, claim worker.ActionClaim, receipt shard.IssueTicketsReceipt) error {
	if receipt.TicketOrderID == uuid.Nil || len(receipt.TicketIDs) == 0 ||
		len(receipt.TicketCodes) != len(receipt.TicketIDs) ||
		receipt.OrderCreatedAt.IsZero() || receipt.IssuedAt.IsZero() {
		return worker.ErrStoreUnavailable
	}
	var trainRunID, ownerID uuid.UUID
	var shardID string
	var generation int64
	if err := tx.QueryRow(ctx, `SELECT train_run_id,shard_id,assignment_generation,owner_user_id
FROM public.reservation_shard_locators WHERE reservation_id=$1 FOR UPDATE`, claim.Issue.ReservationID).Scan(
		&trainRunID, &shardID, &generation, &ownerID,
	); err != nil || trainRunID != claim.Issue.TrainRunID || ownerID != claim.Issue.OwnerID ||
		shardID == "" || generation < 1 {
		return worker.ErrStoreUnavailable
	}
	if err := oneRow(tx.Exec(ctx, `INSERT INTO public.ticket_order_shard_locators(
 ticket_order_id,reservation_id,train_run_id,shard_id,assignment_generation,
 owner_user_id,status,total_amount_minor,currency,created_at
) VALUES($1,$2,$3,$4,$5,$6,'confirmed',$7,$8,$9)
ON CONFLICT(ticket_order_id) DO UPDATE SET status=EXCLUDED.status
WHERE ticket_order_shard_locators.reservation_id=EXCLUDED.reservation_id
 AND ticket_order_shard_locators.train_run_id=EXCLUDED.train_run_id
 AND ticket_order_shard_locators.shard_id=EXCLUDED.shard_id
 AND ticket_order_shard_locators.assignment_generation=EXCLUDED.assignment_generation
 AND ticket_order_shard_locators.owner_user_id=EXCLUDED.owner_user_id
 AND ticket_order_shard_locators.total_amount_minor=EXCLUDED.total_amount_minor
 AND ticket_order_shard_locators.currency=EXCLUDED.currency
 AND ticket_order_shard_locators.created_at=EXCLUDED.created_at`,
		receipt.TicketOrderID, claim.Issue.ReservationID, trainRunID, shardID, generation,
		ownerID, receipt.AmountMinor, receipt.Currency, receipt.OrderCreatedAt.UTC())); err != nil {
		return err
	}
	seenIDs := make(map[uuid.UUID]struct{}, len(receipt.TicketIDs))
	seenCodes := make(map[string]struct{}, len(receipt.TicketCodes))
	for index, ticketID := range receipt.TicketIDs {
		ticketCode := receipt.TicketCodes[index]
		if ticketID == uuid.Nil || !shard.ValidTicketCode(ticketCode) {
			return worker.ErrStoreUnavailable
		}
		if _, duplicate := seenIDs[ticketID]; duplicate {
			return worker.ErrStoreUnavailable
		}
		if _, duplicate := seenCodes[ticketCode]; duplicate {
			return worker.ErrStoreUnavailable
		}
		seenIDs[ticketID] = struct{}{}
		seenCodes[ticketCode] = struct{}{}
		if err := oneRow(tx.Exec(ctx, `INSERT INTO public.ticket_shard_locators(
 ticket_id,ticket_order_id,reservation_id,train_run_id,shard_id,assignment_generation,owner_user_id
) VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT(ticket_id) DO UPDATE SET ticket_order_id=EXCLUDED.ticket_order_id
WHERE ticket_shard_locators.ticket_order_id=EXCLUDED.ticket_order_id
 AND ticket_shard_locators.reservation_id=EXCLUDED.reservation_id
 AND ticket_shard_locators.train_run_id=EXCLUDED.train_run_id
 AND ticket_shard_locators.shard_id=EXCLUDED.shard_id
 AND ticket_shard_locators.assignment_generation=EXCLUDED.assignment_generation
 AND ticket_shard_locators.owner_user_id=EXCLUDED.owner_user_id`,
			ticketID, receipt.TicketOrderID, claim.Issue.ReservationID, trainRunID, shardID, generation, ownerID)); err != nil {
			return err
		}
		if err := oneRow(tx.Exec(ctx, `INSERT INTO public.ticket_code_directory(ticket_code,ticket_id)
VALUES($1,$2)
ON CONFLICT(ticket_code) DO UPDATE SET ticket_id=EXCLUDED.ticket_id
WHERE ticket_code_directory.ticket_id=EXCLUDED.ticket_id`, ticketCode, ticketID)); err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) FailAction(ctx context.Context, claim worker.ActionClaim, failure worker.Failure) error {
	if claim.SagaID == uuid.Nil || claim.LeaseOwner == "" || failure.Category == "" {
		return worker.ErrStoreUnavailable
	}
	tx, err := store.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	if failure.Compensate {
		if claim.Type != worker.ActionIssueTickets {
			return worker.ErrStoreUnavailable
		}
		if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_intents AS intent
SET state='refund_pending'
FROM public.payment_sagas AS saga
WHERE saga.saga_id=$1 AND saga.payment_intent_id=intent.payment_intent_id
 AND saga.lease_owner=$2 AND saga.lease_until>=clock_timestamp()
 AND intent.state='ticket_issue_pending'`, claim.SagaID, claim.LeaseOwner)); err != nil {
			return err
		}
		if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_sagas
SET state='compensating',current_step='refund',lease_owner=NULL,lease_until=NULL,
 bounded_error_category=$3,next_attempt_at=clock_timestamp()
WHERE saga_id=$1 AND state='issuing_tickets' AND lease_owner=$2
 AND lease_until>=clock_timestamp()`, claim.SagaID, claim.LeaseOwner, failure.Category)); err != nil {
			return err
		}
		intentID := claim.Issue.PaymentIntentID
		if intentID == uuid.Nil {
			return worker.ErrStoreUnavailable
		}
		if err := insertOperation(ctx, tx, intentID, claim.Provider, domain.OperationRefund,
			claim.Issue.AmountMinor, claim.Issue.Currency); err != nil {
			return err
		}
	} else if failure.ManualReview {
		if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_sagas
SET state='manual_review',lease_owner=NULL,lease_until=NULL,bounded_error_category=$3
WHERE saga_id=$1 AND lease_owner=$2 AND lease_until>=clock_timestamp()
 AND state NOT IN ('completed','compensated','failed')`, claim.SagaID, claim.LeaseOwner, failure.Category)); err != nil {
			return err
		}
		var intentID uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT payment_intent_id FROM public.payment_sagas WHERE saga_id=$1`, claim.SagaID).Scan(&intentID); err != nil {
			return worker.ErrStoreUnavailable
		}
		if _, err := tx.Exec(ctx, `UPDATE public.payment_intents SET state='manual_review'
WHERE payment_intent_id=$1 AND state NOT IN ('completed','voided','refunded','cancelled','failed','expired','manual_review')`, intentID); err != nil {
			return worker.ErrStoreUnavailable
		}
		if err := insertReview(ctx, tx, intentID, uuid.Nil, uuid.Nil, failure.Category); err != nil {
			return err
		}
	} else if err := oneRow(tx.Exec(ctx, `UPDATE public.payment_sagas
SET lease_owner=NULL,lease_until=NULL,bounded_error_category=$3,next_attempt_at=$4
WHERE saga_id=$1 AND lease_owner=$2 AND lease_until>=clock_timestamp()`,
		claim.SagaID, claim.LeaseOwner, failure.Category, failure.NextAttemptAt)); err != nil {
		return err
	}
	return commit(ctx, tx)
}
