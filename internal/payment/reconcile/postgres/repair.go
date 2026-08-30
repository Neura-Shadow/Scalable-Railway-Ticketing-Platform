package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	paymentreconcile "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/reconcile"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var repairActionNamespace = uuid.MustParse("8fd46050-e41f-5b3c-876c-77d4f4fa2570")
var controlPaymentIdentityNamespace = uuid.MustParse("d4afbc57-7bb0-5f88-bff4-62db186e9ea8")

type CommandGateway interface {
	IssueTickets(context.Context, paymentshard.IssueTicketsCommand) (paymentshard.IssueTicketsReceipt, error)
	MarkRefundPending(context.Context, paymentshard.MarkRefundPendingCommand) (paymentshard.MarkRefundPendingReceipt, error)
	CancelVoidedReservation(context.Context, paymentshard.CancelVoidedReservationCommand) (paymentshard.CancelVoidedReservationReceipt, error)
	ApplyRefundCompensation(context.Context, paymentshard.ApplyRefundCompensationCommand) (paymentshard.ApplyRefundCompensationReceipt, error)
}

type BeginReceiptVerifier interface {
	VerifyBeginPaymentReceipt(context.Context, uuid.UUID, uuid.UUID, [sha256.Size]byte) error
}

type Repairer struct {
	control controlDB
	gateway CommandGateway
	begin   BeginReceiptVerifier
}

func NewRepairer(control controlDB, gateway CommandGateway, begin BeginReceiptVerifier) (*Repairer, error) {
	if control == nil || gateway == nil || begin == nil {
		return nil, paymentreconcile.ErrInvalidConfiguration
	}
	return &Repairer{control: control, gateway: gateway, begin: begin}, nil
}

type repairEvidence struct {
	SagaID, IntentID, ReservationID, TrainRunID, OwnerID uuid.UUID
	OperationID                                          uuid.UUID
	IntentState, SagaState, Step, Currency               string
	BoundedErrorCategory                                 string
	AmountMinor                                          int64
	Proof                                                [sha256.Size]byte
	CompletedAt                                          time.Time
	ActiveLease                                          bool
}

func (s *Store) loadRecordedCommands(ctx context.Context, intentID uuid.UUID) ([]paymentreconcile.RecordedCommand, error) {
	result := make([]paymentreconcile.RecordedCommand, 0, 4)
	for _, kind := range []string{"issue_tickets", "mark_refund_pending", "cancel_voided_reservation", "apply_refund_compensation"} {
		evidence, err := loadRepairEvidence(ctx, s.control, intentID, kind)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		command, err := recordedCommand(evidence, kind)
		if err != nil {
			return nil, err
		}
		result = append(result, command)
	}
	return result, nil
}

func loadRepairEvidence(ctx context.Context, control controlDB, intentID uuid.UUID, kind string) (repairEvidence, error) {
	operationType := map[string]string{
		"issue_tickets": "capture", "mark_refund_pending": "capture", "cancel_voided_reservation": "void", "apply_refund_compensation": "refund",
	}[kind]
	if ctx == nil || control == nil || intentID == uuid.Nil || operationType == "" {
		return repairEvidence{}, paymentreconcile.ErrInvalidRequest
	}
	var value repairEvidence
	var proof []byte
	var completed pgtype.Timestamptz
	err := control.QueryRow(ctx, `
SELECT saga.saga_id,intent.payment_intent_id,intent.reservation_id,intent.train_run_id,
       intent.owner_user_id,operation.operation_id,intent.state,saga.state,
       saga.current_step,intent.currency,intent.amount_minor,
       COALESCE(saga.bounded_error_category,''),
       operation.response_fingerprint,operation.completed_at,
       (saga.lease_owner IS NOT NULL AND saga.lease_until>=clock_timestamp())
       OR EXISTS (
          SELECT 1 FROM public.payment_saga_actions AS active_action
          WHERE active_action.saga_id=saga.saga_id
            AND active_action.state='processing'
            AND active_action.lease_until>=clock_timestamp()
       )
FROM public.payment_intents AS intent
JOIN public.payment_sagas AS saga ON saga.payment_intent_id=intent.payment_intent_id
JOIN public.payment_operations AS operation ON operation.payment_intent_id=intent.payment_intent_id
WHERE intent.payment_intent_id=$1 AND operation.operation_type=$2
  AND operation.state='succeeded' AND operation.response_fingerprint IS NOT NULL`, intentID, operationType).Scan(
		&value.SagaID, &value.IntentID, &value.ReservationID, &value.TrainRunID,
		&value.OwnerID, &value.OperationID, &value.IntentState, &value.SagaState,
		&value.Step, &value.Currency, &value.AmountMinor, &value.BoundedErrorCategory, &proof, &completed, &value.ActiveLease,
	)
	if err != nil {
		return repairEvidence{}, err
	}
	if len(proof) != sha256.Size || !completed.Valid || value.SagaID == uuid.Nil || value.OperationID == uuid.Nil || value.AmountMinor <= 0 {
		return repairEvidence{}, paymentreconcile.ErrRepairUnavailable
	}
	copy(value.Proof[:], proof)
	value.CompletedAt = completed.Time.UTC()
	return value, nil
}

func recordedCommand(evidence repairEvidence, kind string) (paymentreconcile.RecordedCommand, error) {
	command := paymentreconcile.RecordedCommand{Kind: kind}
	switch kind {
	case "issue_tickets":
		command.ID = deterministicRepairID(evidence.SagaID, "issue_tickets_command")
		command.Fingerprint = repairFingerprint(evidence.SagaID, "issue_tickets", evidence.Proof)
	case "mark_refund_pending":
		command.ID = deterministicRepairID(evidence.SagaID, "mark_refund_pending_command")
		command.Fingerprint = repairFingerprint(evidence.SagaID, "mark_refund_pending", evidence.Proof)
	case "apply_refund_compensation":
		command.ID = deterministicRepairID(evidence.SagaID, "refund_compensation_command")
		command.Fingerprint = repairFingerprint(evidence.SagaID, "compensate", evidence.Proof)
	case "cancel_voided_reservation":
		value := voidCommand(evidence)
		command.ID, command.Fingerprint = value.CommandID, value.RequestFingerprint
	default:
		return paymentreconcile.RecordedCommand{}, paymentreconcile.ErrInvalidRequest
	}
	return command, nil
}

func (r *Repairer) ReplayRecordedCommand(ctx context.Context, intentID uuid.UUID, recorded paymentreconcile.RecordedCommand) error {
	if r == nil || r.control == nil || r.gateway == nil || r.begin == nil || ctx == nil || intentID == uuid.Nil {
		return paymentreconcile.ErrRepairUnavailable
	}
	if recorded.Kind == "finalize_reservation_begin" {
		return r.finalizeReservationBegin(ctx, intentID, recorded)
	}
	evidence, err := loadRepairEvidence(ctx, r.control, intentID, recorded.Kind)
	if err != nil {
		return fmt.Errorf("load recorded repair evidence: %w", err)
	}
	expected, err := recordedCommand(evidence, recorded.Kind)
	if err != nil || expected != recorded || evidence.ActiveLease {
		return paymentreconcile.ErrRepairUnavailable
	}
	leaseOwner, err := r.claimRepairSaga(ctx, evidence, recorded.Kind)
	if err != nil {
		return err
	}
	releaseLease := true
	defer func() {
		if releaseLease {
			r.releaseRepairLease(context.WithoutCancel(ctx), evidence.SagaID, leaseOwner)
		}
	}()
	switch recorded.Kind {
	case "issue_tickets":
		command := issueCommand(evidence)
		receipt, callErr := r.gateway.IssueTickets(ctx, command)
		if callErr != nil {
			return callErr
		}
		if err := r.finalizeIssue(ctx, evidence, command, receipt, leaseOwner); err != nil {
			return err
		}
	case "mark_refund_pending":
		command := markRefundPendingCommand(evidence)
		receipt, callErr := r.gateway.MarkRefundPending(ctx, command)
		if callErr != nil {
			return callErr
		}
		if err := r.finalizeMarkRefundPending(ctx, evidence, command, receipt, leaseOwner); err != nil {
			return err
		}
	case "cancel_voided_reservation":
		command := voidCommand(evidence)
		receipt, callErr := r.gateway.CancelVoidedReservation(ctx, command)
		if callErr != nil {
			return callErr
		}
		if err := r.finalizeVoid(ctx, evidence, command, receipt, leaseOwner); err != nil {
			return err
		}
	case "apply_refund_compensation":
		command := compensationCommand(evidence)
		receipt, callErr := r.gateway.ApplyRefundCompensation(ctx, command)
		if callErr != nil {
			return callErr
		}
		if err := r.finalizeCompensation(ctx, evidence, command, receipt, leaseOwner); err != nil {
			return err
		}
	default:
		return paymentreconcile.ErrRepairUnavailable
	}
	releaseLease = false
	return nil
}

func beginPaymentCommandID(sagaID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(controlPaymentIdentityNamespace, []byte(sagaID.String()+":secure_reservation"))
}

func checkoutOperationIdentity(intentID uuid.UUID) (uuid.UUID, [sha256.Size]byte) {
	identity := []byte(intentID.String() + ":create_checkout")
	return uuid.NewSHA1(controlPaymentIdentityNamespace, identity), sha256.Sum256(append([]byte("provider:v1:"), identity...))
}

func (r *Repairer) finalizeReservationBegin(ctx context.Context, intentID uuid.UUID, recorded paymentreconcile.RecordedCommand) error {
	if recorded.ID == uuid.Nil || recorded.Fingerprint == ([sha256.Size]byte{}) || recorded.Kind != "finalize_reservation_begin" {
		return paymentreconcile.ErrRepairUnavailable
	}
	if err := r.begin.VerifyBeginPaymentReceipt(ctx, intentID, recorded.ID, recorded.Fingerprint); err != nil {
		return paymentreconcile.ErrRepairUnavailable
	}
	tx, err := r.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var sagaID, reservationID, trainRunID, ownerID uuid.UUID
	var intentState, sagaState, step, providerName, currency, directoryState string
	var amountMinor int64
	var fingerprint []byte
	var activeLease bool
	if err := tx.QueryRow(ctx, `SELECT saga.saga_id,intent.reservation_id,intent.train_run_id,intent.owner_user_id,
 intent.state,saga.state,saga.current_step,intent.provider,intent.currency,intent.amount_minor,
 intent.request_fingerprint,directory.state,
 saga.lease_owner IS NOT NULL AND saga.lease_until>=clock_timestamp()
FROM public.payment_intents AS intent
JOIN public.payment_sagas AS saga ON saga.payment_intent_id=intent.payment_intent_id
JOIN public.reservation_directory AS directory
  ON directory.reservation_id=intent.reservation_id AND directory.train_run_id=intent.train_run_id
 AND directory.owner_user_id=intent.owner_user_id
WHERE intent.payment_intent_id=$1 FOR UPDATE OF intent,saga,directory`, intentID).Scan(
		&sagaID, &reservationID, &trainRunID, &ownerID, &intentState, &sagaState, &step,
		&providerName, &currency, &amountMinor, &fingerprint, &directoryState, &activeLease); err != nil {
		return paymentreconcile.ErrRepairUnavailable
	}
	if intentState != "reservation_securing" || sagaState != "created" || step != "secure_reservation" || activeLease ||
		(directoryState != "active" && directoryState != "moving") || len(fingerprint) != sha256.Size ||
		recorded.ID != beginPaymentCommandID(sagaID) {
		return paymentreconcile.ErrRepairUnavailable
	}
	var storedFingerprint [sha256.Size]byte
	copy(storedFingerprint[:], fingerprint)
	if storedFingerprint != recorded.Fingerprint || reservationID == uuid.Nil || trainRunID == uuid.Nil || ownerID == uuid.Nil ||
		providerName == "" || amountMinor < 0 || len(currency) != 3 {
		return paymentreconcile.ErrRepairUnavailable
	}
	if tag, err := tx.Exec(ctx, `UPDATE public.payment_intents SET state='checkout_pending'
WHERE payment_intent_id=$1 AND state='reservation_securing'`, intentID); err != nil || tag.RowsAffected() != 1 {
		return paymentreconcile.ErrRepairUnavailable
	}
	if tag, err := tx.Exec(ctx, `UPDATE public.payment_sagas
SET state='reservation_secured',current_step='create_checkout',next_attempt_at=clock_timestamp(),lease_owner=NULL,lease_until=NULL,bounded_error_category=NULL
WHERE saga_id=$1 AND state='created' AND current_step='secure_reservation'
  AND (lease_until IS NULL OR lease_until<clock_timestamp())`, sagaID); err != nil || tag.RowsAffected() != 1 {
		return paymentreconcile.ErrRepairUnavailable
	}
	operationID, operationHash := checkoutOperationIdentity(intentID)
	if _, err := tx.Exec(ctx, `INSERT INTO public.payment_operations(
 operation_id,payment_intent_id,provider,operation_type,provider_idempotency_key_hash,amount_minor,currency
) VALUES($1,$2,$3,'create_checkout',$4,$5,$6) ON CONFLICT DO NOTHING`,
		operationID, intentID, providerName, operationHash[:], amountMinor, currency); err != nil {
		return err
	}
	var storedOperationID uuid.UUID
	var storedHash []byte
	var storedAmount int64
	var storedCurrency, storedProvider string
	if err := tx.QueryRow(ctx, `SELECT operation_id,provider_idempotency_key_hash,amount_minor,currency,provider
FROM public.payment_operations WHERE payment_intent_id=$1 AND operation_type='create_checkout'`, intentID).Scan(
		&storedOperationID, &storedHash, &storedAmount, &storedCurrency, &storedProvider); err != nil ||
		storedOperationID != operationID || len(storedHash) != sha256.Size || storedAmount != amountMinor ||
		storedCurrency != currency || storedProvider != providerName {
		return paymentreconcile.ErrRepairUnavailable
	}
	var storedOperationHash [sha256.Size]byte
	copy(storedOperationHash[:], storedHash)
	if storedOperationHash != operationHash {
		return paymentreconcile.ErrRepairUnavailable
	}
	if err := insertRepairAudit(ctx, tx, intentID, paymentreconcile.ScopeIntents, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repairer) claimRepairSaga(ctx context.Context, evidence repairEvidence, kind string) (string, error) {
	expectedIntent, expectedSaga, expectedStep := "", "", ""
	switch kind {
	case "issue_tickets":
		expectedIntent, expectedSaga, expectedStep = "ticket_issue_pending", "issuing_tickets", "issue_tickets"
	case "mark_refund_pending":
		expectedIntent, expectedSaga, expectedStep = "refund_pending", "compensating", "refund"
	case "cancel_voided_reservation":
		expectedIntent, expectedSaga, expectedStep = "voided", "compensating", "compensate"
	case "apply_refund_compensation":
		expectedIntent, expectedSaga, expectedStep = "refunded", "refunding", "compensate"
	default:
		return "", paymentreconcile.ErrRepairUnavailable
	}
	regular := evidence.IntentState == expectedIntent && evidence.SagaState == expectedSaga && evidence.Step == expectedStep
	manualFinalize := evidence.SagaState == "manual_review" && evidence.Step == expectedStep &&
		evidence.BoundedErrorCategory == "database_finalize_failed" &&
		(evidence.IntentState == expectedIntent || (kind == "issue_tickets" || kind == "mark_refund_pending") && evidence.IntentState == "manual_review")
	if !regular && !manualFinalize {
		return "", paymentreconcile.ErrRepairUnavailable
	}
	leaseOwner := "payment-admin-repair:" + uuid.NewString()
	tx, err := r.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := lockSagaActionBoundary(ctx, tx, evidence.SagaID, evidence.IntentID); err != nil {
		return "", err
	}
	if manualFinalize {
		if kind == "issue_tickets" || kind == "mark_refund_pending" {
			if tag, execErr := tx.Exec(ctx, `UPDATE public.payment_intents SET state=$2
WHERE payment_intent_id=$1 AND state='manual_review'`, evidence.IntentID, expectedIntent); execErr != nil || tag.RowsAffected() != 1 {
				return "", paymentreconcile.ErrRepairUnavailable
			}
		}
		if tag, execErr := tx.Exec(ctx, `UPDATE public.payment_sagas
SET state=$2,bounded_error_category=NULL
WHERE saga_id=$1 AND state='manual_review' AND current_step=$3
  AND bounded_error_category='database_finalize_failed'
  AND (lease_until IS NULL OR lease_until<clock_timestamp())`, evidence.SagaID, expectedSaga, expectedStep); execErr != nil || tag.RowsAffected() != 1 {
			return "", paymentreconcile.ErrRepairUnavailable
		}
	}
	tag, err := tx.Exec(ctx, `UPDATE public.payment_sagas AS saga
SET lease_owner=$3,lease_until=clock_timestamp()+interval '2 minutes'
FROM public.payment_intents AS intent
WHERE saga.saga_id=$1 AND intent.payment_intent_id=$2
  AND intent.payment_intent_id=saga.payment_intent_id
  AND intent.state=$4 AND saga.state=$5 AND saga.current_step=$6
  AND (saga.lease_until IS NULL OR saga.lease_until<clock_timestamp())
  AND NOT EXISTS (
    SELECT 1 FROM public.payment_saga_actions AS active_action
    WHERE active_action.saga_id=saga.saga_id
      AND active_action.state='processing'
      AND active_action.lease_until>=clock_timestamp()
  )`,
		evidence.SagaID, evidence.IntentID, leaseOwner, expectedIntent, expectedSaga, expectedStep)
	if err != nil || tag.RowsAffected() != 1 {
		return "", paymentreconcile.ErrRepairUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return leaseOwner, nil
}

func (r *Repairer) releaseRepairLease(ctx context.Context, sagaID uuid.UUID, leaseOwner string) {
	tx, err := r.control.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `UPDATE public.payment_sagas SET lease_owner=NULL,lease_until=NULL
WHERE saga_id=$1 AND lease_owner=$2`, sagaID, leaseOwner); err != nil {
		return
	}
	_ = tx.Commit(ctx)
}

// ResumeTicketIssuance advances only a capture-proven saga into the existing
// deterministic issuance command, then replays that command through the shard
// gateway. It never invents a command or accepts financial fields from CLI.
func (r *Repairer) ResumeTicketIssuance(ctx context.Context, intentID uuid.UUID) error {
	evidence, err := loadRepairEvidence(ctx, r.control, intentID, "issue_tickets")
	if err != nil {
		return err
	}
	if evidence.ActiveLease {
		return paymentreconcile.ErrRepairUnavailable
	}
	if evidence.IntentState != "ticket_issue_pending" || evidence.SagaState != "issuing_tickets" || evidence.Step != "issue_tickets" {
		tx, err := r.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		if evidence.IntentState == "manual_review" {
			if evidence.Step != "issue_tickets" || !issuanceRetrySafeCategory(evidence.BoundedErrorCategory) {
				return paymentreconcile.ErrRepairUnavailable
			}
			if tag, execErr := tx.Exec(ctx, `UPDATE public.payment_intents SET state='captured' WHERE payment_intent_id=$1 AND state='manual_review'`, intentID); execErr != nil || tag.RowsAffected() != 1 {
				return paymentreconcile.ErrRepairUnavailable
			}
			evidence.IntentState = "captured"
		}
		if evidence.SagaState == "manual_review" {
			if evidence.Step != "issue_tickets" || !issuanceRetrySafeCategory(evidence.BoundedErrorCategory) {
				return paymentreconcile.ErrRepairUnavailable
			}
			if tag, execErr := tx.Exec(ctx, `UPDATE public.payment_sagas SET state='captured',current_step='issue_tickets',bounded_error_category=NULL,lease_owner=NULL,lease_until=NULL
WHERE saga_id=$1 AND state='manual_review' AND (lease_until IS NULL OR lease_until<clock_timestamp())`, evidence.SagaID); execErr != nil || tag.RowsAffected() != 1 {
				return paymentreconcile.ErrRepairUnavailable
			}
			evidence.SagaState = "captured"
		}
		if evidence.IntentState != "captured" || evidence.SagaState != "captured" {
			return paymentreconcile.ErrRepairUnavailable
		}
		if tag, execErr := tx.Exec(ctx, `UPDATE public.payment_intents SET state='ticket_issue_pending' WHERE payment_intent_id=$1 AND state='captured'`, intentID); execErr != nil || tag.RowsAffected() != 1 {
			return paymentreconcile.ErrRepairUnavailable
		}
		if tag, execErr := tx.Exec(ctx, `UPDATE public.payment_sagas SET state='issuing_tickets',current_step='issue_tickets',next_attempt_at=clock_timestamp(),lease_owner=NULL,lease_until=NULL
WHERE saga_id=$1 AND state='captured' AND (lease_until IS NULL OR lease_until<clock_timestamp())`, evidence.SagaID); execErr != nil || tag.RowsAffected() != 1 {
			return paymentreconcile.ErrRepairUnavailable
		}
		if err := appendRepairCaptureLedger(ctx, tx, evidence); err != nil {
			return err
		}
		if err := insertRepairAudit(ctx, tx, intentID, paymentreconcile.ScopeTickets, true); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	recorded, err := recordedCommand(evidence, "issue_tickets")
	if err != nil {
		return err
	}
	return r.ReplayRecordedCommand(ctx, intentID, recorded)
}

func issuanceRetrySafeCategory(category string) bool {
	return category == "shard_command_failed" || category == "database_finalize_failed"
}

// RetryProviderOperation schedules only an already-persisted provider
// operation. The worker retains the stable idempotency identity and query-only
// handling for uncertain state; this method performs no provider call.
func (r *Repairer) RetryProviderOperation(ctx context.Context, intentID, operationID uuid.UUID, operationType string) error {
	if r == nil || ctx == nil || intentID == uuid.Nil || operationID == uuid.Nil ||
		(operationType != "create_checkout" && operationType != "authorize" && operationType != "capture" && operationType != "void" && operationType != "refund") {
		return paymentreconcile.ErrInvalidRequest
	}
	tx, err := r.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var operationState, sagaState, sagaStep string
	var sagaID uuid.UUID
	var leased bool
	if err := tx.QueryRow(ctx, `
SELECT operation.state,saga.state,saga.current_step,saga.saga_id,
       (operation.lease_owner IS NOT NULL AND operation.lease_until>=clock_timestamp())
       OR (saga.lease_owner IS NOT NULL AND saga.lease_until>=clock_timestamp())
FROM public.payment_operations AS operation
JOIN public.payment_intents AS intent ON intent.payment_intent_id=operation.payment_intent_id
JOIN public.payment_sagas AS saga ON saga.payment_intent_id=intent.payment_intent_id
WHERE operation.operation_id=$1 AND operation.payment_intent_id=$2
  AND operation.operation_type=$3 AND operation.amount_minor=intent.amount_minor
  AND operation.currency=intent.currency
FOR UPDATE OF operation,saga`, operationID, intentID, operationType).Scan(&operationState, &sagaState, &sagaStep, &sagaID, &leased); err != nil {
		return paymentreconcile.ErrRepairUnavailable
	}
	if leased || (operationState != "pending" && operationState != "failed_retryable" && operationState != "uncertain") ||
		!providerOperationClaimable(operationType, sagaState, sagaStep) {
		return paymentreconcile.ErrRepairUnavailable
	}
	if tag, err := tx.Exec(ctx, `UPDATE public.payment_operations SET next_attempt_at=clock_timestamp(),lease_owner=NULL,lease_until=NULL
WHERE operation_id=$1 AND state=$2 AND (lease_until IS NULL OR lease_until<clock_timestamp())`, operationID, operationState); err != nil || tag.RowsAffected() != 1 {
		return paymentreconcile.ErrRepairUnavailable
	}
	if tag, err := tx.Exec(ctx, `UPDATE public.payment_sagas SET next_attempt_at=clock_timestamp(),lease_owner=NULL,lease_until=NULL
WHERE saga_id=$1 AND state=$2 AND (lease_until IS NULL OR lease_until<clock_timestamp())`, sagaID, sagaState); err != nil || tag.RowsAffected() != 1 {
		return paymentreconcile.ErrRepairUnavailable
	}
	if err := insertRepairAudit(ctx, tx, intentID, paymentreconcile.ScopeOperations, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func providerOperationClaimable(operationType, sagaState, sagaStep string) bool {
	switch operationType {
	case "create_checkout":
		return sagaState == "reservation_secured" && sagaStep == "create_checkout"
	case "authorize":
		return sagaState == "awaiting_provider" && sagaStep == "await_provider"
	case "capture":
		return sagaState == "authorized" && sagaStep == "capture"
	case "void":
		return sagaState == "compensating" && sagaStep == "void"
	case "refund":
		return sagaState == "refunding" && sagaStep == "refund"
	default:
		return false
	}
}

func (r *Repairer) RetrySaga(ctx context.Context, sagaID uuid.UUID) error {
	if r == nil || ctx == nil || sagaID == uuid.Nil {
		return paymentreconcile.ErrInvalidRequest
	}
	var intentID uuid.UUID
	var state, step string
	if err := r.control.QueryRow(ctx, `SELECT payment_intent_id,state,current_step FROM public.payment_sagas WHERE saga_id=$1`, sagaID).Scan(&intentID, &state, &step); err != nil {
		return paymentreconcile.ErrRepairUnavailable
	}
	if state == "issuing_tickets" || state == "captured" || state == "manual_review" && step == "issue_tickets" {
		return r.ResumeTicketIssuance(ctx, intentID)
	}
	if step == "compensate" {
		for _, kind := range []string{"apply_refund_compensation", "cancel_voided_reservation"} {
			evidence, err := loadRepairEvidence(ctx, r.control, intentID, kind)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
			recorded, err := recordedCommand(evidence, kind)
			if err != nil {
				return err
			}
			return r.ReplayRecordedCommand(ctx, intentID, recorded)
		}
	}
	operationType := map[string]string{
		"create_checkout": "create_checkout", "authorize": "authorize", "capture": "capture", "void": "void", "refund": "refund", "reconcile_provider": "capture",
	}[step]
	if operationType == "" {
		return paymentreconcile.ErrRepairUnavailable
	}
	var operationID uuid.UUID
	if err := r.control.QueryRow(ctx, `SELECT operation_id FROM public.payment_operations
WHERE payment_intent_id=$1 AND operation_type=$2 AND state IN ('pending','failed_retryable','uncertain')
ORDER BY created_at,operation_id LIMIT 1`, intentID, operationType).Scan(&operationID); err != nil {
		return paymentreconcile.ErrRepairUnavailable
	}
	return r.RetryProviderOperation(ctx, intentID, operationID, operationType)
}

func (r *Repairer) MarkManualReview(ctx context.Context, intentID uuid.UUID) error {
	if r == nil || ctx == nil || intentID == uuid.Nil {
		return paymentreconcile.ErrInvalidRequest
	}
	tx, err := r.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := lockSagaActionBoundary(ctx, tx, uuid.Nil, intentID); err != nil {
		return err
	}
	if tag, err := tx.Exec(ctx, `UPDATE public.payment_intents SET state='manual_review'
WHERE payment_intent_id=$1 AND state IN ('reservation_securing','checkout_pending','awaiting_customer','authorization_pending','authorized','capture_pending','captured','ticket_issue_pending','void_pending','refund_pending')`, intentID); err != nil || tag.RowsAffected() != 1 {
		return paymentreconcile.ErrRepairUnavailable
	}
	if tag, err := tx.Exec(ctx, `UPDATE public.payment_sagas SET state='manual_review',lease_owner=NULL,lease_until=NULL,bounded_error_category='operator_requested',next_attempt_at=clock_timestamp()
WHERE payment_intent_id=$1 AND state NOT IN ('completed','compensated','failed','manual_review')
  AND (lease_until IS NULL OR lease_until<clock_timestamp())
  AND NOT EXISTS (
    SELECT 1 FROM public.payment_saga_actions AS active_action
    WHERE active_action.saga_id=payment_sagas.saga_id
      AND active_action.state='processing'
      AND active_action.lease_until>=clock_timestamp()
  )`, intentID); err != nil || tag.RowsAffected() != 1 {
		return paymentreconcile.ErrRepairUnavailable
	}
	if _, err := tx.Exec(ctx, `INSERT INTO public.payment_manual_review_cases(review_case_id,payment_intent_id,reason_category,review_due_at)
VALUES($1,$2,'operator_requested',clock_timestamp())
ON CONFLICT(payment_intent_id,reason_category) WHERE payment_intent_id IS NOT NULL AND state IN ('open','assigned','investigating') DO NOTHING`, uuid.New(), intentID); err != nil {
		return err
	}
	if err := insertRepairAudit(ctx, tx, intentID, paymentreconcile.ScopeAll, false); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// lockSagaActionBoundary gives repair/admin mutations the same row-lock
// boundary as ClaimActions. Once the saga row is locked, a processing action
// cannot appear until this transaction commits; an action already performing
// external I/O makes the repair fail closed.
func lockSagaActionBoundary(ctx context.Context, tx pgx.Tx, sagaID, intentID uuid.UUID) error {
	var activeAction bool
	err := tx.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM public.payment_saga_actions AS active_action
  WHERE active_action.saga_id=saga.saga_id
    AND active_action.state='processing'
    AND active_action.lease_until>=clock_timestamp()
)
FROM public.payment_sagas AS saga
WHERE ($1::uuid='00000000-0000-0000-0000-000000000000' OR saga.saga_id=$1)
  AND ($2::uuid='00000000-0000-0000-0000-000000000000' OR saga.payment_intent_id=$2)
FOR UPDATE OF saga`, sagaID, intentID).Scan(&activeAction)
	if err != nil || activeAction {
		return paymentreconcile.ErrRepairUnavailable
	}
	return nil
}

func insertRepairAudit(ctx context.Context, tx pgx.Tx, intentID uuid.UUID, scope paymentreconcile.Scope, repaired bool) error {
	mode := "detect_only"
	repairCount := 0
	if repaired {
		mode, repairCount = "safe_repair", 1
	}
	_, err := tx.Exec(ctx, `INSERT INTO public.payment_reconciliation_checkpoints(
 reconciliation_id,scope,payment_intent_id,mode,state,rows_examined,mismatch_count,repair_count,attempts,started_at,completed_at
) VALUES($1,$2,$3,$4,'mismatch',1,1,$5,1,clock_timestamp(),clock_timestamp())`, uuid.New(), string(scope), intentID, mode, repairCount)
	return err
}

func issueCommand(e repairEvidence) paymentshard.IssueTicketsCommand {
	return paymentshard.IssueTicketsCommand{
		CommandID: deterministicRepairID(e.SagaID, "issue_tickets_command"), IssuanceID: paymentshard.DeterministicIssuanceID(e.SagaID),
		PaymentIntentID: e.IntentID, PaymentOperationID: e.OperationID, ReservationID: e.ReservationID,
		TrainRunID: e.TrainRunID, OwnerID: e.OwnerID, AmountMinor: e.AmountMinor, Currency: e.Currency,
		CaptureProofHash: e.Proof, RequestFingerprint: repairFingerprint(e.SagaID, "issue_tickets", e.Proof),
	}
}

func markRefundPendingCommand(e repairEvidence) paymentshard.MarkRefundPendingCommand {
	return paymentshard.MarkRefundPendingCommand{
		CommandID:       deterministicRepairID(e.SagaID, "mark_refund_pending_command"),
		PaymentIntentID: e.IntentID, ReservationID: e.ReservationID, TrainRunID: e.TrainRunID,
		OwnerID: e.OwnerID, AmountMinor: e.AmountMinor, Currency: e.Currency,
		CaptureProofHash: e.Proof, RequestFingerprint: repairFingerprint(e.SagaID, "mark_refund_pending", e.Proof),
	}
}

func voidCommand(e repairEvidence) paymentshard.CancelVoidedReservationCommand {
	value := paymentshard.CancelVoidedReservationCommand{
		CommandID: deterministicRepairID(e.SagaID, "void_cancellation_command"), VoidOperationID: e.OperationID,
		PaymentIntentID: e.IntentID, ReservationID: e.ReservationID, TrainRunID: e.TrainRunID, OwnerID: e.OwnerID,
		AmountMinor: e.AmountMinor, Currency: e.Currency, VoidProofHash: e.Proof, VoidedAt: e.CompletedAt,
	}
	value.RequestFingerprint = paymentshard.VoidCancellationFingerprint(value)
	return value
}

func compensationCommand(e repairEvidence) paymentshard.ApplyRefundCompensationCommand {
	return paymentshard.ApplyRefundCompensationCommand{
		CommandID: deterministicRepairID(e.SagaID, "refund_compensation_command"), CompensationID: deterministicRepairID(e.SagaID, "refund_compensation"),
		RefundOperationID: e.OperationID, PaymentIntentID: e.IntentID, ReservationID: e.ReservationID,
		TrainRunID: e.TrainRunID, OwnerID: e.OwnerID, AmountMinor: e.AmountMinor, Currency: e.Currency,
		RefundProofHash: e.Proof, RefundedAt: e.CompletedAt, RequestFingerprint: repairFingerprint(e.SagaID, "compensate", e.Proof),
	}
}

func deterministicRepairID(sagaID uuid.UUID, purpose string) uuid.UUID {
	return uuid.NewSHA1(repairActionNamespace, []byte(sagaID.String()+":"+purpose))
}

func repairFingerprint(sagaID uuid.UUID, action string, proof [sha256.Size]byte) [sha256.Size]byte {
	value := append([]byte("payment-action-v1:"+sagaID.String()+":"+action+":"), proof[:]...)
	return sha256.Sum256(value)
}

func (r *Repairer) finalizeIssue(ctx context.Context, e repairEvidence, command paymentshard.IssueTicketsCommand, receipt paymentshard.IssueTicketsReceipt, leaseOwner string) error {
	if !validIssueRepairReceipt(e, command, receipt) {
		return paymentreconcile.ErrRepairUnavailable
	}
	tx, err := r.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var intentState, sagaState, step, shardID string
	var trainRunID, ownerID uuid.UUID
	var generation int64
	var activeLease bool
	if err := tx.QueryRow(ctx, `
SELECT intent.state,saga.state,saga.current_step,locator.train_run_id,
       locator.shard_id,locator.assignment_generation,locator.owner_user_id,
       saga.lease_owner=$3 AND saga.lease_until>=clock_timestamp()
FROM public.payment_intents AS intent
JOIN public.payment_sagas AS saga ON saga.payment_intent_id=intent.payment_intent_id
JOIN public.reservation_shard_locators AS locator ON locator.reservation_id=intent.reservation_id
WHERE intent.payment_intent_id=$1 AND saga.saga_id=$2 FOR UPDATE OF intent,saga,locator`, e.IntentID, e.SagaID, leaseOwner).Scan(
		&intentState, &sagaState, &step, &trainRunID, &shardID, &generation, &ownerID, &activeLease); err != nil {
		return err
	}
	if intentState != "ticket_issue_pending" || sagaState != "issuing_tickets" || step != "issue_tickets" ||
		trainRunID != e.TrainRunID || ownerID != e.OwnerID || shardID == "" || generation < 1 || !activeLease {
		return paymentreconcile.ErrRepairUnavailable
	}
	if tag, err := tx.Exec(ctx, `INSERT INTO public.ticket_order_shard_locators(
 ticket_order_id,reservation_id,train_run_id,shard_id,assignment_generation,owner_user_id,status,total_amount_minor,currency,created_at
) VALUES($1,$2,$3,$4,$5,$6,'confirmed',$7,$8,$9)
ON CONFLICT(ticket_order_id) DO UPDATE SET status=EXCLUDED.status
WHERE ticket_order_shard_locators.reservation_id=EXCLUDED.reservation_id
 AND ticket_order_shard_locators.train_run_id=EXCLUDED.train_run_id
 AND ticket_order_shard_locators.shard_id=EXCLUDED.shard_id
 AND ticket_order_shard_locators.assignment_generation=EXCLUDED.assignment_generation
 AND ticket_order_shard_locators.owner_user_id=EXCLUDED.owner_user_id
 AND ticket_order_shard_locators.total_amount_minor=EXCLUDED.total_amount_minor
 AND ticket_order_shard_locators.currency=EXCLUDED.currency
 AND ticket_order_shard_locators.created_at=EXCLUDED.created_at`, receipt.TicketOrderID, e.ReservationID, trainRunID, shardID, generation, ownerID, e.AmountMinor, e.Currency, receipt.OrderCreatedAt.UTC()); err != nil || tag.RowsAffected() != 1 {
		return paymentreconcile.ErrRepairUnavailable
	}
	for index, ticketID := range receipt.TicketIDs {
		ticketCode := receipt.TicketCodes[index]
		if ticketID == uuid.Nil || !paymentshard.ValidTicketCode(ticketCode) {
			return paymentreconcile.ErrRepairUnavailable
		}
		if tag, err := tx.Exec(ctx, `INSERT INTO public.ticket_shard_locators(
 ticket_id,ticket_order_id,reservation_id,train_run_id,shard_id,assignment_generation,owner_user_id
) VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT(ticket_id) DO UPDATE SET ticket_order_id=EXCLUDED.ticket_order_id
WHERE ticket_shard_locators.ticket_order_id=EXCLUDED.ticket_order_id
 AND ticket_shard_locators.reservation_id=EXCLUDED.reservation_id
 AND ticket_shard_locators.train_run_id=EXCLUDED.train_run_id
 AND ticket_shard_locators.shard_id=EXCLUDED.shard_id
 AND ticket_shard_locators.assignment_generation=EXCLUDED.assignment_generation
 AND ticket_shard_locators.owner_user_id=EXCLUDED.owner_user_id`, ticketID, receipt.TicketOrderID, e.ReservationID, trainRunID, shardID, generation, ownerID); err != nil || tag.RowsAffected() != 1 {
			return paymentreconcile.ErrRepairUnavailable
		}
		if tag, err := tx.Exec(ctx, `INSERT INTO public.ticket_code_directory(ticket_code,ticket_id)
VALUES($1,$2)
ON CONFLICT(ticket_code) DO UPDATE SET ticket_id=EXCLUDED.ticket_id
WHERE ticket_code_directory.ticket_id=EXCLUDED.ticket_id`, ticketCode, ticketID); err != nil || tag.RowsAffected() != 1 {
			return paymentreconcile.ErrRepairUnavailable
		}
	}
	if err := appendRepairIssuanceLedger(ctx, tx, e, command.IssuanceID, receipt.IssuedAt); err != nil {
		return err
	}
	if tag, err := tx.Exec(ctx, `UPDATE public.payment_intents SET state='completed',completed_at=clock_timestamp()
WHERE payment_intent_id=$1 AND state='ticket_issue_pending'`, e.IntentID); err != nil || tag.RowsAffected() != 1 {
		return paymentreconcile.ErrRepairUnavailable
	}
	if tag, err := tx.Exec(ctx, `UPDATE public.payment_sagas SET state='completed',current_step='complete',completed_at=clock_timestamp(),lease_owner=NULL,lease_until=NULL,bounded_error_category=NULL
WHERE saga_id=$1 AND state='issuing_tickets' AND current_step='issue_tickets'
  AND lease_owner=$2 AND lease_until>=clock_timestamp()`, e.SagaID, leaseOwner); err != nil || tag.RowsAffected() != 1 {
		return paymentreconcile.ErrRepairUnavailable
	}
	return tx.Commit(ctx)
}

func (r *Repairer) finalizeVoid(ctx context.Context, e repairEvidence, command paymentshard.CancelVoidedReservationCommand, receipt paymentshard.CancelVoidedReservationReceipt, leaseOwner string) error {
	if !validVoidRepairReceipt(e, command, receipt) {
		return paymentreconcile.ErrRepairUnavailable
	}
	return r.finalizeTerminalCompensation(ctx, e, "voided", "compensating", receipt.TicketOrderID, false, leaseOwner)
}

func (r *Repairer) finalizeMarkRefundPending(ctx context.Context, e repairEvidence, command paymentshard.MarkRefundPendingCommand, receipt paymentshard.MarkRefundPendingReceipt, leaseOwner string) error {
	if receipt.CommandID != command.CommandID || receipt.PaymentIntentID != e.IntentID ||
		receipt.ReservationID != e.ReservationID || receipt.TicketOrderID == uuid.Nil {
		return paymentreconcile.ErrRepairUnavailable
	}
	tx, err := r.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var intentState, sagaState, step string
	var activeLease bool
	if err := tx.QueryRow(ctx, `SELECT intent.state,saga.state,saga.current_step,
 saga.lease_owner=$3 AND saga.lease_until>=clock_timestamp()
FROM public.payment_intents AS intent
JOIN public.payment_sagas AS saga ON saga.payment_intent_id=intent.payment_intent_id
WHERE intent.payment_intent_id=$1 AND saga.saga_id=$2 FOR UPDATE OF intent,saga`, e.IntentID, e.SagaID, leaseOwner).Scan(
		&intentState, &sagaState, &step, &activeLease); err != nil ||
		intentState != "refund_pending" || sagaState != "compensating" || step != "refund" || !activeLease {
		return paymentreconcile.ErrRepairUnavailable
	}
	if tag, err := tx.Exec(ctx, `UPDATE public.payment_sagas
SET state='refunding',current_step='refund',lease_owner=NULL,lease_until=NULL,
 bounded_error_category=NULL,next_attempt_at=clock_timestamp()
WHERE saga_id=$1 AND state='compensating' AND current_step='refund'
 AND lease_owner=$2 AND lease_until>=clock_timestamp()`, e.SagaID, leaseOwner); err != nil || tag.RowsAffected() != 1 {
		return paymentreconcile.ErrRepairUnavailable
	}
	return tx.Commit(ctx)
}

func (r *Repairer) finalizeCompensation(ctx context.Context, e repairEvidence, command paymentshard.ApplyRefundCompensationCommand, receipt paymentshard.ApplyRefundCompensationReceipt, leaseOwner string) error {
	if !validCompensationRepairReceipt(e, command, receipt) {
		return paymentreconcile.ErrRepairUnavailable
	}
	return r.finalizeTerminalCompensation(ctx, e, "refunded", "refunding", receipt.TicketOrderID, receipt.CancelledTicketCount > 0, leaseOwner)
}

func validIssueRepairReceipt(e repairEvidence, command paymentshard.IssueTicketsCommand, receipt paymentshard.IssueTicketsReceipt) bool {
	if receipt.CommandID != command.CommandID || receipt.IssuanceID != command.IssuanceID || receipt.PaymentIntentID != e.IntentID ||
		receipt.ReservationID != e.ReservationID || receipt.TicketOrderID == uuid.Nil || len(receipt.TicketIDs) == 0 ||
		len(receipt.TicketCodes) != len(receipt.TicketIDs) ||
		receipt.AmountMinor != e.AmountMinor || receipt.Currency != e.Currency ||
		receipt.OrderCreatedAt.IsZero() || receipt.IssuedAt.IsZero() {
		return false
	}
	seen := make(map[uuid.UUID]struct{}, len(receipt.TicketIDs))
	seenCodes := make(map[string]struct{}, len(receipt.TicketCodes))
	for index, ticketID := range receipt.TicketIDs {
		ticketCode := receipt.TicketCodes[index]
		if ticketID == uuid.Nil || !paymentshard.ValidTicketCode(ticketCode) {
			return false
		}
		if _, duplicate := seen[ticketID]; duplicate {
			return false
		}
		seen[ticketID] = struct{}{}
		if _, duplicate := seenCodes[ticketCode]; duplicate {
			return false
		}
		seenCodes[ticketCode] = struct{}{}
	}
	return true
}

func validVoidRepairReceipt(e repairEvidence, command paymentshard.CancelVoidedReservationCommand, receipt paymentshard.CancelVoidedReservationReceipt) bool {
	return receipt.CommandID == command.CommandID && receipt.VoidOperationID == e.OperationID &&
		receipt.PaymentIntentID == e.IntentID && receipt.ReservationID == e.ReservationID &&
		receipt.TicketOrderID != uuid.Nil && receipt.ReleasedSeatCount > 0 && !receipt.CancelledAt.IsZero()
}

func validCompensationRepairReceipt(e repairEvidence, command paymentshard.ApplyRefundCompensationCommand, receipt paymentshard.ApplyRefundCompensationReceipt) bool {
	return receipt.CommandID == command.CommandID && receipt.CompensationID == command.CompensationID &&
		receipt.PaymentIntentID == e.IntentID && receipt.ReservationID == e.ReservationID &&
		receipt.TicketOrderID != uuid.Nil && receipt.ReleasedSeatCount > 0 && receipt.CancelledTicketCount >= 0
}

func (r *Repairer) finalizeTerminalCompensation(ctx context.Context, e repairEvidence, intentState, sagaState string, orderID uuid.UUID, requireLocator bool, leaseOwner string) error {
	if requireLocator && orderID == uuid.Nil {
		return paymentreconcile.ErrRepairUnavailable
	}
	tx, err := r.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var currentIntent, currentSaga, currentStep string
	var activeLease bool
	if err := tx.QueryRow(ctx, `SELECT intent.state,saga.state,saga.current_step,
 saga.lease_owner=$3 AND saga.lease_until>=clock_timestamp()
FROM public.payment_intents AS intent
JOIN public.payment_sagas AS saga ON saga.payment_intent_id=intent.payment_intent_id
WHERE intent.payment_intent_id=$1 AND saga.saga_id=$2 FOR UPDATE OF intent,saga`, e.IntentID, e.SagaID, leaseOwner).Scan(
		&currentIntent, &currentSaga, &currentStep, &activeLease); err != nil ||
		currentIntent != intentState || currentSaga != sagaState || currentStep != "compensate" || !activeLease {
		return paymentreconcile.ErrRepairUnavailable
	}
	if orderID != uuid.Nil {
		tag, err := tx.Exec(ctx, `UPDATE public.ticket_order_shard_locators SET status='cancelled'
WHERE ticket_order_id=$1 AND reservation_id=$2 AND owner_user_id=$3`, orderID, e.ReservationID, e.OwnerID)
		if err != nil || requireLocator && tag.RowsAffected() != 1 || !requireLocator && tag.RowsAffected() != 0 {
			return paymentreconcile.ErrRepairUnavailable
		}
	} else if requireLocator {
		return paymentreconcile.ErrRepairUnavailable
	}
	if intentState == "refunded" {
		if err := appendRepairRefundLedger(ctx, tx, e, requireLocator); err != nil {
			return err
		}
	}
	if tag, err := tx.Exec(ctx, `UPDATE public.payment_intents SET state='cancelled'
WHERE payment_intent_id=$1 AND state=$2`, e.IntentID, intentState); err != nil || tag.RowsAffected() != 1 {
		return paymentreconcile.ErrRepairUnavailable
	}
	if tag, err := tx.Exec(ctx, `UPDATE public.payment_sagas SET state='compensated',current_step='complete',completed_at=clock_timestamp(),lease_owner=NULL,lease_until=NULL,bounded_error_category=NULL
WHERE saga_id=$1 AND state=$2 AND current_step='compensate'
  AND lease_owner=$3 AND lease_until>=clock_timestamp()`, e.SagaID, sagaState, leaseOwner); err != nil || tag.RowsAffected() != 1 {
		return paymentreconcile.ErrRepairUnavailable
	}
	return tx.Commit(ctx)
}

var _ paymentreconcile.Repairer = (*Repairer)(nil)
