// Package postgres persists the provider-neutral payment control plane.
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"regexp"

	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Store struct{ db DB }

func NewStore(db DB) (*Store, error) {
	if db == nil {
		return nil, paymentapp.ErrPaymentUnavailable
	}
	return &Store{db: db}, nil
}

func (store *Store) LookupIntentByIdempotency(
	ctx context.Context,
	ownerID uuid.UUID,
	keyHash [sha256.Size]byte,
	fingerprint [sha256.Size]byte,
) (paymentapp.IntentRecord, bool, error) {
	if store == nil || store.db == nil || ctx == nil || ownerID == uuid.Nil || zeroDigest(keyHash) || zeroDigest(fingerprint) {
		return paymentapp.IntentRecord{}, false, paymentapp.ErrPaymentUnavailable
	}
	record, storedFingerprint, err := scanIntent(store.db.QueryRow(ctx, selectIntent+`
WHERE intent.owner_user_id=$1 AND intent.idempotency_key_hash=$2`, ownerID, keyHash[:]))
	if errors.Is(err, pgx.ErrNoRows) {
		return paymentapp.IntentRecord{}, false, nil
	}
	if err != nil {
		return paymentapp.IntentRecord{}, false, err
	}
	if storedFingerprint != fingerprint {
		return paymentapp.IntentRecord{}, false, paymentapp.ErrPaymentConflict
	}
	return record, true, nil
}

func (store *Store) ReserveIntent(ctx context.Context, request paymentapp.ReserveIntentRequest) (paymentapp.IntentRecord, bool, error) {
	if store == nil || store.db == nil || ctx == nil || !validReserveRequest(request) {
		return paymentapp.IntentRecord{}, false, paymentapp.ErrPaymentUnavailable
	}
	tx, err := store.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return paymentapp.IntentRecord{}, false, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	existing, storedFingerprint, err := scanIntent(tx.QueryRow(ctx, selectIntent+`
WHERE intent.owner_user_id=$1 AND intent.idempotency_key_hash=$2
FOR UPDATE OF intent,saga`, request.OwnerID, request.IdempotencyKeyHash[:]))
	if err == nil {
		if storedFingerprint != request.RequestFingerprint {
			return paymentapp.IntentRecord{}, false, paymentapp.ErrPaymentConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return paymentapp.IntentRecord{}, false, err
		}
		return existing, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return paymentapp.IntentRecord{}, false, err
	}

	_, _, err = scanIntent(tx.QueryRow(ctx, selectIntent+`
WHERE intent.reservation_id=$1
  AND intent.state NOT IN ('completed','voided','refunded','cancelled','failed','expired')
FOR UPDATE OF intent,saga`, request.ReservationID))
	if err == nil {
		return paymentapp.IntentRecord{}, false, paymentapp.ErrPaymentConflict
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return paymentapp.IntentRecord{}, false, err
	}

	_, err = tx.Exec(ctx, `
INSERT INTO public.payment_intents (
    payment_intent_id,reservation_id,train_run_id,owner_user_id,provider,
    amount_minor,currency,state,idempotency_key_hash,request_fingerprint
) VALUES ($1,$2,$3,$4,$5,$6,$7,'reservation_securing',$8,$9)`,
		request.PaymentIntentID, request.ReservationID, request.TrainRunID,
		request.OwnerID, request.Provider, request.AmountMinor, request.Currency,
		request.IdempotencyKeyHash[:], request.RequestFingerprint[:])
	if err != nil {
		if isUniqueViolation(err) {
			_ = tx.Rollback(context.WithoutCancel(ctx))
			record, found, lookupErr := store.LookupIntentByIdempotency(
				ctx, request.OwnerID, request.IdempotencyKeyHash, request.RequestFingerprint,
			)
			if lookupErr != nil {
				return paymentapp.IntentRecord{}, false, lookupErr
			}
			if found {
				return record, true, nil
			}
		}
		return paymentapp.IntentRecord{}, false, normalizeConflict(err)
	}
	_, err = tx.Exec(ctx, `
INSERT INTO public.payment_sagas (
    saga_id,payment_intent_id,reservation_id,current_step,state
) VALUES ($1,$2,$3,'secure_reservation','created')`,
		request.SagaID, request.PaymentIntentID, request.ReservationID)
	if err != nil {
		return paymentapp.IntentRecord{}, false, normalizeConflict(err)
	}
	record, _, err := scanIntent(tx.QueryRow(ctx, selectIntent+`
WHERE intent.payment_intent_id=$1`, request.PaymentIntentID))
	if err != nil {
		return paymentapp.IntentRecord{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentapp.IntentRecord{}, false, err
	}
	return record, false, nil
}

func (store *Store) MarkReservationSecured(
	ctx context.Context,
	intentID uuid.UUID,
	commandID uuid.UUID,
	fingerprint [sha256.Size]byte,
) (paymentapp.IntentRecord, error) {
	if store == nil || store.db == nil || ctx == nil || intentID == uuid.Nil || commandID == uuid.Nil || zeroDigest(fingerprint) {
		return paymentapp.IntentRecord{}, paymentapp.ErrControlFinalizationDeferred
	}
	tx, err := store.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return paymentapp.IntentRecord{}, paymentapp.ErrControlFinalizationDeferred
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	record, storedFingerprint, err := scanIntent(tx.QueryRow(ctx, selectIntent+`
WHERE intent.payment_intent_id=$1
FOR UPDATE OF intent,saga`, intentID))
	if errors.Is(err, pgx.ErrNoRows) {
		return paymentapp.IntentRecord{}, paymentapp.ErrPaymentNotFound
	}
	if err != nil {
		return paymentapp.IntentRecord{}, paymentapp.ErrControlFinalizationDeferred
	}
	if record.BeginCommandID != commandID || storedFingerprint != fingerprint {
		return paymentapp.IntentRecord{}, paymentapp.ErrPaymentConflict
	}

	if _, err = tx.Exec(ctx, `
UPDATE public.payment_intents
SET state='checkout_pending'
WHERE payment_intent_id=$1 AND state='reservation_securing'`, intentID); err != nil {
		return paymentapp.IntentRecord{}, paymentapp.ErrControlFinalizationDeferred
	}
	if _, err = tx.Exec(ctx, `
UPDATE public.payment_sagas
SET state='reservation_secured',current_step='create_checkout',next_attempt_at=clock_timestamp()
WHERE payment_intent_id=$1 AND state='created'`, intentID); err != nil {
		return paymentapp.IntentRecord{}, paymentapp.ErrControlFinalizationDeferred
	}
	operationID, operationHash := operationIdentity(intentID, "create_checkout")
	if _, err = tx.Exec(ctx, `
INSERT INTO public.payment_operations (
    operation_id,payment_intent_id,provider,operation_type,
    provider_idempotency_key_hash,amount_minor,currency
)
SELECT $2,intent.payment_intent_id,intent.provider,'create_checkout',$3,
       intent.amount_minor,intent.currency
FROM public.payment_intents AS intent
WHERE intent.payment_intent_id=$1
ON CONFLICT DO NOTHING`, intentID, operationID, operationHash[:]); err != nil {
		return paymentapp.IntentRecord{}, paymentapp.ErrControlFinalizationDeferred
	}
	var (
		storedOperationID uuid.UUID
		storedHash        []byte
		storedAmount      int64
		storedCurrency    string
	)
	if err = tx.QueryRow(ctx, `
SELECT operation_id,provider_idempotency_key_hash,amount_minor,currency
FROM public.payment_operations
WHERE payment_intent_id=$1 AND operation_type='create_checkout'`, intentID).Scan(
		&storedOperationID, &storedHash, &storedAmount, &storedCurrency,
	); err != nil {
		return paymentapp.IntentRecord{}, paymentapp.ErrControlFinalizationDeferred
	}
	if storedOperationID != operationID || !bytesEqual(storedHash, operationHash[:]) ||
		storedAmount != record.AmountMinor || storedCurrency != record.Currency {
		return paymentapp.IntentRecord{}, paymentapp.ErrPaymentConflict
	}
	record, _, err = scanIntent(tx.QueryRow(ctx, selectIntent+`
WHERE intent.payment_intent_id=$1`, intentID))
	if err != nil {
		return paymentapp.IntentRecord{}, paymentapp.ErrControlFinalizationDeferred
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentapp.IntentRecord{}, paymentapp.ErrControlFinalizationDeferred
	}
	return record, nil
}

func (store *Store) GetOwnedIntent(ctx context.Context, ownerID, intentID uuid.UUID) (paymentapp.IntentRecord, error) {
	if store == nil || store.db == nil || ctx == nil || ownerID == uuid.Nil || intentID == uuid.Nil {
		return paymentapp.IntentRecord{}, paymentapp.ErrPaymentNotFound
	}
	record, _, err := scanIntent(store.db.QueryRow(ctx, selectIntent+`
WHERE intent.payment_intent_id=$1 AND intent.owner_user_id=$2`, intentID, ownerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return paymentapp.IntentRecord{}, paymentapp.ErrPaymentNotFound
	}
	return record, err
}

func (store *Store) RequestCancellation(ctx context.Context, request paymentapp.CancelIntentRequest) (paymentapp.IntentRecord, error) {
	if store == nil || store.db == nil || ctx == nil || !validCancellationRequest(request) {
		return paymentapp.IntentRecord{}, paymentapp.ErrPaymentConflict
	}
	tx, err := store.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return paymentapp.IntentRecord{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	record, _, err := scanIntent(tx.QueryRow(ctx, selectIntent+`
WHERE intent.payment_intent_id=$1 AND intent.owner_user_id=$2
FOR UPDATE OF intent,saga`, request.PaymentIntentID, request.OwnerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return paymentapp.IntentRecord{}, paymentapp.ErrPaymentNotFound
	}
	if err != nil {
		return paymentapp.IntentRecord{}, err
	}

	var captured bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM public.payment_operations
    WHERE payment_intent_id=$1 AND operation_type='capture' AND state='succeeded'
)`, request.PaymentIntentID).Scan(&captured); err != nil {
		return paymentapp.IntentRecord{}, err
	}
	kind := "void"
	if captured {
		kind = "refund"
	}
	providerHash := cancellationOperationHash(request.OwnerID, request.IdempotencyKeyHash)
	if stateRequiresCapture(record.State) && !captured {
		return paymentapp.IntentRecord{}, paymentapp.ErrPaymentConflict
	}

	var existingIntentID uuid.UUID
	var existingKind string
	err = tx.QueryRow(ctx, `
SELECT payment_intent_id,operation_type
FROM public.payment_operations
WHERE provider=$1 AND provider_idempotency_key_hash=$2`, record.Provider, providerHash[:]).Scan(&existingIntentID, &existingKind)
	if err == nil {
		if existingIntentID != request.PaymentIntentID || existingKind != kind {
			return paymentapp.IntentRecord{}, paymentapp.ErrPaymentConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return paymentapp.IntentRecord{}, err
		}
		return record, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return paymentapp.IntentRecord{}, err
	}

	var existingHash []byte
	err = tx.QueryRow(ctx, `
SELECT provider_idempotency_key_hash
FROM public.payment_operations
WHERE payment_intent_id=$1 AND operation_type=$2`, request.PaymentIntentID, kind).Scan(&existingHash)
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return paymentapp.IntentRecord{}, err
		}
		return record, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return paymentapp.IntentRecord{}, err
	}

	if _, err = tx.Exec(ctx, `
INSERT INTO public.payment_operations (
    operation_id,payment_intent_id,provider,operation_type,
    provider_idempotency_key_hash,amount_minor,currency
)
SELECT $2,intent.payment_intent_id,intent.provider,$3,$4,
       intent.amount_minor,intent.currency
FROM public.payment_intents AS intent
WHERE intent.payment_intent_id=$1
ON CONFLICT DO NOTHING`, request.PaymentIntentID, request.OperationID, kind, providerHash[:]); err != nil {
		return paymentapp.IntentRecord{}, normalizeConflict(err)
	}
	var (
		storedIntentID uuid.UUID
		storedHash     []byte
		storedAmount   int64
		storedCurrency string
	)
	if err = tx.QueryRow(ctx, `
SELECT payment_intent_id,provider_idempotency_key_hash,amount_minor,currency
FROM public.payment_operations
WHERE payment_intent_id=$1 AND operation_type=$2`, request.PaymentIntentID, kind).Scan(
		&storedIntentID, &storedHash, &storedAmount, &storedCurrency,
	); err != nil {
		return paymentapp.IntentRecord{}, err
	}
	if storedIntentID != request.PaymentIntentID || !bytesEqual(storedHash, providerHash[:]) ||
		storedAmount != record.AmountMinor || storedCurrency != record.Currency {
		return paymentapp.IntentRecord{}, paymentapp.ErrPaymentConflict
	}
	if kind == "refund" {
		_, err = tx.Exec(ctx, `
UPDATE public.payment_intents SET state='refund_pending'
WHERE payment_intent_id=$1
  AND state IN ('captured','ticket_issue_pending','completed','manual_review')`, request.PaymentIntentID)
	} else {
		_, err = tx.Exec(ctx, `
UPDATE public.payment_intents SET state='void_pending'
WHERE payment_intent_id=$1
  AND state IN ('awaiting_customer','authorized','manual_review')`, request.PaymentIntentID)
	}
	if err != nil {
		return paymentapp.IntentRecord{}, err
	}
	if _, err = tx.Exec(ctx, `
UPDATE public.payment_sagas
SET state='compensating',current_step=$2,next_attempt_at=clock_timestamp()
WHERE payment_intent_id=$1
  AND state IN ('awaiting_provider','authorized','captured','issuing_tickets','manual_review')`, request.PaymentIntentID, kind); err != nil {
		return paymentapp.IntentRecord{}, err
	}
	record, _, err = scanIntent(tx.QueryRow(ctx, selectIntent+`
WHERE intent.payment_intent_id=$1`, request.PaymentIntentID))
	if err != nil {
		return paymentapp.IntentRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentapp.IntentRecord{}, err
	}
	return record, nil
}

const selectIntent = `
SELECT intent.payment_intent_id,saga.saga_id,intent.reservation_id,
       intent.train_run_id,intent.owner_user_id,intent.provider,
       COALESCE(intent.provider_payment_id,''),
       COALESCE(intent.hosted_session_ref,''),intent.amount_minor,
       intent.currency,intent.state,intent.request_fingerprint,
       intent.created_at,intent.updated_at
FROM public.payment_intents AS intent
JOIN public.payment_sagas AS saga
  ON saga.payment_intent_id=intent.payment_intent_id `

func scanIntent(row pgx.Row) (paymentapp.IntentRecord, [sha256.Size]byte, error) {
	var (
		record      paymentapp.IntentRecord
		fingerprint []byte
	)
	err := row.Scan(
		&record.ID, &record.SagaID, &record.ReservationID, &record.TrainRunID,
		&record.OwnerID, &record.Provider, &record.ProviderPaymentID,
		&record.HostedSessionRef, &record.AmountMinor, &record.Currency,
		&record.State, &fingerprint, &record.CreatedAt, &record.UpdatedAt,
	)
	if err != nil {
		return paymentapp.IntentRecord{}, [sha256.Size]byte{}, err
	}
	if len(fingerprint) != sha256.Size {
		return paymentapp.IntentRecord{}, [sha256.Size]byte{}, fmt.Errorf("invalid stored payment fingerprint length %d", len(fingerprint))
	}
	var digest [sha256.Size]byte
	copy(digest[:], fingerprint)
	record.BeginCommandID = beginCommandID(record.SagaID)
	return record, digest, nil
}

func beginCommandID(sagaID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(paymentIdentityNamespace, []byte(sagaID.String()+":secure_reservation"))
}

func operationIdentity(intentID uuid.UUID, kind string) (uuid.UUID, [sha256.Size]byte) {
	identity := []byte(intentID.String() + ":" + kind)
	return uuid.NewSHA1(paymentIdentityNamespace, identity), sha256.Sum256(append([]byte("provider:v1:"), identity...))
}

func validReserveRequest(request paymentapp.ReserveIntentRequest) bool {
	return request.PaymentIntentID != uuid.Nil && request.SagaID != uuid.Nil &&
		request.BeginCommandID != uuid.Nil && request.ReservationID != uuid.Nil &&
		request.TrainRunID != uuid.Nil && request.OwnerID != uuid.Nil &&
		providerPattern.MatchString(request.Provider) && request.AmountMinor >= 0 &&
		currencyPattern.MatchString(request.Currency) && !zeroDigest(request.IdempotencyKeyHash) &&
		!zeroDigest(request.RequestFingerprint)
}

func validCancellationRequest(request paymentapp.CancelIntentRequest) bool {
	if request.OwnerID == uuid.Nil || request.PaymentIntentID == uuid.Nil || request.OperationID == uuid.Nil ||
		zeroDigest(request.IdempotencyKeyHash) || zeroDigest(request.RequestFingerprint) {
		return false
	}
	return request.RequestFingerprint == cancellationFingerprint(request.OwnerID, request.PaymentIntentID)
}

func cancellationFingerprint(ownerID, intentID uuid.UUID) [sha256.Size]byte {
	digest := sha256.New()
	writeIdentityField(digest, "v1")
	writeIdentityField(digest, "payment_intent.cancel")
	writeIdentityField(digest, ownerID.String())
	writeIdentityField(digest, intentID.String())
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func cancellationOperationHash(ownerID uuid.UUID, keyHash [sha256.Size]byte) [sha256.Size]byte {
	digest := sha256.New()
	writeIdentityField(digest, "v1")
	writeIdentityField(digest, "payment_operation.cancel")
	writeIdentityField(digest, ownerID.String())
	writeIdentityField(digest, string(keyHash[:]))
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func writeIdentityField(writer hash.Hash, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = writer.Write(size[:])
	_, _ = writer.Write([]byte(value))
}

func stateRequiresCapture(state string) bool {
	switch state {
	case "captured", "ticket_issue_pending", "completed", "refund_pending", "refunded":
		return true
	default:
		return false
	}
}

func normalizeConflict(err error) error {
	if isUniqueViolation(err) {
		return paymentapp.ErrPaymentConflict
	}
	return err
}

func isUniqueViolation(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && postgresError.Code == "23505"
}

func zeroDigest(value [sha256.Size]byte) bool {
	return value == [sha256.Size]byte{}
}

func bytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var difference byte
	for index := range left {
		difference |= left[index] ^ right[index]
	}
	return difference == 0
}

var paymentIdentityNamespace = uuid.MustParse("d4afbc57-7bb0-5f88-bff4-62db186e9ea8")

var (
	providerPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
)

var _ paymentapp.IntentStore = (*Store)(nil)
