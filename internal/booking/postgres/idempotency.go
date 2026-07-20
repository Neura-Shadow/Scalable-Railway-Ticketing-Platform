package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Operation string

const (
	OperationReservationCreate  Operation = "reservation.create"
	OperationReservationConfirm Operation = "reservation.confirm"
	OperationReservationCancel  Operation = "reservation.cancel"
)

var (
	ErrIdempotencyConflict   = errors.New("idempotency key reused with a different request")
	ErrIdempotencyInProgress = errors.New("committed idempotency record is still in progress")
)

type IdempotencyInput struct {
	UserID             uuid.UUID
	Operation          Operation
	KeyHash            []byte
	RequestFingerprint []byte
	ExpiresAt          time.Time
}

type IdempotencyAcquisition struct {
	RecordID   uuid.UUID
	Owned      bool
	Replayed   bool
	ResourceID uuid.UUID
}

type CompletedCreateHoldLookupParams struct {
	UserID             uuid.UUID
	IdempotencyKeyHash []byte
	RequestFingerprint []byte
}

// LookupCompletedCreateHold performs a bounded read-only replay probe. It does
// not acquire or create an idempotency record, so missing, expired, and
// in-progress commands cannot bypass admission or enter booking execution.
func (s *Store) LookupCompletedCreateHold(
	ctx context.Context,
	params CompletedCreateHoldLookupParams,
) (CreateHoldResult, bool, error) {
	if s == nil || s.pool == nil || params.UserID == uuid.Nil ||
		len(params.IdempotencyKeyHash) != 32 || len(params.RequestFingerprint) != 32 {
		return CreateHoldResult{}, false, ErrInvalidArgument
	}

	var (
		fingerprint   []byte
		status        string
		resourceID    *uuid.UUID
		reservationID *uuid.UUID
		totalAmount   *int64
		currency      *string
		seatCount     *int
	)
	err := s.pool.QueryRow(ctx, `
SELECT ir.request_fingerprint, ir.status, ir.resource_id,
       r.id, r.total_amount_minor, r.currency,
       (SELECT count(*) FROM reservation_seats AS rs WHERE rs.reservation_id = r.id)
FROM idempotency_records AS ir
LEFT JOIN reservations AS r
  ON r.id = ir.resource_id
 AND r.user_id = ir.user_id
WHERE ir.user_id = $1
  AND ir.operation = 'reservation.create'
  AND ir.key_hash = $2
  AND ir.expires_at > clock_timestamp()`,
		params.UserID, params.IdempotencyKeyHash,
	).Scan(
		&fingerprint, &status, &resourceID,
		&reservationID, &totalAmount, &currency, &seatCount,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return CreateHoldResult{}, false, nil
		}
		return CreateHoldResult{}, false, fmt.Errorf("lookup completed create hold: %w", err)
	}
	if status != "completed" {
		return CreateHoldResult{}, false, nil
	}
	if !bytes.Equal(fingerprint, params.RequestFingerprint) {
		return CreateHoldResult{}, false, ErrIdempotencyConflict
	}
	if resourceID == nil || reservationID == nil || totalAmount == nil || currency == nil || seatCount == nil ||
		*resourceID == uuid.Nil || *reservationID != *resourceID {
		return CreateHoldResult{}, false, ErrPersistenceInvariant
	}
	result := CreateHoldResult{
		ReservationID: *reservationID, TotalAmountMinor: *totalAmount,
		Currency: *currency, SeatCount: *seatCount, Replayed: true,
	}
	return result, true, nil
}

func (tx *Tx) AcquireIdempotency(ctx context.Context, input IdempotencyInput) (IdempotencyAcquisition, error) {
	if tx == nil || tx.tx == nil || input.UserID == uuid.Nil || !validOperation(input.Operation) ||
		len(input.KeyHash) != 32 || len(input.RequestFingerprint) != 32 || input.ExpiresAt.IsZero() {
		return IdempotencyAcquisition{}, ErrInvalidArgument
	}

	var insertedID uuid.UUID
	err := tx.tx.QueryRow(ctx, `
INSERT INTO idempotency_records (
    user_id, operation, key_hash, request_fingerprint, expires_at
)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (user_id, operation, key_hash) DO UPDATE
SET id = gen_random_uuid(),
    request_fingerprint = EXCLUDED.request_fingerprint,
    status = 'in_progress',
    resource_type = NULL,
    resource_id = NULL,
    expires_at = EXCLUDED.expires_at,
    created_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE idempotency_records.expires_at <= clock_timestamp()
RETURNING id`, input.UserID, input.Operation, input.KeyHash, input.RequestFingerprint, input.ExpiresAt.UTC()).Scan(&insertedID)
	inserted := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return IdempotencyAcquisition{}, fmt.Errorf("insert idempotency record: %w", err)
	}

	var (
		recordID    uuid.UUID
		fingerprint []byte
		status      string
		resourceID  *uuid.UUID
	)
	err = tx.tx.QueryRow(ctx, `
SELECT id, request_fingerprint, status, resource_id
FROM idempotency_records
WHERE user_id = $1
  AND operation = $2
  AND key_hash = $3
FOR UPDATE`, input.UserID, input.Operation, input.KeyHash).Scan(&recordID, &fingerprint, &status, &resourceID)
	if err != nil {
		return IdempotencyAcquisition{}, fmt.Errorf("lock idempotency record: %w", err)
	}
	if !bytes.Equal(fingerprint, input.RequestFingerprint) {
		return IdempotencyAcquisition{}, ErrIdempotencyConflict
	}

	switch status {
	case "in_progress":
		if !inserted || insertedID != recordID {
			return IdempotencyAcquisition{}, ErrIdempotencyInProgress
		}
		return IdempotencyAcquisition{RecordID: recordID, Owned: true}, nil
	case "completed":
		if resourceID == nil || *resourceID == uuid.Nil {
			return IdempotencyAcquisition{}, ErrPersistenceInvariant
		}
		return IdempotencyAcquisition{RecordID: recordID, Replayed: true, ResourceID: *resourceID}, nil
	default:
		return IdempotencyAcquisition{}, ErrPersistenceInvariant
	}
}

func (tx *Tx) CompleteIdempotency(ctx context.Context, recordID, reservationID uuid.UUID) error {
	if tx == nil || tx.tx == nil || recordID == uuid.Nil || reservationID == uuid.Nil {
		return ErrInvalidArgument
	}
	commandTag, err := tx.tx.Exec(ctx, `
UPDATE idempotency_records
SET status = 'completed',
    resource_type = 'reservation',
    resource_id = $2
WHERE id = $1
  AND status = 'in_progress'`, recordID, reservationID)
	if err != nil {
		return fmt.Errorf("complete idempotency record: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ErrPersistenceInvariant
	}
	return nil
}

func validOperation(operation Operation) bool {
	switch operation {
	case OperationReservationCreate, OperationReservationConfirm, OperationReservationCancel:
		return true
	default:
		return false
	}
}
