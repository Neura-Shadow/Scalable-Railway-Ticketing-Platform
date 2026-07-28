package postgres

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// acquireGlobalIdempotencyClaim preserves the existing user/operation/key
// uniqueness contract while completed results remain authoritative only in
// the routed shard-local idempotency record. The route and fence have already
// been validated by the surrounding transaction.
func (tx *Tx) acquireGlobalIdempotencyClaim(ctx context.Context, input IdempotencyInput) (time.Time, time.Time, error) {
	if tx == nil || tx.tx == nil || tx.routed == nil || tx.route.TrainRunID() == uuid.Nil {
		return time.Time{}, time.Time{}, ErrInvalidArgument
	}
	var acquiredAt time.Time
	if err := tx.tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&acquiredAt); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("derive idempotency acquisition time: %w", err)
	}
	acquiredAt = acquiredAt.UTC()
	// A rollback window can retain the previous shard copy. Retire an expired
	// claim and all of its exact copies before repointing the uniqueness key.
	// Lock any existing claim before deciding whether it was expired at the
	// single acquisition timestamp; later statements must not independently
	// cross the expiry boundary and repoint a foreign claim.
	// The routed transaction holds only its own assignment lock. A claim owned
	// by another train run must fail closed so this request cannot mutate that
	// run while its migration copier may be active. The migration-aware cleanup
	// path retires it after the old assignment is stable again.
	var (
		claimID         uuid.UUID
		fingerprint     []byte
		claimTrainRunID pgtype.UUID
		expiresAt       time.Time
	)
	err := tx.tx.QueryRow(ctx, `
SELECT id, request_fingerprint, train_run_id, expires_at
FROM public.booking_idempotency_key_claims
WHERE user_id = $1
  AND operation = $2
  AND key_hash = $3
FOR UPDATE`, input.UserID, input.Operation, input.KeyHash).Scan(
		&claimID, &fingerprint, &claimTrainRunID, &expiresAt,
	)
	if err != nil && err != pgx.ErrNoRows {
		return time.Time{}, time.Time{}, fmt.Errorf("lock booking idempotency key claim: %w", err)
	}
	if err == nil {
		if expiresAt.After(acquiredAt) {
			if !bytes.Equal(fingerprint, input.RequestFingerprint) || !claimTrainRunID.Valid ||
				claimTrainRunID.Bytes != tx.route.TrainRunID() {
				return time.Time{}, time.Time{}, ErrIdempotencyConflict
			}
			return expiresAt.UTC(), acquiredAt, nil
		}
		if !claimTrainRunID.Valid || claimTrainRunID.Bytes != tx.route.TrainRunID() {
			return time.Time{}, time.Time{}, sharding.ErrShardUnavailable
		}
		if err := retireExpiredIdempotencyClaims(ctx, tx.tx, []uuid.UUID{claimID}); err != nil {
			return time.Time{}, time.Time{}, err
		}
	}
	if _, err := tx.tx.Exec(ctx, `
INSERT INTO public.booking_idempotency_key_claims (
    user_id, operation, key_hash, request_fingerprint, train_run_id, expires_at
)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (user_id, operation, key_hash) DO NOTHING`,
		input.UserID,
		input.Operation,
		input.KeyHash,
		input.RequestFingerprint,
		tx.route.TrainRunID(),
		acquiredAt.Add(24*time.Hour),
	); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("acquire booking idempotency key claim: %w", err)
	}

	var trainRunID uuid.UUID
	if err := tx.tx.QueryRow(ctx, `
SELECT request_fingerprint, train_run_id, expires_at
FROM public.booking_idempotency_key_claims
WHERE user_id = $1
  AND operation = $2
  AND key_hash = $3
FOR UPDATE`, input.UserID, input.Operation, input.KeyHash).Scan(
		&fingerprint, &trainRunID, &expiresAt,
	); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("lock inserted booking idempotency key claim: %w", err)
	}
	if !bytes.Equal(fingerprint, input.RequestFingerprint) {
		return time.Time{}, time.Time{}, ErrIdempotencyConflict
	}
	if trainRunID != tx.route.TrainRunID() {
		return time.Time{}, time.Time{}, ErrIdempotencyConflict
	}
	if !expiresAt.After(acquiredAt) {
		return time.Time{}, time.Time{}, sharding.ErrShardUnavailable
	}
	return expiresAt.UTC(), acquiredAt, nil
}
