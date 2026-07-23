package postgres

import (
	"bytes"
	"context"
	"fmt"

	"github.com/google/uuid"
)

// acquireGlobalIdempotencyClaim preserves the existing user/operation/key
// uniqueness contract while completed results remain authoritative only in
// the routed shard-local idempotency record. The route and fence have already
// been validated by the surrounding transaction.
func (tx *Tx) acquireGlobalIdempotencyClaim(ctx context.Context, input IdempotencyInput) error {
	if tx == nil || tx.tx == nil || tx.routed == nil || tx.route.TrainRunID() == uuid.Nil {
		return ErrInvalidArgument
	}
	if _, err := tx.tx.Exec(ctx, `
INSERT INTO public.booking_idempotency_key_claims (
    user_id, operation, key_hash, request_fingerprint, train_run_id,
    shard_id, assignment_generation, expires_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (user_id, operation, key_hash) DO UPDATE
SET request_fingerprint = EXCLUDED.request_fingerprint,
    train_run_id = EXCLUDED.train_run_id,
    shard_id = EXCLUDED.shard_id,
    assignment_generation = EXCLUDED.assignment_generation,
    local_record_id = NULL,
    expires_at = EXCLUDED.expires_at,
    created_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE booking_idempotency_key_claims.expires_at <= clock_timestamp()`,
		input.UserID,
		input.Operation,
		input.KeyHash,
		input.RequestFingerprint,
		tx.route.TrainRunID(),
		tx.route.ShardID().String(),
		tx.route.Generation().Int64(),
		input.ExpiresAt.UTC(),
	); err != nil {
		return fmt.Errorf("acquire booking idempotency key claim: %w", err)
	}

	var (
		fingerprint []byte
		trainRunID  uuid.UUID
		shardID     string
		generation  int64
	)
	if err := tx.tx.QueryRow(ctx, `
SELECT request_fingerprint, train_run_id, shard_id, assignment_generation
FROM public.booking_idempotency_key_claims
WHERE user_id = $1
  AND operation = $2
  AND key_hash = $3
FOR UPDATE`, input.UserID, input.Operation, input.KeyHash).Scan(
		&fingerprint, &trainRunID, &shardID, &generation,
	); err != nil {
		return fmt.Errorf("lock booking idempotency key claim: %w", err)
	}
	if !bytes.Equal(fingerprint, input.RequestFingerprint) {
		return ErrIdempotencyConflict
	}
	if trainRunID != tx.route.TrainRunID() || shardID != tx.route.ShardID().String() ||
		generation != tx.route.Generation().Int64() {
		return ErrIdempotencyConflict
	}
	return nil
}

func (tx *Tx) bindGlobalIdempotencyClaim(
	ctx context.Context,
	input IdempotencyInput,
	localRecordID uuid.UUID,
) error {
	if tx == nil || tx.tx == nil || tx.routed == nil || localRecordID == uuid.Nil {
		return ErrInvalidArgument
	}
	result, err := tx.tx.Exec(ctx, `
UPDATE public.booking_idempotency_key_claims
SET local_record_id = $4,
    updated_at = clock_timestamp()
WHERE user_id = $1
  AND operation = $2
  AND key_hash = $3
  AND request_fingerprint = $5
  AND train_run_id = $6
  AND shard_id = $7
  AND assignment_generation = $8`,
		input.UserID,
		input.Operation,
		input.KeyHash,
		localRecordID,
		input.RequestFingerprint,
		tx.route.TrainRunID(),
		tx.route.ShardID().String(),
		tx.route.Generation().Int64(),
	)
	if err != nil {
		return fmt.Errorf("bind booking idempotency key claim: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrPersistenceInvariant
	}
	return nil
}
