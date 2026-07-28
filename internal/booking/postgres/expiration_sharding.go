package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// expireDueAcrossShards walks the fixed topology in round-robin order. Each
// claim is revalidated through its locator and database fence in its own
// transaction; a failed shard is isolated for this pass.
func (s *Store) expireDueAcrossShards(ctx context.Context, limit int) ([]uuid.UUID, error) {
	shards, err := s.shards.ListEnabledShards(ctx)
	if err != nil {
		return nil, err
	}
	var (
		expired      = make([]uuid.UUID, 0, limit)
		excluded     = make(map[sharding.ShardID][]uuid.UUID, len(shards))
		failedShards = make(map[sharding.ShardID]struct{}, len(shards))
		failures     []error
		madeProgress = true
		attempts     int
	)
	for len(expired) < limit && attempts < limit && madeProgress {
		madeProgress = false
		for _, shardID := range shards {
			if len(expired) >= limit {
				break
			}
			if _, failed := failedShards[shardID]; failed {
				continue
			}
			reservationID, found, findErr := s.findDueReservationOnShard(ctx, shardID, excluded[shardID])
			if findErr != nil {
				failedShards[shardID] = struct{}{}
				failures = append(failures, findErr)
				continue
			}
			if !found {
				continue
			}
			excluded[shardID] = append(excluded[shardID], reservationID)
			attempts++
			madeProgress = true
			expiredNow, expireErr := s.expireLocatedReservation(ctx, reservationID)
			if expireErr != nil {
				failures = append(failures, fmt.Errorf("expire located reservation: %w", expireErr))
				continue
			}
			if expiredNow {
				expired = append(expired, reservationID)
			}
		}
	}
	if cleanupErr := s.cleanupExpiredIdempotencyAcrossShards(ctx, 1000); cleanupErr != nil {
		failures = append(failures, cleanupErr)
	}
	return expired, errors.Join(failures...)
}

func (s *Store) findDueReservationOnShard(
	ctx context.Context,
	shardID sharding.ShardID,
	excluded []uuid.UUID,
) (uuid.UUID, bool, error) {
	query, ok := dueReservationQuery(shardID)
	if !ok {
		return uuid.Nil, false, sharding.ErrShardUnavailable
	}
	excludedStrings := make([]string, len(excluded))
	for index, id := range excluded {
		excludedStrings[index] = id.String()
	}
	var reservationID uuid.UUID
	if err := s.pool.QueryRow(
		ctx,
		query,
		excludedStrings,
		sharding.SupportedFencingProtocolVersion,
	).Scan(&reservationID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, false, nil
		}
		return uuid.Nil, false, sharding.ErrShardUnavailable
	}
	return reservationID, true, nil
}

func (s *Store) expireLocatedReservation(ctx context.Context, reservationID uuid.UUID) (bool, error) {
	tx, err := s.beginReservationMaintenanceWrite(ctx, reservationID)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var id uuid.UUID
	err = tx.tx.QueryRow(ctx, `
SELECT id
FROM reservations
WHERE id = $1
  AND status = 'held'
  AND expires_at <= clock_timestamp()
FOR UPDATE`, reservationID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock routed due reservation: %w", err)
	}
	result, err := tx.tx.Exec(ctx, `
UPDATE reservations
SET status = 'expired'
WHERE id = $1
  AND status = 'held'
  AND expires_at <= clock_timestamp()`, id)
	if err != nil {
		return false, fmt.Errorf("expire routed reservation: %w", err)
	}
	if result.RowsAffected() != 1 {
		return false, ErrPersistenceInvariant
	}
	released, err := tx.releaseReservationSeats(ctx, id)
	if err != nil {
		return false, err
	}
	if err := tx.closeReservationQuotaClaim(ctx, id); err != nil {
		return false, err
	}
	if err := tx.appendReservationEvent(ctx, id, "reservation.expired", map[string]any{
		"reservationId": id, "status": "expired", "releasedSeatCount": released,
	}); err != nil {
		return false, err
	}
	if err := tx.recordSuccessfulGenerationWrite(ctx); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) cleanupExpiredIdempotencyAcrossShards(ctx context.Context, limit int) error {
	if limit <= 0 {
		return ErrInvalidArgument
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sharding.ErrShardUnavailable
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// Lock routing authority before key claims, matching the routed-write and
	// migration lock order. Stable assignments are the only safe cleanup
	// boundary; a migration may retain copies on both source and target.
	rows, err := tx.Query(ctx, expiredIdempotencyRouteSelectionQuery, limit)
	if err != nil {
		return sharding.ErrShardUnavailable
	}
	trainRunIDs := make([]uuid.UUID, 0, limit)
	for rows.Next() {
		var trainRunID uuid.UUID
		if err := rows.Scan(&trainRunID); err != nil {
			rows.Close()
			return sharding.ErrShardUnavailable
		}
		trainRunIDs = append(trainRunIDs, trainRunID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return sharding.ErrShardUnavailable
	}
	rows.Close()

	claimIDs := make([]uuid.UUID, 0, limit)
	if len(trainRunIDs) > 0 {
		rows, err = tx.Query(ctx, expiredIdempotencyRoutedClaimSelectionQuery, trainRunIDs, limit)
		if err != nil {
			return sharding.ErrShardUnavailable
		}
		for rows.Next() {
			var claimID uuid.UUID
			if err := rows.Scan(&claimID); err != nil {
				rows.Close()
				return sharding.ErrShardUnavailable
			}
			claimIDs = append(claimIDs, claimID)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return sharding.ErrShardUnavailable
		}
		rows.Close()
	}

	remaining := limit - len(claimIDs)
	if remaining > 0 {
		rows, err = tx.Query(ctx, expiredIdempotencyLegacyClaimSelectionQuery, remaining)
		if err != nil {
			return sharding.ErrShardUnavailable
		}
		for rows.Next() {
			var claimID uuid.UUID
			if err := rows.Scan(&claimID); err != nil {
				rows.Close()
				return sharding.ErrShardUnavailable
			}
			claimIDs = append(claimIDs, claimID)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return sharding.ErrShardUnavailable
		}
		rows.Close()
	}
	if len(claimIDs) == 0 {
		return tx.Commit(ctx)
	}

	if err := retireExpiredIdempotencyClaims(ctx, tx, claimIDs); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return sharding.ErrShardUnavailable
	}
	return nil
}

// retireExpiredIdempotencyClaims deletes every exact shard-local copy before
// releasing its global uniqueness claim. The caller must already hold the
// applicable assignment locks followed by the claim locks.
func retireExpiredIdempotencyClaims(ctx context.Context, tx pgx.Tx, claimIDs []uuid.UUID) error {
	if len(claimIDs) == 0 {
		return nil
	}
	// A pre-cutover rollback intentionally retains the target copy, so cleanup
	// must cover the complete fixed topology rather than only current authority.
	for _, query := range expiredIdempotencyRecordDeleteQueries {
		if _, err := tx.Exec(ctx, query, claimIDs); err != nil {
			return sharding.ErrShardUnavailable
		}
	}
	var localRecordRemains bool
	if err := tx.QueryRow(ctx, expiredIdempotencyRecordVerificationQuery, claimIDs).Scan(&localRecordRemains); err != nil {
		return sharding.ErrShardUnavailable
	}
	if localRecordRemains {
		return ErrPersistenceInvariant
	}
	result, err := tx.Exec(ctx, `
DELETE FROM public.booking_idempotency_key_claims
WHERE id = ANY($1::uuid[])`, claimIDs)
	if err != nil {
		return sharding.ErrShardUnavailable
	}
	if result.RowsAffected() != int64(len(claimIDs)) {
		return ErrPersistenceInvariant
	}
	return nil
}

const expiredIdempotencyRouteSelectionQuery = `
SELECT assignment.train_run_id
FROM public.train_run_shard_assignments AS assignment
WHERE assignment.assignment_state = 'stable'
  AND assignment.active_migration_id IS NULL
  AND EXISTS (
      SELECT 1
      FROM public.booking_idempotency_key_claims AS claim
      WHERE claim.train_run_id = assignment.train_run_id
        AND claim.expires_at <= clock_timestamp()
  )
ORDER BY assignment.train_run_id
FOR UPDATE OF assignment SKIP LOCKED
LIMIT $1`

const expiredIdempotencyRoutedClaimSelectionQuery = `
SELECT claim.id
FROM public.booking_idempotency_key_claims AS claim
WHERE claim.train_run_id = ANY($1::uuid[])
  AND claim.expires_at <= clock_timestamp()
ORDER BY claim.expires_at, claim.id
FOR UPDATE OF claim SKIP LOCKED
LIMIT $2`

const expiredIdempotencyLegacyClaimSelectionQuery = `
SELECT claim.id
FROM public.booking_idempotency_key_claims AS claim
WHERE claim.train_run_id IS NULL
  AND claim.expires_at <= clock_timestamp()
ORDER BY claim.expires_at, claim.id
FOR UPDATE OF claim SKIP LOCKED
LIMIT $1`

var expiredIdempotencyRecordDeleteQueries = [...]string{
	`DELETE FROM public.idempotency_records AS record
USING public.booking_idempotency_key_claims AS claim
WHERE claim.id = ANY($1::uuid[])
  AND record.user_id = claim.user_id
  AND record.operation = claim.operation
  AND record.key_hash = claim.key_hash
  AND record.request_fingerprint = claim.request_fingerprint
  AND record.train_run_id IS NOT DISTINCT FROM claim.train_run_id
  AND record.expires_at = claim.expires_at`,
	`DELETE FROM booking_shard_0.idempotency_records AS record
USING public.booking_idempotency_key_claims AS claim
WHERE claim.id = ANY($1::uuid[])
  AND record.user_id = claim.user_id
  AND record.operation = claim.operation
  AND record.key_hash = claim.key_hash
  AND record.request_fingerprint = claim.request_fingerprint
  AND record.train_run_id IS NOT DISTINCT FROM claim.train_run_id
  AND record.expires_at = claim.expires_at`,
	`DELETE FROM booking_shard_1.idempotency_records AS record
USING public.booking_idempotency_key_claims AS claim
WHERE claim.id = ANY($1::uuid[])
  AND record.user_id = claim.user_id
  AND record.operation = claim.operation
  AND record.key_hash = claim.key_hash
  AND record.request_fingerprint = claim.request_fingerprint
  AND record.train_run_id IS NOT DISTINCT FROM claim.train_run_id
  AND record.expires_at = claim.expires_at`,
}

const expiredIdempotencyRecordVerificationQuery = `
SELECT EXISTS (
    SELECT 1
    FROM public.booking_idempotency_key_claims AS claim
    JOIN public.idempotency_records AS record
      ON record.user_id = claim.user_id
     AND record.operation = claim.operation
     AND record.key_hash = claim.key_hash
    WHERE claim.id = ANY($1::uuid[])
    UNION ALL
    SELECT 1
    FROM public.booking_idempotency_key_claims AS claim
    JOIN booking_shard_0.idempotency_records AS record
      ON record.user_id = claim.user_id
     AND record.operation = claim.operation
     AND record.key_hash = claim.key_hash
    WHERE claim.id = ANY($1::uuid[])
    UNION ALL
    SELECT 1
    FROM public.booking_idempotency_key_claims AS claim
    JOIN booking_shard_1.idempotency_records AS record
      ON record.user_id = claim.user_id
     AND record.operation = claim.operation
     AND record.key_hash = claim.key_hash
    WHERE claim.id = ANY($1::uuid[])
)`

func dueReservationQuery(shardID sharding.ShardID) (string, bool) {
	const suffix = `
JOIN public.reservation_shard_locators AS locator
  ON locator.reservation_id = reservation.id
 AND locator.train_run_id = reservation.train_run_id
JOIN public.train_run_shard_assignments AS assignment
  ON assignment.train_run_id = locator.train_run_id
 AND assignment.shard_id = locator.shard_id
 AND assignment.assignment_generation = locator.assignment_generation
JOIN public.booking_shards AS catalog
  ON catalog.shard_id = locator.shard_id
WHERE reservation.status = 'held'
  AND reservation.expires_at <= clock_timestamp()
  AND NOT (reservation.id = ANY($1::uuid[]))
  AND assignment.assignment_state IN ('stable', 'rollback_window')
  AND catalog.enabled
  AND catalog.write_enabled
  AND catalog.state IN ('active', 'draining')
  AND catalog.minimum_fencing_protocol_version <= $2
  AND locator.shard_id = `
	const order = `
ORDER BY reservation.expires_at, reservation.id
LIMIT 1`
	switch shardID {
	case sharding.ShardLegacy:
		return "SELECT reservation.id FROM public.reservations AS reservation" + suffix + "'legacy'" + order, true
	case sharding.ShardZero:
		return "SELECT reservation.id FROM booking_shard_0.reservations AS reservation" + suffix + "'shard-0'" + order, true
	case sharding.ShardOne:
		return "SELECT reservation.id FROM booking_shard_1.reservations AS reservation" + suffix + "'shard-1'" + order, true
	default:
		return "", false
	}
}
