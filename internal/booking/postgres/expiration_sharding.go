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
	shards, err := s.shards.ListEnabledShards(ctx)
	if err != nil {
		return err
	}
	perShard := limit / len(shards)
	if perShard < 1 {
		perShard = 1
	}
	var failures []error
	for _, shardID := range shards {
		query, ok := expiredIdempotencyCleanupQuery(shardID)
		if !ok {
			failures = append(failures, sharding.ErrShardUnavailable)
			continue
		}
		if _, err := s.pool.Exec(ctx, query, perShard); err != nil {
			failures = append(failures, sharding.ErrShardUnavailable)
		}
	}
	if _, err := s.pool.Exec(ctx, `
WITH expired AS (
    SELECT user_id, operation, key_hash
    FROM public.booking_idempotency_key_claims
    WHERE expires_at <= clock_timestamp()
    ORDER BY expires_at, user_id, operation, key_hash
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
DELETE FROM public.booking_idempotency_key_claims AS claim
USING expired
WHERE claim.user_id = expired.user_id
  AND claim.operation = expired.operation
  AND claim.key_hash = expired.key_hash`, limit); err != nil {
		failures = append(failures, sharding.ErrShardUnavailable)
	}
	return errors.Join(failures...)
}

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

func expiredIdempotencyCleanupQuery(shardID sharding.ShardID) (string, bool) {
	const legacy = `
WITH expired AS (
    SELECT record.id FROM public.idempotency_records AS record
    WHERE record.expires_at <= clock_timestamp()
      AND EXISTS (
          SELECT 1
          FROM public.train_run_shard_assignments AS assignment
          WHERE assignment.train_run_id = record.train_run_id
            AND assignment.shard_id = 'legacy'
            AND assignment.assignment_state = 'stable'
            AND assignment.active_migration_id IS NULL
      )
    ORDER BY record.expires_at, record.id FOR UPDATE OF record SKIP LOCKED LIMIT $1
)
DELETE FROM public.idempotency_records AS record
USING expired WHERE record.id = expired.id`
	const shardZero = `
WITH expired AS (
    SELECT record.id FROM booking_shard_0.idempotency_records AS record
    WHERE record.expires_at <= clock_timestamp()
      AND EXISTS (
          SELECT 1
          FROM public.train_run_shard_assignments AS assignment
          WHERE assignment.train_run_id = record.train_run_id
            AND assignment.shard_id = 'shard-0'
            AND assignment.assignment_state = 'stable'
            AND assignment.active_migration_id IS NULL
      )
    ORDER BY record.expires_at, record.id FOR UPDATE OF record SKIP LOCKED LIMIT $1
)
DELETE FROM booking_shard_0.idempotency_records AS record
USING expired WHERE record.id = expired.id`
	const shardOne = `
WITH expired AS (
    SELECT record.id FROM booking_shard_1.idempotency_records AS record
    WHERE record.expires_at <= clock_timestamp()
      AND EXISTS (
          SELECT 1
          FROM public.train_run_shard_assignments AS assignment
          WHERE assignment.train_run_id = record.train_run_id
            AND assignment.shard_id = 'shard-1'
            AND assignment.assignment_state = 'stable'
            AND assignment.active_migration_id IS NULL
      )
    ORDER BY record.expires_at, record.id FOR UPDATE OF record SKIP LOCKED LIMIT $1
)
DELETE FROM booking_shard_1.idempotency_records AS record
USING expired WHERE record.id = expired.id`
	switch shardID {
	case sharding.ShardLegacy:
		return legacy, true
	case sharding.ShardZero:
		return shardZero, true
	case sharding.ShardOne:
		return shardOne, true
	default:
		return "", false
	}
}
