package postgres

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var locatorLockQueries = []string{
	`SELECT reservation_id FROM public.reservation_shard_locators WHERE train_run_id = $1 ORDER BY reservation_id FOR UPDATE`,
	`SELECT ticket_order_id FROM public.ticket_order_shard_locators WHERE train_run_id = $1 ORDER BY ticket_order_id FOR UPDATE`,
	`SELECT ticket_id FROM public.ticket_shard_locators WHERE train_run_id = $1 ORDER BY ticket_id FOR UPDATE`,
}

func (tx *Transaction) LockLocatorsForUpdate(ctx context.Context, trainRunID uuid.UUID, rowCap int64) (int64, error) {
	if tx == nil || tx.tx == nil || trainRunID == uuid.Nil || rowCap <= 0 {
		return 0, control.ErrInvalidInput
	}
	var total int64
	err := tx.tx.QueryRow(ctx, `
SELECT count(*)::bigint
FROM (
    SELECT reservation_id AS id
    FROM public.reservation_shard_locators WHERE train_run_id = $1
    UNION ALL
    SELECT ticket_order_id AS id
    FROM public.ticket_order_shard_locators WHERE train_run_id = $1
    UNION ALL
    SELECT ticket_id AS id
    FROM public.ticket_shard_locators WHERE train_run_id = $1
    LIMIT ($2 + 1)
) AS bounded_locator_keys`, trainRunID, rowCap).Scan(&total)
	if err != nil {
		return 0, ErrPersistence
	}
	if total < 0 {
		return 0, ErrPersistence
	}
	if total > rowCap {
		return total, control.ErrLocatorRowCapExceeded
	}
	var locked int64
	for _, query := range locatorLockQueries {
		rows, err := tx.tx.Query(ctx, query, trainRunID)
		if err != nil {
			return 0, ErrPersistence
		}
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil || id == uuid.Nil {
				rows.Close()
				return 0, ErrPersistence
			}
			locked++
			if locked > rowCap {
				rows.Close()
				return locked, control.ErrLocatorRowCapExceeded
			}
		}
		rowsErr := rows.Err()
		rows.Close()
		if rowsErr != nil {
			return 0, ErrPersistence
		}
	}
	if locked != total {
		return 0, ErrPersistence
	}
	return locked, nil
}

func (tx *Transaction) ActivateRoute(ctx context.Context, expected, next sharding.ShardRoute) error {
	if tx == nil || tx.tx == nil || expected.TrainRunID() == uuid.Nil ||
		expected.TrainRunID() != next.TrainRunID() || expected.ShardID() == next.ShardID() ||
		expected.Generation().Int64() <= 0 || next.Generation().Int64() <= expected.Generation().Int64() {
		return control.ErrInvalidInput
	}
	if _, err := schemaForShard(expected.ShardID()); err != nil {
		return control.ErrInvalidInput
	}
	if _, err := schemaForShard(next.ShardID()); err != nil {
		return control.ErrInvalidInput
	}

	var migrationID uuid.UUID
	var sourceShard, targetShard string
	var sourceGeneration, targetGeneration int64
	err := tx.tx.QueryRow(ctx, `
SELECT migration.id,
       migration.source_shard_id,
       migration.target_shard_id,
       migration.source_generation,
       migration.target_generation
FROM public.train_run_shard_assignments AS assignment
JOIN public.train_run_shard_migrations AS migration
  ON migration.id = assignment.active_migration_id
WHERE assignment.train_run_id = $1
  AND assignment.shard_id = $2
  AND assignment.assignment_generation = $3`,
		expected.TrainRunID(), expected.ShardID().String(), expected.Generation().Int64()).Scan(
		&migrationID, &sourceShard, &targetShard, &sourceGeneration, &targetGeneration,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return control.ErrActiveRouteMismatch
	}
	if err != nil {
		return ErrPersistence
	}
	reason := ""
	switch {
	case expected.ShardID().String() == sourceShard && expected.Generation().Int64() == sourceGeneration &&
		next.ShardID().String() == targetShard && next.Generation().Int64() == targetGeneration:
		reason = "shard_cutover"
	case expected.ShardID().String() == targetShard && expected.Generation().Int64() == targetGeneration &&
		next.ShardID().String() == sourceShard && next.Generation().Int64() > targetGeneration:
		reason = "shard_rollback"
	default:
		return control.ErrActiveRouteMismatch
	}

	var stale bool
	err = tx.tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM public.reservation_shard_locators
    WHERE train_run_id = $1 AND (shard_id <> $2 OR assignment_generation <> $3)
    UNION ALL
    SELECT 1 FROM public.ticket_order_shard_locators
    WHERE train_run_id = $1 AND (shard_id <> $2 OR assignment_generation <> $3)
    UNION ALL
    SELECT 1 FROM public.ticket_shard_locators
    WHERE train_run_id = $1 AND (shard_id <> $2 OR assignment_generation <> $3)
    UNION ALL
    SELECT 1 FROM public.booking_idempotency_key_claims
    WHERE train_run_id = $1 AND (shard_id <> $2 OR assignment_generation <> $3)
)`, expected.TrainRunID(), expected.ShardID().String(), expected.Generation().Int64()).Scan(&stale)
	if err != nil {
		return ErrPersistence
	}
	if stale {
		return control.ErrActiveRouteMismatch
	}

	for _, table := range []string{
		"reservation_shard_locators", "ticket_order_shard_locators", "ticket_shard_locators",
	} {
		query := `UPDATE public.` + table + `
SET shard_id = $4, assignment_generation = $5, updated_at = clock_timestamp()
WHERE train_run_id = $1 AND shard_id = $2 AND assignment_generation = $3`
		if _, err := tx.tx.Exec(ctx, query, expected.TrainRunID(), expected.ShardID().String(),
			expected.Generation().Int64(), next.ShardID().String(), next.Generation().Int64()); err != nil {
			return ErrPersistence
		}
	}
	if _, err := tx.tx.Exec(ctx, `
UPDATE public.booking_idempotency_key_claims
SET shard_id = $4,
    assignment_generation = $5,
    updated_at = clock_timestamp()
WHERE train_run_id = $1 AND shard_id = $2 AND assignment_generation = $3`,
		expected.TrainRunID(), expected.ShardID().String(), expected.Generation().Int64(),
		next.ShardID().String(), next.Generation().Int64()); err != nil {
		return ErrPersistence
	}

	commandTag, err := tx.tx.Exec(ctx, `
UPDATE public.train_run_shard_assignments
SET shard_id = $4,
    assignment_generation = $5,
    availability_generation = availability_generation + 1,
    updated_at = clock_timestamp()
WHERE train_run_id = $1
  AND shard_id = $2
  AND assignment_generation = $3
  AND active_migration_id = $6`,
		expected.TrainRunID(), expected.ShardID().String(), expected.Generation().Int64(),
		next.ShardID().String(), next.Generation().Int64(), migrationID)
	if err != nil {
		return ErrPersistence
	}
	if commandTag.RowsAffected() != 1 {
		return control.ErrActiveRouteMismatch
	}

	if reason == "shard_cutover" {
		commandTag, err = tx.tx.Exec(ctx, `
INSERT INTO public.train_run_generation_writes (
    train_run_id, assignment_generation, shard_id, migration_id,
    successful_write_count, first_successful_write_at, last_successful_write_at
) VALUES ($1, $2, $3, $4, 0, NULL, NULL)
ON CONFLICT (train_run_id, assignment_generation) DO NOTHING`,
			next.TrainRunID(), next.Generation().Int64(), next.ShardID().String(), migrationID)
		if err != nil {
			return ErrPersistence
		}
		if commandTag.RowsAffected() != 1 {
			return control.ErrTargetWriteEvidence
		}
	}

	commandTag, err = tx.tx.Exec(ctx, `
INSERT INTO public.outbox_events (
    aggregate_type, aggregate_id, event_type, payload,
    train_run_id, shard_id, assignment_generation
) VALUES (
    'train_run', $1::uuid, 'trainrun.updated',
    jsonb_build_object('trainRunId', $1::uuid::text, 'reason', $2::text),
    $1::uuid, $3, $4
)`, next.TrainRunID(), reason, next.ShardID().String(), next.Generation().Int64())
	if err != nil || commandTag.RowsAffected() != 1 {
		return ErrPersistence
	}
	return nil
}

func (tx *Transaction) HasDurableTargetWrites(ctx context.Context, target sharding.ShardRoute) (bool, error) {
	if tx == nil || tx.tx == nil || target.TrainRunID() == uuid.Nil || target.Generation().Int64() <= 0 {
		return false, control.ErrInvalidInput
	}
	if _, err := schemaForShard(target.ShardID()); err != nil {
		return false, control.ErrInvalidInput
	}
	var writes int64
	err := tx.tx.QueryRow(ctx, `
SELECT evidence.successful_write_count
FROM public.train_run_generation_writes AS evidence
JOIN public.train_run_shard_migrations AS migration
  ON migration.id = evidence.migration_id
 AND migration.train_run_id = evidence.train_run_id
 AND migration.target_shard_id = evidence.shard_id
 AND migration.target_generation = evidence.assignment_generation
WHERE evidence.train_run_id = $1
  AND evidence.shard_id = $2
  AND evidence.assignment_generation = $3
FOR UPDATE OF evidence`, target.TrainRunID(), target.ShardID().String(), target.Generation().Int64()).Scan(&writes)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, control.ErrTargetWriteEvidence
	}
	if err != nil || writes < 0 {
		return false, ErrPersistence
	}
	return writes > 0, nil
}
