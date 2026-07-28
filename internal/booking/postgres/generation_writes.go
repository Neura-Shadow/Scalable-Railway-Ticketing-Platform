package postgres

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// recordSuccessfulGenerationWrite makes direct rollback conservative. It is
// called only for successful non-replay routed mutations and runs inside the
// same fenced transaction as the booking change. Stable routes without an
// active migration do not need rollback-window evidence.
func (tx *Tx) recordSuccessfulGenerationWrite(ctx context.Context) error {
	if tx == nil || tx.tx == nil {
		return ErrInvalidArgument
	}
	if tx.routed == nil {
		return nil
	}
	route := tx.route
	if route.TrainRunID() == [16]byte{} || route.Generation().Int64() <= 0 {
		return ErrPersistenceInvariant
	}

	var migrationID pgtype.UUID
	var assignmentState string
	err := tx.tx.QueryRow(ctx, `
SELECT active_migration_id, assignment_state
FROM public.train_run_shard_assignments
WHERE train_run_id = $1
  AND shard_id = $2
  AND assignment_generation = $3`,
		route.TrainRunID(), route.ShardID().String(), route.Generation().Int64(),
	).Scan(&migrationID, &assignmentState)
	if errors.Is(err, pgx.ErrNoRows) {
		return sharding.ErrAssignmentStale
	}
	if err != nil {
		return ErrPersistenceInvariant
	}
	if !migrationID.Valid {
		if assignmentState == "rollback_window" {
			return ErrPersistenceInvariant
		}
		return nil
	}

	tag, err := tx.tx.Exec(ctx, `
INSERT INTO public.train_run_generation_writes (
    train_run_id, assignment_generation, shard_id, migration_id,
    successful_write_count, first_successful_write_at, last_successful_write_at
)
VALUES ($1, $2, $3, $4, 1, clock_timestamp(), clock_timestamp())
ON CONFLICT (train_run_id, assignment_generation) DO UPDATE
SET successful_write_count = public.train_run_generation_writes.successful_write_count + 1,
    first_successful_write_at = COALESCE(
        public.train_run_generation_writes.first_successful_write_at,
        EXCLUDED.first_successful_write_at
    ),
    last_successful_write_at = EXCLUDED.last_successful_write_at
WHERE public.train_run_generation_writes.shard_id = EXCLUDED.shard_id
  AND public.train_run_generation_writes.migration_id = EXCLUDED.migration_id`,
		route.TrainRunID(), route.Generation().Int64(), route.ShardID().String(), migrationID.Bytes,
	)
	if err != nil || tag.RowsAffected() != 1 {
		return ErrPersistenceInvariant
	}
	return nil
}
