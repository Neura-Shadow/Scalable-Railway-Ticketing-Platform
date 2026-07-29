package physical

import (
	"context"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/jackc/pgx/v5"
)

// BeginWrite establishes the only transaction shape allowed to mutate a
// physical booking shard. All authority checks are local to that PostgreSQL
// database and the locks remain held until the caller commits or rolls back.
func BeginWrite(
	ctx context.Context,
	handle Handle,
	route sharding.ShardRoute,
	expectedSnapshotVersion int64,
) (pgx.Tx, error) {
	if ctx == nil || handle.pool == nil {
		return nil, sharding.ErrShardUnavailable
	}
	if handle.shardID != route.ShardID() {
		return nil, sharding.ErrAssignmentStale
	}
	if !handle.writeEnabled {
		return nil, sharding.ErrWriteFenced
	}
	tx, err := handle.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, sharding.ErrShardUnavailable
	}
	rollback := func(result error) (pgx.Tx, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, result
	}

	var (
		generation      int64
		writeEnabled    bool
		assignmentEpoch int64
		snapshotActive  bool
		snapshotVersion int64
		bookingState    string
	)
	if err := tx.QueryRow(ctx, `
SELECT fence.generation,
       fence.write_enabled,
       fence.assignment_epoch,
       snapshot.active,
       snapshot.source_version,
       snapshot.booking_state
FROM train_run_write_fences AS fence
JOIN train_run_booking_snapshots AS snapshot
  ON snapshot.train_run_id = fence.train_run_id
WHERE fence.train_run_id = $1
FOR UPDATE OF fence, snapshot`, route.TrainRunID()).Scan(
		&generation,
		&writeEnabled,
		&assignmentEpoch,
		&snapshotActive,
		&snapshotVersion,
		&bookingState,
	); err != nil {
		return rollback(sharding.ErrShardUnavailable)
	}
	if generation != route.Generation().Int64() || assignmentEpoch != route.Generation().Int64() {
		return rollback(sharding.ErrAssignmentStale)
	}
	if !writeEnabled || !snapshotActive || (bookingState != "active" && bookingState != "stable") {
		return rollback(sharding.ErrWriteFenced)
	}
	if expectedSnapshotVersion <= 0 || snapshotVersion != expectedSnapshotVersion {
		return rollback(sharding.ErrAssignmentStale)
	}
	return tx, nil
}
