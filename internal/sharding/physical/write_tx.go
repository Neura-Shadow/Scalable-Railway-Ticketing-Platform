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
		generation       int64
		writeEnabled     bool
		fenceState       string
		snapshotBookable bool
		snapshotVersion  int64
		serviceStatus    string
	)
	if err := tx.QueryRow(ctx, `
SELECT fence.assignment_generation,
       fence.write_enabled,
       fence.state,
       snapshot.bookable,
       snapshot.source_version,
       snapshot.status
FROM train_run_write_fences AS fence
JOIN train_run_booking_snapshots AS snapshot
  ON snapshot.train_run_id = fence.train_run_id
 AND snapshot.assignment_generation = fence.assignment_generation
WHERE fence.train_run_id = $1
FOR UPDATE OF fence, snapshot`, route.TrainRunID()).Scan(
		&generation,
		&writeEnabled,
		&fenceState,
		&snapshotBookable,
		&snapshotVersion,
		&serviceStatus,
	); err != nil {
		return rollback(sharding.ErrShardUnavailable)
	}
	if generation != route.Generation().Int64() {
		return rollback(sharding.ErrAssignmentStale)
	}
	if !writeEnabled || fenceState != "active" || !snapshotBookable ||
		(serviceStatus != "scheduled" && serviceStatus != "boarding") {
		return rollback(sharding.ErrWriteFenced)
	}
	if expectedSnapshotVersion <= 0 || snapshotVersion != expectedSnapshotVersion {
		return rollback(sharding.ErrAssignmentStale)
	}
	return tx, nil
}
