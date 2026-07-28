package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func schemaForShard(shard sharding.ShardID) (string, error) {
	switch shard {
	case sharding.ShardLegacy:
		return "public", nil
	case sharding.ShardZero:
		return "booking_shard_0", nil
	case sharding.ShardOne:
		return "booking_shard_1", nil
	default:
		return "", control.ErrInvalidInput
	}
}

func (tx *Transaction) ActiveRouteForUpdate(ctx context.Context, trainRunID uuid.UUID) (sharding.ShardRoute, error) {
	if tx == nil || tx.tx == nil || trainRunID == uuid.Nil {
		return sharding.ShardRoute{}, control.ErrInvalidInput
	}
	var rawShard string
	var rawGeneration int64
	err := tx.tx.QueryRow(ctx, `
SELECT shard_id, assignment_generation
FROM public.train_run_shard_assignments
WHERE train_run_id = $1
FOR UPDATE`, trainRunID).Scan(&rawShard, &rawGeneration)
	if errors.Is(err, pgx.ErrNoRows) {
		return sharding.ShardRoute{}, control.ErrActiveRouteMismatch
	}
	if err != nil {
		return sharding.ShardRoute{}, ErrPersistence
	}
	shard, err := sharding.ParseShardID(rawShard)
	if err != nil {
		return sharding.ShardRoute{}, control.ErrActiveRouteMismatch
	}
	generation, err := sharding.NewAssignmentGeneration(rawGeneration)
	if err != nil {
		return sharding.ShardRoute{}, control.ErrActiveRouteMismatch
	}
	route, err := sharding.NewShardRoute(trainRunID, shard, generation)
	if err != nil {
		return sharding.ShardRoute{}, control.ErrActiveRouteMismatch
	}
	return route, nil
}

func (tx *Transaction) RequireShardWritableForUpdate(ctx context.Context, shardID sharding.ShardID) error {
	if tx == nil || tx.tx == nil {
		return control.ErrInvalidInput
	}
	parsed, err := sharding.ParseShardID(shardID.String())
	if err != nil || parsed != shardID {
		return control.ErrInvalidInput
	}
	var enabled bool
	var writeEnabled bool
	var state string
	var minimumFencingProtocolVersion int32
	err = tx.tx.QueryRow(ctx, `
SELECT enabled, write_enabled, state, minimum_fencing_protocol_version
FROM public.booking_shards
WHERE shard_id = $1
FOR UPDATE`, shardID.String()).Scan(
		&enabled,
		&writeEnabled,
		&state,
		&minimumFencingProtocolVersion,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return control.ErrShardNotWritable
	}
	if err != nil {
		return ErrPersistence
	}
	if !enabled || !writeEnabled || state != "active" ||
		minimumFencingProtocolVersion <= 0 ||
		minimumFencingProtocolVersion > sharding.SupportedFencingProtocolVersion {
		return control.ErrShardNotWritable
	}
	return nil
}

func (tx *Transaction) WriteFenceEnabledForUpdate(ctx context.Context, route sharding.ShardRoute) (bool, error) {
	schema, err := schemaForShard(route.ShardID())
	if err != nil || route.TrainRunID() == uuid.Nil || route.Generation().Int64() <= 0 {
		return false, control.ErrInvalidInput
	}
	var generation int64
	var enabled bool
	query := fmt.Sprintf(`
SELECT assignment_generation, write_enabled
FROM %s.train_run_write_fences
WHERE train_run_id = $1
FOR UPDATE`, schema)
	err = tx.tx.QueryRow(ctx, query, route.TrainRunID()).Scan(&generation, &enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, control.ErrWriteFenceMismatch
	}
	if err != nil {
		return false, ErrPersistence
	}
	if generation != route.Generation().Int64() {
		return false, control.ErrWriteFenceMismatch
	}
	return enabled, nil
}

func (tx *Transaction) SetWriteFence(ctx context.Context, route sharding.ShardRoute, enabled bool) error {
	schema, err := schemaForShard(route.ShardID())
	if err != nil || route.TrainRunID() == uuid.Nil || route.Generation().Int64() <= 0 {
		return control.ErrInvalidInput
	}
	query := fmt.Sprintf(`
INSERT INTO %s.train_run_write_fences (
    train_run_id, assignment_generation, write_enabled
) VALUES ($1, $2, $3)
ON CONFLICT (train_run_id) DO UPDATE
SET assignment_generation = EXCLUDED.assignment_generation,
    write_enabled = EXCLUDED.write_enabled,
    updated_at = clock_timestamp()
WHERE %s.train_run_write_fences.assignment_generation <= EXCLUDED.assignment_generation`, schema, schema)
	commandTag, err := tx.tx.Exec(ctx, query, route.TrainRunID(), route.Generation().Int64(), enabled)
	if err != nil {
		return ErrPersistence
	}
	if commandTag.RowsAffected() != 1 {
		return control.ErrWriteFenceMismatch
	}
	return nil
}

// QuiesceWrites takes the local fence row lock. The service already owns the
// migration and assignment locks, preserving the global lock order.
func (tx *Transaction) QuiesceWrites(ctx context.Context, route sharding.ShardRoute) error {
	_, err := tx.WriteFenceEnabledForUpdate(ctx, route)
	return err
}
