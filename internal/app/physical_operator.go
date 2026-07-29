package app

import (
	"context"
	"errors"

	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type physicalTrainRunCancellationExecutor interface {
	CancelTrainRun(context.Context, uuid.UUID) error
}

// PhysicalTrainRunCancellation preserves mixed-pilot legacy routes. It only
// invokes the shard-local protocol when the authoritative control assignment
// explicitly names physical PostgreSQL storage.
type PhysicalTrainRunCancellation struct {
	control  controlRouteReader
	executor physicalTrainRunCancellationExecutor
}

func NewPhysicalTrainRunCancellation(control controlRouteReader, executor physicalTrainRunCancellationExecutor) (*PhysicalTrainRunCancellation, error) {
	if control == nil || executor == nil {
		return nil, bookingpostgres.ErrInvalidArgument
	}
	return &PhysicalTrainRunCancellation{control: control, executor: executor}, nil
}

func (cancellation *PhysicalTrainRunCancellation) CancelTrainRun(ctx context.Context, trainRunID uuid.UUID) error {
	if cancellation == nil || ctx == nil || trainRunID == uuid.Nil {
		return bookingpostgres.ErrInvalidArgument
	}
	var storageKind, assignmentState string
	err := cancellation.control.QueryRow(ctx, `
SELECT shard.storage_kind,assignment.assignment_state
FROM train_run_shard_assignments AS assignment
JOIN booking_shards AS shard ON shard.shard_id=assignment.shard_id
WHERE assignment.train_run_id=$1`, trainRunID).Scan(&storageKind, &assignmentState)
	if errors.Is(err, pgx.ErrNoRows) {
		return sharding.ErrShardUnavailable
	}
	if err != nil {
		return sharding.ErrShardUnavailable
	}
	if storageKind != "postgres" {
		if storageKind == "legacy_schema" || storageKind == "logical_schema" {
			return nil
		}
		return sharding.ErrShardUnavailable
	}
	if assignmentState != "stable" && assignmentState != "migrating" && assignmentState != "rollback_window" {
		return sharding.ErrWriteFenced
	}
	return cancellation.executor.CancelTrainRun(ctx, trainRunID)
}
