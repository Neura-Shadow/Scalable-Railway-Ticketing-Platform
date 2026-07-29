package postgres

import (
	"bytes"
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ReserveLifecycle durably records the control half of a confirm/cancel saga.
// The route is read from reservation_directory under lock, so a request can
// never be redirected using a client-provided or cache-only shard hint.
func (repository *Repository) ReserveLifecycle(ctx context.Context, request command.LifecycleRequest) (command.Command, error) {
	if repository == nil || ctx == nil {
		return command.Command{}, ErrInvalidOptions
	}
	tx, err := repository.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return command.Command{}, ErrControlWrite
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if existing, found, loadErr := loadExistingLifecycleCommand(ctx, tx, request); loadErr != nil {
		return command.Command{}, loadErr
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return command.Command{}, ErrControlWrite
		}
		return existing, nil
	}

	var trainRunID uuid.UUID
	var rawShardID, directoryState, storageKind string
	var rawGeneration int64
	err = tx.QueryRow(ctx, `
SELECT directory.train_run_id, directory.last_known_shard_id,
       directory.last_known_generation, directory.state, shard.storage_kind
FROM public.reservation_directory AS directory
JOIN public.booking_shards AS shard
  ON shard.shard_id = directory.last_known_shard_id
WHERE directory.reservation_id = $1
  AND directory.owner_user_id = $2
  AND shard.enabled
  AND shard.write_enabled
  AND shard.health_state = 'healthy'
  AND shard.state = 'active'
  AND shard.protocol_version = 1
  AND shard.schema_version = 1
  AND shard.minimum_fencing_protocol_version <= $3
FOR UPDATE OF directory`, request.ReservationID, request.OwnerUserID,
		sharding.SupportedFencingProtocolVersion).Scan(
		&trainRunID, &rawShardID, &rawGeneration, &directoryState, &storageKind,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return command.Command{}, ErrRouteUnavailable
	}
	if err != nil || directoryState != "active" || storageKind != "postgres" {
		return command.Command{}, ErrRouteUnavailable
	}
	shardID, err := sharding.ParseShardID(rawShardID)
	if err != nil || (shardID != sharding.ShardPhysicalZero && shardID != sharding.ShardPhysicalOne) {
		return command.Command{}, ErrRouteUnavailable
	}
	generation, err := sharding.NewAssignmentGeneration(rawGeneration)
	if err != nil {
		return command.Command{}, ErrRouteUnavailable
	}
	route, err := sharding.NewShardRoute(trainRunID, shardID, generation)
	if err != nil {
		return command.Command{}, ErrRouteUnavailable
	}
	result := command.Command{
		ID: uuid.New(), Operation: request.Operation, OwnerUserID: request.OwnerUserID,
		TrainRunID: trainRunID, ReservationID: request.ReservationID, Route: route,
		RequestFingerprint: request.RequestFingerprint, State: command.StateReserved,
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.booking_commands (
    command_id, operation, owner_user_id, train_run_id, reservation_id,
    idempotency_key_hash, request_fingerprint, target_shard_id,
    assignment_generation, state
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'reserved')`,
		result.ID, result.Operation, result.OwnerUserID, result.TrainRunID,
		result.ReservationID, request.IdempotencyKeyHash[:], request.RequestFingerprint[:],
		result.Route.ShardID().String(), result.Route.Generation().Int64()); err != nil {
		return command.Command{}, ErrControlWrite
	}
	if err := tx.Commit(ctx); err != nil {
		return command.Command{}, ErrControlWrite
	}
	return result, nil
}

func loadExistingLifecycleCommand(ctx context.Context, tx pgx.Tx, request command.LifecycleRequest) (command.Command, bool, error) {
	var result command.Command
	var rawShardID, rawState string
	var rawGeneration int64
	var rawFingerprint []byte
	err := tx.QueryRow(ctx, `
SELECT command_id, operation, owner_user_id, train_run_id, reservation_id,
       target_shard_id, assignment_generation, request_fingerprint, state
FROM public.booking_commands
WHERE owner_user_id=$1 AND operation=$2 AND idempotency_key_hash=$3
FOR UPDATE`, request.OwnerUserID, request.Operation, request.IdempotencyKeyHash[:]).Scan(
		&result.ID, &result.Operation, &result.OwnerUserID, &result.TrainRunID,
		&result.ReservationID, &rawShardID, &rawGeneration, &rawFingerprint, &rawState,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return command.Command{}, false, nil
	}
	if err != nil {
		return command.Command{}, false, ErrControlWrite
	}
	if result.ReservationID != request.ReservationID || len(rawFingerprint) != 32 ||
		!bytes.Equal(rawFingerprint, request.RequestFingerprint[:]) {
		return command.Command{}, false, ErrIdempotencyConflict
	}
	shardID, err := sharding.ParseShardID(rawShardID)
	if err != nil {
		return command.Command{}, false, ErrRouteUnavailable
	}
	generation, err := sharding.NewAssignmentGeneration(rawGeneration)
	if err != nil {
		return command.Command{}, false, ErrRouteUnavailable
	}
	result.Route, err = sharding.NewShardRoute(result.TrainRunID, shardID, generation)
	if err != nil {
		return command.Command{}, false, ErrRouteUnavailable
	}
	copy(result.RequestFingerprint[:], rawFingerprint)
	result.State = command.State(rawState)
	return result, true, nil
}
