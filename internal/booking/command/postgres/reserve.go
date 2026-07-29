package postgres

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (repository *Repository) Reserve(ctx context.Context, request command.ReserveRequest) (command.Command, error) {
	if repository == nil || ctx == nil {
		return command.Command{}, ErrInvalidOptions
	}
	tx, err := repository.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return command.Command{}, ErrControlWrite
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var active bool
	if err := tx.QueryRow(ctx, `
SELECT active
FROM public.users
WHERE id = $1
FOR UPDATE`, request.OwnerUserID).Scan(&active); err != nil || !active {
		return command.Command{}, ErrControlWrite
	}

	existing, found, err := loadExistingCommand(ctx, tx, request)
	if err != nil {
		return command.Command{}, err
	}
	if found {
		if err := tx.Commit(ctx); err != nil {
			return command.Command{}, ErrControlWrite
		}
		return existing, nil
	}
	holdTTL := time.Until(request.Payload.HoldExpiresAt)
	if holdTTL <= 0 || holdTTL > repository.options.LeaseTTL {
		return command.Command{}, ErrInvalidPayload
	}
	if err := lockOwnedPassengers(ctx, tx, request); err != nil {
		return command.Command{}, err
	}

	route, err := loadPhysicalRoute(ctx, tx, request.TrainRunID)
	if err != nil {
		return command.Command{}, err
	}
	var activeHolds, activeTrainRunHolds, activePassengers int
	if err := tx.QueryRow(ctx, `
SELECT count(*)::integer,
       count(*) FILTER (WHERE train_run_id = $2)::integer,
       COALESCE(sum(passenger_count), 0)::integer
FROM public.booking_quota_leases
WHERE owner_user_id = $1
  AND state IN ('pending', 'active_hold', 'repair_required')`,
		request.OwnerUserID,
		request.TrainRunID,
	).Scan(&activeHolds, &activeTrainRunHolds, &activePassengers); err != nil {
		return command.Command{}, ErrControlWrite
	}
	if activeHolds+1 > repository.options.MaxActiveHoldsPerUser ||
		activeTrainRunHolds+1 > repository.options.MaxActiveHoldsPerTrainRun ||
		activePassengers+request.PassengerCount > repository.options.MaxActivePassengersPerUser {
		return command.Command{}, ErrQuotaExceeded
	}

	result := command.Command{
		ID:                 uuid.New(),
		Operation:          request.Operation,
		OwnerUserID:        request.OwnerUserID,
		TrainRunID:         request.TrainRunID,
		ReservationID:      uuid.New(),
		Route:              route,
		RequestFingerprint: request.RequestFingerprint,
		State:              command.StateReserved,
		Payload:            command.CloneCreateReservationPayload(request.Payload),
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.booking_commands (
    command_id, operation, owner_user_id, train_run_id, reservation_id,
    idempotency_key_hash, request_fingerprint, target_shard_id,
    assignment_generation, state
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'reserved')`,
		result.ID,
		result.Operation,
		result.OwnerUserID,
		result.TrainRunID,
		result.ReservationID,
		request.IdempotencyKeyHash[:],
		request.RequestFingerprint[:],
		result.Route.ShardID().String(),
		result.Route.Generation().Int64(),
	); err != nil {
		return command.Command{}, ErrControlWrite
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.booking_quota_leases (
    lease_id, command_id, owner_user_id, train_run_id, passenger_count,
    state, expires_at
) VALUES ($1, $2, $3, $4, $5, 'pending', $6)`,
		uuid.New(),
		result.ID,
		result.OwnerUserID,
		result.TrainRunID,
		request.PassengerCount,
		request.Payload.HoldExpiresAt,
	); err != nil {
		return command.Command{}, ErrControlWrite
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.reservation_directory (
    reservation_id, train_run_id, owner_user_id, command_id, state,
    last_known_shard_id, last_known_generation
) VALUES ($1, $2, $3, $4, 'pending', $5, $6)`,
		result.ReservationID,
		result.TrainRunID,
		result.OwnerUserID,
		result.ID,
		result.Route.ShardID().String(),
		result.Route.Generation().Int64(),
	); err != nil {
		return command.Command{}, ErrControlWrite
	}
	if err := tx.Commit(ctx); err != nil {
		return command.Command{}, ErrControlWrite
	}
	return result, nil
}

func lockOwnedPassengers(ctx context.Context, tx pgx.Tx, request command.ReserveRequest) error {
	if len(request.Payload.PassengerIDs) != request.PassengerCount || request.PassengerCount < 1 {
		return ErrPassengerOwnership
	}
	var owned int
	err := tx.QueryRow(ctx, `
SELECT count(*)::integer
FROM (
    SELECT id
    FROM public.passengers
    WHERE user_id = $1
      AND id = ANY($2::uuid[])
    ORDER BY id
    FOR UPDATE
) AS owned_passengers`, request.OwnerUserID, request.Payload.PassengerIDs).Scan(&owned)
	if err != nil {
		return ErrControlWrite
	}
	if owned != request.PassengerCount {
		return ErrPassengerOwnership
	}
	return nil
}

func loadExistingCommand(
	ctx context.Context,
	tx pgx.Tx,
	request command.ReserveRequest,
) (command.Command, bool, error) {
	var (
		result         command.Command
		rawShardID     string
		rawGeneration  int64
		rawFingerprint []byte
		rawState       string
		leaseExpiresAt time.Time
		passengerCount int
	)
	err := tx.QueryRow(ctx, `
SELECT command.command_id, command.operation, command.owner_user_id,
       command.train_run_id, command.reservation_id,
       command.target_shard_id, command.assignment_generation,
       command.request_fingerprint, command.state, lease.expires_at,
       lease.passenger_count
FROM public.booking_commands AS command
JOIN public.booking_quota_leases AS lease
  ON lease.command_id = command.command_id
WHERE command.owner_user_id = $1
  AND command.operation = $2
  AND command.idempotency_key_hash = $3
FOR UPDATE OF command, lease`, request.OwnerUserID, request.Operation, request.IdempotencyKeyHash[:]).Scan(
		&result.ID,
		&result.Operation,
		&result.OwnerUserID,
		&result.TrainRunID,
		&result.ReservationID,
		&rawShardID,
		&rawGeneration,
		&rawFingerprint,
		&rawState,
		&leaseExpiresAt,
		&passengerCount,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return command.Command{}, false, nil
	}
	if err != nil {
		return command.Command{}, false, ErrControlWrite
	}
	if result.TrainRunID != request.TrainRunID || passengerCount != request.PassengerCount ||
		len(rawFingerprint) != 32 ||
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
	result.Payload = command.CloneCreateReservationPayload(request.Payload)
	result.Payload.HoldExpiresAt = leaseExpiresAt
	return result, true, nil
}

func loadPhysicalRoute(ctx context.Context, tx pgx.Tx, trainRunID uuid.UUID) (sharding.ShardRoute, error) {
	var rawShardID string
	var rawGeneration int64
	err := tx.QueryRow(ctx, `
SELECT assignment.shard_id, assignment.assignment_generation
FROM public.train_run_shard_assignments AS assignment
JOIN public.booking_shards AS shard ON shard.shard_id = assignment.shard_id
WHERE assignment.train_run_id = $1
  AND assignment.assignment_state IN ('stable', 'rollback_window')
  AND shard.storage_kind = 'postgres'
  AND shard.enabled
  AND shard.write_enabled
  AND shard.health_state = 'healthy'
  AND shard.state = 'active'
  AND shard.protocol_version = 1
  AND shard.schema_version = 1
  AND shard.minimum_fencing_protocol_version <= $2`, trainRunID, sharding.SupportedFencingProtocolVersion).Scan(
		&rawShardID,
		&rawGeneration,
	)
	if err != nil {
		return sharding.ShardRoute{}, ErrRouteUnavailable
	}
	shardID, err := sharding.ParseShardID(rawShardID)
	if err != nil || (shardID != sharding.ShardPhysicalZero && shardID != sharding.ShardPhysicalOne) {
		return sharding.ShardRoute{}, ErrRouteUnavailable
	}
	generation, err := sharding.NewAssignmentGeneration(rawGeneration)
	if err != nil {
		return sharding.ShardRoute{}, ErrRouteUnavailable
	}
	route, err := sharding.NewShardRoute(trainRunID, shardID, generation)
	if err != nil {
		return sharding.ShardRoute{}, ErrRouteUnavailable
	}
	return route, nil
}
