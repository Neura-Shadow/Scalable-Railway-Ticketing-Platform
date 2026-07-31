// Package postgres persists the control half of durable operator booking
// commands. It never opens a physical-shard connection.
package postgres

import (
	"bytes"
	"context"
	"errors"
	"reflect"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrControlStore        = errors.New("operator command control store failed")
	ErrIdempotencyConflict = errors.New("operator command idempotency conflict")
	ErrRouteUnavailable    = errors.New("operator command route unavailable")
)

type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type Store struct{ db DB }

func NewStore(db DB) (*Store, error) {
	if nilInterface(db) {
		return nil, ErrControlStore
	}
	return &Store{db: db}, nil
}

func (store *Store) Reserve(ctx context.Context, request operatorcommand.ReserveRequest) (operatorcommand.Command, error) {
	if store == nil || ctx == nil || !operatorcommand.ValidReserveRequest(request) {
		return operatorcommand.Command{}, ErrControlStore
	}
	tx, err := store.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return operatorcommand.Command{}, ErrControlStore
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	command, found, err := loadByIdempotency(ctx, tx, request.ActorID, request.Operation, request.IdempotencyKeyHash)
	if err != nil {
		return operatorcommand.Command{}, err
	}
	if found {
		if !matchesRequest(command, request) {
			return operatorcommand.Command{}, ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return operatorcommand.Command{}, ErrControlStore
		}
		return command, nil
	}

	var rawShardID string
	var rawGeneration int64
	err = tx.QueryRow(ctx, `SELECT assignment.shard_id,assignment.assignment_generation
FROM public.train_run_shard_assignments AS assignment
JOIN public.booking_shards AS shard ON shard.shard_id=assignment.shard_id
LEFT JOIN public.physical_shard_migrations AS migration
  ON migration.migration_id=assignment.active_physical_migration_id
 AND migration.train_run_id=assignment.train_run_id
WHERE assignment.train_run_id=$1
  AND assignment.shard_id IN ('physical-shard-0','physical-shard-1')
  AND shard.storage_kind='postgres' AND shard.enabled AND shard.write_enabled
  AND shard.health_state='healthy' AND shard.state='active'
  AND (
    (assignment.assignment_state='stable'
      AND assignment.active_migration_id IS NULL
      AND assignment.active_physical_migration_id IS NULL)
    OR
    (assignment.assignment_state='rollback_window'
      AND assignment.active_migration_id IS NULL
      AND assignment.active_physical_migration_id IS NOT NULL
      AND migration.state='rollback_window'
      AND migration.target_shard_id=assignment.shard_id
      AND migration.target_generation=assignment.assignment_generation)
  )
FOR UPDATE OF assignment`, request.TrainRunID).Scan(&rawShardID, &rawGeneration)
	if err != nil {
		return operatorcommand.Command{}, ErrRouteUnavailable
	}
	route, err := newPhysicalRoute(request.TrainRunID, rawShardID, rawGeneration)
	if err != nil {
		return operatorcommand.Command{}, ErrRouteUnavailable
	}
	command = operatorcommand.Command{
		ID: uuid.New(), ActorID: request.ActorID, TrainRunID: request.TrainRunID,
		ResourceID: request.ResourceID, Operation: request.Operation,
		IdempotencyKeyHash: request.IdempotencyKeyHash, RequestFingerprint: request.RequestFingerprint,
		Route: route, ExpectedSourceVersion: request.ExpectedSourceVersion,
		ExpectedBookingPolicyVersion: request.ExpectedBookingPolicyVersion,
		FinalizePayload:              request.FinalizePayload, State: operatorcommand.StateReserved,
	}
	if request.Operation == operatorcommand.OperationFareInstall {
		var fareID uuid.UUID
		err := tx.QueryRow(ctx, `SELECT id FROM public.fares
WHERE id=$1 AND train_run_id=$2 AND active
  AND source_version=$3 AND from_stop_index=$4 AND to_stop_index=$5 AND seat_class=$6
FOR KEY SHARE`, request.ResourceID, request.TrainRunID, request.ExpectedSourceVersion,
			request.FinalizePayload.FromStopIndex, request.FinalizePayload.ToStopIndex,
			request.FinalizePayload.SeatClass).Scan(&fareID)
		if err != nil || fareID != request.ResourceID {
			return operatorcommand.Command{}, ErrControlStore
		}
	}
	insertArgs := []any{
		command.ID, command.ActorID, command.Operation, command.IdempotencyKeyHash[:],
		command.RequestFingerprint[:], command.TrainRunID, command.ResourceID,
		command.Route.ShardID().String(), command.Route.Generation().Int64(),
		command.ExpectedSourceVersion, nullablePositive(command.ExpectedBookingPolicyVersion),
	}
	insertArgs = append(insertArgs, finalizePayloadArgs(command.Operation, command.FinalizePayload)...)
	tag, err := tx.Exec(ctx, `INSERT INTO public.operator_booking_commands(
 command_id,actor_id,operation,idempotency_key_hash,request_fingerprint,
 train_run_id,resource_id,target_shard_id,assignment_generation,
 expected_source_version,expected_booking_policy_version,finalize_from_stop_index,
 finalize_to_stop_index,finalize_seat_class,finalize_amount_minor,
 finalize_currency,finalize_seat_active,state
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,'reserved')`, insertArgs...)
	if err != nil || tag.RowsAffected() != 1 {
		return operatorcommand.Command{}, ErrControlStore
	}
	if err := tx.Commit(ctx); err != nil {
		return operatorcommand.Command{}, ErrControlStore
	}
	return command, nil
}

func (store *Store) Claim(ctx context.Context, options operatorcommand.ClaimOptions) ([]operatorcommand.Candidate, error) {
	if store == nil || ctx == nil || !operatorcommand.ValidClaimOptions(options) {
		return nil, ErrControlStore
	}
	tx, err := store.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, ErrControlStore
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	rows, err := tx.Query(ctx, `WITH claimable AS (
 SELECT command_id FROM public.operator_booking_commands
 WHERE state IN ('reserved','committed_on_shard','needs_repair')
   AND (lease_until IS NULL OR lease_until<clock_timestamp())
 ORDER BY updated_at,command_id FOR UPDATE SKIP LOCKED LIMIT $1
), claimed AS (
 UPDATE public.operator_booking_commands AS command_row
 SET lease_owner=$2,lease_until=clock_timestamp()+($3::bigint*interval '1 millisecond'),
     attempt_count=attempt_count+1,updated_at=clock_timestamp()
 FROM claimable WHERE command_row.command_id=claimable.command_id
 RETURNING command_row.*
)
SELECT command_id,actor_id,operation,idempotency_key_hash,request_fingerprint,
 train_run_id,resource_id,target_shard_id,assignment_generation,
 expected_source_version,expected_booking_policy_version,finalize_from_stop_index,
 finalize_to_stop_index,finalize_seat_class,finalize_amount_minor,finalize_currency,
 finalize_seat_active,result_source_version,
 result_booking_policy_version,state,lease_owner,lease_until
FROM claimed ORDER BY updated_at,command_id`, options.BatchSize, options.WorkerID, options.LeaseTTL.Milliseconds())
	if err != nil {
		return nil, ErrControlStore
	}
	defer rows.Close()
	result := make([]operatorcommand.Candidate, 0, options.BatchSize)
	for rows.Next() {
		candidate, scanErr := scanCandidate(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, candidate)
	}
	if rows.Err() != nil {
		return nil, ErrControlStore
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, ErrControlStore
	}
	return result, nil
}

func (store *Store) Fail(ctx context.Context, request operatorcommand.FailureRequest) error {
	if store == nil || ctx == nil || !operatorcommand.ValidFailureRequest(request) {
		return ErrControlStore
	}
	tx, err := store.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ErrControlStore
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	command := request.Command
	tag, err := tx.Exec(ctx, `UPDATE public.operator_booking_commands
SET state='failed',result_source_version=NULL,result_booking_policy_version=NULL,
    lease_owner=NULL,lease_until=NULL,completed_at=clock_timestamp(),
    bounded_error_category=$12
WHERE command_id=$1 AND actor_id=$2 AND operation=$3
  AND idempotency_key_hash=$4 AND request_fingerprint=$5
  AND train_run_id=$6 AND resource_id=$7 AND target_shard_id=$8
  AND assignment_generation=$9 AND expected_source_version=$10
  AND expected_booking_policy_version IS NOT DISTINCT FROM $11
  AND state='reserved'
  AND lease_owner=$13 AND lease_until>=clock_timestamp()`,
		command.ID, command.ActorID, command.Operation, command.IdempotencyKeyHash[:],
		command.RequestFingerprint[:], command.TrainRunID, command.ResourceID,
		command.Route.ShardID().String(), command.Route.Generation().Int64(),
		command.ExpectedSourceVersion, nullablePositive(command.ExpectedBookingPolicyVersion),
		string(request.Category), request.LeaseOwner)
	if err != nil || tag.RowsAffected() != 1 {
		return ErrControlStore
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrControlStore
	}
	return nil
}

type rowScanner interface{ Scan(...any) error }

func loadByIdempotency(ctx context.Context, tx pgx.Tx, actorID uuid.UUID, operation operatorcommand.Operation, keyHash [32]byte) (operatorcommand.Command, bool, error) {
	row := tx.QueryRow(ctx, `SELECT command_id,actor_id,operation,idempotency_key_hash,request_fingerprint,
 train_run_id,resource_id,target_shard_id,assignment_generation,expected_source_version,
 expected_booking_policy_version,finalize_from_stop_index,finalize_to_stop_index,
 finalize_seat_class,finalize_amount_minor,finalize_currency,finalize_seat_active,
 result_source_version,result_booking_policy_version,state
FROM public.operator_booking_commands
WHERE actor_id=$1 AND operation=$2 AND idempotency_key_hash=$3 FOR UPDATE`, actorID, operation, keyHash[:])
	command, err := scanCommand(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return operatorcommand.Command{}, false, nil
	}
	if err != nil {
		return operatorcommand.Command{}, false, err
	}
	return command, true, nil
}

func scanCommand(row rowScanner) (operatorcommand.Command, error) {
	var command operatorcommand.Command
	var rawKeyHash, rawFingerprint []byte
	var rawShardID, rawState string
	var rawGeneration int64
	var expectedPolicy, resultSource, resultPolicy pgtype.Int8
	var fromStop, toStop pgtype.Int4
	var seatClass, currency pgtype.Text
	var amount pgtype.Int8
	var seatActive pgtype.Bool
	if err := row.Scan(&command.ID, &command.ActorID, &command.Operation, &rawKeyHash, &rawFingerprint,
		&command.TrainRunID, &command.ResourceID, &rawShardID, &rawGeneration,
		&command.ExpectedSourceVersion, &expectedPolicy, &fromStop, &toStop, &seatClass, &amount,
		&currency, &seatActive, &resultSource, &resultPolicy, &rawState); err != nil {
		return operatorcommand.Command{}, err
	}
	if len(rawKeyHash) != 32 || len(rawFingerprint) != 32 {
		return operatorcommand.Command{}, ErrControlStore
	}
	copy(command.IdempotencyKeyHash[:], rawKeyHash)
	copy(command.RequestFingerprint[:], rawFingerprint)
	command.ExpectedBookingPolicyVersion = int8Value(expectedPolicy)
	command.FinalizePayload = payloadValue(fromStop, toStop, seatClass, amount, currency, seatActive)
	command.ResultSourceVersion = int8Value(resultSource)
	command.ResultBookingPolicyVersion = int8Value(resultPolicy)
	command.State = operatorcommand.State(rawState)
	var err error
	command.Route, err = newPhysicalRoute(command.TrainRunID, rawShardID, rawGeneration)
	if err != nil {
		return operatorcommand.Command{}, ErrControlStore
	}
	return command, nil
}

func scanCandidate(row rowScanner) (operatorcommand.Candidate, error) {
	var candidate operatorcommand.Candidate
	var rawKeyHash, rawFingerprint []byte
	var rawShardID, rawState string
	var rawGeneration int64
	var expectedPolicy, resultSource, resultPolicy pgtype.Int8
	var fromStop, toStop pgtype.Int4
	var seatClass, currency pgtype.Text
	var amount pgtype.Int8
	var seatActive pgtype.Bool
	if err := row.Scan(&candidate.Command.ID, &candidate.Command.ActorID, &candidate.Command.Operation,
		&rawKeyHash, &rawFingerprint, &candidate.Command.TrainRunID, &candidate.Command.ResourceID,
		&rawShardID, &rawGeneration, &candidate.Command.ExpectedSourceVersion, &expectedPolicy,
		&fromStop, &toStop, &seatClass, &amount, &currency, &seatActive,
		&resultSource, &resultPolicy, &rawState, &candidate.LeaseOwner, &candidate.LeaseUntil); err != nil {
		return operatorcommand.Candidate{}, ErrControlStore
	}
	if len(rawKeyHash) != 32 || len(rawFingerprint) != 32 {
		return operatorcommand.Candidate{}, ErrControlStore
	}
	copy(candidate.Command.IdempotencyKeyHash[:], rawKeyHash)
	copy(candidate.Command.RequestFingerprint[:], rawFingerprint)
	candidate.Command.ExpectedBookingPolicyVersion = int8Value(expectedPolicy)
	candidate.Command.FinalizePayload = payloadValue(fromStop, toStop, seatClass, amount, currency, seatActive)
	candidate.Command.ResultSourceVersion = int8Value(resultSource)
	candidate.Command.ResultBookingPolicyVersion = int8Value(resultPolicy)
	candidate.Command.State = operatorcommand.State(rawState)
	var err error
	candidate.Command.Route, err = newPhysicalRoute(candidate.Command.TrainRunID, rawShardID, rawGeneration)
	if err != nil {
		return operatorcommand.Candidate{}, ErrControlStore
	}
	return candidate, nil
}

func newPhysicalRoute(trainRunID uuid.UUID, rawShardID string, rawGeneration int64) (sharding.ShardRoute, error) {
	shardID, err := sharding.ParseShardID(rawShardID)
	if err != nil || (shardID != sharding.ShardPhysicalZero && shardID != sharding.ShardPhysicalOne) {
		return sharding.ShardRoute{}, ErrRouteUnavailable
	}
	generation, err := sharding.NewAssignmentGeneration(rawGeneration)
	if err != nil {
		return sharding.ShardRoute{}, ErrRouteUnavailable
	}
	return sharding.NewShardRoute(trainRunID, shardID, generation)
}

func matchesRequest(command operatorcommand.Command, request operatorcommand.ReserveRequest) bool {
	return command.ActorID == request.ActorID && command.TrainRunID == request.TrainRunID &&
		command.ResourceID == request.ResourceID && command.Operation == request.Operation &&
		bytes.Equal(command.IdempotencyKeyHash[:], request.IdempotencyKeyHash[:]) &&
		bytes.Equal(command.RequestFingerprint[:], request.RequestFingerprint[:]) &&
		command.ExpectedSourceVersion == request.ExpectedSourceVersion &&
		command.ExpectedBookingPolicyVersion == request.ExpectedBookingPolicyVersion &&
		command.FinalizePayload == request.FinalizePayload
}

func finalizePayloadArgs(operation operatorcommand.Operation, payload operatorcommand.BoundedFinalizePayload) []any {
	switch operation {
	case operatorcommand.OperationFareInstall:
		return []any{payload.FromStopIndex, payload.ToStopIndex, payload.SeatClass,
			payload.AmountMinor, payload.Currency, nil}
	case operatorcommand.OperationSeatDisable, operatorcommand.OperationSeatEnable:
		return []any{nil, nil, nil, nil, nil, payload.SeatActive}
	default:
		return []any{nil, nil, nil, nil, nil, nil}
	}
}

func payloadValue(fromStop, toStop pgtype.Int4, seatClass pgtype.Text, amount pgtype.Int8, currency pgtype.Text, seatActive pgtype.Bool) operatorcommand.BoundedFinalizePayload {
	result := operatorcommand.BoundedFinalizePayload{}
	if fromStop.Valid {
		result.FromStopIndex = int(fromStop.Int32)
	}
	if toStop.Valid {
		result.ToStopIndex = int(toStop.Int32)
	}
	if seatClass.Valid {
		result.SeatClass = seatClass.String
	}
	if amount.Valid {
		result.AmountMinor = amount.Int64
	}
	if currency.Valid {
		result.Currency = currency.String
	}
	if seatActive.Valid {
		result.SeatActive = seatActive.Bool
	}
	return result
}

func nullablePositive(value int64) any {
	if value <= 0 {
		return nil
	}
	return value
}

func int8Value(value pgtype.Int8) int64 {
	if !value.Valid {
		return 0
	}
	return value.Int64
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

var _ operatorcommand.Store = (*Store)(nil)
