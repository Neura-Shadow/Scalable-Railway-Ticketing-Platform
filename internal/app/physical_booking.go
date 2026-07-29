package app

import (
	"context"
	"errors"

	bookingcommand "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	commandphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/physical"
	commandpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/postgres"
	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type physicalCreateCoordinator interface {
	Execute(context.Context, bookingcommand.ReserveRequest) (bookingcommand.Result, error)
	ExecuteLifecycle(context.Context, bookingcommand.LifecycleRequest) (bookingcommand.Result, error)
}

type controlRouteReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// HybridReservationCommands preserves the legacy/logical path and opts into
// the cross-database saga only when the control assignment names a physical
// PostgreSQL shard. The decision comes from the control database, never Redis.
type HybridReservationCommands struct {
	control  controlRouteReader
	legacy   reservationCommands
	replays  completedCreateHoldReader
	physical physicalCreateCoordinator
	router   physicalRouteResolver
}

func NewHybridReservationCommands(
	control controlRouteReader,
	legacy reservationCommands,
	physical physicalCreateCoordinator,
	router physicalRouteResolver,
) (*HybridReservationCommands, error) {
	if control == nil || legacy == nil || physical == nil || router == nil {
		return nil, bookingpostgres.ErrInvalidArgument
	}
	replays, _ := legacy.(completedCreateHoldReader)
	return &HybridReservationCommands{
		control: control, legacy: legacy, replays: replays,
		physical: physical, router: router,
	}, nil
}

func (commands *HybridReservationCommands) CreateHold(
	ctx context.Context,
	params bookingpostgres.CreateHoldParams,
) (bookingpostgres.CreateHoldResult, error) {
	physical, err := commands.assignedPhysical(ctx, params.TrainRunID)
	if err != nil {
		return bookingpostgres.CreateHoldResult{}, err
	}
	if !physical {
		return commands.legacy.CreateHold(ctx, params)
	}
	snapshotVersion, err := commands.physicalSnapshotVersion(ctx, params.TrainRunID)
	if err != nil {
		return bookingpostgres.CreateHoldResult{}, err
	}
	keyHash, fingerprint, ok := commandHashes(params.IdempotencyKeyHash, params.RequestFingerprint)
	if !ok {
		return bookingpostgres.CreateHoldResult{}, bookingpostgres.ErrInvalidArgument
	}
	result, err := commands.physical.Execute(ctx, bookingcommand.ReserveRequest{
		OwnerUserID:        params.UserID,
		TrainRunID:         params.TrainRunID,
		Operation:          bookingcommand.OperationCreateReservation,
		IdempotencyKeyHash: keyHash,
		RequestFingerprint: fingerprint,
		PassengerCount:     len(params.PassengerIDs),
		Payload: bookingcommand.CreateReservationPayload{
			FromStopIndex:           params.FromStopIndex,
			ToStopIndex:             params.ToStopIndex,
			SeatClass:               params.SeatClass,
			PassengerIDs:            append([]uuid.UUID(nil), params.PassengerIDs...),
			HoldExpiresAt:           params.HoldExpiresAt,
			ExpectedSnapshotVersion: snapshotVersion,
		},
	})
	if err != nil {
		return bookingpostgres.CreateHoldResult{}, mapPhysicalCommandError(err)
	}
	return bookingpostgres.CreateHoldResult{
		ReservationID: result.ReservationID,
		SeatCount:     len(params.PassengerIDs),
	}, nil
}

func (commands *HybridReservationCommands) ConfirmReservation(
	ctx context.Context,
	params bookingpostgres.ReservationCommandParams,
) (bookingpostgres.ConfirmReservationResult, error) {
	physical, err := commands.reservationIsPhysical(ctx, params.ReservationID)
	if err != nil {
		return bookingpostgres.ConfirmReservationResult{}, err
	}
	if physical {
		keyHash, fingerprint, ok := commandHashes(params.IdempotencyKeyHash, params.RequestFingerprint)
		if !ok {
			return bookingpostgres.ConfirmReservationResult{}, bookingpostgres.ErrInvalidArgument
		}
		result, executeErr := commands.physical.ExecuteLifecycle(ctx, bookingcommand.LifecycleRequest{
			OwnerUserID: params.UserID, ReservationID: params.ReservationID,
			Operation:          bookingcommand.OperationConfirmReservation,
			IdempotencyKeyHash: keyHash, RequestFingerprint: fingerprint,
		})
		if executeErr != nil {
			return bookingpostgres.ConfirmReservationResult{}, mapPhysicalCommandError(executeErr)
		}
		return bookingpostgres.ConfirmReservationResult{
			ReservationID: result.ReservationID, TicketOrderID: result.TicketOrderID,
			TicketCount: result.TicketCount, Replayed: result.Replayed,
		}, nil
	}
	return commands.legacy.ConfirmReservation(ctx, params)
}

func (commands *HybridReservationCommands) CancelReservation(
	ctx context.Context,
	params bookingpostgres.ReservationCommandParams,
) (bookingpostgres.CancelReservationResult, error) {
	physical, err := commands.reservationIsPhysical(ctx, params.ReservationID)
	if err != nil {
		return bookingpostgres.CancelReservationResult{}, err
	}
	if physical {
		keyHash, fingerprint, ok := commandHashes(params.IdempotencyKeyHash, params.RequestFingerprint)
		if !ok {
			return bookingpostgres.CancelReservationResult{}, bookingpostgres.ErrInvalidArgument
		}
		result, executeErr := commands.physical.ExecuteLifecycle(ctx, bookingcommand.LifecycleRequest{
			OwnerUserID: params.UserID, ReservationID: params.ReservationID,
			Operation:          bookingcommand.OperationCancelReservation,
			IdempotencyKeyHash: keyHash, RequestFingerprint: fingerprint,
		})
		if executeErr != nil {
			return bookingpostgres.CancelReservationResult{}, mapPhysicalCommandError(executeErr)
		}
		return bookingpostgres.CancelReservationResult{
			ReservationID: result.ReservationID, ReleasedSeatCount: result.ReleasedSeats,
			Replayed: result.Replayed,
		}, nil
	}
	return commands.legacy.CancelReservation(ctx, params)
}

func (commands *HybridReservationCommands) LookupCompletedCreateHold(
	ctx context.Context,
	params bookingpostgres.CompletedCreateHoldLookupParams,
) (bookingpostgres.CreateHoldResult, bool, error) {
	physical, err := commands.assignedPhysical(ctx, params.TrainRunID)
	if err != nil {
		return bookingpostgres.CreateHoldResult{}, false, err
	}
	if !physical {
		if commands.replays == nil {
			return bookingpostgres.CreateHoldResult{}, false, nil
		}
		return commands.replays.LookupCompletedCreateHold(ctx, params)
	}
	if len(params.IdempotencyKeyHash) != 32 || len(params.RequestFingerprint) != 32 {
		return bookingpostgres.CreateHoldResult{}, false, bookingpostgres.ErrInvalidArgument
	}
	var reservationID uuid.UUID
	err = commands.control.QueryRow(ctx, `
SELECT reservation_id
FROM public.booking_commands
WHERE owner_user_id = $1
  AND train_run_id = $2
  AND operation = 'reservation.create'
  AND idempotency_key_hash = $3
  AND request_fingerprint = $4
  AND state = 'finalized'`, params.UserID, params.TrainRunID,
		params.IdempotencyKeyHash, params.RequestFingerprint).Scan(&reservationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return bookingpostgres.CreateHoldResult{}, false, nil
	}
	if err != nil || reservationID == uuid.Nil {
		return bookingpostgres.CreateHoldResult{}, false, sharding.ErrShardUnavailable
	}
	return bookingpostgres.CreateHoldResult{ReservationID: reservationID, Replayed: true}, true, nil
}

func (commands *HybridReservationCommands) assignedPhysical(ctx context.Context, trainRunID uuid.UUID) (bool, error) {
	if commands == nil || ctx == nil || trainRunID == uuid.Nil {
		return false, bookingpostgres.ErrInvalidArgument
	}
	var storageKind string
	err := commands.control.QueryRow(ctx, `
SELECT shard.storage_kind
FROM public.train_run_shard_assignments AS assignment
JOIN public.booking_shards AS shard ON shard.shard_id = assignment.shard_id
WHERE assignment.train_run_id = $1
  AND assignment.assignment_state IN ('stable', 'rollback_window')`, trainRunID).Scan(&storageKind)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, bookingpostgres.ErrNotFound
	}
	if err != nil {
		return false, sharding.ErrShardUnavailable
	}
	return storageKind == "postgres", nil
}

func (commands *HybridReservationCommands) physicalSnapshotVersion(ctx context.Context, trainRunID uuid.UUID) (int64, error) {
	resolved, err := commands.router.Resolve(ctx, trainRunID, false)
	if err != nil {
		return 0, sharding.ErrShardUnavailable
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, sharding.ErrShardUnavailable
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var snapshotVersion int64
	if err := tx.QueryRow(ctx, `
SELECT source_version
FROM public.train_run_booking_snapshots
WHERE train_run_id = $1
  AND assignment_generation = $2
  AND active
FOR SHARE`, trainRunID, resolved.Route.Generation().Int64()).Scan(&snapshotVersion); err != nil || snapshotVersion <= 0 {
		return 0, sharding.ErrShardUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, sharding.ErrShardUnavailable
	}
	return snapshotVersion, nil
}

func (commands *HybridReservationCommands) reservationIsPhysical(ctx context.Context, reservationID uuid.UUID) (bool, error) {
	if commands == nil || ctx == nil || reservationID == uuid.Nil {
		return false, bookingpostgres.ErrInvalidArgument
	}
	var storageKind string
	err := commands.control.QueryRow(ctx, `
SELECT shard.storage_kind
FROM public.reservation_directory AS directory
JOIN public.booking_shards AS shard
  ON shard.shard_id = directory.last_known_shard_id
WHERE directory.reservation_id = $1`, reservationID).Scan(&storageKind)
	if errors.Is(err, pgx.ErrNoRows) {
		// A version-8 reservation has no required directory before migration 9
		// backfill in older fixtures. Preserve the proven legacy resolver.
		return false, nil
	}
	if err != nil {
		return false, sharding.ErrShardUnavailable
	}
	return storageKind == "postgres", nil
}

func commandHashes(key, fingerprint []byte) ([32]byte, [32]byte, bool) {
	var keyHash, requestFingerprint [32]byte
	if len(key) != len(keyHash) || len(fingerprint) != len(requestFingerprint) {
		return keyHash, requestFingerprint, false
	}
	copy(keyHash[:], key)
	copy(requestFingerprint[:], fingerprint)
	return keyHash, requestFingerprint, true
}

func mapPhysicalCommandError(err error) error {
	switch {
	case errors.Is(err, commandpostgres.ErrQuotaExceeded):
		return bookingpostgres.ErrReservationQuotaExceeded
	case errors.Is(err, commandpostgres.ErrIdempotencyConflict):
		return bookingpostgres.ErrIdempotencyConflict
	case errors.Is(err, commandpostgres.ErrPassengerOwnership):
		return bookingpostgres.ErrNotFound
	case errors.Is(err, bookingcommand.ErrInvalidCommand):
		return bookingpostgres.ErrInvalidArgument
	case errors.Is(err, commandphysical.ErrInvalidPayload):
		return bookingpostgres.ErrInvalidState
	case errors.Is(err, sharding.ErrAssignmentStale), errors.Is(err, sharding.ErrWriteFenced):
		return err
	default:
		return sharding.ErrShardUnavailable
	}
}

var _ reservationCommands = (*HybridReservationCommands)(nil)
var _ completedCreateHoldReader = (*HybridReservationCommands)(nil)
