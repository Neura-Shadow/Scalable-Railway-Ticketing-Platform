// Package physical executes one durable booking command inside exactly one
// physical booking-shard transaction.
package physical

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidExecutor       = errors.New("invalid physical booking executor")
	ErrInvalidPayload        = errors.New("invalid physical booking payload")
	ErrFareUnavailable       = errors.New("physical booking fare unavailable")
	ErrInsufficientInventory = errors.New("physical booking inventory unavailable")
	ErrReservationExpired    = errors.New("physical booking reservation expired")
	ErrInvalidLifecycleState = errors.New("physical booking lifecycle state invalid")
	ErrShardPersistence      = errors.New("physical booking persistence failed")
)

const mutationSavepoint = "booking_command_mutation"

type durableRejection struct {
	cause error
}

func (rejection *durableRejection) Error() string { return rejection.cause.Error() }
func (rejection *durableRejection) Unwrap() error { return rejection.cause }

type RouteResolver interface {
	Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error)
}

type Options struct {
	MaxHoldTTL time.Duration
}

type Executor struct {
	router  RouteResolver
	options Options
	now     func() time.Time
}

func NewExecutor(router RouteResolver, options Options) (*Executor, error) {
	if nilResolver(router) || options.MaxHoldTTL <= 0 || options.MaxHoldTTL > 24*time.Hour {
		return nil, ErrInvalidExecutor
	}
	return &Executor{router: router, options: options, now: time.Now}, nil
}

func (executor *Executor) Execute(ctx context.Context, bookingCommand command.Command) (command.Receipt, error) {
	if executor == nil || ctx == nil || !executor.validCommand(bookingCommand) {
		return command.Receipt{}, ErrInvalidPayload
	}
	resolved, err := executor.router.Resolve(ctx, bookingCommand.TrainRunID, false)
	if err != nil {
		return command.Receipt{}, sharding.ErrShardUnavailable
	}
	if !sameRoute(resolved.Route, bookingCommand.Route) {
		return executor.refreshAndExecute(ctx, bookingCommand)
	}
	receipt, err := executor.executeOnce(ctx, bookingCommand, resolved)
	if errors.Is(err, sharding.ErrAssignmentStale) {
		return executor.refreshAndExecute(ctx, bookingCommand)
	}
	return receipt, err
}

func (executor *Executor) refreshAndExecute(ctx context.Context, bookingCommand command.Command) (command.Receipt, error) {
	resolved, err := executor.router.Resolve(ctx, bookingCommand.TrainRunID, true)
	if err != nil {
		return command.Receipt{}, sharding.ErrShardUnavailable
	}
	// Commands are never silently retargeted after the control transaction.
	// A changed assignment is repaired by the saga coordinator, not by a shard.
	if !sameRoute(resolved.Route, bookingCommand.Route) {
		return command.Receipt{}, sharding.ErrAssignmentStale
	}
	return executor.executeOnce(ctx, bookingCommand, resolved)
}

func (executor *Executor) executeOnce(
	ctx context.Context,
	bookingCommand command.Command,
	resolved shardphysical.Resolution,
) (command.Receipt, error) {
	if resolved.Handle.ShardID() != bookingCommand.Route.ShardID() || resolved.Handle.Pool() == nil {
		return command.Receipt{}, sharding.ErrShardUnavailable
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return command.Receipt{}, sharding.ErrShardUnavailable
	}
	rollback := func(result error) (command.Receipt, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return command.Receipt{}, result
	}

	if receipt, found, err := loadReceipt(ctx, tx, bookingCommand); err != nil {
		return rollback(err)
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return command.Receipt{}, ErrShardPersistence
		}
		return receipt, nil
	}
	if bookingCommand.Operation == command.OperationConfirmReservation ||
		bookingCommand.Operation == command.OperationCancelReservation {
		receipt, err := executor.executeLifecycleTx(ctx, tx, bookingCommand, resolved)
		if err != nil {
			var rejection *durableRejection
			if errors.As(err, &rejection) {
				if commitErr := tx.Commit(ctx); commitErr != nil {
					return command.Receipt{}, ErrShardPersistence
				}
				return command.Receipt{}, rejection.cause
			}
			return rollback(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return command.Receipt{}, ErrShardPersistence
		}
		return receipt, nil
	}
	now := executor.now()
	if !bookingCommand.Payload.HoldExpiresAt.After(now) ||
		bookingCommand.Payload.HoldExpiresAt.Sub(now) > executor.options.MaxHoldTTL {
		return rollback(ErrInvalidPayload)
	}
	if !resolved.Handle.WriteEnabled() {
		return rollback(sharding.ErrWriteFenced)
	}

	segmentCount, err := lockLocalAuthority(ctx, tx, bookingCommand)
	if err != nil {
		return rollback(err)
	}
	// The fence lock serializes cutover with command execution. Recheck after
	// taking it so a concurrently committed receipt is returned, never applied.
	if receipt, found, err := loadReceipt(ctx, tx, bookingCommand); err != nil {
		return rollback(err)
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return command.Receipt{}, ErrShardPersistence
		}
		return receipt, nil
	}

	if err := execOne(ctx, tx, `
INSERT INTO booking_command_receipts (
    id, command_id, train_run_id, assignment_generation, command_type,
    request_fingerprint, status
) VALUES ($1, $2, $3, $4, $5, $6, 'started')`,
		uuid.NewSHA1(bookingCommand.ID, []byte("command-receipt")), bookingCommand.ID,
		bookingCommand.TrainRunID, bookingCommand.Route.Generation().Int64(),
		bookingCommand.Operation, bookingCommand.RequestFingerprint[:],
	); err != nil {
		return rollback(err)
	}
	if err := beginMutationSavepoint(ctx, tx); err != nil {
		return rollback(err)
	}
	rejectOrRollback := func(result error) (command.Receipt, error) {
		code, permanent := rejectionCode(result)
		if !permanent {
			return rollback(result)
		}
		if err := rollbackMutationSavepoint(ctx, tx); err != nil {
			return rollback(err)
		}
		if err := persistRejectedReceipt(ctx, tx, bookingCommand.ID, code); err != nil {
			return rollback(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return command.Receipt{}, ErrShardPersistence
		}
		return command.Receipt{}, result
	}

	var (
		fareID     uuid.UUID
		fareAmount int64
		currency   string
	)
	err = tx.QueryRow(ctx, `
SELECT id, amount_minor, currency
FROM booking_fare_snapshots
WHERE train_run_id = $1
  AND assignment_generation = $2
  AND from_stop_index = $3
  AND to_stop_index = $4
  AND seat_class = $5
  AND active
ORDER BY source_version DESC, id
LIMIT 1
FOR SHARE`, bookingCommand.TrainRunID, bookingCommand.Route.Generation().Int64(),
		bookingCommand.Payload.FromStopIndex, bookingCommand.Payload.ToStopIndex,
		bookingCommand.Payload.SeatClass,
	).Scan(&fareID, &fareAmount, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectOrRollback(ErrFareUnavailable)
	}
	if err != nil || fareID == uuid.Nil || fareAmount < 0 || len(currency) != 3 {
		return rollback(ErrShardPersistence)
	}
	if fareAmount > math.MaxInt64/int64(len(bookingCommand.Payload.PassengerIDs)) {
		return rollback(ErrInvalidPayload)
	}
	totalAmount := fareAmount * int64(len(bookingCommand.Payload.PassengerIDs))

	mask, err := domain.NewSegmentMask(segmentCount, bookingCommand.Payload.FromStopIndex, bookingCommand.Payload.ToStopIndex)
	if err != nil {
		return rollback(ErrInvalidPayload)
	}
	seatIDs := make([]uuid.UUID, 0, len(bookingCommand.Payload.PassengerIDs))
	for range bookingCommand.Payload.PassengerIDs {
		var seatID uuid.UUID
		err := tx.QueryRow(ctx, `
WITH candidate AS MATERIALIZED (
    SELECT inventory.seat_id
    FROM seat_inventory AS inventory
    JOIN booking_seat_catalog AS seat
      ON seat.train_run_id = inventory.train_run_id
     AND seat.assignment_generation = inventory.assignment_generation
     AND seat.seat_id = inventory.seat_id
    WHERE inventory.train_run_id = $1
      AND inventory.assignment_generation = $2
      AND inventory.seat_class = $3
      AND seat.active
      AND CASE
            WHEN bit_length(inventory.occupied_segments) = $4
            THEN (inventory.occupied_segments & $5::bit varying)
                 = repeat('0', $4)::bit varying
            ELSE false
          END
    ORDER BY seat.coach_order, seat.seat_order, seat.seat_id
    FOR UPDATE OF inventory SKIP LOCKED
    LIMIT 1
), updated AS (
    UPDATE seat_inventory AS inventory
    SET occupied_segments = inventory.occupied_segments | $5::bit varying,
        version = inventory.version + 1
    FROM candidate
    WHERE inventory.train_run_id = $1
      AND inventory.assignment_generation = $2
      AND inventory.seat_id = candidate.seat_id
    RETURNING inventory.seat_id
)
SELECT seat_id FROM updated`, bookingCommand.TrainRunID, bookingCommand.Route.Generation().Int64(),
			bookingCommand.Payload.SeatClass, segmentCount, mask.String()).Scan(&seatID)
		if errors.Is(err, pgx.ErrNoRows) {
			return rejectOrRollback(ErrInsufficientInventory)
		}
		if err != nil || seatID == uuid.Nil {
			return rollback(ErrShardPersistence)
		}
		seatIDs = append(seatIDs, seatID)
	}

	if err := execOne(ctx, tx, `
INSERT INTO reservations (
    id, user_id, train_run_id, assignment_generation, segment_count,
    from_stop_index, to_stop_index, seat_class, status, expires_at,
    total_amount_minor, currency
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'held', $9, $10, $11)`,
		bookingCommand.ReservationID, bookingCommand.OwnerUserID, bookingCommand.TrainRunID,
		bookingCommand.Route.Generation().Int64(), segmentCount, bookingCommand.Payload.FromStopIndex,
		bookingCommand.Payload.ToStopIndex, bookingCommand.Payload.SeatClass,
		bookingCommand.Payload.HoldExpiresAt, totalAmount, currency,
	); err != nil {
		return rollback(err)
	}
	for index, passengerID := range bookingCommand.Payload.PassengerIDs {
		if err := execOne(ctx, tx, `
INSERT INTO reservation_seats (
    id, reservation_id, train_run_id, assignment_generation, segment_count,
    seat_id, passenger_id, fare_snapshot_id, segment_mask,
    fare_amount_minor, currency
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::bit varying, $10, $11)`,
			uuid.NewSHA1(bookingCommand.ReservationID, []byte("passenger:"+passengerID.String())),
			bookingCommand.ReservationID, bookingCommand.TrainRunID,
			bookingCommand.Route.Generation().Int64(), segmentCount, seatIDs[index], passengerID,
			fareID, mask.String(), fareAmount, currency,
		); err != nil {
			return rollback(err)
		}
	}

	payload, err := json.Marshal(map[string]any{
		"command_id": bookingCommand.ID, "reservation_id": bookingCommand.ReservationID,
		"train_run_id":          bookingCommand.TrainRunID,
		"assignment_generation": bookingCommand.Route.Generation().Int64(), "status": "held",
	})
	if err != nil {
		return rollback(ErrShardPersistence)
	}
	if err := execOne(ctx, tx, `
INSERT INTO outbox_events (
    id, train_run_id, assignment_generation, aggregate_type, aggregate_id,
    event_type, payload
) VALUES ($1, $2, $3, 'reservation', $4, 'reservation.held', $5::jsonb)`,
		uuid.NewSHA1(bookingCommand.ID, []byte("reservation-held")), bookingCommand.TrainRunID,
		bookingCommand.Route.Generation().Int64(), bookingCommand.ReservationID, string(payload),
	); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `
INSERT INTO train_run_target_write_evidence (
    id, train_run_id, assignment_generation, successful_write_count,
    first_successful_write_at, last_successful_write_at, last_command_id
) VALUES ($1, $2, $3, 1, clock_timestamp(), clock_timestamp(), $4)
ON CONFLICT (train_run_id, assignment_generation) DO UPDATE
SET successful_write_count = train_run_target_write_evidence.successful_write_count + 1,
    first_successful_write_at = COALESCE(
        train_run_target_write_evidence.first_successful_write_at,
        EXCLUDED.first_successful_write_at
    ),
    last_successful_write_at = EXCLUDED.last_successful_write_at,
    last_command_id = EXCLUDED.last_command_id`,
		uuid.NewSHA1(bookingCommand.TrainRunID, []byte(
			"target-write-evidence:"+bookingCommand.Route.ShardID().String()+":"+
				strconv.FormatInt(bookingCommand.Route.Generation().Int64(), 10),
		)),
		bookingCommand.TrainRunID, bookingCommand.Route.Generation().Int64(), bookingCommand.ID,
	); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `
UPDATE booking_command_receipts
SET status = 'succeeded',
    result_type = 'reservation',
    result_id = $2,
    completed_at = clock_timestamp()
WHERE command_id = $1
  AND status = 'started'`, bookingCommand.ID, bookingCommand.ReservationID); err != nil {
		return rollback(err)
	}
	if err := releaseMutationSavepoint(ctx, tx); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return command.Receipt{}, ErrShardPersistence
	}
	return committedReceipt(bookingCommand), nil
}

func loadReceipt(ctx context.Context, tx pgx.Tx, bookingCommand command.Command) (command.Receipt, bool, error) {
	var (
		fingerprint []byte
		resourceID  uuid.UUID
		status      string
		errorCode   string
	)
	err := tx.QueryRow(ctx, `
SELECT request_fingerprint,
       COALESCE(result_id, '00000000-0000-0000-0000-000000000000'::uuid),
       status, COALESCE(error_code, '')
FROM booking_command_receipts
WHERE command_id = $1`, bookingCommand.ID).Scan(&fingerprint, &resourceID, &status, &errorCode)
	if errors.Is(err, pgx.ErrNoRows) {
		return command.Receipt{}, false, nil
	}
	if err != nil {
		return command.Receipt{}, false, ErrShardPersistence
	}
	if len(fingerprint) != 32 {
		return command.Receipt{}, false, command.ErrReceiptMismatch
	}
	var stored [32]byte
	copy(stored[:], fingerprint)
	if stored != bookingCommand.RequestFingerprint {
		return command.Receipt{}, false, command.ErrReceiptMismatch
	}
	if status == "rejected" {
		if resourceID != uuid.Nil {
			return command.Receipt{}, false, command.ErrReceiptMismatch
		}
		if rejection, ok := rejectionFromCode(errorCode); ok {
			return command.Receipt{}, false, rejection
		}
		return command.Receipt{}, false, command.ErrReceiptMismatch
	}
	if status != "succeeded" || resourceID != bookingCommand.ReservationID || errorCode != "" {
		return command.Receipt{}, false, command.ErrReceiptMismatch
	}
	receipt := committedReceipt(bookingCommand)
	switch bookingCommand.Operation {
	case command.OperationConfirmReservation:
		if err := tx.QueryRow(ctx, `
SELECT ticket_order.id, count(ticket.id)::integer,
       ticket_order.total_amount_minor,ticket_order.currency,ticket_order.created_at
FROM ticket_orders AS ticket_order
JOIN tickets AS ticket ON ticket.ticket_order_id=ticket_order.id
WHERE ticket_order.reservation_id=$1
GROUP BY ticket_order.id`, bookingCommand.ReservationID).Scan(
			&receipt.TicketOrderID, &receipt.TicketCount, &receipt.TotalAmountMinor,
			&receipt.Currency, &receipt.OrderCreatedAt); err != nil ||
			receipt.TicketOrderID == uuid.Nil || receipt.TicketCount < 1 || receipt.TotalAmountMinor < 0 ||
			receipt.TicketCount > command.MaxReceiptTickets || len(receipt.Currency) != 3 || receipt.OrderCreatedAt.IsZero() {
			return command.Receipt{}, false, ErrShardPersistence
		}
		receipt.TicketIDs, receipt.TicketCodes, err = loadOrderTicketIdentities(ctx, tx, receipt.TicketOrderID, receipt.TicketCount)
		if err != nil {
			return command.Receipt{}, false, err
		}
	case command.OperationCancelReservation:
		if err := tx.QueryRow(ctx, `SELECT count(*)::integer FROM reservation_seats WHERE reservation_id=$1`,
			bookingCommand.ReservationID).Scan(&receipt.ReleasedSeatCount); err != nil || receipt.ReleasedSeatCount < 0 {
			return command.Receipt{}, false, ErrShardPersistence
		}
	}
	return receipt, true, nil
}

func beginMutationSavepoint(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, "SAVEPOINT "+mutationSavepoint); err != nil {
		return ErrShardPersistence
	}
	return nil
}

func rollbackMutationSavepoint(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+mutationSavepoint); err != nil {
		return ErrShardPersistence
	}
	return releaseMutationSavepoint(ctx, tx)
}

func releaseMutationSavepoint(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+mutationSavepoint); err != nil {
		return ErrShardPersistence
	}
	return nil
}

func persistRejectedReceipt(ctx context.Context, tx pgx.Tx, commandID uuid.UUID, code string) error {
	return execOne(ctx, tx, `
UPDATE booking_command_receipts
SET status='rejected', error_code=$2, completed_at=clock_timestamp()
WHERE command_id=$1 AND status='started'`, commandID, code)
}

func rejectionCode(err error) (string, bool) {
	switch {
	case errors.Is(err, ErrFareUnavailable):
		return "fare_unavailable", true
	case errors.Is(err, ErrInsufficientInventory):
		return "inventory_unavailable", true
	case errors.Is(err, ErrReservationExpired):
		return "reservation_expired", true
	case errors.Is(err, ErrInvalidLifecycleState):
		return "invalid_lifecycle_state", true
	default:
		return "", false
	}
}

func rejectionFromCode(code string) (error, bool) {
	switch code {
	case "fare_unavailable":
		return ErrFareUnavailable, true
	case "inventory_unavailable":
		return ErrInsufficientInventory, true
	case "reservation_expired":
		return ErrReservationExpired, true
	case "invalid_lifecycle_state":
		return ErrInvalidLifecycleState, true
	default:
		return nil, false
	}
}

func lockLocalAuthority(ctx context.Context, tx pgx.Tx, bookingCommand command.Command) (int, error) {
	var (
		generation      int64
		writeEnabled    bool
		fenceState      string
		bookable        bool
		snapshotVersion int64
		serviceStatus   string
		segmentCount    int
	)
	err := tx.QueryRow(ctx, `
SELECT fence.assignment_generation,
       fence.write_enabled,
       fence.state,
       snapshot.bookable,
       snapshot.source_version,
       snapshot.status,
       snapshot.segment_count
FROM train_run_write_fences AS fence
JOIN train_run_booking_snapshots AS snapshot
  ON snapshot.train_run_id = fence.train_run_id
 AND snapshot.assignment_generation = fence.assignment_generation
WHERE fence.train_run_id = $1
FOR UPDATE OF fence, snapshot`, bookingCommand.TrainRunID).Scan(
		&generation, &writeEnabled, &fenceState, &bookable, &snapshotVersion, &serviceStatus, &segmentCount,
	)
	if err != nil {
		return 0, sharding.ErrShardUnavailable
	}
	if generation != bookingCommand.Route.Generation().Int64() ||
		snapshotVersion != bookingCommand.Payload.ExpectedSnapshotVersion {
		return 0, sharding.ErrAssignmentStale
	}
	if !writeEnabled || fenceState != "active" || !bookable ||
		(serviceStatus != "scheduled" && serviceStatus != "boarding") {
		return 0, sharding.ErrWriteFenced
	}
	if segmentCount <= 0 || bookingCommand.Payload.ToStopIndex > segmentCount {
		return 0, ErrInvalidPayload
	}
	return segmentCount, nil
}

func execOne(ctx context.Context, tx pgx.Tx, query string, args ...any) error {
	tag, err := tx.Exec(ctx, query, args...)
	if err != nil || tag.RowsAffected() != 1 {
		return ErrShardPersistence
	}
	return nil
}

func (executor *Executor) validCommand(bookingCommand command.Command) bool {
	payload := bookingCommand.Payload
	if bookingCommand.ID == uuid.Nil ||
		bookingCommand.OwnerUserID == uuid.Nil || bookingCommand.TrainRunID == uuid.Nil ||
		bookingCommand.ReservationID == uuid.Nil || bookingCommand.RequestFingerprint == [32]byte{} ||
		bookingCommand.Route.TrainRunID() != bookingCommand.TrainRunID ||
		bookingCommand.Route.Generation().Int64() <= 0 {
		return false
	}
	if bookingCommand.Operation == command.OperationConfirmReservation ||
		bookingCommand.Operation == command.OperationCancelReservation {
		return bookingCommand.Payload.FromStopIndex == 0 && bookingCommand.Payload.ToStopIndex == 0 &&
			bookingCommand.Payload.SeatClass == "" && len(bookingCommand.Payload.PassengerIDs) == 0 &&
			bookingCommand.Payload.HoldExpiresAt.IsZero() && bookingCommand.Payload.ExpectedSnapshotVersion == 0
	}
	if bookingCommand.Operation != command.OperationCreateReservation {
		return false
	}
	if len(payload.PassengerIDs) < 1 || len(payload.PassengerIDs) > 6 ||
		payload.FromStopIndex < 0 || payload.ToStopIndex <= payload.FromStopIndex ||
		(payload.SeatClass != "standard" && payload.SeatClass != "business" && payload.SeatClass != "first") ||
		payload.ExpectedSnapshotVersion <= 0 {
		return false
	}
	if payload.HoldExpiresAt.IsZero() {
		return false
	}
	seen := make(map[uuid.UUID]struct{}, len(payload.PassengerIDs))
	for _, passengerID := range payload.PassengerIDs {
		if passengerID == uuid.Nil {
			return false
		}
		if _, duplicate := seen[passengerID]; duplicate {
			return false
		}
		seen[passengerID] = struct{}{}
	}
	return true
}

func committedReceipt(bookingCommand command.Command) command.Receipt {
	return command.Receipt{
		CommandID: bookingCommand.ID, RequestFingerprint: bookingCommand.RequestFingerprint,
		ResultResourceID: bookingCommand.ReservationID, Status: command.ReceiptCommitted,
	}
}

func sameRoute(left, right sharding.ShardRoute) bool {
	return left.TrainRunID() == right.TrainRunID() && left.ShardID() == right.ShardID() &&
		left.Generation() == right.Generation()
}

func nilResolver(value RouteResolver) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}
