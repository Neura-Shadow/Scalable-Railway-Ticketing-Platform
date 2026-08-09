// Package command coordinates durable control-plane booking intent with one
// independently committed physical-shard execution receipt.
package command

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

var (
	ErrInvalidCommand       = errors.New("invalid booking command")
	ErrControlUnavailable   = errors.New("booking control unavailable")
	ErrShardExecution       = errors.New("booking shard execution failed")
	ErrReceiptMismatch      = errors.New("booking command receipt mismatch")
	ErrFinalizationDeferred = errors.New("booking control finalization deferred")
)

type Operation string

const (
	OperationCreateReservation  Operation = "reservation.create"
	OperationConfirmReservation Operation = "reservation.confirm"
	OperationCancelReservation  Operation = "reservation.cancel"
)

type ReceiptStatus string

const (
	ReceiptCommitted ReceiptStatus = "committed"
	ReceiptFailed    ReceiptStatus = "failed"
)

type State string

const (
	StateReserved         State = "reserved"
	StateExecuting        State = "executing"
	StateCommittedOnShard State = "committed_on_shard"
	StateFinalized        State = "finalized"
	StateFailed           State = "failed"
	StateExpired          State = "expired"
	StateNeedsRepair      State = "needs_repair"
)

type ReserveRequest struct {
	OwnerUserID        uuid.UUID
	TrainRunID         uuid.UUID
	Operation          Operation
	IdempotencyKeyHash [32]byte
	RequestFingerprint [32]byte
	PassengerCount     int
	Payload            CreateReservationPayload
}

// LifecycleRequest binds a confirm or cancel command to the owner-visible
// reservation identity. The control repository resolves the immutable target
// route from reservation_directory; callers never supply a shard.
type LifecycleRequest struct {
	OwnerUserID        uuid.UUID
	ReservationID      uuid.UUID
	Operation          Operation
	IdempotencyKeyHash [32]byte
	RequestFingerprint [32]byte
}

// CreateReservationPayload is the immutable, fingerprint-bound input needed
// by one physical shard to execute a reservation command. Passenger profile
// data remains in the control database; only opaque passenger UUIDs cross the
// shard boundary.
type CreateReservationPayload struct {
	FromStopIndex           int
	ToStopIndex             int
	SeatClass               string
	PassengerIDs            []uuid.UUID
	HoldExpiresAt           time.Time
	ExpectedSnapshotVersion int64
}

type Command struct {
	ID                 uuid.UUID
	Operation          Operation
	OwnerUserID        uuid.UUID
	TrainRunID         uuid.UUID
	ReservationID      uuid.UUID
	Route              sharding.ShardRoute
	RequestFingerprint [32]byte
	State              State
	Payload            CreateReservationPayload
}

type Receipt struct {
	CommandID          uuid.UUID
	RequestFingerprint [32]byte
	ResultResourceID   uuid.UUID
	Status             ReceiptStatus
	TicketOrderID      uuid.UUID
	TicketCount        int
	TicketIDs          []uuid.UUID
	TicketCodes        []string
	ReleasedSeatCount  int
	TotalAmountMinor   int64
	Currency           string
	OrderCreatedAt     time.Time
}

// MaxReceiptTickets bounds cross-database receipt reconstruction and control
// locator writes. It matches the public admission passenger ceiling.
const MaxReceiptTickets = 100

type Result struct {
	CommandID     uuid.UUID
	ReservationID uuid.UUID
	TicketOrderID uuid.UUID
	TicketCount   int
	ReleasedSeats int
	Replayed      bool
}

// ControlRepository makes reservation of command identity, quota lease, and
// pending directory one control transaction; Finalize is a second idempotent
// transaction after the shard receipt exists.
type ControlRepository interface {
	Reserve(context.Context, ReserveRequest) (Command, error)
	ReserveLifecycle(context.Context, LifecycleRequest) (Command, error)
	Finalize(context.Context, Command, Receipt) error
}

// ShardExecutor must atomically commit the receipt with the booking mutation.
// Re-executing one command and fingerprint returns the same receipt.
type ShardExecutor interface {
	Execute(context.Context, Command) (Receipt, error)
}

// ExecuteLifecycle runs a confirm/cancel saga. Even a finalized control
// command is re-read from the shard receipt so the HTTP result can be rebuilt
// without making the control database authoritative for shard-local details.
func (coordinator *Coordinator) ExecuteLifecycle(ctx context.Context, request LifecycleRequest) (Result, error) {
	if coordinator == nil || ctx == nil || !validLifecycleRequest(request) {
		return Result{}, ErrInvalidCommand
	}
	bookingCommand, err := coordinator.control.ReserveLifecycle(ctx, request)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrControlUnavailable, err)
	}
	if !validLifecycleCommand(bookingCommand, request) {
		return Result{}, ErrInvalidCommand
	}
	if bookingCommand.State == StateFailed || bookingCommand.State == StateExpired {
		return Result{}, ErrShardExecution
	}
	replayed := bookingCommand.State == StateFinalized
	receipt, err := coordinator.shard.Execute(ctx, bookingCommand)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrShardExecution, err)
	}
	if receipt.Status != ReceiptCommitted || receipt.CommandID != bookingCommand.ID ||
		receipt.RequestFingerprint != bookingCommand.RequestFingerprint ||
		receipt.ResultResourceID != bookingCommand.ReservationID {
		return Result{}, ErrReceiptMismatch
	}
	if !replayed {
		if err := coordinator.control.Finalize(ctx, bookingCommand, receipt); err != nil {
			return Result{}, fmt.Errorf("%w: %w", ErrFinalizationDeferred, err)
		}
	}
	return Result{
		CommandID: bookingCommand.ID, ReservationID: receipt.ResultResourceID,
		TicketOrderID: receipt.TicketOrderID, TicketCount: receipt.TicketCount,
		ReleasedSeats: receipt.ReleasedSeatCount, Replayed: replayed,
	}, nil
}

func validLifecycleRequest(request LifecycleRequest) bool {
	return request.OwnerUserID != uuid.Nil && request.ReservationID != uuid.Nil &&
		(request.Operation == OperationConfirmReservation || request.Operation == OperationCancelReservation) &&
		request.IdempotencyKeyHash != [32]byte{} && request.RequestFingerprint != [32]byte{}
}

func validLifecycleCommand(bookingCommand Command, request LifecycleRequest) bool {
	return bookingCommand.ID != uuid.Nil && bookingCommand.OwnerUserID == request.OwnerUserID &&
		bookingCommand.ReservationID == request.ReservationID && bookingCommand.TrainRunID != uuid.Nil &&
		bookingCommand.Operation == request.Operation && bookingCommand.RequestFingerprint == request.RequestFingerprint &&
		bookingCommand.Route.TrainRunID() == bookingCommand.TrainRunID && bookingCommand.Route.Generation().Int64() > 0 &&
		(bookingCommand.State == StateReserved || bookingCommand.State == StateExecuting ||
			bookingCommand.State == StateCommittedOnShard || bookingCommand.State == StateFinalized ||
			bookingCommand.State == StateNeedsRepair)
}

type Coordinator struct {
	control ControlRepository
	shard   ShardExecutor
}

func NewCoordinator(control ControlRepository, shard ShardExecutor) (*Coordinator, error) {
	if nilInterface(control) || nilInterface(shard) {
		return nil, ErrInvalidCommand
	}
	return &Coordinator{control: control, shard: shard}, nil
}

func (coordinator *Coordinator) Execute(ctx context.Context, request ReserveRequest) (Result, error) {
	if coordinator == nil || ctx == nil || !validReserveRequest(request) {
		return Result{}, ErrInvalidCommand
	}
	bookingCommand, err := coordinator.control.Reserve(ctx, request)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrControlUnavailable, err)
	}
	if !validCommandForRequest(bookingCommand, request) {
		return Result{}, ErrInvalidCommand
	}
	if bookingCommand.State == StateFinalized {
		return Result{CommandID: bookingCommand.ID, ReservationID: bookingCommand.ReservationID}, nil
	}
	if bookingCommand.State == StateFailed || bookingCommand.State == StateExpired {
		return Result{}, ErrShardExecution
	}
	receipt, err := coordinator.shard.Execute(ctx, bookingCommand)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrShardExecution, err)
	}
	if receipt.Status != ReceiptCommitted || receipt.CommandID != bookingCommand.ID ||
		receipt.RequestFingerprint != bookingCommand.RequestFingerprint ||
		receipt.ResultResourceID != bookingCommand.ReservationID {
		return Result{}, ErrReceiptMismatch
	}
	if err := coordinator.control.Finalize(ctx, bookingCommand, receipt); err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrFinalizationDeferred, err)
	}
	return Result{CommandID: bookingCommand.ID, ReservationID: receipt.ResultResourceID}, nil
}

func validReserveRequest(request ReserveRequest) bool {
	return request.OwnerUserID != uuid.Nil && request.TrainRunID != uuid.Nil &&
		request.Operation == OperationCreateReservation && request.IdempotencyKeyHash != [32]byte{} &&
		request.RequestFingerprint != [32]byte{} && request.PassengerCount > 0 && request.PassengerCount <= 6 &&
		validCreateReservationPayload(request.Payload, request.PassengerCount)
}

func validCommandForRequest(command Command, request ReserveRequest) bool {
	return command.ID != uuid.Nil && command.ReservationID != uuid.Nil && command.OwnerUserID == request.OwnerUserID &&
		command.TrainRunID == request.TrainRunID && command.Operation == request.Operation &&
		command.RequestFingerprint == request.RequestFingerprint && command.Route.TrainRunID() == request.TrainRunID &&
		equalCreateReservationPayload(command.Payload, request.Payload) &&
		(command.State == StateReserved || command.State == StateExecuting || command.State == StateCommittedOnShard ||
			command.State == StateFinalized || command.State == StateNeedsRepair)
}

func validCreateReservationPayload(payload CreateReservationPayload, passengerCount int) bool {
	if payload.FromStopIndex < 0 || payload.ToStopIndex <= payload.FromStopIndex ||
		(payload.SeatClass != "standard" && payload.SeatClass != "business" && payload.SeatClass != "first") ||
		len(payload.PassengerIDs) != passengerCount || payload.HoldExpiresAt.IsZero() ||
		payload.ExpectedSnapshotVersion <= 0 {
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

func equalCreateReservationPayload(left, right CreateReservationPayload) bool {
	left = CloneCreateReservationPayload(left)
	right = CloneCreateReservationPayload(right)
	// Hold expiry is assigned by the API clock, not supplied by the customer or
	// included in the request fingerprint. Replays use the control lease's
	// stored expiry and must not conflict only because a retry observed a newer
	// server timestamp.
	return left.FromStopIndex == right.FromStopIndex && left.ToStopIndex == right.ToStopIndex &&
		left.SeatClass == right.SeatClass &&
		left.ExpectedSnapshotVersion == right.ExpectedSnapshotVersion &&
		reflect.DeepEqual(left.PassengerIDs, right.PassengerIDs)
}

// CloneCreateReservationPayload returns a detached canonical payload. Sorting
// opaque passenger IDs keeps seat assignment stable across equivalent retries.
func CloneCreateReservationPayload(payload CreateReservationPayload) CreateReservationPayload {
	payload.PassengerIDs = append([]uuid.UUID(nil), payload.PassengerIDs...)
	sort.Slice(payload.PassengerIDs, func(left, right int) bool {
		return bytes.Compare(payload.PassengerIDs[left][:], payload.PassengerIDs[right][:]) < 0
	})
	return payload
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
