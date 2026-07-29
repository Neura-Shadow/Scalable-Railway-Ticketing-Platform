// Package command coordinates durable control-plane booking intent with one
// independently committed physical-shard execution receipt.
package command

import (
	"bytes"
	"context"
	"errors"
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

const OperationCreateReservation Operation = "reservation.create"

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
}

type Result struct {
	CommandID     uuid.UUID
	ReservationID uuid.UUID
}

// ControlRepository makes reservation of command identity, quota lease, and
// pending directory one control transaction; Finalize is a second idempotent
// transaction after the shard receipt exists.
type ControlRepository interface {
	Reserve(context.Context, ReserveRequest) (Command, error)
	Finalize(context.Context, Command, Receipt) error
}

// ShardExecutor must atomically commit the receipt with the booking mutation.
// Re-executing one command and fingerprint returns the same receipt.
type ShardExecutor interface {
	Execute(context.Context, Command) (Receipt, error)
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
		return Result{}, ErrControlUnavailable
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
		return Result{}, ErrShardExecution
	}
	if receipt.Status != ReceiptCommitted || receipt.CommandID != bookingCommand.ID ||
		receipt.RequestFingerprint != bookingCommand.RequestFingerprint ||
		receipt.ResultResourceID != bookingCommand.ReservationID {
		return Result{}, ErrReceiptMismatch
	}
	if err := coordinator.control.Finalize(ctx, bookingCommand, receipt); err != nil {
		return Result{}, ErrFinalizationDeferred
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
	return left.FromStopIndex == right.FromStopIndex && left.ToStopIndex == right.ToStopIndex &&
		left.SeatClass == right.SeatClass && left.HoldExpiresAt.Equal(right.HoldExpiresAt) &&
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
