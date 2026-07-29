// Package operatorcommand coordinates durable operator booking intent with an
// independently committed physical-shard receipt. It contains no projection
// SQL; the application finalizer owns the atomic control projection update.
package operatorcommand

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

var (
	ErrInvalidRequest       = errors.New("invalid operator booking command")
	ErrControlUnavailable   = errors.New("operator command control unavailable")
	ErrShardExecution       = errors.New("operator command shard execution failed")
	ErrShardUnreachable     = errors.New("operator command shard unreachable")
	ErrReceiptMismatch      = errors.New("operator command receipt mismatch")
	ErrFinalizationDeferred = errors.New("operator command finalization deferred")
	ErrInvalidOptions       = errors.New("invalid operator command recovery options")
)

type Operation string

const (
	OperationFareInstall       Operation = "fare.install"
	OperationSeatDisable       Operation = "seat.disable"
	OperationSeatEnable        Operation = "seat.enable"
	OperationBookingPolicyBump Operation = "booking_policy.bump"
)

type State string

const (
	StateReserved         State = "reserved"
	StateCommittedOnShard State = "committed_on_shard"
	StateNeedsRepair      State = "needs_repair"
	StateFinalized        State = "finalized"
	StateFailed           State = "failed"
)

type BoundedFinalizePayload struct {
	FromStopIndex int
	ToStopIndex   int
	SeatClass     string
	AmountMinor   int64
	Currency      string
	SeatActive    bool
}

type Mutation = BoundedFinalizePayload

// Request carries the bounded mutation needed by the shard executor. Reserve
// persists the fixed identities, hashes, versions, route, and only the bounded
// fields needed to recover a reserved command after a process crash.
type Request struct {
	ActorID                      uuid.UUID
	TrainRunID                   uuid.UUID
	ResourceID                   uuid.UUID
	Operation                    Operation
	IdempotencyKeyHash           [32]byte
	RequestFingerprint           [32]byte
	ExpectedSourceVersion        int64
	ExpectedBookingPolicyVersion int64
	Mutation                     Mutation
}

type ReserveRequest struct {
	ActorID                      uuid.UUID
	TrainRunID                   uuid.UUID
	ResourceID                   uuid.UUID
	Operation                    Operation
	IdempotencyKeyHash           [32]byte
	RequestFingerprint           [32]byte
	ExpectedSourceVersion        int64
	ExpectedBookingPolicyVersion int64
	FinalizePayload              BoundedFinalizePayload
}

type Command struct {
	ID                           uuid.UUID
	ActorID                      uuid.UUID
	TrainRunID                   uuid.UUID
	ResourceID                   uuid.UUID
	Operation                    Operation
	IdempotencyKeyHash           [32]byte
	RequestFingerprint           [32]byte
	Route                        sharding.ShardRoute
	ExpectedSourceVersion        int64
	ExpectedBookingPolicyVersion int64
	FinalizePayload              BoundedFinalizePayload
	ResultSourceVersion          int64
	ResultBookingPolicyVersion   int64
	State                        State
}

type Receipt struct {
	CommandID                  uuid.UUID
	TrainRunID                 uuid.UUID
	ResourceID                 uuid.UUID
	Operation                  Operation
	RequestFingerprint         [32]byte
	HistoricalShardID          sharding.ShardID
	HistoricalGeneration       int64
	ResultSourceVersion        int64
	ResultBookingPolicyVersion int64
}

type Result struct {
	CommandID            uuid.UUID
	ResourceID           uuid.UUID
	SourceVersion        int64
	BookingPolicyVersion int64
	Replayed             bool
}

type Candidate struct {
	Command    Command
	LeaseOwner string
	LeaseUntil time.Time
}

type ClaimOptions struct {
	WorkerID  string
	BatchSize int
	LeaseTTL  time.Duration
}

type FailureCategory string

const FailureShardRejected FailureCategory = "shard_rejected"

// FailureRequest terminalizes a command only when its immutable identity and
// mandatory recovery lease still match the durable control row.
type FailureRequest struct {
	Command    Command
	Category   FailureCategory
	LeaseOwner string
}

type Store interface {
	Reserve(context.Context, ReserveRequest) (Command, error)
	Claim(context.Context, ClaimOptions) ([]Candidate, error)
	Fail(context.Context, FailureRequest) error
}

type ShardExecutor interface {
	Execute(context.Context, Command, Mutation) (Receipt, error)
}

// ReceiptInspector resolves the train run's current authoritative physical
// shard. The immutable receipt still carries the command's historical route.
type ReceiptInspector interface {
	Inspect(context.Context, Candidate) (Receipt, bool, error)
}

// Finalizer must update the bounded ledger result/state and the operator-facing
// control projection in one transaction. Implementations must be idempotent.
type Finalizer interface {
	Finalize(context.Context, Command, Receipt) error
}

const (
	MaxClaimBatch     = 100
	MaxClaimLeaseTTL  = 5 * time.Minute
	MaxInspectTimeout = 10 * time.Second
)

var workerIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$`)

func validOperation(operation Operation) bool {
	switch operation {
	case OperationFareInstall, OperationSeatDisable, OperationSeatEnable, OperationBookingPolicyBump:
		return true
	default:
		return false
	}
}

func ValidClaimOptions(options ClaimOptions) bool {
	return workerIDPattern.MatchString(options.WorkerID) && options.BatchSize >= 1 &&
		options.BatchSize <= MaxClaimBatch && options.LeaseTTL > 0 && options.LeaseTTL <= MaxClaimLeaseTTL
}

func ValidFailureRequest(request FailureRequest) bool {
	command := request.Command
	return request.Category == FailureShardRejected && command.State == StateReserved &&
		workerIDPattern.MatchString(request.LeaseOwner) &&
		ValidReserveRequest(ReserveRequest{
			ActorID: command.ActorID, TrainRunID: command.TrainRunID, ResourceID: command.ResourceID,
			Operation: command.Operation, IdempotencyKeyHash: command.IdempotencyKeyHash,
			RequestFingerprint:           command.RequestFingerprint,
			ExpectedSourceVersion:        command.ExpectedSourceVersion,
			ExpectedBookingPolicyVersion: command.ExpectedBookingPolicyVersion,
			FinalizePayload:              command.FinalizePayload,
		}) && command.ID != uuid.Nil && command.Route.TrainRunID() == command.TrainRunID &&
		command.Route.Generation().Int64() > 0 &&
		(command.Route.ShardID() == sharding.ShardPhysicalZero || command.Route.ShardID() == sharding.ShardPhysicalOne)
}

func ValidReserveRequest(request ReserveRequest) bool {
	if request.ActorID == uuid.Nil || request.TrainRunID == uuid.Nil || request.ResourceID == uuid.Nil ||
		!validOperation(request.Operation) || request.IdempotencyKeyHash == [32]byte{} ||
		request.RequestFingerprint == [32]byte{} || request.ExpectedSourceVersion <= 0 {
		return false
	}
	switch request.Operation {
	case OperationFareInstall:
		return request.ExpectedBookingPolicyVersion == 0 && request.FinalizePayload.FromStopIndex >= 0 &&
			request.FinalizePayload.ToStopIndex > request.FinalizePayload.FromStopIndex &&
			validSeatClassValue(request.FinalizePayload.SeatClass) && request.FinalizePayload.AmountMinor >= 0 &&
			validCurrencyValue(request.FinalizePayload.Currency)
	case OperationSeatDisable:
		return request.ExpectedBookingPolicyVersion == 0 && !request.FinalizePayload.SeatActive
	case OperationSeatEnable:
		return request.ExpectedBookingPolicyVersion == 0 && request.FinalizePayload.SeatActive
	case OperationBookingPolicyBump:
		return request.ResourceID == request.TrainRunID && request.ExpectedBookingPolicyVersion > 0 &&
			request.FinalizePayload == (BoundedFinalizePayload{})
	default:
		return false
	}
}

func validSeatClassValue(value string) bool {
	return value == "standard" || value == "business" || value == "first"
}

func validCurrencyValue(value string) bool {
	if len(value) != 3 {
		return false
	}
	for _, character := range value {
		if character < 'A' || character > 'Z' {
			return false
		}
	}
	return true
}
