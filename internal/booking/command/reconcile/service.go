// Package reconcile repairs the control half of a physical booking command
// saga by inspecting immutable shard-local command receipts. It never exposes
// a seat-inventory mutation capability.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/google/uuid"
)

var (
	ErrInvalidOptions   = errors.New("invalid booking command reconciler options")
	ErrInvalidCandidate = errors.New("invalid booking command reconciliation candidate")
	ErrShardUnreachable = errors.New("booking command shard unreachable")
	ErrReceiptMismatch  = errors.New("booking command reconciliation receipt mismatch")
)

const (
	maxBatchSize      = 500
	maxLeaseTTL       = 5 * time.Minute
	maxInspectTimeout = 10 * time.Second
)

var workerIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$`)

type ObservationKind string

const (
	ObservationMissing   ObservationKind = "missing"
	ObservationStarted   ObservationKind = "started"
	ObservationCommitted ObservationKind = "committed"
	ObservationRejected  ObservationKind = "rejected"
)

type Outcome string

const (
	OutcomeDeferred  Outcome = "deferred"
	OutcomeFinalized Outcome = "finalized"
	OutcomeFailed    Outcome = "failed"
	OutcomeExpired   Outcome = "expired"
)

type FailureCategory string

const FailureShardRejected FailureCategory = "shard_rejected"

// Candidate contains only the identity, routing, and lease data needed for
// receipt inspection. The original booking payload is deliberately absent.
type Candidate struct {
	Command        command.Command
	QuotaExpiresAt time.Time
}

// Observation is the immutable result of one authoritative receipt lookup.
// Missing means the shard was reached and returned no row.
type Observation struct {
	Kind               ObservationKind
	CommandID          uuid.UUID
	RequestFingerprint [32]byte
	ResultResourceID   uuid.UUID
	ErrorCode          string
	Receipt            command.Receipt
}

type ClaimOptions struct {
	WorkerID  string
	BatchSize int
	LeaseTTL  time.Duration
}

type Store interface {
	Claim(context.Context, ClaimOptions) ([]Candidate, error)
	Finalize(context.Context, Candidate, command.Receipt) error
	Fail(context.Context, Candidate, FailureCategory) error
	Expire(context.Context, Candidate) error
}

type ShardInspector interface {
	Inspect(context.Context, Candidate) (Observation, error)
}

type Options struct {
	WorkerID       string
	BatchSize      int
	LeaseTTL       time.Duration
	InspectTimeout time.Duration
}

type Result struct {
	Claimed   int
	Finalized int
	Failed    int
	Expired   int
	Deferred  int
	Failures  int
}

type Service struct {
	store     Store
	inspector ShardInspector
	options   Options
	now       func() time.Time
}

func New(store Store, inspector ShardInspector, options Options) (*Service, error) {
	if nilInterface(store) || nilInterface(inspector) || !workerIDPattern.MatchString(options.WorkerID) ||
		options.BatchSize < 1 || options.BatchSize > maxBatchSize ||
		options.LeaseTTL <= 0 || options.LeaseTTL > maxLeaseTTL ||
		options.InspectTimeout <= 0 || options.InspectTimeout > maxInspectTimeout ||
		options.InspectTimeout >= options.LeaseTTL {
		return nil, ErrInvalidOptions
	}
	return &Service{store: store, inspector: inspector, options: options, now: time.Now}, nil
}

// Inspect performs one read-only, deadline-bounded receipt lookup.
func (service *Service) Inspect(ctx context.Context, candidate Candidate) (Observation, error) {
	if service == nil || ctx == nil || !validCandidate(candidate) {
		return Observation{}, ErrInvalidCandidate
	}
	inspectionContext, cancel := context.WithTimeout(ctx, service.options.InspectTimeout)
	defer cancel()
	observation, err := service.inspector.Inspect(inspectionContext, candidate)
	if err != nil {
		return Observation{}, err
	}
	if !validObservation(candidate, observation) {
		return Observation{}, ErrReceiptMismatch
	}
	return observation, nil
}

// Repair re-inspects the authoritative shard receipt before applying one
// bounded control-plane transition. Unknown outcomes remain unchanged.
func (service *Service) Repair(ctx context.Context, candidate Candidate) (Outcome, error) {
	observation, err := service.Inspect(ctx, candidate)
	if err != nil {
		return OutcomeDeferred, err
	}
	switch observation.Kind {
	case ObservationCommitted:
		receipt := observation.Receipt
		if receipt.CommandID == uuid.Nil {
			receipt = command.Receipt{
				CommandID: candidate.Command.ID, RequestFingerprint: candidate.Command.RequestFingerprint,
				ResultResourceID: candidate.Command.ReservationID, Status: command.ReceiptCommitted,
			}
		}
		if err := service.store.Finalize(ctx, candidate, receipt); err != nil {
			return OutcomeDeferred, err
		}
		return OutcomeFinalized, nil
	case ObservationRejected:
		if err := service.store.Fail(ctx, candidate, FailureShardRejected); err != nil {
			return OutcomeDeferred, err
		}
		return OutcomeFailed, nil
	case ObservationMissing:
		if service.now().Before(candidate.QuotaExpiresAt) || candidate.Command.State == command.StateFinalized {
			return OutcomeDeferred, nil
		}
		if err := service.store.Expire(ctx, candidate); err != nil {
			return OutcomeDeferred, err
		}
		return OutcomeExpired, nil
	case ObservationStarted:
		return OutcomeDeferred, nil
	default:
		return OutcomeDeferred, ErrReceiptMismatch
	}
}

func (service *Service) RunOnce(ctx context.Context) (Result, error) {
	if service == nil || ctx == nil {
		return Result{}, ErrInvalidOptions
	}
	candidates, err := service.store.Claim(ctx, ClaimOptions{
		WorkerID: service.options.WorkerID, BatchSize: service.options.BatchSize, LeaseTTL: service.options.LeaseTTL,
	})
	if err != nil {
		return Result{}, err
	}
	if len(candidates) > service.options.BatchSize {
		return Result{}, fmt.Errorf("%w: store exceeded batch bound", ErrInvalidCandidate)
	}
	result := Result{Claimed: len(candidates)}
	var failures []error
	for _, candidate := range candidates {
		outcome, repairErr := service.Repair(ctx, candidate)
		switch outcome {
		case OutcomeFinalized:
			result.Finalized++
		case OutcomeFailed:
			result.Failed++
		case OutcomeExpired:
			result.Expired++
		default:
			result.Deferred++
		}
		if repairErr != nil {
			result.Failures++
			failures = append(failures, repairErr)
		}
	}
	return result, errors.Join(failures...)
}

func validCandidate(candidate Candidate) bool {
	cmd := candidate.Command
	return cmd.ID != uuid.Nil && cmd.Operation == command.OperationCreateReservation &&
		cmd.OwnerUserID != uuid.Nil && cmd.TrainRunID != uuid.Nil && cmd.ReservationID != uuid.Nil &&
		cmd.RequestFingerprint != [32]byte{} && cmd.Route.TrainRunID() == cmd.TrainRunID &&
		cmd.Route.Generation().Int64() > 0 && !candidate.QuotaExpiresAt.IsZero() &&
		(cmd.State == command.StateReserved || cmd.State == command.StateExecuting ||
			cmd.State == command.StateCommittedOnShard || cmd.State == command.StateNeedsRepair ||
			cmd.State == command.StateFinalized)
}

func validObservation(candidate Candidate, observation Observation) bool {
	if observation.Kind == ObservationMissing {
		return observation.CommandID == uuid.Nil && observation.RequestFingerprint == [32]byte{} &&
			observation.ResultResourceID == uuid.Nil && observation.Receipt.CommandID == uuid.Nil
	}
	commandID, fingerprint, resultID := observation.CommandID, observation.RequestFingerprint, observation.ResultResourceID
	if observation.Receipt.CommandID != uuid.Nil {
		commandID = observation.Receipt.CommandID
		fingerprint = observation.Receipt.RequestFingerprint
		resultID = observation.Receipt.ResultResourceID
	}
	if commandID != candidate.Command.ID || fingerprint != candidate.Command.RequestFingerprint {
		return false
	}
	switch observation.Kind {
	case ObservationCommitted:
		return resultID == candidate.Command.ReservationID && observation.Receipt.Status == command.ReceiptCommitted
	case ObservationRejected:
		return resultID == uuid.Nil && len(observation.ErrorCode) > 0 && len(observation.ErrorCode) <= 64
	case ObservationStarted:
		return resultID == uuid.Nil && observation.ErrorCode == ""
	default:
		return false
	}
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
