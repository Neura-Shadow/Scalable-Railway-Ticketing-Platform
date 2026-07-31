package operatorcommand

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

type Coordinator struct {
	store     Store
	executor  ShardExecutor
	finalizer Finalizer
}

func NewCoordinator(store Store, executor ShardExecutor, finalizer Finalizer) (*Coordinator, error) {
	if nilInterface(store) || nilInterface(executor) || nilInterface(finalizer) {
		return nil, ErrInvalidRequest
	}
	return &Coordinator{store: store, executor: executor, finalizer: finalizer}, nil
}

// Execute always reserves the durable control identity and route before the
// first shard call. A finalizer error leaves the shard receipt recoverable.
func (coordinator *Coordinator) Execute(ctx context.Context, request Request) (Result, error) {
	if coordinator == nil || ctx == nil || !validRequest(request) {
		return Result{}, ErrInvalidRequest
	}
	command, err := coordinator.store.Reserve(ctx, ReserveRequest{
		ActorID: request.ActorID, TrainRunID: request.TrainRunID, ResourceID: request.ResourceID,
		Operation: request.Operation, IdempotencyKeyHash: request.IdempotencyKeyHash,
		RequestFingerprint: request.RequestFingerprint, ExpectedSourceVersion: request.ExpectedSourceVersion,
		ExpectedBookingPolicyVersion: request.ExpectedBookingPolicyVersion,
		FinalizePayload:              request.Mutation,
	})
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrControlUnavailable, err)
	}
	if !validCommandForRequest(command, request) {
		return Result{}, ErrInvalidRequest
	}
	if command.State == StateFailed {
		return Result{}, ErrShardExecution
	}
	if command.State == StateFinalized {
		return resultFromCommand(command, true), nil
	}
	receipt, err := coordinator.executor.Execute(ctx, command, request.Mutation)
	if err != nil {
		// A direct retry cannot distinguish a pre-commit rejection from a
		// committed shard receipt whose control finalization was interrupted.
		// Only recovery may terminalize after an authoritative receipt miss.
		return Result{}, fmt.Errorf("%w: %w", ErrShardExecution, err)
	}
	if !ValidReceipt(command, receipt) {
		return Result{}, ErrReceiptMismatch
	}
	if err := coordinator.finalizer.Finalize(ctx, command, receipt); err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrFinalizationDeferred, err)
	}
	return resultFromReceipt(receipt, false), nil
}

func deterministicShardRejection(err error) bool {
	return errors.Is(err, sharding.ErrAssignmentStale) || errors.Is(err, sharding.ErrWriteFenced)
}

type RecoveryOptions struct {
	ClaimOptions
	InspectTimeout time.Duration
}

type RecoveryResult struct {
	Claimed   int
	Finalized int
	Failed    int
	Deferred  int
	Failures  int
}

type RecoveryService struct {
	store     Store
	executor  ShardExecutor
	inspector ReceiptInspector
	finalizer Finalizer
	options   RecoveryOptions
}

func NewRecoveryService(store Store, executor ShardExecutor, inspector ReceiptInspector, finalizer Finalizer, options RecoveryOptions) (*RecoveryService, error) {
	if nilInterface(store) || nilInterface(executor) || nilInterface(inspector) || nilInterface(finalizer) ||
		!ValidClaimOptions(options.ClaimOptions) || options.InspectTimeout <= 0 ||
		options.InspectTimeout > MaxInspectTimeout || options.InspectTimeout >= options.LeaseTTL {
		return nil, ErrInvalidOptions
	}
	return &RecoveryService{store: store, executor: executor, inspector: inspector, finalizer: finalizer, options: options}, nil
}

func (service *RecoveryService) RunOnce(ctx context.Context) (RecoveryResult, error) {
	if service == nil || ctx == nil {
		return RecoveryResult{}, ErrInvalidOptions
	}
	candidates, err := service.store.Claim(ctx, service.options.ClaimOptions)
	if err != nil {
		return RecoveryResult{}, err
	}
	if len(candidates) > service.options.BatchSize {
		return RecoveryResult{}, ErrInvalidOptions
	}
	result := RecoveryResult{Claimed: len(candidates)}
	var failures []error
	for _, candidate := range candidates {
		if !validCandidate(candidate) {
			result.Failures++
			failures = append(failures, ErrReceiptMismatch)
			continue
		}
		inspectContext, cancel := context.WithTimeout(ctx, service.options.InspectTimeout)
		receipt, found, inspectErr := service.inspector.Inspect(inspectContext, candidate)
		cancel()
		if inspectErr != nil {
			result.Deferred++
			result.Failures++
			failures = append(failures, inspectErr)
			continue
		}
		if !found {
			if candidate.Command.State != StateReserved {
				result.Deferred++
				continue
			}
			executionContext, executionCancel := context.WithTimeout(ctx, service.options.InspectTimeout)
			receipt, inspectErr = service.executor.Execute(
				executionContext, candidate.Command, candidate.Command.FinalizePayload,
			)
			executionCancel()
			if inspectErr != nil {
				if deterministicShardRejection(inspectErr) {
					if failErr := service.store.Fail(ctx, FailureRequest{
						Command: candidate.Command, Category: FailureShardRejected,
						LeaseOwner: candidate.LeaseOwner,
					}); failErr == nil {
						result.Failed++
						continue
					} else {
						inspectErr = failErr
					}
				}
				result.Deferred++
				result.Failures++
				failures = append(failures, inspectErr)
				continue
			}
		}
		if !ValidReceipt(candidate.Command, receipt) {
			result.Deferred++
			result.Failures++
			failures = append(failures, ErrReceiptMismatch)
			continue
		}
		if finalizeErr := service.finalizer.Finalize(ctx, candidate.Command, receipt); finalizeErr != nil {
			result.Deferred++
			result.Failures++
			failures = append(failures, finalizeErr)
			continue
		}
		result.Finalized++
	}
	return result, errors.Join(failures...)
}

func ValidReceipt(command Command, receipt Receipt) bool {
	if receipt.CommandID != command.ID || receipt.TrainRunID != command.TrainRunID ||
		receipt.ResourceID != command.ResourceID || receipt.Operation != command.Operation ||
		receipt.RequestFingerprint != command.RequestFingerprint ||
		receipt.HistoricalShardID != command.Route.ShardID() ||
		receipt.HistoricalGeneration != command.Route.Generation().Int64() ||
		receipt.ResultSourceVersion != command.ExpectedSourceVersion+1 {
		return false
	}
	if command.Operation == OperationBookingPolicyBump {
		return command.ExpectedBookingPolicyVersion > 0 &&
			receipt.ResultBookingPolicyVersion == command.ExpectedBookingPolicyVersion+1
	}
	return command.ExpectedBookingPolicyVersion == 0 && receipt.ResultBookingPolicyVersion == 0
}

func validRequest(request Request) bool {
	return ValidReserveRequest(ReserveRequest{ActorID: request.ActorID, TrainRunID: request.TrainRunID,
		ResourceID: request.ResourceID, Operation: request.Operation,
		IdempotencyKeyHash: request.IdempotencyKeyHash, RequestFingerprint: request.RequestFingerprint,
		ExpectedSourceVersion:        request.ExpectedSourceVersion,
		ExpectedBookingPolicyVersion: request.ExpectedBookingPolicyVersion, FinalizePayload: request.Mutation})
}

func validCommandForRequest(command Command, request Request) bool {
	valid := command.ID != uuid.Nil && command.ActorID == request.ActorID && command.TrainRunID == request.TrainRunID &&
		command.ResourceID == request.ResourceID && command.Operation == request.Operation &&
		command.IdempotencyKeyHash == request.IdempotencyKeyHash &&
		command.RequestFingerprint == request.RequestFingerprint &&
		command.ExpectedSourceVersion == request.ExpectedSourceVersion &&
		command.ExpectedBookingPolicyVersion == request.ExpectedBookingPolicyVersion &&
		command.FinalizePayload == request.Mutation &&
		command.Route.TrainRunID() == request.TrainRunID && physicalRoute(command.Route) &&
		(command.State == StateReserved || command.State == StateCommittedOnShard ||
			command.State == StateNeedsRepair || command.State == StateFinalized || command.State == StateFailed)
	if !valid || command.State != StateFinalized {
		return valid
	}
	if command.ResultSourceVersion != command.ExpectedSourceVersion+1 {
		return false
	}
	if command.Operation == OperationBookingPolicyBump {
		return command.ResultBookingPolicyVersion == command.ExpectedBookingPolicyVersion+1
	}
	return command.ResultBookingPolicyVersion == 0
}

func validCandidate(candidate Candidate) bool {
	command := candidate.Command
	return command.ID != uuid.Nil && command.ActorID != uuid.Nil && command.TrainRunID != uuid.Nil &&
		command.ResourceID != uuid.Nil && validOperation(command.Operation) &&
		command.IdempotencyKeyHash != [32]byte{} && command.RequestFingerprint != [32]byte{} &&
		command.ExpectedSourceVersion > 0 && physicalRoute(command.Route) &&
		(command.State == StateReserved || command.State == StateCommittedOnShard || command.State == StateNeedsRepair) &&
		workerIDPattern.MatchString(candidate.LeaseOwner) && !candidate.LeaseUntil.IsZero()
}

func physicalRoute(route sharding.ShardRoute) bool {
	return route.TrainRunID() != uuid.Nil && route.Generation().Int64() > 0 &&
		(route.ShardID() == sharding.ShardPhysicalZero || route.ShardID() == sharding.ShardPhysicalOne)
}

func resultFromReceipt(receipt Receipt, replayed bool) Result {
	return Result{CommandID: receipt.CommandID, ResourceID: receipt.ResourceID,
		SourceVersion:        receipt.ResultSourceVersion,
		BookingPolicyVersion: receipt.ResultBookingPolicyVersion, Replayed: replayed}
}

func resultFromCommand(command Command, replayed bool) Result {
	return Result{CommandID: command.ID, ResourceID: command.ResourceID,
		SourceVersion:        command.ResultSourceVersion,
		BookingPolicyVersion: command.ResultBookingPolicyVersion, Replayed: replayed}
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
