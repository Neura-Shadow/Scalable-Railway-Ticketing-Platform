package main

import (
	"context"
	"errors"
	"time"

	bookingcommand "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	commandphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/physical"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
)

type physicalBookingMetrics interface {
	RecordBookingCommand(operation, result, reason string)
	RecordBookingCommandFinalizeFailure(reason string)
	RecordPhysicalShardRoute(operation, result, reason, shardID, storageKind string, duration time.Duration)
	RecordPhysicalShardUnavailable(operation, reason, shardID string)
	RecordPhysicalShardFenceRejected(operation, reason, shardID string)
}

type observedPhysicalExecutor struct {
	next    bookingcommand.ShardExecutor
	metrics physicalBookingMetrics
}

func (executor observedPhysicalExecutor) Execute(
	ctx context.Context,
	command bookingcommand.Command,
) (bookingcommand.Receipt, error) {
	started := time.Now()
	receipt, err := executor.next.Execute(ctx, command)
	operation := physicalBookingOperation(command.Operation)
	result, reason := physicalExecutionOutcome(err)
	shardID := command.Route.ShardID().String()
	executor.metrics.RecordPhysicalShardRoute(
		operation, result, reason, shardID, "postgres", time.Since(started),
	)
	if result == "unavailable" {
		executor.metrics.RecordPhysicalShardUnavailable(operation, reason, shardID)
	}
	if reason == "write_disabled" || reason == "stale_generation" {
		executor.metrics.RecordPhysicalShardFenceRejected(operation, reason, shardID)
	}
	return receipt, err
}

type physicalCoordinator interface {
	Execute(context.Context, bookingcommand.ReserveRequest) (bookingcommand.Result, error)
	ExecuteLifecycle(context.Context, bookingcommand.LifecycleRequest) (bookingcommand.Result, error)
}

type observedPhysicalCoordinator struct {
	next    physicalCoordinator
	metrics physicalBookingMetrics
}

func (coordinator observedPhysicalCoordinator) Execute(
	ctx context.Context,
	request bookingcommand.ReserveRequest,
) (bookingcommand.Result, error) {
	result, err := coordinator.next.Execute(ctx, request)
	coordinator.record(request.Operation, err)
	return result, err
}

func (coordinator observedPhysicalCoordinator) ExecuteLifecycle(
	ctx context.Context,
	request bookingcommand.LifecycleRequest,
) (bookingcommand.Result, error) {
	result, err := coordinator.next.ExecuteLifecycle(ctx, request)
	coordinator.record(request.Operation, err)
	return result, err
}

func (coordinator observedPhysicalCoordinator) record(operation bookingcommand.Operation, err error) {
	result, reason := physicalCoordinatorOutcome(err)
	coordinator.metrics.RecordBookingCommand(physicalBookingOperation(operation), result, reason)
	if errors.Is(err, bookingcommand.ErrFinalizationDeferred) {
		coordinator.metrics.RecordBookingCommandFinalizeFailure(reason)
	}
}

func physicalBookingOperation(operation bookingcommand.Operation) string {
	switch operation {
	case bookingcommand.OperationCreateReservation:
		return "create"
	case bookingcommand.OperationConfirmReservation:
		return "confirm"
	case bookingcommand.OperationCancelReservation:
		return "cancel"
	default:
		return "unknown"
	}
}

func physicalExecutionOutcome(err error) (string, string) {
	switch {
	case err == nil:
		return "success", "none"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "unavailable", "timeout"
	case errors.Is(err, sharding.ErrWriteFenced):
		return "rejected", "write_disabled"
	case errors.Is(err, sharding.ErrAssignmentStale):
		return "rejected", "stale_generation"
	case errors.Is(err, sharding.ErrShardUnavailable):
		return "unavailable", "database"
	case errors.Is(err, commandphysical.ErrInvalidPayload),
		errors.Is(err, commandphysical.ErrFareUnavailable),
		errors.Is(err, commandphysical.ErrInsufficientInventory):
		return "rejected", "validation"
	default:
		return "failure", "database"
	}
}

func physicalCoordinatorOutcome(err error) (string, string) {
	switch {
	case err == nil:
		return "success", "none"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "unavailable", "timeout"
	case errors.Is(err, bookingcommand.ErrInvalidCommand):
		return "rejected", "validation"
	case errors.Is(err, bookingcommand.ErrReceiptMismatch):
		return "rejected", "receipt"
	case errors.Is(err, bookingcommand.ErrFinalizationDeferred):
		return "deferred", "database"
	case errors.Is(err, sharding.ErrWriteFenced):
		return "rejected", "write_disabled"
	case errors.Is(err, sharding.ErrAssignmentStale):
		return "rejected", "stale_generation"
	case errors.Is(err, bookingcommand.ErrControlUnavailable),
		errors.Is(err, sharding.ErrShardUnavailable):
		return "unavailable", "database"
	case errors.Is(err, commandphysical.ErrInvalidPayload),
		errors.Is(err, commandphysical.ErrFareUnavailable),
		errors.Is(err, commandphysical.ErrInsufficientInventory):
		return "rejected", "validation"
	default:
		return "failure", "database"
	}
}
