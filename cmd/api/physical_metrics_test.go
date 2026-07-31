package main

import (
	"context"
	"errors"
	"testing"
	"time"

	bookingcommand "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	commandphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/physical"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

func TestObservedPhysicalExecutorUsesBoundedLabels(t *testing.T) {
	trainRunID := uuid.New()
	generation, err := sharding.NewAssignmentGeneration(3)
	if err != nil {
		t.Fatal(err)
	}
	route, err := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	if err != nil {
		t.Fatal(err)
	}
	metrics := &physicalMetricsSpy{}
	executor := observedPhysicalExecutor{
		next: shardExecutorStub{err: sharding.ErrWriteFenced}, metrics: metrics,
	}
	_, executeErr := executor.Execute(context.Background(), bookingcommand.Command{
		Operation: bookingcommand.OperationConfirmReservation, TrainRunID: trainRunID, Route: route,
	})
	if !errors.Is(executeErr, sharding.ErrWriteFenced) {
		t.Fatalf("Execute() error = %v", executeErr)
	}
	if len(metrics.routes) != 1 || metrics.routes[0] != "confirm:rejected:write_disabled:physical-shard-0:postgres" {
		t.Fatalf("routes = %v", metrics.routes)
	}
	if len(metrics.fences) != 1 || metrics.fences[0] != "confirm:write_disabled:physical-shard-0" {
		t.Fatalf("fences = %v", metrics.fences)
	}
	if len(metrics.unavailable) != 0 {
		t.Fatalf("unavailable = %v", metrics.unavailable)
	}
}

func TestObservedPhysicalCoordinatorClassifiesFinalizationAndLifecycle(t *testing.T) {
	metrics := &physicalMetricsSpy{}
	coordinator := observedPhysicalCoordinator{
		next: coordinatorStub{err: errors.Join(
			bookingcommand.ErrFinalizationDeferred, errors.New("credential-bearing detail"),
		)},
		metrics: metrics,
	}
	_, err := coordinator.ExecuteLifecycle(context.Background(), bookingcommand.LifecycleRequest{
		Operation: bookingcommand.OperationCancelReservation,
	})
	if !errors.Is(err, bookingcommand.ErrFinalizationDeferred) {
		t.Fatalf("ExecuteLifecycle() error = %v", err)
	}
	if len(metrics.commands) != 1 || metrics.commands[0] != "cancel:deferred:database" {
		t.Fatalf("commands = %v", metrics.commands)
	}
	if len(metrics.finalizeFailures) != 1 || metrics.finalizeFailures[0] != "database" {
		t.Fatalf("finalize failures = %v", metrics.finalizeFailures)
	}
}

func TestPhysicalMetricsOutcomeVocabulary(t *testing.T) {
	tests := []struct {
		err         error
		wantResult  string
		wantReason  string
		coordinator bool
	}{
		{err: nil, wantResult: "success", wantReason: "none"},
		{err: context.DeadlineExceeded, wantResult: "unavailable", wantReason: "timeout"},
		{err: sharding.ErrAssignmentStale, wantResult: "rejected", wantReason: "stale_generation"},
		{err: sharding.ErrShardUnavailable, wantResult: "unavailable", wantReason: "database"},
		{err: bookingcommand.ErrReceiptMismatch, wantResult: "rejected", wantReason: "receipt", coordinator: true},
		{err: bookingcommand.ErrFinalizationDeferred, wantResult: "deferred", wantReason: "database", coordinator: true},
		{err: commandphysical.ErrReservationExpired, wantResult: "rejected", wantReason: "validation"},
		{err: commandphysical.ErrInvalidLifecycleState, wantResult: "rejected", wantReason: "validation", coordinator: true},
	}
	for _, testCase := range tests {
		result, reason := physicalExecutionOutcome(testCase.err)
		if testCase.coordinator {
			result, reason = physicalCoordinatorOutcome(testCase.err)
		}
		if result != testCase.wantResult || reason != testCase.wantReason {
			t.Errorf("outcome(%v) = (%q,%q), want (%q,%q)", testCase.err, result, reason, testCase.wantResult, testCase.wantReason)
		}
	}
}

type shardExecutorStub struct{ err error }

func (stub shardExecutorStub) Execute(context.Context, bookingcommand.Command) (bookingcommand.Receipt, error) {
	return bookingcommand.Receipt{}, stub.err
}

type coordinatorStub struct{ err error }

func (stub coordinatorStub) Execute(context.Context, bookingcommand.ReserveRequest) (bookingcommand.Result, error) {
	return bookingcommand.Result{}, stub.err
}

func (stub coordinatorStub) ExecuteLifecycle(context.Context, bookingcommand.LifecycleRequest) (bookingcommand.Result, error) {
	return bookingcommand.Result{}, stub.err
}

type physicalMetricsSpy struct {
	commands         []string
	finalizeFailures []string
	routes           []string
	unavailable      []string
	fences           []string
}

func (metrics *physicalMetricsSpy) RecordBookingCommand(operation, result, reason string) {
	metrics.commands = append(metrics.commands, operation+":"+result+":"+reason)
}

func (metrics *physicalMetricsSpy) RecordBookingCommandFinalizeFailure(reason string) {
	metrics.finalizeFailures = append(metrics.finalizeFailures, reason)
}

func (metrics *physicalMetricsSpy) RecordPhysicalShardRoute(operation, result, reason, shardID, storageKind string, _ time.Duration) {
	metrics.routes = append(metrics.routes, operation+":"+result+":"+reason+":"+shardID+":"+storageKind)
}

func (metrics *physicalMetricsSpy) RecordPhysicalShardUnavailable(operation, reason, shardID string) {
	metrics.unavailable = append(metrics.unavailable, operation+":"+reason+":"+shardID)
}

func (metrics *physicalMetricsSpy) RecordPhysicalShardFenceRejected(operation, reason, shardID string) {
	metrics.fences = append(metrics.fences, operation+":"+reason+":"+shardID)
}
