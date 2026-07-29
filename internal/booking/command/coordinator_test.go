package command_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

func TestCoordinatorRetryFinalizesOneShardCommittedReservation(t *testing.T) {
	t.Parallel()

	trainRunID := uuid.New()
	reservationID := uuid.New()
	passengerID := uuid.New()
	payload := command.CreateReservationPayload{
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: []uuid.UUID{passengerID}, HoldExpiresAt: time.Now().UTC().Add(5 * time.Minute),
		ExpectedSnapshotVersion: 3,
	}
	generation, err := sharding.NewAssignmentGeneration(4)
	if err != nil {
		t.Fatal(err)
	}
	route, err := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	if err != nil {
		t.Fatal(err)
	}
	control := &controlRepository{
		command: command.Command{
			ID:                 uuid.New(),
			Operation:          command.OperationCreateReservation,
			OwnerUserID:        uuid.New(),
			TrainRunID:         trainRunID,
			ReservationID:      reservationID,
			Route:              route,
			RequestFingerprint: [32]byte{1, 2, 3},
			State:              command.StateReserved,
			Payload:            payload,
		},
		finalizeFailures: 1,
	}
	shard := &shardExecutor{receipt: command.Receipt{
		CommandID:          control.command.ID,
		RequestFingerprint: control.command.RequestFingerprint,
		ResultResourceID:   reservationID,
		Status:             command.ReceiptCommitted,
	}}
	coordinator, err := command.NewCoordinator(control, shard)
	if err != nil {
		t.Fatal(err)
	}
	request := command.ReserveRequest{
		OwnerUserID:        control.command.OwnerUserID,
		TrainRunID:         trainRunID,
		Operation:          command.OperationCreateReservation,
		IdempotencyKeyHash: [32]byte{9, 8, 7},
		RequestFingerprint: control.command.RequestFingerprint,
		PassengerCount:     1,
		Payload:            payload,
	}

	_, err = coordinator.Execute(context.Background(), request)
	if !errors.Is(err, command.ErrFinalizationDeferred) {
		t.Fatalf("first Execute() error = %v, want %v", err, command.ErrFinalizationDeferred)
	}
	result, err := coordinator.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("retry Execute() error = %v", err)
	}
	if result.ReservationID != reservationID || result.CommandID != control.command.ID {
		t.Fatalf("retry result = %+v", result)
	}
	if control.reserveCalls != 2 || control.finalizeCalls != 2 || shard.executeCalls != 2 {
		t.Fatalf("calls = reserve %d execute %d finalize %d", control.reserveCalls, shard.executeCalls, control.finalizeCalls)
	}
}

func TestCoordinatorLifecycleRetryAfterControlFinalizationFailure(t *testing.T) {
	t.Parallel()
	trainRunID, reservationID, ownerID := uuid.New(), uuid.New(), uuid.New()
	generation, _ := sharding.NewAssignmentGeneration(9)
	route, _ := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalOne, generation)
	control := &controlRepository{command: command.Command{
		ID: uuid.New(), Operation: command.OperationCancelReservation, OwnerUserID: ownerID,
		TrainRunID: trainRunID, ReservationID: reservationID, Route: route,
		RequestFingerprint: [32]byte{4}, State: command.StateReserved,
	}, finalizeFailures: 1}
	shard := &shardExecutor{receipt: command.Receipt{
		CommandID: control.command.ID, RequestFingerprint: control.command.RequestFingerprint,
		ResultResourceID: reservationID, Status: command.ReceiptCommitted, ReleasedSeatCount: 2,
	}}
	coordinator, err := command.NewCoordinator(control, shard)
	if err != nil {
		t.Fatal(err)
	}
	request := command.LifecycleRequest{OwnerUserID: ownerID, ReservationID: reservationID,
		Operation: command.OperationCancelReservation, IdempotencyKeyHash: [32]byte{3},
		RequestFingerprint: control.command.RequestFingerprint}
	if _, err = coordinator.ExecuteLifecycle(context.Background(), request); !errors.Is(err, command.ErrFinalizationDeferred) {
		t.Fatalf("first lifecycle error=%v", err)
	}
	result, err := coordinator.ExecuteLifecycle(context.Background(), request)
	if err != nil || result.ReleasedSeats != 2 || shard.executeCalls != 2 || control.finalizeCalls != 2 {
		t.Fatalf("retry result=%+v err=%v shard=%d finalize=%d", result, err, shard.executeCalls, control.finalizeCalls)
	}
}

type controlRepository struct {
	command          command.Command
	reserveCalls     int
	finalizeCalls    int
	finalizeFailures int
}

func (repo *controlRepository) Reserve(context.Context, command.ReserveRequest) (command.Command, error) {
	repo.reserveCalls++
	return repo.command, nil
}

func (repo *controlRepository) ReserveLifecycle(context.Context, command.LifecycleRequest) (command.Command, error) {
	repo.reserveCalls++
	return repo.command, nil
}

func (repo *controlRepository) Finalize(context.Context, command.Command, command.Receipt) error {
	repo.finalizeCalls++
	if repo.finalizeFailures > 0 {
		repo.finalizeFailures--
		return errors.New("control unavailable")
	}
	return nil
}

type shardExecutor struct {
	receipt      command.Receipt
	executeCalls int
}

func (executor *shardExecutor) Execute(context.Context, command.Command) (command.Receipt, error) {
	executor.executeCalls++
	return executor.receipt, nil
}
