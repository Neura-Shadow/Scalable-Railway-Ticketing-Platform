package command_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

func TestCoordinatorOneHundredConcurrentSameKeyRequestsConvergeOnOnePhysicalMutation(t *testing.T) {
	t.Parallel()

	const requestCount = 100
	trainRunID, reservationID, ownerID, passengerID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	generation, err := sharding.NewAssignmentGeneration(7)
	if err != nil {
		t.Fatal(err)
	}
	route, err := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	if err != nil {
		t.Fatal(err)
	}
	payload := command.CreateReservationPayload{
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: []uuid.UUID{passengerID}, HoldExpiresAt: time.Now().UTC().Add(5 * time.Minute),
		ExpectedSnapshotVersion: 3,
	}
	bookingCommand := command.Command{
		ID: uuid.New(), Operation: command.OperationCreateReservation, OwnerUserID: ownerID,
		TrainRunID: trainRunID, ReservationID: reservationID, Route: route,
		RequestFingerprint: [32]byte{4, 5, 6}, State: command.StateReserved, Payload: payload,
	}
	control := &convergingControlRepository{command: bookingCommand, commandRows: 1}
	shard := &convergingShardExecutor{receipt: command.Receipt{
		CommandID: bookingCommand.ID, RequestFingerprint: bookingCommand.RequestFingerprint,
		ResultResourceID: reservationID, Status: command.ReceiptCommitted,
	}}
	coordinator, err := command.NewCoordinator(control, shard)
	if err != nil {
		t.Fatal(err)
	}
	request := command.ReserveRequest{
		OwnerUserID: ownerID, TrainRunID: trainRunID, Operation: command.OperationCreateReservation,
		IdempotencyKeyHash: [32]byte{9, 8, 7}, RequestFingerprint: bookingCommand.RequestFingerprint,
		PassengerCount: 1, Payload: payload,
	}

	start := make(chan struct{})
	var ready sync.WaitGroup
	var finished sync.WaitGroup
	ready.Add(requestCount)
	finished.Add(requestCount)
	results := make(chan command.Result, requestCount)
	errorsSeen := make(chan error, requestCount)
	for range requestCount {
		go func() {
			defer finished.Done()
			ready.Done()
			<-start
			result, executeErr := coordinator.Execute(context.Background(), request)
			if executeErr != nil {
				errorsSeen <- executeErr
				return
			}
			results <- result
		}()
	}
	ready.Wait()
	close(start)
	finished.Wait()
	close(results)
	close(errorsSeen)

	if len(errorsSeen) != 0 || len(results) != requestCount {
		t.Fatalf("concurrent results = %d errors = %d, want %d/0", len(results), len(errorsSeen), requestCount)
	}
	for result := range results {
		if result.CommandID != bookingCommand.ID || result.ReservationID != reservationID {
			t.Fatalf("concurrent result = %+v", result)
		}
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if control.reserveCalls != requestCount || control.commandRows != 1 || control.finalizationRows != 1 ||
		shard.receiptRows != 1 || shard.reservationRows != 1 || shard.seatMutations != 1 || shard.outboxRows != 1 {
		t.Fatalf("concurrent effects: reserves=%d commands=%d finalizations=%d receipts=%d reservations=%d seats=%d outbox=%d",
			control.reserveCalls, control.commandRows, control.finalizationRows, shard.receiptRows,
			shard.reservationRows, shard.seatMutations, shard.outboxRows)
	}
}

func TestCoordinatorReplayIgnoresServerDerivedHoldExpiry(t *testing.T) {
	t.Parallel()

	trainRunID, reservationID, ownerID, passengerID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	generation, err := sharding.NewAssignmentGeneration(7)
	if err != nil {
		t.Fatal(err)
	}
	route, err := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	if err != nil {
		t.Fatal(err)
	}
	storedPayload := command.CreateReservationPayload{
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: []uuid.UUID{passengerID}, HoldExpiresAt: time.Now().UTC().Add(5 * time.Minute),
		ExpectedSnapshotVersion: 3,
	}
	bookingCommand := command.Command{
		ID: uuid.New(), Operation: command.OperationCreateReservation, OwnerUserID: ownerID,
		TrainRunID: trainRunID, ReservationID: reservationID, Route: route,
		RequestFingerprint: [32]byte{4, 5, 6}, State: command.StateReserved, Payload: storedPayload,
	}
	control := &controlRepository{command: bookingCommand}
	shard := &shardExecutor{receipt: command.Receipt{
		CommandID: bookingCommand.ID, RequestFingerprint: bookingCommand.RequestFingerprint,
		ResultResourceID: reservationID, Status: command.ReceiptCommitted,
	}}
	coordinator, err := command.NewCoordinator(control, shard)
	if err != nil {
		t.Fatal(err)
	}
	retryPayload := storedPayload
	retryPayload.HoldExpiresAt = storedPayload.HoldExpiresAt.Add(2 * time.Second)
	result, err := coordinator.Execute(context.Background(), command.ReserveRequest{
		OwnerUserID: ownerID, TrainRunID: trainRunID, Operation: command.OperationCreateReservation,
		IdempotencyKeyHash: [32]byte{9, 8, 7}, RequestFingerprint: bookingCommand.RequestFingerprint,
		PassengerCount: 1, Payload: retryPayload,
	})
	if err != nil {
		t.Fatalf("Execute() server-derived expiry replay error = %v", err)
	}
	if result.CommandID != bookingCommand.ID || result.ReservationID != reservationID {
		t.Fatalf("Execute() server-derived expiry replay result = %+v", result)
	}
}

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

func TestCoordinatorFinalizedLifecycleReplayReadsReceiptWithoutReapplyingControlProjection(t *testing.T) {
	t.Parallel()
	trainRunID, reservationID, ownerID := uuid.New(), uuid.New(), uuid.New()
	generation, _ := sharding.NewAssignmentGeneration(9)
	route, _ := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalOne, generation)
	control := &controlRepository{command: command.Command{
		ID: uuid.New(), Operation: command.OperationConfirmReservation, OwnerUserID: ownerID,
		TrainRunID: trainRunID, ReservationID: reservationID, Route: route,
		RequestFingerprint: [32]byte{4}, State: command.StateFinalized,
	}}
	shard := &shardExecutor{receipt: command.Receipt{
		CommandID: control.command.ID, RequestFingerprint: control.command.RequestFingerprint,
		ResultResourceID: reservationID, Status: command.ReceiptCommitted,
		TicketOrderID: uuid.New(), TicketCount: 1,
	}}
	coordinator, _ := command.NewCoordinator(control, shard)
	result, err := coordinator.ExecuteLifecycle(context.Background(), command.LifecycleRequest{
		OwnerUserID: ownerID, ReservationID: reservationID,
		Operation: command.OperationConfirmReservation, IdempotencyKeyHash: [32]byte{3},
		RequestFingerprint: control.command.RequestFingerprint,
	})
	if err != nil || !result.Replayed || shard.executeCalls != 1 || control.finalizeCalls != 0 {
		t.Fatalf("result=%+v err=%v shard=%d finalize=%d", result, err, shard.executeCalls, control.finalizeCalls)
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

type convergingControlRepository struct {
	mu               sync.Mutex
	command          command.Command
	reserveCalls     int
	commandRows      int
	finalizationRows int
}

func (repo *convergingControlRepository) Reserve(context.Context, command.ReserveRequest) (command.Command, error) {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	repo.reserveCalls++
	return repo.command, nil
}

func (repo *convergingControlRepository) ReserveLifecycle(context.Context, command.LifecycleRequest) (command.Command, error) {
	return command.Command{}, errors.New("unexpected lifecycle request")
}

func (repo *convergingControlRepository) Finalize(context.Context, command.Command, command.Receipt) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.command.State != command.StateFinalized {
		repo.command.State = command.StateFinalized
		repo.finalizationRows++
	}
	return nil
}

type convergingShardExecutor struct {
	mu              sync.Mutex
	receipt         command.Receipt
	receiptRows     int
	reservationRows int
	seatMutations   int
	outboxRows      int
}

func (executor *convergingShardExecutor) Execute(context.Context, command.Command) (command.Receipt, error) {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if executor.receiptRows == 0 {
		executor.receiptRows = 1
		executor.reservationRows = 1
		executor.seatMutations = 1
		executor.outboxRows = 1
	}
	return executor.receipt, nil
}

func (executor *shardExecutor) Execute(context.Context, command.Command) (command.Receipt, error) {
	executor.executeCalls++
	return executor.receipt, nil
}
