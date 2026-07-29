package command_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/google/uuid"
)

func TestCoordinatorRejectsCreatePayloadWhosePassengerCountDoesNotMatch(t *testing.T) {
	t.Parallel()

	control := &controlRepository{}
	coordinator, err := command.NewCoordinator(control, &shardExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	request := command.ReserveRequest{
		OwnerUserID:        uuid.New(),
		TrainRunID:         uuid.New(),
		Operation:          command.OperationCreateReservation,
		IdempotencyKeyHash: [32]byte{1},
		RequestFingerprint: [32]byte{2},
		PassengerCount:     2,
		Payload: command.CreateReservationPayload{
			FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
			PassengerIDs:  []uuid.UUID{uuid.New()},
			HoldExpiresAt: time.Now().UTC().Add(5 * time.Minute), ExpectedSnapshotVersion: 3,
		},
	}

	_, err = coordinator.Execute(context.Background(), request)
	if !errors.Is(err, command.ErrInvalidCommand) {
		t.Fatalf("Execute() error = %v, want %v", err, command.ErrInvalidCommand)
	}
	if control.reserveCalls != 0 {
		t.Fatalf("control Reserve() calls = %d, want 0", control.reserveCalls)
	}
}
