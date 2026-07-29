package app

import (
	"context"
	"strings"

	commandphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/physical"
	"github.com/google/uuid"
)

type OperatorFareMutation struct {
	ActorID               string
	IdempotencyKey        string
	TrainRunID            uuid.UUID
	FareID                uuid.UUID
	ExpectedSourceVersion int64
	FromStopIndex         int
	ToStopIndex           int
	SeatClass             string
	AmountMinor           int64
	Currency              string
}

type OperatorSeatMutation struct {
	ActorID               string
	IdempotencyKey        string
	TrainRunID            uuid.UUID
	SeatID                uuid.UUID
	ExpectedSourceVersion int64
	Active                bool
}

type OperatorPolicyMutation struct {
	ActorID                      string
	IdempotencyKey               string
	TrainRunID                   uuid.UUID
	ExpectedSourceVersion        int64
	ExpectedBookingPolicyVersion int64
}

type physicalOperatorSnapshotExecutor interface {
	InstallFare(context.Context, commandphysical.FareInstallCommand) (commandphysical.OperatorMutationResult, error)
	SetSeatActive(context.Context, commandphysical.SeatActiveCommand) (commandphysical.OperatorMutationResult, error)
	BumpBookingPolicy(context.Context, commandphysical.BookingPolicyBumpCommand) (commandphysical.OperatorMutationResult, error)
}

func validOperatorMutationIdentity(actorID, key string) bool {
	actorID = strings.TrimSpace(actorID)
	if actorID == "" || len(key) < 8 || len(key) > 128 {
		return false
	}
	for _, character := range key {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}
