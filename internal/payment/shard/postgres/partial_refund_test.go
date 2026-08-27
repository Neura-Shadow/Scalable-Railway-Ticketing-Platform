package postgres

import (
	"testing"
	"time"

	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

func TestSelectedRefundCommandRequiresCanonicalTicketsAndRegionalFence(t *testing.T) {
	t.Parallel()
	trainRunID := uuid.New()
	generation, _ := sharding.NewAssignmentGeneration(3)
	route, _ := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	left, right := uuid.New(), uuid.New()
	if left.String() > right.String() {
		left, right = right, left
	}
	command := paymentshard.ApplySelectedTicketRefundCommand{
		CommandID: uuid.New(), RefundRequestID: uuid.New(), RefundOperationID: uuid.New(),
		PaymentIntentID: uuid.New(), ReservationID: uuid.New(), TicketOrderID: uuid.New(),
		TrainRunID: trainRunID, OwnerID: uuid.New(), Region: "region-b", RegionalEpoch: 9,
		AmountMinor: 900, Currency: "TWD", ProviderProofHash: [32]byte{1},
		RequestFingerprint: [32]byte{2}, TicketIDs: []uuid.UUID{left, right}, RefundedAt: time.Now(),
	}
	if !validSelectedRefundCommand(route, command) {
		t.Fatal("valid selected refund command rejected")
	}
	command.TicketIDs = []uuid.UUID{right, left}
	if validSelectedRefundCommand(route, command) {
		t.Fatal("non-canonical ticket order accepted")
	}
	command.TicketIDs = []uuid.UUID{left, left}
	if validSelectedRefundCommand(route, command) {
		t.Fatal("duplicate ticket IDs accepted")
	}
	command.TicketIDs = []uuid.UUID{left}
	command.RegionalEpoch = 0
	if validSelectedRefundCommand(route, command) {
		t.Fatal("missing regional epoch accepted")
	}
}
