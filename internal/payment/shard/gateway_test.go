package shard_test

import (
	"context"
	"errors"
	"testing"
	"time"

	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

func TestGatewayResolvesCurrentAssignmentForEveryPaymentCommand(t *testing.T) {
	t.Parallel()
	owner, reservationID, trainRunID, intentID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	route1 := paymentRoute(t, trainRunID, sharding.ShardPhysicalZero, 5)
	route2 := paymentRoute(t, trainRunID, sharding.ShardPhysicalOne, 6)
	directory := &directoryFake{routes: []sharding.ShardRoute{route1, route2}}
	store := &shardStoreFake{snapshot: paymentapp.ReservationSnapshot{
		ID: reservationID, OwnerID: owner, TrainRunID: trainRunID, Status: "held",
		AmountMinor: 12500, Currency: "TWD", ExpiresAt: time.Now().Add(time.Minute),
	}}
	gateway, err := shard.NewGateway(directory, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.GetPayableReservation(context.Background(), reservationID); err != nil {
		t.Fatal(err)
	}
	command := paymentapp.BeginPaymentCommand{
		CommandID: uuid.New(), PaymentIntentID: intentID, ReservationID: reservationID,
		TrainRunID: trainRunID, OwnerID: owner, AmountMinor: 12500, Currency: "TWD",
		GraceExpiresAt: time.Now().Add(10 * time.Minute), RequestFingerprint: [32]byte{1},
	}
	if _, err := gateway.BeginPayment(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if store.inspectRoute != route1 || store.beginRoute != route2 {
		t.Fatalf("inspect route=%#v begin route=%#v", store.inspectRoute, store.beginRoute)
	}
}

func TestGatewayRefreshesOnceAfterLocalFenceRejectsStaleRoute(t *testing.T) {
	t.Parallel()
	reservationID, trainRunID := uuid.New(), uuid.New()
	stale := paymentRoute(t, trainRunID, sharding.ShardPhysicalZero, 5)
	current := paymentRoute(t, trainRunID, sharding.ShardPhysicalOne, 6)
	directory := &directoryFake{routes: []sharding.ShardRoute{stale, current}}
	store := &shardStoreFake{beginErrors: []error{sharding.ErrAssignmentStale, nil}}
	gateway, _ := shard.NewGateway(directory, store)
	command := paymentapp.BeginPaymentCommand{
		CommandID: uuid.New(), PaymentIntentID: uuid.New(), ReservationID: reservationID,
		TrainRunID: trainRunID, OwnerID: uuid.New(), AmountMinor: 12500, Currency: "TWD",
		GraceExpiresAt: time.Now().Add(time.Minute), RequestFingerprint: [32]byte{1},
	}
	receipt, err := gateway.BeginPayment(context.Background(), command)
	if err != nil || receipt.CommandID != command.CommandID || store.beginCalls != 2 || store.beginRoute != current {
		t.Fatalf("receipt=%#v error=%v calls=%d route=%#v", receipt, err, store.beginCalls, store.beginRoute)
	}
}

func TestGatewayDoesNotRetryNonFencingPaymentFailure(t *testing.T) {
	t.Parallel()
	trainRunID := uuid.New()
	directory := &directoryFake{routes: []sharding.ShardRoute{paymentRoute(t, trainRunID, sharding.ShardPhysicalZero, 1)}}
	store := &shardStoreFake{beginErrors: []error{errors.New("persistence failed")}}
	gateway, _ := shard.NewGateway(directory, store)
	command := paymentapp.BeginPaymentCommand{CommandID: uuid.New(), PaymentIntentID: uuid.New(), ReservationID: uuid.New(), TrainRunID: trainRunID, OwnerID: uuid.New(), AmountMinor: 1, Currency: "TWD", GraceExpiresAt: time.Now().Add(time.Minute), RequestFingerprint: [32]byte{1}}
	if _, err := gateway.BeginPayment(context.Background(), command); !errors.Is(err, shard.ErrShardPaymentUnavailable) || store.beginCalls != 1 {
		t.Fatalf("error=%v calls=%d", err, store.beginCalls)
	}
}

func TestGatewayRefreshesVoidCancellationExactlyOnceAfterFenceMove(t *testing.T) {
	t.Parallel()
	reservationID, trainRunID := uuid.New(), uuid.New()
	stale := paymentRoute(t, trainRunID, sharding.ShardPhysicalZero, 5)
	current := paymentRoute(t, trainRunID, sharding.ShardPhysicalOne, 6)
	directory := &directoryFake{routes: []sharding.ShardRoute{stale, current}}
	store := &shardStoreFake{voidErrors: []error{sharding.ErrWriteFenced, nil}}
	gateway, _ := shard.NewGateway(directory, store)
	command := shard.CancelVoidedReservationCommand{
		CommandID: uuid.New(), VoidOperationID: uuid.New(), PaymentIntentID: uuid.New(),
		ReservationID: reservationID, TrainRunID: trainRunID, OwnerID: uuid.New(),
		AmountMinor: 1, Currency: "TWD", VoidProofHash: [32]byte{1}, VoidedAt: time.Now(),
	}
	command.RequestFingerprint = shard.VoidCancellationFingerprint(command)
	receipt, err := gateway.CancelVoidedReservation(context.Background(), command)
	if err != nil || receipt.CommandID != command.CommandID || store.voidCalls != 2 || store.voidRoute != current {
		t.Fatalf("receipt=%+v error=%v calls=%d route=%+v", receipt, err, store.voidCalls, store.voidRoute)
	}
}

func paymentRoute(t *testing.T, trainRunID uuid.UUID, shardID sharding.ShardID, generation int64) sharding.ShardRoute {
	t.Helper()
	gen, err := sharding.NewAssignmentGeneration(generation)
	if err != nil {
		t.Fatal(err)
	}
	route, err := sharding.NewShardRoute(trainRunID, shardID, gen)
	if err != nil {
		t.Fatal(err)
	}
	return route
}

type directoryFake struct {
	routes []sharding.ShardRoute
	calls  int
}

func (fake *directoryFake) ResolveReservation(_ context.Context, _ uuid.UUID, _ bool) (sharding.ShardRoute, error) {
	index := fake.calls
	if index >= len(fake.routes) {
		index = len(fake.routes) - 1
	}
	fake.calls++
	return fake.routes[index], nil
}

type shardStoreFake struct {
	snapshot     paymentapp.ReservationSnapshot
	inspectRoute sharding.ShardRoute
	beginRoute   sharding.ShardRoute
	beginErrors  []error
	beginCalls   int
	voidRoute    sharding.ShardRoute
	voidErrors   []error
	voidCalls    int
}

func (fake *shardStoreFake) GetPayableReservation(_ context.Context, route sharding.ShardRoute, _ uuid.UUID) (paymentapp.ReservationSnapshot, error) {
	fake.inspectRoute = route
	return fake.snapshot, nil
}

func (fake *shardStoreFake) BeginPayment(_ context.Context, route sharding.ShardRoute, command paymentapp.BeginPaymentCommand) (paymentapp.BeginPaymentReceipt, error) {
	fake.beginRoute = route
	index := fake.beginCalls
	fake.beginCalls++
	if index < len(fake.beginErrors) && fake.beginErrors[index] != nil {
		return paymentapp.BeginPaymentReceipt{}, fake.beginErrors[index]
	}
	return paymentapp.BeginPaymentReceipt{CommandID: command.CommandID, PaymentIntentID: command.PaymentIntentID, RequestFingerprint: command.RequestFingerprint}, nil
}

func (fake *shardStoreFake) IssueTickets(_ context.Context, _ sharding.ShardRoute, command shard.IssueTicketsCommand) (shard.IssueTicketsReceipt, error) {
	return shard.IssueTicketsReceipt{CommandID: command.CommandID, IssuanceID: command.IssuanceID, PaymentIntentID: command.PaymentIntentID, ReservationID: command.ReservationID}, nil
}

func (fake *shardStoreFake) MarkRefundPending(_ context.Context, _ sharding.ShardRoute, command shard.MarkRefundPendingCommand) (shard.MarkRefundPendingReceipt, error) {
	return shard.MarkRefundPendingReceipt{CommandID: command.CommandID, PaymentIntentID: command.PaymentIntentID, ReservationID: command.ReservationID}, nil
}

func (fake *shardStoreFake) CancelVoidedReservation(_ context.Context, route sharding.ShardRoute, command shard.CancelVoidedReservationCommand) (shard.CancelVoidedReservationReceipt, error) {
	fake.voidRoute = route
	index := fake.voidCalls
	fake.voidCalls++
	if index < len(fake.voidErrors) && fake.voidErrors[index] != nil {
		return shard.CancelVoidedReservationReceipt{}, fake.voidErrors[index]
	}
	return shard.CancelVoidedReservationReceipt{CommandID: command.CommandID, VoidOperationID: command.VoidOperationID, PaymentIntentID: command.PaymentIntentID, ReservationID: command.ReservationID}, nil
}

func (fake *shardStoreFake) ApplyRefundCompensation(_ context.Context, _ sharding.ShardRoute, command shard.ApplyRefundCompensationCommand) (shard.ApplyRefundCompensationReceipt, error) {
	return shard.ApplyRefundCompensationReceipt{CommandID: command.CommandID, CompensationID: command.CompensationID, PaymentIntentID: command.PaymentIntentID, ReservationID: command.ReservationID}, nil
}
