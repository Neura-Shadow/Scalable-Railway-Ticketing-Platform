package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	"github.com/google/uuid"
)

func TestCreateIntentDerivesFinancialsAndSecuresReservationOnce(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	owner, reservationID, trainRunID := uuid.New(), uuid.New(), uuid.New()
	store := &intentStoreFake{}
	reservations := &reservationGatewayFake{snapshot: application.ReservationSnapshot{
		ID: reservationID, OwnerID: owner, TrainRunID: trainRunID, Status: "held",
		AmountMinor: 12500, Currency: "TWD", ExpiresAt: now.Add(10 * time.Minute),
	}}
	service := application.NewService(store, reservations, func() time.Time { return now }, uuid.New).
		WithProcessingGrace(2 * time.Minute)

	first, err := service.CreateIntent(context.Background(), application.CreateIntentCommand{
		OwnerID: owner, ReservationID: reservationID, IdempotencyKey: "payment-key-123",
	})
	if err != nil {
		t.Fatalf("CreateIntent() error = %v", err)
	}
	second, err := service.CreateIntent(context.Background(), application.CreateIntentCommand{
		OwnerID: owner, ReservationID: reservationID, IdempotencyKey: "payment-key-123",
	})
	if err != nil {
		t.Fatalf("CreateIntent() replay error = %v", err)
	}
	if first.ID == uuid.Nil || second.ID != first.ID || first.AmountMinor != 12500 || first.Currency != "TWD" {
		t.Fatalf("intent replay/financials = first %#v second %#v", first, second)
	}
	if reservations.beginCalls != 1 {
		t.Fatalf("BeginPayment calls = %d, want 1", reservations.beginCalls)
	}
	if reservations.lastBegin.PaymentIntentID != first.ID || reservations.lastBegin.AmountMinor != 12500 || reservations.lastBegin.Currency != "TWD" {
		t.Fatalf("begin command = %#v", reservations.lastBegin)
	}
	if !reservations.lastBegin.GraceExpiresAt.Equal(now.Add(2 * time.Minute)) {
		t.Fatalf("grace expiry = %s", reservations.lastBegin.GraceExpiresAt)
	}
	if store.lastReserve.IdempotencyKeyHash == [32]byte{} || store.lastReserve.RequestFingerprint == [32]byte{} {
		t.Fatal("raw idempotency identity was not reduced to bounded hashes")
	}
}

func TestCreateIntentRejectsNonPayableReservationBeforeControlMutation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		snapshot application.ReservationSnapshot
		want     error
	}{
		{name: "wrong owner", snapshot: application.ReservationSnapshot{ID: uuid.New(), OwnerID: uuid.New(), TrainRunID: uuid.New(), Status: "held", AmountMinor: 100, Currency: "TWD", ExpiresAt: now.Add(time.Minute)}, want: application.ErrPaymentNotFound},
		{name: "confirmed", snapshot: application.ReservationSnapshot{ID: uuid.New(), OwnerID: uuid.New(), TrainRunID: uuid.New(), Status: "confirmed", AmountMinor: 100, Currency: "TWD", ExpiresAt: now.Add(time.Minute)}, want: application.ErrReservationNotPayable},
		{name: "expired", snapshot: application.ReservationSnapshot{ID: uuid.New(), OwnerID: uuid.New(), TrainRunID: uuid.New(), Status: "held", AmountMinor: 100, Currency: "TWD", ExpiresAt: now}, want: application.ErrReservationNotPayable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			owner := test.snapshot.OwnerID
			if test.name == "wrong owner" {
				owner = uuid.New()
			}
			store := &intentStoreFake{}
			service := application.NewService(store, &reservationGatewayFake{snapshot: test.snapshot}, func() time.Time { return now }, uuid.New)
			_, err := service.CreateIntent(context.Background(), application.CreateIntentCommand{OwnerID: owner, ReservationID: test.snapshot.ID, IdempotencyKey: "payment-key-123"})
			if !errors.Is(err, test.want) {
				t.Fatalf("CreateIntent() error = %v, want %v", err, test.want)
			}
			if store.reserveCalls != 0 {
				t.Fatalf("ReserveIntent calls = %d, want 0", store.reserveCalls)
			}
		})
	}
}

func TestCreateIntentReturnsStableResourceWhenControlFinalizationIsDeferred(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	owner, reservationID := uuid.New(), uuid.New()
	store := &intentStoreFake{finalizeErr: application.ErrControlFinalizationDeferred}
	reservations := &reservationGatewayFake{snapshot: application.ReservationSnapshot{
		ID: reservationID, OwnerID: owner, TrainRunID: uuid.New(), Status: "held",
		AmountMinor: 12500, Currency: "TWD", ExpiresAt: now.Add(time.Minute),
	}}
	service := application.NewService(store, reservations, func() time.Time { return now }, uuid.New)
	intent, err := service.CreateIntent(context.Background(), application.CreateIntentCommand{OwnerID: owner, ReservationID: reservationID, IdempotencyKey: "payment-key-123"})
	if !errors.Is(err, application.ErrControlFinalizationDeferred) || intent.ID == uuid.Nil {
		t.Fatalf("CreateIntent() intent=%#v error=%v", intent, err)
	}
	store.finalizeErr = nil
	replayed, err := service.CreateIntent(context.Background(), application.CreateIntentCommand{OwnerID: owner, ReservationID: reservationID, IdempotencyKey: "payment-key-123"})
	if err != nil || replayed.ID != intent.ID || replayed.State != "checkout_pending" || reservations.beginCalls != 1 || store.finalizeCalls != 2 {
		t.Fatalf("replay intent=%#v error=%v beginCalls=%d finalizeCalls=%d", replayed, err, reservations.beginCalls, store.finalizeCalls)
	}
}

func TestOwnedIntentReadAndCancellationAreBoundedAndIdempotent(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	owner, intentID := uuid.New(), uuid.New()
	store := &intentStoreFake{intent: application.IntentRecord{
		ID: intentID, OwnerID: owner, ReservationID: uuid.New(), TrainRunID: uuid.New(),
		Provider: "sandbox", State: "awaiting_customer", AmountMinor: 12500, Currency: "TWD",
	}}
	service := application.NewService(store, &reservationGatewayFake{}, func() time.Time { return now }, uuid.New)

	got, err := service.GetIntent(context.Background(), owner, intentID)
	if err != nil || got.ID != intentID {
		t.Fatalf("GetIntent() = %#v, %v", got, err)
	}
	first, err := service.CancelIntent(context.Background(), application.CancelIntentCommand{
		OwnerID: owner, PaymentIntentID: intentID, IdempotencyKey: "cancel-payment-1",
	})
	if err != nil {
		t.Fatalf("CancelIntent() error = %v", err)
	}
	second, err := service.CancelIntent(context.Background(), application.CancelIntentCommand{
		OwnerID: owner, PaymentIntentID: intentID, IdempotencyKey: "cancel-payment-1",
	})
	if err != nil || second.ID != first.ID || store.cancelCalls != 2 || store.lastCancel.IdempotencyKeyHash == [32]byte{} {
		t.Fatalf("cancel replay first=%#v second=%#v calls=%d err=%v", first, second, store.cancelCalls, err)
	}
	if first.State != "void_pending" {
		t.Fatalf("cancelled intent state = %q, want void_pending", first.State)
	}
}

type intentStoreFake struct {
	intent        application.IntentRecord
	lastReserve   application.ReserveIntentRequest
	reserveCalls  int
	finalizeErr   error
	secured       bool
	lastCancel    application.CancelIntentRequest
	cancelCalls   int
	finalizeCalls int
}

func (store *intentStoreFake) LookupIntentByIdempotency(_ context.Context, _ uuid.UUID, keyHash, fingerprint [32]byte) (application.IntentRecord, bool, error) {
	if store.intent.ID == uuid.Nil {
		return application.IntentRecord{}, false, nil
	}
	if keyHash != store.lastReserve.IdempotencyKeyHash || fingerprint != store.lastReserve.RequestFingerprint {
		return application.IntentRecord{}, false, application.ErrPaymentConflict
	}
	return store.intent, true, nil
}

func (store *intentStoreFake) ReserveIntent(_ context.Context, request application.ReserveIntentRequest) (application.IntentRecord, bool, error) {
	store.reserveCalls++
	store.lastReserve = request
	if store.intent.ID != uuid.Nil {
		return store.intent, true, nil
	}
	store.intent = application.IntentRecord{
		ID: request.PaymentIntentID, SagaID: request.SagaID, BeginCommandID: request.BeginCommandID, ReservationID: request.ReservationID,
		TrainRunID: request.TrainRunID, OwnerID: request.OwnerID, Provider: request.Provider,
		AmountMinor: request.AmountMinor, Currency: request.Currency, State: "reservation_securing",
	}
	return store.intent, false, nil
}

func (store *intentStoreFake) MarkReservationSecured(_ context.Context, intentID, commandID uuid.UUID, fingerprint [32]byte) (application.IntentRecord, error) {
	store.finalizeCalls++
	if intentID != store.intent.ID || commandID != store.intent.BeginCommandID || fingerprint != store.lastReserve.RequestFingerprint {
		return application.IntentRecord{}, application.ErrPaymentConflict
	}
	if store.secured {
		return store.intent, nil
	}
	if store.finalizeErr != nil {
		return store.intent, store.finalizeErr
	}
	store.secured = true
	store.intent.State = "checkout_pending"
	return store.intent, nil
}

func (store *intentStoreFake) GetOwnedIntent(_ context.Context, ownerID, intentID uuid.UUID) (application.IntentRecord, error) {
	if store.intent.ID != intentID || store.intent.OwnerID != ownerID {
		return application.IntentRecord{}, application.ErrPaymentNotFound
	}
	return store.intent, nil
}

func (store *intentStoreFake) RequestCancellation(_ context.Context, request application.CancelIntentRequest) (application.IntentRecord, error) {
	store.cancelCalls++
	if store.cancelCalls > 1 && (request.OwnerID != store.lastCancel.OwnerID || request.PaymentIntentID != store.lastCancel.PaymentIntentID ||
		request.IdempotencyKeyHash != store.lastCancel.IdempotencyKeyHash || request.RequestFingerprint != store.lastCancel.RequestFingerprint) {
		return application.IntentRecord{}, application.ErrPaymentConflict
	}
	if store.cancelCalls == 1 {
		store.lastCancel = request
	}
	store.intent.State = "void_pending"
	return store.intent, nil
}

type reservationGatewayFake struct {
	snapshot   application.ReservationSnapshot
	beginCalls int
	lastBegin  application.BeginPaymentCommand
}

func (gateway *reservationGatewayFake) GetPayableReservation(context.Context, uuid.UUID) (application.ReservationSnapshot, error) {
	return gateway.snapshot, nil
}

func (gateway *reservationGatewayFake) BeginPayment(_ context.Context, command application.BeginPaymentCommand) (application.BeginPaymentReceipt, error) {
	if gateway.beginCalls > 0 {
		return application.BeginPaymentReceipt{CommandID: gateway.lastBegin.CommandID, PaymentIntentID: gateway.lastBegin.PaymentIntentID, RequestFingerprint: gateway.lastBegin.RequestFingerprint}, nil
	}
	gateway.beginCalls++
	gateway.lastBegin = command
	return application.BeginPaymentReceipt{CommandID: command.CommandID, PaymentIntentID: command.PaymentIntentID, RequestFingerprint: command.RequestFingerprint}, nil
}
