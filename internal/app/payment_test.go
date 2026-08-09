package app

import (
	"context"
	"errors"
	"testing"
	"time"

	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

func TestPaymentServiceMapsOwnerScopedCommandsAndSafeView(t *testing.T) {
	t.Parallel()
	owner, reservationID, intentID := uuid.New(), uuid.New(), uuid.New()
	createdAt := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	useCases := &paymentUseCasesFake{record: paymentapp.IntentRecord{
		ID: intentID, ReservationID: reservationID, OwnerID: owner, TrainRunID: uuid.New(),
		Provider: "sandbox", ProviderPaymentID: "internal-provider-id", HostedSessionRef: "checkout-session-1",
		AmountMinor: 12500, Currency: "TWD", State: "awaiting_customer", CreatedAt: createdAt, UpdatedAt: createdAt,
	}}
	service := NewPaymentService(useCases)
	view, err := service.CreatePaymentIntent(context.Background(), httpapi.CreatePaymentIntentCommand{
		OwnerID: owner.String(), ReservationID: reservationID.String(), IdempotencyKey: "payment-request-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if useCases.created.OwnerID != owner || useCases.created.ReservationID != reservationID || view.ID != intentID.String() || view.HostedSessionRef != "checkout-session-1" {
		t.Fatalf("command=%#v view=%#v", useCases.created, view)
	}
	if view.ReservationID != reservationID.String() || view.AmountMinor != 12500 || view.Currency != "TWD" || view.State != "awaiting_customer" {
		t.Fatalf("safe view=%#v", view)
	}
	cancelled, err := service.CancelReservationPayment(context.Background(), httpapi.CancelReservationPaymentCommand{
		OwnerID: owner.String(), ReservationID: reservationID.String(), IdempotencyKey: "cancel-payment-1",
	})
	if err != nil || cancelled.ID != intentID.String() || useCases.reservationCancelled.ReservationID != reservationID {
		t.Fatalf("cancelled=%#v command=%#v error=%v", cancelled, useCases.reservationCancelled, err)
	}
}

func TestPaymentServiceReturnsStableResourceForDeferredFinalization(t *testing.T) {
	t.Parallel()
	owner, reservationID, intentID := uuid.New(), uuid.New(), uuid.New()
	useCases := &paymentUseCasesFake{record: paymentapp.IntentRecord{ID: intentID, OwnerID: owner, ReservationID: reservationID, AmountMinor: 1, Currency: "TWD", State: "reservation_securing"}, err: paymentapp.ErrControlFinalizationDeferred}
	view, err := NewPaymentService(useCases).CreatePaymentIntent(context.Background(), httpapi.CreatePaymentIntentCommand{OwnerID: owner.String(), ReservationID: reservationID.String(), IdempotencyKey: "payment-request-1"})
	if err != nil || view.ID != intentID.String() {
		t.Fatalf("view=%#v error=%v", view, err)
	}
}

func TestPaymentServiceRecordsMeasuredCreateIntentOutcome(t *testing.T) {
	t.Parallel()
	owner, reservationID := uuid.New(), uuid.New()
	metrics := &paymentIntentMetricsFake{}
	useCases := &paymentUseCasesFake{delay: 2 * time.Millisecond, record: paymentapp.IntentRecord{
		ID: uuid.New(), OwnerID: owner, ReservationID: reservationID,
		State: "reservation_securing", Currency: "TWD",
	}}
	_, err := NewPaymentService(useCases, metrics).CreatePaymentIntent(context.Background(), httpapi.CreatePaymentIntentCommand{
		OwnerID: owner.String(), ReservationID: reservationID.String(), IdempotencyKey: "payment-request-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if metrics.calls != 1 || metrics.state != "reservation_securing" || metrics.result != "success" || metrics.duration <= 0 {
		t.Fatalf("metrics = calls %d state %q result %q duration %s", metrics.calls, metrics.state, metrics.result, metrics.duration)
	}
}

func TestPaymentServiceMapsBoundedErrors(t *testing.T) {
	t.Parallel()
	owner, intentID := uuid.New(), uuid.New()
	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "not found", err: paymentapp.ErrPaymentNotFound, want: httpapi.ErrNotFound},
		{name: "not payable", err: paymentapp.ErrReservationNotPayable, want: httpapi.ErrReservationNotPayable},
		{name: "conflict", err: paymentapp.ErrPaymentConflict, want: httpapi.ErrPaymentIntentConflict},
		{name: "unavailable", err: errors.New("postgres://secret"), want: httpapi.ErrPaymentProviderUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := NewPaymentService(&paymentUseCasesFake{err: test.err})
			_, err := service.GetPaymentIntent(context.Background(), owner.String(), intentID.String())
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
		})
	}
}

type paymentUseCasesFake struct {
	created              paymentapp.CreateIntentCommand
	cancelled            paymentapp.CancelIntentCommand
	reservationCancelled paymentapp.CancelReservationCommand
	record               paymentapp.IntentRecord
	err                  error
	delay                time.Duration
}

type paymentIntentMetricsFake struct {
	state, result string
	duration      time.Duration
	calls         int
}

func (fake *paymentIntentMetricsFake) RecordPaymentIntent(state, result string, duration time.Duration) {
	fake.calls++
	fake.state, fake.result, fake.duration = state, result, duration
}

func (fake *paymentUseCasesFake) CreateIntent(_ context.Context, command paymentapp.CreateIntentCommand) (paymentapp.IntentRecord, error) {
	fake.created = command
	if fake.delay > 0 {
		time.Sleep(fake.delay)
	}
	return fake.record, fake.err
}

func (fake *paymentUseCasesFake) GetIntent(context.Context, uuid.UUID, uuid.UUID) (paymentapp.IntentRecord, error) {
	return fake.record, fake.err
}

func (fake *paymentUseCasesFake) CancelIntent(_ context.Context, command paymentapp.CancelIntentCommand) (paymentapp.IntentRecord, error) {
	fake.cancelled = command
	return fake.record, fake.err
}

func (fake *paymentUseCasesFake) CancelReservation(_ context.Context, command paymentapp.CancelReservationCommand) (paymentapp.IntentRecord, error) {
	fake.reservationCancelled = command
	return fake.record, fake.err
}
