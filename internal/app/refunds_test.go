package app

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/refund"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

func TestTicketRefundServiceUsesOwnerAndServerDerivedUseCase(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	ownerID, orderID, ticketID, requestID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	useCases := &ticketRefundUseCasesFake{result: refund.RefundRequest{
		ID: requestID, OrderID: orderID, OwnerID: ownerID, TicketIDs: []uuid.UUID{ticketID},
		State: refund.SagaCreated, AmountMinor: 500, Currency: "TWD", CreatedAt: now, UpdatedAt: now,
	}}
	service := NewTicketRefundService(useCases)
	view, err := service.CreateTicketRefund(context.Background(), httpapi.CreateTicketRefundCommand{
		OwnerID: ownerID.String(), TicketOrderID: orderID.String(), TicketIDs: []string{ticketID.String()},
		IdempotencyKey: "refund-001",
	})
	if err != nil {
		t.Fatalf("CreateTicketRefund: %v", err)
	}
	if useCases.ownerID != ownerID || useCases.orderID != orderID || useCases.request.IdempotencyKey != "refund-001" ||
		len(useCases.request.TicketIDs) != 1 || useCases.request.TicketIDs[0] != ticketID {
		t.Fatalf("use case input = owner=%s order=%s request=%+v", useCases.ownerID, useCases.orderID, useCases.request)
	}
	if view.ID != requestID.String() || view.TicketOrderID != orderID.String() || view.AmountMinor != 500 || view.Currency != "TWD" || view.State != string(refund.SagaCreated) {
		t.Fatalf("view = %+v", view)
	}
}

func TestTicketRefundServiceRecordsDurableSelectionWithoutReplayInflation(t *testing.T) {
	t.Parallel()
	ownerID, orderID := uuid.New(), uuid.New()
	tickets := []uuid.UUID{uuid.New(), uuid.New()}
	metric := &ticketRefundMetricsFake{}
	useCases := &ticketRefundUseCasesFake{result: refund.RefundRequest{
		ID: uuid.New(), OrderID: orderID, OwnerID: ownerID, TicketIDs: tickets,
		Provider: "stripe", Currency: "TWD", State: refund.SagaCreated,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}}
	service := NewTicketRefundService(useCases, metric)
	command := httpapi.CreateTicketRefundCommand{
		OwnerID: ownerID.String(), TicketOrderID: orderID.String(),
		TicketIDs: []string{tickets[0].String(), tickets[1].String()}, IdempotencyKey: "refund-metric",
	}
	if _, err := service.CreateTicketRefund(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	useCases.result.Replayed = true
	if _, err := service.CreateTicketRefund(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if len(metric.observations) != 2 || metric.observations[0] != "stripe:accepted:none:TWD:2" ||
		metric.observations[1] != "stripe:duplicate:duplicate:TWD:0" {
		t.Fatalf("observations = %v", metric.observations)
	}
}

func TestTicketRefundServiceMapsBoundedErrors(t *testing.T) {
	t.Parallel()
	ownerID, orderID, ticketID := uuid.New(), uuid.New(), uuid.New()
	tests := []struct {
		err  error
		want error
	}{
		{err: refund.ErrNotFound, want: httpapi.ErrNotFound},
		{err: refund.ErrIdempotencyConflict, want: httpapi.ErrConflict},
		{err: refund.ErrTicketUnavailable, want: httpapi.ErrRefundFailed},
		{err: refund.ErrCapabilityUnavailable, want: httpapi.ErrRefundFailed},
		{err: errors.New("postgres unavailable"), want: httpapi.ErrUnavailable},
	}
	for _, test := range tests {
		service := NewTicketRefundService(&ticketRefundUseCasesFake{err: test.err})
		_, err := service.CreateTicketRefund(context.Background(), httpapi.CreateTicketRefundCommand{
			OwnerID: ownerID.String(), TicketOrderID: orderID.String(), TicketIDs: []string{ticketID.String()}, IdempotencyKey: "refund-001",
		})
		if !errors.Is(err, test.want) {
			t.Fatalf("input error=%v got=%v want=%v", test.err, err, test.want)
		}
	}
}

type ticketRefundUseCasesFake struct {
	ownerID uuid.UUID
	orderID uuid.UUID
	request refund.Request
	result  refund.RefundRequest
	err     error
}

type ticketRefundMetricsFake struct{ observations []string }

func (fake *ticketRefundMetricsFake) RecordPartialRefund(provider, result, reason, currency string, tickets int, _ time.Duration) {
	fake.observations = append(fake.observations, provider+":"+result+":"+reason+":"+currency+":"+fmt.Sprint(tickets))
}

func (fake *ticketRefundUseCasesFake) Request(_ context.Context, ownerID, orderID uuid.UUID, request refund.Request) (refund.RefundRequest, error) {
	fake.ownerID, fake.orderID, fake.request = ownerID, orderID, request
	return fake.result, fake.err
}

func (fake *ticketRefundUseCasesFake) Get(_ context.Context, _, _ uuid.UUID) (refund.RefundRequest, error) {
	return fake.result, fake.err
}
