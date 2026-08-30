package app

import (
	"context"
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/refund"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

type ticketRefundUseCases interface {
	Request(context.Context, uuid.UUID, uuid.UUID, refund.Request) (refund.RefundRequest, error)
	Get(context.Context, uuid.UUID, uuid.UUID) (refund.RefundRequest, error)
}

type ticketRefundMetrics interface {
	RecordPartialRefund(provider, result, reason, currency string, tickets int, duration time.Duration)
}

type TicketRefundService struct {
	useCases ticketRefundUseCases
	metrics  ticketRefundMetrics
}

func NewTicketRefundService(useCases ticketRefundUseCases, metrics ...ticketRefundMetrics) *TicketRefundService {
	service := &TicketRefundService{useCases: useCases}
	if len(metrics) > 0 {
		service.metrics = metrics[0]
	}
	return service
}

func (service *TicketRefundService) CreateTicketRefund(ctx context.Context, command httpapi.CreateTicketRefundCommand) (httpapi.TicketRefundView, error) {
	if service == nil || service.useCases == nil {
		return httpapi.TicketRefundView{}, httpapi.ErrPaymentNotEnabled
	}
	ownerID, orderID, err := parsePaymentIDs(command.OwnerID, command.TicketOrderID)
	if err != nil || len(command.TicketIDs) == 0 {
		return httpapi.TicketRefundView{}, httpapi.ErrInvalidInput
	}
	ticketIDs := make([]uuid.UUID, 0, len(command.TicketIDs))
	for _, raw := range command.TicketIDs {
		ticketID, parseErr := uuid.Parse(raw)
		if parseErr != nil {
			return httpapi.TicketRefundView{}, httpapi.ErrInvalidInput
		}
		ticketIDs = append(ticketIDs, ticketID)
	}
	started := time.Now()
	record, requestErr := service.useCases.Request(ctx, ownerID, orderID, refund.Request{
		TicketIDs: ticketIDs, IdempotencyKey: command.IdempotencyKey,
	})
	if requestErr != nil {
		return httpapi.TicketRefundView{}, mapTicketRefundError(requestErr)
	}
	result, reason, tickets := "accepted", "none", len(record.TicketIDs)
	if record.Replayed {
		result, reason, tickets = "duplicate", "duplicate", 0
	} else if record.State == refund.SagaManualReview {
		result, reason = "manual_review", "manual_review"
	}
	service.record(record.Provider, result, reason, record.Currency, tickets, started)
	return ticketRefundView(record), nil
}

func (service *TicketRefundService) GetTicketRefund(ctx context.Context, ownerRaw, requestRaw string) (httpapi.TicketRefundView, error) {
	if service == nil || service.useCases == nil {
		return httpapi.TicketRefundView{}, httpapi.ErrPaymentNotEnabled
	}
	ownerID, requestID, err := parsePaymentIDs(ownerRaw, requestRaw)
	if err != nil {
		return httpapi.TicketRefundView{}, httpapi.ErrInvalidInput
	}
	record, err := service.useCases.Get(ctx, ownerID, requestID)
	if err != nil {
		return httpapi.TicketRefundView{}, mapTicketRefundError(err)
	}
	return ticketRefundView(record), nil
}

func (service *TicketRefundService) record(provider, result, reason, currency string, tickets int, started time.Time) {
	if service != nil && service.metrics != nil {
		service.metrics.RecordPartialRefund(provider, result, reason, currency, tickets, time.Since(started))
	}
}

func ticketRefundView(record refund.RefundRequest) httpapi.TicketRefundView {
	ticketIDs := make([]string, 0, len(record.TicketIDs))
	for _, ticketID := range record.TicketIDs {
		ticketIDs = append(ticketIDs, ticketID.String())
	}
	var completedAt *time.Time
	if record.CompletedAt != nil {
		value := record.CompletedAt.UTC()
		completedAt = &value
	}
	return httpapi.TicketRefundView{
		ID: record.ID.String(), TicketOrderID: record.OrderID.String(), TicketIDs: ticketIDs,
		State: string(record.State), AmountMinor: record.AmountMinor, Currency: record.Currency,
		CreatedAt: record.CreatedAt.UTC(), UpdatedAt: record.UpdatedAt.UTC(), CompletedAt: completedAt,
	}
}

func mapTicketRefundError(err error) error {
	switch {
	case errors.Is(err, refund.ErrInvalidRequest):
		return httpapi.ErrInvalidInput
	case errors.Is(err, refund.ErrNotFound):
		return httpapi.ErrNotFound
	case errors.Is(err, refund.ErrIdempotencyConflict):
		return httpapi.ErrConflict
	case errors.Is(err, refund.ErrCutoffPassed), errors.Is(err, refund.ErrTicketUnavailable),
		errors.Is(err, refund.ErrCurrencyMismatch), errors.Is(err, refund.ErrAmountOverflow),
		errors.Is(err, refund.ErrRefundLimit), errors.Is(err, refund.ErrCapabilityUnavailable),
		errors.Is(err, refund.ErrSnapshotConflict):
		return httpapi.ErrRefundFailed
	default:
		return httpapi.ErrUnavailable
	}
}

var _ httpapi.TicketRefundService = (*TicketRefundService)(nil)
