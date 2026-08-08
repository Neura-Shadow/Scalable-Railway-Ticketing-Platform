package app

import (
	"context"
	"errors"
	"time"

	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

type paymentUseCases interface {
	CreateIntent(context.Context, paymentapp.CreateIntentCommand) (paymentapp.IntentRecord, error)
	GetIntent(context.Context, uuid.UUID, uuid.UUID) (paymentapp.IntentRecord, error)
	CancelIntent(context.Context, paymentapp.CancelIntentCommand) (paymentapp.IntentRecord, error)
	CancelReservation(context.Context, paymentapp.CancelReservationCommand) (paymentapp.IntentRecord, error)
}

type PaymentService struct{ useCases paymentUseCases }

func NewPaymentService(useCases paymentUseCases) *PaymentService {
	return &PaymentService{useCases: useCases}
}

func (service *PaymentService) CreatePaymentIntent(ctx context.Context, command httpapi.CreatePaymentIntentCommand) (httpapi.PaymentIntentView, error) {
	owner, reservation, err := parsePaymentIDs(command.OwnerID, command.ReservationID)
	if err != nil {
		return httpapi.PaymentIntentView{}, err
	}
	if service == nil || service.useCases == nil {
		return httpapi.PaymentIntentView{}, httpapi.ErrPaymentNotEnabled
	}
	record, useCaseErr := service.useCases.CreateIntent(ctx, paymentapp.CreateIntentCommand{
		OwnerID: owner, ReservationID: reservation, IdempotencyKey: command.IdempotencyKey,
	})
	if useCaseErr != nil {
		if record.ID != uuid.Nil && (errors.Is(useCaseErr, paymentapp.ErrControlFinalizationDeferred) || errors.Is(useCaseErr, paymentapp.ErrShardPaymentCommandDeferred)) {
			return paymentIntentView(record), nil
		}
		return httpapi.PaymentIntentView{}, mapPaymentError(useCaseErr)
	}
	return paymentIntentView(record), nil
}

func (service *PaymentService) GetPaymentIntent(ctx context.Context, ownerID, paymentIntentID string) (httpapi.PaymentIntentView, error) {
	owner, intentID, err := parsePaymentIDs(ownerID, paymentIntentID)
	if err != nil {
		return httpapi.PaymentIntentView{}, err
	}
	if service == nil || service.useCases == nil {
		return httpapi.PaymentIntentView{}, httpapi.ErrPaymentNotEnabled
	}
	record, err := service.useCases.GetIntent(ctx, owner, intentID)
	if err != nil {
		return httpapi.PaymentIntentView{}, mapPaymentError(err)
	}
	return paymentIntentView(record), nil
}

func (service *PaymentService) CancelPaymentIntent(ctx context.Context, command httpapi.CancelPaymentIntentCommand) (httpapi.PaymentIntentView, error) {
	owner, intentID, err := parsePaymentIDs(command.OwnerID, command.PaymentIntentID)
	if err != nil {
		return httpapi.PaymentIntentView{}, err
	}
	if service == nil || service.useCases == nil {
		return httpapi.PaymentIntentView{}, httpapi.ErrPaymentNotEnabled
	}
	record, err := service.useCases.CancelIntent(ctx, paymentapp.CancelIntentCommand{
		OwnerID: owner, PaymentIntentID: intentID, IdempotencyKey: command.IdempotencyKey,
	})
	if err != nil {
		return httpapi.PaymentIntentView{}, mapPaymentError(err)
	}
	return paymentIntentView(record), nil
}

func (service *PaymentService) CancelReservationPayment(ctx context.Context, command httpapi.CancelReservationPaymentCommand) (httpapi.PaymentIntentView, error) {
	owner, reservationID, err := parsePaymentIDs(command.OwnerID, command.ReservationID)
	if err != nil {
		return httpapi.PaymentIntentView{}, err
	}
	if service == nil || service.useCases == nil {
		return httpapi.PaymentIntentView{}, httpapi.ErrPaymentNotEnabled
	}
	record, err := service.useCases.CancelReservation(ctx, paymentapp.CancelReservationCommand{
		OwnerID: owner, ReservationID: reservationID, IdempotencyKey: command.IdempotencyKey,
	})
	if err != nil {
		return httpapi.PaymentIntentView{}, mapPaymentError(err)
	}
	return paymentIntentView(record), nil
}

func parsePaymentIDs(left, right string) (uuid.UUID, uuid.UUID, error) {
	first, err := uuid.Parse(left)
	if err != nil {
		return uuid.Nil, uuid.Nil, httpapi.ErrInvalidInput
	}
	second, err := uuid.Parse(right)
	if err != nil {
		return uuid.Nil, uuid.Nil, httpapi.ErrInvalidInput
	}
	return first, second, nil
}

func paymentIntentView(record paymentapp.IntentRecord) httpapi.PaymentIntentView {
	var completedAt *time.Time
	if record.CompletedAt != nil {
		value := record.CompletedAt.UTC()
		completedAt = &value
	}
	return httpapi.PaymentIntentView{
		ID: record.ID.String(), ReservationID: record.ReservationID.String(), State: record.State,
		AmountMinor: record.AmountMinor, Currency: record.Currency, HostedSessionRef: record.HostedSessionRef,
		CreatedAt: record.CreatedAt.UTC(), UpdatedAt: record.UpdatedAt.UTC(), CompletedAt: completedAt,
	}
}

func mapPaymentError(err error) error {
	switch {
	case errors.Is(err, paymentapp.ErrInvalidPaymentRequest):
		return httpapi.ErrInvalidInput
	case errors.Is(err, paymentapp.ErrPaymentNotFound):
		return httpapi.ErrNotFound
	case errors.Is(err, paymentapp.ErrPaymentConflict):
		return httpapi.ErrPaymentIntentConflict
	case errors.Is(err, paymentapp.ErrReservationNotPayable):
		return httpapi.ErrReservationNotPayable
	case errors.Is(err, paymentapp.ErrControlFinalizationDeferred), errors.Is(err, paymentapp.ErrShardPaymentCommandDeferred):
		return httpapi.ErrPaymentProcessing
	default:
		return httpapi.ErrPaymentProviderUnavailable
	}
}

var _ httpapi.PaymentService = (*PaymentService)(nil)
var _ httpapi.ReservationPaymentCancellationService = (*PaymentService)(nil)
