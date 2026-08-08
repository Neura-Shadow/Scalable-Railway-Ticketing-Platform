// Package shard keeps payment reservation and ticket transitions bound to the
// current authoritative physical shard. Payment intents never retain a shard
// identity and every command re-resolves the reservation directory.
package shard

import (
	"context"
	"errors"

	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

var (
	ErrInvalidGateway          = errors.New("invalid payment shard gateway")
	ErrShardPaymentUnavailable = errors.New("payment shard unavailable")
)

type Directory interface {
	ResolveReservation(context.Context, uuid.UUID, bool) (sharding.ShardRoute, error)
}

type Store interface {
	GetPayableReservation(context.Context, sharding.ShardRoute, uuid.UUID) (paymentapp.ReservationSnapshot, error)
	BeginPayment(context.Context, sharding.ShardRoute, paymentapp.BeginPaymentCommand) (paymentapp.BeginPaymentReceipt, error)
	IssueTickets(context.Context, sharding.ShardRoute, IssueTicketsCommand) (IssueTicketsReceipt, error)
	MarkRefundPending(context.Context, sharding.ShardRoute, MarkRefundPendingCommand) (MarkRefundPendingReceipt, error)
	CancelVoidedReservation(context.Context, sharding.ShardRoute, CancelVoidedReservationCommand) (CancelVoidedReservationReceipt, error)
	ApplyRefundCompensation(context.Context, sharding.ShardRoute, ApplyRefundCompensationCommand) (ApplyRefundCompensationReceipt, error)
}

func (gateway *Gateway) CancelVoidedReservation(ctx context.Context, command CancelVoidedReservationCommand) (CancelVoidedReservationReceipt, error) {
	if gateway == nil || ctx == nil || command.ReservationID == uuid.Nil || command.TrainRunID == uuid.Nil {
		return CancelVoidedReservationReceipt{}, paymentapp.ErrReservationNotPayable
	}
	route, err := gateway.directory.ResolveReservation(ctx, command.ReservationID, false)
	if err != nil || route.TrainRunID() != command.TrainRunID {
		return CancelVoidedReservationReceipt{}, mapShardError(err)
	}
	receipt, err := gateway.store.CancelVoidedReservation(ctx, route, command)
	if errors.Is(err, sharding.ErrAssignmentStale) || errors.Is(err, sharding.ErrWriteFenced) {
		refreshed, refreshErr := gateway.directory.ResolveReservation(ctx, command.ReservationID, true)
		if refreshErr != nil || refreshed.TrainRunID() != command.TrainRunID {
			return CancelVoidedReservationReceipt{}, ErrShardPaymentUnavailable
		}
		return gateway.store.CancelVoidedReservation(ctx, refreshed, command)
	}
	if err != nil {
		return CancelVoidedReservationReceipt{}, mapShardError(err)
	}
	return receipt, nil
}

func (gateway *Gateway) MarkRefundPending(ctx context.Context, command MarkRefundPendingCommand) (MarkRefundPendingReceipt, error) {
	if gateway == nil || ctx == nil || command.ReservationID == uuid.Nil || command.TrainRunID == uuid.Nil {
		return MarkRefundPendingReceipt{}, paymentapp.ErrReservationNotPayable
	}
	route, err := gateway.directory.ResolveReservation(ctx, command.ReservationID, false)
	if err != nil || route.TrainRunID() != command.TrainRunID {
		return MarkRefundPendingReceipt{}, mapShardError(err)
	}
	receipt, err := gateway.store.MarkRefundPending(ctx, route, command)
	if errors.Is(err, sharding.ErrAssignmentStale) || errors.Is(err, sharding.ErrWriteFenced) {
		refreshed, refreshErr := gateway.directory.ResolveReservation(ctx, command.ReservationID, true)
		if refreshErr != nil || refreshed.TrainRunID() != command.TrainRunID {
			return MarkRefundPendingReceipt{}, ErrShardPaymentUnavailable
		}
		return gateway.store.MarkRefundPending(ctx, refreshed, command)
	}
	if err != nil {
		return MarkRefundPendingReceipt{}, mapShardError(err)
	}
	return receipt, nil
}

func (gateway *Gateway) ApplyRefundCompensation(ctx context.Context, command ApplyRefundCompensationCommand) (ApplyRefundCompensationReceipt, error) {
	if gateway == nil || ctx == nil || command.ReservationID == uuid.Nil || command.TrainRunID == uuid.Nil {
		return ApplyRefundCompensationReceipt{}, paymentapp.ErrReservationNotPayable
	}
	route, err := gateway.directory.ResolveReservation(ctx, command.ReservationID, false)
	if err != nil || route.TrainRunID() != command.TrainRunID {
		return ApplyRefundCompensationReceipt{}, mapShardError(err)
	}
	receipt, err := gateway.store.ApplyRefundCompensation(ctx, route, command)
	if errors.Is(err, sharding.ErrAssignmentStale) || errors.Is(err, sharding.ErrWriteFenced) {
		refreshed, refreshErr := gateway.directory.ResolveReservation(ctx, command.ReservationID, true)
		if refreshErr != nil || refreshed.TrainRunID() != command.TrainRunID {
			return ApplyRefundCompensationReceipt{}, ErrShardPaymentUnavailable
		}
		return gateway.store.ApplyRefundCompensation(ctx, refreshed, command)
	}
	if err != nil {
		return ApplyRefundCompensationReceipt{}, mapShardError(err)
	}
	return receipt, nil
}

func (gateway *Gateway) IssueTickets(ctx context.Context, command IssueTicketsCommand) (IssueTicketsReceipt, error) {
	if gateway == nil || ctx == nil || command.ReservationID == uuid.Nil || command.TrainRunID == uuid.Nil {
		return IssueTicketsReceipt{}, paymentapp.ErrReservationNotPayable
	}
	route, err := gateway.directory.ResolveReservation(ctx, command.ReservationID, false)
	if err != nil || route.TrainRunID() != command.TrainRunID {
		return IssueTicketsReceipt{}, mapShardError(err)
	}
	receipt, err := gateway.store.IssueTickets(ctx, route, command)
	if errors.Is(err, sharding.ErrAssignmentStale) || errors.Is(err, sharding.ErrWriteFenced) {
		refreshed, refreshErr := gateway.directory.ResolveReservation(ctx, command.ReservationID, true)
		if refreshErr != nil || refreshed.TrainRunID() != command.TrainRunID {
			return IssueTicketsReceipt{}, ErrShardPaymentUnavailable
		}
		return gateway.store.IssueTickets(ctx, refreshed, command)
	}
	if err != nil {
		return IssueTicketsReceipt{}, mapShardError(err)
	}
	return receipt, nil
}

type Gateway struct {
	directory Directory
	store     Store
}

func NewGateway(directory Directory, store Store) (*Gateway, error) {
	if directory == nil || store == nil {
		return nil, ErrInvalidGateway
	}
	return &Gateway{directory: directory, store: store}, nil
}

func (gateway *Gateway) GetPayableReservation(ctx context.Context, reservationID uuid.UUID) (paymentapp.ReservationSnapshot, error) {
	if gateway == nil || ctx == nil || reservationID == uuid.Nil {
		return paymentapp.ReservationSnapshot{}, paymentapp.ErrPaymentNotFound
	}
	route, err := gateway.directory.ResolveReservation(ctx, reservationID, false)
	if err != nil {
		return paymentapp.ReservationSnapshot{}, mapShardError(err)
	}
	snapshot, err := gateway.store.GetPayableReservation(ctx, route, reservationID)
	if errors.Is(err, sharding.ErrAssignmentStale) || errors.Is(err, sharding.ErrWriteFenced) {
		route, err = gateway.directory.ResolveReservation(ctx, reservationID, true)
		if err == nil {
			snapshot, err = gateway.store.GetPayableReservation(ctx, route, reservationID)
		}
	}
	if err != nil {
		return paymentapp.ReservationSnapshot{}, mapShardError(err)
	}
	return snapshot, nil
}

func (gateway *Gateway) BeginPayment(ctx context.Context, command paymentapp.BeginPaymentCommand) (paymentapp.BeginPaymentReceipt, error) {
	if gateway == nil || ctx == nil || command.ReservationID == uuid.Nil || command.TrainRunID == uuid.Nil {
		return paymentapp.BeginPaymentReceipt{}, paymentapp.ErrReservationNotPayable
	}
	route, err := gateway.directory.ResolveReservation(ctx, command.ReservationID, false)
	if err != nil || route.TrainRunID() != command.TrainRunID {
		return paymentapp.BeginPaymentReceipt{}, mapShardError(err)
	}
	receipt, err := gateway.store.BeginPayment(ctx, route, command)
	if errors.Is(err, sharding.ErrAssignmentStale) || errors.Is(err, sharding.ErrWriteFenced) {
		refreshed, refreshErr := gateway.directory.ResolveReservation(ctx, command.ReservationID, true)
		if refreshErr != nil || refreshed.TrainRunID() != command.TrainRunID {
			return paymentapp.BeginPaymentReceipt{}, ErrShardPaymentUnavailable
		}
		return gateway.store.BeginPayment(ctx, refreshed, command)
	}
	if err != nil {
		return paymentapp.BeginPaymentReceipt{}, mapShardError(err)
	}
	return receipt, nil
}

func mapShardError(err error) error {
	switch {
	case errors.Is(err, paymentapp.ErrPaymentNotFound):
		return paymentapp.ErrPaymentNotFound
	case errors.Is(err, paymentapp.ErrReservationNotPayable):
		return paymentapp.ErrReservationNotPayable
	case errors.Is(err, paymentapp.ErrPaymentConflict):
		return paymentapp.ErrPaymentConflict
	default:
		return ErrShardPaymentUnavailable
	}
}

var _ paymentapp.ReservationGateway = (*Gateway)(nil)
