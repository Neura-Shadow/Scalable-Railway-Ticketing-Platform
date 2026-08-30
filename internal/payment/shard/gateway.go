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
	ErrInvalidGateway            = errors.New("invalid payment shard gateway")
	ErrShardPaymentUnavailable   = errors.New("payment shard unavailable")
	ErrTicketPlanUnavailable     = errors.New("ticket issue plan unavailable")
	ErrTicketClaimUnavailable    = errors.New("ticket code claim unavailable")
	ErrTicketClaimReadFailed     = errors.New("ticket code claim authorization unavailable")
	ErrTicketClaimReadTimeout    = errors.New("ticket code claim authorization timeout")
	ErrTicketClaimSQLFailed      = errors.New("ticket code claim authorization query failed")
	ErrTicketClaimScanFailed     = errors.New("ticket code claim authorization scan failed")
	ErrTicketClaimDecodeFailed   = errors.New("ticket code claim authorization decode failed")
	ErrTicketClaimUnauthorized   = errors.New("ticket code claim not authorized")
	ErrTicketClaimConflict       = errors.New("ticket code claim conflict")
	ErrTicketClaimCommitFailed   = errors.New("ticket code claim commit unavailable")
	ErrTicketIssueUnavailable    = errors.New("ticket issue command unavailable")
	ErrSelectedRefundUnavailable = errors.New("selected ticket refund command unavailable")
)

type Directory interface {
	ResolveReservation(context.Context, uuid.UUID, bool) (sharding.ShardRoute, error)
}

type Store interface {
	GetPayableReservation(context.Context, sharding.ShardRoute, uuid.UUID) (paymentapp.ReservationSnapshot, error)
	BeginPayment(context.Context, sharding.ShardRoute, paymentapp.BeginPaymentCommand) (paymentapp.BeginPaymentReceipt, error)
	PlanTicketIssue(context.Context, sharding.ShardRoute, IssueTicketsCommand) (TicketIdentityPlan, error)
	IssueTickets(context.Context, sharding.ShardRoute, IssueTicketsCommand) (IssueTicketsReceipt, error)
	MarkRefundPending(context.Context, sharding.ShardRoute, MarkRefundPendingCommand) (MarkRefundPendingReceipt, error)
	CancelVoidedReservation(context.Context, sharding.ShardRoute, CancelVoidedReservationCommand) (CancelVoidedReservationReceipt, error)
	ApplyRefundCompensation(context.Context, sharding.ShardRoute, ApplyRefundCompensationCommand) (ApplyRefundCompensationReceipt, error)
}

// TicketCodeClaimer owns the control-plane uniqueness boundary. Claims are
// immutable tombstones: a shard issue is never attempted until every planned
// ticket identity has been exclusively reserved in control PostgreSQL.
type TicketCodeClaimer interface {
	ClaimTicketCodes(context.Context, IssueTicketsCommand, TicketIdentityPlan) error
}

type SelectedRefundStore interface {
	ApplySelectedTicketRefund(context.Context, sharding.ShardRoute, ApplySelectedTicketRefundCommand) (SelectedTicketRefundReceipt, error)
}

type RefundOrderStore interface {
	LoadRefundOrder(context.Context, sharding.ShardRoute, uuid.UUID, uuid.UUID) (RefundOrderSnapshot, error)
}

type PreparedRefundStore interface {
	PrepareSelectedTicketRefund(context.Context, sharding.ShardRoute, PrepareSelectedTicketRefundCommand) (SelectedTicketRefundPrepareReceipt, error)
	ReleaseSelectedTicketRefund(context.Context, sharding.ShardRoute, ReleaseSelectedTicketRefundCommand) (SelectedTicketRefundReleaseReceipt, error)
}

func (gateway *Gateway) PrepareSelectedTicketRefund(ctx context.Context, command PrepareSelectedTicketRefundCommand) (SelectedTicketRefundPrepareReceipt, error) {
	if gateway == nil || ctx == nil || command.ReservationID == uuid.Nil || command.TrainRunID == uuid.Nil {
		return SelectedTicketRefundPrepareReceipt{}, ErrSelectedRefundUnavailable
	}
	store, ok := gateway.store.(PreparedRefundStore)
	if !ok {
		return SelectedTicketRefundPrepareReceipt{}, ErrSelectedRefundUnavailable
	}
	route, err := gateway.directory.ResolveReservation(ctx, command.ReservationID, false)
	if err != nil || route.TrainRunID() != command.TrainRunID {
		return SelectedTicketRefundPrepareReceipt{}, mapShardError(err)
	}
	receipt, err := store.PrepareSelectedTicketRefund(ctx, route, command)
	if errors.Is(err, sharding.ErrAssignmentStale) || errors.Is(err, sharding.ErrWriteFenced) {
		refreshed, refreshErr := gateway.directory.ResolveReservation(ctx, command.ReservationID, true)
		if refreshErr != nil || refreshed.TrainRunID() != command.TrainRunID {
			return SelectedTicketRefundPrepareReceipt{}, ErrShardPaymentUnavailable
		}
		return store.PrepareSelectedTicketRefund(ctx, refreshed, command)
	}
	if err != nil {
		return SelectedTicketRefundPrepareReceipt{}, mapShardError(err)
	}
	return receipt, nil
}

func (gateway *Gateway) ReleaseSelectedTicketRefund(ctx context.Context, command ReleaseSelectedTicketRefundCommand) (SelectedTicketRefundReleaseReceipt, error) {
	if gateway == nil || ctx == nil || command.ReservationID == uuid.Nil || command.TrainRunID == uuid.Nil {
		return SelectedTicketRefundReleaseReceipt{}, ErrSelectedRefundUnavailable
	}
	store, ok := gateway.store.(PreparedRefundStore)
	if !ok {
		return SelectedTicketRefundReleaseReceipt{}, ErrSelectedRefundUnavailable
	}
	route, err := gateway.directory.ResolveReservation(ctx, command.ReservationID, false)
	if err != nil || route.TrainRunID() != command.TrainRunID {
		return SelectedTicketRefundReleaseReceipt{}, mapShardError(err)
	}
	receipt, err := store.ReleaseSelectedTicketRefund(ctx, route, command)
	if errors.Is(err, sharding.ErrAssignmentStale) || errors.Is(err, sharding.ErrWriteFenced) {
		refreshed, refreshErr := gateway.directory.ResolveReservation(ctx, command.ReservationID, true)
		if refreshErr != nil || refreshed.TrainRunID() != command.TrainRunID {
			return SelectedTicketRefundReleaseReceipt{}, ErrShardPaymentUnavailable
		}
		return store.ReleaseSelectedTicketRefund(ctx, refreshed, command)
	}
	if err != nil {
		return SelectedTicketRefundReleaseReceipt{}, mapShardError(err)
	}
	return receipt, nil
}

func (gateway *Gateway) ApplySelectedTicketRefund(ctx context.Context, command ApplySelectedTicketRefundCommand) (SelectedTicketRefundReceipt, error) {
	if gateway == nil || ctx == nil || command.ReservationID == uuid.Nil || command.TrainRunID == uuid.Nil {
		return SelectedTicketRefundReceipt{}, ErrSelectedRefundUnavailable
	}
	store, ok := gateway.store.(SelectedRefundStore)
	if !ok {
		return SelectedTicketRefundReceipt{}, ErrSelectedRefundUnavailable
	}
	route, err := gateway.directory.ResolveReservation(ctx, command.ReservationID, false)
	if err != nil || route.TrainRunID() != command.TrainRunID {
		return SelectedTicketRefundReceipt{}, mapShardError(err)
	}
	receipt, err := store.ApplySelectedTicketRefund(ctx, route, command)
	if errors.Is(err, sharding.ErrAssignmentStale) || errors.Is(err, sharding.ErrWriteFenced) {
		refreshed, refreshErr := gateway.directory.ResolveReservation(ctx, command.ReservationID, true)
		if refreshErr != nil || refreshed.TrainRunID() != command.TrainRunID {
			return SelectedTicketRefundReceipt{}, ErrShardPaymentUnavailable
		}
		return store.ApplySelectedTicketRefund(ctx, refreshed, command)
	}
	if err != nil {
		return SelectedTicketRefundReceipt{}, mapShardError(err)
	}
	return receipt, nil
}

func (gateway *Gateway) LoadRefundOrder(ctx context.Context, reservationID, orderID, ownerID uuid.UUID) (RefundOrderSnapshot, error) {
	if gateway == nil || ctx == nil || reservationID == uuid.Nil || orderID == uuid.Nil || ownerID == uuid.Nil {
		return RefundOrderSnapshot{}, ErrSelectedRefundUnavailable
	}
	store, ok := gateway.store.(RefundOrderStore)
	if !ok {
		return RefundOrderSnapshot{}, ErrSelectedRefundUnavailable
	}
	route, err := gateway.directory.ResolveReservation(ctx, reservationID, false)
	if err != nil {
		return RefundOrderSnapshot{}, mapShardError(err)
	}
	snapshot, err := store.LoadRefundOrder(ctx, route, orderID, ownerID)
	if errors.Is(err, sharding.ErrAssignmentStale) || errors.Is(err, sharding.ErrWriteFenced) {
		refreshed, refreshErr := gateway.directory.ResolveReservation(ctx, reservationID, true)
		if refreshErr == nil {
			snapshot, err = store.LoadRefundOrder(ctx, refreshed, orderID, ownerID)
		}
	}
	if err != nil {
		return RefundOrderSnapshot{}, mapShardError(err)
	}
	return snapshot, nil
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
	if gateway == nil || ctx == nil || gateway.ticketCodes == nil || command.ReservationID == uuid.Nil || command.TrainRunID == uuid.Nil {
		return IssueTicketsReceipt{}, paymentapp.ErrReservationNotPayable
	}
	route, err := gateway.directory.ResolveReservation(ctx, command.ReservationID, false)
	if err != nil || route.TrainRunID() != command.TrainRunID {
		return IssueTicketsReceipt{}, mapShardError(err)
	}
	plan, err := gateway.store.PlanTicketIssue(ctx, route, command)
	if err != nil {
		return IssueTicketsReceipt{}, errors.Join(ErrTicketPlanUnavailable, mapShardError(err))
	}
	if err := gateway.ticketCodes.ClaimTicketCodes(ctx, command, plan); err != nil {
		return IssueTicketsReceipt{}, errors.Join(ErrTicketClaimUnavailable, err)
	}
	command.PlannedTicketIDs = append([]uuid.UUID(nil), plan.TicketIDs...)
	command.PlannedTicketCodes = append([]string(nil), plan.TicketCodes...)
	receipt, err := gateway.store.IssueTickets(ctx, route, command)
	if errors.Is(err, sharding.ErrAssignmentStale) || errors.Is(err, sharding.ErrWriteFenced) {
		refreshed, refreshErr := gateway.directory.ResolveReservation(ctx, command.ReservationID, true)
		if refreshErr != nil || refreshed.TrainRunID() != command.TrainRunID {
			return IssueTicketsReceipt{}, errors.Join(ErrTicketIssueUnavailable, ErrShardPaymentUnavailable)
		}
		refreshedReceipt, refreshedErr := gateway.store.IssueTickets(ctx, refreshed, command)
		if refreshedErr != nil {
			return IssueTicketsReceipt{}, errors.Join(ErrTicketIssueUnavailable, mapShardError(refreshedErr))
		}
		return refreshedReceipt, nil
	}
	if err != nil {
		return IssueTicketsReceipt{}, errors.Join(ErrTicketIssueUnavailable, mapShardError(err))
	}
	return receipt, nil
}

type Gateway struct {
	directory   Directory
	store       Store
	ticketCodes TicketCodeClaimer
}

type GatewayOption func(*Gateway) error

func WithTicketCodeClaimer(claimer TicketCodeClaimer) GatewayOption {
	return func(gateway *Gateway) error {
		if claimer == nil {
			return ErrInvalidGateway
		}
		gateway.ticketCodes = claimer
		return nil
	}
}

func NewGateway(directory Directory, store Store, options ...GatewayOption) (*Gateway, error) {
	if directory == nil || store == nil {
		return nil, ErrInvalidGateway
	}
	gateway := &Gateway{directory: directory, store: store}
	for _, option := range options {
		if option == nil || option(gateway) != nil {
			return nil, ErrInvalidGateway
		}
	}
	return gateway, nil
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
