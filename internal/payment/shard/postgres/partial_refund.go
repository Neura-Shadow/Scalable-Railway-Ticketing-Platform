package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"time"

	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const maxSelectedRefundTickets = 1000

func (store *Store) LoadRefundOrder(ctx context.Context, route sharding.ShardRoute, orderID, ownerID uuid.UUID) (paymentshard.RefundOrderSnapshot, error) {
	if orderID == uuid.Nil || ownerID == uuid.Nil {
		return paymentshard.RefundOrderSnapshot{}, paymentapp.ErrPaymentNotFound
	}
	resolved, err := store.resolve(ctx, route, false, false)
	if err != nil {
		return paymentshard.RefundOrderSnapshot{}, err
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return paymentshard.RefundOrderSnapshot{}, paymentshard.ErrShardPaymentUnavailable
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	snapshot := paymentshard.RefundOrderSnapshot{TicketOrderID: orderID, TrainRunID: route.TrainRunID(), AssignmentGeneration: uint64(route.Generation().Int64())}
	err = tx.QueryRow(ctx, `
SELECT reservation_id,payment_intent_id,user_id,status,
       captured_amount_minor,refunded_amount_minor,currency
FROM public.ticket_orders
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3`,
		orderID, route.TrainRunID(), route.Generation().Int64()).Scan(
		&snapshot.ReservationID, &snapshot.PaymentIntentID, &snapshot.OwnerID, &snapshot.Status,
		&snapshot.CapturedMinor, &snapshot.RefundedMinor, &snapshot.Currency,
	)
	if errors.Is(err, pgx.ErrNoRows) || snapshot.OwnerID != ownerID {
		return paymentshard.RefundOrderSnapshot{}, paymentapp.ErrPaymentNotFound
	}
	if err != nil || snapshot.PaymentIntentID == uuid.Nil || snapshot.ReservationID == uuid.Nil ||
		(snapshot.Status != "issued" && snapshot.Status != "partially_refunded") {
		return paymentshard.RefundOrderSnapshot{}, paymentapp.ErrReservationNotPayable
	}
	rows, err := tx.Query(ctx, `
SELECT ticket.id,ticket.reservation_seat_id,ticket.status,
       seat.fare_amount_minor,seat.currency
FROM public.tickets AS ticket
JOIN public.reservation_seats AS seat
  ON seat.id=ticket.reservation_seat_id
 AND seat.train_run_id=ticket.train_run_id
 AND seat.assignment_generation=ticket.assignment_generation
WHERE ticket.ticket_order_id=$1
  AND ticket.train_run_id=$2
  AND ticket.assignment_generation=$3
ORDER BY ticket.id
LIMIT $4`, orderID, route.TrainRunID(), route.Generation().Int64(), maxSelectedRefundTickets+1)
	if err != nil {
		return paymentshard.RefundOrderSnapshot{}, paymentshard.ErrShardPaymentUnavailable
	}
	defer rows.Close()
	for rows.Next() {
		var ticket paymentshard.RefundTicketSnapshot
		if err := rows.Scan(&ticket.TicketID, &ticket.ReservationSeatID, &ticket.State, &ticket.FareMinor, &ticket.Currency); err != nil {
			return paymentshard.RefundOrderSnapshot{}, paymentshard.ErrShardPaymentUnavailable
		}
		snapshot.Tickets = append(snapshot.Tickets, ticket)
	}
	if rows.Err() != nil || len(snapshot.Tickets) == 0 || len(snapshot.Tickets) > maxSelectedRefundTickets {
		return paymentshard.RefundOrderSnapshot{}, paymentshard.ErrShardPaymentUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentshard.RefundOrderSnapshot{}, paymentshard.ErrShardPaymentUnavailable
	}
	return snapshot, nil
}

func (store *Store) PrepareSelectedTicketRefund(ctx context.Context, route sharding.ShardRoute, command paymentshard.PrepareSelectedTicketRefundCommand) (paymentshard.SelectedTicketRefundPrepareReceipt, error) {
	if !validPrepareSelectedRefundCommand(route, command) {
		return paymentshard.SelectedTicketRefundPrepareReceipt{}, paymentapp.ErrReservationNotPayable
	}
	resolved, err := store.resolve(ctx, route, false, true)
	if err != nil {
		return paymentshard.SelectedTicketRefundPrepareReceipt{}, err
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return paymentshard.SelectedTicketRefundPrepareReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	rollback := func(result error) (paymentshard.SelectedTicketRefundPrepareReceipt, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return paymentshard.SelectedTicketRefundPrepareReceipt{}, result
	}
	if receipt, found, err := loadSelectedRefundPrepareReceipt(ctx, tx, command); err != nil {
		return rollback(err)
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return paymentshard.SelectedTicketRefundPrepareReceipt{}, paymentshard.ErrShardPaymentUnavailable
		}
		return receipt, nil
	}
	if store.deployment != nil && (command.Region != store.deployment.Region().String() || command.RegionalEpoch != int64(store.deployment.Epoch().Uint64())) {
		return rollback(sharding.ErrWriteFenced)
	}
	if err := store.authorizeRegional(ctx, tx); err != nil {
		return rollback(err)
	}
	if store.deployment == nil {
		if err := lockRegionalAuthority(ctx, tx, command.Region, command.RegionalEpoch); err != nil {
			return rollback(err)
		}
	}
	if err := lockFence(ctx, tx, route); err != nil {
		return rollback(err)
	}

	var departureAt time.Time
	if err := tx.QueryRow(ctx, `SELECT scheduled_departure_at FROM public.train_run_booking_snapshots
WHERE train_run_id=$1 AND assignment_generation=$2 FOR SHARE`, route.TrainRunID(), route.Generation().Int64()).Scan(&departureAt); err != nil || departureAt.IsZero() || command.EligibilityCutoffAt.After(departureAt.UTC()) {
		return rollback(paymentapp.ErrReservationNotPayable)
	}
	var reservationOwner, reservationIntent uuid.UUID
	var reservationState, reservationCurrency string
	var reservationAmount int64
	if err := tx.QueryRow(ctx, `SELECT user_id,payment_intent_id,status,total_amount_minor,currency
FROM public.reservations WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3 FOR UPDATE`,
		command.ReservationID, route.TrainRunID(), route.Generation().Int64()).Scan(
		&reservationOwner, &reservationIntent, &reservationState, &reservationAmount, &reservationCurrency); err != nil {
		return rollback(paymentapp.ErrPaymentNotFound)
	}
	if reservationOwner != command.OwnerID || reservationIntent != command.PaymentIntentID || reservationCurrency != command.Currency ||
		(reservationState != "confirmed" && reservationState != "partially_refunded") {
		return rollback(paymentapp.ErrReservationNotPayable)
	}
	var orderOwner, orderReservation, orderIntent uuid.UUID
	var orderState, orderCurrency string
	var capturedMinor, refundedMinor int64
	if err := tx.QueryRow(ctx, `SELECT user_id,reservation_id,payment_intent_id,status,captured_amount_minor,refunded_amount_minor,currency
FROM public.ticket_orders WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3 FOR UPDATE`,
		command.TicketOrderID, route.TrainRunID(), route.Generation().Int64()).Scan(
		&orderOwner, &orderReservation, &orderIntent, &orderState, &capturedMinor, &refundedMinor, &orderCurrency); err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if orderOwner != command.OwnerID || orderReservation != command.ReservationID || orderIntent != command.PaymentIntentID ||
		orderCurrency != command.Currency || capturedMinor != reservationAmount ||
		(orderState != "issued" && orderState != "partially_refunded") || refundedMinor < 0 ||
		refundedMinor > capturedMinor || command.AmountMinor > capturedMinor-refundedMinor {
		return rollback(paymentapp.ErrReservationNotPayable)
	}
	tickets, err := lockPrepareRefundTickets(ctx, tx, route, command)
	if err != nil || len(tickets) != len(command.TicketIDs) {
		return rollback(paymentapp.ErrReservationNotPayable)
	}
	selectedAmount := int64(0)
	for index, ticket := range tickets {
		if ticket.ticketID != command.TicketIDs[index] || ticket.state != "active" || ticket.currency != command.Currency || ticket.fareMinor <= 0 {
			return rollback(paymentapp.ErrReservationNotPayable)
		}
		if selectedAmount > math.MaxInt64-ticket.fareMinor {
			return rollback(paymentapp.ErrPaymentConflict)
		}
		selectedAmount += ticket.fareMinor
	}
	if selectedAmount != command.AmountMinor {
		return rollback(paymentapp.ErrPaymentConflict)
	}
	if err := execOne(ctx, tx, `UPDATE public.ticket_orders SET status='partial_refund_pending'
WHERE id=$1 AND status=$2`, command.TicketOrderID, orderState); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `UPDATE public.reservations SET status='partially_refund_pending'
WHERE id=$1 AND status=$2`, command.ReservationID, reservationState); err != nil {
		return rollback(err)
	}
	tag, err := tx.Exec(ctx, `UPDATE public.tickets SET status='refund_pending'
WHERE ticket_order_id=$1 AND id=ANY($2::uuid[]) AND status='active'`, command.TicketOrderID, command.TicketIDs)
	if err != nil || tag.RowsAffected() != int64(len(tickets)) {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	receipt := paymentshard.SelectedTicketRefundPrepareReceipt{
		ReceiptID: uuid.NewSHA1(command.RefundRequestID, []byte("ticket-refund-prepare-receipt")), CommandID: command.CommandID,
		RefundRequestID: command.RefundRequestID, RefundOperationID: command.RefundOperationID,
		PaymentIntentID: command.PaymentIntentID, ReservationID: command.ReservationID, TicketOrderID: command.TicketOrderID,
		TrainRunID: route.TrainRunID(), AssignmentGeneration: uint64(route.Generation().Int64()), AmountMinor: command.AmountMinor,
		Currency: command.Currency, RequestFingerprint: command.RequestFingerprint,
		SelectedTicketCount: len(command.TicketIDs), PreparedAt: command.PreparedAt.UTC(),
	}
	if err := tx.QueryRow(ctx, `INSERT INTO public.ticket_refund_prepare_receipts(
 id,command_id,refund_request_id,refund_operation_id,payment_intent_id,reservation_id,ticket_order_id,
 train_run_id,assignment_generation,request_fingerprint,amount_minor,currency,ticket_ids,
 prior_order_state,prior_reservation_state,state,requested_at,eligibility_cutoff_at,prepared_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'prepared',$16,$17,$18)
RETURNING prepared_at`,
		receipt.ReceiptID, command.CommandID, command.RefundRequestID, command.RefundOperationID, command.PaymentIntentID,
		command.ReservationID, command.TicketOrderID, route.TrainRunID(), route.Generation().Int64(), command.RequestFingerprint[:],
		command.AmountMinor, command.Currency, command.TicketIDs, orderState, reservationState, command.RequestedAt.UTC(),
		command.EligibilityCutoffAt.UTC(), command.PreparedAt.UTC()).Scan(&receipt.PreparedAt); err != nil {
		return rollback(err)
	}
	receipt.PreparedAt = receipt.PreparedAt.UTC()
	if err := recordTargetWrite(ctx, tx, route, command.CommandID); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentshard.SelectedTicketRefundPrepareReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	return receipt, nil
}

func (store *Store) ReleaseSelectedTicketRefund(ctx context.Context, route sharding.ShardRoute, command paymentshard.ReleaseSelectedTicketRefundCommand) (paymentshard.SelectedTicketRefundReleaseReceipt, error) {
	if !validReleaseSelectedRefundCommand(route, command) {
		return paymentshard.SelectedTicketRefundReleaseReceipt{}, paymentapp.ErrReservationNotPayable
	}
	resolved, err := store.resolve(ctx, route, false, true)
	if err != nil {
		return paymentshard.SelectedTicketRefundReleaseReceipt{}, err
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return paymentshard.SelectedTicketRefundReleaseReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	rollback := func(result error) (paymentshard.SelectedTicketRefundReleaseReceipt, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return paymentshard.SelectedTicketRefundReleaseReceipt{}, result
	}
	if store.deployment != nil && (command.Region != store.deployment.Region().String() || command.RegionalEpoch != int64(store.deployment.Epoch().Uint64())) {
		return rollback(sharding.ErrWriteFenced)
	}
	if err := store.authorizeRegional(ctx, tx); err != nil {
		return rollback(err)
	}
	if store.deployment == nil {
		if err := lockRegionalAuthority(ctx, tx, command.Region, command.RegionalEpoch); err != nil {
			return rollback(err)
		}
	}
	if err := lockFence(ctx, tx, route); err != nil {
		return rollback(err)
	}

	var receiptID, requestID, operationID, intentID, reservationID, orderID, trainRunID uuid.UUID
	var generation int64
	var fingerprint []byte
	var ticketIDs []uuid.UUID
	var priorOrderState, priorReservationState, state string
	var resolvedAt *time.Time
	err = tx.QueryRow(ctx, `SELECT id,refund_request_id,refund_operation_id,payment_intent_id,
 reservation_id,ticket_order_id,train_run_id,assignment_generation,request_fingerprint,ticket_ids,
 prior_order_state,prior_reservation_state,state,resolved_at
FROM public.ticket_refund_prepare_receipts
WHERE id=$1 FOR UPDATE`, command.PrepareReceiptID).Scan(
		&receiptID, &requestID, &operationID, &intentID, &reservationID, &orderID, &trainRunID,
		&generation, &fingerprint, &ticketIDs, &priorOrderState, &priorReservationState, &state, &resolvedAt)
	if err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if receiptID != command.PrepareReceiptID || requestID != command.RefundRequestID || operationID != command.RefundOperationID ||
		intentID != command.PaymentIntentID || reservationID != command.ReservationID || orderID != command.TicketOrderID ||
		trainRunID != command.TrainRunID || generation != route.Generation().Int64() || len(fingerprint) != sha256.Size ||
		!bytes.Equal(fingerprint, command.RequestFingerprint[:]) || len(ticketIDs) != len(command.TicketIDs) {
		return rollback(paymentapp.ErrPaymentConflict)
	}
	for index := range ticketIDs {
		if ticketIDs[index] != command.TicketIDs[index] {
			return rollback(paymentapp.ErrPaymentConflict)
		}
	}
	receipt := paymentshard.SelectedTicketRefundReleaseReceipt{
		ReceiptID:        uuid.NewSHA1(command.PrepareReceiptID, []byte("ticket-refund-prepare-release-receipt")),
		PrepareReceiptID: command.PrepareReceiptID, CommandID: command.CommandID,
		RefundRequestID: command.RefundRequestID, RefundOperationID: command.RefundOperationID,
		TrainRunID: command.TrainRunID, AssignmentGeneration: uint64(generation),
		RequestFingerprint: command.RequestFingerprint, ReleasedTicketCount: len(ticketIDs), ReleasedAt: command.ReleasedAt.UTC(),
	}
	if state == "released" {
		if resolvedAt == nil || resolvedAt.IsZero() {
			return rollback(paymentapp.ErrPaymentConflict)
		}
		receipt.ReleasedAt = resolvedAt.UTC()
		if err := tx.Commit(ctx); err != nil {
			return paymentshard.SelectedTicketRefundReleaseReceipt{}, paymentshard.ErrShardPaymentUnavailable
		}
		return receipt, nil
	}
	if state != "prepared" || resolvedAt != nil {
		return rollback(paymentapp.ErrPaymentConflict)
	}

	var orderOwner, orderReservation, orderIntent uuid.UUID
	var orderState string
	if err := tx.QueryRow(ctx, `SELECT user_id,reservation_id,payment_intent_id,status FROM public.ticket_orders
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3 FOR UPDATE`, command.TicketOrderID,
		route.TrainRunID(), route.Generation().Int64()).Scan(&orderOwner, &orderReservation, &orderIntent, &orderState); err != nil ||
		orderOwner != command.OwnerID || orderReservation != command.ReservationID || orderIntent != command.PaymentIntentID || orderState != "partial_refund_pending" {
		return rollback(paymentapp.ErrReservationNotPayable)
	}
	var reservationOwner, reservationIntent uuid.UUID
	var reservationState string
	if err := tx.QueryRow(ctx, `SELECT user_id,payment_intent_id,status FROM public.reservations
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3 FOR UPDATE`, command.ReservationID,
		route.TrainRunID(), route.Generation().Int64()).Scan(&reservationOwner, &reservationIntent, &reservationState); err != nil ||
		reservationOwner != command.OwnerID || reservationIntent != command.PaymentIntentID || reservationState != "partially_refund_pending" {
		return rollback(paymentapp.ErrReservationNotPayable)
	}
	prepareCommand := paymentshard.PrepareSelectedTicketRefundCommand{TicketOrderID: command.TicketOrderID, TicketIDs: command.TicketIDs}
	tickets, err := lockPrepareRefundTickets(ctx, tx, route, prepareCommand)
	if err != nil || len(tickets) != len(command.TicketIDs) {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	for index, ticket := range tickets {
		if ticket.ticketID != command.TicketIDs[index] || ticket.state != "refund_pending" {
			return rollback(paymentapp.ErrPaymentConflict)
		}
	}
	if tag, err := tx.Exec(ctx, `UPDATE public.tickets SET status='active'
WHERE ticket_order_id=$1 AND id=ANY($2::uuid[]) AND status='refund_pending'`, command.TicketOrderID, command.TicketIDs); err != nil || tag.RowsAffected() != int64(len(ticketIDs)) {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if err := execOne(ctx, tx, `UPDATE public.ticket_orders SET status=$2 WHERE id=$1 AND status='partial_refund_pending'`, command.TicketOrderID, priorOrderState); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `UPDATE public.reservations SET status=$2 WHERE id=$1 AND status='partially_refund_pending'`, command.ReservationID, priorReservationState); err != nil {
		return rollback(err)
	}
	if err := tx.QueryRow(ctx, `UPDATE public.ticket_refund_prepare_receipts
SET state='released',resolved_at=$2 WHERE id=$1 AND state='prepared'
RETURNING resolved_at`, command.PrepareReceiptID, command.ReleasedAt.UTC()).Scan(&receipt.ReleasedAt); err != nil {
		return rollback(err)
	}
	receipt.ReleasedAt = receipt.ReleasedAt.UTC()
	if err := recordTargetWrite(ctx, tx, route, command.CommandID); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentshard.SelectedTicketRefundReleaseReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	return receipt, nil
}

func (store *Store) ApplySelectedTicketRefund(ctx context.Context, route sharding.ShardRoute, command paymentshard.ApplySelectedTicketRefundCommand) (paymentshard.SelectedTicketRefundReceipt, error) {
	if !validSelectedRefundCommand(route, command) {
		return paymentshard.SelectedTicketRefundReceipt{}, paymentapp.ErrReservationNotPayable
	}
	resolved, err := store.resolve(ctx, route, false, true)
	if err != nil {
		return paymentshard.SelectedTicketRefundReceipt{}, err
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return paymentshard.SelectedTicketRefundReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	rollback := func(result error) (paymentshard.SelectedTicketRefundReceipt, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return paymentshard.SelectedTicketRefundReceipt{}, result
	}
	if receipt, found, err := loadSelectedRefundReceipt(ctx, tx, command); err != nil {
		return rollback(err)
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return paymentshard.SelectedTicketRefundReceipt{}, paymentshard.ErrShardPaymentUnavailable
		}
		return receipt, nil
	}
	if store.deployment != nil && (command.Region != store.deployment.Region().String() || command.RegionalEpoch != int64(store.deployment.Epoch().Uint64())) {
		return rollback(sharding.ErrWriteFenced)
	}
	if err := store.authorizeRegional(ctx, tx); err != nil {
		return rollback(err)
	}
	if store.deployment == nil {
		if err := lockRegionalAuthority(ctx, tx, command.Region, command.RegionalEpoch); err != nil {
			return rollback(err)
		}
	}
	if err := lockFence(ctx, tx, route); err != nil {
		return rollback(err)
	}
	if receipt, found, err := loadSelectedRefundReceipt(ctx, tx, command); err != nil {
		return rollback(err)
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return paymentshard.SelectedTicketRefundReceipt{}, paymentshard.ErrShardPaymentUnavailable
		}
		return receipt, nil
	}
	var prepareState string
	var prepareFingerprint []byte
	var preparedTicketIDs []uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT state,request_fingerprint,ticket_ids
FROM public.ticket_refund_prepare_receipts
WHERE refund_request_id=$1 AND refund_operation_id=$2 AND payment_intent_id=$3
  AND reservation_id=$4 AND ticket_order_id=$5 AND train_run_id=$6
  AND assignment_generation=$7 FOR UPDATE`, command.RefundRequestID, command.RefundOperationID,
		command.PaymentIntentID, command.ReservationID, command.TicketOrderID, route.TrainRunID(), route.Generation().Int64()).Scan(
		&prepareState, &prepareFingerprint, &preparedTicketIDs); err != nil || prepareState != "prepared" ||
		len(prepareFingerprint) != sha256.Size || !bytes.Equal(prepareFingerprint, command.RequestFingerprint[:]) ||
		len(preparedTicketIDs) != len(command.TicketIDs) {
		return rollback(paymentapp.ErrReservationNotPayable)
	}
	for index := range preparedTicketIDs {
		if preparedTicketIDs[index] != command.TicketIDs[index] {
			return rollback(paymentapp.ErrPaymentConflict)
		}
	}

	var reservationOwner, reservationIntent uuid.UUID
	var reservationState, reservationCurrency string
	var reservationAmount int64
	if err := tx.QueryRow(ctx, `
SELECT user_id,payment_intent_id,status,total_amount_minor,currency
FROM public.reservations
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3
FOR UPDATE`, command.ReservationID, route.TrainRunID(), route.Generation().Int64()).Scan(
		&reservationOwner, &reservationIntent, &reservationState, &reservationAmount, &reservationCurrency,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return rollback(paymentapp.ErrPaymentNotFound)
		}
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if reservationOwner != command.OwnerID {
		return rollback(paymentapp.ErrPaymentNotFound)
	}
	if reservationIntent != command.PaymentIntentID || reservationCurrency != command.Currency ||
		reservationState != "partially_refund_pending" {
		return rollback(paymentapp.ErrReservationNotPayable)
	}

	var orderOwner, orderReservation, orderIntent uuid.UUID
	var orderState, orderCurrency string
	var capturedMinor, refundedMinor int64
	if err := tx.QueryRow(ctx, `
SELECT user_id,reservation_id,payment_intent_id,status,
       captured_amount_minor,refunded_amount_minor,currency
FROM public.ticket_orders
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3
FOR UPDATE`, command.TicketOrderID, route.TrainRunID(), route.Generation().Int64()).Scan(
		&orderOwner, &orderReservation, &orderIntent, &orderState,
		&capturedMinor, &refundedMinor, &orderCurrency,
	); err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if orderOwner != command.OwnerID || orderReservation != command.ReservationID || orderIntent != command.PaymentIntentID ||
		orderCurrency != command.Currency || capturedMinor != reservationAmount ||
		orderState != "partial_refund_pending" ||
		refundedMinor < 0 || refundedMinor > capturedMinor || command.AmountMinor > capturedMinor-refundedMinor {
		return rollback(paymentapp.ErrReservationNotPayable)
	}

	tickets, err := lockSelectedRefundTickets(ctx, tx, route, command)
	if err != nil {
		return rollback(err)
	}
	if len(tickets) != len(command.TicketIDs) {
		return rollback(paymentapp.ErrReservationNotPayable)
	}
	selectedAmount := int64(0)
	seatIDs := make([]uuid.UUID, 0, len(tickets))
	for index, ticket := range tickets {
		if ticket.ticketID != command.TicketIDs[index] || ticket.state != "refund_pending" || ticket.currency != command.Currency || ticket.fareMinor <= 0 {
			return rollback(paymentapp.ErrReservationNotPayable)
		}
		if selectedAmount > math.MaxInt64-ticket.fareMinor {
			return rollback(paymentapp.ErrPaymentConflict)
		}
		selectedAmount += ticket.fareMinor
		seatIDs = append(seatIDs, ticket.reservationSeatID)
	}
	if selectedAmount != command.AmountMinor {
		return rollback(paymentapp.ErrPaymentConflict)
	}
	var activeBefore int
	if err := tx.QueryRow(ctx, `SELECT count(*)::integer FROM public.tickets
WHERE ticket_order_id=$1 AND train_run_id=$2 AND assignment_generation=$3 AND status='active'`,
		command.TicketOrderID, route.TrainRunID(), route.Generation().Int64()).Scan(&activeBefore); err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}

	tag, err := tx.Exec(ctx, `
UPDATE public.seat_inventory AS inventory
SET occupied_segments=inventory.occupied_segments & ~seat.segment_mask,
    version=inventory.version+1
FROM public.reservation_seats AS seat
WHERE seat.id=ANY($1::uuid[])
  AND inventory.train_run_id=seat.train_run_id
  AND inventory.assignment_generation=seat.assignment_generation
  AND inventory.seat_id=seat.seat_id
  AND (inventory.occupied_segments & seat.segment_mask)=seat.segment_mask`, seatIDs)
	if err != nil || tag.RowsAffected() != int64(len(tickets)) {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	tag, err = tx.Exec(ctx, `UPDATE public.tickets SET status='refunded'
WHERE ticket_order_id=$1 AND id=ANY($2::uuid[]) AND status='refund_pending'`, command.TicketOrderID, command.TicketIDs)
	if err != nil || tag.RowsAffected() != int64(len(tickets)) {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}

	activeAfter := activeBefore
	resultState := "partially_refunded"
	reservationResultState := "partially_refunded"
	if activeAfter == 0 {
		resultState = "refunded"
		reservationResultState = "cancelled"
	}
	if err := execOne(ctx, tx, `UPDATE public.ticket_orders
SET status=$2,refunded_amount_minor=refunded_amount_minor+$3
WHERE id=$1 AND status='partial_refund_pending'
  AND refunded_amount_minor+$3 <= captured_amount_minor`, command.TicketOrderID, resultState, command.AmountMinor); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `UPDATE public.reservations SET status=$2
WHERE id=$1 AND status='partially_refund_pending'`, command.ReservationID, reservationResultState); err != nil {
		return rollback(err)
	}

	receiptID := uuid.NewSHA1(command.RefundRequestID, []byte("ticket-refund-compensation-receipt"))
	if err := execOne(ctx, tx, `
INSERT INTO public.ticket_refund_compensation_receipts(
 id,command_id,refund_request_id,refund_operation_id,payment_intent_id,
 reservation_id,ticket_order_id,train_run_id,assignment_generation,
 request_fingerprint,provider_proof_hash,amount_minor,currency,
 selected_ticket_count,released_seat_count,resulting_active_ticket_count,
 resulting_order_state,committed_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$14,$15,$16,$17)`,
		receiptID, command.CommandID, command.RefundRequestID, command.RefundOperationID,
		command.PaymentIntentID, command.ReservationID, command.TicketOrderID,
		route.TrainRunID(), route.Generation().Int64(), command.RequestFingerprint[:], command.ProviderProofHash[:],
		command.AmountMinor, command.Currency, len(tickets), activeAfter, resultState, command.RefundedAt.UTC()); err != nil {
		return rollback(err)
	}
	for _, ticket := range tickets {
		maskHash := sha256.Sum256([]byte(ticket.segmentMask))
		if err := execOne(ctx, tx, `
INSERT INTO public.selected_ticket_refund_receipts(
 id,compensation_receipt_id,refund_request_id,ticket_id,reservation_seat_id,
 train_run_id,assignment_generation,fare_amount_minor,currency,segment_mask_hash,released_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			uuid.NewSHA1(receiptID, ticket.ticketID[:]), receiptID, command.RefundRequestID,
			ticket.ticketID, ticket.reservationSeatID, route.TrainRunID(), route.Generation().Int64(),
			ticket.fareMinor, ticket.currency, maskHash[:], command.RefundedAt.UTC()); err != nil {
			return rollback(err)
		}
	}
	if err := execOne(ctx, tx, `UPDATE public.ticket_refund_prepare_receipts
SET state='applied',resolved_at=$2 WHERE refund_request_id=$1 AND state='prepared'`, command.RefundRequestID, command.RefundedAt.UTC()); err != nil {
		return rollback(err)
	}

	commonPayload := map[string]any{
		"refund_request_id": command.RefundRequestID, "ticket_order_id": command.TicketOrderID,
		"reservation_id": command.ReservationID, "selected_ticket_count": len(tickets),
		"resulting_order_state": resultState,
	}
	if err := appendRefundOutbox(ctx, tx, route, command.CommandID, command.TicketOrderID, "ticket_order", "ticket_order."+resultState, commonPayload); err != nil {
		return rollback(err)
	}
	if err := appendRefundOutbox(ctx, tx, route, command.CommandID, command.ReservationID, "reservation", "reservation."+reservationResultState, commonPayload); err != nil {
		return rollback(err)
	}
	for _, ticket := range tickets {
		if err := appendRefundOutbox(ctx, tx, route, command.CommandID, ticket.ticketID, "ticket", "ticket.refunded", map[string]any{
			"refund_request_id": command.RefundRequestID, "ticket_order_id": command.TicketOrderID, "ticket_id": ticket.ticketID,
		}); err != nil {
			return rollback(err)
		}
	}
	if err := recordTargetWrite(ctx, tx, route, command.CommandID); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentshard.SelectedTicketRefundReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	return paymentshard.SelectedTicketRefundReceipt{
		ReceiptID: receiptID, CommandID: command.CommandID, RefundRequestID: command.RefundRequestID,
		RefundOperationID: command.RefundOperationID, PaymentIntentID: command.PaymentIntentID,
		ReservationID: command.ReservationID, TicketOrderID: command.TicketOrderID,
		TrainRunID: route.TrainRunID(), AssignmentGeneration: uint64(route.Generation().Int64()),
		AmountMinor: command.AmountMinor, Currency: command.Currency, SelectedTicketCount: len(tickets),
		ReleasedSeatCount: len(tickets), ResultingActiveTicketCount: activeAfter,
		ResultingOrderState: resultState, CommittedAt: command.RefundedAt.UTC(),
	}, nil
}

type lockedRefundTicket struct {
	ticketID, reservationSeatID  uuid.UUID
	state, currency, segmentMask string
	fareMinor                    int64
}

func lockPrepareRefundTickets(ctx context.Context, tx pgx.Tx, route sharding.ShardRoute, command paymentshard.PrepareSelectedTicketRefundCommand) ([]lockedRefundTicket, error) {
	rows, err := tx.Query(ctx, `SELECT ticket.id,ticket.reservation_seat_id,ticket.status,
       seat.fare_amount_minor,seat.currency,seat.segment_mask::text
FROM public.tickets AS ticket
JOIN public.reservation_seats AS seat
  ON seat.id=ticket.reservation_seat_id AND seat.train_run_id=ticket.train_run_id
 AND seat.assignment_generation=ticket.assignment_generation
WHERE ticket.ticket_order_id=$1 AND ticket.id=ANY($2::uuid[])
  AND ticket.train_run_id=$3 AND ticket.assignment_generation=$4
ORDER BY ticket.id FOR UPDATE OF ticket,seat`, command.TicketOrderID, command.TicketIDs, route.TrainRunID(), route.Generation().Int64())
	if err != nil {
		return nil, paymentshard.ErrShardPaymentUnavailable
	}
	defer rows.Close()
	result := make([]lockedRefundTicket, 0, len(command.TicketIDs))
	for rows.Next() {
		var ticket lockedRefundTicket
		if err := rows.Scan(&ticket.ticketID, &ticket.reservationSeatID, &ticket.state, &ticket.fareMinor, &ticket.currency, &ticket.segmentMask); err != nil {
			return nil, paymentshard.ErrShardPaymentUnavailable
		}
		result = append(result, ticket)
	}
	if rows.Err() != nil {
		return nil, paymentshard.ErrShardPaymentUnavailable
	}
	return result, nil
}

func loadSelectedRefundPrepareReceipt(ctx context.Context, tx pgx.Tx, command paymentshard.PrepareSelectedTicketRefundCommand) (paymentshard.SelectedTicketRefundPrepareReceipt, bool, error) {
	var receipt paymentshard.SelectedTicketRefundPrepareReceipt
	var fingerprint []byte
	var ticketIDs []uuid.UUID
	var state string
	var requestedAt, eligibilityCutoffAt time.Time
	err := tx.QueryRow(ctx, `SELECT id,command_id,refund_request_id,refund_operation_id,payment_intent_id,
 reservation_id,ticket_order_id,train_run_id,assignment_generation,request_fingerprint,
 amount_minor,currency,ticket_ids,state,requested_at,eligibility_cutoff_at,prepared_at
FROM public.ticket_refund_prepare_receipts
WHERE command_id=$1 OR refund_request_id=$2 OR refund_operation_id=$3`,
		command.CommandID, command.RefundRequestID, command.RefundOperationID).Scan(
		&receipt.ReceiptID, &receipt.CommandID, &receipt.RefundRequestID, &receipt.RefundOperationID,
		&receipt.PaymentIntentID, &receipt.ReservationID, &receipt.TicketOrderID, &receipt.TrainRunID,
		&receipt.AssignmentGeneration, &fingerprint, &receipt.AmountMinor, &receipt.Currency, &ticketIDs, &state,
		&requestedAt, &eligibilityCutoffAt, &receipt.PreparedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return paymentshard.SelectedTicketRefundPrepareReceipt{}, false, nil
	}
	if err != nil {
		return paymentshard.SelectedTicketRefundPrepareReceipt{}, false, paymentshard.ErrShardPaymentUnavailable
	}
	if receipt.CommandID != command.CommandID || receipt.RefundRequestID != command.RefundRequestID ||
		receipt.RefundOperationID != command.RefundOperationID || receipt.PaymentIntentID != command.PaymentIntentID ||
		receipt.ReservationID != command.ReservationID || receipt.TicketOrderID != command.TicketOrderID ||
		receipt.TrainRunID != command.TrainRunID || receipt.AmountMinor != command.AmountMinor || receipt.Currency != command.Currency ||
		receipt.AssignmentGeneration == 0 || state != "prepared" || !requestedAt.Equal(command.RequestedAt.UTC().Truncate(time.Microsecond)) ||
		!eligibilityCutoffAt.Equal(command.EligibilityCutoffAt.UTC().Truncate(time.Microsecond)) || len(fingerprint) != sha256.Size ||
		!bytes.Equal(fingerprint, command.RequestFingerprint[:]) || len(ticketIDs) != len(command.TicketIDs) {
		return paymentshard.SelectedTicketRefundPrepareReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	for index := range ticketIDs {
		if ticketIDs[index] != command.TicketIDs[index] {
			return paymentshard.SelectedTicketRefundPrepareReceipt{}, false, paymentapp.ErrPaymentConflict
		}
	}
	copy(receipt.RequestFingerprint[:], fingerprint)
	receipt.SelectedTicketCount = len(ticketIDs)
	receipt.PreparedAt = receipt.PreparedAt.UTC()
	return receipt, true, nil
}

func lockSelectedRefundTickets(ctx context.Context, tx pgx.Tx, route sharding.ShardRoute, command paymentshard.ApplySelectedTicketRefundCommand) ([]lockedRefundTicket, error) {
	rows, err := tx.Query(ctx, `
SELECT ticket.id,ticket.reservation_seat_id,ticket.status,
       seat.fare_amount_minor,seat.currency,seat.segment_mask::text
FROM public.tickets AS ticket
JOIN public.reservation_seats AS seat
  ON seat.id=ticket.reservation_seat_id
 AND seat.train_run_id=ticket.train_run_id
 AND seat.assignment_generation=ticket.assignment_generation
WHERE ticket.ticket_order_id=$1
  AND ticket.id=ANY($2::uuid[])
  AND ticket.train_run_id=$3
  AND ticket.assignment_generation=$4
ORDER BY ticket.id
FOR UPDATE OF ticket,seat`, command.TicketOrderID, command.TicketIDs, route.TrainRunID(), route.Generation().Int64())
	if err != nil {
		return nil, paymentshard.ErrShardPaymentUnavailable
	}
	defer rows.Close()
	result := make([]lockedRefundTicket, 0, len(command.TicketIDs))
	for rows.Next() {
		var ticket lockedRefundTicket
		if err := rows.Scan(&ticket.ticketID, &ticket.reservationSeatID, &ticket.state, &ticket.fareMinor, &ticket.currency, &ticket.segmentMask); err != nil {
			return nil, paymentshard.ErrShardPaymentUnavailable
		}
		result = append(result, ticket)
	}
	if rows.Err() != nil {
		return nil, paymentshard.ErrShardPaymentUnavailable
	}
	return result, nil
}

func loadSelectedRefundReceipt(ctx context.Context, tx pgx.Tx, command paymentshard.ApplySelectedTicketRefundCommand) (paymentshard.SelectedTicketRefundReceipt, bool, error) {
	rows, err := tx.Query(ctx, `
SELECT id,command_id,refund_request_id,refund_operation_id,payment_intent_id,
       reservation_id,ticket_order_id,train_run_id,assignment_generation,
       request_fingerprint,provider_proof_hash,amount_minor,currency,
       selected_ticket_count,released_seat_count,resulting_active_ticket_count,
       resulting_order_state,committed_at
FROM public.ticket_refund_compensation_receipts
WHERE command_id=$1 OR refund_request_id=$2 OR refund_operation_id=$3
ORDER BY id LIMIT 2`, command.CommandID, command.RefundRequestID, command.RefundOperationID)
	if err != nil {
		return paymentshard.SelectedTicketRefundReceipt{}, false, paymentshard.ErrShardPaymentUnavailable
	}
	defer rows.Close()
	var receipt paymentshard.SelectedTicketRefundReceipt
	var fingerprint, proof []byte
	count := 0
	for rows.Next() {
		count++
		if err := rows.Scan(&receipt.ReceiptID, &receipt.CommandID, &receipt.RefundRequestID,
			&receipt.RefundOperationID, &receipt.PaymentIntentID, &receipt.ReservationID,
			&receipt.TicketOrderID, &receipt.TrainRunID, &receipt.AssignmentGeneration,
			&fingerprint, &proof, &receipt.AmountMinor, &receipt.Currency,
			&receipt.SelectedTicketCount, &receipt.ReleasedSeatCount,
			&receipt.ResultingActiveTicketCount, &receipt.ResultingOrderState, &receipt.CommittedAt); err != nil {
			return paymentshard.SelectedTicketRefundReceipt{}, false, paymentshard.ErrShardPaymentUnavailable
		}
	}
	if rows.Err() != nil {
		return paymentshard.SelectedTicketRefundReceipt{}, false, paymentshard.ErrShardPaymentUnavailable
	}
	if count == 0 {
		return paymentshard.SelectedTicketRefundReceipt{}, false, nil
	}
	if count != 1 || receipt.CommandID != command.CommandID || receipt.RefundRequestID != command.RefundRequestID ||
		receipt.RefundOperationID != command.RefundOperationID || receipt.PaymentIntentID != command.PaymentIntentID ||
		receipt.ReservationID != command.ReservationID || receipt.TicketOrderID != command.TicketOrderID ||
		receipt.TrainRunID != command.TrainRunID || receipt.AmountMinor != command.AmountMinor ||
		receipt.Currency != command.Currency || receipt.SelectedTicketCount != len(command.TicketIDs) ||
		receipt.ReleasedSeatCount != len(command.TicketIDs) || len(fingerprint) != sha256.Size || len(proof) != sha256.Size ||
		!bytes.Equal(fingerprint, command.RequestFingerprint[:]) || !bytes.Equal(proof, command.ProviderProofHash[:]) {
		return paymentshard.SelectedTicketRefundReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	ticketRows, err := tx.Query(ctx, `SELECT ticket_id
FROM public.selected_ticket_refund_receipts
WHERE compensation_receipt_id=$1 ORDER BY ticket_id`, receipt.ReceiptID)
	if err != nil {
		return paymentshard.SelectedTicketRefundReceipt{}, false, paymentshard.ErrShardPaymentUnavailable
	}
	defer ticketRows.Close()
	index := 0
	for ticketRows.Next() {
		var ticketID uuid.UUID
		if err := ticketRows.Scan(&ticketID); err != nil || index >= len(command.TicketIDs) || ticketID != command.TicketIDs[index] {
			return paymentshard.SelectedTicketRefundReceipt{}, false, paymentapp.ErrPaymentConflict
		}
		index++
	}
	if ticketRows.Err() != nil || index != len(command.TicketIDs) {
		return paymentshard.SelectedTicketRefundReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	receipt.CommittedAt = receipt.CommittedAt.UTC()
	return receipt, true, nil
}

func validSelectedRefundCommand(route sharding.ShardRoute, command paymentshard.ApplySelectedTicketRefundCommand) bool {
	if command.CommandID == uuid.Nil || command.RefundRequestID == uuid.Nil || command.RefundOperationID == uuid.Nil ||
		command.PaymentIntentID == uuid.Nil || command.ReservationID == uuid.Nil || command.TicketOrderID == uuid.Nil ||
		command.TrainRunID != route.TrainRunID() || command.OwnerID == uuid.Nil ||
		(command.Region != "region-a" && command.Region != "region-b") || command.RegionalEpoch <= 0 ||
		command.AmountMinor <= 0 || len(command.Currency) != 3 || command.ProviderProofHash == [32]byte{} ||
		command.RequestFingerprint == [32]byte{} || command.RefundedAt.IsZero() ||
		len(command.TicketIDs) == 0 || len(command.TicketIDs) > maxSelectedRefundTickets {
		return false
	}
	for index, ticketID := range command.TicketIDs {
		if ticketID == uuid.Nil || (index > 0 && bytes.Compare(command.TicketIDs[index-1][:], ticketID[:]) >= 0) {
			return false
		}
	}
	return true
}

func validPrepareSelectedRefundCommand(route sharding.ShardRoute, command paymentshard.PrepareSelectedTicketRefundCommand) bool {
	if command.CommandID == uuid.Nil || command.RefundRequestID == uuid.Nil || command.RefundOperationID == uuid.Nil ||
		command.PaymentIntentID == uuid.Nil || command.ReservationID == uuid.Nil || command.TicketOrderID == uuid.Nil ||
		command.TrainRunID != route.TrainRunID() || command.OwnerID == uuid.Nil ||
		(command.Region != "region-a" && command.Region != "region-b") || command.RegionalEpoch <= 0 ||
		command.AmountMinor <= 0 || len(command.Currency) != 3 || command.RequestFingerprint == [32]byte{} ||
		command.RequestedAt.IsZero() || command.EligibilityCutoffAt.IsZero() || command.PreparedAt.IsZero() ||
		!command.RequestedAt.Before(command.EligibilityCutoffAt) || command.PreparedAt.Before(command.RequestedAt) ||
		len(command.TicketIDs) == 0 || len(command.TicketIDs) > maxSelectedRefundTickets {
		return false
	}
	for index, ticketID := range command.TicketIDs {
		if ticketID == uuid.Nil || (index > 0 && bytes.Compare(command.TicketIDs[index-1][:], ticketID[:]) >= 0) {
			return false
		}
	}
	return true
}

func validReleaseSelectedRefundCommand(route sharding.ShardRoute, command paymentshard.ReleaseSelectedTicketRefundCommand) bool {
	if command.CommandID == uuid.Nil || command.PrepareReceiptID == uuid.Nil || command.RefundRequestID == uuid.Nil ||
		command.RefundOperationID == uuid.Nil || command.PaymentIntentID == uuid.Nil || command.ReservationID == uuid.Nil ||
		command.TicketOrderID == uuid.Nil || command.TrainRunID != route.TrainRunID() || command.OwnerID == uuid.Nil ||
		(command.Region != "region-a" && command.Region != "region-b") || command.RegionalEpoch <= 0 ||
		command.RequestFingerprint == [32]byte{} || command.ReleasedAt.IsZero() ||
		len(command.TicketIDs) == 0 || len(command.TicketIDs) > maxSelectedRefundTickets {
		return false
	}
	for index, ticketID := range command.TicketIDs {
		if ticketID == uuid.Nil || (index > 0 && bytes.Compare(command.TicketIDs[index-1][:], ticketID[:]) >= 0) {
			return false
		}
	}
	return true
}

func lockRegionalAuthority(ctx context.Context, tx pgx.Tx, expectedRegion string, expectedEpoch int64) error {
	var region, state string
	var epoch int64
	var writesEnabled bool
	if err := tx.QueryRow(ctx, `SELECT region,epoch,state,writes_enabled
FROM public.regional_write_authority WHERE singleton FOR UPDATE`).Scan(&region, &epoch, &state, &writesEnabled); err != nil {
		return paymentshard.ErrShardPaymentUnavailable
	}
	if region != expectedRegion || epoch != expectedEpoch || state != "active" || !writesEnabled {
		return sharding.ErrWriteFenced
	}
	return nil
}

func appendRefundOutbox(ctx context.Context, tx pgx.Tx, route sharding.ShardRoute, commandID, aggregateID uuid.UUID, aggregateType, eventType string, fields map[string]any) error {
	payload, err := json.Marshal(fields)
	if err != nil {
		return paymentshard.ErrShardPaymentUnavailable
	}
	eventID := uuid.NewSHA1(commandID, []byte(eventType+":"+aggregateID.String()))
	return execOne(ctx, tx, `INSERT INTO public.outbox_events(
 id,train_run_id,assignment_generation,aggregate_type,aggregate_id,event_type,payload
) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb)`, eventID, route.TrainRunID(), route.Generation().Int64(), aggregateType, aggregateID, eventType, string(payload))
}

var _ paymentshard.SelectedRefundStore = (*Store)(nil)
var _ paymentshard.RefundOrderStore = (*Store)(nil)
var _ paymentshard.PreparedRefundStore = (*Store)(nil)
