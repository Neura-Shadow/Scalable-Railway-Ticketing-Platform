package postgres

import (
	"context"
	"errors"

	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (store *Store) MarkRefundPending(ctx context.Context, route sharding.ShardRoute, command paymentshard.MarkRefundPendingCommand) (paymentshard.MarkRefundPendingReceipt, error) {
	if !validRefundPendingCommand(route, command) {
		return paymentshard.MarkRefundPendingReceipt{}, paymentapp.ErrReservationNotPayable
	}
	resolved, err := store.resolve(ctx, route, false, true)
	if err != nil {
		return paymentshard.MarkRefundPendingReceipt{}, err
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return paymentshard.MarkRefundPendingReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	rollback := func(result error) (paymentshard.MarkRefundPendingReceipt, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return paymentshard.MarkRefundPendingReceipt{}, result
	}
	if resourceID, state, found, err := loadPaymentCommandReceipt(ctx, tx, command.CommandID, command.PaymentIntentID, command.RequestFingerprint); err != nil {
		return rollback(err)
	} else if found {
		if state != "refund_pending" || resourceID != command.ReservationID {
			return rollback(paymentapp.ErrPaymentConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return paymentshard.MarkRefundPendingReceipt{}, paymentshard.ErrShardPaymentUnavailable
		}
		return paymentshard.MarkRefundPendingReceipt{CommandID: command.CommandID, PaymentIntentID: command.PaymentIntentID, ReservationID: command.ReservationID, TicketOrderID: uuid.NewSHA1(command.PaymentIntentID, []byte("ticket-order"))}, nil
	}
	if err := lockFence(ctx, tx, route); err != nil {
		return rollback(err)
	}
	if resourceID, state, found, err := loadPaymentCommandReceipt(ctx, tx, command.CommandID, command.PaymentIntentID, command.RequestFingerprint); err != nil {
		return rollback(err)
	} else if found {
		if state != "refund_pending" || resourceID != command.ReservationID {
			return rollback(paymentapp.ErrPaymentConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return paymentshard.MarkRefundPendingReceipt{}, paymentshard.ErrShardPaymentUnavailable
		}
		return paymentshard.MarkRefundPendingReceipt{CommandID: command.CommandID, PaymentIntentID: command.PaymentIntentID, ReservationID: command.ReservationID, TicketOrderID: uuid.NewSHA1(command.PaymentIntentID, []byte("ticket-order"))}, nil
	}
	var owner uuid.UUID
	var state, currency string
	var amount int64
	var intent pgtype.UUID
	if err := tx.QueryRow(ctx, `
SELECT user_id,status,total_amount_minor,currency,payment_intent_id
FROM public.reservations
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3
FOR UPDATE`, command.ReservationID, route.TrainRunID(), route.Generation().Int64()).Scan(
		&owner, &state, &amount, &currency, &intent); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return rollback(paymentapp.ErrPaymentNotFound)
		}
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if owner != command.OwnerID {
		return rollback(paymentapp.ErrPaymentNotFound)
	}
	if !intent.Valid || intent.Bytes != command.PaymentIntentID || amount != command.AmountMinor || currency != command.Currency ||
		(state != "payment_pending" && state != "payment_review" && state != "confirmed" && state != "refund_pending") {
		return rollback(paymentapp.ErrReservationNotPayable)
	}
	ticketOrderID := uuid.NewSHA1(command.PaymentIntentID, []byte("ticket-order"))
	var orderState string
	if err := tx.QueryRow(ctx, `
SELECT status FROM public.ticket_orders
WHERE id=$1 AND reservation_id=$2 AND payment_intent_id=$3
  AND total_amount_minor=$4 AND currency=$5
FOR UPDATE`, ticketOrderID, command.ReservationID, command.PaymentIntentID,
		command.AmountMinor, command.Currency).Scan(&orderState); err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if state == "refund_pending" && orderState == "refund_pending" {
		return rollback(paymentapp.ErrPaymentConflict)
	}
	if err := execOne(ctx, tx, `
INSERT INTO public.payment_command_receipts(
 id,command_id,payment_intent_id,reservation_id,train_run_id,
 assignment_generation,operation,request_fingerprint,amount_minor,currency,status
) VALUES($1,$2,$3,$4,$5,$6,'reservation.refund_pending',$7,$8,$9,'started')`,
		uuid.NewSHA1(command.CommandID, []byte("payment-command-receipt")), command.CommandID,
		command.PaymentIntentID, command.ReservationID, route.TrainRunID(), route.Generation().Int64(),
		command.RequestFingerprint[:], command.AmountMinor, command.Currency); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `UPDATE public.reservations SET status='refund_pending'
WHERE id=$1 AND status IN('payment_pending','payment_review','confirmed')`, command.ReservationID); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `
UPDATE public.ticket_orders
SET status='refund_pending',authorized_amount_minor=total_amount_minor,
    captured_amount_minor=total_amount_minor
WHERE id=$1 AND status IN(
 'payment_pending','payment_authorized','payment_captured','issuance_pending','issued','manual_review'
)`, ticketOrderID); err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE public.tickets SET status='refund_pending'
WHERE ticket_order_id=$1 AND status='active'`, ticketOrderID); err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if err := appendOutbox(ctx, tx, route, command.ReservationID, "reservation", "reservation.refund_pending", map[string]any{
		"payment_intent_id": command.PaymentIntentID, "reservation_id": command.ReservationID,
		"ticket_order_id": ticketOrderID,
	}); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `
UPDATE public.payment_command_receipts
SET status='succeeded',result_resource_id=$2,result_status='refund_pending',committed_at=clock_timestamp()
WHERE command_id=$1 AND status='started'`, command.CommandID, command.ReservationID); err != nil {
		return rollback(err)
	}
	if err := recordTargetWrite(ctx, tx, route, command.CommandID); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentshard.MarkRefundPendingReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	return paymentshard.MarkRefundPendingReceipt{CommandID: command.CommandID, PaymentIntentID: command.PaymentIntentID, ReservationID: command.ReservationID, TicketOrderID: ticketOrderID}, nil
}

func (store *Store) ApplyRefundCompensation(ctx context.Context, route sharding.ShardRoute, command paymentshard.ApplyRefundCompensationCommand) (paymentshard.ApplyRefundCompensationReceipt, error) {
	if !validCompensationCommand(route, command) {
		return paymentshard.ApplyRefundCompensationReceipt{}, paymentapp.ErrReservationNotPayable
	}
	resolved, err := store.resolve(ctx, route, false, true)
	if err != nil {
		return paymentshard.ApplyRefundCompensationReceipt{}, err
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return paymentshard.ApplyRefundCompensationReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	rollback := func(result error) (paymentshard.ApplyRefundCompensationReceipt, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return paymentshard.ApplyRefundCompensationReceipt{}, result
	}
	if receipt, found, err := loadCompensationCommand(ctx, tx, command); err != nil {
		return rollback(err)
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return paymentshard.ApplyRefundCompensationReceipt{}, paymentshard.ErrShardPaymentUnavailable
		}
		return receipt, nil
	}
	if err := lockFence(ctx, tx, route); err != nil {
		return rollback(err)
	}
	if receipt, found, err := loadCompensationCommand(ctx, tx, command); err != nil {
		return rollback(err)
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return paymentshard.ApplyRefundCompensationReceipt{}, paymentshard.ErrShardPaymentUnavailable
		}
		return receipt, nil
	}
	var owner uuid.UUID
	var state, currency string
	var amount int64
	var intent pgtype.UUID
	if err := tx.QueryRow(ctx, `
SELECT user_id,status,total_amount_minor,currency,payment_intent_id
FROM public.reservations
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3
FOR UPDATE`, command.ReservationID, route.TrainRunID(), route.Generation().Int64()).Scan(
		&owner, &state, &amount, &currency, &intent); err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if owner != command.OwnerID {
		return rollback(paymentapp.ErrPaymentNotFound)
	}
	if state != "refund_pending" || !intent.Valid || intent.Bytes != command.PaymentIntentID ||
		amount != command.AmountMinor || currency != command.Currency {
		return rollback(paymentapp.ErrReservationNotPayable)
	}
	ticketOrderID := uuid.NewSHA1(command.PaymentIntentID, []byte("ticket-order"))
	var orderState string
	if err := tx.QueryRow(ctx, `
SELECT status FROM public.ticket_orders WHERE id=$1 AND reservation_id=$2
  AND payment_intent_id=$3 AND total_amount_minor=$4 AND currency=$5 FOR UPDATE`,
		ticketOrderID, command.ReservationID, command.PaymentIntentID, command.AmountMinor, command.Currency).Scan(&orderState); err != nil || orderState != "refund_pending" {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if err := execOne(ctx, tx, `
INSERT INTO public.payment_command_receipts(
 id,command_id,payment_intent_id,reservation_id,train_run_id,
 assignment_generation,operation,request_fingerprint,amount_minor,currency,status
) VALUES($1,$2,$3,$4,$5,$6,'reservation.payment_cancelled',$7,$8,$9,'started')`,
		uuid.NewSHA1(command.CommandID, []byte("payment-command-receipt")), command.CommandID,
		command.PaymentIntentID, command.ReservationID, route.TrainRunID(), route.Generation().Int64(),
		command.RequestFingerprint[:], command.AmountMinor, command.Currency); err != nil {
		return rollback(err)
	}
	refundReceiptID := uuid.NewSHA1(command.RefundOperationID, []byte("payment-refund-receipt"))
	if err := execOne(ctx, tx, `
INSERT INTO public.payment_refund_receipts(
 id,refund_operation_id,payment_intent_id,reservation_id,ticket_order_id,
 train_run_id,assignment_generation,refund_proof_hash,captured_amount_minor,
 refunded_amount_minor,currency,refunded_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$9,$10,$11)`, refundReceiptID,
		command.RefundOperationID, command.PaymentIntentID, command.ReservationID, ticketOrderID,
		route.TrainRunID(), route.Generation().Int64(), command.RefundProofHash[:], command.AmountMinor,
		command.Currency, command.RefundedAt.UTC()); err != nil {
		return rollback(err)
	}
	var seatCount int
	if err := tx.QueryRow(ctx, `SELECT count(*)::integer FROM public.reservation_seats WHERE reservation_id=$1`, command.ReservationID).Scan(&seatCount); err != nil || seatCount < 1 {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.seat_inventory AS inventory
SET occupied_segments=inventory.occupied_segments & ~seat.segment_mask,
    version=inventory.version+1
FROM public.reservation_seats AS seat
WHERE seat.reservation_id=$1
  AND inventory.train_run_id=seat.train_run_id
  AND inventory.assignment_generation=seat.assignment_generation
  AND inventory.seat_id=seat.seat_id
  AND (inventory.occupied_segments & seat.segment_mask)=seat.segment_mask`, command.ReservationID)
	if err != nil || tag.RowsAffected() != int64(seatCount) {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	rows, err := tx.Query(ctx, `
SELECT id FROM public.tickets
WHERE ticket_order_id=$1 AND status IN('active','refund_pending')
ORDER BY id FOR UPDATE`, ticketOrderID)
	if err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	ticketIDs := make([]uuid.UUID, 0, 8)
	for rows.Next() {
		var ticketID uuid.UUID
		if err := rows.Scan(&ticketID); err != nil || ticketID == uuid.Nil {
			rows.Close()
			return rollback(paymentshard.ErrShardPaymentUnavailable)
		}
		ticketIDs = append(ticketIDs, ticketID)
	}
	if rows.Err() != nil || len(ticketIDs) > 100 {
		rows.Close()
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	rows.Close()
	ticketCount := len(ticketIDs)
	tag, err = tx.Exec(ctx, `UPDATE public.tickets SET status='cancelled'
WHERE ticket_order_id=$1 AND status IN('active','refund_pending')`, ticketOrderID)
	if err != nil || tag.RowsAffected() != int64(ticketCount) {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if err := execOne(ctx, tx, `UPDATE public.ticket_orders
SET status='refunded',authorized_amount_minor=total_amount_minor,
    captured_amount_minor=total_amount_minor,refunded_amount_minor=total_amount_minor
WHERE id=$1 AND status='refund_pending'`, ticketOrderID); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `UPDATE public.reservations SET status='cancelled'
WHERE id=$1 AND status='refund_pending'`, command.ReservationID); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `
INSERT INTO public.payment_compensation_receipts(
 id,compensation_id,payment_intent_id,reservation_id,ticket_order_id,
 refund_receipt_id,train_run_id,assignment_generation,released_seat_count,
 cancelled_ticket_count
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		uuid.NewSHA1(command.CompensationID, []byte("payment-compensation-receipt")), command.CompensationID,
		command.PaymentIntentID, command.ReservationID, ticketOrderID, refundReceiptID,
		route.TrainRunID(), route.Generation().Int64(), seatCount, ticketCount); err != nil {
		return rollback(err)
	}
	for _, event := range []struct {
		aggregate  uuid.UUID
		kind, name string
	}{
		{command.ReservationID, "reservation", "reservation.cancelled"},
		{ticketOrderID, "payment", "payment.compensation_applied"},
	} {
		if err := appendOutbox(ctx, tx, route, event.aggregate, event.kind, event.name, map[string]any{
			"payment_intent_id": command.PaymentIntentID, "reservation_id": command.ReservationID,
			"ticket_order_id": ticketOrderID,
		}); err != nil {
			return rollback(err)
		}
	}
	for _, ticketID := range ticketIDs {
		if err := appendOutbox(ctx, tx, route, ticketID, "ticket", "ticket.cancelled", map[string]any{
			"payment_intent_id": command.PaymentIntentID, "reservation_id": command.ReservationID,
			"ticket_order_id": ticketOrderID, "ticket_id": ticketID,
		}); err != nil {
			return rollback(err)
		}
	}
	if err := execOne(ctx, tx, `
UPDATE public.payment_command_receipts
SET status='succeeded',result_resource_id=$2,result_status='cancelled',committed_at=clock_timestamp()
WHERE command_id=$1 AND status='started'`, command.CommandID, command.ReservationID); err != nil {
		return rollback(err)
	}
	if err := recordTargetWrite(ctx, tx, route, command.CommandID); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentshard.ApplyRefundCompensationReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	return paymentshard.ApplyRefundCompensationReceipt{
		CommandID: command.CommandID, CompensationID: command.CompensationID,
		PaymentIntentID: command.PaymentIntentID, ReservationID: command.ReservationID,
		TicketOrderID: ticketOrderID, ReleasedSeatCount: seatCount, CancelledTicketCount: ticketCount,
	}, nil
}

func loadCompensationCommand(ctx context.Context, tx pgx.Tx, command paymentshard.ApplyRefundCompensationCommand) (paymentshard.ApplyRefundCompensationReceipt, bool, error) {
	resourceID, state, commandFound, err := loadPaymentCommandReceipt(
		ctx, tx, command.CommandID, command.PaymentIntentID, command.RequestFingerprint,
	)
	if err != nil {
		return paymentshard.ApplyRefundCompensationReceipt{}, false, err
	}
	receipt, receiptFound, err := loadCompensationReceipt(ctx, tx, command)
	if err != nil {
		return paymentshard.ApplyRefundCompensationReceipt{}, false, err
	}
	if commandFound {
		if state != "cancelled" || resourceID != command.ReservationID || !receiptFound {
			return paymentshard.ApplyRefundCompensationReceipt{}, false, paymentapp.ErrPaymentConflict
		}
		return receipt, true, nil
	}
	if receiptFound {
		// The compensation identity is already bound to a different command.
		return paymentshard.ApplyRefundCompensationReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	return paymentshard.ApplyRefundCompensationReceipt{}, false, nil
}

func validRefundPendingCommand(route sharding.ShardRoute, command paymentshard.MarkRefundPendingCommand) bool {
	return command.CommandID != uuid.Nil && command.PaymentIntentID != uuid.Nil && command.ReservationID != uuid.Nil &&
		command.TrainRunID == route.TrainRunID() && command.OwnerID != uuid.Nil && command.AmountMinor >= 0 &&
		len(command.Currency) == 3 && command.CaptureProofHash != [32]byte{} && command.RequestFingerprint != [32]byte{}
}

func validCompensationCommand(route sharding.ShardRoute, command paymentshard.ApplyRefundCompensationCommand) bool {
	return command.CommandID != uuid.Nil && command.CompensationID != uuid.Nil && command.RefundOperationID != uuid.Nil &&
		command.PaymentIntentID != uuid.Nil && command.ReservationID != uuid.Nil && command.OwnerID != uuid.Nil &&
		command.TrainRunID == route.TrainRunID() && command.AmountMinor > 0 && len(command.Currency) == 3 &&
		command.RefundProofHash != [32]byte{} && command.RequestFingerprint != [32]byte{} && !command.RefundedAt.IsZero()
}

func loadPaymentCommandReceipt(ctx context.Context, tx pgx.Tx, commandID, intentID uuid.UUID, fingerprint [32]byte) (uuid.UUID, string, bool, error) {
	var storedIntent, resourceID uuid.UUID
	var storedFingerprint []byte
	var status, resultState string
	err := tx.QueryRow(ctx, `
SELECT payment_intent_id,request_fingerprint,status,result_resource_id,result_status
FROM public.payment_command_receipts WHERE command_id=$1`, commandID).Scan(
		&storedIntent, &storedFingerprint, &status, &resourceID, &resultState,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, "", false, nil
	}
	if err != nil || storedIntent != intentID || len(storedFingerprint) != 32 || status != "succeeded" {
		return uuid.Nil, "", false, paymentapp.ErrPaymentConflict
	}
	var stored [32]byte
	copy(stored[:], storedFingerprint)
	if stored != fingerprint {
		return uuid.Nil, "", false, paymentapp.ErrPaymentConflict
	}
	return resourceID, resultState, true, nil
}

func loadCompensationReceipt(ctx context.Context, tx pgx.Tx, command paymentshard.ApplyRefundCompensationCommand) (paymentshard.ApplyRefundCompensationReceipt, bool, error) {
	var intentID, reservationID, orderID, refundReceiptID uuid.UUID
	var released, cancelled int
	err := tx.QueryRow(ctx, `
SELECT payment_intent_id,reservation_id,ticket_order_id,refund_receipt_id,
       released_seat_count,cancelled_ticket_count
FROM public.payment_compensation_receipts WHERE compensation_id=$1`, command.CompensationID).Scan(
		&intentID, &reservationID, &orderID, &refundReceiptID, &released, &cancelled,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return paymentshard.ApplyRefundCompensationReceipt{}, false, nil
	}
	if err != nil || intentID != command.PaymentIntentID || reservationID != command.ReservationID {
		return paymentshard.ApplyRefundCompensationReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	var operationID uuid.UUID
	var proof []byte
	var captured, refunded int64
	var currency string
	if err := tx.QueryRow(ctx, `
SELECT refund_operation_id,refund_proof_hash,captured_amount_minor,refunded_amount_minor,currency
FROM public.payment_refund_receipts WHERE id=$1`, refundReceiptID).Scan(
		&operationID, &proof, &captured, &refunded, &currency); err != nil || operationID != command.RefundOperationID ||
		len(proof) != 32 || captured != command.AmountMinor || refunded != command.AmountMinor || currency != command.Currency {
		return paymentshard.ApplyRefundCompensationReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	var storedProof [32]byte
	copy(storedProof[:], proof)
	if storedProof != command.RefundProofHash {
		return paymentshard.ApplyRefundCompensationReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	return paymentshard.ApplyRefundCompensationReceipt{
		CommandID: command.CommandID, CompensationID: command.CompensationID,
		PaymentIntentID: intentID, ReservationID: reservationID, TicketOrderID: orderID,
		ReleasedSeatCount: released, CancelledTicketCount: cancelled,
	}, true, nil
}
