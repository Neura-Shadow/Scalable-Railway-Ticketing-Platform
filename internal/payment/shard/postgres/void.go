package postgres

import (
	"context"
	"errors"
	"time"

	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// CancelVoidedReservation releases the reservation only after a successful
// provider void has been durably reduced to an opaque proof. The train-run
// fence serializes concurrent commands, while payment_command_receipts makes
// a committed release replayable without touching inventory a second time.
func (store *Store) CancelVoidedReservation(ctx context.Context, route sharding.ShardRoute, command paymentshard.CancelVoidedReservationCommand) (paymentshard.CancelVoidedReservationReceipt, error) {
	if !validVoidCancellationCommand(route, command) {
		return paymentshard.CancelVoidedReservationReceipt{}, paymentapp.ErrReservationNotPayable
	}
	resolved, err := store.resolve(ctx, route, false, true)
	if err != nil {
		return paymentshard.CancelVoidedReservationReceipt{}, err
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return paymentshard.CancelVoidedReservationReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	rollback := func(result error) (paymentshard.CancelVoidedReservationReceipt, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return paymentshard.CancelVoidedReservationReceipt{}, result
	}
	if receipt, found, err := loadVoidCancellationReceipt(ctx, tx, command); err != nil {
		return rollback(err)
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return paymentshard.CancelVoidedReservationReceipt{}, paymentshard.ErrShardPaymentUnavailable
		}
		return receipt, nil
	}
	if err := store.authorizeRegional(ctx, tx); err != nil {
		return rollback(err)
	}
	if err := lockFence(ctx, tx, route); err != nil {
		return rollback(err)
	}
	if receipt, found, err := loadVoidCancellationReceipt(ctx, tx, command); err != nil {
		return rollback(err)
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return paymentshard.CancelVoidedReservationReceipt{}, paymentshard.ErrShardPaymentUnavailable
		}
		return receipt, nil
	}
	// Once the train-run fence is held, any cancellation authority for this
	// intent must be this exact command. This check runs before domain-state
	// validation so a replay under a different identity is a conflict instead
	// of being mistaken for a newly non-payable reservation.
	var priorCommand uuid.UUID
	err = tx.QueryRow(ctx, `SELECT command_id FROM public.payment_command_receipts
WHERE payment_intent_id=$1 AND operation='reservation.payment_cancelled'
ORDER BY created_at,command_id LIMIT 1`, command.PaymentIntentID).Scan(&priorCommand)
	if err == nil {
		return rollback(paymentapp.ErrPaymentConflict)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}

	var owner uuid.UUID
	var reservationState, currency string
	var amount int64
	var intent pgtype.UUID
	err = tx.QueryRow(ctx, `
SELECT user_id,status,total_amount_minor,currency,payment_intent_id
FROM public.reservations
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3
FOR UPDATE`, command.ReservationID, route.TrainRunID(), route.Generation().Int64()).Scan(
		&owner, &reservationState, &amount, &currency, &intent)
	if errors.Is(err, pgx.ErrNoRows) || owner != command.OwnerID {
		return rollback(paymentapp.ErrPaymentNotFound)
	}
	if err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if !intent.Valid || intent.Bytes != command.PaymentIntentID || amount != command.AmountMinor ||
		currency != command.Currency || (reservationState != "payment_pending" && reservationState != "payment_review") {
		return rollback(paymentapp.ErrReservationNotPayable)
	}

	ticketOrderID := uuid.NewSHA1(command.PaymentIntentID, []byte("ticket-order"))
	var orderState string
	var authorized, captured, refunded int64
	err = tx.QueryRow(ctx, `
SELECT status,authorized_amount_minor,captured_amount_minor,refunded_amount_minor
FROM public.ticket_orders
WHERE id=$1 AND reservation_id=$2 AND payment_intent_id=$3
  AND train_run_id=$4 AND assignment_generation=$5
  AND total_amount_minor=$6 AND currency=$7
FOR UPDATE`, ticketOrderID, command.ReservationID, command.PaymentIntentID,
		route.TrainRunID(), route.Generation().Int64(), command.AmountMinor, command.Currency).Scan(
		&orderState, &authorized, &captured, &refunded)
	if err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if (orderState != "payment_pending" && orderState != "payment_authorized" && orderState != "manual_review") ||
		captured != 0 || refunded != 0 || (authorized != 0 && authorized != command.AmountMinor) {
		return rollback(paymentapp.ErrPaymentConflict)
	}

	var issued, tickets, refunds, compensations int
	if err := tx.QueryRow(ctx, `
SELECT
 (SELECT count(*)::integer FROM public.ticket_issuance_receipts WHERE payment_intent_id=$1),
 (SELECT count(*)::integer FROM public.tickets WHERE ticket_order_id=$2),
 (SELECT count(*)::integer FROM public.payment_refund_receipts WHERE payment_intent_id=$1),
 (SELECT count(*)::integer FROM public.payment_compensation_receipts WHERE payment_intent_id=$1)`,
		command.PaymentIntentID, ticketOrderID).Scan(&issued, &tickets, &refunds, &compensations); err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if issued != 0 || tickets != 0 || refunds != 0 || compensations != 0 {
		return rollback(paymentapp.ErrPaymentConflict)
	}

	var seatCount int
	if err := tx.QueryRow(ctx, `SELECT count(*)::integer FROM public.reservation_seats
WHERE reservation_id=$1 AND train_run_id=$2 AND assignment_generation=$3`,
		command.ReservationID, route.TrainRunID(), route.Generation().Int64()).Scan(&seatCount); err != nil || seatCount < 1 || seatCount > 100 {
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
	if err := execOne(ctx, tx, `UPDATE public.ticket_orders SET status='cancelled'
WHERE id=$1 AND status IN('payment_pending','payment_authorized','manual_review')
 AND captured_amount_minor=0 AND refunded_amount_minor=0`, ticketOrderID); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `UPDATE public.reservations SET status='cancelled'
WHERE id=$1 AND status IN('payment_pending','payment_review')`, command.ReservationID); err != nil {
		return rollback(err)
	}
	for _, event := range []struct {
		aggregate uuid.UUID
		kind      string
		name      string
	}{
		{command.ReservationID, "reservation", "reservation.cancelled"},
		{ticketOrderID, "payment", "payment.void_compensation_applied"},
	} {
		if err := appendOutbox(ctx, tx, route, event.aggregate, event.kind, event.name, map[string]any{
			"payment_intent_id": command.PaymentIntentID, "reservation_id": command.ReservationID,
			"ticket_order_id": ticketOrderID,
		}); err != nil {
			return rollback(err)
		}
	}
	var committedAt time.Time
	if err := tx.QueryRow(ctx, `
UPDATE public.payment_command_receipts
SET status='succeeded',result_resource_id=$2,result_status='cancelled',committed_at=clock_timestamp()
WHERE command_id=$1 AND status='started'
RETURNING committed_at`, command.CommandID, command.ReservationID).Scan(&committedAt); err != nil || committedAt.IsZero() {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if err := recordTargetWrite(ctx, tx, route, command.CommandID); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentshard.CancelVoidedReservationReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	return paymentshard.CancelVoidedReservationReceipt{
		CommandID: command.CommandID, VoidOperationID: command.VoidOperationID,
		PaymentIntentID: command.PaymentIntentID, ReservationID: command.ReservationID,
		TicketOrderID: ticketOrderID, ReleasedSeatCount: seatCount, CancelledAt: committedAt.UTC(),
	}, nil
}

func validVoidCancellationCommand(route sharding.ShardRoute, command paymentshard.CancelVoidedReservationCommand) bool {
	return command.CommandID != uuid.Nil && command.VoidOperationID != uuid.Nil && command.PaymentIntentID != uuid.Nil &&
		command.ReservationID != uuid.Nil && command.OwnerID != uuid.Nil && command.TrainRunID == route.TrainRunID() &&
		command.AmountMinor >= 0 && len(command.Currency) == 3 && command.VoidProofHash != [32]byte{} &&
		command.RequestFingerprint == paymentshard.VoidCancellationFingerprint(command) && !command.VoidedAt.IsZero()
}

func loadVoidCancellationReceipt(ctx context.Context, tx pgx.Tx, command paymentshard.CancelVoidedReservationCommand) (paymentshard.CancelVoidedReservationReceipt, bool, error) {
	var intentID, reservationID, resourceID uuid.UUID
	var operation, status, resultStatus string
	var fingerprint []byte
	var committedAt pgtype.Timestamptz
	err := tx.QueryRow(ctx, `
SELECT payment_intent_id,reservation_id,operation,request_fingerprint,status,
       result_resource_id,result_status,committed_at
FROM public.payment_command_receipts WHERE command_id=$1`, command.CommandID).Scan(
		&intentID, &reservationID, &operation, &fingerprint, &status, &resourceID, &resultStatus, &committedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return paymentshard.CancelVoidedReservationReceipt{}, false, nil
	}
	if err != nil || intentID != command.PaymentIntentID || reservationID != command.ReservationID ||
		operation != "reservation.payment_cancelled" || len(fingerprint) != 32 || status != "succeeded" ||
		resourceID != command.ReservationID || resultStatus != "cancelled" || !committedAt.Valid {
		return paymentshard.CancelVoidedReservationReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	var stored [32]byte
	copy(stored[:], fingerprint)
	if stored != command.RequestFingerprint {
		return paymentshard.CancelVoidedReservationReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	var seatCount int
	if err := tx.QueryRow(ctx, `SELECT count(*)::integer FROM public.reservation_seats
WHERE reservation_id=$1`, command.ReservationID).Scan(&seatCount); err != nil || seatCount < 1 || seatCount > 100 {
		return paymentshard.CancelVoidedReservationReceipt{}, false, paymentshard.ErrShardPaymentUnavailable
	}
	return paymentshard.CancelVoidedReservationReceipt{
		CommandID: command.CommandID, VoidOperationID: command.VoidOperationID,
		PaymentIntentID: intentID, ReservationID: reservationID,
		TicketOrderID:     uuid.NewSHA1(intentID, []byte("ticket-order")),
		ReleasedSeatCount: seatCount, CancelledAt: committedAt.Time.UTC(),
	}, true, nil
}
