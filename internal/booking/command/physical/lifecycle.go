package physical

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (executor *Executor) executeLifecycleTx(ctx context.Context, tx pgx.Tx, bookingCommand command.Command, resolved shardphysical.Resolution) (command.Receipt, error) {
	if !resolved.Handle.WriteEnabled() {
		return command.Receipt{}, sharding.ErrWriteFenced
	}
	if err := lockLifecycleAuthority(ctx, tx, bookingCommand); err != nil {
		return command.Receipt{}, err
	}
	if receipt, found, err := loadReceipt(ctx, tx, bookingCommand); err != nil {
		return command.Receipt{}, err
	} else if found {
		return receipt, nil
	}
	if err := execOne(ctx, tx, `
INSERT INTO booking_command_receipts(
 id,command_id,train_run_id,assignment_generation,command_type,request_fingerprint,status
) VALUES($1,$2,$3,$4,$5,$6,'started')`,
		uuid.NewSHA1(bookingCommand.ID, []byte("command-receipt")), bookingCommand.ID,
		bookingCommand.TrainRunID, bookingCommand.Route.Generation().Int64(),
		bookingCommand.Operation, bookingCommand.RequestFingerprint[:]); err != nil {
		return command.Receipt{}, err
	}
	if err := beginMutationSavepoint(ctx, tx); err != nil {
		return command.Receipt{}, err
	}

	var receipt command.Receipt
	var changed bool
	var err error
	switch bookingCommand.Operation {
	case command.OperationConfirmReservation:
		receipt, changed, err = confirmReservation(ctx, tx, bookingCommand)
	case command.OperationCancelReservation:
		receipt, changed, err = cancelReservation(ctx, tx, bookingCommand)
	default:
		return command.Receipt{}, ErrInvalidPayload
	}
	if err != nil {
		if code, permanent := rejectionCode(err); permanent {
			if rollbackErr := rollbackMutationSavepoint(ctx, tx); rollbackErr != nil {
				return command.Receipt{}, rollbackErr
			}
			if persistErr := persistRejectedReceipt(ctx, tx, bookingCommand.ID, code); persistErr != nil {
				return command.Receipt{}, persistErr
			}
			return command.Receipt{}, &durableRejection{cause: err}
		}
		return command.Receipt{}, err
	}
	if changed {
		if err := recordTargetWrite(ctx, tx, bookingCommand); err != nil {
			return command.Receipt{}, err
		}
	}
	if err := execOne(ctx, tx, `
UPDATE booking_command_receipts
SET status='succeeded',result_type='reservation',result_id=$2,completed_at=clock_timestamp()
WHERE command_id=$1 AND status='started'`, bookingCommand.ID, bookingCommand.ReservationID); err != nil {
		return command.Receipt{}, err
	}
	if err := releaseMutationSavepoint(ctx, tx); err != nil {
		return command.Receipt{}, err
	}
	return receipt, nil
}

func lockLifecycleAuthority(ctx context.Context, tx pgx.Tx, bookingCommand command.Command) error {
	var generation int64
	var writeEnabled bool
	var state, snapshotStatus string
	err := tx.QueryRow(ctx, `
SELECT fence.assignment_generation,fence.write_enabled,fence.state,snapshot.status
FROM train_run_write_fences AS fence
JOIN train_run_booking_snapshots AS snapshot
  ON snapshot.train_run_id=fence.train_run_id
 AND snapshot.assignment_generation=fence.assignment_generation
WHERE fence.train_run_id=$1
FOR UPDATE OF fence,snapshot`, bookingCommand.TrainRunID).Scan(&generation, &writeEnabled, &state, &snapshotStatus)
	if err != nil {
		return sharding.ErrShardUnavailable
	}
	if generation != bookingCommand.Route.Generation().Int64() {
		return sharding.ErrAssignmentStale
	}
	if !writeEnabled || state != "active" {
		return sharding.ErrWriteFenced
	}
	if bookingCommand.Operation == command.OperationConfirmReservation &&
		snapshotStatus != "scheduled" && snapshotStatus != "boarding" {
		return sharding.ErrWriteFenced
	}
	return nil
}

func lockOwnedPhysicalReservation(ctx context.Context, tx pgx.Tx, bookingCommand command.Command) (string, int64, string, error) {
	var status, currency string
	var total int64
	err := tx.QueryRow(ctx, `
SELECT status,total_amount_minor,currency
FROM reservations
WHERE id=$1 AND user_id=$2 AND train_run_id=$3 AND assignment_generation=$4
FOR UPDATE`, bookingCommand.ReservationID, bookingCommand.OwnerUserID,
		bookingCommand.TrainRunID, bookingCommand.Route.Generation().Int64()).Scan(&status, &total, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, "", ErrInvalidPayload
	}
	if err != nil || total < 0 || len(currency) != 3 {
		return "", 0, "", ErrShardPersistence
	}
	return status, total, currency, nil
}

func confirmReservation(ctx context.Context, tx pgx.Tx, bookingCommand command.Command) (command.Receipt, bool, error) {
	status, total, currency, err := lockOwnedPhysicalReservation(ctx, tx, bookingCommand)
	if err != nil {
		return command.Receipt{}, false, err
	}
	if status != "held" && status != "confirmed" {
		if status == "expired" {
			return command.Receipt{}, false, ErrReservationExpired
		}
		return command.Receipt{}, false, ErrInvalidLifecycleState
	}
	if status == "held" {
		tag, err := tx.Exec(ctx, `
UPDATE reservations SET status='confirmed'
			WHERE id=$1 AND status='held' AND expires_at>clock_timestamp()`, bookingCommand.ReservationID)
		if err != nil {
			return command.Receipt{}, false, ErrShardPersistence
		}
		if tag.RowsAffected() != 1 {
			return command.Receipt{}, false, ErrReservationExpired
		}
	}
	orderID := uuid.NewSHA1(bookingCommand.ID, []byte("ticket-order"))
	if _, err := tx.Exec(ctx, `
INSERT INTO ticket_orders(id,reservation_id,user_id,train_run_id,assignment_generation,status,total_amount_minor,currency)
VALUES($1,$2,$3,$4,$5,'confirmed',$6,$7)
ON CONFLICT(reservation_id) DO NOTHING`, orderID, bookingCommand.ReservationID,
		bookingCommand.OwnerUserID, bookingCommand.TrainRunID,
		bookingCommand.Route.Generation().Int64(), total, currency); err != nil {
		return command.Receipt{}, false, ErrShardPersistence
	}
	if err := tx.QueryRow(ctx, `SELECT id FROM ticket_orders WHERE reservation_id=$1 AND user_id=$2`,
		bookingCommand.ReservationID, bookingCommand.OwnerUserID).Scan(&orderID); err != nil || orderID == uuid.Nil {
		return command.Receipt{}, false, ErrShardPersistence
	}
	rows, err := tx.Query(ctx, `
SELECT id FROM reservation_seats WHERE reservation_id=$1 ORDER BY id FOR SHARE`, bookingCommand.ReservationID)
	if err != nil {
		return command.Receipt{}, false, ErrShardPersistence
	}
	var seatIDs []uuid.UUID
	for rows.Next() {
		var seatID uuid.UUID
		if err := rows.Scan(&seatID); err != nil || seatID == uuid.Nil {
			return command.Receipt{}, false, ErrShardPersistence
		}
		seatIDs = append(seatIDs, seatID)
	}
	if rows.Err() != nil || len(seatIDs) == 0 || len(seatIDs) > command.MaxReceiptTickets {
		rows.Close()
		return command.Receipt{}, false, ErrShardPersistence
	}
	rows.Close()
	for _, reservationSeatID := range seatIDs {
		ticketID := uuid.NewSHA1(bookingCommand.ID, reservationSeatID[:])
		ticketCode := "TKT" + ticketID.String()
		if _, err := tx.Exec(ctx, `
INSERT INTO tickets(id,ticket_order_id,reservation_seat_id,train_run_id,assignment_generation,ticket_code,status)
VALUES($1,$2,$3,$4,$5,$6,'active')
ON CONFLICT(reservation_seat_id) DO NOTHING`, ticketID, orderID, reservationSeatID,
			bookingCommand.TrainRunID, bookingCommand.Route.Generation().Int64(), ticketCode); err != nil {
			return command.Receipt{}, false, ErrShardPersistence
		}
		if status == "held" {
			ticketPayload, marshalErr := json.Marshal(map[string]any{
				"command_id": bookingCommand.ID, "ticket_id": ticketID,
				"ticket_order_id": orderID, "reservation_id": bookingCommand.ReservationID,
				"train_run_id":          bookingCommand.TrainRunID,
				"assignment_generation": bookingCommand.Route.Generation().Int64(),
			})
			if marshalErr != nil {
				return command.Receipt{}, false, ErrShardPersistence
			}
			if err := execOne(ctx, tx, `
INSERT INTO outbox_events(id,train_run_id,assignment_generation,aggregate_type,aggregate_id,event_type,payload)
VALUES($1,$2,$3,'ticket',$4,'ticket.created',$5::jsonb)`,
				uuid.NewSHA1(ticketID, []byte("ticket.created")), bookingCommand.TrainRunID,
				bookingCommand.Route.Generation().Int64(), ticketID, string(ticketPayload)); err != nil {
				return command.Receipt{}, false, err
			}
		}
	}
	changed := status == "held"
	if changed {
		if err := appendLifecycleEvent(ctx, tx, bookingCommand, "reservation.confirmed", map[string]any{
			"status": "confirmed", "ticket_order_id": orderID, "ticket_count": len(seatIDs),
		}); err != nil {
			return command.Receipt{}, false, err
		}
	}
	receipt := committedReceipt(bookingCommand)
	receipt.TicketOrderID, receipt.TicketCount = orderID, len(seatIDs)
	receipt.TicketIDs, err = loadOrderTicketIDs(ctx, tx, orderID, len(seatIDs))
	if err != nil {
		return command.Receipt{}, false, err
	}
	receipt.TotalAmountMinor, receipt.Currency = total, currency
	if err := tx.QueryRow(ctx, `SELECT created_at FROM ticket_orders WHERE id=$1`, orderID).Scan(&receipt.OrderCreatedAt); err != nil {
		return command.Receipt{}, false, ErrShardPersistence
	}
	return receipt, changed, nil
}

func loadOrderTicketIDs(ctx context.Context, tx pgx.Tx, orderID uuid.UUID, expected int) ([]uuid.UUID, error) {
	if orderID == uuid.Nil || expected < 1 || expected > command.MaxReceiptTickets {
		return nil, ErrShardPersistence
	}
	rows, err := tx.Query(ctx, `SELECT id FROM tickets WHERE ticket_order_id=$1 ORDER BY id LIMIT $2`,
		orderID, command.MaxReceiptTickets+1)
	if err != nil {
		return nil, ErrShardPersistence
	}
	defer rows.Close()
	ticketIDs := make([]uuid.UUID, 0, expected)
	for rows.Next() {
		var ticketID uuid.UUID
		if err := rows.Scan(&ticketID); err != nil || ticketID == uuid.Nil {
			return nil, ErrShardPersistence
		}
		ticketIDs = append(ticketIDs, ticketID)
	}
	if rows.Err() != nil || len(ticketIDs) != expected || len(ticketIDs) > command.MaxReceiptTickets {
		return nil, ErrShardPersistence
	}
	return ticketIDs, nil
}

func cancelReservation(ctx context.Context, tx pgx.Tx, bookingCommand command.Command) (command.Receipt, bool, error) {
	status, _, _, err := lockOwnedPhysicalReservation(ctx, tx, bookingCommand)
	if err != nil {
		return command.Receipt{}, false, err
	}
	if status != "held" && status != "confirmed" && status != "cancelled" {
		return command.Receipt{}, false, ErrInvalidLifecycleState
	}
	var released int
	if err := tx.QueryRow(ctx, `SELECT count(*)::integer FROM reservation_seats WHERE reservation_id=$1`,
		bookingCommand.ReservationID).Scan(&released); err != nil || released < 1 {
		return command.Receipt{}, false, ErrShardPersistence
	}
	if status != "cancelled" {
		if err := execOne(ctx, tx, `UPDATE reservations SET status='cancelled' WHERE id=$1 AND status=$2`,
			bookingCommand.ReservationID, status); err != nil {
			return command.Receipt{}, false, err
		}
		tag, err := tx.Exec(ctx, `
UPDATE seat_inventory AS inventory
SET occupied_segments=inventory.occupied_segments & ~seat.segment_mask,
    version=inventory.version+1
FROM reservation_seats AS seat
WHERE seat.reservation_id=$1
  AND inventory.train_run_id=seat.train_run_id
  AND inventory.assignment_generation=seat.assignment_generation
  AND inventory.seat_id=seat.seat_id
  AND (inventory.occupied_segments & seat.segment_mask)=seat.segment_mask`, bookingCommand.ReservationID)
		if err != nil || tag.RowsAffected() != int64(released) {
			return command.Receipt{}, false, ErrShardPersistence
		}
		if _, err := tx.Exec(ctx, `UPDATE ticket_orders SET status='cancelled' WHERE reservation_id=$1`, bookingCommand.ReservationID); err != nil {
			return command.Receipt{}, false, ErrShardPersistence
		}
		if _, err := tx.Exec(ctx, `
UPDATE tickets SET status='cancelled'
WHERE ticket_order_id IN (SELECT id FROM ticket_orders WHERE reservation_id=$1)`, bookingCommand.ReservationID); err != nil {
			return command.Receipt{}, false, ErrShardPersistence
		}
	}
	changed := status != "cancelled"
	if changed {
		if err := appendLifecycleEvent(ctx, tx, bookingCommand, "reservation.cancelled", map[string]any{
			"status": "cancelled", "released_seat_count": released,
		}); err != nil {
			return command.Receipt{}, false, err
		}
	}
	receipt := committedReceipt(bookingCommand)
	receipt.ReleasedSeatCount = released
	return receipt, changed, nil
}

func appendLifecycleEvent(ctx context.Context, tx pgx.Tx, bookingCommand command.Command, eventType string, fields map[string]any) error {
	fields["command_id"] = bookingCommand.ID
	fields["reservation_id"] = bookingCommand.ReservationID
	fields["train_run_id"] = bookingCommand.TrainRunID
	fields["assignment_generation"] = bookingCommand.Route.Generation().Int64()
	payload, err := json.Marshal(fields)
	if err != nil {
		return ErrShardPersistence
	}
	return execOne(ctx, tx, `
INSERT INTO outbox_events(id,train_run_id,assignment_generation,aggregate_type,aggregate_id,event_type,payload)
VALUES($1,$2,$3,'reservation',$4,$5,$6::jsonb)`,
		uuid.NewSHA1(bookingCommand.ID, []byte(eventType)), bookingCommand.TrainRunID,
		bookingCommand.Route.Generation().Int64(), bookingCommand.ReservationID, eventType, string(payload))
}

func recordTargetWrite(ctx context.Context, tx pgx.Tx, bookingCommand command.Command) error {
	return execOne(ctx, tx, `
INSERT INTO train_run_target_write_evidence(
 id,train_run_id,assignment_generation,successful_write_count,
 first_successful_write_at,last_successful_write_at,last_command_id
) VALUES($1,$2,$3,1,clock_timestamp(),clock_timestamp(),$4)
ON CONFLICT(train_run_id,assignment_generation) DO UPDATE
SET successful_write_count=train_run_target_write_evidence.successful_write_count+1,
    first_successful_write_at=COALESCE(train_run_target_write_evidence.first_successful_write_at,EXCLUDED.first_successful_write_at),
    last_successful_write_at=EXCLUDED.last_successful_write_at,last_command_id=EXCLUDED.last_command_id`,
		uuid.NewSHA1(bookingCommand.TrainRunID, []byte("target-write-evidence:"+
			bookingCommand.Route.ShardID().String()+":"+strconv.FormatInt(bookingCommand.Route.Generation().Int64(), 10))),
		bookingCommand.TrainRunID, bookingCommand.Route.Generation().Int64(), bookingCommand.ID)
}
