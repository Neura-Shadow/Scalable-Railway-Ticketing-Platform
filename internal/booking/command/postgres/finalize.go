package postgres

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (repository *Repository) Finalize(
	ctx context.Context,
	bookingCommand command.Command,
	receipt command.Receipt,
) error {
	if repository == nil || ctx == nil || receipt.Status != command.ReceiptCommitted ||
		receipt.CommandID != bookingCommand.ID || receipt.ResultResourceID != bookingCommand.ReservationID ||
		receipt.RequestFingerprint != bookingCommand.RequestFingerprint {
		return ErrControlWrite
	}
	if bookingCommand.Operation == command.OperationConfirmReservation ||
		bookingCommand.Operation == command.OperationCancelReservation {
		return repository.finalizeLifecycle(ctx, bookingCommand, receipt)
	}
	tx, err := repository.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ErrControlWrite
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var state string
	var fingerprint []byte
	var reservationID uuid.UUID
	if err := tx.QueryRow(ctx, `
SELECT state, request_fingerprint, reservation_id
FROM public.booking_commands
WHERE command_id = $1
FOR UPDATE`, bookingCommand.ID).Scan(&state, &fingerprint, &reservationID); err != nil {
		return ErrControlWrite
	}
	if len(fingerprint) != 32 || !bytes.Equal(fingerprint, receipt.RequestFingerprint[:]) ||
		reservationID != receipt.ResultResourceID || state == string(command.StateFailed) || state == string(command.StateExpired) {
		return ErrIdempotencyConflict
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.booking_commands
SET state = 'finalized',
    result_resource_id = $2,
    finalized_at = COALESCE(finalized_at, clock_timestamp()),
    lease_owner = NULL,
    lease_until = NULL,
    bounded_error_category = NULL
WHERE command_id = $1`, bookingCommand.ID, receipt.ResultResourceID); err != nil {
		return ErrControlWrite
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.reservation_directory
SET state = 'active',
    active_at = COALESCE(active_at, clock_timestamp()),
    last_known_shard_id = $2,
    last_known_generation = $3,
    bounded_error_category = NULL
WHERE reservation_id = $1
  AND command_id = $4
  AND state IN ('pending', 'active')`,
		receipt.ResultResourceID,
		bookingCommand.Route.ShardID().String(),
		bookingCommand.Route.Generation().Int64(),
		bookingCommand.ID,
	); err != nil {
		return ErrControlWrite
	}
	locatorTag, err := tx.Exec(ctx, `
INSERT INTO public.reservation_shard_locators(
 reservation_id,train_run_id,shard_id,assignment_generation,owner_user_id
) VALUES($1,$2,$3,$4,$5)
ON CONFLICT(reservation_id) DO UPDATE
SET train_run_id=EXCLUDED.train_run_id,shard_id=EXCLUDED.shard_id,
    assignment_generation=EXCLUDED.assignment_generation,owner_user_id=EXCLUDED.owner_user_id
WHERE reservation_shard_locators.train_run_id=EXCLUDED.train_run_id
  AND reservation_shard_locators.shard_id=EXCLUDED.shard_id
  AND reservation_shard_locators.assignment_generation=EXCLUDED.assignment_generation
  AND reservation_shard_locators.owner_user_id=EXCLUDED.owner_user_id`,
		bookingCommand.ReservationID, bookingCommand.TrainRunID, bookingCommand.Route.ShardID().String(),
		bookingCommand.Route.Generation().Int64(), bookingCommand.OwnerUserID)
	if err != nil || locatorTag.RowsAffected() != 1 {
		return ErrControlWrite
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.booking_quota_leases
SET state = 'active_hold'
WHERE command_id = $1
  AND state IN ('pending', 'active_hold', 'repair_required')`, bookingCommand.ID); err != nil {
		return ErrControlWrite
	}
	payload, err := json.Marshal(struct {
		CommandID     uuid.UUID `json:"command_id"`
		ReservationID uuid.UUID `json:"reservation_id"`
	}{CommandID: bookingCommand.ID, ReservationID: receipt.ResultResourceID})
	if err != nil {
		return ErrControlWrite
	}
	eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("booking-command.finalized:"+bookingCommand.ID.String()))
	if _, err := tx.Exec(ctx, `
INSERT INTO public.outbox_events (
    id, aggregate_type, aggregate_id, event_type, event_version, payload
) VALUES ($1, 'booking_command', $2, 'booking_command.finalized', 1, $3)
ON CONFLICT (id) DO NOTHING`, eventID, bookingCommand.ID, payload); err != nil {
		return ErrControlWrite
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrControlWrite
	}
	repository.recordQuota("finalize", "committed", "none")
	repository.recordDirectory("success", "none")
	return nil
}

func (repository *Repository) finalizeLifecycle(ctx context.Context, bookingCommand command.Command, receipt command.Receipt) error {
	tx, err := repository.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ErrControlWrite
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var state string
	var fingerprint []byte
	var reservationID uuid.UUID
	if err := tx.QueryRow(ctx, `
SELECT state, request_fingerprint, reservation_id
FROM public.booking_commands WHERE command_id=$1 FOR UPDATE`, bookingCommand.ID).Scan(
		&state, &fingerprint, &reservationID); err != nil || len(fingerprint) != 32 ||
		!bytes.Equal(fingerprint, receipt.RequestFingerprint[:]) || reservationID != receipt.ResultResourceID ||
		state == string(command.StateFailed) || state == string(command.StateExpired) {
		return ErrControlWrite
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.booking_commands
SET state='finalized', result_resource_id=$2,
    finalized_at=COALESCE(finalized_at,clock_timestamp()),
    lease_owner=NULL, lease_until=NULL, bounded_error_category=NULL
WHERE command_id=$1`, bookingCommand.ID, receipt.ResultResourceID); err != nil {
		return ErrControlWrite
	}
	if bookingCommand.Operation == command.OperationConfirmReservation {
		if !validConfirmationReceipt(receipt) || receipt.TotalAmountMinor < 0 ||
			len(receipt.Currency) != 3 || receipt.OrderCreatedAt.IsZero() {
			return ErrControlWrite
		}
		locatorTag, err := tx.Exec(ctx, `
INSERT INTO public.ticket_order_shard_locators(
 ticket_order_id,reservation_id,train_run_id,shard_id,assignment_generation,
 owner_user_id,status,total_amount_minor,currency,created_at
) VALUES($1,$2,$3,$4,$5,$6,'confirmed',$7,$8,$9)
ON CONFLICT(ticket_order_id) DO UPDATE
SET status='confirmed',total_amount_minor=EXCLUDED.total_amount_minor,
    currency=EXCLUDED.currency,created_at=EXCLUDED.created_at
WHERE ticket_order_shard_locators.reservation_id=EXCLUDED.reservation_id
  AND ticket_order_shard_locators.owner_user_id=EXCLUDED.owner_user_id
  AND ticket_order_shard_locators.train_run_id=EXCLUDED.train_run_id
  AND ticket_order_shard_locators.shard_id=EXCLUDED.shard_id
  AND ticket_order_shard_locators.assignment_generation=EXCLUDED.assignment_generation`,
			receipt.TicketOrderID, bookingCommand.ReservationID, bookingCommand.TrainRunID,
			bookingCommand.Route.ShardID().String(), bookingCommand.Route.Generation().Int64(),
			bookingCommand.OwnerUserID, receipt.TotalAmountMinor, receipt.Currency, receipt.OrderCreatedAt.UTC())
		if err != nil || locatorTag.RowsAffected() != 1 {
			return ErrControlWrite
		}
		if err := insertTicketLocators(ctx, tx, bookingCommand, receipt); err != nil {
			return err
		}
	} else if bookingCommand.Operation == command.OperationCancelReservation {
		if _, err := tx.Exec(ctx, `
UPDATE public.ticket_order_shard_locators SET status='cancelled'
WHERE reservation_id=$1 AND owner_user_id=$2`, bookingCommand.ReservationID, bookingCommand.OwnerUserID); err != nil {
			return ErrControlWrite
		}
	}
	// The directory remains a locator, not a lifecycle state store. Release the
	// create command's global quota only after the shard receipt has committed.
	if _, err := tx.Exec(ctx, `
UPDATE public.booking_quota_leases AS quota
SET state='released', released_at=COALESCE(released_at,clock_timestamp())
FROM public.reservation_directory AS directory
WHERE directory.reservation_id=$1
  AND quota.command_id=directory.command_id
  AND quota.state IN ('pending','active_hold','repair_required','released')`,
		bookingCommand.ReservationID); err != nil {
		return ErrControlWrite
	}
	payload, err := json.Marshal(struct {
		CommandID     uuid.UUID         `json:"command_id"`
		ReservationID uuid.UUID         `json:"reservation_id"`
		Operation     command.Operation `json:"operation"`
	}{bookingCommand.ID, bookingCommand.ReservationID, bookingCommand.Operation})
	if err != nil {
		return ErrControlWrite
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.outbox_events(id,aggregate_type,aggregate_id,event_type,event_version,payload)
VALUES($1,'booking_command',$2,'booking_command.finalized',1,$3)
ON CONFLICT(id) DO NOTHING`,
		uuid.NewSHA1(uuid.NameSpaceOID, []byte("booking-command.finalized:"+bookingCommand.ID.String())),
		bookingCommand.ID, payload); err != nil {
		return ErrControlWrite
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrControlWrite
	}
	repository.recordQuota("release", "committed", "none")
	repository.recordDirectory("success", "none")
	return nil
}

func validConfirmationReceipt(receipt command.Receipt) bool {
	if receipt.TicketOrderID == uuid.Nil || receipt.TicketCount < 1 ||
		receipt.TicketCount > command.MaxReceiptTickets || len(receipt.TicketIDs) != receipt.TicketCount {
		return false
	}
	seen := make(map[uuid.UUID]struct{}, len(receipt.TicketIDs))
	for _, ticketID := range receipt.TicketIDs {
		if ticketID == uuid.Nil {
			return false
		}
		if _, duplicate := seen[ticketID]; duplicate {
			return false
		}
		seen[ticketID] = struct{}{}
	}
	return true
}

func insertTicketLocators(ctx context.Context, tx pgx.Tx, bookingCommand command.Command, receipt command.Receipt) error {
	for _, ticketID := range receipt.TicketIDs {
		tag, err := tx.Exec(ctx, `
INSERT INTO public.ticket_shard_locators(
 ticket_id,ticket_order_id,reservation_id,train_run_id,shard_id,assignment_generation,owner_user_id
) VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT(ticket_id) DO UPDATE SET ticket_order_id=EXCLUDED.ticket_order_id
WHERE ticket_shard_locators.ticket_order_id=EXCLUDED.ticket_order_id
  AND ticket_shard_locators.reservation_id=EXCLUDED.reservation_id
  AND ticket_shard_locators.owner_user_id=EXCLUDED.owner_user_id
  AND ticket_shard_locators.train_run_id=EXCLUDED.train_run_id
  AND ticket_shard_locators.shard_id=EXCLUDED.shard_id
  AND ticket_shard_locators.assignment_generation=EXCLUDED.assignment_generation`,
			ticketID, receipt.TicketOrderID, bookingCommand.ReservationID, bookingCommand.TrainRunID,
			bookingCommand.Route.ShardID().String(), bookingCommand.Route.Generation().Int64(), bookingCommand.OwnerUserID)
		if err != nil || tag.RowsAffected() != 1 {
			return ErrControlWrite
		}
	}
	return nil
}
