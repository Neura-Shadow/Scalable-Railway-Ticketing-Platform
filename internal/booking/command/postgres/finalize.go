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
	return nil
}
