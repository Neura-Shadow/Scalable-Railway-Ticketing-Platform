package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

func (tx *Tx) insertReservationLocator(
	ctx context.Context,
	reservationID, ownerID uuid.UUID,
) error {
	if tx == nil || tx.tx == nil || reservationID == uuid.Nil || ownerID == uuid.Nil {
		return ErrInvalidArgument
	}
	if tx.routed == nil {
		return nil
	}
	result, err := tx.tx.Exec(ctx, `
INSERT INTO public.reservation_shard_locators (
    reservation_id, train_run_id, shard_id, assignment_generation, owner_user_id
)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (reservation_id) DO NOTHING`,
		reservationID,
		tx.route.TrainRunID(),
		tx.route.ShardID().String(),
		tx.route.Generation().Int64(),
		ownerID,
	)
	if err != nil {
		return fmt.Errorf("insert reservation locator: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrPersistenceInvariant
	}
	return nil
}

func (tx *Tx) insertTicketOrderLocator(
	ctx context.Context,
	orderID, reservationID, ownerID uuid.UUID,
	status string,
	totalAmountMinor int64,
	currency string,
	createdAt time.Time,
) error {
	if tx == nil || tx.tx == nil || orderID == uuid.Nil || reservationID == uuid.Nil ||
		ownerID == uuid.Nil || status == "" || totalAmountMinor < 0 ||
		currency == "" || createdAt.IsZero() {
		return ErrInvalidArgument
	}
	if tx.routed == nil {
		return nil
	}
	result, err := tx.tx.Exec(ctx, `
INSERT INTO public.ticket_order_shard_locators (
    ticket_order_id, reservation_id, train_run_id, shard_id,
    assignment_generation, owner_user_id, status, total_amount_minor,
    currency, created_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (ticket_order_id) DO NOTHING`,
		orderID,
		reservationID,
		tx.route.TrainRunID(),
		tx.route.ShardID().String(),
		tx.route.Generation().Int64(),
		ownerID,
		status,
		totalAmountMinor,
		currency,
		createdAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("insert ticket-order locator: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrPersistenceInvariant
	}
	return nil
}

func (tx *Tx) insertTicketLocator(
	ctx context.Context,
	ticketID, orderID, reservationID, ownerID uuid.UUID,
) error {
	if tx == nil || tx.tx == nil || ticketID == uuid.Nil || orderID == uuid.Nil ||
		reservationID == uuid.Nil || ownerID == uuid.Nil {
		return ErrInvalidArgument
	}
	if tx.routed == nil {
		return nil
	}
	result, err := tx.tx.Exec(ctx, `
INSERT INTO public.ticket_shard_locators (
    ticket_id, ticket_order_id, reservation_id, train_run_id,
    shard_id, assignment_generation, owner_user_id
)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (ticket_id) DO NOTHING`,
		ticketID,
		orderID,
		reservationID,
		tx.route.TrainRunID(),
		tx.route.ShardID().String(),
		tx.route.Generation().Int64(),
		ownerID,
	)
	if err != nil {
		return fmt.Errorf("insert ticket locator: %w", err)
	}
	if result.RowsAffected() != 1 {
		return ErrPersistenceInvariant
	}
	return nil
}

func (tx *Tx) updateTicketOrderLocatorStatus(ctx context.Context, reservationID uuid.UUID, status string) error {
	if tx == nil || tx.tx == nil || reservationID == uuid.Nil || status == "" {
		return ErrInvalidArgument
	}
	if tx.routed == nil {
		return nil
	}
	if _, err := tx.tx.Exec(ctx, `
UPDATE public.ticket_order_shard_locators
SET status = $2,
    updated_at = clock_timestamp()
WHERE reservation_id = $1`, reservationID, status); err != nil {
		return fmt.Errorf("update ticket-order locator: %w", err)
	}
	return nil
}
