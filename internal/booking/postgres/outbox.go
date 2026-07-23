package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

const maxOutboxPayloadBytes = 64 * 1024

func (tx *Tx) appendReservationEvent(ctx context.Context, reservationID uuid.UUID, eventType string, payload map[string]any) error {
	if reservationID == uuid.Nil || !validReservationEvent(eventType) {
		return ErrInvalidArgument
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode outbox payload: %w", err)
	}
	if len(encoded) > maxOutboxPayloadBytes {
		return ErrInvalidArgument
	}
	query := `
INSERT INTO outbox_events (
    aggregate_type, aggregate_id, event_type, event_version, payload
)
VALUES ('reservation', $1, $2, 1, $3::jsonb)`
	args := []any{reservationID, eventType, encoded}
	if tx.routed != nil {
		query = `
INSERT INTO public.outbox_events (
    aggregate_type, aggregate_id, event_type, event_version, payload,
    train_run_id, shard_id, assignment_generation
)
VALUES ('reservation', $1, $2, 1, $3::jsonb, $4, $5, $6)`
		args = append(args, tx.route.TrainRunID(), tx.route.ShardID().String(), tx.route.Generation().Int64())
	}
	if _, err := tx.tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("append outbox event: %w", err)
	}
	return nil
}

func (tx *Tx) appendTicketEvent(ctx context.Context, ticketID uuid.UUID, payload map[string]any) error {
	if ticketID == uuid.Nil {
		return ErrInvalidArgument
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode ticket outbox payload: %w", err)
	}
	if len(encoded) > maxOutboxPayloadBytes {
		return ErrInvalidArgument
	}
	query := `
INSERT INTO outbox_events (
    aggregate_type, aggregate_id, event_type, event_version, payload
)
VALUES ('ticket', $1, 'ticket.created', 1, $2::jsonb)`
	args := []any{ticketID, encoded}
	if tx.routed != nil {
		query = `
INSERT INTO public.outbox_events (
    aggregate_type, aggregate_id, event_type, event_version, payload,
    train_run_id, shard_id, assignment_generation
)
VALUES ('ticket', $1, 'ticket.created', 1, $2::jsonb, $3, $4, $5)`
		args = append(args, tx.route.TrainRunID(), tx.route.ShardID().String(), tx.route.Generation().Int64())
	}
	if _, err := tx.tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("append ticket outbox event: %w", err)
	}
	return nil
}

func validReservationEvent(value string) bool {
	switch value {
	case "reservation.held", "reservation.confirmed", "reservation.expired", "reservation.cancelled":
		return true
	default:
		return false
	}
}
