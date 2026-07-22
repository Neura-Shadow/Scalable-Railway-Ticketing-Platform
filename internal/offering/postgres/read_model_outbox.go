package postgres

import (
	"context"
	"fmt"
)

func appendReadModelEvent(
	ctx context.Context,
	tx DBTX,
	aggregateType string,
	aggregateID string,
	eventType string,
) error {
	if tx == nil {
		return ErrInvalidConfiguration
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
		VALUES ($1, $2::uuid, $3, '{}'::jsonb)
	`, aggregateType, aggregateID, eventType); err != nil {
		return fmt.Errorf("append read-model outbox event: %w", err)
	}
	return nil
}
