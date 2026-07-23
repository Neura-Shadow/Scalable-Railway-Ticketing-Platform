package readmodel

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ProjectionLag reports the age of the oldest projection-affecting outbox
// event that has not reached the durable read-model receipt boundary. It
// therefore keeps increasing while publication or projection processing is
// stopped, including before a progress row exists.
func (s *Store) ProjectionLag(ctx context.Context, consumerName string) (time.Duration, error) {
	if !validRuntimeIdentifier(consumerName, 128) {
		return 0, ErrInvalidEvent
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, fmt.Errorf("%w: begin projection lag observation", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var lagSeconds float64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(
			EXTRACT(EPOCH FROM (clock_timestamp() - min(event.created_at))),
			0
		)
		FROM outbox_events AS event
		WHERE event.event_type = ANY($1::text[])
		  AND NOT EXISTS (
			SELECT 1
			FROM read_model_event_receipts AS receipt
			WHERE receipt.consumer_name = $2
			  AND receipt.event_id = event.id
		)
	`, projectionReadModelEventTypes(), consumerName).Scan(&lagSeconds); err != nil {
		return 0, fmt.Errorf("%w: query projection lag", ErrPersistence)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("%w: commit projection lag observation", ErrPersistence)
	}
	if lagSeconds <= 0 {
		return 0, nil
	}
	return time.Duration(lagSeconds * float64(time.Second)), nil
}

// NextReconciliationTrainRun returns one stable, bounded reconciliation
// candidate. Callers retain the returned UUID as the next cursor and reset the
// cursor after found=false.
func (s *Store) NextReconciliationTrainRun(ctx context.Context, after string) (string, bool, error) {
	afterID := uuid.Nil
	if after != "" {
		parsed, err := uuid.Parse(after)
		if err != nil || parsed == uuid.Nil || parsed.String() != after {
			return "", false, ErrInvalidTrainRunID
		}
		afterID = parsed
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return "", false, fmt.Errorf("%w: begin reconciliation candidate scan", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var trainRunID uuid.UUID
	if err := tx.QueryRow(ctx, `
		SELECT id
		FROM train_runs
		WHERE id > $1
		ORDER BY id
		LIMIT 1
	`, afterID).Scan(&trainRunID); err != nil {
		if err == pgx.ErrNoRows {
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return "", false, fmt.Errorf("%w: commit empty reconciliation candidate scan", ErrPersistence)
			}
			return "", false, nil
		}
		return "", false, fmt.Errorf("%w: query reconciliation candidate", ErrPersistence)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", false, fmt.Errorf("%w: commit reconciliation candidate scan", ErrPersistence)
	}
	return trainRunID.String(), true, nil
}
