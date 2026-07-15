package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/eventrelay/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, errors.New("outbox store requires a database pool")
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Claim(ctx context.Context, workerID string, batchSize int, now, staleBefore time.Time) ([]domain.Event, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin outbox claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
WITH candidates AS MATERIALIZED (
    SELECT id
    FROM outbox_events
    WHERE (status = 'pending' AND next_attempt_at <= $1)
       OR (status = 'processing' AND locked_at <= $2)
    ORDER BY created_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT $3
), claimed AS (
    UPDATE outbox_events AS event
    SET status = 'processing',
        attempts = event.attempts + 1,
        locked_at = $1,
        locked_by = $4
    FROM candidates
    WHERE event.id = candidates.id
    RETURNING event.id, event.aggregate_type, event.aggregate_id,
              event.event_type, event.event_version, event.payload,
              event.attempts, event.created_at
)
SELECT id, aggregate_type, aggregate_id, event_type, event_version,
       payload, attempts, created_at
FROM claimed
ORDER BY created_at, id`, now, staleBefore, batchSize, workerID)
	if err != nil {
		return nil, fmt.Errorf("claim outbox rows: %w", err)
	}
	events, err := pgx.CollectRows(rows, pgx.RowToStructByName[domain.Event])
	if err != nil {
		return nil, fmt.Errorf("scan outbox rows: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit outbox claim: %w", err)
	}
	return events, nil
}

func (s *Store) MarkPublished(ctx context.Context, eventID uuid.UUID, workerID string, publishedAt time.Time) error {
	tag, err := s.pool.Exec(ctx, `
UPDATE outbox_events
SET status = 'published', published_at = $3, locked_at = NULL, locked_by = NULL
WHERE id = $1 AND status = 'processing' AND locked_by = $2`, eventID, workerID, publishedAt)
	if err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("outbox finalize ownership conflict")
	}
	return nil
}

func (s *Store) MarkFailed(ctx context.Context, eventID uuid.UUID, workerID string, nextAttemptAt time.Time, deadLetter bool) error {
	status := "pending"
	if deadLetter {
		status = "dead_letter"
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE outbox_events
SET status = $3, next_attempt_at = $4, locked_at = NULL, locked_by = NULL
WHERE id = $1 AND status = 'processing' AND locked_by = $2`, eventID, workerID, status, nextAttemptAt)
	if err != nil {
		return fmt.Errorf("mark outbox failed: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("outbox retry ownership conflict")
	}
	return nil
}
