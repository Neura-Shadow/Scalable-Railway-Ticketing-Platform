package readmodel

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const MaxOutboxReplayBatchSize = 100

type OutboxReplayOptions struct {
	After string
	Limit int
}

type OutboxReplayPage struct {
	Events     []ProjectionEvent
	NextCursor string
	HasMore    bool
}

type replayCandidate struct {
	event       ProjectionEvent
	publishedAt time.Time
}

func (s *Store) MissingPublishedEvents(
	ctx context.Context,
	consumerName string,
	options OutboxReplayOptions,
) (OutboxReplayPage, error) {
	if !validRuntimeIdentifier(consumerName, 128) || options.Limit < 1 || options.Limit > MaxOutboxReplayBatchSize {
		return OutboxReplayPage{}, ErrInvalidEvent
	}
	afterPublishedAt, afterID, err := parseOutboxReplayCursor(options.After)
	if err != nil {
		return OutboxReplayPage{}, err
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return OutboxReplayPage{}, fmt.Errorf("%w: begin missing outbox scan", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	rows, err := tx.Query(ctx, `
		SELECT o.id, o.event_type, o.aggregate_type, o.aggregate_id, o.published_at
		FROM outbox_events AS o
		WHERE o.status = 'published'
		  AND o.published_at IS NOT NULL
		  AND o.event_type = ANY($1::text[])
		  AND NOT EXISTS (
			SELECT 1
			FROM read_model_event_receipts AS receipt
			WHERE receipt.consumer_name = $2
			  AND receipt.event_id = o.id
		  )
		  AND ($3::timestamptz IS NULL OR (o.published_at, o.id) > ($3, $4))
		ORDER BY o.published_at, o.id
		LIMIT $5
	`, allReadModelEventTypes(), consumerName, afterPublishedAt, afterID, options.Limit+1)
	if err != nil {
		return OutboxReplayPage{}, fmt.Errorf("%w: query missing outbox events", ErrPersistence)
	}
	defer rows.Close()
	candidates := make([]replayCandidate, 0, options.Limit+1)
	for rows.Next() {
		var candidate replayCandidate
		var eventID, aggregateID uuid.UUID
		if err := rows.Scan(
			&eventID,
			&candidate.event.EventType,
			&candidate.event.AggregateType,
			&aggregateID,
			&candidate.publishedAt,
		); err != nil {
			return OutboxReplayPage{}, fmt.Errorf("%w: scan missing outbox event", ErrPersistence)
		}
		candidate.event.ConsumerName = consumerName
		candidate.event.EventID = eventID.String()
		candidate.event.AggregateID = aggregateID.String()
		if !validEventPair(candidate.event.EventType, candidate.event.AggregateType) {
			return OutboxReplayPage{}, ErrInvalidEvent
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return OutboxReplayPage{}, fmt.Errorf("%w: iterate missing outbox events", ErrPersistence)
	}
	page := OutboxReplayPage{HasMore: len(candidates) > options.Limit}
	if page.HasMore {
		candidates = candidates[:options.Limit]
	}
	page.Events = make([]ProjectionEvent, 0, len(candidates))
	for _, candidate := range candidates {
		page.Events = append(page.Events, candidate.event)
		page.NextCursor = formatOutboxReplayCursor(candidate.publishedAt, candidate.event.EventID)
	}
	if err := tx.Commit(ctx); err != nil {
		return OutboxReplayPage{}, fmt.Errorf("%w: commit missing outbox scan", ErrPersistence)
	}
	return page, nil
}

func parseOutboxReplayCursor(raw string) (*time.Time, uuid.UUID, error) {
	if raw == "" {
		return nil, uuid.Nil, nil
	}
	parts := strings.Split(raw, "|")
	if len(parts) != 2 {
		return nil, uuid.Nil, ErrInvalidEvent
	}
	publishedAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return nil, uuid.Nil, ErrInvalidEvent
	}
	eventID, err := uuid.Parse(parts[1])
	if err != nil || eventID == uuid.Nil || eventID.String() != parts[1] {
		return nil, uuid.Nil, ErrInvalidEvent
	}
	return &publishedAt, eventID, nil
}

func formatOutboxReplayCursor(publishedAt time.Time, eventID string) string {
	return publishedAt.UTC().Format(time.RFC3339Nano) + "|" + eventID
}

func allReadModelEventTypes() []string {
	return []string{
		"reservation.held", "reservation.confirmed", "reservation.expired", "reservation.cancelled",
		"ticket.created",
		"trainrun.created", "trainrun.updated", "trainrun.cancelled",
		"hot_train_policy.created", "hot_train_policy.updated", "hot_train_policy.disabled",
		"station.created", "station.updated", "station.disabled",
		"route.created", "route.updated", "route.disabled",
		"train.updated", "coach.updated", "seat.updated",
		"fare.created", "fare.updated", "fare.disabled",
	}
}

func projectionReadModelEventTypes() []string {
	return []string{
		"trainrun.created", "trainrun.updated", "trainrun.cancelled",
		"station.created", "station.updated", "station.disabled",
		"route.created", "route.updated", "route.disabled",
		"train.updated", "fare.created", "fare.updated", "fare.disabled",
	}
}
