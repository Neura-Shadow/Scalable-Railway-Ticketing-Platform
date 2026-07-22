package readmodel

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	eventPhaseInvalidating = "invalidating"
	eventPhaseProcessing   = "processing"
	eventPhaseFinalizing   = "finalizing"
)

var (
	ErrProjectionPending     = errors.New("read-model projection event has more bounded work")
	ErrProjectionUnavailable = errors.New("read-model projection temporarily unavailable")
)

type EventProgress struct {
	Complete           bool
	Phase              string
	AfterTrainRunID    string
	ProcessedTrainRuns int
}

func (s *Store) BeginEventProgress(
	ctx context.Context,
	event ProjectionEvent,
	projectionAffecting bool,
) (EventProgress, error) {
	eventID, aggregateID, err := validateProjectionEvent(event)
	if err != nil {
		return EventProgress{}, err
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return EventProgress{}, fmt.Errorf("%w: begin event progress", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockProjectionEvent(ctx, tx, event.ConsumerName, eventID); err != nil {
		return EventProgress{}, err
	}

	receiptExists, err := existingReceiptMatches(ctx, tx, event, eventID, aggregateID)
	if err != nil {
		return EventProgress{}, err
	}
	if receiptExists {
		if err := tx.Commit(ctx); err != nil {
			return EventProgress{}, fmt.Errorf("%w: commit completed event lookup", ErrPersistence)
		}
		return EventProgress{Complete: true}, nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO read_model_event_progress (
			consumer_name, event_id, event_type, aggregate_type, aggregate_id,
			projection_affecting
		) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (consumer_name, event_id) DO NOTHING
	`, event.ConsumerName, eventID, event.EventType, event.AggregateType, aggregateID, projectionAffecting); err != nil {
		return EventProgress{}, fmt.Errorf("%w: create event progress", ErrPersistence)
	}
	progress, err := loadEventProgressForUpdate(ctx, tx, event, eventID, aggregateID, projectionAffecting)
	if err != nil {
		return EventProgress{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return EventProgress{}, fmt.Errorf("%w: commit event progress", ErrPersistence)
	}
	return progress, nil
}

func (s *Store) MarkEventInvalidated(
	ctx context.Context,
	event ProjectionEvent,
	projectionAffecting bool,
) (EventProgress, error) {
	eventID, aggregateID, err := validateProjectionEvent(event)
	if err != nil {
		return EventProgress{}, err
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return EventProgress{}, fmt.Errorf("%w: begin invalidation checkpoint", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockProjectionEvent(ctx, tx, event.ConsumerName, eventID); err != nil {
		return EventProgress{}, err
	}
	progress, err := loadEventProgressForUpdate(ctx, tx, event, eventID, aggregateID, projectionAffecting)
	if err != nil {
		return EventProgress{}, err
	}
	if progress.Phase == eventPhaseInvalidating {
		if _, err := tx.Exec(ctx, `
			UPDATE read_model_event_progress
			SET phase = $3, updated_at = clock_timestamp()
			WHERE consumer_name = $1 AND event_id = $2
		`, event.ConsumerName, eventID, eventPhaseProcessing); err != nil {
			return EventProgress{}, fmt.Errorf("%w: checkpoint event invalidation", ErrPersistence)
		}
		progress.Phase = eventPhaseProcessing
	}
	if err := tx.Commit(ctx); err != nil {
		return EventProgress{}, fmt.Errorf("%w: commit invalidation checkpoint", ErrPersistence)
	}
	return progress, nil
}

func (s *Store) ProcessEventPage(
	ctx context.Context,
	event ProjectionEvent,
	projectionAffecting bool,
	expectedAfter string,
	rawTrainRunIDs []string,
	hasMore bool,
) (ProcessEventResult, error) {
	eventID, aggregateID, err := validateProjectionEvent(event)
	if err != nil {
		return ProcessEventResult{}, err
	}
	trainRunIDs, err := parseBoundedTrainRunIDs(rawTrainRunIDs)
	if err != nil {
		return ProcessEventResult{}, err
	}
	if hasMore && len(trainRunIDs) == 0 {
		return ProcessEventResult{}, ErrInvalidEvent
	}
	if _, err := parseEventCursor(expectedAfter); err != nil {
		return ProcessEventResult{}, err
	}
	processedAt := s.clock.Now().UTC()
	if processedAt.IsZero() {
		return ProcessEventResult{}, ErrInvalidStore
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ProcessEventResult{}, fmt.Errorf("%w: begin event page", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockProjectionEvent(ctx, tx, event.ConsumerName, eventID); err != nil {
		return ProcessEventResult{}, err
	}
	progress, err := loadEventProgressForUpdate(ctx, tx, event, eventID, aggregateID, projectionAffecting)
	if err != nil {
		return ProcessEventResult{}, err
	}
	if progress.Phase != eventPhaseProcessing || progress.AfterTrainRunID != expectedAfter {
		return ProcessEventResult{}, ErrProjectionPending
	}
	for _, trainRunID := range trainRunIDs {
		if expectedAfter != "" && strings.Compare(trainRunID.String(), expectedAfter) <= 0 {
			return ProcessEventResult{}, ErrInvalidEvent
		}
		if err := lockTrainRunProjection(ctx, tx, trainRunID); err != nil {
			return ProcessEventResult{}, err
		}
	}

	result := ProcessEventResult{}
	if projectionAffecting {
		for _, trainRunID := range trainRunIDs {
			rebuild, rebuildErr := rebuildTrainRunTx(ctx, tx, trainRunID, processedAt)
			if rebuildErr != nil {
				return ProcessEventResult{}, rebuildErr
			}
			result.TrainRunsRebuilt++
			result.RowsWritten += rebuild.RowsWritten
			result.Deleted = result.Deleted || rebuild.Deleted
		}
	}
	nextAfter := expectedAfter
	if len(trainRunIDs) > 0 {
		nextAfter = trainRunIDs[len(trainRunIDs)-1].String()
	}
	nextPhase := eventPhaseFinalizing
	if hasMore {
		nextPhase = eventPhaseProcessing
	}
	if _, err := tx.Exec(ctx, `
		UPDATE read_model_event_progress
		SET phase = $3,
			after_train_run_id = NULLIF($4, '')::uuid,
			processed_train_runs = processed_train_runs + $5,
			updated_at = clock_timestamp()
		WHERE consumer_name = $1 AND event_id = $2
	`, event.ConsumerName, eventID, nextPhase, nextAfter, len(trainRunIDs)); err != nil {
		return ProcessEventResult{}, fmt.Errorf("%w: checkpoint event page", ErrPersistence)
	}
	if err := tx.Commit(ctx); err != nil {
		return ProcessEventResult{}, fmt.Errorf("%w: commit event page", ErrPersistence)
	}
	return result, nil
}

func (s *Store) CompleteEvent(ctx context.Context, event ProjectionEvent, projectionAffecting bool) (bool, error) {
	eventID, aggregateID, err := validateProjectionEvent(event)
	if err != nil {
		return false, err
	}
	processedAt := s.clock.Now().UTC()
	if processedAt.IsZero() {
		return false, ErrInvalidStore
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, fmt.Errorf("%w: begin event completion", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockProjectionEvent(ctx, tx, event.ConsumerName, eventID); err != nil {
		return false, err
	}
	receiptExists, err := existingReceiptMatches(ctx, tx, event, eventID, aggregateID)
	if err != nil {
		return false, err
	}
	if receiptExists {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("%w: commit duplicate event completion", ErrPersistence)
		}
		return true, nil
	}
	progress, err := loadEventProgressForUpdate(ctx, tx, event, eventID, aggregateID, projectionAffecting)
	if err != nil {
		return false, err
	}
	if progress.Phase != eventPhaseFinalizing {
		return false, ErrProjectionPending
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO read_model_event_receipts (
			consumer_name, event_id, event_type, aggregate_type, aggregate_id, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6)
	`, event.ConsumerName, eventID, event.EventType, event.AggregateType, aggregateID, processedAt); err != nil {
		return false, fmt.Errorf("%w: write completed event receipt", ErrPersistence)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM read_model_event_progress
		WHERE consumer_name = $1 AND event_id = $2
	`, event.ConsumerName, eventID); err != nil {
		return false, fmt.Errorf("%w: clear completed event progress", ErrPersistence)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("%w: commit completed event", ErrPersistence)
	}
	return false, nil
}

func loadEventProgressForUpdate(
	ctx context.Context,
	tx pgx.Tx,
	event ProjectionEvent,
	eventID uuid.UUID,
	aggregateID uuid.UUID,
	projectionAffecting bool,
) (EventProgress, error) {
	var progress EventProgress
	var eventType string
	var aggregateType string
	var storedAggregateID uuid.UUID
	var storedProjectionAffecting bool
	if err := tx.QueryRow(ctx, `
		SELECT event_type, aggregate_type, aggregate_id, projection_affecting,
			phase, COALESCE(after_train_run_id::text, ''), processed_train_runs
		FROM read_model_event_progress
		WHERE consumer_name = $1 AND event_id = $2
		FOR UPDATE
	`, event.ConsumerName, eventID).Scan(
		&eventType,
		&aggregateType,
		&storedAggregateID,
		&storedProjectionAffecting,
		&progress.Phase,
		&progress.AfterTrainRunID,
		&progress.ProcessedTrainRuns,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return EventProgress{}, ErrProjectionPending
		}
		return EventProgress{}, fmt.Errorf("%w: read event progress", ErrPersistence)
	}
	if eventType != event.EventType || aggregateType != event.AggregateType ||
		storedAggregateID != aggregateID || storedProjectionAffecting != projectionAffecting {
		return EventProgress{}, ErrInvalidEvent
	}
	return progress, nil
}

func existingReceiptMatches(
	ctx context.Context,
	tx pgx.Tx,
	event ProjectionEvent,
	eventID uuid.UUID,
	aggregateID uuid.UUID,
) (bool, error) {
	var eventType string
	var aggregateType string
	var storedAggregateID uuid.UUID
	err := tx.QueryRow(ctx, `
		SELECT event_type, aggregate_type, aggregate_id
		FROM read_model_event_receipts
		WHERE consumer_name = $1 AND event_id = $2
	`, event.ConsumerName, eventID).Scan(&eventType, &aggregateType, &storedAggregateID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("%w: read event receipt", ErrPersistence)
	}
	if eventType != event.EventType || aggregateType != event.AggregateType || storedAggregateID != aggregateID {
		return false, ErrInvalidEvent
	}
	return true, nil
}

func lockProjectionEvent(ctx context.Context, tx pgx.Tx, consumerName string, eventID uuid.UUID) error {
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(hashtextextended($1, 7213008))
	`, consumerName+":"+eventID.String()); err != nil {
		return fmt.Errorf("%w: lock projection event", ErrPersistence)
	}
	return nil
}

func parseEventCursor(raw string) (uuid.UUID, error) {
	if raw == "" {
		return uuid.Nil, nil
	}
	value, err := uuid.Parse(raw)
	if err != nil || value == uuid.Nil || value.String() != raw {
		return uuid.Nil, ErrInvalidEvent
	}
	return value, nil
}
