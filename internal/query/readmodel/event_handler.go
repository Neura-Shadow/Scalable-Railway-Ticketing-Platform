package readmodel

import (
	"context"
	"fmt"
	"strings"
	"time"

	querycache "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/cache"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type EventImpactResolver interface {
	AffectedTrainRunIDs(context.Context, ProjectionEvent, string, int) (EventImpactPage, error)
}

type EventImpactPage struct {
	TrainRunIDs []string
	HasMore     bool
}

type VersionRotator interface {
	Rotate(context.Context, string) (string, error)
}

type EventCoordinator struct {
	store    *Store
	impacts  EventImpactResolver
	versions VersionRotator
	metrics  EventMetrics
}

type EventMetrics interface {
	RecordEvent(eventType, result string)
	RecordDuplicateEvent(eventType string)
	RecordRebuild(result, reason string, rows int64, duration time.Duration)
	RecordCacheInvalidation(cacheType, eventType, result, reason string)
}

func NewEventCoordinator(
	store *Store,
	impacts EventImpactResolver,
	versions VersionRotator,
) (*EventCoordinator, error) {
	if store == nil || impacts == nil || versions == nil {
		return nil, ErrInvalidWorker
	}
	return &EventCoordinator{store: store, impacts: impacts, versions: versions}, nil
}

func (coordinator *EventCoordinator) WithMetrics(metrics EventMetrics) *EventCoordinator {
	if coordinator != nil {
		coordinator.metrics = metrics
	}
	return coordinator
}

func (coordinator *EventCoordinator) HandleEvent(ctx context.Context, event ProjectionEvent) error {
	if !validEventPair(event.EventType, event.AggregateType) {
		coordinator.recordEvent(event.EventType, "failure")
		return ErrInvalidEvent
	}
	projectionAffecting := projectionAffectingEvent(event.EventType)
	progress, err := coordinator.store.BeginEventProgress(ctx, event, projectionAffecting)
	if err != nil {
		coordinator.recordEvent(event.EventType, "failure")
		return err
	}
	if progress.Complete {
		coordinator.recordEvent(event.EventType, "success")
		coordinator.recordDuplicate(event.EventType)
		return nil
	}
	if progress.Phase == eventPhaseInvalidating {
		if err := coordinator.rotateGlobalCaches(ctx, event); err != nil {
			coordinator.recordEvent(event.EventType, "failure")
			return err
		}
		progress, err = coordinator.store.MarkEventInvalidated(ctx, event, projectionAffecting)
		if err != nil {
			coordinator.recordEvent(event.EventType, "failure")
			return err
		}
	}
	if progress.Phase == eventPhaseProcessing {
		page := EventImpactPage{}
		if projectionAffecting || availabilityAffectingEvent(event.EventType) {
			page, err = coordinator.impacts.AffectedTrainRunIDs(
				ctx,
				event,
				progress.AfterTrainRunID,
				MaxRebuildAllBatchSize,
			)
			if err != nil {
				coordinator.recordEvent(event.EventType, "failure")
				return err
			}
		}
		if availabilityAffectingEvent(event.EventType) {
			if err := coordinator.rotateAvailabilityCaches(ctx, event, page.TrainRunIDs); err != nil {
				coordinator.recordEvent(event.EventType, "failure")
				return err
			}
		}
		started := time.Now()
		result, processErr := coordinator.store.ProcessEventPage(
			ctx,
			event,
			projectionAffecting,
			progress.AfterTrainRunID,
			page.TrainRunIDs,
			page.HasMore,
		)
		if processErr != nil {
			coordinator.recordEvent(event.EventType, "failure")
			coordinator.recordRebuild("failure", "database", 0, time.Since(started))
			return processErr
		}
		if projectionAffecting && len(page.TrainRunIDs) > 0 {
			coordinator.recordRebuild("success", "none", result.RowsWritten, time.Since(started))
		}
		if page.HasMore {
			return ErrProjectionPending
		}
		progress.Phase = eventPhaseFinalizing
	}
	if progress.Phase != eventPhaseFinalizing {
		return ErrProjectionPending
	}
	duplicate, err := coordinator.store.CompleteEvent(ctx, event, projectionAffecting)
	if err != nil {
		coordinator.recordEvent(event.EventType, "failure")
		return err
	}
	coordinator.recordEvent(event.EventType, "success")
	if duplicate {
		coordinator.recordDuplicate(event.EventType)
	}
	return nil
}

func (coordinator *EventCoordinator) rotateGlobalCaches(ctx context.Context, event ProjectionEvent) error {
	if stationAffectingEvent(event.EventType) {
		if _, err := coordinator.versions.Rotate(ctx, querycache.StationVersionKey()); err != nil {
			coordinator.recordInvalidation("stations", event.EventType, "failure", "redis")
			return fmt.Errorf("rotate station cache namespace: %w", err)
		}
		coordinator.recordInvalidation("stations", event.EventType, "success", "none")
	}
	if searchAffectingEvent(event.EventType) {
		if _, err := coordinator.versions.Rotate(ctx, querycache.SearchVersionKey()); err != nil {
			coordinator.recordInvalidation("train_search", event.EventType, "failure", "redis")
			return fmt.Errorf("rotate search cache namespace: %w", err)
		}
		coordinator.recordInvalidation("train_search", event.EventType, "success", "none")
	}
	return nil
}

func (coordinator *EventCoordinator) rotateAvailabilityCaches(
	ctx context.Context,
	event ProjectionEvent,
	trainRunIDs []string,
) error {
	for _, trainRunID := range trainRunIDs {
		key, err := querycache.AvailabilityVersionKey(trainRunID)
		if err != nil {
			return err
		}
		if _, err := coordinator.versions.Rotate(ctx, key); err != nil {
			coordinator.recordInvalidation("availability", event.EventType, "failure", "redis")
			return fmt.Errorf("rotate availability cache namespace: %w", err)
		}
		coordinator.recordInvalidation("availability", event.EventType, "success", "none")
	}
	return nil
}

func (coordinator *EventCoordinator) recordEvent(eventType, result string) {
	if coordinator.metrics != nil {
		coordinator.metrics.RecordEvent(eventType, result)
	}
}

func (coordinator *EventCoordinator) recordDuplicate(eventType string) {
	if coordinator.metrics != nil {
		coordinator.metrics.RecordDuplicateEvent(eventType)
	}
}

func (coordinator *EventCoordinator) recordRebuild(result, reason string, rows int64, duration time.Duration) {
	if coordinator.metrics != nil {
		coordinator.metrics.RecordRebuild(result, reason, rows, duration)
	}
}

func (coordinator *EventCoordinator) recordInvalidation(cacheType, eventType, result, reason string) {
	if coordinator.metrics != nil {
		coordinator.metrics.RecordCacheInvalidation(cacheType, eventType, result, reason)
	}
}

type impactQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

type PostgresImpactResolver struct {
	db impactQueryer
}

func NewPostgresImpactResolver(db impactQueryer) (*PostgresImpactResolver, error) {
	if db == nil {
		return nil, ErrInvalidStore
	}
	return &PostgresImpactResolver{db: db}, nil
}

func (resolver *PostgresImpactResolver) AffectedTrainRunIDs(
	ctx context.Context,
	event ProjectionEvent,
	after string,
	limit int,
) (EventImpactPage, error) {
	aggregateID, err := uuid.Parse(event.AggregateID)
	if err != nil || aggregateID == uuid.Nil {
		return EventImpactPage{}, ErrInvalidEvent
	}
	afterID, err := parseEventCursor(after)
	if err != nil || limit < 1 || limit > MaxRebuildAllBatchSize {
		return EventImpactPage{}, ErrInvalidEvent
	}
	if event.AggregateType == "train_run" {
		if afterID != uuid.Nil {
			return EventImpactPage{}, nil
		}
		return EventImpactPage{TrainRunIDs: []string{aggregateID.String()}}, nil
	}
	baseQuery, ok := impactQuery(event.AggregateType)
	if !ok {
		return EventImpactPage{}, nil
	}
	query := `WITH impacted AS (` + baseQuery + `)
		SELECT train_run_id
		FROM impacted
		WHERE train_run_id > $2
		ORDER BY train_run_id
		LIMIT $3`
	rows, err := resolver.db.Query(ctx, query, aggregateID, afterID, limit+1)
	if err != nil {
		return EventImpactPage{}, fmt.Errorf("%w: query event impact", ErrPersistence)
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var trainRunID uuid.UUID
		if err := rows.Scan(&trainRunID); err != nil {
			return EventImpactPage{}, fmt.Errorf("%w: scan event impact", ErrPersistence)
		}
		ids = append(ids, trainRunID.String())
	}
	if err := rows.Err(); err != nil {
		return EventImpactPage{}, fmt.Errorf("%w: iterate event impact", ErrPersistence)
	}
	page := EventImpactPage{HasMore: len(ids) > limit}
	if page.HasMore {
		ids = ids[:limit]
	}
	page.TrainRunIDs = ids
	return page, nil
}

func impactQuery(aggregateType string) (string, bool) {
	switch aggregateType {
	case "station":
		return `SELECT DISTINCT tr.id AS train_run_id FROM route_stops rs JOIN train_runs tr ON tr.route_id = rs.route_id WHERE rs.station_id = $1`, true
	case "route":
		return `SELECT tr.id AS train_run_id FROM train_runs tr WHERE tr.route_id = $1`, true
	case "train":
		return `SELECT tr.id AS train_run_id FROM train_runs tr WHERE tr.train_id = $1`, true
	case "fare":
		return `SELECT DISTINCT tr.id AS train_run_id FROM fares f JOIN train_runs tr ON tr.id = f.train_run_id OR (f.train_run_id IS NULL AND tr.route_id = f.route_id) WHERE f.id = $1`, true
	case "coach":
		return `SELECT tr.id AS train_run_id FROM coaches c JOIN train_runs tr ON tr.train_id = c.train_id WHERE c.id = $1`, true
	case "seat":
		return `SELECT tr.id AS train_run_id FROM seats s JOIN coaches c ON c.id = s.coach_id JOIN train_runs tr ON tr.train_id = c.train_id WHERE s.id = $1`, true
	case "reservation":
		return `SELECT tr.id AS train_run_id FROM reservations r JOIN train_runs tr ON tr.id = r.train_run_id WHERE r.id = $1`, true
	case "ticket":
		return `SELECT tr.id AS train_run_id FROM tickets t JOIN reservation_seats rs ON rs.id = t.reservation_seat_id JOIN reservations r ON r.id = rs.reservation_id JOIN train_runs tr ON tr.id = r.train_run_id WHERE t.id = $1`, true
	default:
		return "", false
	}
}

func validEventPair(eventType, aggregateType string) bool {
	expected, exists := map[string]string{
		"reservation.held": "reservation", "reservation.confirmed": "reservation",
		"reservation.expired": "reservation", "reservation.cancelled": "reservation",
		"ticket.created":   "ticket",
		"trainrun.created": "train_run", "trainrun.updated": "train_run", "trainrun.cancelled": "train_run",
		"hot_train_policy.created": "hot_train_policy", "hot_train_policy.updated": "hot_train_policy",
		"hot_train_policy.disabled": "hot_train_policy",
		"station.created":           "station", "station.updated": "station", "station.disabled": "station",
		"route.created": "route", "route.updated": "route", "route.disabled": "route",
		"train.updated": "train", "coach.updated": "coach", "seat.updated": "seat",
		"fare.created": "fare", "fare.updated": "fare", "fare.disabled": "fare",
	}[eventType]
	return exists && expected == aggregateType
}

func projectionAffectingEvent(eventType string) bool {
	return strings.HasPrefix(eventType, "trainrun.") || strings.HasPrefix(eventType, "station.") ||
		strings.HasPrefix(eventType, "route.") || strings.HasPrefix(eventType, "train.") ||
		strings.HasPrefix(eventType, "fare.")
}

func stationAffectingEvent(eventType string) bool { return strings.HasPrefix(eventType, "station.") }

func searchAffectingEvent(eventType string) bool {
	return projectionAffectingEvent(eventType)
}

func availabilityAffectingEvent(eventType string) bool {
	return strings.HasPrefix(eventType, "reservation.") || eventType == "ticket.created" ||
		strings.HasPrefix(eventType, "trainrun.") || eventType == "coach.updated" || eventType == "seat.updated"
}

var _ EventHandler = (*EventCoordinator)(nil)
