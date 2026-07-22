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
	AffectedTrainRunIDs(context.Context, ProjectionEvent) ([]string, error)
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
	trainRunIDs := []string(nil)
	if projectionAffectingEvent(event.EventType) || availabilityAffectingEvent(event.EventType) {
		var err error
		trainRunIDs, err = coordinator.impacts.AffectedTrainRunIDs(ctx, event)
		if err != nil {
			return err
		}
	}
	projectionIDs := trainRunIDs
	if !projectionAffectingEvent(event.EventType) {
		projectionIDs = nil
	}
	started := time.Now()
	result, err := coordinator.store.ProcessEvent(ctx, event, projectionIDs)
	if err != nil {
		coordinator.recordEvent(event.EventType, "failure")
		coordinator.recordRebuild("failure", "database", 0, time.Since(started))
		return err
	}
	coordinator.recordEvent(event.EventType, "success")
	if result.Duplicate {
		coordinator.recordDuplicate(event.EventType)
	} else if len(projectionIDs) > 0 {
		coordinator.recordRebuild("success", "none", result.RowsWritten, time.Since(started))
	}
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
	if availabilityAffectingEvent(event.EventType) {
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
) ([]string, error) {
	aggregateID, err := uuid.Parse(event.AggregateID)
	if err != nil || aggregateID == uuid.Nil {
		return nil, ErrInvalidEvent
	}
	if event.AggregateType == "train_run" {
		return []string{aggregateID.String()}, nil
	}
	query, ok := impactQuery(event.AggregateType)
	if !ok {
		return nil, nil
	}
	rows, err := resolver.db.Query(ctx, query, aggregateID, MaxRebuildAllBatchSize+1)
	if err != nil {
		return nil, fmt.Errorf("%w: query event impact", ErrPersistence)
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var trainRunID uuid.UUID
		if err := rows.Scan(&trainRunID); err != nil {
			return nil, fmt.Errorf("%w: scan event impact", ErrPersistence)
		}
		ids = append(ids, trainRunID.String())
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate event impact", ErrPersistence)
	}
	if len(ids) > MaxRebuildAllBatchSize {
		return nil, ErrProjectionLimit
	}
	return ids, nil
}

func impactQuery(aggregateType string) (string, bool) {
	const suffix = ` ORDER BY tr.id LIMIT $2`
	switch aggregateType {
	case "station":
		return `SELECT DISTINCT tr.id FROM route_stops rs JOIN train_runs tr ON tr.route_id = rs.route_id WHERE rs.station_id = $1` + suffix, true
	case "route":
		return `SELECT tr.id FROM train_runs tr WHERE tr.route_id = $1` + suffix, true
	case "train":
		return `SELECT tr.id FROM train_runs tr WHERE tr.train_id = $1` + suffix, true
	case "fare":
		return `SELECT DISTINCT tr.id FROM fares f JOIN train_runs tr ON tr.id = f.train_run_id OR (f.train_run_id IS NULL AND tr.route_id = f.route_id) WHERE f.id = $1` + suffix, true
	case "coach":
		return `SELECT tr.id FROM coaches c JOIN train_runs tr ON tr.train_id = c.train_id WHERE c.id = $1` + suffix, true
	case "seat":
		return `SELECT tr.id FROM seats s JOIN coaches c ON c.id = s.coach_id JOIN train_runs tr ON tr.train_id = c.train_id WHERE s.id = $1` + suffix, true
	case "reservation":
		return `SELECT tr.id FROM reservations r JOIN train_runs tr ON tr.id = r.train_run_id WHERE r.id = $1` + suffix, true
	case "ticket":
		return `SELECT tr.id FROM tickets t JOIN reservation_seats rs ON rs.id = t.reservation_seat_id JOIN reservations r ON r.id = rs.reservation_id JOIN train_runs tr ON tr.id = r.train_run_id WHERE t.id = $1` + suffix, true
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
