package readmodel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	querydomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const MaxProjectionRowsPerTrainRun = 100_000

var (
	ErrInvalidStore      = errors.New("read-model store configuration invalid")
	ErrInvalidTrainRunID = errors.New("read-model train-run ID invalid")
	ErrInvalidEvent      = errors.New("read-model projection event invalid")
	ErrProjectionSource  = errors.New("read-model projection source invalid")
	ErrProjectionLimit   = errors.New("read-model projection row limit exceeded")
	ErrPersistence       = errors.New("read-model persistence failure")
)

type transactionStarter interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type timeSource interface {
	Now() time.Time
}

type Store struct {
	db    transactionStarter
	clock timeSource
}

type RebuildResult struct {
	TrainRunID  string
	RowsWritten int64
	Deleted     bool
}

type ProjectionEvent struct {
	ConsumerName  string
	EventID       string
	EventType     string
	AggregateType string
	AggregateID   string
}

type ProcessEventResult struct {
	Duplicate   bool
	RowsWritten int64
	Deleted     bool
}

func NewStore(db transactionStarter, clock timeSource) (*Store, error) {
	if db == nil || clock == nil {
		return nil, ErrInvalidStore
	}
	return &Store{db: db, clock: clock}, nil
}

func (s *Store) RebuildTrainRun(ctx context.Context, rawTrainRunID string) (RebuildResult, error) {
	trainRunID, err := uuid.Parse(rawTrainRunID)
	if err != nil || trainRunID == uuid.Nil {
		return RebuildResult{}, ErrInvalidTrainRunID
	}
	rebuiltAt := s.clock.Now().UTC()
	if rebuiltAt.IsZero() {
		return RebuildResult{}, ErrInvalidStore
	}

	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return RebuildResult{}, fmt.Errorf("%w: begin projection rebuild", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	result, err := rebuildTrainRunTx(ctx, tx, trainRunID, rebuiltAt)
	if err != nil {
		return RebuildResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RebuildResult{}, fmt.Errorf("%w: commit train-run projection", ErrPersistence)
	}
	return result, nil
}

func (s *Store) ProcessTrainRunEvent(ctx context.Context, event ProjectionEvent) (ProcessEventResult, error) {
	eventID, trainRunID, err := validateTrainRunEvent(event)
	if err != nil {
		return ProcessEventResult{}, err
	}
	processedAt := s.clock.Now().UTC()
	if processedAt.IsZero() {
		return ProcessEventResult{}, ErrInvalidStore
	}

	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return ProcessEventResult{}, fmt.Errorf("%w: begin event projection", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	tag, err := tx.Exec(ctx, `
		INSERT INTO read_model_event_receipts (
			consumer_name,
			event_id,
			event_type,
			aggregate_type,
			aggregate_id,
			processed_at
		) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (consumer_name, event_id) DO NOTHING
	`, event.ConsumerName, eventID, event.EventType, event.AggregateType, trainRunID, processedAt)
	if err != nil {
		return ProcessEventResult{}, fmt.Errorf("%w: write event receipt", ErrPersistence)
	}
	if tag.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return ProcessEventResult{}, fmt.Errorf("%w: commit duplicate event", ErrPersistence)
		}
		return ProcessEventResult{Duplicate: true}, nil
	}

	rebuild, err := rebuildTrainRunTx(ctx, tx, trainRunID, processedAt)
	if err != nil {
		return ProcessEventResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ProcessEventResult{}, fmt.Errorf("%w: commit event projection", ErrPersistence)
	}
	return ProcessEventResult{RowsWritten: rebuild.RowsWritten, Deleted: rebuild.Deleted}, nil
}

func validateTrainRunEvent(event ProjectionEvent) (uuid.UUID, uuid.UUID, error) {
	if event.ConsumerName != strings.TrimSpace(event.ConsumerName) ||
		len(event.ConsumerName) == 0 || len(event.ConsumerName) > 128 ||
		event.AggregateType != "train_run" {
		return uuid.Nil, uuid.Nil, ErrInvalidEvent
	}
	switch event.EventType {
	case "trainrun.created", "trainrun.updated", "trainrun.cancelled":
	default:
		return uuid.Nil, uuid.Nil, ErrInvalidEvent
	}
	eventID, err := uuid.Parse(event.EventID)
	if err != nil || eventID == uuid.Nil {
		return uuid.Nil, uuid.Nil, ErrInvalidEvent
	}
	trainRunID, err := uuid.Parse(event.AggregateID)
	if err != nil || trainRunID == uuid.Nil {
		return uuid.Nil, uuid.Nil, ErrInvalidEvent
	}
	return eventID, trainRunID, nil
}

func rebuildTrainRunTx(
	ctx context.Context,
	tx pgx.Tx,
	trainRunID uuid.UUID,
	rebuiltAt time.Time,
) (RebuildResult, error) {

	var lockedID uuid.UUID
	err := tx.QueryRow(ctx, `
		SELECT id
		FROM train_runs
		WHERE id = $1
		FOR SHARE
	`, trainRunID).Scan(&lockedID)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, deleteErr := tx.Exec(ctx, `
			DELETE FROM train_run_journey_read_model
			WHERE train_run_id = $1
		`, trainRunID); deleteErr != nil {
			return RebuildResult{}, fmt.Errorf("%w: delete missing train-run projection", ErrPersistence)
		}
		return RebuildResult{TrainRunID: trainRunID.String(), Deleted: true}, nil
	}
	if err != nil {
		return RebuildResult{}, fmt.Errorf("%w: lock train-run source", ErrPersistence)
	}

	sourceRows, err := loadProjectionRows(ctx, tx, trainRunID, rebuiltAt)
	if err != nil {
		return RebuildResult{}, err
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM train_run_journey_read_model
		WHERE train_run_id = $1
	`, trainRunID); err != nil {
		return RebuildResult{}, fmt.Errorf("%w: replace train-run projection", ErrPersistence)
	}
	if len(sourceRows) > 0 {
		inserted, err := tx.CopyFrom(
			ctx,
			pgx.Identifier{"train_run_journey_read_model"},
			[]string{
				"train_run_id",
				"route_id",
				"train_id",
				"train_code",
				"service_date",
				"train_run_status",
				"from_station_id",
				"from_station_code",
				"from_station_name",
				"from_stop_index",
				"to_station_id",
				"to_station_code",
				"to_station_name",
				"to_stop_index",
				"departure_at",
				"arrival_at",
				"seat_class",
				"fare_amount_minor",
				"currency",
				"source_updated_at",
				"rebuilt_at",
			},
			pgx.CopyFromRows(sourceRows),
		)
		if err != nil || inserted != int64(len(sourceRows)) {
			return RebuildResult{}, fmt.Errorf("%w: copy train-run projection", ErrPersistence)
		}
	}
	return RebuildResult{TrainRunID: trainRunID.String(), RowsWritten: int64(len(sourceRows))}, nil
}

func (s *Store) DeleteTrainRunProjection(ctx context.Context, rawTrainRunID string) (int64, error) {
	trainRunID, err := uuid.Parse(rawTrainRunID)
	if err != nil || trainRunID == uuid.Nil {
		return 0, ErrInvalidTrainRunID
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("%w: begin projection delete", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	tag, err := tx.Exec(ctx, `
		DELETE FROM train_run_journey_read_model
		WHERE train_run_id = $1
	`, trainRunID)
	if err != nil {
		return 0, fmt.Errorf("%w: delete train-run projection", ErrPersistence)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("%w: commit projection delete", ErrPersistence)
	}
	return tag.RowsAffected(), nil
}

func loadProjectionRows(
	ctx context.Context,
	tx pgx.Tx,
	trainRunID uuid.UUID,
	rebuiltAt time.Time,
) ([][]any, error) {
	rows, err := tx.Query(ctx, `
		SELECT
			tr.id,
			tr.route_id,
			tr.train_id,
			t.code,
			tr.service_date,
			tr.status,
			origin_station.id,
			origin_station.code,
			origin_station.name,
			origin.stop_index,
			destination_station.id,
			destination_station.code,
			destination_station.name,
			destination.stop_index,
			tr.scheduled_departure_at,
			route_origin.departure_offset_minutes,
			origin.departure_offset_minutes,
			destination.arrival_offset_minutes,
			selected_fare.seat_class,
			selected_fare.amount_minor,
			selected_fare.currency,
			GREATEST(
				tr.updated_at,
				t.updated_at,
				r.updated_at,
				origin_station.updated_at,
				destination_station.updated_at,
				selected_fare.updated_at
			) AS source_updated_at
		FROM train_runs AS tr
		JOIN trains AS t
		  ON t.id = tr.train_id
		 AND t.active
		JOIN routes AS r
		  ON r.id = tr.route_id
		 AND r.active
		JOIN route_stops AS route_origin
		  ON route_origin.route_id = tr.route_id
		 AND route_origin.stop_index = 0
		JOIN route_stops AS origin
		  ON origin.route_id = tr.route_id
		JOIN stations AS origin_station
		  ON origin_station.id = origin.station_id
		 AND origin_station.active
		JOIN route_stops AS destination
		  ON destination.route_id = tr.route_id
		 AND origin.stop_index < destination.stop_index
		JOIN stations AS destination_station
		  ON destination_station.id = destination.station_id
		 AND destination_station.active
		JOIN LATERAL (
			SELECT DISTINCT ON (fare.seat_class)
				fare.seat_class,
				fare.amount_minor,
				fare.currency,
				fare.updated_at
			FROM fares AS fare
			WHERE fare.active
			  AND fare.from_stop_index = origin.stop_index
			  AND fare.to_stop_index = destination.stop_index
			  AND (
				fare.train_run_id = tr.id
				OR (fare.train_run_id IS NULL AND fare.route_id = tr.route_id)
			  )
			ORDER BY
				fare.seat_class,
				(fare.train_run_id IS NOT NULL) DESC,
				fare.updated_at DESC,
				fare.id
		) AS selected_fare ON true
		WHERE tr.id = $1
		ORDER BY
			origin.stop_index,
			destination.stop_index,
			selected_fare.seat_class
	`, trainRunID)
	if err != nil {
		return nil, fmt.Errorf("%w: query projection source", ErrPersistence)
	}
	defer rows.Close()

	projectionRows := make([][]any, 0)
	for rows.Next() {
		var (
			rowTrainRunID                   uuid.UUID
			routeID                         uuid.UUID
			trainID                         uuid.UUID
			trainCode                       string
			serviceDate                     time.Time
			status                          string
			fromStationID                   uuid.UUID
			fromStationCode                 string
			fromStationName                 string
			fromStopIndex                   int
			toStationID                     uuid.UUID
			toStationCode                   string
			toStationName                   string
			toStopIndex                     int
			scheduledDepartureAt            time.Time
			firstDepartureOffsetMinutes     int
			originDepartureOffsetMinutes    int
			destinationArrivalOffsetMinutes int
			seatClass                       string
			fareAmountMinor                 int64
			currency                        string
			sourceUpdatedAt                 time.Time
		)
		if err := rows.Scan(
			&rowTrainRunID,
			&routeID,
			&trainID,
			&trainCode,
			&serviceDate,
			&status,
			&fromStationID,
			&fromStationCode,
			&fromStationName,
			&fromStopIndex,
			&toStationID,
			&toStationCode,
			&toStationName,
			&toStopIndex,
			&scheduledDepartureAt,
			&firstDepartureOffsetMinutes,
			&originDepartureOffsetMinutes,
			&destinationArrivalOffsetMinutes,
			&seatClass,
			&fareAmountMinor,
			&currency,
			&sourceUpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("%w: scan projection source", ErrPersistence)
		}
		departureAt, arrivalAt, err := querydomain.AnchorJourneyTimes(
			scheduledDepartureAt,
			firstDepartureOffsetMinutes,
			originDepartureOffsetMinutes,
			destinationArrivalOffsetMinutes,
		)
		if err != nil {
			return nil, ErrProjectionSource
		}
		if len(projectionRows) >= MaxProjectionRowsPerTrainRun {
			return nil, ErrProjectionLimit
		}
		projectionRows = append(projectionRows, []any{
			rowTrainRunID,
			routeID,
			trainID,
			trainCode,
			serviceDate,
			status,
			fromStationID,
			fromStationCode,
			fromStationName,
			fromStopIndex,
			toStationID,
			toStationCode,
			toStationName,
			toStopIndex,
			departureAt,
			arrivalAt,
			seatClass,
			fareAmountMinor,
			currency,
			sourceUpdatedAt.UTC(),
			rebuiltAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate projection source", ErrPersistence)
	}
	return projectionRows, nil
}
