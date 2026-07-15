package postgres

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/jackc/pgx/v5"
)

type TrainRun struct {
	ID                   string
	TrainID              string
	RouteID              string
	ServiceDate          time.Time
	ScheduledDepartureAt time.Time
	Status               domain.TrainRunStatus
	SegmentCount         int
	InventoryRows        int64
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

type CommissionTrainRunParams struct {
	TrainID              string
	RouteID              string
	ServiceDate          time.Time
	ScheduledDepartureAt time.Time
}

func (s *Store) CommissionTrainRun(ctx context.Context, params CommissionTrainRunParams) (TrainRun, error) {
	if strings.TrimSpace(params.TrainID) == "" || strings.TrimSpace(params.RouteID) == "" || params.ServiceDate.IsZero() || params.ScheduledDepartureAt.IsZero() {
		return TrainRun{}, ErrInvalidInput
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return TrainRun{}, safeError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var trainActive, routeActive bool
	if err := tx.QueryRow(ctx, `SELECT active FROM trains WHERE id = $1 FOR SHARE`, params.TrainID).Scan(&trainActive); err != nil {
		return TrainRun{}, safeError(err)
	}
	if err := tx.QueryRow(ctx, `SELECT active FROM routes WHERE id = $1 FOR SHARE`, params.RouteID).Scan(&routeActive); err != nil {
		return TrainRun{}, safeError(err)
	}
	if !trainActive || !routeActive {
		return TrainRun{}, ErrInvalidInput
	}

	var segmentCount int
	if err := tx.QueryRow(ctx, `
		SELECT count(*)::integer - 1
		FROM (
			SELECT stop_index
			FROM route_stops
			WHERE route_id = $1
			ORDER BY stop_index
			FOR SHARE
		) AS locked_stops
	`, params.RouteID).Scan(&segmentCount); err != nil {
		return TrainRun{}, safeError(err)
	}
	if segmentCount <= 0 {
		return TrainRun{}, ErrInvalidInput
	}

	var run TrainRun
	var status string
	err = tx.QueryRow(ctx, `
		INSERT INTO train_runs (
			train_id, route_id, service_date, scheduled_departure_at, status, segment_count
		)
		VALUES ($1, $2, $3, $4, 'scheduled', $5)
		RETURNING id::text, train_id::text, route_id::text, service_date,
		          scheduled_departure_at, status, segment_count, created_at, updated_at
	`, params.TrainID, params.RouteID, params.ServiceDate.Format(time.DateOnly), params.ScheduledDepartureAt.UTC(), segmentCount).Scan(
		&run.ID, &run.TrainID, &run.RouteID, &run.ServiceDate, &run.ScheduledDepartureAt,
		&status, &run.SegmentCount, &run.CreatedAt, &run.UpdatedAt,
	)
	if err != nil {
		return TrainRun{}, safeError(err)
	}

	result, err := tx.Exec(ctx, `
		INSERT INTO seat_inventory (
			train_run_id, segment_count, seat_id, seat_class, occupied_segments
		)
		SELECT $1, $2, s.id, c.seat_class, repeat('0', $2)::bit varying
		FROM seats AS s
		JOIN coaches AS c ON c.id = s.coach_id
		WHERE c.train_id = $3 AND s.active
		ORDER BY c.coach_number, s.seat_number, s.id
	`, run.ID, segmentCount, params.TrainID)
	if err != nil {
		return TrainRun{}, safeError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return TrainRun{}, safeError(err)
	}

	parsedStatus, err := domain.ParseTrainRunStatus(status)
	if err != nil {
		return TrainRun{}, ErrPersistence
	}
	run.Status = parsedStatus
	run.InventoryRows = result.RowsAffected()
	normalizeTrainRunTimes(&run)
	return run, nil
}

func (s *Store) UpdateTrainRunStatus(ctx context.Context, trainRunID string, next domain.TrainRunStatus) (TrainRun, error) {
	if strings.TrimSpace(trainRunID) == "" {
		return TrainRun{}, ErrNotFound
	}
	if !next.IsValid() {
		return TrainRun{}, domain.ErrInvalidTrainRunStatus
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return TrainRun{}, safeError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var lockedID string
	if err := tx.QueryRow(ctx, `
		SELECT id::text
		FROM train_runs
		WHERE id = $1
		FOR UPDATE
	`, trainRunID).Scan(&lockedID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TrainRun{}, ErrNotFound
		}
		return TrainRun{}, safeError(err)
	}

	var run TrainRun
	var status string
	err = tx.QueryRow(ctx, `
		UPDATE train_runs
		SET status = $2
		WHERE id = $1
		RETURNING id::text, train_id::text, route_id::text, service_date,
		          scheduled_departure_at, status, segment_count, created_at, updated_at
	`, lockedID, next.String()).Scan(
		&run.ID, &run.TrainID, &run.RouteID, &run.ServiceDate, &run.ScheduledDepartureAt,
		&status, &run.SegmentCount, &run.CreatedAt, &run.UpdatedAt,
	)
	if err != nil {
		return TrainRun{}, safeError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return TrainRun{}, safeError(err)
	}
	parsedStatus, err := domain.ParseTrainRunStatus(status)
	if err != nil {
		return TrainRun{}, ErrPersistence
	}
	run.Status = parsedStatus
	normalizeTrainRunTimes(&run)
	return run, nil
}

func normalizeTrainRunTimes(run *TrainRun) {
	run.ServiceDate = time.Date(run.ServiceDate.Year(), run.ServiceDate.Month(), run.ServiceDate.Day(), 0, 0, 0, 0, time.UTC)
	run.ScheduledDepartureAt = run.ScheduledDepartureAt.UTC()
	run.CreatedAt = run.CreatedAt.UTC()
	run.UpdatedAt = run.UpdatedAt.UTC()
}
