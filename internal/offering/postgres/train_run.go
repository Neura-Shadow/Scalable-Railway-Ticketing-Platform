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
	OutboxCreated        bool
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
	var operatingTimezone string
	if err := tx.QueryRow(ctx, `SELECT active FROM trains WHERE id = $1 FOR SHARE`, params.TrainID).Scan(&trainActive); err != nil {
		return TrainRun{}, safeError(err)
	}
	if err := tx.QueryRow(ctx, `SELECT active, operating_timezone FROM routes WHERE id = $1 FOR SHARE`, params.RouteID).Scan(&routeActive, &operatingTimezone); err != nil {
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
	var firstDepartureOffset int
	if err := tx.QueryRow(ctx, `
		SELECT departure_offset_minutes
		FROM route_stops
		WHERE route_id = $1 AND stop_index = 0
		FOR SHARE
	`, params.RouteID).Scan(&firstDepartureOffset); err != nil {
		return TrainRun{}, safeError(err)
	}
	expectedDeparture, err := materializeDeparture(params.ServiceDate, operatingTimezone, firstDepartureOffset)
	if err != nil || !params.ScheduledDepartureAt.Equal(expectedDeparture) {
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

	var lockedID, previousStatus string
	if err := tx.QueryRow(ctx, `
		SELECT id::text, status
		FROM train_runs
		WHERE id = $1
		FOR UPDATE
	`, trainRunID).Scan(&lockedID, &previousStatus); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TrainRun{}, ErrNotFound
		}
		return TrainRun{}, safeError(err)
	}
	current, err := domain.ParseTrainRunStatus(previousStatus)
	if err != nil {
		return TrainRun{}, ErrPersistence
	}
	if err := domain.ValidateTrainRunTransition(current, next); err != nil {
		return TrainRun{}, err
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
	if next == domain.TrainRunStatusCancelled && current != domain.TrainRunStatusCancelled {
		if _, err := tx.Exec(ctx, `
			INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
			VALUES ('train_run', $1::uuid, 'trainrun.cancelled', jsonb_build_object(
				'trainRunId', $1::text,
				'status', 'cancelled'
			))
		`, lockedID); err != nil {
			return TrainRun{}, safeError(err)
		}
		run.OutboxCreated = true
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

func materializeDeparture(serviceDate time.Time, timezone string, offsetMinutes int) (time.Time, error) {
	if serviceDate.IsZero() || offsetMinutes < 0 {
		return time.Time{}, ErrInvalidInput
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, ErrInvalidInput
	}
	naive := time.Date(serviceDate.Year(), serviceDate.Month(), serviceDate.Day(), 0, 0, 0, 0, time.UTC).Add(time.Duration(offsetMinutes) * time.Minute)
	candidate := time.Date(naive.Year(), naive.Month(), naive.Day(), naive.Hour(), naive.Minute(), naive.Second(), 0, location)
	localized := candidate.In(location)
	if localized.Year() != naive.Year() || localized.Month() != naive.Month() || localized.Day() != naive.Day() || localized.Hour() != naive.Hour() || localized.Minute() != naive.Minute() {
		return time.Time{}, ErrInvalidInput
	}
	for delta := -3 * time.Hour; delta <= 3*time.Hour; delta += time.Minute {
		if delta == 0 {
			continue
		}
		alternative := candidate.Add(delta).In(location)
		if alternative.Year() == naive.Year() && alternative.Month() == naive.Month() && alternative.Day() == naive.Day() && alternative.Hour() == naive.Hour() && alternative.Minute() == naive.Minute() {
			return time.Time{}, ErrInvalidInput
		}
	}
	return candidate.UTC(), nil
}
