package readmodel

import (
	"context"
	"fmt"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/jackc/pgx/v5"
)

func (s *Store) SearchTrainRuns(
	ctx context.Context,
	request querypostgres.SearchRequest,
) ([]querypostgres.SearchResult, error) {
	normalized, err := querypostgres.NormalizeSearch(request)
	if err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("%w: begin projection search", ErrPersistence)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var projectionAvailable bool
	if err := tx.QueryRow(ctx, `
		SELECT state.ready AND NOT EXISTS (
			SELECT 1 FROM read_model_event_progress WHERE projection_affecting
		)
		FROM read_model_projection_state AS state
		WHERE state.projection_name = 'journey_search'
	`).Scan(&projectionAvailable); err != nil {
		return nil, fmt.Errorf("%w: inspect projection progress", ErrPersistence)
	}
	if !projectionAvailable {
		return nil, ErrProjectionUnavailable
	}
	rows, err := tx.Query(ctx, `
		SELECT
			train_run_id::text,
			train_id::text,
			train_code,
			route_id::text,
			service_date,
			train_run_status,
			from_stop_index,
			to_stop_index,
			departure_at,
			arrival_at,
			fare_amount_minor,
			currency
		FROM train_run_journey_read_model
		WHERE from_station_code = $1
		  AND to_station_code = $2
		  AND service_date = $3
		  AND seat_class = $4
		  AND train_run_status = 'scheduled'
		ORDER BY `+projectionSearchOrder(normalized.Sort)+`
		LIMIT $5 OFFSET $6
	`,
		normalized.OriginCode.String(),
		normalized.DestinationCode.String(),
		normalized.ServiceDate.Format(time.DateOnly),
		normalized.SeatClass.String(),
		normalized.PageSize,
		normalized.Offset,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: query journey projection", ErrPersistence)
	}
	defer rows.Close()
	results := make([]querypostgres.SearchResult, 0)
	for rows.Next() {
		var result querypostgres.SearchResult
		var status string
		if err := rows.Scan(
			&result.TrainRunID,
			&result.TrainID,
			&result.TrainCode,
			&result.RouteID,
			&result.ServiceDate,
			&status,
			&result.FromStopIndex,
			&result.ToStopIndex,
			&result.DepartureAt,
			&result.ArrivalAt,
			&result.FareAmountMinor,
			&result.Currency,
		); err != nil {
			return nil, fmt.Errorf("%w: scan journey projection", ErrPersistence)
		}
		parsedStatus, err := domain.ParseTrainRunStatus(status)
		if err != nil {
			return nil, ErrProjectionSource
		}
		result.Status = parsedStatus
		result.SeatClass = normalized.SeatClass
		result.ServiceDate = time.Date(
			result.ServiceDate.Year(), result.ServiceDate.Month(), result.ServiceDate.Day(), 0, 0, 0, 0, time.UTC,
		)
		result.DepartureAt = result.DepartureAt.UTC()
		result.ArrivalAt = result.ArrivalAt.UTC()
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate journey projection", ErrPersistence)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("%w: commit projection search", ErrPersistence)
	}
	return results, nil
}

func projectionSearchOrder(sort querypostgres.SortOrder) string {
	switch sort {
	case querypostgres.SortDepartureDesc:
		return "departure_at DESC, train_run_id DESC"
	case querypostgres.SortFareAsc:
		return "fare_amount_minor ASC, departure_at ASC, train_run_id ASC"
	case querypostgres.SortFareDesc:
		return "fare_amount_minor DESC, departure_at ASC, train_run_id ASC"
	default:
		return "departure_at ASC, train_run_id ASC"
	}
}
