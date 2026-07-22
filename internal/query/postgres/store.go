package postgres

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	querydomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type DBTX interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Store struct {
	db DBTX
}

func NewStore(db DBTX) (*Store, error) {
	if db == nil {
		return nil, ErrInvalidQuery
	}
	return &Store{db: db}, nil
}

type Station struct {
	ID        string
	Code      domain.StationCode
	Name      string
	Timezone  string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (s *Store) ListStations(ctx context.Context) ([]Station, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id::text, code, name, timezone, created_at, updated_at
		FROM stations
		WHERE active
		ORDER BY code, id
	`)
	if err != nil {
		return nil, safeQueryError(err)
	}
	defer rows.Close()

	stations := make([]Station, 0)
	for rows.Next() {
		var station Station
		var code string
		if err := rows.Scan(&station.ID, &code, &station.Name, &station.Timezone, &station.CreatedAt, &station.UpdatedAt); err != nil {
			return nil, safeQueryError(err)
		}
		parsedCode, err := domain.NewStationCode(code)
		if err != nil {
			return nil, ErrPersistence
		}
		station.Code = parsedCode
		station.CreatedAt = station.CreatedAt.UTC()
		station.UpdatedAt = station.UpdatedAt.UTC()
		stations = append(stations, station)
	}
	if err := rows.Err(); err != nil {
		return nil, safeQueryError(err)
	}
	return stations, nil
}

type SearchResult struct {
	TrainRunID                      string
	TrainID                         string
	TrainCode                       string
	TrainName                       string
	RouteID                         string
	RouteCode                       string
	RouteName                       string
	ServiceDate                     time.Time
	ScheduledDepartureAt            time.Time
	Status                          domain.TrainRunStatus
	SegmentCount                    int
	FromStopIndex                   int
	ToStopIndex                     int
	OriginDepartureOffsetMinutes    int
	DestinationArrivalOffsetMinutes int
	DepartureAt                     time.Time
	ArrivalAt                       time.Time
	SeatClass                       domain.SeatClass
	FareAmountMinor                 int64
	Currency                        string
}

const anchoredDepartureOrderSQL = "tr.scheduled_departure_at + make_interval(mins => origin.departure_offset_minutes - route_origin.departure_offset_minutes)"

func (s *Store) SearchTrainRuns(ctx context.Context, request SearchRequest) ([]SearchResult, error) {
	normalized, err := NormalizeSearch(request)
	if err != nil {
		return nil, err
	}
	orderBy := searchOrderBy(normalized.Sort)
	rows, err := s.db.Query(ctx, `
		SELECT tr.id::text, tr.train_id::text, t.code, t.name,
		       tr.route_id::text, r.code, r.name, tr.service_date,
		       tr.scheduled_departure_at, tr.status, tr.segment_count,
		       origin.stop_index, destination.stop_index,
		       route_origin.departure_offset_minutes,
		       origin.departure_offset_minutes, destination.arrival_offset_minutes,
		       selected_fare.amount_minor, selected_fare.currency
		FROM train_runs AS tr
		JOIN trains AS t ON t.id = tr.train_id AND t.active
		JOIN routes AS r ON r.id = tr.route_id AND r.active
		JOIN route_stops AS route_origin ON route_origin.route_id = tr.route_id AND route_origin.stop_index = 0
		JOIN route_stops AS origin ON origin.route_id = tr.route_id
		JOIN stations AS origin_station ON origin_station.id = origin.station_id AND origin_station.code = $1 AND origin_station.active
		JOIN route_stops AS destination ON destination.route_id = tr.route_id
		JOIN stations AS destination_station ON destination_station.id = destination.station_id AND destination_station.code = $2 AND destination_station.active
		JOIN LATERAL (
			SELECT f.amount_minor, f.currency
			FROM fares AS f
			WHERE f.active
			  AND f.seat_class = $4
			  AND f.from_stop_index = origin.stop_index
			  AND f.to_stop_index = destination.stop_index
			  AND (f.train_run_id = tr.id OR (f.train_run_id IS NULL AND f.route_id = tr.route_id))
			ORDER BY (f.train_run_id IS NOT NULL) DESC
			LIMIT 1
		) AS selected_fare ON true
		WHERE tr.service_date = $3
		  AND tr.status = 'scheduled'
		  AND origin.stop_index < destination.stop_index
		ORDER BY `+orderBy+`
		LIMIT $5 OFFSET $6
	`, normalized.OriginCode.String(), normalized.DestinationCode.String(), normalized.ServiceDate.Format(time.DateOnly), normalized.SeatClass.String(), normalized.PageSize, normalized.Offset)
	if err != nil {
		return nil, safeQueryError(err)
	}
	defer rows.Close()

	results := make([]SearchResult, 0)
	for rows.Next() {
		var result SearchResult
		var status string
		var firstDepartureOffsetMinutes int
		if err := rows.Scan(
			&result.TrainRunID, &result.TrainID, &result.TrainCode, &result.TrainName,
			&result.RouteID, &result.RouteCode, &result.RouteName, &result.ServiceDate,
			&result.ScheduledDepartureAt, &status, &result.SegmentCount,
			&result.FromStopIndex, &result.ToStopIndex,
			&firstDepartureOffsetMinutes,
			&result.OriginDepartureOffsetMinutes, &result.DestinationArrivalOffsetMinutes,
			&result.FareAmountMinor, &result.Currency,
		); err != nil {
			return nil, safeQueryError(err)
		}
		parsedStatus, err := domain.ParseTrainRunStatus(status)
		if err != nil {
			return nil, ErrPersistence
		}
		result.Status = parsedStatus
		result.SeatClass = normalized.SeatClass
		result.ServiceDate = dateUTC(result.ServiceDate)
		result.ScheduledDepartureAt = result.ScheduledDepartureAt.UTC()
		result.DepartureAt, result.ArrivalAt, err = querydomain.AnchorJourneyTimes(
			result.ScheduledDepartureAt,
			firstDepartureOffsetMinutes,
			result.OriginDepartureOffsetMinutes,
			result.DestinationArrivalOffsetMinutes,
		)
		if err != nil {
			return nil, ErrPersistence
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, safeQueryError(err)
	}
	return results, nil
}

func searchOrderBy(sortOrder SortOrder) string {
	switch sortOrder {
	case SortDepartureDesc:
		return anchoredDepartureOrderSQL + " DESC, tr.id DESC"
	case SortFareAsc:
		return "selected_fare.amount_minor ASC, " + anchoredDepartureOrderSQL + " ASC, tr.id ASC"
	case SortFareDesc:
		return "selected_fare.amount_minor DESC, " + anchoredDepartureOrderSQL + " ASC, tr.id ASC"
	default:
		return anchoredDepartureOrderSQL + " ASC, tr.id ASC"
	}
}

type AvailabilityRequest struct {
	TrainRunID      string
	OriginCode      string
	DestinationCode string
	SeatClass       string
}

type Availability struct {
	TrainRunID                      string
	TrainCode                       string
	ScheduledDepartureAt            time.Time
	FromStopIndex                   int
	ToStopIndex                     int
	SegmentCount                    int
	OriginDepartureOffsetMinutes    int
	DestinationArrivalOffsetMinutes int
	DepartureAt                     time.Time
	ArrivalAt                       time.Time
	SeatClass                       domain.SeatClass
	AvailableSeats                  int64
	FareAmountMinor                 int64
	Currency                        string
}

func (s *Store) Availability(ctx context.Context, request AvailabilityRequest) (Availability, error) {
	if strings.TrimSpace(request.TrainRunID) == "" {
		return Availability{}, ErrInvalidQuery
	}
	origin, err := domain.NewStationCode(request.OriginCode)
	if err != nil {
		return Availability{}, ErrInvalidJourney
	}
	destination, err := domain.NewStationCode(request.DestinationCode)
	if err != nil || origin == destination {
		return Availability{}, ErrInvalidJourney
	}
	seatClass, err := domain.ParseSeatClass(request.SeatClass)
	if err != nil {
		return Availability{}, err
	}

	var result Availability
	var firstDepartureOffsetMinutes int
	err = s.db.QueryRow(ctx, `
		WITH journey AS (
			SELECT tr.id, tr.route_id, tr.segment_count, t.code AS train_code,
			       tr.scheduled_departure_at,
			       origin.stop_index AS from_stop_index,
			       destination.stop_index AS to_stop_index,
			       route_origin.departure_offset_minutes AS first_departure_offset_minutes,
			       origin.departure_offset_minutes,
			       destination.arrival_offset_minutes,
			       repeat('0', origin.stop_index)::bit varying
			       || repeat('1', destination.stop_index - origin.stop_index)::bit varying
			       || repeat('0', tr.segment_count - destination.stop_index)::bit varying AS requested_mask
			FROM train_runs AS tr
			JOIN trains AS t ON t.id = tr.train_id AND t.active
			JOIN routes AS r ON r.id = tr.route_id AND r.active
			JOIN route_stops AS route_origin ON route_origin.route_id = tr.route_id AND route_origin.stop_index = 0
			JOIN route_stops AS origin ON origin.route_id = tr.route_id
			JOIN stations AS origin_station ON origin_station.id = origin.station_id AND origin_station.code = $2 AND origin_station.active
			JOIN route_stops AS destination ON destination.route_id = tr.route_id
			JOIN stations AS destination_station ON destination_station.id = destination.station_id AND destination_station.code = $3 AND destination_station.active
			WHERE tr.id = $1
			  AND tr.status = 'scheduled'
			  AND origin.stop_index < destination.stop_index
		), priced AS (
			SELECT journey.*, selected_fare.amount_minor, selected_fare.currency
			FROM journey
			JOIN LATERAL (
				SELECT f.amount_minor, f.currency
				FROM fares AS f
				WHERE f.active
				  AND f.seat_class = $4
				  AND f.from_stop_index = journey.from_stop_index
				  AND f.to_stop_index = journey.to_stop_index
				  AND (f.train_run_id = journey.id OR (f.train_run_id IS NULL AND f.route_id = journey.route_id))
				ORDER BY (f.train_run_id IS NOT NULL) DESC
				LIMIT 1
			) AS selected_fare ON true
		)
		SELECT priced.id::text, priced.train_code, priced.scheduled_departure_at,
		       priced.from_stop_index, priced.to_stop_index, priced.segment_count,
		       priced.first_departure_offset_minutes,
		       priced.departure_offset_minutes, priced.arrival_offset_minutes,
		       priced.amount_minor, priced.currency,
		       count(si.seat_id) FILTER (
				WHERE CASE
					WHEN si.seat_id IS NULL THEN false
					WHEN bit_length(si.occupied_segments) <> priced.segment_count THEN false
					ELSE (si.occupied_segments & priced.requested_mask) = repeat('0', priced.segment_count)::bit varying
				END
		       )::bigint AS available_seats
		FROM priced
		LEFT JOIN seat_inventory AS si
		  ON si.train_run_id = priced.id
		 AND si.seat_class = $4
		GROUP BY priced.id, priced.train_code, priced.scheduled_departure_at,
		         priced.from_stop_index, priced.to_stop_index, priced.segment_count,
		         priced.first_departure_offset_minutes,
		         priced.departure_offset_minutes, priced.arrival_offset_minutes,
		         priced.amount_minor, priced.currency
	`, request.TrainRunID, origin.String(), destination.String(), seatClass.String()).Scan(
		&result.TrainRunID, &result.TrainCode, &result.ScheduledDepartureAt,
		&result.FromStopIndex, &result.ToStopIndex, &result.SegmentCount,
		&firstDepartureOffsetMinutes,
		&result.OriginDepartureOffsetMinutes, &result.DestinationArrivalOffsetMinutes,
		&result.FareAmountMinor, &result.Currency, &result.AvailableSeats,
	)
	if err != nil {
		return Availability{}, safeQueryError(err)
	}
	result.SeatClass = seatClass
	result.ScheduledDepartureAt = result.ScheduledDepartureAt.UTC()
	result.DepartureAt, result.ArrivalAt, err = querydomain.AnchorJourneyTimes(
		result.ScheduledDepartureAt,
		firstDepartureOffsetMinutes,
		result.OriginDepartureOffsetMinutes,
		result.DestinationArrivalOffsetMinutes,
	)
	if err != nil {
		return Availability{}, ErrPersistence
	}
	return result, nil
}

func dateUTC(value time.Time) time.Time {
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}

func safeQueryError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		switch pgError.Code {
		case "23514", "22001", "22P02":
			return ErrInvalidQuery
		}
	}
	return ErrPersistence
}
