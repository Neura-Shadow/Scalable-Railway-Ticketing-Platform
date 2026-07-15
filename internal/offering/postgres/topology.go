package postgres

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/jackc/pgx/v5"
)

var entityCodePattern = regexp.MustCompile(`^[A-Z0-9][A-Z0-9_-]{1,31}$`)

type RouteStop struct {
	StationID              string
	StationCode            domain.StationCode
	StopIndex              int
	ArrivalOffsetMinutes   int
	DepartureOffsetMinutes int
}

type Route struct {
	ID                string
	Code              string
	Name              string
	OperatingTimezone string
	Active            bool
	SegmentCount      int
	Stops             []RouteStop
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type CreateRouteParams struct {
	Route             domain.Route
	OperatingTimezone string
}

func (s *Store) CreateRoute(ctx context.Context, params CreateRouteParams) (Route, error) {
	timezone := strings.TrimSpace(params.OperatingTimezone)
	if params.Route.SegmentCount() <= 0 || !entityCodePattern.MatchString(params.Route.Code()) || runeLengthOutside(params.Route.Name(), 1, 120) || runeLengthOutside(timezone, 1, 64) {
		return Route{}, ErrInvalidInput
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return Route{}, ErrInvalidInput
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Route{}, safeError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var route Route
	err = tx.QueryRow(ctx, `
		INSERT INTO routes (code, name, operating_timezone)
		VALUES ($1, $2, $3)
		RETURNING id::text, code, name, operating_timezone, active, created_at, updated_at
	`, params.Route.Code(), params.Route.Name(), timezone).Scan(
		&route.ID, &route.Code, &route.Name, &route.OperatingTimezone, &route.Active, &route.CreatedAt, &route.UpdatedAt,
	)
	if err != nil {
		return Route{}, safeError(err)
	}

	domainStops := params.Route.Stops()
	route.Stops = make([]RouteStop, 0, len(domainStops))
	for _, stop := range domainStops {
		var stationID string
		err := tx.QueryRow(ctx, `
			INSERT INTO route_stops (
				route_id, station_id, stop_index, arrival_offset_minutes, departure_offset_minutes
			)
			SELECT $1, id, $3, $4, $5
			FROM stations
			WHERE code = $2 AND active
			RETURNING station_id::text
		`, route.ID, stop.StationCode().String(), stop.StopIndex(), stop.ArrivalOffsetMinutes(), stop.DepartureOffsetMinutes()).Scan(&stationID)
		if errors.Is(err, pgx.ErrNoRows) {
			return Route{}, ErrNotFound
		}
		if err != nil {
			return Route{}, safeError(err)
		}
		route.Stops = append(route.Stops, RouteStop{
			StationID: stationID, StationCode: stop.StationCode(), StopIndex: stop.StopIndex(),
			ArrivalOffsetMinutes: stop.ArrivalOffsetMinutes(), DepartureOffsetMinutes: stop.DepartureOffsetMinutes(),
		})
	}
	if err := tx.Commit(ctx); err != nil {
		return Route{}, safeError(err)
	}
	route.SegmentCount = len(route.Stops) - 1
	route.CreatedAt = route.CreatedAt.UTC()
	route.UpdatedAt = route.UpdatedAt.UTC()
	return route, nil
}

type Train struct {
	ID        string
	Code      string
	Name      string
	Active    bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

type CreateTrainParams struct {
	Code string
	Name string
}

func (s *Store) CreateTrain(ctx context.Context, params CreateTrainParams) (Train, error) {
	code := strings.ToUpper(strings.TrimSpace(params.Code))
	name := strings.TrimSpace(params.Name)
	if !entityCodePattern.MatchString(code) || runeLengthOutside(name, 1, 120) {
		return Train{}, ErrInvalidInput
	}
	var train Train
	err := s.db.QueryRow(ctx, `
		INSERT INTO trains (code, name)
		VALUES ($1, $2)
		RETURNING id::text, code, name, active, created_at, updated_at
	`, code, name).Scan(&train.ID, &train.Code, &train.Name, &train.Active, &train.CreatedAt, &train.UpdatedAt)
	if err != nil {
		return Train{}, safeError(err)
	}
	train.CreatedAt = train.CreatedAt.UTC()
	train.UpdatedAt = train.UpdatedAt.UTC()
	return train, nil
}

type Coach struct {
	ID          string
	TrainID     string
	CoachNumber string
	SeatClass   domain.SeatClass
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type CreateCoachParams struct {
	TrainID     string
	CoachNumber string
	SeatClass   domain.SeatClass
}

func (s *Store) CreateCoach(ctx context.Context, params CreateCoachParams) (Coach, error) {
	number := strings.TrimSpace(params.CoachNumber)
	if params.TrainID == "" || runeLengthOutside(number, 1, 16) || !params.SeatClass.IsValid() {
		return Coach{}, ErrInvalidInput
	}
	var coach Coach
	var seatClass string
	err := s.db.QueryRow(ctx, `
		INSERT INTO coaches (train_id, coach_number, seat_class)
		VALUES ($1, $2, $3)
		RETURNING id::text, train_id::text, coach_number, seat_class, created_at, updated_at
	`, params.TrainID, number, params.SeatClass.String()).Scan(
		&coach.ID, &coach.TrainID, &coach.CoachNumber, &seatClass, &coach.CreatedAt, &coach.UpdatedAt,
	)
	if err != nil {
		return Coach{}, safeError(err)
	}
	parsedClass, err := domain.ParseSeatClass(seatClass)
	if err != nil {
		return Coach{}, ErrPersistence
	}
	coach.SeatClass = parsedClass
	coach.CreatedAt = coach.CreatedAt.UTC()
	coach.UpdatedAt = coach.UpdatedAt.UTC()
	return coach, nil
}

type Seat struct {
	ID         string
	CoachID    string
	SeatNumber string
	SeatType   string
	Active     bool
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type CreateSeatParams struct {
	CoachID    string
	SeatNumber string
	SeatType   string
}

func (s *Store) CreateSeat(ctx context.Context, params CreateSeatParams) (Seat, error) {
	number := strings.TrimSpace(params.SeatNumber)
	seatType := strings.ToLower(strings.TrimSpace(params.SeatType))
	if params.CoachID == "" || runeLengthOutside(number, 1, 16) || !validSeatType(seatType) {
		return Seat{}, ErrInvalidInput
	}
	var seat Seat
	err := s.db.QueryRow(ctx, `
		INSERT INTO seats (coach_id, seat_number, seat_type)
		VALUES ($1, $2, $3)
		RETURNING id::text, coach_id::text, seat_number, seat_type, active, created_at, updated_at
	`, params.CoachID, number, seatType).Scan(
		&seat.ID, &seat.CoachID, &seat.SeatNumber, &seat.SeatType, &seat.Active, &seat.CreatedAt, &seat.UpdatedAt,
	)
	if err != nil {
		return Seat{}, safeError(err)
	}
	seat.CreatedAt = seat.CreatedAt.UTC()
	seat.UpdatedAt = seat.UpdatedAt.UTC()
	return seat, nil
}

func validSeatType(value string) bool {
	switch value {
	case "window", "aisle", "middle", "other":
		return true
	default:
		return false
	}
}

type Fare struct {
	ID            string
	TrainRunID    string
	RouteID       string
	FromStopIndex int
	ToStopIndex   int
	SeatClass     domain.SeatClass
	AmountMinor   int64
	Currency      string
	Active        bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type CreateFareParams struct {
	TrainRunID    string
	RouteID       string
	FromStopIndex int
	ToStopIndex   int
	SeatClass     domain.SeatClass
	AmountMinor   int64
	Currency      string
}

func (s *Store) CreateFare(ctx context.Context, params CreateFareParams) (Fare, error) {
	currency := strings.ToUpper(strings.TrimSpace(params.Currency))
	if (params.TrainRunID == "") == (params.RouteID == "") || params.FromStopIndex < 0 || params.FromStopIndex >= params.ToStopIndex || !params.SeatClass.IsValid() || params.AmountMinor < 0 || !validCurrency(currency) {
		return Fare{}, ErrInvalidInput
	}

	var row pgx.Row
	if params.TrainRunID != "" {
		row = s.db.QueryRow(ctx, `
			INSERT INTO fares (train_run_id, from_stop_index, to_stop_index, seat_class, amount_minor, currency)
			SELECT id, $2, $3, $4, $5, $6
			FROM train_runs
			WHERE id = $1 AND segment_count >= $3
			RETURNING id::text, train_run_id::text, COALESCE(route_id::text, ''), from_stop_index, to_stop_index, seat_class, amount_minor, currency, active, created_at, updated_at
		`, params.TrainRunID, params.FromStopIndex, params.ToStopIndex, params.SeatClass.String(), params.AmountMinor, currency)
	} else {
		row = s.db.QueryRow(ctx, `
			INSERT INTO fares (route_id, from_stop_index, to_stop_index, seat_class, amount_minor, currency)
			SELECT r.id, $2, $3, $4, $5, $6
			FROM routes AS r
			WHERE r.id = $1
			  AND (SELECT count(*)::integer FROM route_stops WHERE route_id = r.id) > $3
			RETURNING id::text, COALESCE(train_run_id::text, ''), route_id::text, from_stop_index, to_stop_index, seat_class, amount_minor, currency, active, created_at, updated_at
		`, params.RouteID, params.FromStopIndex, params.ToStopIndex, params.SeatClass.String(), params.AmountMinor, currency)
	}
	return scanFare(row)
}

func validCurrency(value string) bool {
	if len(value) != 3 {
		return false
	}
	for _, character := range value {
		if character < 'A' || character > 'Z' {
			return false
		}
	}
	return true
}

func scanFare(row pgx.Row) (Fare, error) {
	var fare Fare
	var seatClass string
	if err := row.Scan(
		&fare.ID, &fare.TrainRunID, &fare.RouteID, &fare.FromStopIndex, &fare.ToStopIndex,
		&seatClass, &fare.AmountMinor, &fare.Currency, &fare.Active, &fare.CreatedAt, &fare.UpdatedAt,
	); err != nil {
		return Fare{}, safeError(err)
	}
	parsedClass, err := domain.ParseSeatClass(seatClass)
	if err != nil {
		return Fare{}, ErrPersistence
	}
	fare.SeatClass = parsedClass
	fare.CreatedAt = fare.CreatedAt.UTC()
	fare.UpdatedAt = fare.UpdatedAt.UTC()
	return fare, nil
}
