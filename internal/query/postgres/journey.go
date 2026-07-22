package postgres

import (
	"context"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	querydomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query"
)

// Journey is the authoritative persisted stop-index resolution for one train
// run. Store may be constructed with a transaction-scoped DBTX so booking can
// resolve these values inside its own authoritative transaction.
type Journey struct {
	TrainRunID                      string
	TrainID                         string
	TrainCode                       string
	RouteID                         string
	RouteCode                       string
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
}

func (s *Store) ResolveJourney(ctx context.Context, trainRunID, rawOrigin, rawDestination string) (Journey, error) {
	if strings.TrimSpace(trainRunID) == "" {
		return Journey{}, ErrInvalidQuery
	}
	origin, err := domain.NewStationCode(rawOrigin)
	if err != nil {
		return Journey{}, ErrInvalidJourney
	}
	destination, err := domain.NewStationCode(rawDestination)
	if err != nil || origin == destination {
		return Journey{}, ErrInvalidJourney
	}

	var journey Journey
	var status string
	var firstDepartureOffsetMinutes int
	// scheduled_departure_at is already the UTC departure instant at stop 0.
	// Later stop times therefore use offsets relative to that first departure.
	err = s.db.QueryRow(ctx, `
		SELECT tr.id::text, tr.train_id::text, t.code,
		       tr.route_id::text, r.code, tr.service_date,
		       tr.scheduled_departure_at, tr.status, tr.segment_count,
		       origin.stop_index, destination.stop_index,
		       route_origin.departure_offset_minutes,
		       origin.departure_offset_minutes, destination.arrival_offset_minutes
		FROM train_runs AS tr
		JOIN trains AS t ON t.id = tr.train_id AND t.active
		JOIN routes AS r ON r.id = tr.route_id AND r.active
		JOIN route_stops AS route_origin ON route_origin.route_id = tr.route_id AND route_origin.stop_index = 0
		JOIN route_stops AS origin ON origin.route_id = tr.route_id
		JOIN stations AS origin_station ON origin_station.id = origin.station_id AND origin_station.code = $2 AND origin_station.active
		JOIN route_stops AS destination ON destination.route_id = tr.route_id
		JOIN stations AS destination_station ON destination_station.id = destination.station_id AND destination_station.code = $3 AND destination_station.active
		WHERE tr.id = $1
		  AND origin.stop_index < destination.stop_index
	`, trainRunID, origin.String(), destination.String()).Scan(
		&journey.TrainRunID, &journey.TrainID, &journey.TrainCode,
		&journey.RouteID, &journey.RouteCode, &journey.ServiceDate,
		&journey.ScheduledDepartureAt, &status, &journey.SegmentCount,
		&journey.FromStopIndex, &journey.ToStopIndex,
		&firstDepartureOffsetMinutes,
		&journey.OriginDepartureOffsetMinutes, &journey.DestinationArrivalOffsetMinutes,
	)
	if err != nil {
		return Journey{}, safeQueryError(err)
	}
	parsedStatus, err := domain.ParseTrainRunStatus(status)
	if err != nil {
		return Journey{}, ErrPersistence
	}
	journey.Status = parsedStatus
	journey.ServiceDate = dateUTC(journey.ServiceDate)
	journey.ScheduledDepartureAt = journey.ScheduledDepartureAt.UTC()
	journey.DepartureAt, journey.ArrivalAt, err = querydomain.AnchorJourneyTimes(
		journey.ScheduledDepartureAt,
		firstDepartureOffsetMinutes,
		journey.OriginDepartureOffsetMinutes,
		journey.DestinationArrivalOffsetMinutes,
	)
	if err != nil {
		return Journey{}, ErrPersistence
	}
	return journey, nil
}
