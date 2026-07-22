package query

import (
	"errors"
	"math"
	"time"
)

var ErrInvalidJourneyTime = errors.New("invalid journey time")

// AnchorJourneyTime applies the route's established first-stop departure
// anchor exactly once. scheduledDepartureAt is the UTC instant at stop zero,
// while targetOffsetMinutes is measured from the operator-local service-date
// origin.
func AnchorJourneyTime(
	scheduledDepartureAt time.Time,
	firstDepartureOffsetMinutes int,
	targetOffsetMinutes int,
) (time.Time, error) {
	if scheduledDepartureAt.IsZero() || firstDepartureOffsetMinutes < 0 || targetOffsetMinutes < firstDepartureOffsetMinutes {
		return time.Time{}, ErrInvalidJourneyTime
	}
	deltaMinutes := int64(targetOffsetMinutes) - int64(firstDepartureOffsetMinutes)
	if deltaMinutes > math.MaxInt64/int64(time.Minute) {
		return time.Time{}, ErrInvalidJourneyTime
	}
	return scheduledDepartureAt.UTC().Add(time.Duration(deltaMinutes) * time.Minute), nil
}

// AnchorJourneyTimes resolves one ordered journey using the same anchor for
// origin departure and destination arrival.
func AnchorJourneyTimes(
	scheduledDepartureAt time.Time,
	firstDepartureOffsetMinutes int,
	originDepartureOffsetMinutes int,
	destinationArrivalOffsetMinutes int,
) (time.Time, time.Time, error) {
	departureAt, err := AnchorJourneyTime(
		scheduledDepartureAt,
		firstDepartureOffsetMinutes,
		originDepartureOffsetMinutes,
	)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	arrivalAt, err := AnchorJourneyTime(
		scheduledDepartureAt,
		firstDepartureOffsetMinutes,
		destinationArrivalOffsetMinutes,
	)
	if err != nil || !departureAt.Before(arrivalAt) {
		return time.Time{}, time.Time{}, ErrInvalidJourneyTime
	}
	return departureAt, arrivalAt, nil
}
