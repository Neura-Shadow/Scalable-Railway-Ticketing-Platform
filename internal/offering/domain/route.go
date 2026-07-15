package domain

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidRoute             = errors.New("invalid route")
	ErrInvalidRouteStop         = errors.New("invalid route stop")
	ErrRouteNeedsTwoStops       = errors.New("route requires at least two stops")
	ErrNonContiguousStopIndices = errors.New("route stop indices must be contiguous and zero-based")
	ErrDuplicateRouteStation    = errors.New("route contains a duplicate station")
	ErrNondecreasingStopOffsets = errors.New("route stop offsets must be nondecreasing")
)

type RouteStop struct {
	stationCode            StationCode
	stopIndex              int
	arrivalOffsetMinutes   int
	departureOffsetMinutes int
}

func NewRouteStop(stationCode StationCode, stopIndex, arrivalOffsetMinutes, departureOffsetMinutes int) (RouteStop, error) {
	if !stationCode.IsValid() || stopIndex < 0 {
		return RouteStop{}, ErrInvalidRouteStop
	}
	if arrivalOffsetMinutes < 0 || departureOffsetMinutes < arrivalOffsetMinutes {
		return RouteStop{}, fmt.Errorf("%w: %w", ErrInvalidRouteStop, ErrNondecreasingStopOffsets)
	}
	return RouteStop{
		stationCode:            stationCode,
		stopIndex:              stopIndex,
		arrivalOffsetMinutes:   arrivalOffsetMinutes,
		departureOffsetMinutes: departureOffsetMinutes,
	}, nil
}

func (s RouteStop) StationCode() StationCode {
	return s.stationCode
}

func (s RouteStop) StopIndex() int {
	return s.stopIndex
}

func (s RouteStop) ArrivalOffsetMinutes() int {
	return s.arrivalOffsetMinutes
}

func (s RouteStop) DepartureOffsetMinutes() int {
	return s.departureOffsetMinutes
}

type Route struct {
	code  string
	name  string
	stops []RouteStop
}

func NewRoute(code, name string, stops []RouteStop) (Route, error) {
	normalizedCode := strings.ToUpper(strings.TrimSpace(code))
	normalizedName := strings.TrimSpace(name)
	if normalizedCode == "" || normalizedName == "" {
		return Route{}, ErrInvalidRoute
	}
	if len(stops) < 2 {
		return Route{}, fmt.Errorf("%w: %w", ErrInvalidRoute, ErrRouteNeedsTwoStops)
	}

	seenStations := make(map[StationCode]struct{}, len(stops))
	for index, stop := range stops {
		if !stop.stationCode.IsValid() || stop.arrivalOffsetMinutes < 0 || stop.departureOffsetMinutes < stop.arrivalOffsetMinutes {
			return Route{}, fmt.Errorf("%w: %w", ErrInvalidRoute, ErrInvalidRouteStop)
		}
		if stop.stopIndex != index {
			return Route{}, fmt.Errorf("%w: %w", ErrInvalidRoute, ErrNonContiguousStopIndices)
		}
		if _, exists := seenStations[stop.stationCode]; exists {
			return Route{}, fmt.Errorf("%w: %w", ErrInvalidRoute, ErrDuplicateRouteStation)
		}
		seenStations[stop.stationCode] = struct{}{}
		if index > 0 && stops[index-1].departureOffsetMinutes > stop.arrivalOffsetMinutes {
			return Route{}, fmt.Errorf("%w: %w", ErrInvalidRoute, ErrNondecreasingStopOffsets)
		}
	}

	return Route{
		code:  normalizedCode,
		name:  normalizedName,
		stops: append([]RouteStop(nil), stops...),
	}, nil
}

func (r Route) Code() string {
	return r.code
}

func (r Route) Name() string {
	return r.name
}

func (r Route) Stops() []RouteStop {
	return append([]RouteStop(nil), r.stops...)
}

func (r Route) SegmentCount() int {
	if len(r.stops) < 2 {
		return 0
	}
	return len(r.stops) - 1
}
