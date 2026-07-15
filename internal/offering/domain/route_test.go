package domain_test

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
)

func TestNewRouteAcceptsOrderedOvernightStops(t *testing.T) {
	t.Parallel()

	stops := []domain.RouteStop{
		mustRouteStop(t, "TPE", 0, 0, 10),
		mustRouteStop(t, "TXG", 1, 80, 85),
		mustRouteStop(t, "KHH", 2, 1_500, 1_505),
	}
	route, err := domain.NewRoute("west-1", "Western Overnight", stops)
	if err != nil {
		t.Fatalf("NewRoute() error = %v", err)
	}
	if got, want := route.SegmentCount(), 2; got != want {
		t.Fatalf("Route.SegmentCount() = %d, want %d", got, want)
	}
	if got, want := route.Code(), "WEST-1"; got != want {
		t.Fatalf("Route.Code() = %q, want %q", got, want)
	}
	if got, want := route.Stops()[2].DepartureOffsetMinutes(), 1_505; got != want {
		t.Fatalf("last departure offset = %d, want %d", got, want)
	}
}

func TestNewRouteRejectsNonContiguousStopIndices(t *testing.T) {
	t.Parallel()

	stops := []domain.RouteStop{
		mustRouteStop(t, "TPE", 0, 0, 10),
		mustRouteStop(t, "KHH", 2, 100, 110),
	}
	_, err := domain.NewRoute("WEST", "Western", stops)
	if !errors.Is(err, domain.ErrNonContiguousStopIndices) {
		t.Fatalf("NewRoute() error = %v, want ErrNonContiguousStopIndices", err)
	}
}

func TestNewRouteRejectsDuplicateStations(t *testing.T) {
	t.Parallel()

	stops := []domain.RouteStop{
		mustRouteStop(t, "TPE", 0, 0, 10),
		mustRouteStop(t, "TPE", 1, 100, 110),
	}
	_, err := domain.NewRoute("LOOP", "Loop", stops)
	if !errors.Is(err, domain.ErrDuplicateRouteStation) {
		t.Fatalf("NewRoute() error = %v, want ErrDuplicateRouteStation", err)
	}
}

func TestNewRouteRejectsDecreasingOffsets(t *testing.T) {
	t.Parallel()

	stops := []domain.RouteStop{
		mustRouteStop(t, "TPE", 0, 100, 110),
		mustRouteStop(t, "KHH", 1, 109, 120),
	}
	_, err := domain.NewRoute("WEST", "Western", stops)
	if !errors.Is(err, domain.ErrNondecreasingStopOffsets) {
		t.Fatalf("NewRoute() error = %v, want ErrNondecreasingStopOffsets", err)
	}
}

func TestNewRouteRequiresAtLeastOneSegment(t *testing.T) {
	t.Parallel()

	stops := []domain.RouteStop{mustRouteStop(t, "TPE", 0, 0, 10)}
	_, err := domain.NewRoute("EMPTY", "No Journey", stops)
	if !errors.Is(err, domain.ErrRouteNeedsTwoStops) {
		t.Fatalf("NewRoute() error = %v, want ErrRouteNeedsTwoStops", err)
	}
}

func TestNewRouteStopRejectsInvalidOffsets(t *testing.T) {
	t.Parallel()

	code, err := domain.NewStationCode("TPE")
	if err != nil {
		t.Fatal(err)
	}
	_, err = domain.NewRouteStop(code, 0, 10, 9)
	if !errors.Is(err, domain.ErrNondecreasingStopOffsets) {
		t.Fatalf("NewRouteStop() error = %v, want ErrNondecreasingStopOffsets", err)
	}
}

func TestNewRouteStopRejectsInvalidStationAndIndex(t *testing.T) {
	t.Parallel()

	validCode, err := domain.NewStationCode("TPE")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		code  domain.StationCode
		index int
	}{
		{name: "invalid station", code: domain.StationCode("bad!"), index: 0},
		{name: "negative index", code: validCode, index: -1},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := domain.NewRouteStop(tt.code, tt.index, 0, 0)
			if !errors.Is(err, domain.ErrInvalidRouteStop) {
				t.Fatalf("NewRouteStop() error = %v, want ErrInvalidRouteStop", err)
			}
		})
	}
}

func TestRouteStopsReturnsDefensiveCopy(t *testing.T) {
	t.Parallel()

	stops := []domain.RouteStop{
		mustRouteStop(t, "TPE", 0, 0, 10),
		mustRouteStop(t, "KHH", 1, 100, 110),
	}
	route, err := domain.NewRoute("WEST", "Western", stops)
	if err != nil {
		t.Fatal(err)
	}

	returned := route.Stops()
	returned[0] = returned[1]
	if got, want := route.Stops()[0].StationCode().String(), "TPE"; got != want {
		t.Fatalf("route first station = %q after caller mutation, want %q", got, want)
	}
}

func mustRouteStop(t *testing.T, station string, index, arrival, departure int) domain.RouteStop {
	t.Helper()

	code, err := domain.NewStationCode(station)
	if err != nil {
		t.Fatalf("NewStationCode(%q) error = %v", station, err)
	}
	stop, err := domain.NewRouteStop(code, index, arrival, departure)
	if err != nil {
		t.Fatalf("NewRouteStop() error = %v", err)
	}
	return stop
}
