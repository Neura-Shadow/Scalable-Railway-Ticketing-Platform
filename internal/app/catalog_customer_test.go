package app

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	accountspostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/postgres"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

type passengerStoreFake struct {
	owner       string
	items       []accountspostgres.Passenger
	err         error
	createCalls int
	updateCalls int
}

func (f *passengerStoreFake) ListPassengers(_ context.Context, owner string) ([]accountspostgres.Passenger, error) {
	f.owner = owner
	return f.items, f.err
}
func (f *passengerStoreFake) CreatePassenger(_ context.Context, input accountspostgres.CreatePassengerParams) (accountspostgres.Passenger, error) {
	f.createCalls++
	f.owner = input.UserID
	return accountspostgres.Passenger{ID: "p-new", UserID: input.UserID, DisplayName: input.DisplayName}, f.err
}
func (f *passengerStoreFake) GetPassenger(_ context.Context, owner, id string) (accountspostgres.Passenger, error) {
	f.owner = owner
	return accountspostgres.Passenger{ID: id, UserID: owner, DisplayName: "Rider"}, f.err
}
func (f *passengerStoreFake) UpdatePassenger(_ context.Context, input accountspostgres.UpdatePassengerParams) (accountspostgres.Passenger, error) {
	f.updateCalls++
	f.owner = input.UserID
	return accountspostgres.Passenger{ID: input.PassengerID, UserID: input.UserID, DisplayName: input.DisplayName}, f.err
}
func (f *passengerStoreFake) DeletePassenger(_ context.Context, owner, _ string) error {
	f.owner = owner
	return f.err
}

func TestPassengerServiceKeepsOwnerScopeAndPaginatesAfterSorting(t *testing.T) {
	store := &passengerStoreFake{items: []accountspostgres.Passenger{
		{ID: "2", DisplayName: "Zulu", CreatedAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
		{ID: "1", DisplayName: "Alpha", CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{ID: "3", DisplayName: "Mike", CreatedAt: time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)},
	}}
	page, err := NewPassengerService(store).ListPassengers(context.Background(), "owner-7", httpapi.PageRequest{Page: 2, Limit: 1, Sort: "display_name"})
	if err != nil {
		t.Fatalf("ListPassengers() error = %v", err)
	}
	if store.owner != "owner-7" || page.Total != 3 || len(page.Items) != 1 || page.Items[0].DisplayName != "Mike" {
		t.Fatalf("page = %#v, owner = %q", page, store.owner)
	}
}

func TestPassengerServiceHugePageReturnsEmptyWithoutIntegerOverflow(t *testing.T) {
	store := &passengerStoreFake{items: []accountspostgres.Passenger{{ID: "1", DisplayName: "Rider"}}}
	page, err := NewPassengerService(store).ListPassengers(context.Background(), "owner", httpapi.PageRequest{Page: math.MaxInt, Limit: 100})
	if err != nil {
		t.Fatalf("ListPassengers() error = %v", err)
	}
	if len(page.Items) != 0 || page.Total != 1 {
		t.Fatalf("huge page = %#v", page)
	}
}

func TestPassengerServiceMapsOwnerMissToSafeNotFound(t *testing.T) {
	store := &passengerStoreFake{err: accountspostgres.ErrPassengerNotFound}
	_, err := NewPassengerService(store).GetPassenger(context.Background(), "owner", "someone-elses-passenger")
	if err != httpapi.ErrNotFound {
		t.Fatalf("error = %v", err)
	}
}

func TestPassengerServiceRejectsInvalidDisplayNameBeforeStore(t *testing.T) {
	t.Parallel()

	for name, invalid := range map[string]string{
		"overlong":  strings.Repeat("界", 101),
		"control":   "Rider\x00",
		"malformed": string([]byte{0xff}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := &passengerStoreFake{}
			service := NewPassengerService(store)
			if _, err := service.CreatePassenger(context.Background(), "owner", invalid); err != httpapi.ErrInvalidInput {
				t.Fatalf("CreatePassenger() error = %v, want %v", err, httpapi.ErrInvalidInput)
			}
			if _, err := service.UpdatePassenger(context.Background(), "owner", "passenger", invalid); err != httpapi.ErrInvalidInput {
				t.Fatalf("UpdatePassenger() error = %v, want %v", err, httpapi.ErrInvalidInput)
			}
			if store.createCalls != 0 || store.updateCalls != 0 {
				t.Fatalf("store calls = create %d, update %d; want zero", store.createCalls, store.updateCalls)
			}
		})
	}
}

type offeringQueriesFake struct {
	stations     []querypostgres.Station
	search       []querypostgres.SearchResult
	availability querypostgres.Availability
	err          error
	searchInput  querypostgres.SearchRequest
}

func (f *offeringQueriesFake) ListStations(context.Context) ([]querypostgres.Station, error) {
	return f.stations, f.err
}
func (f *offeringQueriesFake) SearchTrainRuns(_ context.Context, input querypostgres.SearchRequest) ([]querypostgres.SearchResult, error) {
	f.searchInput = input
	return f.search, f.err
}
func (f *offeringQueriesFake) Availability(context.Context, querypostgres.AvailabilityRequest) (querypostgres.Availability, error) {
	return f.availability, f.err
}

func TestOfferingSearchMapsQueryResultsAndAvailability(t *testing.T) {
	departure := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	backend := &offeringQueriesFake{
		search:       []querypostgres.SearchResult{{TrainRunID: "run", TrainCode: "R100", DepartureAt: departure, ArrivalAt: departure.Add(time.Hour), SeatClass: offeringdomain.SeatClassStandard, FareAmountMinor: 1200, Currency: "TWD"}},
		availability: querypostgres.Availability{TrainRunID: "run", AvailableSeats: 9},
	}
	page, err := NewOfferingQueries(backend).SearchTrainRuns(context.Background(), httpapi.TrainRunSearch{
		OriginStationCode: "TPE", DestinationStationCode: "KHH", ServiceDate: departure, SeatClass: "standard",
		Page: httpapi.PageRequest{Page: 1, Limit: 20, Sort: "-fare_minor"},
	})
	if err != nil {
		t.Fatalf("SearchTrainRuns() error = %v", err)
	}
	if backend.searchInput.Sort != "fare_desc" || len(page.Items) != 1 || page.Items[0].AvailableSeatCount != 9 || page.Items[0].OriginStationCode != "TPE" {
		t.Fatalf("page = %#v, query = %#v", page, backend.searchInput)
	}
}

func TestOfferingQueriesReturnOnlySafeErrors(t *testing.T) {
	backend := &offeringQueriesFake{err: errors.New("postgres secret detail")}
	_, err := NewOfferingQueries(backend).ListStations(context.Background(), httpapi.PageRequest{Page: 1, Limit: 20, Sort: "code"})
	if err != httpapi.ErrUnavailable {
		t.Fatalf("error = %v", err)
	}
	backend.err = querypostgres.ErrInvalidJourney
	_, err = NewOfferingQueries(backend).GetAvailability(context.Background(), httpapi.AvailabilityQuery{TrainRunID: "run"})
	if err != httpapi.ErrInvalidInput {
		t.Fatalf("error = %v", err)
	}
}
