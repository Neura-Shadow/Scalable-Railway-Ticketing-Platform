package app

import (
	"context"
	"errors"
	"sort"
	"strings"

	accountsdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
	accountspostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/postgres"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

type passengerStore interface {
	ListPassengers(context.Context, string) ([]accountspostgres.Passenger, error)
	CreatePassenger(context.Context, accountspostgres.CreatePassengerParams) (accountspostgres.Passenger, error)
	GetPassenger(context.Context, string, string) (accountspostgres.Passenger, error)
	UpdatePassenger(context.Context, accountspostgres.UpdatePassengerParams) (accountspostgres.Passenger, error)
	DeletePassenger(context.Context, string, string) error
}

type PassengerService struct{ store passengerStore }

func NewPassengerService(store passengerStore) *PassengerService {
	return &PassengerService{store: store}
}

func (s *PassengerService) ListPassengers(ctx context.Context, ownerID string, page httpapi.PageRequest) (httpapi.PassengerPage, error) {
	if s == nil || s.store == nil || strings.TrimSpace(ownerID) == "" {
		return httpapi.PassengerPage{}, httpapi.ErrInvalidInput
	}
	items, err := s.store.ListPassengers(ctx, ownerID)
	if err != nil {
		return httpapi.PassengerPage{}, mapPassengerError(err)
	}
	descending := strings.HasPrefix(page.Sort, "-")
	sort.SliceStable(items, func(i, j int) bool {
		less := items[i].CreatedAt.Before(items[j].CreatedAt)
		if strings.TrimPrefix(page.Sort, "-") == "display_name" {
			less = items[i].DisplayName < items[j].DisplayName
		}
		if descending {
			return !less && items[i] != items[j]
		}
		return less
	})
	normalizedPage, normalizedLimit, start, end := pageBounds(page, len(items))
	views := make([]httpapi.PassengerView, 0, end-start)
	for _, item := range items[start:end] {
		views = append(views, passengerView(item))
	}
	return httpapi.PassengerPage{Items: views, Page: normalizedPage, Limit: normalizedLimit, Total: int64(len(items))}, nil
}

func (s *PassengerService) CreatePassenger(ctx context.Context, ownerID, displayName string) (httpapi.PassengerView, error) {
	if s == nil || s.store == nil {
		return httpapi.PassengerView{}, httpapi.ErrUnavailable
	}
	if !accountsdomain.ValidPassengerDisplayName(displayName) {
		return httpapi.PassengerView{}, httpapi.ErrInvalidInput
	}
	item, err := s.store.CreatePassenger(ctx, accountspostgres.CreatePassengerParams{UserID: ownerID, DisplayName: displayName})
	if err != nil {
		return httpapi.PassengerView{}, mapPassengerError(err)
	}
	return passengerView(item), nil
}

func (s *PassengerService) GetPassenger(ctx context.Context, ownerID, passengerID string) (httpapi.PassengerView, error) {
	if s == nil || s.store == nil {
		return httpapi.PassengerView{}, httpapi.ErrUnavailable
	}
	item, err := s.store.GetPassenger(ctx, ownerID, passengerID)
	if err != nil {
		return httpapi.PassengerView{}, mapPassengerError(err)
	}
	return passengerView(item), nil
}

func (s *PassengerService) UpdatePassenger(ctx context.Context, ownerID, passengerID, displayName string) (httpapi.PassengerView, error) {
	if s == nil || s.store == nil {
		return httpapi.PassengerView{}, httpapi.ErrUnavailable
	}
	if !accountsdomain.ValidPassengerDisplayName(displayName) {
		return httpapi.PassengerView{}, httpapi.ErrInvalidInput
	}
	item, err := s.store.UpdatePassenger(ctx, accountspostgres.UpdatePassengerParams{UserID: ownerID, PassengerID: passengerID, DisplayName: displayName})
	if err != nil {
		return httpapi.PassengerView{}, mapPassengerError(err)
	}
	return passengerView(item), nil
}

func (s *PassengerService) DeletePassenger(ctx context.Context, ownerID, passengerID string) error {
	if s == nil || s.store == nil {
		return httpapi.ErrUnavailable
	}
	return mapNilOrPassengerError(s.store.DeletePassenger(ctx, ownerID, passengerID))
}

func passengerView(item accountspostgres.Passenger) httpapi.PassengerView {
	return httpapi.PassengerView{ID: item.ID, DisplayName: item.DisplayName, CreatedAt: item.CreatedAt.UTC(), UpdatedAt: item.UpdatedAt.UTC()}
}

func mapPassengerError(err error) error {
	switch {
	case errors.Is(err, accountspostgres.ErrPassengerNotFound), errors.Is(err, accountspostgres.ErrUserNotFound):
		return httpapi.ErrNotFound
	case errors.Is(err, accountspostgres.ErrInvalidInput):
		return httpapi.ErrInvalidInput
	default:
		return httpapi.ErrUnavailable
	}
}
func mapNilOrPassengerError(err error) error {
	if err == nil {
		return nil
	}
	return mapPassengerError(err)
}

type offeringQueryStore interface {
	ListStations(context.Context) ([]querypostgres.Station, error)
	SearchTrainRuns(context.Context, querypostgres.SearchRequest) ([]querypostgres.SearchResult, error)
	Availability(context.Context, querypostgres.AvailabilityRequest) (querypostgres.Availability, error)
}

type offeringBatchAvailabilityStore interface {
	AvailabilityBatch(context.Context, []querypostgres.AvailabilityRequest) ([]querypostgres.Availability, error)
}

type OfferingQueries struct{ store offeringQueryStore }

func NewOfferingQueries(store offeringQueryStore) *OfferingQueries {
	return &OfferingQueries{store: store}
}

func (q *OfferingQueries) ListStations(ctx context.Context, page httpapi.PageRequest) (httpapi.StationPage, error) {
	if q == nil || q.store == nil {
		return httpapi.StationPage{}, httpapi.ErrUnavailable
	}
	items, err := q.store.ListStations(ctx)
	if err != nil {
		return httpapi.StationPage{}, mapQueryError(err)
	}
	desc := strings.HasPrefix(page.Sort, "-")
	field := strings.TrimPrefix(page.Sort, "-")
	sort.SliceStable(items, func(i, j int) bool {
		var less bool
		if field == "name" {
			less = items[i].Name < items[j].Name
		} else {
			less = items[i].Code.String() < items[j].Code.String()
		}
		if desc {
			return !less && items[i].ID != items[j].ID
		}
		return less
	})
	p, l, start, end := pageBounds(page, len(items))
	views := make([]httpapi.StationView, 0, end-start)
	for _, item := range items[start:end] {
		views = append(views, httpapi.StationView{ID: item.ID, Code: item.Code.String(), Name: item.Name, Timezone: item.Timezone})
	}
	return httpapi.StationPage{Items: views, Page: p, Limit: l, Total: int64(len(items))}, nil
}

func (q *OfferingQueries) SearchTrainRuns(ctx context.Context, search httpapi.TrainRunSearch) (httpapi.TrainRunPage, error) {
	if q == nil || q.store == nil {
		return httpapi.TrainRunPage{}, httpapi.ErrUnavailable
	}
	sortOrder, ok := querySort(search.Page.Sort)
	if !ok {
		return httpapi.TrainRunPage{}, httpapi.ErrInvalidInput
	}
	items, err := q.store.SearchTrainRuns(ctx, querypostgres.SearchRequest{OriginCode: search.OriginStationCode, DestinationCode: search.DestinationStationCode, ServiceDate: search.ServiceDate, SeatClass: search.SeatClass, Page: search.Page.Page, PageSize: search.Page.Limit, Sort: sortOrder})
	if err != nil {
		return httpapi.TrainRunPage{}, mapQueryError(err)
	}
	views := make([]httpapi.TrainRunView, 0, len(items))
	availabilityByRun := make(map[string]querypostgres.Availability, len(items))
	if batchStore, ok := q.store.(offeringBatchAvailabilityStore); ok && len(items) > 0 {
		requests := make([]querypostgres.AvailabilityRequest, 0, len(items))
		for _, item := range items {
			requests = append(requests, querypostgres.AvailabilityRequest{
				TrainRunID: item.TrainRunID, OriginCode: search.OriginStationCode,
				DestinationCode: search.DestinationStationCode, SeatClass: search.SeatClass,
			})
		}
		availabilityItems, err := batchStore.AvailabilityBatch(ctx, requests)
		if err != nil {
			return httpapi.TrainRunPage{}, mapQueryError(err)
		}
		for _, availability := range availabilityItems {
			availabilityByRun[availability.TrainRunID] = availability
		}
	}
	for _, item := range items {
		availability, exists := availabilityByRun[item.TrainRunID]
		if !exists {
			var err error
			availability, err = q.store.Availability(ctx, querypostgres.AvailabilityRequest{TrainRunID: item.TrainRunID, OriginCode: search.OriginStationCode, DestinationCode: search.DestinationStationCode, SeatClass: search.SeatClass})
			if err != nil {
				return httpapi.TrainRunPage{}, mapQueryError(err)
			}
		}
		views = append(views, httpapi.TrainRunView{ID: item.TrainRunID, TrainCode: item.TrainCode, OriginStationCode: search.OriginStationCode, DestinationStationCode: search.DestinationStationCode, DepartureAt: item.DepartureAt, ArrivalAt: item.ArrivalAt, SeatClass: item.SeatClass.String(), AvailableSeatCount: int(availability.AvailableSeats), FareMinor: availability.FareAmountMinor, Currency: availability.Currency})
	}
	return httpapi.TrainRunPage{Items: views, Page: normalizePage(search.Page.Page), Limit: normalizeLimit(search.Page.Limit), Total: int64(len(views))}, nil
}

func (q *OfferingQueries) GetAvailability(ctx context.Context, input httpapi.AvailabilityQuery) (httpapi.AvailabilityView, error) {
	if q == nil || q.store == nil {
		return httpapi.AvailabilityView{}, httpapi.ErrUnavailable
	}
	item, err := q.store.Availability(ctx, querypostgres.AvailabilityRequest{TrainRunID: input.TrainRunID, OriginCode: input.OriginStationCode, DestinationCode: input.DestinationStationCode, SeatClass: input.SeatClass})
	if err != nil {
		return httpapi.AvailabilityView{}, mapQueryError(err)
	}
	return httpapi.AvailabilityView{TrainRunID: item.TrainRunID, TrainCode: item.TrainCode, OriginStationCode: input.OriginStationCode, DestinationStationCode: input.DestinationStationCode, DepartureAt: item.DepartureAt, ArrivalAt: item.ArrivalAt, SeatClass: item.SeatClass.String(), AvailableSeatCount: int(item.AvailableSeats), FareMinor: item.FareAmountMinor, Currency: item.Currency}, nil
}

func querySort(value string) (string, bool) {
	switch value {
	case "", "departure_at":
		return "departure_asc", true
	case "-departure_at":
		return "departure_desc", true
	case "fare_minor":
		return "fare_asc", true
	case "-fare_minor":
		return "fare_desc", true
	default:
		return "", false
	}
}
func mapQueryError(err error) error {
	switch {
	case errors.Is(err, querypostgres.ErrInvalidQuery), errors.Is(err, querypostgres.ErrInvalidJourney), errors.Is(err, querypostgres.ErrInvalidSort), errors.Is(err, offeringdomain.ErrInvalidSeatClass):
		return httpapi.ErrInvalidInput
	case errors.Is(err, querypostgres.ErrNotFound):
		return httpapi.ErrNotFound
	default:
		return httpapi.ErrUnavailable
	}
}

func normalizePage(value int) int {
	if value < 1 {
		return 1
	}
	if value > querypostgres.MaxPage {
		return querypostgres.MaxPage
	}
	return value
}
func normalizeLimit(value int) int {
	if value < 1 {
		return 20
	}
	if value > 100 {
		return 100
	}
	return value
}
func pageBounds(page httpapi.PageRequest, total int) (int, int, int, int) {
	p, l := normalizePage(page.Page), normalizeLimit(page.Limit)
	start := total
	if p <= total/l+1 {
		start = (p - 1) * l
		if start > total {
			start = total
		}
	}
	end := start + l
	if end > total {
		end = total
	}
	return p, l, start, end
}

var (
	_ httpapi.PassengerService = (*PassengerService)(nil)
	_ httpapi.OfferingQueries  = (*OfferingQueries)(nil)
)
