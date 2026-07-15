package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

type offeringStub struct {
	search httpapi.TrainRunSearch
	called bool
}

func (s *offeringStub) ListStations(context.Context, httpapi.PageRequest) (httpapi.StationPage, error) {
	return httpapi.StationPage{}, nil
}

func (s *offeringStub) SearchTrainRuns(_ context.Context, search httpapi.TrainRunSearch) (httpapi.TrainRunPage, error) {
	s.called = true
	s.search = search
	return httpapi.TrainRunPage{}, nil
}

func (s *offeringStub) GetAvailability(context.Context, httpapi.AvailabilityQuery) (httpapi.AvailabilityView, error) {
	return httpapi.AvailabilityView{}, nil
}

func TestTrainSearchNormalizesPaginationAndAllowsOnlySafeSorts(t *testing.T) {
	t.Parallel()

	offering := &offeringStub{}
	router := httpapi.New(httpapi.Dependencies{Offering: offering})
	url := "/api/v1/train-runs/search?origin_station_code=TPE&destination_station_code=KHH&service_date=2026-08-01&page=0&limit=999&sort=-fare_minor"
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, url, nil))

	if response.Code != http.StatusOK {
		t.Fatalf("search status = %d, body = %s", response.Code, response.Body)
	}
	if offering.search.Page.Page != 1 || offering.search.Page.Limit != 100 || offering.search.Page.Sort != "-fare_minor" {
		t.Fatalf("normalized page = %+v", offering.search.Page)
	}
}

func TestTrainSearchRejectsUnsafeSortBeforeCallingApplication(t *testing.T) {
	t.Parallel()

	offering := &offeringStub{}
	router := httpapi.New(httpapi.Dependencies{Offering: offering})
	url := "/api/v1/train-runs/search?origin_station_code=TPE&destination_station_code=KHH&service_date=2026-08-01&sort=departure_at%3BDROP+TABLE+seats"
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, url, nil))

	if response.Code != http.StatusBadRequest {
		t.Fatalf("unsafe sort status = %d, want 400", response.Code)
	}
	if offering.called {
		t.Fatal("application port called with unsafe sort")
	}
}
