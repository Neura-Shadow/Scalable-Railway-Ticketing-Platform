package httpapi_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

type passengerServiceStub struct {
	owner              string
	createdDisplayName string
	updatedDisplayName string
}

func (s *passengerServiceStub) ListPassengers(context.Context, string, httpapi.PageRequest) (httpapi.PassengerPage, error) {
	return httpapi.PassengerPage{}, nil
}

func (s *passengerServiceStub) CreatePassenger(_ context.Context, owner, displayName string) (httpapi.PassengerView, error) {
	s.owner = owner
	s.createdDisplayName = displayName
	return httpapi.PassengerView{ID: "passenger-1", DisplayName: displayName}, nil
}

func (s *passengerServiceStub) GetPassenger(context.Context, string, string) (httpapi.PassengerView, error) {
	return httpapi.PassengerView{}, nil
}

func (s *passengerServiceStub) UpdatePassenger(_ context.Context, owner, id, displayName string) (httpapi.PassengerView, error) {
	s.owner = owner
	s.updatedDisplayName = displayName
	return httpapi.PassengerView{ID: id, DisplayName: displayName}, nil
}

func (s *passengerServiceStub) DeletePassenger(context.Context, string, string) error { return nil }

func TestPassengerOwnershipComesFromJWTClaims(t *testing.T) {
	t.Parallel()

	parser := &tokenParserStub{identity: httpapi.Identity{Subject: "customer-from-jwt", Role: httpapi.RoleCustomer}}
	passengers := &passengerServiceStub{}
	router := httpapi.New(httpapi.Dependencies{TokenParser: parser, Passengers: passengers})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/passengers", bytes.NewBufferString(`{"display_name":"Synthetic Passenger"}`))
	request.Header.Set("Authorization", "Bearer signed-token")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("create passenger status = %d, body = %s", response.Code, response.Body)
	}
	if passengers.owner != "customer-from-jwt" {
		t.Fatalf("passenger owner = %q", passengers.owner)
	}
}

func TestPassengerCreationFailsClosedWhenLimiterBackendFails(t *testing.T) {
	parser := &tokenParserStub{identity: httpapi.Identity{Subject: "customer-from-jwt", Role: httpapi.RoleCustomer}}
	passengers := &passengerServiceStub{}
	router := httpapi.New(httpapi.Dependencies{TokenParser: parser, Passengers: passengers, RateLimiter: &rateLimiterStub{err: errors.New("redis unavailable")}})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/passengers", bytes.NewBufferString(`{"display_name":"Synthetic Passenger"}`))
	request.Header.Set("Authorization", "Bearer signed-token")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("create passenger limiter outage status = %d, want 503", response.Code)
	}
	if passengers.owner != "" {
		t.Fatal("passenger service called after limiter backend failure")
	}
}

func TestPassengerCreateAndUpdateUseSharedRuneBoundary(t *testing.T) {
	t.Parallel()

	parser := &tokenParserStub{identity: httpapi.Identity{Subject: "customer-from-jwt", Role: httpapi.RoleCustomer}}
	exact := strings.Repeat("界", 100)
	overlong := strings.Repeat("界", 101)

	for _, testCase := range []struct {
		name       string
		method     string
		path       string
		display    string
		wantStatus int
	}{
		{name: "create exact limit", method: http.MethodPost, path: "/api/v1/passengers", display: exact, wantStatus: http.StatusCreated},
		{name: "create above limit", method: http.MethodPost, path: "/api/v1/passengers", display: overlong, wantStatus: http.StatusBadRequest},
		{name: "update exact limit", method: http.MethodPatch, path: "/api/v1/passengers/passenger-1", display: exact, wantStatus: http.StatusOK},
		{name: "update above limit", method: http.MethodPatch, path: "/api/v1/passengers/passenger-1", display: overlong, wantStatus: http.StatusBadRequest},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			passengers := &passengerServiceStub{}
			router := httpapi.New(httpapi.Dependencies{TokenParser: parser, Passengers: passengers})
			body := []byte(`{"display_name":"` + testCase.display + `"}`)
			request := httptest.NewRequest(testCase.method, testCase.path, bytes.NewReader(body))
			request.Header.Set("Authorization", "Bearer signed-token")
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			if response.Code != testCase.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, testCase.wantStatus, response.Body.String())
			}
			if testCase.wantStatus < http.StatusBadRequest {
				if passengers.createdDisplayName != exact && passengers.updatedDisplayName != exact {
					t.Fatal("valid display name did not reach passenger service")
				}
			} else if passengers.createdDisplayName != "" || passengers.updatedDisplayName != "" {
				t.Fatal("invalid display name reached passenger service")
			}
		})
	}
}
