package httpapi_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

type passengerServiceStub struct {
	owner string
}

func (s *passengerServiceStub) ListPassengers(context.Context, string, httpapi.PageRequest) (httpapi.PassengerPage, error) {
	return httpapi.PassengerPage{}, nil
}

func (s *passengerServiceStub) CreatePassenger(_ context.Context, owner, displayName string) (httpapi.PassengerView, error) {
	s.owner = owner
	return httpapi.PassengerView{ID: "passenger-1", DisplayName: displayName}, nil
}

func (s *passengerServiceStub) GetPassenger(context.Context, string, string) (httpapi.PassengerView, error) {
	return httpapi.PassengerView{}, nil
}

func (s *passengerServiceStub) UpdatePassenger(context.Context, string, string, string) (httpapi.PassengerView, error) {
	return httpapi.PassengerView{}, nil
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
