package httpapi_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

type tokenParserStub struct {
	identity httpapi.Identity
	err      error
	raw      string
}

func TestReservationBodyLimitAndMaxPassengersAreEnforcedBeforeApplication(t *testing.T) {
	t.Parallel()

	parser := &tokenParserStub{identity: httpapi.Identity{Subject: "customer-1", Role: httpapi.RoleCustomer}}
	validBody := `{"train_run_id":"run-1","origin_station_code":"TPE","destination_station_code":"KHH","seat_class":"standard","passenger_ids":["p1","p2"]}`

	t.Run("body limit", func(t *testing.T) {
		reservations := &reservationServiceStub{}
		router := httpapi.New(httpapi.Dependencies{TokenParser: parser, Reservations: reservations, MaxRequestBodyBytes: 64})
		request := httptest.NewRequest(http.MethodPost, "/api/v1/reservations", strings.NewReader(validBody))
		request.Header.Set("Authorization", "Bearer signed-token")
		request.Header.Set("Idempotency-Key", "request-key-1")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("oversized request status = %d, want 413", response.Code)
		}
		if reservations.created.OwnerID != "" {
			t.Fatal("application called for oversized request")
		}
	})

	t.Run("max passengers", func(t *testing.T) {
		reservations := &reservationServiceStub{}
		router := httpapi.New(httpapi.Dependencies{TokenParser: parser, Reservations: reservations, MaxPassengers: 1})
		request := httptest.NewRequest(http.MethodPost, "/api/v1/reservations", strings.NewReader(validBody))
		request.Header.Set("Authorization", "Bearer signed-token")
		request.Header.Set("Idempotency-Key", "request-key-1")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("too many passengers status = %d, want 400", response.Code)
		}
		if reservations.created.OwnerID != "" {
			t.Fatal("application called above passenger limit")
		}
	})
}

func TestInternalReservationErrorsAreNotExposed(t *testing.T) {
	t.Parallel()

	secret := "postgres://user:sentinel-password@db/railway"
	parser := &tokenParserStub{identity: httpapi.Identity{Subject: "customer-1", Role: httpapi.RoleCustomer}}
	reservations := &reservationServiceStub{err: errors.New(secret)}
	router := httpapi.New(httpapi.Dependencies{TokenParser: parser, Reservations: reservations})
	body := `{"train_run_id":"run-1","origin_station_code":"TPE","destination_station_code":"KHH","seat_class":"standard","passenger_ids":["p1"]}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/reservations", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer signed-token")
	request.Header.Set("Idempotency-Key", "request-key-1")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("internal error status = %d, want 500", response.Code)
	}
	if strings.Contains(response.Body.String(), secret) {
		t.Fatal("response exposed raw internal error")
	}
}

func TestReservationCreateDegradesOpenWhenRateLimiterBackendFails(t *testing.T) {
	t.Parallel()

	parser := &tokenParserStub{identity: httpapi.Identity{Subject: "customer-1", Role: httpapi.RoleCustomer}}
	reservations := &reservationServiceStub{result: httpapi.ReservationView{ID: "reservation-1", Status: "held"}}
	limiter := &rateLimiterStub{err: errors.New("redis sentinel secret")}
	router := httpapi.New(httpapi.Dependencies{TokenParser: parser, Reservations: reservations, RateLimiter: limiter})
	body := `{"train_run_id":"run-1","origin_station_code":"TPE","destination_station_code":"KHH","seat_class":"standard","passenger_ids":["p1"]}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/reservations", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer signed-token")
	request.Header.Set("Idempotency-Key", "request-key-1")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("rate-limit backend failure status = %d, want 201", response.Code)
	}
	if limiter.input.Scope != httpapi.RateLimitReservationCreate || limiter.input.Key != "customer-1" {
		t.Fatalf("rate limit input = %+v", limiter.input)
	}
}

type metricsSpy struct {
	method   string
	path     string
	status   int
	duration time.Duration
}

func (s *metricsSpy) ObserveHTTP(method, path string, status int, duration time.Duration) {
	s.method = method
	s.path = path
	s.status = status
	s.duration = duration
}

func (s *tokenParserStub) ParseAccessToken(_ context.Context, raw string) (httpapi.Identity, error) {
	s.raw = raw
	return s.identity, s.err
}

type reservationServiceStub struct {
	created           httpapi.CreateReservationCommand
	createHadDeadline bool
	result            httpapi.ReservationView
	err               error
	mutation          httpapi.ReservationMutationCommand
}

func (s *reservationServiceStub) CreateHold(ctx context.Context, command httpapi.CreateReservationCommand) (httpapi.ReservationView, error) {
	s.created = command
	_, s.createHadDeadline = ctx.Deadline()
	return s.result, s.err
}

func (s *reservationServiceStub) GetReservation(context.Context, string, string) (httpapi.ReservationView, error) {
	return s.result, s.err
}

func (s *reservationServiceStub) ConfirmReservation(_ context.Context, command httpapi.ReservationMutationCommand) (httpapi.ReservationView, error) {
	s.mutation = command
	return s.result, s.err
}

func (s *reservationServiceStub) CancelReservation(_ context.Context, command httpapi.ReservationMutationCommand) (httpapi.ReservationView, error) {
	s.mutation = command
	return s.result, s.err
}

func TestCreateReservationUsesOwnerFromBearerClaims(t *testing.T) {
	t.Parallel()

	parser := &tokenParserStub{identity: httpapi.Identity{Subject: "customer-from-jwt", Role: httpapi.RoleCustomer}}
	reservations := &reservationServiceStub{result: httpapi.ReservationView{ID: "reservation-1", Status: "held"}}
	router := httpapi.New(httpapi.Dependencies{TokenParser: parser, Reservations: reservations})
	body := []byte(`{"train_run_id":"run-1","origin_station_code":"TPE","destination_station_code":"KHH","seat_class":"standard","passenger_ids":["passenger-1"]}`)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/reservations", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer signed-token")
	request.Header.Set("Idempotency-Key", "reservation-request-1")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("POST reservation status = %d, body = %s", response.Code, response.Body)
	}
	if parser.raw != "signed-token" {
		t.Fatalf("parser raw token = %q", parser.raw)
	}
	if reservations.created.OwnerID != "customer-from-jwt" {
		t.Fatalf("command owner = %q, want JWT subject", reservations.created.OwnerID)
	}
}

func TestPhysicalQueryTimeoutDoesNotCapReservationSaga(t *testing.T) {
	t.Parallel()

	parser := &tokenParserStub{identity: httpapi.Identity{Subject: "customer-from-jwt", Role: httpapi.RoleCustomer}}
	reservations := &reservationServiceStub{result: httpapi.ReservationView{ID: "reservation-1", Status: "held"}}
	router := httpapi.New(httpapi.Dependencies{
		TokenParser:            parser,
		Reservations:           reservations,
		PhysicalRequestTimeout: time.Millisecond,
	})
	body := []byte(`{"train_run_id":"run-1","origin_station_code":"TPE","destination_station_code":"KHH","seat_class":"standard","passenger_ids":["passenger-1"]}`)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/reservations", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer signed-token")
	request.Header.Set("Idempotency-Key", "reservation-request-1")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("POST reservation status = %d, body = %s", response.Code, response.Body)
	}
	if reservations.createHadDeadline {
		t.Fatal("reservation saga unexpectedly received the physical shard query deadline")
	}
}

func TestMetricsReceiveRouterPatternInsteadOfRawReservationID(t *testing.T) {
	t.Parallel()

	parser := &tokenParserStub{identity: httpapi.Identity{Subject: "customer-1", Role: httpapi.RoleCustomer}}
	reservations := &reservationServiceStub{result: httpapi.ReservationView{ID: "reservation-1", Status: "held"}}
	metrics := &metricsSpy{}
	router := httpapi.New(httpapi.Dependencies{TokenParser: parser, Reservations: reservations, HTTPMetrics: metrics})
	rawID := "reservation-019f661d-e56c-7f93-8618-942adeed5565"
	request := httptest.NewRequest(http.MethodGet, "/api/v1/reservations/"+rawID, nil)
	request.Header.Set("Authorization", "Bearer signed-token")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("GET reservation status = %d, body = %s", response.Code, response.Body)
	}
	if metrics.path != "/api/v1/reservations/:id" {
		t.Fatalf("metrics path = %q, want normalized router pattern", metrics.path)
	}
	if strings.Contains(metrics.path, rawID) {
		t.Fatal("metrics path contains raw reservation ID")
	}
}

func TestConfirmRequiresIdempotencyKeyAndPassesClaimsIdentity(t *testing.T) {
	t.Parallel()

	parser := &tokenParserStub{identity: httpapi.Identity{Subject: "customer-1", Role: httpapi.RoleCustomer}}
	reservations := &reservationServiceStub{result: httpapi.ReservationView{ID: "reservation-1", Status: "confirmed"}}
	router := httpapi.New(httpapi.Dependencies{TokenParser: parser, Reservations: reservations})

	missingKey := httptest.NewRequest(http.MethodPost, "/api/v1/reservations/reservation-1/confirm", nil)
	missingKey.Header.Set("Authorization", "Bearer signed-token")
	missingResponse := httptest.NewRecorder()
	router.ServeHTTP(missingResponse, missingKey)
	if missingResponse.Code != http.StatusBadRequest {
		t.Fatalf("confirm without key status = %d, want 400", missingResponse.Code)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/reservations/reservation-1/confirm", nil)
	request.Header.Set("Authorization", "Bearer signed-token")
	request.Header.Set("Idempotency-Key", "confirm-request-1")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("confirm status = %d, body = %s", response.Code, response.Body)
	}
	if reservations.mutation.OwnerID != "customer-1" || reservations.mutation.IdempotencyKey != "confirm-request-1" {
		t.Fatalf("confirm command = %+v", reservations.mutation)
	}
}
