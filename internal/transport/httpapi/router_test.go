package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

type deadlineOfferingStub struct {
	done            chan error
	stationDeadline chan bool
}

func (stub *deadlineOfferingStub) ListStations(ctx context.Context, _ httpapi.PageRequest) (httpapi.StationPage, error) {
	if stub.stationDeadline != nil {
		_, bounded := ctx.Deadline()
		stub.stationDeadline <- bounded
		return httpapi.StationPage{}, nil
	}
	return httpapi.StationPage{}, errors.New("unexpected call")
}

func (stub *deadlineOfferingStub) SearchTrainRuns(ctx context.Context, _ httpapi.TrainRunSearch) (httpapi.TrainRunPage, error) {
	<-ctx.Done()
	stub.done <- ctx.Err()
	return httpapi.TrainRunPage{}, ctx.Err()
}

func (*deadlineOfferingStub) GetAvailability(context.Context, httpapi.AvailabilityQuery) (httpapi.AvailabilityView, error) {
	return httpapi.AvailabilityView{}, errors.New("unexpected call")
}

func TestLivezIsProcessOnly(t *testing.T) {
	t.Parallel()

	router := httpapi.New(httpapi.Dependencies{})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/livez", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("GET /livez status = %d, want 200", response.Code)
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Status != "ok" {
		t.Fatalf("status = %q, want ok", body.Status)
	}
}

func TestPhysicalAPIRequestTimeoutBoundsTheWholeRequestWithoutExtendingCallerDeadline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		parentTimeout time.Duration
		physicalLimit time.Duration
		maximum       time.Duration
	}{
		{name: "physical deadline", physicalLimit: 25 * time.Millisecond, maximum: 500 * time.Millisecond},
		{name: "earlier caller deadline", parentTimeout: 10 * time.Millisecond, physicalLimit: time.Second, maximum: 400 * time.Millisecond},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			stub := &deadlineOfferingStub{done: make(chan error, 1)}
			router := httpapi.New(httpapi.Dependencies{
				Offering:               stub,
				PhysicalRequestTimeout: test.physicalLimit,
			})
			request := httptest.NewRequest(http.MethodGet, "/api/v1/train-runs/search?origin_station_code=TPE&destination_station_code=KHH&service_date=2026-08-01&seat_class=standard", nil)
			if test.parentTimeout > 0 {
				parent, cancel := context.WithTimeout(request.Context(), test.parentTimeout)
				defer cancel()
				request = request.WithContext(parent)
			}
			response := httptest.NewRecorder()
			started := time.Now()
			router.ServeHTTP(response, request)

			if elapsed := time.Since(started); elapsed > test.maximum {
				t.Fatalf("bounded API request elapsed %s, want <= %s", elapsed, test.maximum)
			}
			select {
			case err := <-stub.done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("offering context error = %v, want deadline exceeded", err)
				}
			default:
				t.Fatal("offering did not observe the bounded request deadline")
			}
		})
	}
}

func TestPhysicalAPIRequestTimeoutDoesNotBoundControlOnlyOfferingRoute(t *testing.T) {
	t.Parallel()

	stub := &deadlineOfferingStub{stationDeadline: make(chan bool, 1)}
	router := httpapi.New(httpapi.Dependencies{
		Offering:               stub,
		PhysicalRequestTimeout: time.Millisecond,
	})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/stations", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/stations status = %d, want 200", response.Code)
	}
	if bounded := <-stub.stationDeadline; bounded {
		t.Fatal("control-only station query unexpectedly received the physical shard query deadline")
	}
}

type readinessStub struct {
	checks []httpapi.ReadinessCheck
	err    error
}

func (s readinessStub) CheckReadiness(context.Context) ([]httpapi.ReadinessCheck, error) {
	return s.checks, s.err
}

func TestReadyzIsStructuredAndDoesNotExposeDependencyErrors(t *testing.T) {
	t.Parallel()

	secret := "postgres://user:sentinel-password@db/railway"
	router := httpapi.New(httpapi.Dependencies{
		Readiness: readinessStub{
			checks: []httpapi.ReadinessCheck{
				{Name: "postgres", Ready: true},
				{Name: "redis", Ready: false},
			},
			err: errors.New(secret),
		},
	})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz status = %d, want 503", response.Code)
	}
	if body := response.Body.String(); strings.Contains(body, secret) {
		t.Fatal("readiness response exposed raw dependency error")
	}
	var body struct {
		Status     string            `json:"status"`
		Components map[string]string `json:"components"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Status != "unready" || body.Components["postgres"] != "up" || body.Components["redis"] != "down" {
		t.Fatalf("unexpected readiness response: %+v", body)
	}
}

func TestReadyzRequiresEveryProductionComponent(t *testing.T) {
	t.Parallel()

	router := httpapi.New(httpapi.Dependencies{Readiness: readinessStub{checks: []httpapi.ReadinessCheck{{Name: "postgres", Ready: true}}}})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("partial readiness status = %d, want 503", response.Code)
	}
	var body struct {
		Components map[string]string `json:"components"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	for _, required := range []string{"postgres", "redis", "migrations", "configuration"} {
		if _, ok := body.Components[required]; !ok {
			t.Errorf("readiness response missing %s", required)
		}
	}
}

func TestReadyzKeepsNonHotAPIReadyWhenRedisIsDegraded(t *testing.T) {
	t.Parallel()

	router := httpapi.New(httpapi.Dependencies{Readiness: readinessStub{checks: []httpapi.ReadinessCheck{
		{Name: "postgres", Ready: true},
		{Name: "redis", Ready: false, Optional: true},
		{Name: "migrations", Ready: true},
		{Name: "configuration", Ready: true},
	}}})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("Redis-degraded readiness status = %d, want 200", response.Code)
	}
	var body struct {
		Status     string            `json:"status"`
		Components map[string]string `json:"components"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Status != "ready" || body.Components["redis"] != "degraded" {
		t.Fatalf("Redis-degraded readiness body = %+v", body)
	}
}

func TestUnknownRouteUsesStandardErrorEnvelopeWithoutEchoingPath(t *testing.T) {
	t.Parallel()

	router := httpapi.New(httpapi.Dependencies{})
	response := httptest.NewRecorder()
	rawPath := "/sentinel-secret-path"
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, rawPath, nil))

	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown route status = %d, want 404", response.Code)
	}
	if strings.Contains(response.Body.String(), rawPath) {
		t.Fatal("error response echoed the caller-provided path")
	}
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if envelope.Error.Code != "not_found" || envelope.Error.Message == "" {
		t.Fatalf("unexpected error envelope: %+v", envelope)
	}
}
