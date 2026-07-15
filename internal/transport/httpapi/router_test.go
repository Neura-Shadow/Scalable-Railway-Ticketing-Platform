package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

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
