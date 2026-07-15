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

type authServiceStub struct {
	registered bool
}

type rateLimiterStub struct {
	allowed bool
	err     error
	input   httpapi.RateLimitRequest
}

func (s *rateLimiterStub) Allow(_ context.Context, input httpapi.RateLimitRequest) (bool, error) {
	s.input = input
	return s.allowed, s.err
}

func (s *authServiceStub) Register(context.Context, httpapi.RegisterCommand) (httpapi.TokenPairView, error) {
	s.registered = true
	return httpapi.TokenPairView{}, nil
}

func (s *authServiceStub) Login(context.Context, httpapi.LoginCommand) (httpapi.TokenPairView, error) {
	return httpapi.TokenPairView{}, nil
}

func (s *authServiceStub) Refresh(context.Context, string) (httpapi.TokenPairView, error) {
	return httpapi.TokenPairView{}, nil
}

func (s *authServiceStub) Logout(context.Context, string, string) error { return nil }

func TestPublicRegistrationCannotSelectPrivilegedRole(t *testing.T) {
	t.Parallel()

	auth := &authServiceStub{}
	router := httpapi.New(httpapi.Dependencies{Auth: auth})
	body := []byte(`{"email":"customer@example.test","password":"correct-horse-battery-staple","role":"admin"}`)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("registration with role status = %d, want 400", response.Code)
	}
	if auth.registered {
		t.Fatal("registration port called with caller-selected role")
	}
}

func TestRegistrationRateLimitDenialReturns429(t *testing.T) {
	t.Parallel()

	auth := &authServiceStub{}
	limiter := &rateLimiterStub{allowed: false}
	router := httpapi.New(httpapi.Dependencies{Auth: auth, RateLimiter: limiter})
	body := []byte(`{"email":"customer@example.test","password":"correct-horse-battery-staple"}`)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader(body))
	request.RemoteAddr = "192.0.2.10:12345"
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("rate-limited registration status = %d, want 429", response.Code)
	}
	if limiter.input.Scope != httpapi.RateLimitRegister || limiter.input.Key != "192.0.2.10" {
		t.Fatalf("rate limit input = %+v", limiter.input)
	}
	if auth.registered {
		t.Fatal("registration port called after rate-limit denial")
	}
}

func TestRegistrationRateLimitUsesForwardedClientOnlyFromTrustedProxy(t *testing.T) {
	auth := &authServiceStub{}
	limiter := &rateLimiterStub{allowed: false}
	router := httpapi.New(httpapi.Dependencies{Auth: auth, RateLimiter: limiter})
	if err := router.SetTrustedProxies([]string{"10.0.0.0/8"}); err != nil {
		t.Fatalf("set trusted proxies: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader([]byte(`{"email":"customer@example.test","password":"correct-horse-battery-staple"}`)))
	request.RemoteAddr = "10.1.2.3:12345"
	request.Header.Set("X-Forwarded-For", "198.51.100.25")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("rate-limited registration status = %d, want 429", response.Code)
	}
	if limiter.input.Key != "198.51.100.25" {
		t.Fatalf("rate-limit key = %q, want forwarded client IP", limiter.input.Key)
	}
}

func TestRegistrationFailsClosedWhenRateLimiterBackendFails(t *testing.T) {
	auth := &authServiceStub{}
	limiter := &rateLimiterStub{err: errors.New("redis unavailable")}
	router := httpapi.New(httpapi.Dependencies{Auth: auth, RateLimiter: limiter})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader([]byte(`{"email":"customer@example.test","password":"correct-horse-battery-staple"}`)))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("registration limiter outage status = %d, want 503", response.Code)
	}
	if auth.registered {
		t.Fatal("registration reached auth service after limiter outage")
	}
}
