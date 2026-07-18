package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

type authServiceStub struct {
	registered  bool
	registerErr error
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

func (s *authServiceStub) Register(context.Context, httpapi.RegisterCommand) error {
	s.registered = true
	return s.registerErr
}

func (s *authServiceStub) Login(context.Context, httpapi.LoginCommand) (httpapi.TokenPairView, error) {
	return httpapi.TokenPairView{}, nil
}

func (s *authServiceStub) Refresh(context.Context, string) (httpapi.TokenPairView, error) {
	return httpapi.TokenPairView{}, nil
}

func (s *authServiceStub) Logout(context.Context, string, string) error { return nil }

func TestRegistrationNewAndDuplicateResponsesAreIndistinguishable(t *testing.T) {
	t.Parallel()

	newAuth := &authServiceStub{}
	newResponse := performRegistration(t, newAuth)
	duplicateAuth := &authServiceStub{registerErr: httpapi.ErrConflict}
	duplicateResponse := performRegistration(t, duplicateAuth)

	if !newAuth.registered || !duplicateAuth.registered {
		t.Fatal("registration service was not called for both valid requests")
	}
	if newResponse.Code != http.StatusAccepted || duplicateResponse.Code != http.StatusAccepted {
		t.Fatalf("registration statuses = new %d, duplicate %d; want both %d", newResponse.Code, duplicateResponse.Code, http.StatusAccepted)
	}
	const wantBody = `{"message":"If the registration request can be processed, the account workflow will continue."}`
	if newResponse.Body.String() != wantBody {
		t.Fatalf("new registration body = %q, want %q", newResponse.Body.String(), wantBody)
	}
	if !bytes.Equal(newResponse.Body.Bytes(), duplicateResponse.Body.Bytes()) {
		t.Fatalf("registration response differs by account existence: new %q, duplicate %q", newResponse.Body.String(), duplicateResponse.Body.String())
	}
}

func TestRegistrationUnknownServiceErrorRemainsGenericFailure(t *testing.T) {
	t.Parallel()

	auth := &authServiceStub{registerErr: errors.New("database secret must not escape")}
	response := performRegistration(t, auth)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("registration status = %d, want %d; body = %s", response.Code, http.StatusInternalServerError, response.Body.String())
	}
	const wantBody = `{"error":{"code":"internal_error","message":"internal server error"}}`
	if response.Body.String() != wantBody {
		t.Fatalf("registration failure body = %q, want %q", response.Body.String(), wantBody)
	}
}

func TestRegistrationRejectsOverlongDisplayNameBeforeAccountLookup(t *testing.T) {
	t.Parallel()

	var responses []*httptest.ResponseRecorder
	for _, registerErr := range []error{nil, httpapi.ErrConflict} {
		auth := &authServiceStub{registerErr: registerErr}
		router := httpapi.New(httpapi.Dependencies{Auth: auth})
		body := []byte(`{"email":"customer@example.test","password":"correct-horse-battery-staple","display_name":"` + strings.Repeat("界", 101) + `"}`)
		request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)

		if response.Code != http.StatusBadRequest {
			t.Fatalf("overlong registration status = %d, want %d; body = %s", response.Code, http.StatusBadRequest, response.Body.String())
		}
		if auth.registered {
			t.Fatal("overlong registration reached account lookup")
		}
		responses = append(responses, response)
	}
	if !bytes.Equal(responses[0].Body.Bytes(), responses[1].Body.Bytes()) {
		t.Fatalf("overlong response differs by configured account outcome: first %q, second %q", responses[0].Body.String(), responses[1].Body.String())
	}
}

func TestRegistrationRejectsControlRunesBeforeAccountLookup(t *testing.T) {
	t.Parallel()

	var responses []*httptest.ResponseRecorder
	for _, registerErr := range []error{nil, httpapi.ErrConflict} {
		auth := &authServiceStub{registerErr: registerErr}
		router := httpapi.New(httpapi.Dependencies{Auth: auth})
		body := []byte(`{"email":"customer@example.test","password":"correct-horse-battery-staple","display_name":"Rider\u0000"}`)
		request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)

		if response.Code != http.StatusBadRequest {
			t.Fatalf("control-rune registration status = %d, want %d; body = %s", response.Code, http.StatusBadRequest, response.Body.String())
		}
		if auth.registered {
			t.Fatal("control-rune registration reached account lookup")
		}
		responses = append(responses, response)
	}
	if !bytes.Equal(responses[0].Body.Bytes(), responses[1].Body.Bytes()) {
		t.Fatalf("control-rune response differs by configured account outcome: first %q, second %q", responses[0].Body.String(), responses[1].Body.String())
	}
}

func TestRegistrationRejectsInvalidCredentialShapeBeforeAccountLookup(t *testing.T) {
	t.Parallel()

	for name, testCase := range map[string]struct {
		email    string
		password string
	}{
		"password below rune minimum": {email: "customer@example.test", password: strings.Repeat("界", 4)},
		"password above bcrypt bytes": {email: "customer@example.test", password: strings.Repeat("界", 25)},
		"email with two separators":   {email: "customer@@example.test", password: "correct-horse-battery-staple"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, registerErr := range []error{nil, httpapi.ErrConflict} {
				auth := &authServiceStub{registerErr: registerErr}
				router := httpapi.New(httpapi.Dependencies{Auth: auth})
				body, err := json.Marshal(map[string]string{
					"email": testCase.email, "password": testCase.password, "display_name": "Rider",
				})
				if err != nil {
					t.Fatalf("marshal request: %v", err)
				}
				request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader(body))
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				router.ServeHTTP(response, request)

				if response.Code != http.StatusBadRequest {
					t.Fatalf("invalid registration status = %d, want %d; body = %s", response.Code, http.StatusBadRequest, response.Body.String())
				}
				if auth.registered {
					t.Fatal("invalid registration reached account lookup")
				}
			}
		})
	}
}

func performRegistration(t *testing.T, auth *authServiceStub) *httptest.ResponseRecorder {
	t.Helper()

	router := httpapi.New(httpapi.Dependencies{Auth: auth})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader([]byte(`{"email":"customer@example.test","password":"correct-horse-battery-staple","display_name":"Rider"}`)))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}

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
