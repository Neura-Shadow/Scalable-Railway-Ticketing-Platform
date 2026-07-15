package app

import (
	"context"
	"errors"
	"testing"
	"time"

	accountsapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/application"
	accountsdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/redisx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

type accessTokenParserFake struct {
	claims accountsapp.TokenClaims
	err    error
	raw    string
}

func (f *accessTokenParserFake) ParseAccessToken(_ context.Context, raw string) (accountsapp.TokenClaims, error) {
	f.raw = raw
	return f.claims, f.err
}

func TestBearerTokenParserMapsVerifiedClaimsToTransportIdentity(t *testing.T) {
	backend := &accessTokenParserFake{claims: accountsapp.TokenClaims{Role: accountsdomain.RoleOperator}}
	backend.claims.Subject = "operator-42"
	parser := NewBearerTokenParser(backend)

	identity, err := parser.ParseAccessToken(context.Background(), "signed-token")
	if err != nil {
		t.Fatalf("ParseAccessToken() error = %v", err)
	}
	if backend.raw != "signed-token" {
		t.Fatalf("raw token = %q", backend.raw)
	}
	if identity != (httpapi.Identity{Subject: "operator-42", Role: httpapi.RoleOperator}) {
		t.Fatalf("identity = %#v", identity)
	}
}

func TestBearerTokenParserReturnsOnlySafeAuthenticationErrors(t *testing.T) {
	for _, test := range []struct {
		name    string
		backend *accessTokenParserFake
	}{
		{name: "JWT rejected", backend: &accessTokenParserFake{err: errors.New("signature details")}},
		{name: "unknown role", backend: &accessTokenParserFake{claims: accountsapp.TokenClaims{Role: accountsdomain.Role("root")}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.backend.claims.Subject = "subject"
			_, err := NewBearerTokenParser(test.backend).ParseAccessToken(context.Background(), "token")
			if err != httpapi.ErrUnauthenticated {
				t.Fatalf("error = %v, want exact safe sentinel", err)
			}
		})
	}
}

type rateLimitBackendFake struct {
	operation string
	subject   string
	limit     redisx.RateLimit
	result    redisx.RateLimitResult
	err       error
}

func (f *rateLimitBackendFake) Allow(_ context.Context, operation, subject string, limit redisx.RateLimit) (redisx.RateLimitResult, error) {
	f.operation, f.subject, f.limit = operation, subject, limit
	return f.result, f.err
}

func TestRateLimiterMapsScopesToBoundedLuaPolicies(t *testing.T) {
	tests := []struct {
		scope     httpapi.RateLimitScope
		operation string
		limit     int64
		window    time.Duration
	}{
		{httpapi.RateLimitRegister, "register", 5, 15 * time.Minute},
		{httpapi.RateLimitLogin, "login", 10, 15 * time.Minute},
		{httpapi.RateLimitReservationCreate, "reservation_create", 10, time.Minute},
	}
	for _, test := range tests {
		t.Run(string(test.scope), func(t *testing.T) {
			backend := &rateLimitBackendFake{result: redisx.RateLimitResult{Allowed: true}}
			allowed, err := NewRateLimiter(backend).Allow(context.Background(), httpapi.RateLimitRequest{Scope: test.scope, Key: "subject"})
			if err != nil || !allowed {
				t.Fatalf("Allow() = %v, %v", allowed, err)
			}
			if backend.operation != test.operation || backend.subject != "subject" {
				t.Fatalf("backend input = %q, %q", backend.operation, backend.subject)
			}
			if backend.limit != (redisx.RateLimit{Limit: test.limit, Window: test.window}) {
				t.Fatalf("policy = %#v", backend.limit)
			}
		})
	}
}

func TestRateLimiterFailsOpenOnBackendOutageButDeniesExceededLimit(t *testing.T) {
	outage := &rateLimitBackendFake{err: redisx.ErrRateLimiterBackend}
	allowed, err := NewRateLimiter(outage).Allow(context.Background(), httpapi.RateLimitRequest{Scope: httpapi.RateLimitLogin, Key: "subject"})
	if err != nil || !allowed {
		t.Fatalf("backend outage should fail open, got %v, %v", allowed, err)
	}

	exceeded := &rateLimitBackendFake{result: redisx.RateLimitResult{Allowed: false}}
	allowed, err = NewRateLimiter(exceeded).Allow(context.Background(), httpapi.RateLimitRequest{Scope: httpapi.RateLimitLogin, Key: "subject"})
	if err != nil || allowed {
		t.Fatalf("exceeded limit should deny, got %v, %v", allowed, err)
	}
}

func TestRateLimiterRejectsUnknownScopeWithoutCallingBackend(t *testing.T) {
	backend := &rateLimitBackendFake{result: redisx.RateLimitResult{Allowed: true}}
	allowed, err := NewRateLimiter(backend).Allow(context.Background(), httpapi.RateLimitRequest{Scope: "unknown", Key: "subject"})
	if allowed || err != httpapi.ErrInvalidInput {
		t.Fatalf("Allow() = %v, %v", allowed, err)
	}
	if backend.operation != "" {
		t.Fatal("backend was called")
	}
}
