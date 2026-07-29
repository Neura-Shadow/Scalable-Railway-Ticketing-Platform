package app

import (
	"context"
	"strings"
	"time"

	accountsapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/application"
	accountsdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/redisx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

type accessTokenParser interface {
	ParseAccessToken(context.Context, string) (accountsapp.TokenClaims, error)
}

// BearerTokenParser adapts verified account JWT claims to the transport's
// deliberately small identity contract.
type BearerTokenParser struct {
	parser accessTokenParser
}

func NewBearerTokenParser(parser accessTokenParser) *BearerTokenParser {
	return &BearerTokenParser{parser: parser}
}

func (p *BearerTokenParser) ParseAccessToken(ctx context.Context, raw string) (httpapi.Identity, error) {
	if p == nil || p.parser == nil || strings.TrimSpace(raw) == "" {
		return httpapi.Identity{}, httpapi.ErrUnauthenticated
	}
	claims, err := p.parser.ParseAccessToken(ctx, raw)
	if err != nil || strings.TrimSpace(claims.Subject) == "" {
		return httpapi.Identity{}, httpapi.ErrUnauthenticated
	}
	role, ok := transportRole(claims.Role)
	if !ok {
		return httpapi.Identity{}, httpapi.ErrUnauthenticated
	}
	return httpapi.Identity{Subject: claims.Subject, Role: role}, nil
}

func transportRole(role accountsdomain.Role) (httpapi.Role, bool) {
	switch role {
	case accountsdomain.RoleCustomer:
		return httpapi.RoleCustomer, true
	case accountsdomain.RoleAdmin:
		return httpapi.RoleAdmin, true
	case accountsdomain.RoleOperator:
		return httpapi.RoleOperator, true
	default:
		return "", false
	}
}

type rateLimitBackend interface {
	Allow(context.Context, string, string, redisx.RateLimit) (redisx.RateLimitResult, error)
}

// RateLimiter owns the bounded public rate-limit policy. The Redis backend
// performs the counter update atomically with Lua and hashes the raw subject.
type RateLimiter struct {
	backend rateLimitBackend
}

func NewRateLimiter(backend rateLimitBackend) *RateLimiter {
	return &RateLimiter{backend: backend}
}

func (l *RateLimiter) Allow(ctx context.Context, request httpapi.RateLimitRequest) (bool, error) {
	operation, limit, ok := rateLimitPolicy(request.Scope)
	if !ok || strings.TrimSpace(request.Key) == "" {
		return false, httpapi.ErrInvalidInput
	}
	if l == nil || l.backend == nil {
		return true, nil
	}
	result, err := l.backend.Allow(ctx, operation, request.Key, limit)
	if err != nil {
		// The transport owns the scope-specific outage policy: authentication
		// and policy mutation fail closed while the ordinary non-hot
		// reservation limiter may fail open.
		return false, err
	}
	return result.Allowed, nil
}

func rateLimitPolicy(scope httpapi.RateLimitScope) (string, redisx.RateLimit, bool) {
	switch scope {
	case httpapi.RateLimitRegister:
		return "register", redisx.RateLimit{Limit: 5, Window: 15 * time.Minute}, true
	case httpapi.RateLimitLogin:
		return "login", redisx.RateLimit{Limit: 10, Window: 15 * time.Minute}, true
	case httpapi.RateLimitReservationCreate:
		return "reservation_create", redisx.RateLimit{Limit: 10, Window: time.Minute}, true
	case httpapi.RateLimitPassengerCreate:
		return "passenger_create", redisx.RateLimit{Limit: 12, Window: time.Hour}, true
	case httpapi.RateLimitPolicyMutation:
		return "hot_train_policy_mutation", redisx.RateLimit{Limit: 20, Window: time.Hour}, true
	case httpapi.RateLimitOperatorBooking:
		return "operator_booking_mutation", redisx.RateLimit{Limit: 120, Window: time.Hour}, true
	default:
		return "", redisx.RateLimit{}, false
	}
}

var (
	_ httpapi.BearerTokenParser = (*BearerTokenParser)(nil)
	_ httpapi.RateLimiter       = (*RateLimiter)(nil)
)
