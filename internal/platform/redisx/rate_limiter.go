package redisx

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrInvalidRateLimit   = errors.New("invalid rate limit configuration")
	ErrRateLimiterBackend = errors.New("rate limiter backend unavailable")
)

const counterScript = `
local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('PTTL', KEYS[1])
return {current, ttl}
`

type RateLimit struct {
	Limit  int64
	Window time.Duration
}

type RateLimitResult struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration
}

type RateLimiter struct {
	client    redis.UniversalClient
	namespace string
	script    *redis.Script
}

func NewRateLimiter(client redis.UniversalClient, namespace string) (*RateLimiter, error) {
	namespace = strings.TrimSpace(namespace)
	if client == nil || namespace == "" {
		return nil, ErrInvalidRateLimit
	}
	return &RateLimiter{client: client, namespace: namespace, script: redis.NewScript(counterScript)}, nil
}

// Allow uses one atomic Lua operation. subject is hashed before becoming a
// Redis key, so email addresses, user IDs, JWTs, and other raw identifiers are
// not exposed through key enumeration or operational diagnostics.
func (l *RateLimiter) Allow(ctx context.Context, operation, subject string, limit RateLimit) (RateLimitResult, error) {
	operation = normalizeOperation(operation)
	if operation == "" || subject == "" || limit.Limit <= 0 || limit.Window <= 0 {
		return RateLimitResult{}, ErrInvalidRateLimit
	}
	digest := sha256.Sum256([]byte(subject))
	key := fmt.Sprintf("ratelimit:%s:v1:%s:%s", l.namespace, operation, hex.EncodeToString(digest[:]))
	value, err := l.script.Run(ctx, l.client, []string{key}, limit.Window.Milliseconds()).Slice()
	if err != nil {
		return RateLimitResult{}, fmt.Errorf("%w: %v", ErrRateLimiterBackend, err)
	}
	if len(value) != 2 {
		return RateLimitResult{}, ErrRateLimiterBackend
	}
	current, okCurrent := value[0].(int64)
	ttlMilliseconds, okTTL := value[1].(int64)
	if !okCurrent || !okTTL {
		return RateLimitResult{}, ErrRateLimiterBackend
	}
	remaining := limit.Limit - current
	if remaining < 0 {
		remaining = 0
	}
	result := RateLimitResult{Allowed: current <= limit.Limit, Remaining: remaining}
	if !result.Allowed && ttlMilliseconds > 0 {
		result.RetryAfter = time.Duration(ttlMilliseconds) * time.Millisecond
	}
	return result, nil
}

func normalizeOperation(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "login", "register", "reservation_create", "passenger_create", "hot_train_policy_mutation":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return ""
	}
}
