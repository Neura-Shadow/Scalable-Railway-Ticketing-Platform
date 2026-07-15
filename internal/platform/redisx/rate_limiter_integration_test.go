package redisx

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestRateLimiterIntegrationUsesAtomicWindowAndHashedSubject(t *testing.T) {
	address := os.Getenv("REDIS_ADDR")
	if address == "" {
		t.Skip("REDIS_ADDR is not set; skipping Redis integration test")
	}
	client := redis.NewClient(&redis.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })

	namespace := "integration-" + uuid.NewString()
	limiter, err := NewRateLimiter(client, namespace)
	if err != nil {
		t.Fatal(err)
	}
	const rawSubject = "customer@example.test"
	for attempt := 1; attempt <= 3; attempt++ {
		result, err := limiter.Allow(context.Background(), "login", rawSubject, RateLimit{Limit: 2, Window: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		if got, want := result.Allowed, attempt <= 2; got != want {
			t.Fatalf("attempt %d allowed=%t, want %t", attempt, got, want)
		}
	}
	if result, err := limiter.Allow(context.Background(), "passenger_create", "customer-id", RateLimit{Limit: 12, Window: time.Hour}); err != nil || !result.Allowed {
		t.Fatalf("passenger_create policy integration = %#v, %v", result, err)
	}

	keys, _, err := client.Scan(context.Background(), 0, "ratelimit:"+namespace+":v1:*", 10).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || strings.Contains(strings.Join(keys, "|"), rawSubject) || strings.Contains(strings.Join(keys, "|"), "customer-id") {
		t.Fatalf("rate limit keys = %q; expected two hashed keys with no raw subject", keys)
	}
	_ = client.Del(context.Background(), keys...).Err()
}
