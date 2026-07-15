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

	keys, _, err := client.Scan(context.Background(), 0, "ratelimit:"+namespace+":v1:*", 10).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || strings.Contains(keys[0], rawSubject) {
		t.Fatalf("rate limit keys = %q; expected one hashed key with no raw subject", keys)
	}
	_ = client.Del(context.Background(), keys...).Err()
}
