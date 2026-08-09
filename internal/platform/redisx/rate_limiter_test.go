package redisx

import (
	"strings"
	"testing"
)

func TestRateLimiterLuaCounterIsAtomicAndBounded(t *testing.T) {
	t.Parallel()

	source := counterScript
	for _, required := range []string{"INCR", "PEXPIRE", "current == 1"} {
		if !strings.Contains(source, required) {
			t.Fatalf("counter script missing %q", required)
		}
	}
	if strings.Contains(strings.ToUpper(source), "REDIS.CALL('KEYS'") {
		t.Fatal("rate limiter must not use the Redis KEYS command")
	}
}

func TestRateLimiterOperationAllowlistIncludesEveryTransportPolicy(t *testing.T) {
	for _, operation := range []string{
		"register",
		"login",
		"reservation_create",
		"passenger_create",
		"hot_train_policy_mutation",
		"operator_booking_mutation",
		"payment_webhook",
	} {
		if normalized := normalizeOperation(operation); normalized != operation {
			t.Fatalf("normalizeOperation(%q) = %q", operation, normalized)
		}
	}
}
