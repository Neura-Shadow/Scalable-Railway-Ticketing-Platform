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
