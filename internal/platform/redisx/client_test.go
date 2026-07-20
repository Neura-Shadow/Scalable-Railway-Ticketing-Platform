package redisx

import (
	"testing"
	"time"
)

func TestBoundedClientOptionsApplyConfiguredTimeoutWithoutImplicitRetries(t *testing.T) {
	t.Parallel()

	const (
		address  = "redis.internal:6379"
		password = "test-password"
	)
	timeout := 750 * time.Millisecond

	options := BoundedClientOptions(address, password, timeout)
	if options.Addr != address || options.Password != password {
		t.Fatalf("connection options = %q/%q", options.Addr, options.Password)
	}
	if options.DialTimeout != timeout ||
		options.ReadTimeout != timeout ||
		options.WriteTimeout != timeout ||
		options.PoolTimeout != timeout {
		t.Fatalf(
			"timeouts = dial:%s read:%s write:%s pool:%s, want %s",
			options.DialTimeout,
			options.ReadTimeout,
			options.WriteTimeout,
			options.PoolTimeout,
			timeout,
		)
	}
	if options.MaxRetries != -1 {
		t.Fatalf("MaxRetries = %d, want -1 to disable implicit retries", options.MaxRetries)
	}
	if options.DialerRetries != 1 {
		t.Fatalf("DialerRetries = %d, want 1 bounded dial attempt", options.DialerRetries)
	}
	if !options.ContextTimeoutEnabled {
		t.Fatal("ContextTimeoutEnabled = false, want true")
	}
}
