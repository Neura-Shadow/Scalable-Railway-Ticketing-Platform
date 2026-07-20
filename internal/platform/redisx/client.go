package redisx

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// BoundedClientOptions applies REDIS_TIMEOUT to every Redis I/O boundary and
// removes implicit command retries. One dial attempt avoids the client's
// multi-attempt default outliving an HTTP upstream timeout during an outage.
func BoundedClientOptions(address, password string, timeout time.Duration) *redis.Options {
	return &redis.Options{
		Addr:                  address,
		Password:              password,
		DialTimeout:           timeout,
		DialerRetries:         1,
		ReadTimeout:           timeout,
		WriteTimeout:          timeout,
		PoolTimeout:           timeout,
		MaxRetries:            -1,
		ContextTimeoutEnabled: true,
	}
}
