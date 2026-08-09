package worker

import (
	"encoding/binary"
	"time"

	"github.com/google/uuid"
)

// retryDelay uses bounded exponential backoff plus deterministic jitter. The
// deterministic seed makes RunOnce tests stable while spreading replicas.
func retryDelay(id uuid.UUID, attempt int, base, maximum time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := base
	for count := 1; count < attempt && delay < maximum; count++ {
		if delay > maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if delay >= maximum {
		return maximum
	}
	window := delay / 4
	if window <= 0 {
		return delay
	}
	seed := binary.BigEndian.Uint64(id[:8]) ^ uint64(attempt)
	jitter := time.Duration(seed % uint64(window+1))
	if delay > maximum-jitter {
		return maximum
	}
	return delay + jitter
}
