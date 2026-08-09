package worker

import (
	"context"
	"errors"
	"time"
)

type RunOnceFunc func(context.Context) (Result, error)

// RunLoop performs an initial pass, then one pass per interval. It owns one
// timer, starts no child goroutines, and returns promptly after cancellation.
func RunLoop(ctx context.Context, interval time.Duration, runOnce RunOnceFunc, observe func(Result, error)) error {
	if ctx == nil || interval <= 0 || runOnce == nil {
		return ErrInvalidConfiguration
	}
	run := func() {
		result, err := runOnce(ctx)
		if observe != nil {
			observe(result, err)
		}
	}
	run()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return ctx.Err()
		case <-ticker.C:
			run()
		}
	}
}
