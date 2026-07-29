// Package workerlane runs independent worker storage lanes without allowing
// one backlog or outage to consume the other lane's entire pass budget.
package workerlane

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrInvalidConfig = errors.New("invalid isolated worker lane configuration")

type Outcome[C, P any] struct {
	Control     C
	Physical    P
	ControlErr  error
	PhysicalErr error
}

// Run starts both lanes together. Each lane receives its own deadline below
// the outer pass context, so a slow control database cannot starve healthy
// physical shards and a failed shard cannot starve the control lane.
func Run[C, P any](
	ctx context.Context,
	controlTimeout time.Duration,
	physicalTimeout time.Duration,
	control func(context.Context) (C, error),
	physical func(context.Context) (P, error),
) (Outcome[C, P], error) {
	if ctx == nil || controlTimeout <= 0 || physicalTimeout <= 0 || control == nil {
		return Outcome[C, P]{}, ErrInvalidConfig
	}
	var outcome Outcome[C, P]
	var wait sync.WaitGroup
	run := func(timeout time.Duration, operation func(context.Context), target *error) {
		wait.Add(1)
		go func() {
			defer wait.Done()
			laneCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			operation(laneCtx)
			if *target == nil && laneCtx.Err() != nil {
				*target = laneCtx.Err()
			}
		}()
	}
	run(controlTimeout, func(laneCtx context.Context) {
		outcome.Control, outcome.ControlErr = control(laneCtx)
	}, &outcome.ControlErr)
	if physical != nil {
		run(physicalTimeout, func(laneCtx context.Context) {
			outcome.Physical, outcome.PhysicalErr = physical(laneCtx)
		}, &outcome.PhysicalErr)
	}
	wait.Wait()
	return outcome, nil
}
