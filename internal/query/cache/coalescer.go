package cache

import (
	"context"
	"errors"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"
)

var ErrInvalidCoalescerKey = errors.New("invalid cache singleflight key")

const maxCoalescedFillDuration = 30 * time.Second

type Coalescer struct {
	group singleflight.Group
}

func (coalescer *Coalescer) Do(
	ctx context.Context,
	key string,
	fill func(context.Context) (any, error),
) (any, error, bool) {
	if key != strings.TrimSpace(key) || len(key) < 1 || len(key) > 512 || fill == nil {
		return nil, ErrInvalidCoalescerKey, false
	}
	if err := ctx.Err(); err != nil {
		return nil, err, false
	}
	result := coalescer.group.DoChan(key, func() (any, error) {
		fillContext, cancel := boundedCoalescedFillContext(ctx)
		defer cancel()
		return fill(fillContext)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err(), false
	case completed := <-result:
		return completed.Val, completed.Err, completed.Shared
	}
}

func boundedCoalescedFillContext(caller context.Context) (context.Context, context.CancelFunc) {
	base := context.WithoutCancel(caller)
	maximumDeadline := time.Now().Add(maxCoalescedFillDuration)
	if callerDeadline, ok := caller.Deadline(); ok && callerDeadline.Before(maximumDeadline) {
		return context.WithDeadline(base, callerDeadline)
	}
	return context.WithDeadline(base, maximumDeadline)
}
