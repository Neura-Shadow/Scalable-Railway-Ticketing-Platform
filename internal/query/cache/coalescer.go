package cache

import (
	"context"
	"errors"
	"strings"

	"golang.org/x/sync/singleflight"
)

var ErrInvalidCoalescerKey = errors.New("invalid cache singleflight key")

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
	return coalescer.group.Do(key, func() (any, error) { return fill(ctx) })
}
