package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
)

// Metrics is an optional bounded observability seam. Implementations must
// normalize labels again at the exporter boundary; Router supplies only the
// fixed vocabulary below and never supplies resource identifiers.
type Metrics interface {
	RecordShardRoute(operation, result, reason, shardID string, duration time.Duration)
	RecordShardRouteCache(result, reason, shardID string)
	RecordShardRouteRefresh(operation, result, shardID string)
	RecordShardAssignmentStale(operation, shardID string)
	RecordShardWriteFenceRejected(operation, reason, shardID string)
	RecordShardUnavailable(operation, reason, shardID string)
}

type Option func(*Router)

// WithMetrics installs an optional bounded metrics recorder.
func WithMetrics(metrics Metrics) Option {
	return func(router *Router) {
		if !isNilInterface(metrics) {
			router.metrics = metrics
		}
	}
}

// WithQueryTimeout bounds catalog lookups and routed transaction setup. It
// does not shorten the caller context used by the booking mutation after the
// validated transaction has been returned.
func WithQueryTimeout(timeout time.Duration) Option {
	return func(router *Router) {
		if timeout > 0 {
			router.queryTimeout = timeout
		}
	}
}

func (router *Router) boundedQueryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if router == nil || router.queryTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, router.queryTimeout)
}

func boundedShardID(shardID sharding.ShardID) string {
	parsed, err := sharding.ParseShardID(shardID.String())
	if err != nil {
		return "unknown"
	}
	return parsed.String()
}

func routeMetricOutcome(ctx context.Context, err error) (string, string) {
	if err == nil {
		return "success", "none"
	}
	if ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "unavailable", "timeout"
	}
	if ctx != nil && errors.Is(ctx.Err(), context.Canceled) {
		return "unavailable", "cancelled"
	}
	switch {
	case errors.Is(err, sharding.ErrAssignmentStale):
		return "stale", "stale_generation"
	case errors.Is(err, sharding.ErrWriteFenced):
		return "rejected", "write_disabled"
	case errors.Is(err, sharding.ErrTrainRunMigrating):
		return "rejected", "migration"
	case errors.Is(err, sharding.ErrLocatorNotFound):
		return "failure", "not_found"
	default:
		return "unavailable", "database"
	}
}

func (router *Router) observeRoute(ctx context.Context, operation, shardID string, started time.Time, err error) {
	if router == nil || router.metrics == nil {
		return
	}
	result, reason := routeMetricOutcome(ctx, err)
	router.metrics.RecordShardRoute(operation, result, reason, shardID, time.Since(started))
	switch {
	case errors.Is(err, sharding.ErrAssignmentStale):
		router.metrics.RecordShardAssignmentStale(operation, shardID)
	case errors.Is(err, sharding.ErrWriteFenced):
		router.metrics.RecordShardWriteFenceRejected(operation, reason, shardID)
	case err != nil && !errors.Is(err, sharding.ErrLocatorNotFound) && !errors.Is(err, sharding.ErrTrainRunMigrating):
		router.metrics.RecordShardUnavailable(operation, reason, shardID)
	}
}
