// Package postgres resolves logical booking routes from the public control
// catalog and establishes schema-local PostgreSQL transactions without
// exposing SQL identifiers to callers.
package postgres

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// DB is the small pgx-compatible seam required by Router. *pgxpool.Pool
// satisfies it directly.
type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// ResolveReservation resolves exactly one global locator. It never probes a
// shard and locator data is revalidated against the assignment by routed
// transaction creation.
func (router *Router) ResolveReservation(ctx context.Context, reservationID uuid.UUID) (sharding.ShardRoute, error) {
	if reservationID == uuid.Nil {
		return sharding.ShardRoute{}, sharding.ErrLocatorNotFound
	}
	return router.resolveLocator(ctx, reservationID, `
SELECT train_run_id, shard_id, assignment_generation
FROM public.reservation_shard_locators
WHERE reservation_id = $1`)
}

// ResolveTicketOrder resolves one globally indexed order without shard fanout.
func (router *Router) ResolveTicketOrder(ctx context.Context, ticketOrderID uuid.UUID) (sharding.ShardRoute, error) {
	if ticketOrderID == uuid.Nil {
		return sharding.ShardRoute{}, sharding.ErrLocatorNotFound
	}
	return router.resolveLocator(ctx, ticketOrderID, `
SELECT train_run_id, shard_id, assignment_generation
FROM public.ticket_order_shard_locators
WHERE ticket_order_id = $1`)
}

// ResolveTicket resolves one globally indexed ticket without shard fanout.
func (router *Router) ResolveTicket(ctx context.Context, ticketID uuid.UUID) (sharding.ShardRoute, error) {
	if ticketID == uuid.Nil {
		return sharding.ShardRoute{}, sharding.ErrLocatorNotFound
	}
	return router.resolveLocator(ctx, ticketID, `
SELECT train_run_id, shard_id, assignment_generation
FROM public.ticket_shard_locators
WHERE ticket_id = $1`)
}

func (router *Router) resolveLocator(ctx context.Context, resourceID uuid.UUID, query string) (sharding.ShardRoute, error) {
	started := time.Now()
	metricShardID := "unknown"
	var resultErr error
	defer func() { router.observeRoute(ctx, "resolve", metricShardID, started, resultErr) }()
	if router == nil || router.db == nil {
		resultErr = sharding.ErrShardUnavailable
		return sharding.ShardRoute{}, resultErr
	}
	ctx, cancel := router.boundedQueryContext(ctx)
	defer cancel()
	var trainRunID uuid.UUID
	var rawShardID string
	var rawGeneration int64
	if err := router.db.QueryRow(ctx, query, resourceID).Scan(&trainRunID, &rawShardID, &rawGeneration); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			resultErr = sharding.ErrLocatorNotFound
			return sharding.ShardRoute{}, resultErr
		}
		resultErr = sharding.ErrShardUnavailable
		return sharding.ShardRoute{}, resultErr
	}
	shardID, err := sharding.ParseShardID(rawShardID)
	if err != nil {
		resultErr = sharding.ErrShardUnavailable
		return sharding.ShardRoute{}, resultErr
	}
	metricShardID = shardID.String()
	generation, err := sharding.NewAssignmentGeneration(rawGeneration)
	if err != nil {
		resultErr = sharding.ErrShardUnavailable
		return sharding.ShardRoute{}, resultErr
	}
	route, err := sharding.NewShardRoute(trainRunID, shardID, generation)
	if err != nil {
		resultErr = sharding.ErrShardUnavailable
		return sharding.ShardRoute{}, resultErr
	}
	return route, nil
}

// RouteCache is optional and advisory. The PostgreSQL assignment remains the
// authority checked by every routed transaction.
type RouteCache interface {
	Get(uuid.UUID) (sharding.ShardRoute, bool)
	Put(sharding.ShardRoute) error
	Invalidate(uuid.UUID)
}

// Router owns catalog lookups and all SQL schema selection.
type Router struct {
	db           DB
	cache        RouteCache
	metrics      Metrics
	queryTimeout time.Duration
}

func NewRouter(db DB, cache RouteCache, options ...Option) (*Router, error) {
	if isNilInterface(db) {
		return nil, sharding.ErrShardUnavailable
	}
	if isNilInterface(cache) {
		cache = nil
	}
	router := &Router{db: db, cache: cache}
	for _, option := range options {
		if option != nil {
			option(router)
		}
	}
	return router, nil
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// ResolveTrainRun returns an opaque fixed route. A cache hit is only a hint;
// BeginTrainRunRead and BeginTrainRunWrite revalidate it transactionally.
func (router *Router) ResolveTrainRun(ctx context.Context, trainRunID uuid.UUID) (route sharding.ShardRoute, resultErr error) {
	started := time.Now()
	metricShardID := "unknown"
	defer func() {
		if validRoute(route) {
			metricShardID = boundedShardID(route.ShardID())
		}
		router.observeRoute(ctx, "resolve", metricShardID, started, resultErr)
	}()
	if router == nil || router.db == nil || trainRunID == uuid.Nil {
		return sharding.ShardRoute{}, sharding.ErrShardUnavailable
	}
	ctx, cancel := router.boundedQueryContext(ctx)
	defer cancel()
	if router.cache != nil {
		if route, ok := router.cache.Get(trainRunID); ok {
			if validRoute(route) && route.TrainRunID() == trainRunID {
				if router.metrics != nil {
					router.metrics.RecordShardRouteCache("hit", "cache_hit", boundedShardID(route.ShardID()))
				}
				return route, nil
			}
			router.cache.Invalidate(trainRunID)
		}
		if router.metrics != nil {
			router.metrics.RecordShardRouteCache("miss", "cache_miss", "unknown")
		}
	}

	return router.loadTrainRun(ctx, trainRunID)
}

// RefreshTrainRun evicts one advisory observation and reloads the public
// assignment. It is the only refresh path used by a bounded stale retry.
func (router *Router) RefreshTrainRun(ctx context.Context, trainRunID uuid.UUID) (route sharding.ShardRoute, resultErr error) {
	started := time.Now()
	defer func() {
		shardID := "unknown"
		if validRoute(route) {
			shardID = boundedShardID(route.ShardID())
		}
		if router != nil && router.metrics != nil {
			result, _ := routeMetricOutcome(ctx, resultErr)
			router.metrics.RecordShardRouteRefresh("refresh", result, shardID)
		}
		router.observeRoute(ctx, "refresh", shardID, started, resultErr)
	}()
	if router == nil || router.db == nil || trainRunID == uuid.Nil {
		return sharding.ShardRoute{}, sharding.ErrShardUnavailable
	}
	ctx, cancel := router.boundedQueryContext(ctx)
	defer cancel()
	if router.cache != nil {
		router.cache.Invalidate(trainRunID)
	}
	return router.loadTrainRun(ctx, trainRunID)
}

// ListEnabledShards returns a fixed-size, allowlisted workset for bounded
// workers and operator fanout. Corrupted or expanded topology fails closed.
func (router *Router) ListEnabledShards(ctx context.Context) ([]sharding.ShardID, error) {
	if router == nil || router.db == nil {
		return nil, sharding.ErrShardUnavailable
	}
	ctx, cancel := router.boundedQueryContext(ctx)
	defer cancel()
	rows, err := router.db.Query(ctx, `
SELECT shard_id, minimum_fencing_protocol_version
FROM public.booking_shards
WHERE enabled AND state <> 'disabled'
ORDER BY CASE shard_id
    WHEN 'legacy' THEN 0
    WHEN 'shard-0' THEN 1
    WHEN 'shard-1' THEN 2
    ELSE 3
END`)
	if err != nil {
		return nil, sharding.ErrShardUnavailable
	}
	defer rows.Close()

	result := make([]sharding.ShardID, 0, 3)
	seen := make(map[sharding.ShardID]struct{}, 3)
	for rows.Next() {
		var rawShardID string
		var minimumFencingProtocolVersion int32
		if err := rows.Scan(&rawShardID, &minimumFencingProtocolVersion); err != nil {
			return nil, sharding.ErrShardUnavailable
		}
		shardID, err := sharding.ParseShardID(rawShardID)
		if err != nil || len(result) == 3 || minimumFencingProtocolVersion <= 0 ||
			minimumFencingProtocolVersion > sharding.SupportedFencingProtocolVersion {
			return nil, sharding.ErrShardUnavailable
		}
		if _, duplicate := seen[shardID]; duplicate {
			return nil, sharding.ErrShardUnavailable
		}
		seen[shardID] = struct{}{}
		result = append(result, shardID)
	}
	if rows.Err() != nil || len(result) == 0 {
		return nil, sharding.ErrShardUnavailable
	}
	return result, nil
}

func (router *Router) loadTrainRun(ctx context.Context, trainRunID uuid.UUID) (sharding.ShardRoute, error) {
	var rawShardID string
	var rawGeneration int64
	var enabled bool
	var state string
	var minimumFencingProtocolVersion int32
	if err := router.db.QueryRow(ctx, `
SELECT assignment.shard_id,
       assignment.assignment_generation,
       shard.enabled,
       shard.state,
       shard.minimum_fencing_protocol_version
FROM public.train_run_shard_assignments AS assignment
JOIN public.booking_shards AS shard ON shard.shard_id = assignment.shard_id
WHERE assignment.train_run_id = $1`, trainRunID).Scan(
		&rawShardID,
		&rawGeneration,
		&enabled,
		&state,
		&minimumFencingProtocolVersion,
	); err != nil {
		return sharding.ShardRoute{}, sharding.ErrShardUnavailable
	}
	if !enabled || !catalogStateAllowsRoute(state) || minimumFencingProtocolVersion <= 0 ||
		minimumFencingProtocolVersion > sharding.SupportedFencingProtocolVersion {
		return sharding.ShardRoute{}, sharding.ErrShardUnavailable
	}
	shardID, err := sharding.ParseShardID(rawShardID)
	if err != nil {
		return sharding.ShardRoute{}, sharding.ErrShardUnavailable
	}
	generation, err := sharding.NewAssignmentGeneration(rawGeneration)
	if err != nil {
		return sharding.ShardRoute{}, sharding.ErrShardUnavailable
	}
	route, err := sharding.NewShardRoute(trainRunID, shardID, generation)
	if err != nil {
		return sharding.ShardRoute{}, sharding.ErrShardUnavailable
	}
	if router.cache != nil {
		// The cache is advisory only. A cache write failure cannot override a
		// validated PostgreSQL assignment or make routing unavailable.
		_ = router.cache.Put(route)
	}
	return route, nil
}
