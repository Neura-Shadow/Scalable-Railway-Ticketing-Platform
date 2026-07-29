package physical

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	maxCatalogRouteTTL      = 5 * time.Minute
	maxCachedPhysicalRoutes = 4096
)

// CatalogReader is the read-only control-plane seam used by CatalogRouter.
// The query returns only bounded routing metadata; connection secrets live in
// the immutable Registry and can never be introduced by a catalog row.
type CatalogReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Resolution binds one observed control-plane assignment to an allowlisted
// physical connection handle.
type Resolution struct {
	Route  sharding.ShardRoute
	Handle Handle
}

type cachedResolution struct {
	value     Resolution
	expiresAt time.Time
}

// CatalogRouter keeps a short-lived, bounded cache of train-run assignments.
// A caller that encounters a database-local stale fence can force exactly one
// control-plane refresh without allowing the catalog to choose a network
// endpoint.
type CatalogRouter struct {
	db       CatalogReader
	registry *Registry
	ttl      time.Duration
	now      func() time.Time
	metrics  Metrics

	mu    sync.RWMutex
	cache map[uuid.UUID]cachedResolution
}

func NewCatalogRouter(db CatalogReader, registry *Registry, ttl time.Duration, options ...RouterOption) (*CatalogRouter, error) {
	if nilCatalogReader(db) || registry == nil || ttl <= 0 || ttl > maxCatalogRouteTTL {
		return nil, ErrInvalidRegistry
	}
	router := &CatalogRouter{
		db: db, registry: registry, ttl: ttl, now: time.Now,
		cache: make(map[uuid.UUID]cachedResolution),
	}
	for _, option := range options {
		if option != nil {
			option(router)
		}
	}
	return router, nil
}

func (router *CatalogRouter) Resolve(ctx context.Context, trainRunID uuid.UUID, forceRefresh bool) (Resolution, error) {
	if router == nil || ctx == nil || trainRunID == uuid.Nil {
		return Resolution{}, sharding.ErrShardUnavailable
	}
	started := router.now()
	if !forceRefresh {
		router.mu.RLock()
		cached, found := router.cache[trainRunID]
		router.mu.RUnlock()
		if found && router.now().Before(cached.expiresAt) {
			router.observeResolve(forceRefresh, cached.value, "success", "none", started)
			return cached.value, nil
		}
	}

	resolved, reason, err := router.load(ctx, trainRunID)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			reason = "timeout"
		}
		router.observeResolve(forceRefresh, resolved, "unavailable", reason, started)
		return Resolution{}, err
	}
	router.mu.Lock()
	if len(router.cache) >= maxCachedPhysicalRoutes {
		router.evictOneLocked()
	}
	router.cache[trainRunID] = cachedResolution{value: resolved, expiresAt: router.now().Add(router.ttl)}
	router.mu.Unlock()
	router.observeResolve(forceRefresh, resolved, "success", "none", started)
	return resolved, nil
}

func (router *CatalogRouter) load(ctx context.Context, trainRunID uuid.UUID) (Resolution, string, error) {
	var (
		rawShardID                    string
		rawGeneration                 int64
		rawStorageKind                string
		connectionRef                 string
		protocolVersion               int32
		schemaVersion                 int32
		enabled                       bool
		writeEnabled                  bool
		rawHealthState                string
		rawCatalogState               string
		minimumFencingProtocolVersion int32
	)
	err := router.db.QueryRow(ctx, `
SELECT assignment.shard_id,
       assignment.assignment_generation,
       shard.storage_kind,
       shard.connection_ref,
       shard.protocol_version,
       shard.schema_version,
       shard.enabled,
       shard.write_enabled,
       shard.health_state,
       shard.state,
       shard.minimum_fencing_protocol_version
FROM public.train_run_shard_assignments AS assignment
JOIN public.booking_shards AS shard
  ON shard.shard_id = assignment.shard_id
WHERE assignment.train_run_id = $1
  AND assignment.assignment_state IN ('stable', 'migrating', 'rollback_window')`, trainRunID).Scan(
		&rawShardID,
		&rawGeneration,
		&rawStorageKind,
		&connectionRef,
		&protocolVersion,
		&schemaVersion,
		&enabled,
		&writeEnabled,
		&rawHealthState,
		&rawCatalogState,
		&minimumFencingProtocolVersion,
	)
	if err != nil {
		reason := "database"
		if errors.Is(err, pgx.ErrNoRows) {
			reason = "catalog"
		}
		return Resolution{}, reason, sharding.ErrShardUnavailable
	}
	shardID, err := sharding.ParseShardID(rawShardID)
	if err != nil || (shardID != sharding.ShardPhysicalZero && shardID != sharding.ShardPhysicalOne) {
		return Resolution{}, "catalog", sharding.ErrShardUnavailable
	}
	failed := Resolution{Handle: Handle{shardID: shardID}}
	if StorageKind(rawStorageKind) == StoragePostgres {
		failed.Handle.storageKind = StoragePostgres
	}
	generation, err := sharding.NewAssignmentGeneration(rawGeneration)
	if err != nil {
		return failed, "catalog", sharding.ErrShardUnavailable
	}
	route, err := sharding.NewShardRoute(trainRunID, shardID, generation)
	if err != nil {
		return failed, "catalog", sharding.ErrShardUnavailable
	}
	failed.Route = route
	if minimumFencingProtocolVersion < 1 || minimumFencingProtocolVersion > sharding.SupportedFencingProtocolVersion {
		return failed, "protocol", sharding.ErrShardUnavailable
	}
	if protocolVersion != SupportedProtocolVersion {
		return failed, "protocol", sharding.ErrShardUnavailable
	}
	if schemaVersion != SupportedSchemaVersion {
		return failed, "schema", sharding.ErrShardUnavailable
	}
	handle, err := router.registry.Resolve(CatalogEntry{
		ShardID:         shardID,
		StorageKind:     StorageKind(rawStorageKind),
		ConnectionRef:   connectionRef,
		ProtocolVersion: protocolVersion,
		SchemaVersion:   schemaVersion,
		Enabled:         enabled,
		WriteEnabled:    writeEnabled,
		HealthState:     HealthState(rawHealthState),
		State:           CatalogState(rawCatalogState),
	})
	if err != nil {
		reason := "catalog"
		if errors.Is(err, ErrUnknownConnectionReference) {
			reason = "unknown_connection_ref"
		}
		return failed, reason, sharding.ErrShardUnavailable
	}
	return Resolution{Route: route, Handle: handle}, "none", nil
}

func (router *CatalogRouter) observeResolve(
	forceRefresh bool,
	resolution Resolution,
	result, reason string,
	started time.Time,
) {
	if router == nil || router.metrics == nil {
		return
	}
	shardID := "unknown"
	storageKind := "unknown"
	if resolution.Handle.shardID == sharding.ShardPhysicalZero ||
		resolution.Handle.shardID == sharding.ShardPhysicalOne {
		shardID = resolution.Handle.shardID.String()
	}
	if resolution.Handle.storageKind == StoragePostgres {
		storageKind = string(StoragePostgres)
	}
	duration := router.now().Sub(started)
	if duration < 0 {
		duration = 0
	}
	router.metrics.RecordPhysicalShardRoute(
		"resolve", result, reason, shardID, storageKind, duration,
	)
	if forceRefresh {
		router.metrics.RecordPhysicalShardRouteRefresh(result, reason, shardID)
	}
	if result != "success" {
		router.metrics.RecordPhysicalShardUnavailable("resolve", reason, shardID)
	}
}

func (router *CatalogRouter) evictOneLocked() {
	now := router.now()
	for trainRunID, cached := range router.cache {
		if !now.Before(cached.expiresAt) {
			delete(router.cache, trainRunID)
			return
		}
	}
	// Map iteration order is intentionally unspecified and therefore avoids a
	// hot-key bias while keeping memory strictly bounded.
	for trainRunID := range router.cache {
		delete(router.cache, trainRunID)
		return
	}
}

func nilCatalogReader(value CatalogReader) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}
