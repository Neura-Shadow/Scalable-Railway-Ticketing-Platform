// Package physical owns the bounded mapping from catalog connection
// references to independently pooled PostgreSQL booking shards.
package physical

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidRegistry            = errors.New("invalid physical shard registry")
	ErrUnknownConnectionReference = errors.New("unknown physical shard connection reference")
	ErrCatalogMismatch            = errors.New("physical shard catalog entry mismatch")
)

const (
	SupportedProtocolVersion int32 = 1
	SupportedSchemaVersion   int32 = 1
)

type StorageKind string

const StoragePostgres StorageKind = "postgres"

type HealthState string

const (
	HealthHealthy     HealthState = "healthy"
	HealthDegraded    HealthState = "degraded"
	HealthUnavailable HealthState = "unavailable"
)

type CatalogState string

const (
	StateActive   CatalogState = "active"
	StateDraining CatalogState = "draining"
	StateDisabled CatalogState = "disabled"
)

// Pool is deliberately small. Concrete pgxpool configuration remains inside
// the runtime adapter while registry tests replace only the database boundary.
type Pool interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Close()
}

// PoolFactory creates a pool only for an already validated configuration
// entry. Catalog values are never passed as DSNs.
type PoolFactory func(context.Context, string, PoolLimits) (Pool, error)

type PoolLimits struct {
	MaxOpenConns   int
	MaxIdleConns   int
	MaxLifetime    time.Duration
	MaxIdleTime    time.Duration
	ConnectTimeout time.Duration
}

type ConnectionConfig struct {
	ShardID sharding.ShardID
	DSN     string
}

type RegistryConfig struct {
	Connections map[string]ConnectionConfig
	MaxCount    int
	Limits      PoolLimits
}

type CatalogEntry struct {
	ShardID         sharding.ShardID
	StorageKind     StorageKind
	ConnectionRef   string
	ProtocolVersion int32
	SchemaVersion   int32
	Enabled         bool
	WriteEnabled    bool
	HealthState     HealthState
	State           CatalogState
}

type Handle struct {
	shardID         sharding.ShardID
	pool            Pool
	storageKind     StorageKind
	protocolVersion int32
	schemaVersion   int32
	healthState     HealthState
	writeEnabled    bool
}

func (h Handle) ShardID() sharding.ShardID { return h.shardID }
func (h Handle) Pool() Pool                { return h.pool }
func (h Handle) WriteEnabled() bool        { return h.writeEnabled }
func (h Handle) StorageKind() StorageKind  { return h.storageKind }
func (h Handle) ProtocolVersion() int32    { return h.protocolVersion }
func (h Handle) SchemaVersion() int32      { return h.schemaVersion }
func (h Handle) HealthState() HealthState  { return h.healthState }

type registeredConnection struct {
	shardID sharding.ShardID
	pool    Pool
}

// Registry is immutable after construction. This prevents a catalog refresh
// or request path from expanding the set of network endpoints.
type Registry struct {
	mu          sync.RWMutex
	connections map[string]registeredConnection
	closed      bool
}

func NewRegistry(ctx context.Context, config RegistryConfig, factory PoolFactory) (*Registry, error) {
	if ctx == nil || factory == nil || config.MaxCount < 1 || config.MaxCount > 2 ||
		len(config.Connections) < 1 || len(config.Connections) > config.MaxCount ||
		config.Limits.MaxOpenConns < 1 || config.Limits.MaxIdleConns < 0 ||
		config.Limits.MaxIdleConns > config.Limits.MaxOpenConns {
		return nil, ErrInvalidRegistry
	}

	refs := make([]string, 0, len(config.Connections))
	for ref := range config.Connections {
		refs = append(refs, ref)
	}
	sort.Strings(refs)

	registry := &Registry{connections: make(map[string]registeredConnection, len(refs))}
	seenShards := make(map[sharding.ShardID]struct{}, len(refs))
	seenDSNs := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		connection := config.Connections[ref]
		if ref != connection.ShardID.String() ||
			(connection.ShardID != sharding.ShardPhysicalZero && connection.ShardID != sharding.ShardPhysicalOne) ||
			strings.TrimSpace(connection.DSN) == "" {
			registry.Close()
			return nil, ErrInvalidRegistry
		}
		if _, duplicate := seenShards[connection.ShardID]; duplicate {
			registry.Close()
			return nil, ErrInvalidRegistry
		}
		if _, duplicate := seenDSNs[connection.DSN]; duplicate {
			registry.Close()
			return nil, ErrInvalidRegistry
		}
		pool, err := factory(ctx, connection.DSN, config.Limits)
		if err != nil || pool == nil {
			registry.Close()
			return nil, ErrInvalidRegistry
		}
		registry.connections[ref] = registeredConnection{shardID: connection.ShardID, pool: pool}
		seenShards[connection.ShardID] = struct{}{}
		seenDSNs[connection.DSN] = struct{}{}
	}
	return registry, nil
}

func (r *Registry) Resolve(entry CatalogEntry) (Handle, error) {
	if r == nil {
		return Handle{}, ErrInvalidRegistry
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return Handle{}, ErrInvalidRegistry
	}
	connection, ok := r.connections[entry.ConnectionRef]
	if !ok {
		return Handle{}, ErrUnknownConnectionReference
	}
	if entry.ShardID != connection.shardID || entry.StorageKind != StoragePostgres ||
		entry.ProtocolVersion != SupportedProtocolVersion || entry.SchemaVersion != SupportedSchemaVersion ||
		!entry.Enabled || (entry.HealthState != HealthHealthy && entry.HealthState != HealthDegraded) ||
		(entry.State != StateActive && entry.State != StateDraining) {
		return Handle{}, ErrCatalogMismatch
	}
	return Handle{
		shardID:         connection.shardID,
		pool:            connection.pool,
		storageKind:     entry.StorageKind,
		protocolVersion: entry.ProtocolVersion,
		schemaVersion:   entry.SchemaVersion,
		healthState:     entry.HealthState,
		writeEnabled:    entry.WriteEnabled,
	}, nil
}

func (r *Registry) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	for _, connection := range r.connections {
		connection.pool.Close()
	}
	r.closed = true
}
