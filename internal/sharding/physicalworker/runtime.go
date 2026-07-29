package physicalworker

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/jackc/pgx/v5"
)

var ErrRuntimeUnavailable = errors.New("physical shard worker runtime unavailable")

type catalogReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Runtime owns only the pools named by the fixed process configuration. The
// control catalog can select metadata for those pools, but can never introduce
// a connection endpoint.
type Runtime struct {
	registry *physical.Registry
	handles  []physical.Handle
}

func NewRuntime(ctx context.Context, cfg config.Config, control catalogReader) (*Runtime, error) {
	return newRuntime(ctx, cfg, control, physical.OpenPGXPool)
}

func newRuntime(
	ctx context.Context,
	cfg config.Config,
	control catalogReader,
	factory physical.PoolFactory,
) (*Runtime, error) {
	if ctx == nil || control == nil || factory == nil ||
		cfg.BookingShardMode != config.BookingShardModePhysical ||
		len(cfg.BookingShardIDs) < 1 || len(cfg.BookingShardIDs) > 2 {
		return nil, ErrRuntimeUnavailable
	}
	connections := make(map[string]physical.ConnectionConfig, len(cfg.BookingShardIDs))
	for _, rawShardID := range cfg.BookingShardIDs {
		shardID, err := sharding.ParseShardID(rawShardID)
		if err != nil || (shardID != sharding.ShardPhysicalZero && shardID != sharding.ShardPhysicalOne) {
			return nil, ErrRuntimeUnavailable
		}
		dsn, ok := cfg.PhysicalShardConnections[rawShardID]
		if !ok {
			return nil, ErrRuntimeUnavailable
		}
		connections[rawShardID] = physical.ConnectionConfig{ShardID: shardID, DSN: dsn}
	}
	registry, err := physical.NewRegistry(ctx, physical.RegistryConfig{
		Connections: connections,
		MaxCount:    cfg.PhysicalShardMaxCount,
		Limits: physical.PoolLimits{
			MaxOpenConns:     cfg.PhysicalShardMaxOpenConns,
			MaxIdleConns:     cfg.PhysicalShardMaxIdleConns,
			MaxLifetime:      cfg.PhysicalShardConnMaxLifetime,
			MaxIdleTime:      cfg.PhysicalShardConnMaxIdleTime,
			ConnectTimeout:   cfg.PhysicalShardConnectTimeout,
			StatementTimeout: cfg.PhysicalShardQueryTimeout,
			LockTimeout:      cfg.PhysicalShardQueryTimeout,
		},
	}, factory)
	if err != nil {
		return nil, ErrRuntimeUnavailable
	}
	runtime := &Runtime{registry: registry}
	for _, rawShardID := range cfg.BookingShardIDs {
		handle, resolveErr := resolveConfiguredHandle(ctx, control, registry, rawShardID)
		if resolveErr != nil {
			runtime.Close()
			return nil, ErrRuntimeUnavailable
		}
		runtime.handles = append(runtime.handles, handle)
	}
	return runtime, nil
}

func resolveConfiguredHandle(
	ctx context.Context,
	control catalogReader,
	registry *physical.Registry,
	rawShardID string,
) (physical.Handle, error) {
	var (
		catalogShardID  string
		storageKind     string
		connectionRef   string
		protocolVersion int32
		schemaVersion   int32
		enabled         bool
		writeEnabled    bool
		healthState     string
		state           string
	)
	err := control.QueryRow(ctx, `
SELECT shard_id, storage_kind, connection_ref, protocol_version,
       schema_version, enabled, write_enabled, health_state, state
FROM public.booking_shards
WHERE shard_id = $1
  AND connection_ref = $1
  AND storage_kind = 'postgres'`, rawShardID).Scan(
		&catalogShardID, &storageKind, &connectionRef, &protocolVersion,
		&schemaVersion, &enabled, &writeEnabled, &healthState, &state,
	)
	if err != nil || catalogShardID != rawShardID {
		return physical.Handle{}, ErrRuntimeUnavailable
	}
	shardID, err := sharding.ParseShardID(catalogShardID)
	if err != nil {
		return physical.Handle{}, ErrRuntimeUnavailable
	}
	handle, err := registry.Resolve(physical.CatalogEntry{
		ShardID: shardID, StorageKind: physical.StorageKind(storageKind), ConnectionRef: connectionRef,
		ProtocolVersion: protocolVersion, SchemaVersion: schemaVersion, Enabled: enabled,
		WriteEnabled: writeEnabled, HealthState: physical.HealthState(healthState), State: physical.CatalogState(state),
	})
	if err != nil {
		return physical.Handle{}, ErrRuntimeUnavailable
	}
	return handle, nil
}

func (runtime *Runtime) Handles() []Handle {
	if runtime == nil {
		return nil
	}
	handles := make([]Handle, len(runtime.handles))
	for index := range runtime.handles {
		handles[index] = runtime.handles[index]
	}
	return handles
}

// Ready validates every configured shard independently against the booking
// database migration contract. Any required shard being unavailable makes the
// process unready, while RunOnce still isolates shard failures after startup.
func (runtime *Runtime) Ready(ctx context.Context) error {
	if runtime == nil || ctx == nil || len(runtime.handles) < 1 {
		return ErrRuntimeUnavailable
	}
	for index := range runtime.handles {
		tx, err := runtime.handles[index].Pool().BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil || tx == nil {
			return ErrRuntimeUnavailable
		}
		var version int
		var dirty bool
		err = tx.QueryRow(ctx, `SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(&version, &dirty)
		_ = tx.Rollback(context.Background())
		if err != nil || version != int(physical.SupportedSchemaVersion) || dirty {
			return ErrRuntimeUnavailable
		}
	}
	return nil
}

func (runtime *Runtime) Close() {
	if runtime != nil && runtime.registry != nil {
		runtime.registry.Close()
	}
}
