package app

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type readinessProbe func(context.Context) error
type migrationProbe func(context.Context) (int, bool, error)

const currentSchemaVersion = 9

const shardReadinessQuery = `
SELECT
    count(*) = 5
    AND count(*) FILTER (WHERE
        (shard_id = 'legacy' AND storage_kind = 'legacy_schema')
        OR (shard_id = 'shard-0' AND storage_kind = 'logical_schema')
        OR (shard_id = 'shard-1' AND storage_kind = 'logical_schema')
        OR (shard_id = 'physical-shard-0' AND storage_kind = 'postgres'
            AND connection_ref = 'physical-shard-0')
        OR (shard_id = 'physical-shard-1' AND storage_kind = 'postgres'
            AND connection_ref = 'physical-shard-1')
    ) = 5
    AND to_regnamespace('booking_shard_0') IS NOT NULL
    AND to_regnamespace('booking_shard_1') IS NOT NULL AS topology_valid,
    count(*) FILTER (WHERE shard_id = ANY($2::text[])
        AND enabled AND state IN ('active', 'draining')) AS serving_count,
    count(*) FILTER (WHERE shard_id = ANY($2::text[])
        AND enabled AND state IN ('active', 'draining')
        AND minimum_fencing_protocol_version > $1) AS incompatible_serving_count
FROM public.booking_shards
WHERE shard_id IN (
    'legacy', 'shard-0', 'shard-1', 'physical-shard-0', 'physical-shard-1'
)`

type namedReadinessProbe struct {
	name  string
	probe readinessProbe
}

type ReadinessChecker struct {
	postgres, redis, shards readinessProbe
	migrations              migrationProbe
	configuration           func() error
	physical                []namedReadinessProbe
}

func NewReadinessChecker(
	pool *pgxpool.Pool,
	client redis.UniversalClient,
	cfg config.Config,
	physicalRegistries ...*shardphysical.Registry,
) *ReadinessChecker {
	postgresProbe := func(ctx context.Context) error {
		if pool == nil {
			return errors.New("postgres unavailable")
		}
		return pool.Ping(ctx)
	}
	redisProbe := func(ctx context.Context) error {
		if client == nil {
			return errors.New("redis unavailable")
		}
		return client.Ping(ctx).Err()
	}
	migrationsProbe := func(ctx context.Context) (int, bool, error) {
		if pool == nil {
			return 0, false, errors.New("postgres unavailable")
		}
		var version int
		var dirty bool
		err := pool.QueryRow(ctx, "SELECT version, dirty FROM schema_migrations LIMIT 1").Scan(&version, &dirty)
		return version, dirty, err
	}
	shardProbe := func(ctx context.Context) error {
		if cfg.BookingShardMode == config.BookingShardModeLegacy {
			return nil
		}
		if pool == nil {
			return errors.New("shard catalog unavailable")
		}
		var topologyValid bool
		var servingCount int
		var incompatibleServingCount int
		err := pool.QueryRow(
			ctx,
			shardReadinessQuery,
			sharding.SupportedFencingProtocolVersion,
			cfg.BookingShardIDs,
		).Scan(
			&topologyValid,
			&servingCount,
			&incompatibleServingCount,
		)
		if err != nil || !topologyValid || servingCount < 1 || incompatibleServingCount != 0 {
			return errors.New("shard catalog unavailable")
		}
		return nil
	}
	checker := newReadinessChecker(postgresProbe, redisProbe, migrationsProbe, shardProbe, cfg.Validate)
	if cfg.BookingShardMode == config.BookingShardModePhysical && len(physicalRegistries) == 1 && physicalRegistries[0] != nil {
		for _, rawShardID := range cfg.BookingShardIDs {
			shardID, err := sharding.ParseShardID(rawShardID)
			if err != nil {
				continue
			}
			checker.physical = append(checker.physical, namedReadinessProbe{
				name:  rawShardID,
				probe: physicalShardReadiness(pool, physicalRegistries[0], shardID),
			})
		}
	}
	return checker
}

func physicalShardReadiness(
	control *pgxpool.Pool,
	registry *shardphysical.Registry,
	shardID sharding.ShardID,
) readinessProbe {
	return func(ctx context.Context) error {
		if control == nil || registry == nil {
			return errors.New("physical shard unavailable")
		}
		var entry shardphysical.CatalogEntry
		var rawShardID, storageKind, healthState, state string
		err := control.QueryRow(ctx, `
SELECT shard_id, storage_kind, connection_ref, protocol_version,
       schema_version, enabled, write_enabled, health_state, state
FROM public.booking_shards
WHERE shard_id = $1`, shardID.String()).Scan(
			&rawShardID, &storageKind, &entry.ConnectionRef,
			&entry.ProtocolVersion, &entry.SchemaVersion, &entry.Enabled,
			&entry.WriteEnabled, &healthState, &state,
		)
		if err != nil || rawShardID != shardID.String() {
			return errors.New("physical shard unavailable")
		}
		entry.ShardID = shardID
		entry.StorageKind = shardphysical.StorageKind(storageKind)
		entry.HealthState = shardphysical.HealthState(healthState)
		entry.State = shardphysical.CatalogState(state)
		handle, err := registry.Resolve(entry)
		if err != nil {
			return errors.New("physical shard unavailable")
		}
		tx, err := handle.Pool().BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			return errors.New("physical shard unavailable")
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		var version int
		var dirty bool
		if err := tx.QueryRow(ctx, `SELECT version, dirty FROM public.schema_migrations LIMIT 1`).Scan(
			&version, &dirty,
		); err != nil || version != int(shardphysical.SupportedSchemaVersion) || dirty {
			return errors.New("physical shard migration unavailable")
		}
		return nil
	}
}
func newReadinessChecker(
	postgres, redis readinessProbe,
	migrations migrationProbe,
	shards readinessProbe,
	configuration func() error,
) *ReadinessChecker {
	return &ReadinessChecker{
		postgres: postgres, redis: redis, migrations: migrations, shards: shards, configuration: configuration,
	}
}
func (r *ReadinessChecker) CheckReadiness(ctx context.Context) ([]httpapi.ReadinessCheck, error) {
	checks := []httpapi.ReadinessCheck{
		{Name: "postgres"},
		{Name: "redis", Optional: true},
		{Name: "migrations"},
		{Name: "shard_catalog"},
		{Name: "configuration"},
	}
	if r == nil {
		return checks, nil
	}
	checks[0].Ready = r.postgres != nil && r.postgres(ctx) == nil
	checks[1].Ready = r.redis != nil && r.redis(ctx) == nil
	if r.migrations != nil {
		version, dirty, err := r.migrations(ctx)
		checks[2].Ready = err == nil && version == currentSchemaVersion && !dirty
	}
	checks[3].Ready = r.shards != nil && r.shards(ctx) == nil
	checks[4].Ready = r.configuration != nil && r.configuration() == nil
	for _, physical := range r.physical {
		checks = append(checks, httpapi.ReadinessCheck{
			Name:     "booking_" + physical.name,
			Ready:    physical.probe != nil && physical.probe(ctx) == nil,
			Optional: true,
		})
	}
	return checks, nil
}

var _ httpapi.ReadinessChecker = (*ReadinessChecker)(nil)
