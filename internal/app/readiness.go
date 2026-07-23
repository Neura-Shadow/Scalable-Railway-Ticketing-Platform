package app

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type readinessProbe func(context.Context) error
type migrationProbe func(context.Context) (int, bool, error)

const currentSchemaVersion = 8

type ReadinessChecker struct {
	postgres, redis, shards readinessProbe
	migrations              migrationProbe
	configuration           func() error
}

func NewReadinessChecker(pool *pgxpool.Pool, client redis.UniversalClient, cfg config.Config) *ReadinessChecker {
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
		err := pool.QueryRow(ctx, `
SELECT
    count(*) = 3
    AND count(*) FILTER (WHERE
        (shard_id = 'legacy' AND storage_kind = 'legacy' AND schema_name = 'public')
        OR (shard_id = 'shard-0' AND storage_kind = 'schema' AND schema_name = 'booking_shard_0')
        OR (shard_id = 'shard-1' AND storage_kind = 'schema' AND schema_name = 'booking_shard_1')
    ) = 3
    AND to_regnamespace('booking_shard_0') IS NOT NULL
    AND to_regnamespace('booking_shard_1') IS NOT NULL AS topology_valid,
    count(*) FILTER (WHERE enabled AND state IN ('active', 'draining')) AS serving_count,
    count(*) FILTER (WHERE enabled AND state IN ('active', 'draining')
        AND minimum_fencing_protocol_version > $1) AS incompatible_serving_count
FROM public.booking_shards
WHERE shard_id IN ('legacy', 'shard-0', 'shard-1')`, sharding.SupportedFencingProtocolVersion).Scan(
			&topologyValid,
			&servingCount,
			&incompatibleServingCount,
		)
		if err != nil || !topologyValid || servingCount < 1 || incompatibleServingCount != 0 {
			return errors.New("shard catalog unavailable")
		}
		return nil
	}
	return newReadinessChecker(postgresProbe, redisProbe, migrationsProbe, shardProbe, cfg.Validate)
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
	return checks, nil
}

var _ httpapi.ReadinessChecker = (*ReadinessChecker)(nil)
