package app

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type readinessProbe func(context.Context) error
type migrationProbe func(context.Context) (int, bool, error)
type ReadinessChecker struct {
	postgres, redis readinessProbe
	migrations      migrationProbe
	configuration   func() error
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
	return newReadinessChecker(postgresProbe, redisProbe, migrationsProbe, cfg.Validate)
}
func newReadinessChecker(postgres, redis readinessProbe, migrations migrationProbe, configuration func() error) *ReadinessChecker {
	return &ReadinessChecker{postgres: postgres, redis: redis, migrations: migrations, configuration: configuration}
}
func (r *ReadinessChecker) CheckReadiness(ctx context.Context) ([]httpapi.ReadinessCheck, error) {
	checks := []httpapi.ReadinessCheck{{Name: "postgres"}, {Name: "redis"}, {Name: "migrations"}, {Name: "configuration"}}
	if r == nil {
		return checks, nil
	}
	checks[0].Ready = r.postgres != nil && r.postgres(ctx) == nil
	checks[1].Ready = r.redis != nil && r.redis(ctx) == nil
	if r.migrations != nil {
		version, dirty, err := r.migrations(ctx)
		checks[2].Ready = err == nil && version == 4 && !dirty
	}
	checks[3].Ready = r.configuration != nil && r.configuration() == nil
	return checks, nil
}

var _ httpapi.ReadinessChecker = (*ReadinessChecker)(nil)
