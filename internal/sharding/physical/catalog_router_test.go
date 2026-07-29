package physical_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestCatalogRouterRefreshesStaleRouteWithoutReadingAConnectionString(t *testing.T) {
	t.Parallel()

	trainRunID := uuid.New()
	db := &catalogDB{rows: []catalogRow{
		{values: []any{"physical-shard-0", int64(3), "postgres", "physical-shard-0", int32(1), int32(1), true, true, "healthy", "active", int32(1)}},
		{values: []any{"physical-shard-1", int64(4), "postgres", "physical-shard-1", int32(1), int32(1), true, true, "healthy", "active", int32(1)}},
	}}
	registry := mustCatalogRegistry(t)
	router, err := physical.NewCatalogRouter(db, registry, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	first, err := router.Resolve(context.Background(), trainRunID, false)
	if err != nil {
		t.Fatalf("first Resolve() error = %v", err)
	}
	refreshed, err := router.Resolve(context.Background(), trainRunID, true)
	if err != nil {
		t.Fatalf("refreshed Resolve() error = %v", err)
	}
	if first.Route.ShardID() != sharding.ShardPhysicalZero || first.Route.Generation().Int64() != 3 ||
		refreshed.Route.ShardID() != sharding.ShardPhysicalOne || refreshed.Route.Generation().Int64() != 4 {
		t.Fatalf("routes = (%s,%d) then (%s,%d)", first.Route.ShardID(), first.Route.Generation().Int64(), refreshed.Route.ShardID(), refreshed.Route.Generation().Int64())
	}
	if strings.Contains(strings.ToLower(db.query), "dsn") || strings.Contains(strings.ToLower(db.query), "database_url") {
		t.Fatalf("catalog query requested a connection secret: %s", db.query)
	}
}

func TestCatalogRouterRejectsUnsupportedCatalogMetadata(t *testing.T) {
	t.Parallel()

	db := &catalogDB{rows: []catalogRow{{values: []any{
		"physical-shard-0", int64(3), "postgres", "physical-shard-0", int32(2), int32(1), true, true, "healthy", "active", int32(1),
	}}}}
	router, err := physical.NewCatalogRouter(db, mustCatalogRegistry(t), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, err = router.Resolve(context.Background(), uuid.New(), false)
	if !errors.Is(err, sharding.ErrShardUnavailable) {
		t.Fatalf("Resolve() error = %v, want %v", err, sharding.ErrShardUnavailable)
	}
}

func mustCatalogRegistry(t *testing.T) *physical.Registry {
	t.Helper()
	registry, err := physical.NewRegistry(context.Background(), physical.RegistryConfig{
		Connections: map[string]physical.ConnectionConfig{
			"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: "postgres://shard-0"},
			"physical-shard-1": {ShardID: sharding.ShardPhysicalOne, DSN: "postgres://shard-1"},
		},
		MaxCount: 2,
		Limits:   physical.PoolLimits{MaxOpenConns: 2, MaxIdleConns: 1},
	}, func(context.Context, string, physical.PoolLimits) (physical.Pool, error) { return &stubPool{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)
	return registry
}

type catalogDB struct {
	rows  []catalogRow
	query string
	calls int
}

func (db *catalogDB) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	db.query = query
	row := db.rows[db.calls]
	db.calls++
	return row
}

type catalogRow struct {
	values []any
	err    error
}

func (row catalogRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	for index := range dest {
		reflect.ValueOf(dest[index]).Elem().Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}
