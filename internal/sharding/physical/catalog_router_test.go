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
	if !strings.Contains(db.query, "'rollback_window'") {
		t.Fatalf("catalog query would stop routing the target during its rollback window: %s", db.query)
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

func TestCatalogRouterMetricsUseBoundedOutcomeReasons(t *testing.T) {
	trainRunID := uuid.New()
	valid := []any{
		"physical-shard-0", int64(3), "postgres", "physical-shard-0",
		int32(1), int32(1), true, true, "healthy", "active", int32(1),
	}
	tests := []struct {
		name        string
		values      []any
		scanErr     error
		refresh     bool
		wantSuccess bool
		wantReason  string
		wantShard   string
		wantStorage string
	}{
		{name: "success", values: valid, wantSuccess: true, wantReason: "none", wantShard: "physical-shard-0", wantStorage: "postgres"},
		{name: "refresh", values: valid, refresh: true, wantSuccess: true, wantReason: "none", wantShard: "physical-shard-0", wantStorage: "postgres"},
		{name: "catalog", values: []any{
			"physical-shard-0", int64(3), "postgres://secret@untrusted", "physical-shard-0",
			int32(1), int32(1), true, true, "healthy", "active", int32(1),
		}, wantReason: "catalog", wantShard: "physical-shard-0", wantStorage: "unknown"},
		{name: "schema", values: []any{
			"physical-shard-0", int64(3), "postgres", "physical-shard-0",
			int32(1), int32(2), true, true, "healthy", "active", int32(1),
		}, wantReason: "schema", wantShard: "physical-shard-0", wantStorage: "postgres"},
		{name: "protocol", values: []any{
			"physical-shard-0", int64(3), "postgres", "physical-shard-0",
			int32(2), int32(1), true, true, "healthy", "active", int32(1),
		}, wantReason: "protocol", wantShard: "physical-shard-0", wantStorage: "postgres"},
		{name: "unknown connection reference", values: []any{
			"physical-shard-0", int64(3), "postgres", "postgres://secret@untrusted",
			int32(1), int32(1), true, true, "healthy", "active", int32(1),
		}, wantReason: "unknown_connection_ref", wantShard: "physical-shard-0", wantStorage: "postgres"},
		{name: "database unavailable", scanErr: errors.New("postgres://secret@host/" + trainRunID.String()), wantReason: "database", wantShard: "unknown", wantStorage: "unknown"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			metrics := &catalogMetrics{}
			db := &catalogDB{rows: []catalogRow{{values: testCase.values, err: testCase.scanErr}}}
			router, err := physical.NewCatalogRouter(
				db, mustCatalogRegistry(t), time.Minute, physical.WithMetrics(metrics),
			)
			if err != nil {
				t.Fatal(err)
			}
			_, resolveErr := router.Resolve(context.Background(), trainRunID, testCase.refresh)
			if testCase.wantSuccess && resolveErr != nil {
				t.Fatalf("Resolve() error = %v", resolveErr)
			}
			if !testCase.wantSuccess && !errors.Is(resolveErr, sharding.ErrShardUnavailable) {
				t.Fatalf("Resolve() error = %v, want shard unavailable", resolveErr)
			}
			if len(metrics.routes) != 1 {
				t.Fatalf("route metrics = %d, want 1", len(metrics.routes))
			}
			wantResult := "unavailable"
			if testCase.wantSuccess {
				wantResult = "success"
			}
			observed := metrics.routes[0]
			if observed.operation != "resolve" || observed.result != wantResult ||
				observed.reason != testCase.wantReason || observed.shardID != testCase.wantShard ||
				observed.storageKind != testCase.wantStorage || observed.duration < 0 {
				t.Fatalf("route metric = %+v", observed)
			}
			if got := len(metrics.refreshes); got != boolCount(testCase.refresh) {
				t.Fatalf("refresh metrics = %d, want %d", got, boolCount(testCase.refresh))
			}
			if got := len(metrics.unavailable); got != boolCount(!testCase.wantSuccess) {
				t.Fatalf("unavailable metrics = %d, want %d", got, boolCount(!testCase.wantSuccess))
			}
			for _, value := range []string{observed.operation, observed.result, observed.reason, observed.shardID, observed.storageKind} {
				if strings.Contains(value, "postgres://") || strings.Contains(value, trainRunID.String()) {
					t.Fatalf("unbounded metric label = %q", value)
				}
			}
		})
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

type observedCatalogRoute struct {
	operation   string
	result      string
	reason      string
	shardID     string
	storageKind string
	duration    time.Duration
}

type catalogMetrics struct {
	routes      []observedCatalogRoute
	refreshes   []observedCatalogRoute
	unavailable []observedCatalogRoute
}

func (metrics *catalogMetrics) RecordPhysicalShardRoute(operation, result, reason, shardID, storageKind string, duration time.Duration) {
	metrics.routes = append(metrics.routes, observedCatalogRoute{
		operation: operation, result: result, reason: reason, shardID: shardID,
		storageKind: storageKind, duration: duration,
	})
}

func (metrics *catalogMetrics) RecordPhysicalShardRouteRefresh(result, reason, shardID string) {
	metrics.refreshes = append(metrics.refreshes, observedCatalogRoute{result: result, reason: reason, shardID: shardID})
}

func (metrics *catalogMetrics) RecordPhysicalShardUnavailable(operation, reason, shardID string) {
	metrics.unavailable = append(metrics.unavailable, observedCatalogRoute{operation: operation, reason: reason, shardID: shardID})
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
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
