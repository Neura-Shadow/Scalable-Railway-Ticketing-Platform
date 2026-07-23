package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type metricCall struct {
	operation string
	result    string
	reason    string
	shardID   string
}

type metricsSpy struct {
	routes      []metricCall
	caches      []metricCall
	refreshes   []metricCall
	stale       []metricCall
	fences      []metricCall
	unavailable []metricCall
}

func (metrics *metricsSpy) RecordShardRoute(operation, result, reason, shardID string, _ time.Duration) {
	metrics.routes = append(metrics.routes, metricCall{operation, result, reason, shardID})
}
func (metrics *metricsSpy) RecordShardRouteCache(result, reason, shardID string) {
	metrics.caches = append(metrics.caches, metricCall{result: result, reason: reason, shardID: shardID})
}
func (metrics *metricsSpy) RecordShardRouteRefresh(operation, result, shardID string) {
	metrics.refreshes = append(metrics.refreshes, metricCall{operation: operation, result: result, shardID: shardID})
}
func (metrics *metricsSpy) RecordShardAssignmentStale(operation, shardID string) {
	metrics.stale = append(metrics.stale, metricCall{operation: operation, shardID: shardID})
}
func (metrics *metricsSpy) RecordShardWriteFenceRejected(operation, reason, shardID string) {
	metrics.fences = append(metrics.fences, metricCall{operation: operation, reason: reason, shardID: shardID})
}
func (metrics *metricsSpy) RecordShardUnavailable(operation, reason, shardID string) {
	metrics.unavailable = append(metrics.unavailable, metricCall{operation: operation, reason: reason, shardID: shardID})
}

func TestRouterMetricsRecordBoundedCacheHit(t *testing.T) {
	trainRunID := uuid.New()
	route := mustRoute(t, trainRunID, sharding.ShardZero, 4)
	cache := &fakeRouteCache{route: route, found: true}
	metrics := &metricsSpy{}
	router, err := NewRouter(&fakeDB{}, cache, WithMetrics(metrics))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := router.ResolveTrainRun(context.Background(), trainRunID); err != nil {
		t.Fatal(err)
	}
	if len(metrics.caches) != 1 || metrics.caches[0] != (metricCall{result: "hit", reason: "cache_hit", shardID: "shard-0"}) {
		t.Fatalf("cache metrics = %+v", metrics.caches)
	}
	if len(metrics.routes) != 1 || metrics.routes[0] != (metricCall{operation: "resolve", result: "success", reason: "none", shardID: "shard-0"}) {
		t.Fatalf("route metrics = %+v", metrics.routes)
	}
}

func TestRouterMetricsNeverExposeInvalidRawDatabaseShard(t *testing.T) {
	metrics := &metricsSpy{}
	db := &fakeDB{queryRow: func(context.Context, string, ...any) pgx.Row {
		return fakeRow{values: []any{
			"booking_shard_0;drop schema public",
			int64(3),
			true,
			"active",
			sharding.SupportedFencingProtocolVersion,
		}}
	}}
	router, err := NewRouter(db, nil, WithMetrics(metrics))
	if err != nil {
		t.Fatal(err)
	}
	_, err = router.ResolveTrainRun(context.Background(), uuid.New())
	if !errors.Is(err, sharding.ErrShardUnavailable) {
		t.Fatalf("error = %v", err)
	}
	if len(metrics.routes) != 1 || metrics.routes[0].shardID != "unknown" || metrics.routes[0].reason != "database" {
		t.Fatalf("route metrics = %+v", metrics.routes)
	}
}

func TestRouterMetricsClassifyAssignmentStale(t *testing.T) {
	trainRunID := uuid.New()
	expected := mustRoute(t, trainRunID, sharding.ShardLegacy, 3)
	tx := fakeAuthorityTx("legacy", 4, true, "active", "stable", false, 4, true)
	metrics := &metricsSpy{}
	router, err := NewRouter(&fakeDB{beginTx: func(context.Context, pgx.TxOptions) (pgx.Tx, error) { return tx, nil }}, nil, WithMetrics(metrics))
	if err != nil {
		t.Fatal(err)
	}
	_, err = router.BeginTrainRunWrite(context.Background(), expected)
	if !errors.Is(err, sharding.ErrAssignmentStale) {
		t.Fatalf("error = %v", err)
	}
	if len(metrics.stale) != 1 || metrics.stale[0].operation != "write" || metrics.stale[0].shardID != "legacy" {
		t.Fatalf("stale metrics = %+v", metrics.stale)
	}
}
