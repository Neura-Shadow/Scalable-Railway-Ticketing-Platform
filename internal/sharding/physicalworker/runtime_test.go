package physicalworker

import (
	"context"
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/jackc/pgx/v5"
)

func TestRuntimeUsesOnlyConfiguredCatalogHandlesAndValidatesSchema(t *testing.T) {
	cfg := config.Defaults()
	cfg.BookingShardMode = config.BookingShardModePhysical
	cfg.BookingShardIDs = []string{"physical-shard-0", "physical-shard-1"}
	cfg.PhysicalShardConnections = map[string]string{
		"physical-shard-0": "configured-zero",
		"physical-shard-1": "configured-one",
	}
	pools := map[string]*queuePool{
		"configured-zero": {transactions: []pgx.Tx{&recordingTx{row: fakeRow{values: []any{int(physical.SupportedSchemaVersion), false}}}}},
		"configured-one":  {transactions: []pgx.Tx{&recordingTx{row: fakeRow{values: []any{int(physical.SupportedSchemaVersion), false}}}}},
	}
	catalog := &runtimeCatalog{rows: map[string]pgx.Row{
		"physical-shard-0": runtimeCatalogRow("physical-shard-0", true),
		"physical-shard-1": runtimeCatalogRow("physical-shard-1", false),
	}}
	var openedLimits []physical.PoolLimits
	runtime, err := newRuntime(context.Background(), cfg, catalog, func(_ context.Context, dsn string, limits physical.PoolLimits) (physical.Pool, error) {
		openedLimits = append(openedLimits, limits)
		pool, ok := pools[dsn]
		if !ok {
			return nil, errors.New("unconfigured endpoint")
		}
		return pool, nil
	})
	if err != nil {
		t.Fatalf("newRuntime() error = %v", err)
	}
	defer runtime.Close()

	if got := len(runtime.Handles()); got != 2 {
		t.Fatalf("handle count = %d, want 2", got)
	}
	if err := runtime.Ready(context.Background()); err != nil {
		t.Fatalf("Ready() error = %v", err)
	}
	if len(catalog.requested) != 2 || catalog.requested[0] != "physical-shard-0" || catalog.requested[1] != "physical-shard-1" {
		t.Fatalf("catalog requests = %v, want exact configured IDs", catalog.requested)
	}
	if len(openedLimits) != 2 {
		t.Fatalf("opened pool limits = %d, want 2", len(openedLimits))
	}
	for _, limits := range openedLimits {
		if limits.StatementTimeout != cfg.PhysicalShardQueryTimeout ||
			limits.LockTimeout != cfg.PhysicalShardQueryTimeout {
			t.Fatalf("worker pool local timeouts = %+v", limits)
		}
	}
}

func TestRuntimeReadinessRejectsDirtyOrWrongShardMigration(t *testing.T) {
	for _, values := range [][]any{{0, false}, {int(physical.SupportedSchemaVersion), true}} {
		cfg := config.Defaults()
		cfg.BookingShardMode = config.BookingShardModePhysical
		cfg.BookingShardIDs = []string{"physical-shard-0"}
		cfg.PhysicalShardConnections = map[string]string{"physical-shard-0": "configured-zero"}
		pool := &queuePool{transactions: []pgx.Tx{&recordingTx{row: fakeRow{values: values}}}}
		runtime, err := newRuntime(context.Background(), cfg, &runtimeCatalog{rows: map[string]pgx.Row{
			"physical-shard-0": runtimeCatalogRow("physical-shard-0", true),
		}}, func(context.Context, string, physical.PoolLimits) (physical.Pool, error) { return pool, nil })
		if err != nil {
			t.Fatalf("newRuntime() error = %v", err)
		}
		if err := runtime.Ready(context.Background()); !errors.Is(err, ErrRuntimeUnavailable) {
			t.Fatalf("Ready() error = %v, want ErrRuntimeUnavailable", err)
		}
		runtime.Close()
	}
}

type runtimeCatalog struct {
	rows      map[string]pgx.Row
	requested []string
}

func (catalog *runtimeCatalog) QueryRow(_ context.Context, _ string, arguments ...any) pgx.Row {
	shardID, _ := arguments[0].(string)
	catalog.requested = append(catalog.requested, shardID)
	if row, ok := catalog.rows[shardID]; ok {
		return row
	}
	return fakeRow{err: errors.New("missing catalog row")}
}

func runtimeCatalogRow(shardID string, writeEnabled bool) pgx.Row {
	return fakeRow{values: []any{
		shardID, "postgres", shardID, int32(1), physical.SupportedSchemaVersion, true,
		writeEnabled, "healthy", "active",
	}}
}
