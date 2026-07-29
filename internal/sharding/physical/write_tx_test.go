package physical_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestBeginWriteRejectsStaleGenerationInsidePhysicalDatabase(t *testing.T) {
	t.Parallel()

	tx := &writeTx{row: writeRow{values: []any{int64(8), true, int64(8), true, int64(3), "stable"}}}
	pool := &writePool{tx: tx}
	handle := mustPhysicalHandle(t, pool, true)
	route := mustPhysicalRoute(t, sharding.ShardPhysicalZero, 7)

	_, err := physical.BeginWrite(context.Background(), handle, route, 3)
	if !errors.Is(err, sharding.ErrAssignmentStale) {
		t.Fatalf("BeginWrite() error = %v, want %v", err, sharding.ErrAssignmentStale)
	}
	if tx.rollbacks != 1 || tx.commits != 0 {
		t.Fatalf("transaction finalization = commits %d rollbacks %d", tx.commits, tx.rollbacks)
	}
}

func mustPhysicalHandle(t *testing.T, pool physical.Pool, writeEnabled bool) physical.Handle {
	t.Helper()
	registry, err := physical.NewRegistry(context.Background(), physical.RegistryConfig{
		Connections: map[string]physical.ConnectionConfig{
			"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: "postgres://booking@shard-0.example/railway"},
		},
		MaxCount: 1,
		Limits:   physical.PoolLimits{MaxOpenConns: 4, MaxIdleConns: 2},
	}, func(context.Context, string, physical.PoolLimits) (physical.Pool, error) { return pool, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)
	handle, err := registry.Resolve(physical.CatalogEntry{
		ShardID:         sharding.ShardPhysicalZero,
		StorageKind:     physical.StoragePostgres,
		ConnectionRef:   "physical-shard-0",
		ProtocolVersion: 1,
		SchemaVersion:   1,
		Enabled:         true,
		WriteEnabled:    writeEnabled,
		HealthState:     physical.HealthHealthy,
		State:           physical.StateActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

func mustPhysicalRoute(t *testing.T, shardID sharding.ShardID, generation int64) sharding.ShardRoute {
	t.Helper()
	value, err := sharding.NewAssignmentGeneration(generation)
	if err != nil {
		t.Fatal(err)
	}
	route, err := sharding.NewShardRoute(uuid.New(), shardID, value)
	if err != nil {
		t.Fatal(err)
	}
	return route
}

type writePool struct {
	tx *writeTx
}

func (pool *writePool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) { return pool.tx, nil }
func (*writePool) Close()                                                      {}

type writeTx struct {
	pgx.Tx
	row       pgx.Row
	commits   int
	rollbacks int
}

func (tx *writeTx) QueryRow(context.Context, string, ...any) pgx.Row { return tx.row }
func (tx *writeTx) Commit(context.Context) error {
	tx.commits++
	return nil
}
func (tx *writeTx) Rollback(context.Context) error {
	tx.rollbacks++
	return nil
}

type writeRow struct {
	values []any
	err    error
}

func (row writeRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	for index := range dest {
		reflect.ValueOf(dest[index]).Elem().Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}
