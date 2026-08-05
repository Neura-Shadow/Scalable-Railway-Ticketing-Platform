package physical_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/jackc/pgx/v5"
)

func TestRegistryRejectsIncompleteTransactionLocalTimeoutsBeforeOpeningPools(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		statement time.Duration
		lock      time.Duration
	}{
		{name: "missing lock timeout", statement: time.Second},
		{name: "lock exceeds statement", statement: time.Second, lock: 2 * time.Second},
		{name: "statement exceeds config bound", statement: 31 * time.Second, lock: time.Second},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			created := 0
			_, err := physical.NewRegistry(context.Background(), physical.RegistryConfig{
				Connections: map[string]physical.ConnectionConfig{
					"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: "postgres://booking@shard-0.example/railway"},
				},
				MaxCount: 1,
				Limits: physical.PoolLimits{
					MaxOpenConns: 4, MaxIdleConns: 2, StatementTimeout: test.statement, LockTimeout: test.lock,
				},
			}, func(context.Context, string, physical.PoolLimits) (physical.Pool, error) {
				created++
				return &stubPool{}, nil
			})
			if !errors.Is(err, physical.ErrInvalidRegistry) {
				t.Fatalf("NewRegistry() error = %v, want %v", err, physical.ErrInvalidRegistry)
			}
			if created != 0 {
				t.Fatalf("pool creations = %d, want 0 for invalid local timeout contract", created)
			}
		})
	}
}

func TestRegistryRejectsCatalogConnectionReferenceNotConfigured(t *testing.T) {
	t.Parallel()

	created := 0
	registry, err := physical.NewRegistry(context.Background(), physical.RegistryConfig{
		Connections: map[string]physical.ConnectionConfig{
			"physical-shard-0": {
				ShardID: sharding.ShardPhysicalZero,
				DSN:     "postgres://booking@shard-0.example/railway",
			},
		},
		MaxCount: 1,
		Limits: physical.PoolLimits{
			MaxOpenConns: 4,
			MaxIdleConns: 2,
		},
	}, func(context.Context, string, physical.PoolLimits) (physical.Pool, error) {
		created++
		return &stubPool{}, nil
	})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	t.Cleanup(registry.Close)

	_, err = registry.Resolve(physical.CatalogEntry{
		ShardID:         sharding.ShardPhysicalZero,
		StorageKind:     physical.StoragePostgres,
		ConnectionRef:   "attacker-controlled-ref",
		ProtocolVersion: 1,
		SchemaVersion:   1,
		Enabled:         true,
		WriteEnabled:    true,
	})
	if !errors.Is(err, physical.ErrUnknownConnectionReference) {
		t.Fatalf("Resolve() error = %v, want %v", err, physical.ErrUnknownConnectionReference)
	}
	if created != 1 {
		t.Fatalf("pool creations = %d, want only the configured startup pool", created)
	}
}

func TestRegistryReturnsOnlyValidatedHandleMetadata(t *testing.T) {
	t.Parallel()

	pool := &stubPool{}
	registry, err := physical.NewRegistry(context.Background(), physical.RegistryConfig{
		Connections: map[string]physical.ConnectionConfig{
			"physical-shard-1": {
				ShardID: sharding.ShardPhysicalOne,
				DSN:     "postgres://booking:sentinel@shard-1.example/railway",
			},
		},
		MaxCount: 1,
		Limits:   physical.PoolLimits{MaxOpenConns: 4, MaxIdleConns: 2},
	}, func(context.Context, string, physical.PoolLimits) (physical.Pool, error) {
		return pool, nil
	})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	t.Cleanup(registry.Close)

	handle, err := registry.Resolve(physical.CatalogEntry{
		ShardID:         sharding.ShardPhysicalOne,
		StorageKind:     physical.StoragePostgres,
		ConnectionRef:   "physical-shard-1",
		ProtocolVersion: 1,
		SchemaVersion:   1,
		Enabled:         true,
		WriteEnabled:    false,
		HealthState:     physical.HealthHealthy,
		State:           physical.StateActive,
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if handle.ShardID() != sharding.ShardPhysicalOne || handle.StorageKind() != physical.StoragePostgres ||
		handle.SchemaVersion() != 1 || handle.ProtocolVersion() != 1 || handle.HealthState() != physical.HealthHealthy ||
		handle.WriteEnabled() || handle.Pool() != pool {
		t.Fatalf("validated handle = %+v", handle)
	}
}

func TestOpenPGXPoolRejectsInvalidDSNWithoutLeakingIt(t *testing.T) {
	t.Parallel()

	_, err := physical.OpenPGXPool(context.Background(), "postgres://booking:sentinel-secret@%zz/railway", physical.PoolLimits{
		MaxOpenConns: 4,
		MaxIdleConns: 2,
	})
	if !errors.Is(err, physical.ErrInvalidRegistry) {
		t.Fatalf("OpenPGXPool() error = %v, want %v", err, physical.ErrInvalidRegistry)
	}
	if strings.Contains(err.Error(), "sentinel-secret") || strings.Contains(err.Error(), "%zz") {
		t.Fatalf("OpenPGXPool() exposed connection material: %v", err)
	}
}

func TestRegistryReportsOnlyBoundedShardPoolSnapshots(t *testing.T) {
	t.Parallel()
	registry, err := physical.NewRegistry(context.Background(), physical.RegistryConfig{
		Connections: map[string]physical.ConnectionConfig{
			"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: "postgres://booking@shard-0.example/railway"},
		},
		MaxCount: 1,
		Limits:   physical.PoolLimits{MaxOpenConns: 4, MaxIdleConns: 2},
	}, func(context.Context, string, physical.PoolLimits) (physical.Pool, error) {
		return &statsPool{snapshot: physical.PoolSnapshot{
			TotalConnections: 4, AcquiredConnections: 3, IdleConnections: 1,
			MaxConnections: 4, AcquireCount: 9, AcquireDuration: time.Second,
			EmptyAcquireCount: 2, CancelledAcquireCount: 1,
		}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)

	snapshots := registry.PoolSnapshots()
	if len(snapshots) != 1 {
		t.Fatalf("PoolSnapshots() count = %d, want 1", len(snapshots))
	}
	if got := snapshots[sharding.ShardPhysicalZero]; got.AcquiredConnections != 3 || got.AcquireCount != 9 {
		t.Fatalf("PoolSnapshots()[physical-shard-0] = %+v", got)
	}
}

func TestOpenPGXPoolPublishesSnapshotThroughTimeoutAdapter(t *testing.T) {
	t.Parallel()
	registry, err := physical.NewRegistry(context.Background(), physical.RegistryConfig{
		Connections: map[string]physical.ConnectionConfig{
			"physical-shard-1": {
				ShardID: sharding.ShardPhysicalOne,
				DSN:     "postgres://synthetic@127.0.0.1:1/railway?connect_timeout=1",
			},
		},
		MaxCount: 1,
		Limits: physical.PoolLimits{
			MaxOpenConns: 7, MaxIdleConns: 2,
			StatementTimeout: time.Second, LockTimeout: time.Second,
		},
	}, physical.OpenPGXPool)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)

	snapshot, ok := registry.PoolSnapshots()[sharding.ShardPhysicalOne]
	if !ok {
		t.Fatal("physical pgx pool snapshot is unavailable")
	}
	if snapshot.MaxConnections != 7 || snapshot.TotalConnections != 0 {
		t.Fatalf("physical pgx pool snapshot = %+v", snapshot)
	}
}

type stubPool struct{}

func (*stubPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, errors.New("unused stub pool")
}
func (*stubPool) Close() {}

type statsPool struct {
	stubPool
	snapshot physical.PoolSnapshot
}

func (pool *statsPool) PoolSnapshot() physical.PoolSnapshot { return pool.snapshot }
