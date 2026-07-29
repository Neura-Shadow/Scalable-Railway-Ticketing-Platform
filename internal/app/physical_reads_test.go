package app

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestHybridReservationReaderReadsExactlyDirectorySelectedPhysicalShard(t *testing.T) {
	t.Parallel()
	owner, reservationID, trainRunID, passengerID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	expires := time.Now().UTC().Add(time.Minute)
	shardTx := &hybridReadTx{row: hybridRow{values: []any{
		reservationID.String(), "held", trainRunID, 0, 2, "standard", expires,
		[]string{passengerID.String()},
	}}}
	registry, err := shardphysical.NewRegistry(context.Background(), shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{
			"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: "synthetic-shard-0"},
		},
		MaxCount: 1,
		Limits:   shardphysical.PoolLimits{MaxOpenConns: 1},
	}, func(context.Context, string, shardphysical.PoolLimits) (shardphysical.Pool, error) {
		return &hybridReadPool{tx: shardTx}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)
	handle, err := registry.Resolve(shardphysical.CatalogEntry{
		ShardID: sharding.ShardPhysicalZero, StorageKind: shardphysical.StoragePostgres,
		ConnectionRef: "physical-shard-0", ProtocolVersion: 1, SchemaVersion: 1,
		Enabled: true, WriteEnabled: true, HealthState: shardphysical.HealthHealthy,
		State: shardphysical.StateActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	generation, _ := sharding.NewAssignmentGeneration(5)
	route, _ := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	control := &hybridControl{rows: []pgx.Row{
		hybridRow{values: []any{trainRunID, "physical-shard-0", int64(5), "active"}},
		hybridRow{values: []any{"TPE", "KHH"}},
	}}
	reader, err := NewHybridReservationReader(control, &hybridLegacyReader{}, &hybridPhysicalRouter{
		resolution: shardphysical.Resolution{Route: route, Handle: handle},
	})
	if err != nil {
		t.Fatal(err)
	}

	detail, err := reader.GetReservationDetail(context.Background(), owner, reservationID)
	if err != nil {
		t.Fatalf("GetReservationDetail() error = %v", err)
	}
	if detail.ID != reservationID.String() || detail.TrainRunID != trainRunID.String() ||
		detail.OriginStationCode != "TPE" || detail.DestinationStationCode != "KHH" ||
		!reflect.DeepEqual(detail.PassengerIDs, []string{passengerID.String()}) || shardTx.commits != 1 {
		t.Fatalf("detail=%+v commits=%d", detail, shardTx.commits)
	}
}

type hybridLegacyReader struct{}

func (*hybridLegacyReader) GetReservationDetail(context.Context, uuid.UUID, uuid.UUID) (ReservationDetail, error) {
	return ReservationDetail{}, ErrReadNotFound
}

type hybridPhysicalRouter struct {
	resolution shardphysical.Resolution
	err        error
}

func (router *hybridPhysicalRouter) Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error) {
	return router.resolution, router.err
}

type hybridReadPool struct{ tx pgx.Tx }

func (pool *hybridReadPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return pool.tx, nil
}
func (*hybridReadPool) Close() {}

type hybridReadTx struct {
	pgx.Tx
	row       pgx.Row
	commits   int
	rollbacks int
}

func (tx *hybridReadTx) QueryRow(context.Context, string, ...any) pgx.Row { return tx.row }
func (tx *hybridReadTx) Commit(context.Context) error                     { tx.commits++; return nil }
func (tx *hybridReadTx) Rollback(context.Context) error                   { tx.rollbacks++; return nil }
