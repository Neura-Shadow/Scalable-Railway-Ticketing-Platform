package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestPhysicalAvailabilityReadsControlMetadataBeforeOneShardTransaction(t *testing.T) {
	t.Parallel()
	trainRunID := uuid.New()
	events := []string{}
	controlDB := &physicalControlDB{trainRunID: trainRunID, events: &events}
	control, err := NewStore(controlDB)
	if err != nil {
		t.Fatal(err)
	}
	pool := &physicalAvailabilityPool{events: &events, row: physicalRow{values: []any{int64(1200), "TWD", int64(4)}}}
	router := physicalAvailabilityRouterFixture(t, trainRunID, 7, pool)
	store, err := NewPhysicalStore(control, router)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Availability(context.Background(), AvailabilityRequest{
		TrainRunID: trainRunID.String(), OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard",
	})
	if err != nil {
		t.Fatalf("Availability() error=%v", err)
	}
	if result.AvailableSeats != 4 || result.FareAmountMinor != 1200 || result.Currency != "TWD" || result.AssignmentGeneration != 7 {
		t.Fatalf("Availability()=%+v", result)
	}
	want := []string{"control-assignment", "control-journey", "shard-begin", "shard-query"}
	if !reflect.DeepEqual(events, want) || pool.commits != 1 || pool.options.AccessMode != pgx.ReadOnly {
		t.Fatalf("events=%v commits=%d options=%+v", events, pool.commits, pool.options)
	}
	if !strings.Contains(pool.query, "seat_inventory") || !strings.Contains(pool.query, "booking_fare_snapshots") ||
		!strings.Contains(pool.query, "train_run_write_fences") {
		t.Fatalf("shard SQL missing authority tables: %s", pool.query)
	}
}

func TestPhysicalAvailabilityGenerationUsesCurrentControlAssignmentWithoutShardRead(t *testing.T) {
	t.Parallel()
	trainRunID := uuid.New()
	events := []string{}
	control, _ := NewStore(&physicalControlDB{trainRunID: trainRunID, events: &events})
	pool := &physicalAvailabilityPool{events: &events}
	store, _ := NewPhysicalStore(control, physicalAvailabilityRouterFixture(t, trainRunID, 7, pool))
	generation, err := store.AvailabilityAssignmentGeneration(context.Background(), trainRunID.String())
	if err != nil || generation != 7 || pool.begins != 0 || !reflect.DeepEqual(events, []string{"control-assignment"}) {
		t.Fatalf("generation=%d err=%v begins=%d events=%v", generation, err, pool.begins, events)
	}
}

func TestPhysicalAvailabilityFailsClosedWhileAssignmentMigrating(t *testing.T) {
	t.Parallel()
	trainRunID := uuid.New()
	events := []string{}
	control, _ := NewStore(&physicalControlDB{trainRunID: trainRunID, assignmentState: "migrating", events: &events})
	pool := &physicalAvailabilityPool{events: &events}
	store, _ := NewPhysicalStore(control, physicalAvailabilityRouterFixture(t, trainRunID, 7, pool))
	request := AvailabilityRequest{TrainRunID: trainRunID.String(), OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard"}
	if _, err := store.Availability(context.Background(), request); !errors.Is(err, ErrPersistence) || pool.begins != 0 {
		t.Fatalf("Availability() err=%v begins=%d", err, pool.begins)
	}
	if _, err := store.AvailabilityAssignmentGeneration(context.Background(), trainRunID.String()); !errors.Is(err, ErrPersistence) {
		t.Fatalf("AvailabilityAssignmentGeneration() err=%v", err)
	}
}

func TestPhysicalAvailabilityBatchIsBoundedAndSequential(t *testing.T) {
	t.Parallel()
	trainRunID := uuid.New()
	events := []string{}
	control, _ := NewStore(&physicalControlDB{trainRunID: trainRunID, events: &events})
	pool := &physicalAvailabilityPool{events: &events, row: physicalRow{values: []any{int64(900), "TWD", int64(2)}}}
	store, _ := NewPhysicalStore(control, physicalAvailabilityRouterFixture(t, trainRunID, 7, pool))
	request := AvailabilityRequest{TrainRunID: trainRunID.String(), OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard"}
	results, err := store.AvailabilityBatch(context.Background(), []AvailabilityRequest{request, request})
	if err != nil || len(results) != 2 || pool.begins != 2 || pool.maxInFlight != 1 {
		t.Fatalf("results=%d err=%v begins=%d max_in_flight=%d", len(results), err, pool.begins, pool.maxInFlight)
	}
}

func physicalAvailabilityRouterFixture(t *testing.T, trainRunID uuid.UUID, rawGeneration int64, pool shardphysical.Pool) *physicalAvailabilityRouterFake {
	t.Helper()
	registry, err := shardphysical.NewRegistry(context.Background(), shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: "synthetic"}},
		MaxCount:    1, Limits: shardphysical.PoolLimits{MaxOpenConns: 1},
	}, func(context.Context, string, shardphysical.PoolLimits) (shardphysical.Pool, error) { return pool, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)
	handle, err := registry.Resolve(shardphysical.CatalogEntry{ShardID: sharding.ShardPhysicalZero,
		StorageKind: shardphysical.StoragePostgres, ConnectionRef: "physical-shard-0", ProtocolVersion: 1,
		SchemaVersion: shardphysical.SupportedSchemaVersion, Enabled: true, WriteEnabled: true, HealthState: shardphysical.HealthHealthy, State: shardphysical.StateActive})
	if err != nil {
		t.Fatal(err)
	}
	generation, _ := sharding.NewAssignmentGeneration(rawGeneration)
	route, _ := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	return &physicalAvailabilityRouterFake{resolution: shardphysical.Resolution{Route: route, Handle: handle}}
}

type physicalAvailabilityRouterFake struct{ resolution shardphysical.Resolution }

func (router *physicalAvailabilityRouterFake) Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error) {
	return router.resolution, nil
}

type physicalControlDB struct {
	trainRunID      uuid.UUID
	assignmentState string
	events          *[]string
}

func (*physicalControlDB) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, nil }
func (db *physicalControlDB) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	if strings.Contains(query, "train_run_shard_assignments") {
		*db.events = append(*db.events, "control-assignment")
		state := db.assignmentState
		if state == "" {
			state = "stable"
		}
		return physicalRow{values: []any{"postgres", "physical-shard-0", int64(7), state}}
	}
	*db.events = append(*db.events, "control-journey")
	departure := time.Date(2026, 7, 29, 1, 0, 0, 0, time.UTC)
	return physicalRow{values: []any{
		db.trainRunID.String(), uuid.NewString(), "R100", uuid.NewString(), "WEST",
		time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC), departure, "scheduled", 3,
		0, 2, 0, 0, 60,
	}}
}

type physicalAvailabilityPool struct {
	events                                 *[]string
	row                                    physicalRow
	query                                  string
	options                                pgx.TxOptions
	begins, commits, inFlight, maxInFlight int
}

func (pool *physicalAvailabilityPool) BeginTx(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	pool.begins++
	pool.inFlight++
	if pool.inFlight > pool.maxInFlight {
		pool.maxInFlight = pool.inFlight
	}
	pool.options = options
	*pool.events = append(*pool.events, "shard-begin")
	return &physicalAvailabilityTx{pool: pool}, nil
}
func (*physicalAvailabilityPool) Close() {}

type physicalAvailabilityTx struct {
	pgx.Tx
	pool *physicalAvailabilityPool
}

func (tx *physicalAvailabilityTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	tx.pool.query = query
	*tx.pool.events = append(*tx.pool.events, "shard-query")
	return tx.pool.row
}
func (tx *physicalAvailabilityTx) Commit(context.Context) error {
	tx.pool.commits++
	tx.pool.inFlight--
	return nil
}
func (tx *physicalAvailabilityTx) Rollback(context.Context) error {
	if tx.pool.inFlight > 0 {
		tx.pool.inFlight--
	}
	return nil
}

type physicalRow struct {
	values []any
	err    error
}

func (row physicalRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	for index := range dest {
		reflect.ValueOf(dest[index]).Elem().Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}
