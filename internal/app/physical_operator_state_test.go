package app

import (
	"context"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestPhysicalOperatorBookingStateReadsAuthoritativeVersionsFromOneShard(t *testing.T) {
	trainRunID, sourceFareID, snapshotFareID, seatID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	tests := []struct {
		name       string
		query      httpapi.OperatorBookingStateQuery
		control    []pgx.Row
		resource   pgx.Row
		wantSource int64
		assert     func(*testing.T, httpapi.OperatorBookingStateView)
	}{
		{name: "fare", query: httpapi.OperatorBookingStateQuery{Kind: httpapi.OperatorBookingFareState,
			TrainRunID: trainRunID.String(), ResourceID: sourceFareID.String()},
			control:    []pgx.Row{hybridRow{values: []any{snapshotFareID}}},
			resource:   hybridRow{values: []any{int64(7), true, 0, 2, "standard", int64(1250), "TWD"}},
			wantSource: 7, assert: func(t *testing.T, view httpapi.OperatorBookingStateView) {
				if view.Active == nil || !*view.Active || view.AmountMinor == nil || *view.AmountMinor != 1250 ||
					view.FromStopIndex == nil || *view.FromStopIndex != 0 || view.ToStopIndex == nil || *view.ToStopIndex != 2 {
					t.Fatalf("fare view = %+v", view)
				}
			}},
		{name: "seat", query: httpapi.OperatorBookingStateQuery{Kind: httpapi.OperatorBookingSeatState,
			TrainRunID: trainRunID.String(), ResourceID: seatID.String()},
			resource: hybridRow{values: []any{int64(8), false}}, wantSource: 8,
			assert: func(t *testing.T, view httpapi.OperatorBookingStateView) {
				if view.Active == nil || *view.Active {
					t.Fatalf("seat view = %+v", view)
				}
			}},
		{name: "policy", query: httpapi.OperatorBookingStateQuery{Kind: httpapi.OperatorBookingPolicyState,
			TrainRunID: trainRunID.String()}, resource: hybridRow{values: []any{int64(9), int64(4)}},
			wantSource: 9, assert: func(t *testing.T, view httpapi.OperatorBookingStateView) {
				if view.BookingPolicyVersion == nil || *view.BookingPolicyVersion != 4 {
					t.Fatalf("policy view = %+v", view)
				}
			}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			tx := &hybridReadTx{rows: []pgx.Row{
				hybridRow{values: []any{int64(5), true, "active", true}}, testCase.resource,
			}}
			pool := &hybridReadPool{tx: tx}
			reader, err := NewPhysicalOperatorBookingStateReader(
				&hybridControl{rows: testCase.control}, operatorStateRouter(t, trainRunID, 5, pool),
			)
			if err != nil {
				t.Fatal(err)
			}
			view, err := reader.GetOperatorBookingState(context.Background(), testCase.query)
			if err != nil {
				t.Fatalf("GetOperatorBookingState() error = %v", err)
			}
			if view.SourceVersion != testCase.wantSource || view.AssignmentGeneration != 5 || tx.commits != 1 ||
				pool.options.IsoLevel != pgx.RepeatableRead || pool.options.AccessMode != pgx.ReadOnly {
				t.Fatalf("view=%+v commits=%d options=%+v", view, tx.commits, pool.options)
			}
			testCase.assert(t, view)
		})
	}
}

func TestPhysicalOperatorBookingStateRejectsStaleLocalFence(t *testing.T) {
	trainRunID := uuid.New()
	tx := &hybridReadTx{rows: []pgx.Row{hybridRow{values: []any{int64(4), true, "active", true}}}}
	reader, err := NewPhysicalOperatorBookingStateReader(
		&hybridControl{}, operatorStateRouter(t, trainRunID, 5, &hybridReadPool{tx: tx}),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.GetOperatorBookingState(context.Background(), httpapi.OperatorBookingStateQuery{
		Kind: httpapi.OperatorBookingPolicyState, TrainRunID: trainRunID.String(),
	})
	if err != httpapi.ErrServiceTemporarilyRebalancing || tx.commits != 0 {
		t.Fatalf("error=%v commits=%d", err, tx.commits)
	}
}

func operatorStateRouter(t *testing.T, trainRunID uuid.UUID, rawGeneration int64, pool *hybridReadPool) physicalRouteResolver {
	t.Helper()
	registry, err := shardphysical.NewRegistry(context.Background(), shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{
			"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: "synthetic-shard-0"},
		}, MaxCount: 1, Limits: shardphysical.PoolLimits{MaxOpenConns: 1},
	}, func(context.Context, string, shardphysical.PoolLimits) (shardphysical.Pool, error) { return pool, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)
	handle, err := registry.Resolve(shardphysical.CatalogEntry{ShardID: sharding.ShardPhysicalZero,
		StorageKind: shardphysical.StoragePostgres, ConnectionRef: "physical-shard-0", ProtocolVersion: 1,
		SchemaVersion: shardphysical.SupportedSchemaVersion, Enabled: true, WriteEnabled: true, HealthState: shardphysical.HealthHealthy,
		State: shardphysical.StateActive})
	if err != nil {
		t.Fatal(err)
	}
	generation, err := sharding.NewAssignmentGeneration(rawGeneration)
	if err != nil {
		t.Fatal(err)
	}
	route, err := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	if err != nil {
		t.Fatal(err)
	}
	return &hybridPhysicalRouter{resolution: shardphysical.Resolution{Route: route, Handle: handle}}
}
