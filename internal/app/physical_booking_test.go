package app

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	bookingcommand "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestHybridReservationCommandsRoutesPhysicalCreateWithControlSnapshotVersion(t *testing.T) {
	t.Parallel()
	owner, trainRunID, passengerID, reservationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	legacy := &hybridLegacy{}
	saga := &hybridSaga{result: bookingcommand.Result{ReservationID: reservationID}}
	commands, err := NewHybridReservationCommands(
		&hybridControl{rows: []pgx.Row{hybridRow{values: []any{"postgres"}}}},
		legacy,
		saga,
		hybridSnapshotRouter(t, trainRunID, 4, 7),
	)
	if err != nil {
		t.Fatal(err)
	}
	holdExpiresAt := time.Now().UTC().Add(time.Minute)
	result, err := commands.CreateHold(context.Background(), bookingpostgres.CreateHoldParams{
		UserID: owner, TrainRunID: trainRunID, FromStopIndex: 1, ToStopIndex: 3,
		SeatClass: "standard", PassengerIDs: []uuid.UUID{passengerID}, HoldExpiresAt: holdExpiresAt,
		IdempotencyKeyHash: makeHash(1), RequestFingerprint: makeHash(2),
	})
	if err != nil {
		t.Fatalf("CreateHold() error = %v", err)
	}
	if result.ReservationID != reservationID || legacy.createCalls != 0 || saga.calls != 1 ||
		saga.request.Payload.ExpectedSnapshotVersion != 7 ||
		!reflect.DeepEqual(saga.request.Payload.PassengerIDs, []uuid.UUID{passengerID}) {
		t.Fatalf("result=%+v request=%+v legacy_calls=%d", result, saga.request, legacy.createCalls)
	}
}

func TestHybridReservationCommandsPreservesLegacyCreatePath(t *testing.T) {
	t.Parallel()
	want := bookingpostgres.CreateHoldResult{ReservationID: uuid.New()}
	legacy := &hybridLegacy{createResult: want}
	commands, err := NewHybridReservationCommands(
		&hybridControl{rows: []pgx.Row{hybridRow{values: []any{"legacy_schema"}}}},
		legacy,
		&hybridSaga{},
		&hybridPhysicalRouter{},
	)
	if err != nil {
		t.Fatal(err)
	}
	got, err := commands.CreateHold(context.Background(), bookingpostgres.CreateHoldParams{TrainRunID: uuid.New()})
	if err != nil || got.ReservationID != want.ReservationID || legacy.createCalls != 1 {
		t.Fatalf("CreateHold() = (%+v, %v), legacy calls=%d", got, err, legacy.createCalls)
	}
}

func TestHybridReservationCommandsNeverFallsBackPhysicalLifecycleToLegacy(t *testing.T) {
	t.Parallel()
	legacy := &hybridLegacy{}
	commands, err := NewHybridReservationCommands(
		&hybridControl{rows: []pgx.Row{hybridRow{values: []any{"postgres"}}}},
		legacy,
		&hybridSaga{},
		&hybridPhysicalRouter{},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = commands.CancelReservation(context.Background(), bookingpostgres.ReservationCommandParams{ReservationID: uuid.New()})
	if !errors.Is(err, sharding.ErrShardUnavailable) || legacy.cancelCalls != 0 {
		t.Fatalf("CancelReservation() error=%v legacy calls=%d", err, legacy.cancelCalls)
	}
}

func hybridSnapshotRouter(t *testing.T, trainRunID uuid.UUID, generation, version int64) *hybridPhysicalRouter {
	t.Helper()
	registry, err := shardphysical.NewRegistry(context.Background(), shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{
			"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: "synthetic-shard-0"},
		},
		MaxCount: 1,
		Limits:   shardphysical.PoolLimits{MaxOpenConns: 1},
	}, func(context.Context, string, shardphysical.PoolLimits) (shardphysical.Pool, error) {
		return &hybridReadPool{tx: &hybridReadTx{row: hybridRow{values: []any{version}}}}, nil
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
	assignment, err := sharding.NewAssignmentGeneration(generation)
	if err != nil {
		t.Fatal(err)
	}
	route, err := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, assignment)
	if err != nil {
		t.Fatal(err)
	}
	return &hybridPhysicalRouter{resolution: shardphysical.Resolution{Route: route, Handle: handle}}
}

func makeHash(value byte) []byte {
	return append([]byte(nil), append([]byte{value}, make([]byte, 31)...)...)
}

type hybridSaga struct {
	request bookingcommand.ReserveRequest
	result  bookingcommand.Result
	err     error
	calls   int
}

func (saga *hybridSaga) Execute(_ context.Context, request bookingcommand.ReserveRequest) (bookingcommand.Result, error) {
	saga.calls++
	saga.request = request
	return saga.result, saga.err
}

type hybridLegacy struct {
	createResult                           bookingpostgres.CreateHoldResult
	createCalls, confirmCalls, cancelCalls int
}

func (legacy *hybridLegacy) CreateHold(context.Context, bookingpostgres.CreateHoldParams) (bookingpostgres.CreateHoldResult, error) {
	legacy.createCalls++
	return legacy.createResult, nil
}
func (legacy *hybridLegacy) ConfirmReservation(context.Context, bookingpostgres.ReservationCommandParams) (bookingpostgres.ConfirmReservationResult, error) {
	legacy.confirmCalls++
	return bookingpostgres.ConfirmReservationResult{}, nil
}
func (legacy *hybridLegacy) CancelReservation(context.Context, bookingpostgres.ReservationCommandParams) (bookingpostgres.CancelReservationResult, error) {
	legacy.cancelCalls++
	return bookingpostgres.CancelReservationResult{}, nil
}

type hybridControl struct {
	rows  []pgx.Row
	calls int
}

func (control *hybridControl) QueryRow(context.Context, string, ...any) pgx.Row {
	row := control.rows[control.calls]
	control.calls++
	return row
}

type hybridRow struct {
	values []any
	err    error
}

func (row hybridRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	for index := range dest {
		reflect.ValueOf(dest[index]).Elem().Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}
