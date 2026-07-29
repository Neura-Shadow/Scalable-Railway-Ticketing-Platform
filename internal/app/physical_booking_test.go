package app

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	bookingcommand "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	commandphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/physical"
	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestMapPhysicalCommandErrorPreservesTerminalDomainSemantics(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		input error
		want  error
	}{
		{input: commandphysical.ErrFareUnavailable, want: bookingpostgres.ErrNotBookable},
		{input: commandphysical.ErrInsufficientInventory, want: bookingpostgres.ErrInsufficientInventory},
		{input: commandphysical.ErrReservationExpired, want: bookingpostgres.ErrReservationExpired},
		{input: commandphysical.ErrInvalidLifecycleState, want: bookingpostgres.ErrInvalidState},
	} {
		if got := mapPhysicalCommandError(errors.Join(bookingcommand.ErrShardExecution, testCase.input)); !errors.Is(got, testCase.want) {
			t.Errorf("mapPhysicalCommandError(%v) = %v, want %v", testCase.input, got, testCase.want)
		}
	}
}

func TestHybridReservationCommandsRoutesPhysicalCreateWithControlSnapshotVersion(t *testing.T) {
	t.Parallel()
	owner, trainRunID, passengerID, reservationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	legacy := &hybridLegacy{}
	saga := &hybridSaga{result: bookingcommand.Result{ReservationID: reservationID}}
	router, pool, snapshotTx := hybridSnapshotRouter(t, trainRunID, 4, 7)
	commands, err := NewHybridReservationCommands(
		&hybridControl{rows: []pgx.Row{hybridRow{values: []any{"postgres"}}}},
		legacy,
		saga,
		router,
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
	if pool.options.AccessMode != pgx.ReadOnly || strings.Contains(snapshotTx.query, "FOR SHARE") {
		t.Fatalf("snapshot read options=%+v query=%q; read-only PostgreSQL transactions cannot take row locks", pool.options, snapshotTx.query)
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
	if !strings.Contains(commands.control.(*hybridControl).query, "'migrating'") {
		t.Fatalf("legacy source is not routable during online base copy: %s", commands.control.(*hybridControl).query)
	}
}

func TestHybridReservationCommandsReportsDrainAsRebalancing(t *testing.T) {
	t.Parallel()
	legacy := &hybridLegacy{}
	saga := &hybridSaga{}
	commands, err := NewHybridReservationCommands(
		&hybridControl{rows: []pgx.Row{
			hybridRow{err: pgx.ErrNoRows},
			hybridRow{values: []any{"draining"}},
		}},
		legacy,
		saga,
		&hybridPhysicalRouter{},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = commands.CreateHold(context.Background(), bookingpostgres.CreateHoldParams{TrainRunID: uuid.New()})
	if !errors.Is(err, sharding.ErrTrainRunMigrating) || legacy.createCalls != 0 || saga.calls != 0 {
		t.Fatalf("CreateHold() error=%v legacy=%d saga=%d", err, legacy.createCalls, saga.calls)
	}
}

func TestHybridReservationCommandsReplaysCopiedLegacyIdempotencyAfterPhysicalCutover(t *testing.T) {
	t.Parallel()
	owner, trainRunID, reservationID := uuid.New(), uuid.New(), uuid.New()
	fingerprint := makeHash(2)
	router, _, targetTx := hybridSnapshotRouter(t, trainRunID, 4, 7)
	targetTx.rows = []pgx.Row{hybridRow{values: []any{
		fingerprint, "completed", reservationID, 1,
	}}}
	commands, err := NewHybridReservationCommands(
		&hybridControl{rows: []pgx.Row{
			hybridRow{values: []any{"postgres"}},
			hybridRow{err: pgx.ErrNoRows},
		}},
		&hybridLegacy{},
		&hybridSaga{},
		router,
	)
	if err != nil {
		t.Fatal(err)
	}
	result, found, err := commands.LookupCompletedCreateHold(context.Background(), bookingpostgres.CompletedCreateHoldLookupParams{
		UserID: owner, TrainRunID: trainRunID,
		IdempotencyKeyHash: makeHash(1), RequestFingerprint: fingerprint,
	})
	if err != nil || !found || !result.Replayed || result.ReservationID != reservationID || result.SeatCount != 1 {
		t.Fatalf("LookupCompletedCreateHold() result=%+v found=%v error=%v", result, found, err)
	}
	if !strings.Contains(targetTx.query, "FROM public.idempotency_records") || targetTx.commits != 1 {
		t.Fatalf("target replay query=%q commits=%d", targetTx.query, targetTx.commits)
	}
}

func TestHybridReservationCommandsRoutesPhysicalLifecycleWithoutLegacyFallback(t *testing.T) {
	t.Parallel()
	legacy := &hybridLegacy{}
	commands, err := NewHybridReservationCommands(
		&hybridControl{rows: []pgx.Row{hybridRow{values: []any{"postgres"}}}},
		legacy,
		&hybridSaga{result: bookingcommand.Result{ReservationID: uuid.New(), ReleasedSeats: 2}},
		&hybridPhysicalRouter{},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := commands.CancelReservation(context.Background(), bookingpostgres.ReservationCommandParams{
		UserID: uuid.New(), ReservationID: uuid.New(), IdempotencyKeyHash: makeHash(4), RequestFingerprint: makeHash(5),
	})
	if err != nil || legacy.cancelCalls != 0 || result.ReleasedSeatCount != 2 {
		t.Fatalf("CancelReservation() result=%+v error=%v legacy calls=%d", result, err, legacy.cancelCalls)
	}
}

func TestPhysicalTrainRunCancellationPreservesLegacyAndEnforcesPhysical(t *testing.T) {
	t.Parallel()
	runID := uuid.New()
	executor := &hybridTrainRunCancellation{}
	legacy, err := NewPhysicalTrainRunCancellation(
		&hybridControl{rows: []pgx.Row{hybridRow{values: []any{"legacy_schema", "stable"}}}}, executor)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.CancelTrainRun(context.Background(), runID); err != nil || executor.calls != 0 {
		t.Fatalf("legacy cancellation err=%v calls=%d", err, executor.calls)
	}
	physical, _ := NewPhysicalTrainRunCancellation(
		&hybridControl{rows: []pgx.Row{hybridRow{values: []any{"postgres", "stable"}}}}, executor)
	if err := physical.CancelTrainRun(context.Background(), runID); err != nil || executor.calls != 1 || executor.runID != runID {
		t.Fatalf("physical cancellation err=%v calls=%d run=%v", err, executor.calls, executor.runID)
	}
	draining, _ := NewPhysicalTrainRunCancellation(
		&hybridControl{rows: []pgx.Row{hybridRow{values: []any{"postgres", "draining"}}}}, executor)
	if err := draining.CancelTrainRun(context.Background(), runID); !errors.Is(err, sharding.ErrWriteFenced) || executor.calls != 1 {
		t.Fatalf("draining cancellation err=%v calls=%d", err, executor.calls)
	}
	migrating, _ := NewPhysicalTrainRunCancellation(
		&hybridControl{rows: []pgx.Row{hybridRow{values: []any{"postgres", "migrating"}}}}, executor)
	if err := migrating.CancelTrainRun(context.Background(), runID); err != nil || executor.calls != 2 {
		t.Fatalf("migrating source cancellation err=%v calls=%d", err, executor.calls)
	}
}

func hybridSnapshotRouter(t *testing.T, trainRunID uuid.UUID, generation, version int64) (*hybridPhysicalRouter, *hybridReadPool, *hybridReadTx) {
	t.Helper()
	snapshotTx := &hybridReadTx{row: hybridRow{values: []any{version}}}
	pool := &hybridReadPool{tx: snapshotTx}
	registry, err := shardphysical.NewRegistry(context.Background(), shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{
			"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: "synthetic-shard-0"},
		},
		MaxCount: 1,
		Limits:   shardphysical.PoolLimits{MaxOpenConns: 1},
	}, func(context.Context, string, shardphysical.PoolLimits) (shardphysical.Pool, error) {
		return pool, nil
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
	return &hybridPhysicalRouter{resolution: shardphysical.Resolution{Route: route, Handle: handle}}, pool, snapshotTx
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

func (saga *hybridSaga) ExecuteLifecycle(_ context.Context, request bookingcommand.LifecycleRequest) (bookingcommand.Result, error) {
	saga.calls++
	return saga.result, saga.err
}

type hybridLegacy struct {
	createResult                           bookingpostgres.CreateHoldResult
	createCalls, confirmCalls, cancelCalls int
}

type hybridTrainRunCancellation struct {
	calls int
	runID uuid.UUID
}

func (executor *hybridTrainRunCancellation) CancelTrainRun(_ context.Context, runID uuid.UUID) error {
	executor.calls++
	executor.runID = runID
	return nil
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
	query string
}

func (control *hybridControl) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	control.query = query
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
