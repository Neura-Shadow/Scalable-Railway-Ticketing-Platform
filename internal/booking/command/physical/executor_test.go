package physical_test

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	commandphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/physical"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestExecutorCommitsReservationReceiptOutboxAndWriteEvidenceTogether(t *testing.T) {
	t.Parallel()

	cmd, resolution := commandFixture(t)
	fareID := uuid.New()
	seatID := uuid.New()
	tx := &scriptedTx{rows: []scriptedRow{
		{err: pgx.ErrNoRows},
		{values: []any{int64(7), true, "active", true, int64(3), "scheduled", 3}},
		{err: pgx.ErrNoRows},
		{values: []any{fareID, int64(1200), "TWD"}},
		{values: []any{seatID}},
	}}
	resolution.Handle = handleForTx(t, tx, true)
	router := &routeResolver{resolution: resolution}
	executor, err := commandphysical.NewExecutor(router, commandphysical.Options{MaxHoldTTL: 30 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}

	receipt, err := executor.Execute(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if receipt.Status != command.ReceiptCommitted || receipt.CommandID != cmd.ID || receipt.ResultResourceID != cmd.ReservationID {
		t.Fatalf("receipt = %+v", receipt)
	}
	if tx.commits != 1 || tx.rollbacks != 0 {
		t.Fatalf("transaction finalization = commits %d rollbacks %d", tx.commits, tx.rollbacks)
	}
	joined := strings.Join(tx.execs, "\n")
	for _, table := range []string{"booking_command_receipts", "reservations", "reservation_seats", "outbox_events", "train_run_target_write_evidence"} {
		if !strings.Contains(joined, table) {
			t.Fatalf("transaction did not mutate %s; SQL = %s", table, joined)
		}
	}
}

func TestExecutorRejectsARetainedPhysicalSourceFenceBeforeAnyCommandWrite(t *testing.T) {
	t.Parallel()

	cmd, resolution := commandFixture(t)
	tx := &scriptedTx{rows: []scriptedRow{
		{err: pgx.ErrNoRows},
		{values: []any{int64(7), false, "retained", true, int64(3), "scheduled", 3}},
	}}
	resolution.Handle = handleForTx(t, tx, true)
	executor, err := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}

	_, err = executor.Execute(context.Background(), cmd)
	if !errors.Is(err, sharding.ErrWriteFenced) {
		t.Fatalf("Execute() error = %v, want ErrWriteFenced", err)
	}
	if len(tx.execs) != 0 || tx.commits != 0 || tx.rollbacks == 0 {
		t.Fatalf("fenced command wrote SQL=%v commits=%d rollbacks=%d", tx.execs, tx.commits, tx.rollbacks)
	}
}

func TestExecutorCancellationRollsBackPartialSeatRelease(t *testing.T) {
	t.Parallel()
	cmd, resolution := commandFixture(t)
	cmd.Operation, cmd.Payload = command.OperationCancelReservation, command.CreateReservationPayload{}
	tx := &scriptedTx{rows: []scriptedRow{
		{err: pgx.ErrNoRows}, {values: []any{int64(7), true, "active", "scheduled"}}, {err: pgx.ErrNoRows},
		{values: []any{"held", int64(1200), "TWD"}}, {values: []any{2}},
	}}
	resolution.Handle = handleForTx(t, tx, true)
	executor, _ := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	_, err := executor.Execute(context.Background(), cmd)
	if !errors.Is(err, commandphysical.ErrShardPersistence) || tx.commits != 0 || tx.rollbacks == 0 {
		t.Fatalf("err=%v commits=%d rollbacks=%d", err, tx.commits, tx.rollbacks)
	}
}

func TestExecutorReturnsCommittedReceiptWithoutReapplyingMutation(t *testing.T) {
	t.Parallel()

	cmd, resolution := commandFixture(t)
	cmd.Payload.HoldExpiresAt = time.Now().UTC().Add(-time.Minute)
	tx := &scriptedTx{rows: []scriptedRow{{values: []any{cmd.RequestFingerprint[:], cmd.ReservationID, "succeeded"}}}}
	resolution.Handle = handleForTx(t, tx, false)
	executor, err := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}

	receipt, err := executor.Execute(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if receipt.Status != command.ReceiptCommitted || len(tx.execs) != 0 || tx.commits != 1 {
		t.Fatalf("replay receipt = %+v, execs = %d, commits = %d", receipt, len(tx.execs), tx.commits)
	}
}

func TestExecutorRefreshesOnceAndRejectsChangedControlAssignment(t *testing.T) {
	t.Parallel()

	cmd, resolution := commandFixture(t)
	staleGeneration, _ := sharding.NewAssignmentGeneration(6)
	staleRoute, _ := sharding.NewShardRoute(cmd.TrainRunID, sharding.ShardPhysicalOne, staleGeneration)
	changedGeneration, _ := sharding.NewAssignmentGeneration(8)
	changedRoute, _ := sharding.NewShardRoute(cmd.TrainRunID, sharding.ShardPhysicalOne, changedGeneration)
	resolution.Route = staleRoute
	router := &routeResolver{resolution: resolution, refreshed: shardphysical.Resolution{Route: changedRoute}}
	executor, err := commandphysical.NewExecutor(router, commandphysical.Options{MaxHoldTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}

	_, err = executor.Execute(context.Background(), cmd)
	if !errors.Is(err, sharding.ErrAssignmentStale) {
		t.Fatalf("Execute() error = %v, want %v", err, sharding.ErrAssignmentStale)
	}
	if !reflect.DeepEqual(router.force, []bool{false, true}) {
		t.Fatalf("force refresh calls = %v", router.force)
	}
}

func TestExecutorCancellationCommitsReceiptMutationOutboxAndEvidenceTogether(t *testing.T) {
	t.Parallel()
	cmd, resolution := commandFixture(t)
	cmd.Operation, cmd.Payload = command.OperationCancelReservation, command.CreateReservationPayload{}
	tx := &scriptedTx{rows: []scriptedRow{
		{err: pgx.ErrNoRows},
		{values: []any{int64(7), true, "active", "scheduled"}},
		{err: pgx.ErrNoRows},
		{values: []any{"held", int64(1200), "TWD"}},
		{values: []any{2}},
	}, affectedByContains: map[string]int64{"UPDATE seat_inventory": 2}}
	resolution.Handle = handleForTx(t, tx, true)
	executor, err := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := executor.Execute(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Execute() error=%v", err)
	}
	if receipt.ReleasedSeatCount != 2 || tx.commits != 1 || tx.rollbacks != 0 {
		t.Fatalf("receipt=%+v commits=%d rollbacks=%d", receipt, tx.commits, tx.rollbacks)
	}
	joined := strings.Join(tx.execs, "\n")
	for _, fragment := range []string{"booking_command_receipts", "UPDATE reservations", "seat_inventory", "outbox_events", "train_run_target_write_evidence"} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("missing %q in SQL: %s", fragment, joined)
		}
	}
}

func TestExecutorLifecycleDuplicateReturnsReceiptWithoutMutation(t *testing.T) {
	t.Parallel()
	cmd, resolution := commandFixture(t)
	cmd.Operation, cmd.Payload = command.OperationCancelReservation, command.CreateReservationPayload{}
	tx := &scriptedTx{rows: []scriptedRow{
		{values: []any{cmd.RequestFingerprint[:], cmd.ReservationID, "succeeded"}},
		{values: []any{2}},
	}}
	resolution.Handle = handleForTx(t, tx, false)
	executor, _ := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	receipt, err := executor.Execute(context.Background(), cmd)
	if err != nil || receipt.ReleasedSeatCount != 2 || len(tx.execs) != 0 || tx.commits != 1 {
		t.Fatalf("receipt=%+v err=%v execs=%d commits=%d", receipt, err, len(tx.execs), tx.commits)
	}
}

func TestExecutorLifecycleRejectsGenerationRaceBeforeMutation(t *testing.T) {
	t.Parallel()
	cmd, resolution := commandFixture(t)
	cmd.Operation, cmd.Payload = command.OperationConfirmReservation, command.CreateReservationPayload{}
	tx := &scriptedTx{rows: []scriptedRow{
		{err: pgx.ErrNoRows}, {values: []any{int64(8), true, "active", "scheduled"}},
		{err: pgx.ErrNoRows}, {values: []any{int64(8), true, "active", "scheduled"}},
	}}
	resolution.Handle = handleForTx(t, tx, true)
	executor, _ := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	_, err := executor.Execute(context.Background(), cmd)
	if !errors.Is(err, sharding.ErrAssignmentStale) || len(tx.execs) != 0 || tx.commits != 0 {
		t.Fatalf("err=%v execs=%d commits=%d", err, len(tx.execs), tx.commits)
	}
}

func TestTrainRunCancellationCommitsLocalSnapshotBeforeControlAndKeepsRelayFenceActive(t *testing.T) {
	t.Parallel()
	cmd, resolution := commandFixture(t)
	tx := &scriptedTx{rows: []scriptedRow{
		{err: pgx.ErrNoRows}, {values: []any{int64(7), true, "active", "scheduled"}},
	}}
	resolution.Handle = handleForTx(t, tx, true)
	executor, _ := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	if err := executor.CancelTrainRun(context.Background(), cmd.TrainRunID); err != nil {
		t.Fatalf("CancelTrainRun() error=%v", err)
	}
	joined := strings.Join(tx.execs, "\n")
	for _, fragment := range []string{"booking_command_receipts", "UPDATE train_run_booking_snapshots", "outbox_events", "train_run_target_write_evidence"} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("missing %q in SQL: %s", fragment, joined)
		}
	}
	if strings.Contains(joined, "UPDATE train_run_write_fences") {
		t.Fatalf("cancellation disabled relay/lifecycle fence: %s", joined)
	}
}

func TestExecutorConfirmationEmitsOneDeterministicEventPerTicket(t *testing.T) {
	t.Parallel()
	cmd, resolution := commandFixture(t)
	cmd.Operation, cmd.Payload = command.OperationConfirmReservation, command.CreateReservationPayload{}
	seatA, seatB := uuid.New(), uuid.New()
	created := time.Now().UTC()
	tx := &scriptedTx{rows: []scriptedRow{
		{err: pgx.ErrNoRows}, {values: []any{int64(7), true, "active", "scheduled"}}, {err: pgx.ErrNoRows},
		{values: []any{"held", int64(2400), "TWD"}},
		{values: []any{uuid.NewSHA1(cmd.ID, []byte("ticket-order"))}}, {values: []any{created}},
	}, queryRows: []pgx.Rows{
		&scriptedRows{values: [][]any{{seatA}, {seatB}}},
		&scriptedRows{values: [][]any{{uuid.NewSHA1(cmd.ID, seatA[:])}, {uuid.NewSHA1(cmd.ID, seatB[:])}}},
	}}
	resolution.Handle = handleForTx(t, tx, true)
	executor, _ := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	receipt, err := executor.Execute(context.Background(), cmd)
	if err != nil || receipt.TicketCount != 2 || len(receipt.TicketIDs) != 2 ||
		receipt.Currency != "TWD" || receipt.OrderCreatedAt != created {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
	wantIDs := map[uuid.UUID]bool{}
	for _, seat := range []uuid.UUID{seatA, seatB} {
		ticketID := uuid.NewSHA1(cmd.ID, seat[:])
		wantIDs[uuid.NewSHA1(ticketID, []byte("ticket.created"))] = true
	}
	seen := 0
	for index, query := range tx.execs {
		if strings.Contains(query, "'ticket.created'") {
			seen++
			eventID, ok := tx.execArguments[index][0].(uuid.UUID)
			if !ok || !wantIDs[eventID] {
				t.Fatalf("unexpected ticket event id=%v", tx.execArguments[index][0])
			}
		}
	}
	if seen != 2 {
		t.Fatalf("ticket.created events=%d", seen)
	}
}

func TestExecutorAlternateConfirmationKeyReplaysAuthoritativeTicketIDsWithoutDuplicateEvents(t *testing.T) {
	t.Parallel()
	cmd, resolution := commandFixture(t)
	cmd.Operation, cmd.Payload = command.OperationConfirmReservation, command.CreateReservationPayload{}
	seatA, seatB := uuid.New(), uuid.New()
	orderID, ticketA, ticketB := uuid.New(), uuid.New(), uuid.New()
	created := time.Now().UTC()
	tx := &scriptedTx{rows: []scriptedRow{
		{err: pgx.ErrNoRows}, {values: []any{int64(7), true, "active", "scheduled"}}, {err: pgx.ErrNoRows},
		{values: []any{"confirmed", int64(2400), "TWD"}},
		{values: []any{orderID}}, {values: []any{created}},
	}, queryRows: []pgx.Rows{
		&scriptedRows{values: [][]any{{seatA}, {seatB}}},
		&scriptedRows{values: [][]any{{ticketA}, {ticketB}}},
	}}
	resolution.Handle = handleForTx(t, tx, true)
	executor, _ := commandphysical.NewExecutor(&routeResolver{resolution: resolution}, commandphysical.Options{MaxHoldTTL: time.Hour})
	receipt, err := executor.Execute(context.Background(), cmd)
	if err != nil || receipt.TicketOrderID != orderID || !reflect.DeepEqual(receipt.TicketIDs, []uuid.UUID{ticketA, ticketB}) {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
	for _, query := range tx.execs {
		if strings.Contains(query, "'ticket.created'") {
			t.Fatalf("alternate confirmation emitted duplicate ticket event: %s", query)
		}
	}
}

func commandFixture(t *testing.T) (command.Command, shardphysical.Resolution) {
	t.Helper()
	trainRunID := uuid.New()
	generation, _ := sharding.NewAssignmentGeneration(7)
	route, _ := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	cmd := command.Command{
		ID: uuid.New(), Operation: command.OperationCreateReservation, OwnerUserID: uuid.New(), TrainRunID: trainRunID,
		ReservationID: uuid.New(), Route: route, RequestFingerprint: [32]byte{1, 2, 3}, State: command.StateReserved,
		Payload: command.CreateReservationPayload{
			FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard", PassengerIDs: []uuid.UUID{uuid.New()},
			HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute), ExpectedSnapshotVersion: 3,
		},
	}
	return cmd, shardphysical.Resolution{Route: route}
}

func handleForTx(t *testing.T, tx pgx.Tx, writeEnabled bool) shardphysical.Handle {
	t.Helper()
	registry, err := shardphysical.NewRegistry(context.Background(), shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{
			"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: "postgres://shard-0"},
		},
		MaxCount: 1, Limits: shardphysical.PoolLimits{MaxOpenConns: 2, MaxIdleConns: 1},
	}, func(context.Context, string, shardphysical.PoolLimits) (shardphysical.Pool, error) {
		return &executorPool{tx: tx}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)
	handle, err := registry.Resolve(shardphysical.CatalogEntry{
		ShardID: sharding.ShardPhysicalZero, StorageKind: shardphysical.StoragePostgres, ConnectionRef: "physical-shard-0",
		ProtocolVersion: 1, SchemaVersion: 1, Enabled: true, WriteEnabled: writeEnabled,
		HealthState: shardphysical.HealthHealthy, State: shardphysical.StateActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

type routeResolver struct {
	resolution shardphysical.Resolution
	refreshed  shardphysical.Resolution
	force      []bool
}

func (resolver *routeResolver) Resolve(_ context.Context, _ uuid.UUID, force bool) (shardphysical.Resolution, error) {
	resolver.force = append(resolver.force, force)
	if force && resolver.refreshed.Route.TrainRunID() != uuid.Nil {
		return resolver.refreshed, nil
	}
	return resolver.resolution, nil
}

type executorPool struct{ tx pgx.Tx }

func (pool *executorPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return pool.tx, nil
}
func (*executorPool) Close() {}

type scriptedTx struct {
	pgx.Tx
	rows               []scriptedRow
	rowIndex           int
	execs              []string
	execArguments      [][]any
	queryRows          []pgx.Rows
	queryIndex         int
	commits            int
	rollbacks          int
	affectedByContains map[string]int64
}

func (tx *scriptedTx) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	row := tx.rows[tx.rowIndex]
	tx.rowIndex++
	return row
}
func (tx *scriptedTx) Exec(_ context.Context, query string, arguments ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, query)
	tx.execArguments = append(tx.execArguments, arguments)
	for fragment, count := range tx.affectedByContains {
		if strings.Contains(query, fragment) {
			return pgconn.NewCommandTag("UPDATE " + strconv.FormatInt(count, 10)), nil
		}
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}
func (tx *scriptedTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	if tx.queryIndex >= len(tx.queryRows) {
		return nil, errors.New("unexpected query")
	}
	rows := tx.queryRows[tx.queryIndex]
	tx.queryIndex++
	return rows, nil
}
func (tx *scriptedTx) Commit(context.Context) error   { tx.commits++; return nil }
func (tx *scriptedTx) Rollback(context.Context) error { tx.rollbacks++; return nil }

type scriptedRow struct {
	values []any
	err    error
}

func (row scriptedRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	for index := range dest {
		reflect.ValueOf(dest[index]).Elem().Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}

type scriptedRows struct {
	values [][]any
	index  int
}

func (rows *scriptedRows) Next() bool { return rows.index < len(rows.values) }
func (rows *scriptedRows) Scan(dest ...any) error {
	row := scriptedRow{values: rows.values[rows.index]}
	rows.index++
	return row.Scan(dest...)
}
func (*scriptedRows) Close()                                       {}
func (*scriptedRows) Err() error                                   { return nil }
func (*scriptedRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (*scriptedRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (*scriptedRows) Values() ([]any, error)                       { return nil, nil }
func (*scriptedRows) RawValues() [][]byte                          { return nil }
func (*scriptedRows) Conn() *pgx.Conn                              { return nil }
