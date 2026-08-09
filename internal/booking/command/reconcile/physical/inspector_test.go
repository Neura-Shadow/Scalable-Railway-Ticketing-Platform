package physical

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/reconcile"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestInspectReadsCommittedReceiptWithoutInventoryMutation(t *testing.T) {
	t.Parallel()
	candidate := physicalCandidate(t)
	tx := &inspectionTx{row: inspectionRow{values: []any{
		candidate.Command.ID, candidate.Command.TrainRunID, candidate.Command.Route.Generation().Int64(),
		string(candidate.Command.Operation), candidate.Command.RequestFingerprint[:], "succeeded",
		pgtype.UUID{Bytes: [16]byte(candidate.Command.ReservationID), Valid: true}, pgtype.Text{},
	}}}
	pool := &inspectionPool{tx: tx}
	inspector := testInspector(t, candidate, pool)

	observation, err := inspector.Inspect(context.Background(), candidate)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if observation.Kind != reconcile.ObservationCommitted ||
		observation.Receipt.ResultResourceID != candidate.Command.ReservationID {
		t.Fatalf("Inspect() = %+v", observation)
	}
	if pool.options.AccessMode != pgx.ReadOnly || !tx.committed {
		t.Fatalf("transaction options = %+v, committed = %v", pool.options, tx.committed)
	}
	if strings.Contains(strings.ToLower(tx.query), "seat_inventory") || !strings.Contains(tx.query, "booking_command_receipts") {
		t.Fatalf("unexpected inspection query = %q", tx.query)
	}
}

func TestInspectReturnsMissingOnlyAfterReachableNoRows(t *testing.T) {
	t.Parallel()
	candidate := physicalCandidate(t)
	tx := &inspectionTx{row: inspectionRow{err: pgx.ErrNoRows}}
	inspector := testInspector(t, candidate, &inspectionPool{tx: tx})

	observation, err := inspector.Inspect(context.Background(), candidate)
	if err != nil || observation.Kind != reconcile.ObservationMissing || !tx.committed {
		t.Fatalf("Inspect() = (%+v, %v), committed = %v", observation, err, tx.committed)
	}
}

func TestInspectConfirmationReturnsAuthoritativeTicketOrderSummary(t *testing.T) {
	t.Parallel()
	candidate := physicalCandidate(t)
	candidate.Command.Operation = command.OperationConfirmReservation
	orderID := uuid.New()
	createdAt := time.Now().UTC()
	tx := &inspectionTx{rows: []pgx.Row{
		inspectionRow{values: []any{
			candidate.Command.ID, candidate.Command.TrainRunID, candidate.Command.Route.Generation().Int64(),
			string(candidate.Command.Operation), candidate.Command.RequestFingerprint[:], "succeeded",
			pgtype.UUID{Bytes: [16]byte(candidate.Command.ReservationID), Valid: true}, pgtype.Text{},
		}},
		inspectionRow{values: []any{orderID, 2, int64(2400), "TWD", createdAt}},
	}, queryRows: &inspectionRows{values: [][]any{{uuid.New()}, {uuid.New()}}}}
	inspector := testInspector(t, candidate, &inspectionPool{tx: tx})

	observation, err := inspector.Inspect(context.Background(), candidate)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	receipt := observation.Receipt
	if observation.Kind != reconcile.ObservationCommitted || receipt.TicketOrderID != orderID ||
		receipt.TicketCount != 2 || len(receipt.TicketIDs) != 2 || receipt.TotalAmountMinor != 2400 || receipt.Currency != "TWD" ||
		!receipt.OrderCreatedAt.Equal(createdAt) || tx.queryCount != 2 {
		t.Fatalf("Inspect() = %+v, queries = %d", observation, tx.queryCount)
	}
}

func testInspector(t *testing.T, candidate reconcile.Candidate, pool *inspectionPool) *Inspector {
	t.Helper()
	registry, err := shardphysical.NewRegistry(context.Background(), shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{
			"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: "postgres://shard-0"},
		},
		MaxCount: 1, Limits: shardphysical.PoolLimits{MaxOpenConns: 1},
	}, func(context.Context, string, shardphysical.PoolLimits) (shardphysical.Pool, error) { return pool, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)
	handle, err := registry.Resolve(shardphysical.CatalogEntry{
		ShardID: sharding.ShardPhysicalZero, StorageKind: shardphysical.StoragePostgres,
		ConnectionRef: "physical-shard-0", ProtocolVersion: 1, SchemaVersion: shardphysical.SupportedSchemaVersion,
		Enabled: true, WriteEnabled: true, HealthState: shardphysical.HealthHealthy,
		State: shardphysical.StateActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	inspector, err := NewInspector(&inspectionResolver{handle: handle})
	if err != nil {
		t.Fatal(err)
	}
	return inspector
}

func physicalCandidate(t *testing.T) reconcile.Candidate {
	t.Helper()
	trainRunID := uuid.New()
	generation, _ := sharding.NewAssignmentGeneration(3)
	route, err := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	if err != nil {
		t.Fatal(err)
	}
	return reconcile.Candidate{Command: command.Command{
		ID: uuid.New(), Operation: command.OperationCreateReservation, OwnerUserID: uuid.New(),
		TrainRunID: trainRunID, ReservationID: uuid.New(), Route: route,
		RequestFingerprint: [32]byte{7}, State: command.StateExecuting,
	}, QuotaExpiresAt: time.Now().Add(time.Minute)}
}

type inspectionResolver struct {
	handle shardphysical.Handle
	err    error
}

func (resolver *inspectionResolver) ResolveHandle(context.Context, sharding.ShardID) (shardphysical.Handle, error) {
	return resolver.handle, resolver.err
}

type inspectionPool struct {
	tx      pgx.Tx
	options pgx.TxOptions
}

func (pool *inspectionPool) BeginTx(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	pool.options = options
	return pool.tx, nil
}
func (*inspectionPool) Close() {}

type inspectionTx struct {
	pgx.Tx
	row        pgx.Row
	rows       []pgx.Row
	queryRows  pgx.Rows
	queryCount int
	query      string
	committed  bool
}

func (tx *inspectionTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return tx.queryRows, nil
}

func (tx *inspectionTx) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	tx.query = query
	if tx.queryCount < len(tx.rows) {
		row := tx.rows[tx.queryCount]
		tx.queryCount++
		return row
	}
	tx.queryCount++
	return tx.row
}
func (tx *inspectionTx) Commit(context.Context) error { tx.committed = true; return nil }
func (*inspectionTx) Rollback(context.Context) error  { return pgx.ErrTxClosed }
func (*inspectionTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("read-only inspector attempted mutation")
}

type inspectionRow struct {
	values []any
	err    error
}

type inspectionRows struct {
	values [][]any
	index  int
}

func (rows *inspectionRows) Next() bool { return rows.index < len(rows.values) }
func (rows *inspectionRows) Scan(destinations ...any) error {
	row := inspectionRow{values: rows.values[rows.index]}
	rows.index++
	return row.Scan(destinations...)
}
func (*inspectionRows) Close()                                       {}
func (*inspectionRows) Err() error                                   { return nil }
func (*inspectionRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (*inspectionRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (*inspectionRows) Values() ([]any, error)                       { return nil, nil }
func (*inspectionRows) RawValues() [][]byte                          { return nil }
func (*inspectionRows) Conn() *pgx.Conn                              { return nil }

func (row inspectionRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != len(row.values) {
		return errors.New("scan arity mismatch")
	}
	for index, destination := range destinations {
		switch pointer := destination.(type) {
		case *uuid.UUID:
			*pointer = row.values[index].(uuid.UUID)
		case *int64:
			*pointer = row.values[index].(int64)
		case *int:
			*pointer = row.values[index].(int)
		case *time.Time:
			*pointer = row.values[index].(time.Time)
		case *[]byte:
			*pointer = append([]byte(nil), row.values[index].([]byte)...)
		case *string:
			*pointer = row.values[index].(string)
		case *pgtype.UUID:
			*pointer = row.values[index].(pgtype.UUID)
		case *pgtype.Text:
			*pointer = row.values[index].(pgtype.Text)
		default:
			return errors.New("unsupported scan destination")
		}
	}
	return nil
}
