package app

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestHybridTicketReaderUsesOwnerLocatorThenExactlyOnePhysicalShard(t *testing.T) {
	t.Parallel()
	owner, orderID, reservationID, trainRunID, ticketID, seatID, passengerID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	created := time.Now().UTC().Truncate(time.Microsecond)
	control := &ticketControl{rows: []pgx.Row{ticketRow{values: []any{
		orderID.String(), reservationID.String(), "confirmed", int64(1200), "TWD", created,
		trainRunID, "physical-shard-0", int64(7), "postgres", "active",
	}}}}
	tx := &ticketTx{row: ticketRow{values: []any{orderID.String(), reservationID.String(), "confirmed", int64(1200), "TWD", created}},
		rows: &ticketRows{values: [][]any{{ticketID.String(), "TKT-code", passengerID.String(), seatID.String(), "active"}}}}
	reader, err := NewHybridTicketReader(control, &ticketLegacy{}, ticketRouter(t, trainRunID, 7, tx))
	if err != nil {
		t.Fatal(err)
	}
	record, err := reader.GetTicketOrderRecord(context.Background(), owner, orderID)
	if err != nil || record.ID != orderID.String() || len(record.Tickets) != 1 || !tx.committed {
		t.Fatalf("record=%+v err=%v committed=%v", record, err, tx.committed)
	}
	if !strings.Contains(control.queries[0], "locator.owner_user_id=$2") || !strings.Contains(control.queries[0], "reservation_directory") ||
		tx.options.AccessMode != pgx.ReadOnly {
		t.Fatalf("control=%s options=%+v", control.queries[0], tx.options)
	}
}

func TestHybridTicketReaderListUsesBoundedLocatorPageAndSequentialExactRead(t *testing.T) {
	t.Parallel()
	owner, orderID, reservationID, trainRunID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	created := time.Now().UTC().Truncate(time.Microsecond)
	control := &ticketControl{queryRows: &ticketRows{values: [][]any{{
		orderID.String(), reservationID.String(), "confirmed", int64(800), "TWD", created,
		trainRunID, "physical-shard-0", int64(3), "postgres", "active", int64(1),
	}}}}
	tx := &ticketTx{row: ticketRow{values: []any{orderID.String(), reservationID.String(), "confirmed", int64(800), "TWD", created}},
		rows: &ticketRows{values: [][]any{{uuid.NewString(), "TKT-code", uuid.NewString(), uuid.NewString(), "active"}}}}
	reader, _ := NewHybridTicketReader(control, &ticketLegacy{}, ticketRouter(t, trainRunID, 3, tx))
	page, err := reader.ListTicketOrderRecords(context.Background(), owner, httpapi.PageRequest{Page: 1, Limit: 1000, Sort: "-created_at"})
	if err != nil || len(page.Items) != 1 || page.Total != 1 || !strings.Contains(control.queries[0], "LIMIT $2 OFFSET $3") {
		t.Fatalf("page=%+v err=%v query=%s", page, err, control.queries[0])
	}
	if got := control.arguments[0][1]; got != 100 {
		t.Fatalf("locator page limit = %v, want 100", got)
	}
}

func TestHybridTicketReaderUsesLegacyOnlyForExplicitLegacyLocator(t *testing.T) {
	t.Parallel()
	owner, orderID, reservationID, trainRunID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	created := time.Now().UTC().Truncate(time.Microsecond)
	control := &ticketControl{rows: []pgx.Row{ticketRow{values: []any{
		orderID.String(), reservationID.String(), "confirmed", int64(1200), "TWD", created,
		trainRunID, "booking-shard-0", int64(4), "legacy_schema", "active",
	}}}}
	want := TicketOrderRecord{ID: orderID.String(), ReservationID: reservationID.String(), Status: "confirmed"}
	legacy := &ticketLegacy{record: want}
	tx := &ticketTx{}
	reader, err := NewHybridTicketReader(control, legacy, ticketRouter(t, trainRunID, 4, tx))
	if err != nil {
		t.Fatal(err)
	}
	record, err := reader.GetTicketOrderRecord(context.Background(), owner, orderID)
	if err != nil || record.ID != want.ID || legacy.gets != 1 || tx.committed {
		t.Fatalf("record=%+v err=%v legacy_gets=%d shard_committed=%v", record, err, legacy.gets, tx.committed)
	}
}

func TestHybridTicketReaderFailsClosedWithoutLegacyFallbackForPhysicalLocator(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name           string
		locatorShard   string
		directoryState string
		wantErr        error
	}{
		{name: "route mismatch", locatorShard: "physical-shard-1", directoryState: "active", wantErr: sharding.ErrAssignmentStale},
		{name: "moving directory", locatorShard: "physical-shard-0", directoryState: "moving", wantErr: sharding.ErrWriteFenced},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			owner, orderID, reservationID, trainRunID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
			control := &ticketControl{rows: []pgx.Row{ticketRow{values: []any{
				orderID.String(), reservationID.String(), "confirmed", int64(1200), "TWD", time.Now().UTC(),
				trainRunID, test.locatorShard, int64(4), "postgres", test.directoryState,
			}}}}
			legacy := &ticketLegacy{}
			tx := &ticketTx{}
			reader, err := NewHybridTicketReader(control, legacy, ticketRouter(t, trainRunID, 4, tx))
			if err != nil {
				t.Fatal(err)
			}
			_, err = reader.GetTicketOrderRecord(context.Background(), owner, orderID)
			if !errors.Is(err, test.wantErr) || legacy.gets != 0 || tx.committed {
				t.Fatalf("err=%v legacy_gets=%d shard_committed=%v", err, legacy.gets, tx.committed)
			}
		})
	}
}

func ticketRouter(t *testing.T, trainRunID uuid.UUID, rawGeneration int64, tx pgx.Tx) *ticketPhysicalRouter {
	t.Helper()
	registry, err := shardphysical.NewRegistry(context.Background(), shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: "synthetic"}},
		MaxCount:    1, Limits: shardphysical.PoolLimits{MaxOpenConns: 1},
	}, func(context.Context, string, shardphysical.PoolLimits) (shardphysical.Pool, error) {
		return &ticketPool{tx: tx}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)
	handle, err := registry.Resolve(shardphysical.CatalogEntry{ShardID: sharding.ShardPhysicalZero, StorageKind: shardphysical.StoragePostgres,
		ConnectionRef: "physical-shard-0", ProtocolVersion: 1, SchemaVersion: shardphysical.SupportedSchemaVersion, Enabled: true, WriteEnabled: true,
		HealthState: shardphysical.HealthHealthy, State: shardphysical.StateActive})
	if err != nil {
		t.Fatal(err)
	}
	generation, _ := sharding.NewAssignmentGeneration(rawGeneration)
	route, _ := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	return &ticketPhysicalRouter{resolution: shardphysical.Resolution{Route: route, Handle: handle}}
}

type ticketPhysicalRouter struct{ resolution shardphysical.Resolution }

func (router *ticketPhysicalRouter) Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error) {
	return router.resolution, nil
}

type ticketControl struct {
	rows      []pgx.Row
	queryRows pgx.Rows
	queries   []string
	arguments [][]any
}

func (control *ticketControl) QueryRow(_ context.Context, query string, arguments ...any) pgx.Row {
	control.queries = append(control.queries, query)
	control.arguments = append(control.arguments, arguments)
	row := control.rows[0]
	control.rows = control.rows[1:]
	return row
}
func (control *ticketControl) Query(_ context.Context, query string, arguments ...any) (pgx.Rows, error) {
	control.queries = append(control.queries, query)
	control.arguments = append(control.arguments, arguments)
	return control.queryRows, nil
}

type ticketLegacy struct {
	record TicketOrderRecord
	ticket TicketRecord
	gets   int
}

func (*ticketLegacy) ListTicketOrderRecords(context.Context, uuid.UUID, httpapi.PageRequest) (TicketOrderRecords, error) {
	return TicketOrderRecords{}, nil
}
func (legacy *ticketLegacy) GetTicketOrderRecord(context.Context, uuid.UUID, uuid.UUID) (TicketOrderRecord, error) {
	legacy.gets++
	if legacy.record.ID != "" {
		return legacy.record, nil
	}
	return TicketOrderRecord{}, ErrReadNotFound
}

func TestHybridTicketReaderGetsOwnerScopedTicketFromExactlyOneCurrentPhysicalShard(t *testing.T) {
	t.Parallel()
	owner, orderID, reservationID, trainRunID, ticketID, seatID, passengerID :=
		uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	control := &ticketControl{rows: []pgx.Row{ticketRow{values: []any{
		orderID, reservationID, trainRunID, "physical-shard-0", int64(7), "postgres", "active",
	}}}}
	tx := &ticketTx{row: ticketRow{values: []any{
		ticketID.String(), "opaque-ticket-code", passengerID.String(), seatID.String(), "active",
	}}}
	reader, err := NewHybridTicketReader(control, &ticketLegacy{}, ticketRouter(t, trainRunID, 7, tx))
	if err != nil {
		t.Fatal(err)
	}
	record, err := reader.GetTicketRecord(context.Background(), owner, ticketID)
	if err != nil || record.ID != ticketID.String() || record.TicketCode != "opaque-ticket-code" || !tx.committed {
		t.Fatalf("record=%+v err=%v committed=%v", record, err, tx.committed)
	}
	if len(control.queries) != 1 || !strings.Contains(control.queries[0], "public.ticket_shard_locators") ||
		!strings.Contains(control.queries[0], "locator.owner_user_id=$2") ||
		!strings.Contains(control.queries[0], "reservation_directory") || tx.options.AccessMode != pgx.ReadOnly {
		t.Fatalf("control=%v options=%+v", control.queries, tx.options)
	}
}

func TestHybridTicketReaderRejectsForeignTicketBeforePhysicalRead(t *testing.T) {
	t.Parallel()
	owner, ticketID, trainRunID := uuid.New(), uuid.New(), uuid.New()
	control := &ticketControl{rows: []pgx.Row{ticketRow{err: pgx.ErrNoRows}}}
	tx := &ticketTx{}
	reader, err := NewHybridTicketReader(control, &ticketLegacy{}, ticketRouter(t, trainRunID, 7, tx))
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.GetTicketRecord(context.Background(), owner, ticketID)
	if !errors.Is(err, ErrReadNotFound) || tx.committed || tx.options.AccessMode == pgx.ReadOnly {
		t.Fatalf("err=%v committed=%v options=%+v", err, tx.committed, tx.options)
	}
}
func (legacy *ticketLegacy) GetTicketRecord(context.Context, uuid.UUID, uuid.UUID) (TicketRecord, error) {
	legacy.gets++
	if legacy.ticket.ID != "" {
		return legacy.ticket, nil
	}
	return TicketRecord{}, ErrReadNotFound
}

type ticketPool struct{ tx pgx.Tx }

func (pool *ticketPool) BeginTx(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	pool.tx.(*ticketTx).options = options
	return pool.tx, nil
}
func (*ticketPool) Close() {}

type ticketTx struct {
	pgx.Tx
	row       pgx.Row
	rows      pgx.Rows
	options   pgx.TxOptions
	committed bool
}

func (tx *ticketTx) QueryRow(context.Context, string, ...any) pgx.Row        { return tx.row }
func (tx *ticketTx) Query(context.Context, string, ...any) (pgx.Rows, error) { return tx.rows, nil }
func (tx *ticketTx) Commit(context.Context) error                            { tx.committed = true; return nil }
func (*ticketTx) Rollback(context.Context) error                             { return nil }

type ticketRow struct {
	values []any
	err    error
}

func (row ticketRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	for i := range dest {
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(row.values[i]))
	}
	return nil
}

type ticketRows struct {
	values [][]any
	index  int
}

func (rows *ticketRows) Next() bool { return rows.index < len(rows.values) }
func (rows *ticketRows) Scan(dest ...any) error {
	row := ticketRow{values: rows.values[rows.index]}
	rows.index++
	return row.Scan(dest...)
}
func (*ticketRows) Close()                                       {}
func (*ticketRows) Err() error                                   { return nil }
func (*ticketRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (*ticketRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (*ticketRows) Values() ([]any, error)                       { return nil, nil }
func (*ticketRows) RawValues() [][]byte                          { return nil }
func (*ticketRows) Conn() *pgx.Conn                              { return nil }
