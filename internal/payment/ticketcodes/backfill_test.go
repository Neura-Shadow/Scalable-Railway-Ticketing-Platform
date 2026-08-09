package ticketcodes

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
	"github.com/jackc/pgx/v5/pgconn"
)

func TestInspectIsDryRunAndDoesNotMutateClaimsOrReadiness(t *testing.T) {
	fixture := backfillFixture(t, "physical_ticket_code_001", nil)
	result, err := fixture.backfill.Inspect(context.Background(), 10)
	if err != nil || result.Missing != 1 || result.Claimed != 0 || result.Ready {
		t.Fatalf("Inspect() = (%+v, %v)", result, err)
	}
	if fixture.controlTx.committed || fixture.controlTx.mutations != 0 || fixture.physicalTx.queryCalls != 0 {
		t.Fatalf("inspect committed=%v mutations=%d shard_queries=%d",
			fixture.controlTx.committed, fixture.controlTx.mutations, fixture.physicalTx.queryCalls)
	}
}

func TestBackfillReadsExactPhysicalLocatorAndMarksReady(t *testing.T) {
	fixture := backfillFixture(t, "physical_ticket_code_001", nil)
	result, err := fixture.backfill.Backfill(context.Background(), 10)
	if err != nil || result.Missing != 1 || result.Claimed != 1 || result.Total != 1 || !result.Ready {
		t.Fatalf("Backfill() = (%+v, %v)", result, err)
	}
	if !fixture.controlTx.committed || fixture.controlTx.mutations != 2 || !fixture.physicalTx.committed {
		t.Fatalf("control committed=%v mutations=%d physical committed=%v",
			fixture.controlTx.committed, fixture.controlTx.mutations, fixture.physicalTx.committed)
	}
	if fixture.physicalTx.ticketID != fixture.ticketID || fixture.physicalTx.trainRunID != fixture.trainRunID ||
		fixture.physicalTx.generation != fixture.generation || !fixture.router.forceRefresh {
		t.Fatalf("physical lookup=(%s,%s,%d refresh=%v), want exact locator",
			fixture.physicalTx.ticketID, fixture.physicalTx.trainRunID, fixture.physicalTx.generation, fixture.router.forceRefresh)
	}
}

func TestBackfillPreservesPreviouslyValidLegacyTicketCode(t *testing.T) {
	fixture := backfillFixture(t, "legacy.ticket/code?0001", nil)
	result, err := fixture.backfill.Backfill(context.Background(), 10)
	if err != nil || result.Claimed != 1 || !result.Ready || !fixture.controlTx.committed {
		t.Fatalf("Backfill() = (%+v, %v), committed=%v", result, err, fixture.controlTx.committed)
	}
}

func TestBackfillCollisionRollsBackWithoutReadyUpdate(t *testing.T) {
	fixture := backfillFixture(t, "physical_ticket_code_001", ErrCodeCollision)
	_, err := fixture.backfill.Backfill(context.Background(), 10)
	if !errors.Is(err, ErrCodeCollision) || fixture.controlTx.committed || !fixture.controlTx.rolledBack ||
		fixture.controlTx.readyUpdates != 0 {
		t.Fatalf("error=%v committed=%v rolled_back=%v ready_updates=%d",
			err, fixture.controlTx.committed, fixture.controlTx.rolledBack, fixture.controlTx.readyUpdates)
	}
}

func TestBackfillMissingAuthoritativeTicketFailsClosed(t *testing.T) {
	fixture := backfillFixture(t, "", nil)
	fixture.physicalTx.rowErr = pgx.ErrNoRows
	_, err := fixture.backfill.Backfill(context.Background(), 10)
	if !errors.Is(err, ErrTicketMissing) || fixture.controlTx.committed || fixture.controlTx.mutations != 0 {
		t.Fatalf("error=%v committed=%v mutations=%d", err, fixture.controlTx.committed, fixture.controlTx.mutations)
	}
}

type testBackfillFixture struct {
	backfill   *Backfiller
	controlTx  *backfillControlTx
	physicalTx *backfillPhysicalTx
	router     *backfillRouter
	ticketID   uuid.UUID
	trainRunID uuid.UUID
	generation int64
}

func backfillFixture(t *testing.T, ticketCode string, insertErr error) testBackfillFixture {
	t.Helper()
	ticketID, trainRunID := uuid.New(), uuid.New()
	const generation int64 = 7
	controlTx := &backfillControlTx{locatorRows: [][]any{{ticketID, trainRunID, "physical-shard-0", generation}}, total: 1, insertErr: insertErr}
	physicalTx := &backfillPhysicalTx{ticketCode: ticketCode}
	registry, err := shardphysical.NewRegistry(context.Background(), shardphysical.RegistryConfig{
		Connections: map[string]shardphysical.ConnectionConfig{"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: "test"}},
		MaxCount:    1,
		Limits:      shardphysical.PoolLimits{MaxOpenConns: 1, StatementTimeout: time.Second, LockTimeout: time.Second},
	}, func(context.Context, string, shardphysical.PoolLimits) (shardphysical.Pool, error) {
		return &backfillPool{tx: physicalTx}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := registry.Resolve(shardphysical.CatalogEntry{
		ShardID: sharding.ShardPhysicalZero, StorageKind: shardphysical.StoragePostgres,
		ConnectionRef: "physical-shard-0", ProtocolVersion: shardphysical.SupportedProtocolVersion,
		SchemaVersion: shardphysical.SupportedSchemaVersion, Enabled: true, WriteEnabled: true,
		HealthState: shardphysical.HealthHealthy, State: shardphysical.StateActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	gen, _ := sharding.NewAssignmentGeneration(generation)
	route, _ := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, gen)
	router := &backfillRouter{resolution: shardphysical.Resolution{Route: route, Handle: handle}}
	backfill, err := NewBackfiller(&backfillControl{tx: controlTx}, router)
	if err != nil {
		t.Fatal(err)
	}
	return testBackfillFixture{backfill: backfill, controlTx: controlTx, physicalTx: physicalTx,
		router: router, ticketID: ticketID, trainRunID: trainRunID, generation: generation}
}

type backfillControl struct{ tx *backfillControlTx }

func (control *backfillControl) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return control.tx, nil
}

type backfillControlTx struct {
	pgx.Tx
	locatorRows  [][]any
	total        int64
	insertErr    error
	mutations    int
	readyUpdates int
	committed    bool
	rolledBack   bool
}

func (tx *backfillControlTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	normalized := strings.ToLower(sql)
	if strings.Contains(normalized, "insert into public.ticket_code_directory") {
		if tx.insertErr != nil {
			return pgconn.NewCommandTag("INSERT 0 0"), tx.insertErr
		}
		tx.mutations++
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	}
	if strings.Contains(normalized, "update public.ticket_code_claim_readiness") {
		tx.mutations++
		tx.readyUpdates++
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
	return pgconn.NewCommandTag("SELECT 1"), nil
}

func (tx *backfillControlTx) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return &backfillRows{values: tx.locatorRows}, nil
}

func (tx *backfillControlTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	if strings.Contains(strings.ToLower(sql), "left join public.ticket_code_directory") {
		return backfillRow{values: []any{0}}
	}
	return backfillRow{values: []any{tx.total}}
}

func (tx *backfillControlTx) Commit(context.Context) error { tx.committed = true; return nil }
func (tx *backfillControlTx) Rollback(context.Context) error {
	if !tx.committed {
		tx.rolledBack = true
	}
	return nil
}

type backfillPool struct{ tx pgx.Tx }

func (pool *backfillPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return pool.tx, nil
}
func (*backfillPool) Close() {}

type backfillPhysicalTx struct {
	pgx.Tx
	ticketCode string
	rowErr     error
	ticketID   uuid.UUID
	trainRunID uuid.UUID
	generation int64
	queryCalls int
	committed  bool
}

func (tx *backfillPhysicalTx) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	tx.queryCalls++
	tx.ticketID, _ = args[0].(uuid.UUID)
	tx.trainRunID, _ = args[1].(uuid.UUID)
	tx.generation, _ = args[2].(int64)
	return backfillRow{values: []any{tx.ticketCode}, err: tx.rowErr}
}
func (tx *backfillPhysicalTx) Commit(context.Context) error { tx.committed = true; return nil }
func (*backfillPhysicalTx) Rollback(context.Context) error  { return nil }

type backfillRouter struct {
	resolution   shardphysical.Resolution
	forceRefresh bool
}

func (router *backfillRouter) Resolve(_ context.Context, _ uuid.UUID, force bool) (shardphysical.Resolution, error) {
	router.forceRefresh = force
	return router.resolution, nil
}

type backfillRow struct {
	values []any
	err    error
}

func (row backfillRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != len(row.values) {
		return errors.New("unexpected scan width")
	}
	for index := range destinations {
		reflect.ValueOf(destinations[index]).Elem().Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}

type backfillRows struct {
	pgx.Rows
	values [][]any
	index  int
}

func (*backfillRows) Close()          {}
func (*backfillRows) Err() error      { return nil }
func (rows *backfillRows) Next() bool { return rows.index < len(rows.values) }
func (rows *backfillRows) Scan(destinations ...any) error {
	row := backfillRow{values: rows.values[rows.index]}
	rows.index++
	return row.Scan(destinations...)
}
func (*backfillRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (*backfillRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (*backfillRows) Values() ([]any, error)                       { return nil, nil }
func (*backfillRows) RawValues() [][]byte                          { return nil }
func (*backfillRows) Conn() *pgx.Conn                              { return nil }
