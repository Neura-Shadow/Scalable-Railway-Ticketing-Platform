package app

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestShardedTicketOrderReadRequiresAuthoritativeLocalOrder(t *testing.T) {
	record := ticketOrderReadRecord()
	tx := &ticketOrderReadPGXTx{row: ticketOrderAuthorityRow{err: pgx.ErrNoRows}}
	reads := ticketOrderReadsForTest(t, record.ID, tx)

	_, err := reads.loadTickets(context.Background(), uuid.MustParse(ticketOrderReadOwnerID), record)
	if !errors.Is(err, ErrReadNotFound) {
		t.Fatalf("loadTickets() error = %v, want ErrReadNotFound", err)
	}
	if tx.queryCalls != 0 {
		t.Fatalf("ticket query calls = %d, want 0 before authoritative owner proof", tx.queryCalls)
	}
	if tx.commitCalls != 0 || tx.rollbackCalls != 1 {
		t.Fatalf("transaction calls commit=%d rollback=%d, want commit=0 rollback=1", tx.commitCalls, tx.rollbackCalls)
	}
}

func TestShardedTicketOrderReadRejectsLocatorSummaryMismatch(t *testing.T) {
	record := ticketOrderReadRecord()
	authoritative := record
	authoritative.Status = "cancelled"
	tx := &ticketOrderReadPGXTx{row: ticketOrderAuthorityRow{record: authoritative}}
	reads := ticketOrderReadsForTest(t, record.ID, tx)

	_, err := reads.loadTickets(context.Background(), uuid.MustParse(ticketOrderReadOwnerID), record)
	if !errors.Is(err, ErrReadNotFound) {
		t.Fatalf("loadTickets() error = %v, want safe ErrReadNotFound", err)
	}
	if tx.queryCalls != 0 {
		t.Fatalf("ticket query calls = %d, want 0 for inconsistent locator", tx.queryCalls)
	}
}

func TestShardedTicketOrderReadReturnsAfterAuthoritativeOwnerAndSummaryProof(t *testing.T) {
	record := ticketOrderReadRecord()
	tx := &ticketOrderReadPGXTx{
		row:  ticketOrderAuthorityRow{record: record},
		rows: &emptyTicketRows{},
	}
	reads := ticketOrderReadsForTest(t, record.ID, tx)

	tickets, err := reads.loadTickets(context.Background(), uuid.MustParse(ticketOrderReadOwnerID), record)
	if err != nil {
		t.Fatalf("loadTickets() error = %v", err)
	}
	if len(tickets) != 0 {
		t.Fatalf("tickets = %v, want empty authoritative result", tickets)
	}
	if tx.queryCalls != 1 || tx.commitCalls != 1 {
		t.Fatalf("transaction calls query=%d commit=%d, want query=1 commit=1", tx.queryCalls, tx.commitCalls)
	}
}

func TestPostgresReadsRejectLocatorWithoutAuthoritativeTicketOrderIntegration(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse PostgreSQL integration configuration: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = "public"
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("open PostgreSQL integration pool: %v", err)
	}
	t.Cleanup(pool.Close)
	var ready bool
	if err := pool.QueryRow(context.Background(), `
SELECT to_regclass('public.train_run_shard_assignments') IS NOT NULL
   AND to_regclass('public.ticket_order_shard_locators') IS NOT NULL`).Scan(&ready); err != nil || !ready {
		t.Fatalf("Milestone 4 schema is not ready: ready=%t err=%v", ready, err)
	}

	fixture := seedMissingAuthoritativeTicketOrder(t, pool)
	router, err := shardingpostgres.NewRouter(pool, nil)
	if err != nil {
		t.Fatalf("create shard router: %v", err)
	}
	reads, err := NewShardedPostgresReads(pool, router)
	if err != nil {
		t.Fatalf("create sharded PostgreSQL reads: %v", err)
	}

	record, err := reads.GetTicketOrderRecord(context.Background(), fixture.ownerID, fixture.orderID)
	if !errors.Is(err, ErrReadNotFound) {
		t.Fatalf("GetTicketOrderRecord() error = %v, want ErrReadNotFound", err)
	}
	if record.ID != "" || record.ReservationID != "" || record.Status != "" || record.Currency != "" ||
		record.TotalAmountMinor != 0 || len(record.Tickets) != 0 || !record.CreatedAt.IsZero() {
		t.Fatalf("GetTicketOrderRecord() leaked locator summary on error: %+v", record)
	}
	if _, err := reads.ListTicketOrderRecords(context.Background(), fixture.ownerID, httpapi.PageRequest{Page: 1, Limit: 20}); !errors.Is(err, ErrReadNotFound) {
		t.Fatalf("ListTicketOrderRecords() error = %v, want ErrReadNotFound", err)
	}
}

const ticketOrderReadOwnerID = "f8b7ac88-559f-4be6-a16c-c77a23a8f1e8"

type missingAuthoritativeTicketOrderFixture struct {
	ownerID              uuid.UUID
	orderID              uuid.UUID
	runID                uuid.UUID
	trainID              uuid.UUID
	routeID              uuid.UUID
	stationIDs           [2]uuid.UUID
	locatorReservationID uuid.UUID
}

func seedMissingAuthoritativeTicketOrder(t *testing.T, pool *pgxpool.Pool) missingAuthoritativeTicketOrderFixture {
	t.Helper()
	fixture := missingAuthoritativeTicketOrderFixture{
		ownerID: uuid.New(), orderID: uuid.New(), runID: uuid.New(), trainID: uuid.New(),
		routeID: uuid.New(), stationIDs: [2]uuid.UUID{uuid.New(), uuid.New()}, locatorReservationID: uuid.New(),
	}
	suffix := strings.ToUpper(strings.ReplaceAll(fixture.runID.String(), "-", ""))[:8]
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin missing-authority fixture: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO public.users (id, email, password_hash) VALUES ($1, $2, $3)`, []any{fixture.ownerID, strings.ToLower(suffix) + "@ticket-read.test", "$2a$12$abcdefghijklmnopqrstuv012345678901234567890123456789"}},
		{`INSERT INTO public.routes (id, code, name, operating_timezone) VALUES ($1, $2, 'Ticket read route', 'UTC')`, []any{fixture.routeID, "TRR" + suffix}},
		{`INSERT INTO public.stations (id, code, name, timezone) VALUES ($1, $2, 'Ticket read origin', 'UTC')`, []any{fixture.stationIDs[0], "TRO" + suffix}},
		{`INSERT INTO public.stations (id, code, name, timezone) VALUES ($1, $2, 'Ticket read destination', 'UTC')`, []any{fixture.stationIDs[1], "TRD" + suffix}},
		{`INSERT INTO public.route_stops (route_id, station_id, stop_index, arrival_offset_minutes, departure_offset_minutes) VALUES ($1, $2, 0, 0, 0)`, []any{fixture.routeID, fixture.stationIDs[0]}},
		{`INSERT INTO public.route_stops (route_id, station_id, stop_index, arrival_offset_minutes, departure_offset_minutes) VALUES ($1, $2, 1, 10, 10)`, []any{fixture.routeID, fixture.stationIDs[1]}},
		{`INSERT INTO public.trains (id, code, name) VALUES ($1, $2, 'Ticket read train')`, []any{fixture.trainID, "TRT" + suffix}},
		{`INSERT INTO public.train_runs (id, train_id, route_id, service_date, scheduled_departure_at, segment_count) VALUES ($1, $2, $3, CURRENT_DATE + 365, clock_timestamp() + interval '365 days', 1)`, []any{fixture.runID, fixture.trainID, fixture.routeID}},
		{`INSERT INTO public.reservation_shard_locators (reservation_id, train_run_id, shard_id, assignment_generation, owner_user_id) VALUES ($1, $2, 'legacy', 1, $3)`, []any{fixture.locatorReservationID, fixture.runID, fixture.ownerID}},
		{`INSERT INTO public.ticket_order_shard_locators (ticket_order_id, reservation_id, train_run_id, shard_id, assignment_generation, owner_user_id, status, total_amount_minor, currency, created_at) VALUES ($1, $2, $3, 'legacy', 1, $4, 'confirmed', 1250, 'TWD', clock_timestamp())`, []any{fixture.orderID, fixture.locatorReservationID, fixture.runID, fixture.ownerID}},
	}
	for index, statement := range statements {
		if _, err := tx.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed missing-authority fixture statement %d: %v", index+1, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit missing-authority fixture: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		cleanupTx, err := pool.BeginTx(cleanupCtx, pgx.TxOptions{})
		if err != nil {
			t.Errorf("begin missing-authority cleanup: %v", err)
			return
		}
		defer func() { _ = cleanupTx.Rollback(context.Background()) }()
		for _, statement := range []struct {
			sql  string
			args []any
		}{
			{`DELETE FROM public.ticket_order_shard_locators WHERE ticket_order_id = $1`, []any{fixture.orderID}},
			{`DELETE FROM public.reservation_shard_locators WHERE reservation_id = $1`, []any{fixture.locatorReservationID}},
			{`DELETE FROM public.train_runs WHERE id = $1`, []any{fixture.runID}},
			{`DELETE FROM public.trains WHERE id = $1`, []any{fixture.trainID}},
			{`DELETE FROM public.route_stops WHERE route_id = $1`, []any{fixture.routeID}},
			{`DELETE FROM public.routes WHERE id = $1`, []any{fixture.routeID}},
			{`DELETE FROM public.stations WHERE id = ANY($1::uuid[])`, []any{fixture.stationIDs[:]}},
			{`DELETE FROM public.users WHERE id = $1`, []any{fixture.ownerID}},
		} {
			if _, err := cleanupTx.Exec(cleanupCtx, statement.sql, statement.args...); err != nil {
				t.Errorf("clean missing-authority fixture: %v", err)
				return
			}
		}
		if err := cleanupTx.Commit(cleanupCtx); err != nil {
			t.Errorf("commit missing-authority cleanup: %v", err)
		}
	})
	return fixture
}

func ticketOrderReadRecord() TicketOrderRecord {
	return TicketOrderRecord{
		ID:               "a223144f-fb4e-44a6-863c-0195c12c54ea",
		ReservationID:    "d69fe67c-33af-4f95-98a8-106b7e815ca3",
		Status:           "confirmed",
		TotalAmountMinor: 1250,
		Currency:         "TWD",
		CreatedAt:        time.Date(2026, 7, 28, 12, 30, 0, 123000, time.UTC),
	}
}

func ticketOrderReadsForTest(t *testing.T, rawOrderID string, tx *ticketOrderReadPGXTx) *PostgresReads {
	t.Helper()
	orderID := uuid.MustParse(rawOrderID)
	runID := uuid.New()
	generation, err := sharding.NewAssignmentGeneration(1)
	if err != nil {
		t.Fatalf("create assignment generation: %v", err)
	}
	route, err := sharding.NewShardRoute(runID, sharding.ShardZero, generation)
	if err != nil {
		t.Fatalf("create shard route: %v", err)
	}
	return &PostgresReads{shards: &ticketOrderReadRouter{
		orderID: orderID,
		ownerID: uuid.MustParse(ticketOrderReadOwnerID),
		route:   route,
		tx:      &ticketOrderReadRoutedTx{tx: tx, route: route},
	}}
}

type ticketOrderReadRouter struct {
	orderID uuid.UUID
	ownerID uuid.UUID
	route   sharding.ShardRoute
	tx      readRoutedTx
}

func (router *ticketOrderReadRouter) ResolveReservationForOwner(context.Context, uuid.UUID, uuid.UUID) (sharding.ShardRoute, error) {
	return sharding.ShardRoute{}, errors.New("unexpected reservation lookup")
}

func (router *ticketOrderReadRouter) ResolveTicketOrderForOwner(_ context.Context, orderID, ownerID uuid.UUID) (sharding.ShardRoute, error) {
	if orderID != router.orderID || ownerID != router.ownerID {
		return sharding.ShardRoute{}, sharding.ErrLocatorNotFound
	}
	return router.route, nil
}

func (router *ticketOrderReadRouter) RefreshTrainRun(context.Context, uuid.UUID) (sharding.ShardRoute, error) {
	return sharding.ShardRoute{}, errors.New("unexpected route refresh")
}

func (router *ticketOrderReadRouter) BeginTrainRunRead(_ context.Context, route sharding.ShardRoute) (readRoutedTx, error) {
	if route != router.route {
		return nil, sharding.ErrAssignmentStale
	}
	return router.tx, nil
}

type ticketOrderReadRoutedTx struct {
	tx    *ticketOrderReadPGXTx
	route sharding.ShardRoute
}

func (tx *ticketOrderReadRoutedTx) PGXTx() pgx.Tx                      { return tx.tx }
func (tx *ticketOrderReadRoutedTx) Route() sharding.ShardRoute         { return tx.route }
func (tx *ticketOrderReadRoutedTx) Commit(ctx context.Context) error   { return tx.tx.Commit(ctx) }
func (tx *ticketOrderReadRoutedTx) Rollback(ctx context.Context) error { return tx.tx.Rollback(ctx) }

type ticketOrderReadPGXTx struct {
	pgx.Tx
	row           pgx.Row
	rows          pgx.Rows
	queryCalls    int
	commitCalls   int
	rollbackCalls int
}

func (tx *ticketOrderReadPGXTx) QueryRow(context.Context, string, ...any) pgx.Row { return tx.row }

func (tx *ticketOrderReadPGXTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	tx.queryCalls++
	return tx.rows, nil
}

func (tx *ticketOrderReadPGXTx) Commit(context.Context) error {
	tx.commitCalls++
	return nil
}

func (tx *ticketOrderReadPGXTx) Rollback(context.Context) error {
	tx.rollbackCalls++
	return nil
}

type ticketOrderAuthorityRow struct {
	record TicketOrderRecord
	err    error
}

func (row ticketOrderAuthorityRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != 6 {
		return errors.New("unexpected authoritative ticket-order scan shape")
	}
	*destinations[0].(*string) = row.record.ID
	*destinations[1].(*string) = row.record.ReservationID
	*destinations[2].(*string) = row.record.Status
	*destinations[3].(*int64) = row.record.TotalAmountMinor
	*destinations[4].(*string) = row.record.Currency
	*destinations[5].(*time.Time) = row.record.CreatedAt
	return nil
}

type emptyTicketRows struct{ pgx.Rows }

func (*emptyTicketRows) Close()     {}
func (*emptyTicketRows) Next() bool { return false }
func (*emptyTicketRows) Err() error { return nil }
