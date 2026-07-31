package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	physicalpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgres16ControlRotationIsAtomic(t *testing.T) {
	pool := openControlDatabase(t)

	t.Run("cutover and rollback rotate every control route monotonically", func(t *testing.T) {
		fixture := seedControlRotation(t, pool)
		control, err := physicalpostgres.NewControl(pool)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := control.SwitchAssignment(context.Background(), physicalmigration.Change{
			MigrationID:   fixture.migrationID,
			ExpectedState: migration.PhysicalStateTargetEnabled,
			NextState:     migration.PhysicalStateSwitchingAssignment,
		}); err != nil {
			t.Fatalf("SwitchAssignment() error = %v", err)
		}
		assertControlRotation(t, pool, fixture, fixture.targetShardID, fixture.targetGeneration,
			migration.PhysicalStateSwitchingAssignment)

		_, err = control.Rollback(context.Background(), physicalmigration.Change{
			MigrationID:        fixture.migrationID,
			ExpectedState:      migration.PhysicalStateSwitchingAssignment,
			NextState:          migration.PhysicalStateRolledBack,
			RollbackGeneration: fixture.targetGeneration,
		})
		if !errors.Is(err, physicalmigration.ErrCheckpointConflict) {
			t.Fatalf("Rollback(non-new generation) error = %v, want checkpoint conflict", err)
		}
		assertControlRotation(t, pool, fixture, fixture.targetShardID, fixture.targetGeneration,
			migration.PhysicalStateSwitchingAssignment)

		rollbackGeneration := fixture.targetGeneration + 1
		if _, err := control.Rollback(context.Background(), physicalmigration.Change{
			MigrationID:        fixture.migrationID,
			ExpectedState:      migration.PhysicalStateSwitchingAssignment,
			NextState:          migration.PhysicalStateRolledBack,
			RollbackGeneration: rollbackGeneration,
		}); err != nil {
			t.Fatalf("Rollback() error = %v", err)
		}
		assertControlRotation(t, pool, fixture, fixture.sourceShardID, rollbackGeneration,
			migration.PhysicalStateRolledBack)
	})

	t.Run("late cutover failure rolls back every control route", func(t *testing.T) {
		fixture := seedControlRotation(t, pool)
		sequenceName := "test_control_rotation_" + strings.ReplaceAll(fixture.migrationID.String(), "-", "")
		installLateCutoverFailure(t, pool, fixture.migrationID, sequenceName)
		control, err := physicalpostgres.NewControl(pool)
		if err != nil {
			t.Fatal(err)
		}

		_, err = control.SwitchAssignment(context.Background(), physicalmigration.Change{
			MigrationID:   fixture.migrationID,
			ExpectedState: migration.PhysicalStateTargetEnabled,
			NextState:     migration.PhysicalStateSwitchingAssignment,
		})
		if !errors.Is(err, physicalmigration.ErrCheckpointConflict) {
			t.Fatalf("SwitchAssignment() error = %v, want checkpoint conflict", err)
		}
		var triggerRan bool
		if err := pool.QueryRow(context.Background(), fmt.Sprintf(
			`SELECT is_called FROM public.%s`, pgx.Identifier{sequenceName}.Sanitize(),
		)).Scan(&triggerRan); err != nil {
			t.Fatalf("read failure-injection sequence: %v", err)
		}
		if !triggerRan {
			t.Fatal("late outbox failure trigger did not run")
		}
		assertControlRotation(t, pool, fixture, fixture.sourceShardID, fixture.sourceGeneration,
			migration.PhysicalStateTargetEnabled)
		var eventCount int
		if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM public.outbox_events
WHERE aggregate_type = 'physical_shard_migration' AND aggregate_id = $1`,
			fixture.migrationID).Scan(&eventCount); err != nil {
			t.Fatalf("count rolled-back cutover events: %v", err)
		}
		if eventCount != 0 {
			t.Fatalf("rolled-back cutover event count = %d, want 0", eventCount)
		}
	})
}

func TestPostgres16PhysicalAssignmentLedgerInvariant(t *testing.T) {
	pool := openControlDatabase(t)

	t.Run("logical source stays writable while physical migration is active", func(t *testing.T) {
		fixture := seedControlTrainRun(t, pool)
		migrationID := uuid.New()
		tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		if _, err := tx.Exec(context.Background(), `
INSERT INTO public.physical_shard_migrations(
    migration_id,train_run_id,source_shard_id,target_shard_id,
    source_generation,target_generation,state
) VALUES($1,$2,'legacy','physical-shard-0',1,2,'planned')`, migrationID, fixture.trainRunID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(context.Background(), `
UPDATE public.train_run_shard_assignments
SET assignment_state='migrating',active_physical_migration_id=$2
WHERE train_run_id=$1`, fixture.trainRunID, migrationID); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(context.Background()); err != nil {
			t.Fatalf("commit logical-source plan: %v", err)
		}
		var writeEnabled bool
		var generation int64
		if err := pool.QueryRow(context.Background(), `
SELECT write_enabled,assignment_generation
FROM public.train_run_write_fences WHERE train_run_id=$1`, fixture.trainRunID).Scan(&writeEnabled, &generation); err != nil {
			t.Fatal(err)
		}
		if !writeEnabled || generation != 1 {
			t.Fatalf("logical source fence = enabled:%t generation:%d, want enabled:true generation:1", writeEnabled, generation)
		}
	})

	t.Run("matching post-cutover physical ledger is accepted", func(t *testing.T) {
		fixture := seedControlTrainRun(t, pool)
		migrationID := uuid.New()
		tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		if _, err := tx.Exec(context.Background(), `
INSERT INTO public.physical_shard_migrations(
    migration_id,train_run_id,source_shard_id,target_shard_id,
    source_generation,target_generation,state,source_fenced_at,target_enabled_at,
    assignment_switched_at,rollback_deadline_at,source_retention_until
) VALUES(
    $1,$2,'physical-shard-0','physical-shard-1',7,8,'rollback_window',
    clock_timestamp(),clock_timestamp(),clock_timestamp(),
    clock_timestamp()+interval '5 minutes',clock_timestamp()+interval '5 minutes'
)`, migrationID, fixture.trainRunID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(context.Background(), `UPDATE public.train_run_write_fences SET write_enabled=false WHERE train_run_id=$1`, fixture.trainRunID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(context.Background(), `
UPDATE public.train_run_shard_assignments
SET shard_id='physical-shard-1',assignment_generation=8,
    assignment_state='rollback_window',active_physical_migration_id=$2
WHERE train_run_id=$1`, fixture.trainRunID, migrationID); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(context.Background()); err != nil {
			t.Fatalf("commit matching physical ledger: %v", err)
		}
	})

	t.Run("physical assignment without ledger fails closed", func(t *testing.T) {
		fixture := seedControlTrainRun(t, pool)
		tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		if _, err := tx.Exec(context.Background(), `UPDATE public.train_run_write_fences SET write_enabled=false WHERE train_run_id=$1`, fixture.trainRunID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(context.Background(), `
UPDATE public.train_run_shard_assignments
SET shard_id='physical-shard-0',assignment_generation=2,assignment_state='migrating'
WHERE train_run_id=$1`, fixture.trainRunID); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(context.Background()); err == nil {
			t.Fatal("physical assignment without an active migration ledger committed")
		}
	})

	t.Run("physical assignment with mismatched ledger fails closed", func(t *testing.T) {
		fixture := seedControlTrainRun(t, pool)
		migrationID := uuid.New()
		tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		if _, err := tx.Exec(context.Background(), `
INSERT INTO public.physical_shard_migrations(
    migration_id,train_run_id,source_shard_id,target_shard_id,
    source_generation,target_generation,state
) VALUES($1,$2,'legacy','physical-shard-0',1,2,'planned')`, migrationID, fixture.trainRunID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(context.Background(), `UPDATE public.train_run_write_fences SET write_enabled=false WHERE train_run_id=$1`, fixture.trainRunID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(context.Background(), `
UPDATE public.train_run_shard_assignments
SET shard_id='physical-shard-1',assignment_generation=2,
    assignment_state='migrating',active_physical_migration_id=$2
WHERE train_run_id=$1`, fixture.trainRunID, migrationID); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(context.Background()); err == nil {
			t.Fatal("physical assignment with a mismatched migration ledger committed")
		}
	})
}

type controlRotationFixture struct {
	ownerID          uuid.UUID
	trainRunID       uuid.UUID
	trainID          uuid.UUID
	routeID          uuid.UUID
	stationIDs       [2]uuid.UUID
	migrationID      uuid.UUID
	commandID        uuid.UUID
	reservationID    uuid.UUID
	ticketOrderID    uuid.UUID
	ticketID         uuid.UUID
	sourceShardID    string
	targetShardID    string
	sourceGeneration int64
	targetGeneration int64
}

func openControlDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("TEST_CONTROL_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_CONTROL_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open control database: %v", err)
	}
	t.Cleanup(pool.Close)
	var ready bool
	if err := pool.QueryRow(ctx, `
SELECT current_setting('server_version_num')::integer >= 160000
   AND to_regclass('public.physical_shard_migrations') IS NOT NULL
   AND to_regclass('public.reservation_directory') IS NOT NULL
   AND to_regclass('public.ticket_shard_locators') IS NOT NULL`).Scan(&ready); err != nil {
		t.Fatalf("check control integration schema: %v", err)
	}
	if !ready {
		t.Skip("PostgreSQL 16 with control schema version 9 is required")
	}
	enablePhysicalCatalogForTest(t, pool)
	return pool
}

type physicalCatalogState struct {
	shardID             string
	enabled             bool
	writeEnabled        bool
	state               string
	healthState         string
	lastHealthChecked   *time.Time
	writeDisabledReason *string
}

func enablePhysicalCatalogForTest(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
SELECT shard_id,enabled,write_enabled,state,health_state,
       last_health_checked_at,write_disabled_reason
FROM public.booking_shards
WHERE shard_id IN ('physical-shard-0','physical-shard-1')
ORDER BY shard_id`)
	if err != nil {
		t.Fatalf("read physical catalog state: %v", err)
	}
	states := make([]physicalCatalogState, 0, 2)
	for rows.Next() {
		var state physicalCatalogState
		if err := rows.Scan(&state.shardID, &state.enabled, &state.writeEnabled, &state.state,
			&state.healthState, &state.lastHealthChecked, &state.writeDisabledReason); err != nil {
			rows.Close()
			t.Fatalf("scan physical catalog state: %v", err)
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatalf("iterate physical catalog state: %v", err)
	}
	rows.Close()
	if len(states) != 2 {
		t.Fatalf("physical catalog rows = %d, want 2", len(states))
	}
	if _, err := pool.Exec(context.Background(), `
UPDATE public.booking_shards
SET enabled=true,write_enabled=true,state='active',health_state='healthy',
    last_health_checked_at=clock_timestamp(),write_disabled_reason=NULL
WHERE shard_id IN ('physical-shard-0','physical-shard-1')`); err != nil {
		t.Fatalf("enable physical catalog for integration test: %v", err)
	}
	t.Cleanup(func() {
		for _, state := range states {
			if _, err := pool.Exec(context.Background(), `
UPDATE public.booking_shards
SET enabled=$2,write_enabled=$3,state=$4,health_state=$5,
    last_health_checked_at=$6,write_disabled_reason=$7
WHERE shard_id=$1`, state.shardID, state.enabled, state.writeEnabled, state.state,
				state.healthState, state.lastHealthChecked, state.writeDisabledReason); err != nil {
				t.Errorf("restore physical catalog %s: %v", state.shardID, err)
			}
		}
	})
}

func seedControlRotation(t *testing.T, pool *pgxpool.Pool) controlRotationFixture {
	t.Helper()
	fixture := controlRotationFixture{
		ownerID: uuid.New(), trainRunID: uuid.New(), trainID: uuid.New(), routeID: uuid.New(),
		stationIDs: [2]uuid.UUID{uuid.New(), uuid.New()}, migrationID: uuid.New(), commandID: uuid.New(),
		reservationID: uuid.New(), ticketOrderID: uuid.New(), ticketID: uuid.New(),
		sourceShardID: "physical-shard-0", targetShardID: "physical-shard-1",
		sourceGeneration: 7, targetGeneration: 8,
	}
	suffix := strings.ToUpper(strings.ReplaceAll(fixture.trainRunID.String(), "-", ""))[:8]
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin control rotation seed: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO public.users(id,email,password_hash) VALUES($1,$2,$3)`, []any{fixture.ownerID, strings.ToLower(suffix) + "@control-rotation.test", "$2a$12$abcdefghijklmnopqrstuv012345678901234567890123456789"}},
		{`INSERT INTO public.routes(id,code,name,operating_timezone) VALUES($1,$2,'Control rotation route','UTC')`, []any{fixture.routeID, "C5R" + suffix}},
		{`INSERT INTO public.stations(id,code,name,timezone) VALUES($1,$2,'Control rotation origin','UTC')`, []any{fixture.stationIDs[0], "C5O" + suffix}},
		{`INSERT INTO public.stations(id,code,name,timezone) VALUES($1,$2,'Control rotation destination','UTC')`, []any{fixture.stationIDs[1], "C5D" + suffix}},
		{`INSERT INTO public.route_stops(route_id,station_id,stop_index,arrival_offset_minutes,departure_offset_minutes) VALUES($1,$2,0,0,0),($1,$3,1,10,10)`, []any{fixture.routeID, fixture.stationIDs[0], fixture.stationIDs[1]}},
		{`INSERT INTO public.trains(id,code,name) VALUES($1,$2,'Control rotation train')`, []any{fixture.trainID, "C5T" + suffix}},
		{`INSERT INTO public.train_runs(id,train_id,route_id,service_date,scheduled_departure_at,segment_count) VALUES($1,$2,$3,CURRENT_DATE+365,clock_timestamp()+interval '365 days',1)`, []any{fixture.trainRunID, fixture.trainID, fixture.routeID}},
		{`INSERT INTO public.physical_shard_migrations(migration_id,train_run_id,source_shard_id,target_shard_id,source_generation,target_generation,state,source_fenced_at,target_enabled_at) VALUES($1,$2,$3,$4,$5,$6,'target_enabled',clock_timestamp(),clock_timestamp())`, []any{fixture.migrationID, fixture.trainRunID, fixture.sourceShardID, fixture.targetShardID, fixture.sourceGeneration, fixture.targetGeneration}},
		{`UPDATE public.train_run_write_fences SET write_enabled=false WHERE train_run_id=$1`, []any{fixture.trainRunID}},
		{`UPDATE public.train_run_shard_assignments SET shard_id=$2,assignment_generation=$3,assignment_state='migrating',active_physical_migration_id=$4 WHERE train_run_id=$1`, []any{fixture.trainRunID, fixture.sourceShardID, fixture.sourceGeneration, fixture.migrationID}},
		{`INSERT INTO public.booking_commands(command_id,operation,owner_user_id,train_run_id,reservation_id,idempotency_key_hash,request_fingerprint,target_shard_id,assignment_generation,state) VALUES($1,'reservation.create',$2,$3,$4,decode(repeat('11',32),'hex'),decode(repeat('22',32),'hex'),$5,$6,'reserved')`, []any{fixture.commandID, fixture.ownerID, fixture.trainRunID, fixture.reservationID, fixture.sourceShardID, fixture.sourceGeneration}},
		{`INSERT INTO public.reservation_directory(reservation_id,train_run_id,owner_user_id,command_id,state,last_known_shard_id,last_known_generation,active_at) VALUES($1,$2,$3,$4,'active',$5,$6,clock_timestamp())`, []any{fixture.reservationID, fixture.trainRunID, fixture.ownerID, fixture.commandID, fixture.sourceShardID, fixture.sourceGeneration}},
		{`INSERT INTO public.reservation_shard_locators(reservation_id,train_run_id,shard_id,assignment_generation,owner_user_id) VALUES($1,$2,$3,$4,$5)`, []any{fixture.reservationID, fixture.trainRunID, fixture.sourceShardID, fixture.sourceGeneration, fixture.ownerID}},
		{`INSERT INTO public.ticket_order_shard_locators(ticket_order_id,reservation_id,train_run_id,shard_id,assignment_generation,owner_user_id,status,total_amount_minor,currency,created_at) VALUES($1,$2,$3,$4,$5,$6,'confirmed',100,'TWD',clock_timestamp())`, []any{fixture.ticketOrderID, fixture.reservationID, fixture.trainRunID, fixture.sourceShardID, fixture.sourceGeneration, fixture.ownerID}},
		{`INSERT INTO public.ticket_shard_locators(ticket_id,ticket_order_id,reservation_id,train_run_id,shard_id,assignment_generation,owner_user_id) VALUES($1,$2,$3,$4,$5,$6,$7)`, []any{fixture.ticketID, fixture.ticketOrderID, fixture.reservationID, fixture.trainRunID, fixture.sourceShardID, fixture.sourceGeneration, fixture.ownerID}},
	}
	for index, statement := range statements {
		if _, err := tx.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed control rotation statement %d: %v", index+1, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit control rotation seed: %v", err)
	}
	t.Cleanup(func() { cleanupControlRotation(t, pool, fixture) })
	return fixture
}

type controlTrainRunFixture struct {
	ownerID    uuid.UUID
	trainRunID uuid.UUID
	trainID    uuid.UUID
	routeID    uuid.UUID
	stationIDs [2]uuid.UUID
}

func seedControlTrainRun(t *testing.T, pool *pgxpool.Pool) controlTrainRunFixture {
	t.Helper()
	fixture := controlTrainRunFixture{
		ownerID: uuid.New(), trainRunID: uuid.New(), trainID: uuid.New(), routeID: uuid.New(),
		stationIDs: [2]uuid.UUID{uuid.New(), uuid.New()},
	}
	suffix := strings.ToUpper(strings.ReplaceAll(fixture.trainRunID.String(), "-", ""))[:8]
	tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO public.users(id,email,password_hash) VALUES($1,$2,$3)`, []any{fixture.ownerID, strings.ToLower(suffix) + "@assignment-ledger.test", "$2a$12$abcdefghijklmnopqrstuv012345678901234567890123456789"}},
		{`INSERT INTO public.routes(id,code,name,operating_timezone) VALUES($1,$2,'Assignment ledger route','UTC')`, []any{fixture.routeID, "L5R" + suffix}},
		{`INSERT INTO public.stations(id,code,name,timezone) VALUES($1,$2,'Assignment ledger origin','UTC')`, []any{fixture.stationIDs[0], "L5O" + suffix}},
		{`INSERT INTO public.stations(id,code,name,timezone) VALUES($1,$2,'Assignment ledger destination','UTC')`, []any{fixture.stationIDs[1], "L5D" + suffix}},
		{`INSERT INTO public.route_stops(route_id,station_id,stop_index,arrival_offset_minutes,departure_offset_minutes) VALUES($1,$2,0,0,0),($1,$3,1,10,10)`, []any{fixture.routeID, fixture.stationIDs[0], fixture.stationIDs[1]}},
		{`INSERT INTO public.trains(id,code,name) VALUES($1,$2,'Assignment ledger train')`, []any{fixture.trainID, "L5T" + suffix}},
		{`INSERT INTO public.train_runs(id,train_id,route_id,service_date,scheduled_departure_at,segment_count) VALUES($1,$2,$3,CURRENT_DATE+365,clock_timestamp()+interval '365 days',1)`, []any{fixture.trainRunID, fixture.trainID, fixture.routeID}},
	}
	for index, statement := range statements {
		if _, err := tx.Exec(context.Background(), statement.sql, statement.args...); err != nil {
			t.Fatalf("seed assignment ledger statement %d: %v", index+1, err)
		}
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit assignment ledger seed: %v", err)
	}
	t.Cleanup(func() { cleanupControlTrainRun(t, pool, fixture) })
	return fixture
}

func cleanupControlTrainRun(t *testing.T, pool *pgxpool.Pool, fixture controlTrainRunFixture) {
	t.Helper()
	tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Errorf("begin assignment ledger cleanup: %v", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	statements := []struct {
		sql  string
		args []any
	}{
		{`DELETE FROM public.train_run_shard_assignments WHERE train_run_id=$1`, []any{fixture.trainRunID}},
		{`DELETE FROM public.physical_shard_migrations WHERE train_run_id=$1`, []any{fixture.trainRunID}},
		{`DELETE FROM public.train_runs WHERE id=$1`, []any{fixture.trainRunID}},
		{`DELETE FROM public.trains WHERE id=$1`, []any{fixture.trainID}},
		{`DELETE FROM public.route_stops WHERE route_id=$1`, []any{fixture.routeID}},
		{`DELETE FROM public.routes WHERE id=$1`, []any{fixture.routeID}},
		{`DELETE FROM public.stations WHERE id=ANY($1::uuid[])`, []any{fixture.stationIDs[:]}},
		{`DELETE FROM public.users WHERE id=$1`, []any{fixture.ownerID}},
	}
	for _, statement := range statements {
		if _, err := tx.Exec(context.Background(), statement.sql, statement.args...); err != nil {
			t.Errorf("clean assignment ledger fixture: %v", err)
			return
		}
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Errorf("commit assignment ledger cleanup: %v", err)
	}
}

func assertControlRotation(t *testing.T, pool *pgxpool.Pool, fixture controlRotationFixture, wantShard string, wantGeneration int64, wantState migration.PhysicalState) {
	t.Helper()
	rows := []struct {
		name  string
		query string
		id    uuid.UUID
	}{
		{"assignment", `SELECT shard_id,assignment_generation FROM public.train_run_shard_assignments WHERE train_run_id=$1`, fixture.trainRunID},
		{"directory", `SELECT last_known_shard_id,last_known_generation FROM public.reservation_directory WHERE reservation_id=$1`, fixture.reservationID},
		{"command", `SELECT target_shard_id,assignment_generation FROM public.booking_commands WHERE command_id=$1`, fixture.commandID},
		{"reservation locator", `SELECT shard_id,assignment_generation FROM public.reservation_shard_locators WHERE reservation_id=$1`, fixture.reservationID},
		{"ticket order locator", `SELECT shard_id,assignment_generation FROM public.ticket_order_shard_locators WHERE ticket_order_id=$1`, fixture.ticketOrderID},
		{"ticket locator", `SELECT shard_id,assignment_generation FROM public.ticket_shard_locators WHERE ticket_id=$1`, fixture.ticketID},
	}
	for _, row := range rows {
		var shardID string
		var generation int64
		if err := pool.QueryRow(context.Background(), row.query, row.id).Scan(&shardID, &generation); err != nil {
			t.Fatalf("read %s: %v", row.name, err)
		}
		if shardID != wantShard || generation != wantGeneration {
			t.Errorf("%s route = %s/%d, want %s/%d", row.name, shardID, generation, wantShard, wantGeneration)
		}
	}
	var state migration.PhysicalState
	if err := pool.QueryRow(context.Background(), `SELECT state FROM public.physical_shard_migrations WHERE migration_id=$1`, fixture.migrationID).Scan(&state); err != nil {
		t.Fatalf("read migration state: %v", err)
	}
	if state != wantState {
		t.Errorf("migration state = %s, want %s", state, wantState)
	}
}

func installLateCutoverFailure(t *testing.T, pool *pgxpool.Pool, migrationID uuid.UUID, sequenceName string) {
	t.Helper()
	identifier := pgx.Identifier{sequenceName}.Sanitize()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
CREATE SEQUENCE public.%[1]s;
CREATE FUNCTION public.%[1]s() RETURNS trigger LANGUAGE plpgsql AS $body$
BEGIN
    IF NEW.aggregate_type = 'physical_shard_migration'
       AND NEW.aggregate_id = '%[2]s'::uuid
       AND NEW.event_type = 'physical_shard_migration.cutover' THEN
        PERFORM nextval('public.%[1]s');
        RAISE EXCEPTION 'injected late control cutover failure';
    END IF;
    RETURN NEW;
END;
$body$;
CREATE TRIGGER %[1]s BEFORE INSERT ON public.outbox_events
FOR EACH ROW EXECUTE FUNCTION public.%[1]s()`, identifier, migrationID)); err != nil {
		t.Fatalf("install late cutover failure: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(
			`DROP TRIGGER IF EXISTS %[1]s ON public.outbox_events; DROP FUNCTION IF EXISTS public.%[1]s(); DROP SEQUENCE IF EXISTS public.%[1]s`, identifier,
		))
	})
}

func cleanupControlRotation(t *testing.T, pool *pgxpool.Pool, fixture controlRotationFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Errorf("begin control rotation cleanup: %v", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	statements := []struct {
		sql  string
		args []any
	}{
		{`DELETE FROM public.outbox_events WHERE aggregate_type='physical_shard_migration' AND aggregate_id=$1`, []any{fixture.migrationID}},
		{`DELETE FROM public.ticket_shard_locators WHERE ticket_id=$1`, []any{fixture.ticketID}},
		{`DELETE FROM public.ticket_order_shard_locators WHERE ticket_order_id=$1`, []any{fixture.ticketOrderID}},
		{`DELETE FROM public.reservation_shard_locators WHERE reservation_id=$1`, []any{fixture.reservationID}},
		{`DELETE FROM public.reservation_directory WHERE reservation_id=$1`, []any{fixture.reservationID}},
		{`DELETE FROM public.booking_commands WHERE command_id=$1`, []any{fixture.commandID}},
		{`DELETE FROM public.train_run_shard_assignments WHERE train_run_id=$1`, []any{fixture.trainRunID}},
		{`DELETE FROM public.physical_shard_migrations WHERE migration_id=$1`, []any{fixture.migrationID}},
		{`DELETE FROM public.train_runs WHERE id=$1`, []any{fixture.trainRunID}},
		{`DELETE FROM public.trains WHERE id=$1`, []any{fixture.trainID}},
		{`DELETE FROM public.route_stops WHERE route_id=$1`, []any{fixture.routeID}},
		{`DELETE FROM public.routes WHERE id=$1`, []any{fixture.routeID}},
		{`DELETE FROM public.stations WHERE id=ANY($1::uuid[])`, []any{fixture.stationIDs[:]}},
		{`DELETE FROM public.users WHERE id=$1`, []any{fixture.ownerID}},
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Errorf("clean control rotation fixture: %v", err)
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Errorf("commit control rotation cleanup: %v", err)
	}
}
