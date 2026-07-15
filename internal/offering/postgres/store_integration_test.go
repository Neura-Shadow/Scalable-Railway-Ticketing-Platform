package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	offeringpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/postgres"
	"github.com/jackc/pgx/v5"
)

func TestStoreStationLifecycle(t *testing.T) {
	store, _ := openTestStore(t)
	ctx := context.Background()

	empty, err := store.ListStations(ctx, false)
	if err != nil {
		t.Fatalf("ListStations() error = %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("ListStations() = %#v, want non-nil empty slice", empty)
	}

	created, err := store.CreateStation(ctx, offeringpostgres.CreateStationParams{
		Code: " tpe ", Name: "  Taipei Main  ", Timezone: "Asia/Taipei",
	})
	if err != nil {
		t.Fatalf("CreateStation() error = %v", err)
	}
	if created.ID == "" || created.Code.String() != "TPE" || created.Name != "Taipei Main" || !created.Active {
		t.Fatalf("CreateStation() = %#v", created)
	}

	_, err = store.CreateStation(ctx, offeringpostgres.CreateStationParams{
		Code: "TPE", Name: "Duplicate", Timezone: "Asia/Taipei",
	})
	if !errors.Is(err, offeringpostgres.ErrConflict) {
		t.Fatalf("duplicate CreateStation() error = %v, want %v", err, offeringpostgres.ErrConflict)
	}

	updated, err := store.UpdateStation(ctx, created.ID, offeringpostgres.UpdateStationParams{
		Code: "TPA", Name: "Taipei Central", Timezone: "Asia/Taipei", Active: false,
	})
	if err != nil {
		t.Fatalf("UpdateStation() error = %v", err)
	}
	if updated.Code.String() != "TPA" || updated.Name != "Taipei Central" || updated.Active {
		t.Fatalf("UpdateStation() = %#v", updated)
	}

	all, err := store.ListStations(ctx, false)
	if err != nil || len(all) != 1 || all[0].ID != created.ID {
		t.Fatalf("ListStations(all) = %#v, error %v", all, err)
	}
	active, err := store.ListStations(ctx, true)
	if err != nil || len(active) != 0 {
		t.Fatalf("ListStations(active) = %#v, error %v", active, err)
	}

	if _, err := store.UpdateStation(ctx, "00000000-0000-0000-0000-000000000000", offeringpostgres.UpdateStationParams{
		Code: "MIS", Name: "Missing", Timezone: "UTC", Active: true,
	}); !errors.Is(err, offeringpostgres.ErrNotFound) {
		t.Fatalf("missing UpdateStation() error = %v, want %v", err, offeringpostgres.ErrNotFound)
	}

	if _, err := domain.NewStationCode(created.Code.String()); err != nil {
		t.Fatalf("persisted station code violates domain contract: %v", err)
	}
}

func TestMigrationRejectsRouteWithFewerThanTwoStops(t *testing.T) {
	_, conn := openTestStore(t)
	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO routes (code, name, operating_timezone) VALUES ('TOO_SHORT', 'Too Short', 'UTC')`); err != nil {
		t.Fatalf("insert route before deferred validation: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("route with zero stops committed")
	}
}

func TestStoreCreatesTopologyFaresAndCommissionsInventoryAtomically(t *testing.T) {
	store, conn := openTestStore(t)
	ctx := context.Background()

	for _, station := range []offeringpostgres.CreateStationParams{
		{Code: "TPE", Name: "Taipei", Timezone: "Asia/Taipei"},
		{Code: "TXG", Name: "Taichung", Timezone: "Asia/Taipei"},
		{Code: "KHH", Name: "Kaohsiung", Timezone: "Asia/Taipei"},
	} {
		if _, err := store.CreateStation(ctx, station); err != nil {
			t.Fatalf("CreateStation(%s) error = %v", station.Code, err)
		}
	}

	routeModel := mustRoute(t)
	route, err := store.CreateRoute(ctx, offeringpostgres.CreateRouteParams{
		Route: routeModel, OperatingTimezone: "Asia/Taipei",
	})
	if err != nil {
		t.Fatalf("CreateRoute() error = %v", err)
	}
	if route.ID == "" || route.SegmentCount != 2 || len(route.Stops) != 3 {
		t.Fatalf("CreateRoute() = %#v", route)
	}

	train, err := store.CreateTrain(ctx, offeringpostgres.CreateTrainParams{Code: "TR100", Name: "Western Express"})
	if err != nil {
		t.Fatalf("CreateTrain() error = %v", err)
	}
	coach, err := store.CreateCoach(ctx, offeringpostgres.CreateCoachParams{
		TrainID: train.ID, CoachNumber: "1", SeatClass: domain.SeatClassStandard,
	})
	if err != nil {
		t.Fatalf("CreateCoach() error = %v", err)
	}
	for _, seat := range []offeringpostgres.CreateSeatParams{
		{CoachID: coach.ID, SeatNumber: "1A", SeatType: "window"},
		{CoachID: coach.ID, SeatNumber: "1B", SeatType: "aisle"},
	} {
		if _, err := store.CreateSeat(ctx, seat); err != nil {
			t.Fatalf("CreateSeat(%s) error = %v", seat.SeatNumber, err)
		}
	}

	serviceDate := time.Date(2026, time.July, 20, 0, 0, 0, 0, time.UTC)
	if _, err := store.CommissionTrainRun(ctx, offeringpostgres.CommissionTrainRunParams{
		TrainID: train.ID, RouteID: route.ID, ServiceDate: serviceDate,
		ScheduledDepartureAt: time.Date(2026, time.July, 19, 22, 0, 0, 0, time.UTC),
	}); !errors.Is(err, offeringpostgres.ErrInvalidInput) {
		t.Fatalf("inconsistent service-date departure error = %v, want %v", err, offeringpostgres.ErrInvalidInput)
	}
	commissioned, err := store.CommissionTrainRun(ctx, offeringpostgres.CommissionTrainRunParams{
		TrainID: train.ID, RouteID: route.ID, ServiceDate: serviceDate,
		ScheduledDepartureAt: time.Date(2026, time.July, 19, 16, 5, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("CommissionTrainRun() error = %v", err)
	}
	if commissioned.SegmentCount != 2 || commissioned.InventoryRows != 2 || commissioned.Status != domain.TrainRunStatusScheduled {
		t.Fatalf("CommissionTrainRun() = %#v", commissioned)
	}

	for _, fare := range []offeringpostgres.CreateFareParams{
		{RouteID: route.ID, FromStopIndex: 0, ToStopIndex: 2, SeatClass: domain.SeatClassStandard, AmountMinor: 1000, Currency: "TWD"},
		{TrainRunID: commissioned.ID, FromStopIndex: 0, ToStopIndex: 2, SeatClass: domain.SeatClassStandard, AmountMinor: 1200, Currency: "TWD"},
	} {
		if _, err := store.CreateFare(ctx, fare); err != nil {
			t.Fatalf("CreateFare() error = %v", err)
		}
	}

	var invalidInventory int
	if err := conn.QueryRow(ctx, `
		SELECT count(*)
		FROM seat_inventory
		WHERE train_run_id = $1
		  AND (bit_length(occupied_segments) <> $2 OR bit_count(occupied_segments) <> 0)
	`, commissioned.ID, commissioned.SegmentCount).Scan(&invalidInventory); err != nil {
		t.Fatalf("inspect commissioned inventory: %v", err)
	}
	if invalidInventory != 0 {
		t.Fatalf("commissioned inventory has %d invalid masks", invalidInventory)
	}

	updated, err := store.UpdateTrainRunStatus(ctx, commissioned.ID, domain.TrainRunStatusBoarding)
	if err != nil {
		t.Fatalf("UpdateTrainRunStatus() error = %v", err)
	}
	if updated.Status != domain.TrainRunStatusBoarding {
		t.Fatalf("UpdateTrainRunStatus() status = %q", updated.Status)
	}
	updated, err = store.UpdateTrainRunStatus(ctx, commissioned.ID, domain.TrainRunStatusCancelled)
	if err != nil || updated.Status != domain.TrainRunStatusCancelled {
		t.Fatalf("cancel train run = %#v, %v", updated, err)
	}
	var cancellationEvents int
	if err := conn.QueryRow(ctx, `
		SELECT count(*)::integer
		FROM outbox_events
		WHERE aggregate_id = $1 AND event_type = 'trainrun.cancelled'
		  AND payload = jsonb_build_object('trainRunId', $1::text, 'status', 'cancelled')
	`, commissioned.ID).Scan(&cancellationEvents); err != nil || cancellationEvents != 1 {
		t.Fatalf("cancellation outbox events = %d, error=%v", cancellationEvents, err)
	}
	if _, err := store.UpdateTrainRunStatus(ctx, commissioned.ID, domain.TrainRunStatusCancelled); err != nil {
		t.Fatalf("idempotent cancellation: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*)::integer FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'trainrun.cancelled'`, commissioned.ID).Scan(&cancellationEvents); err != nil || cancellationEvents != 1 {
		t.Fatalf("idempotent cancellation events = %d, error=%v", cancellationEvents, err)
	}
	if _, err := store.UpdateTrainRunStatus(ctx, commissioned.ID, domain.TrainRunStatusScheduled); !errors.Is(err, domain.ErrInvalidTrainRunTransition) {
		t.Fatalf("reopen cancelled run error = %v, want %v", err, domain.ErrInvalidTrainRunTransition)
	}
	var persistedStatus string
	if err := conn.QueryRow(ctx, `SELECT status FROM train_runs WHERE id = $1`, commissioned.ID).Scan(&persistedStatus); err != nil || persistedStatus != "cancelled" {
		t.Fatalf("terminal status = %q, error=%v", persistedStatus, err)
	}
}

func TestTrainRunCancellationRollsBackWhenOutboxInsertFails(t *testing.T) {
	store, conn := openTestStore(t)
	ctx := context.Background()
	for _, station := range []offeringpostgres.CreateStationParams{{Code: "TPE", Name: "Taipei", Timezone: "UTC"}, {Code: "TXG", Name: "Taichung", Timezone: "UTC"}, {Code: "KHH", Name: "Kaohsiung", Timezone: "UTC"}} {
		if _, err := store.CreateStation(ctx, station); err != nil {
			t.Fatal(err)
		}
	}
	route, err := store.CreateRoute(ctx, offeringpostgres.CreateRouteParams{Route: mustRoute(t), OperatingTimezone: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	train, err := store.CreateTrain(ctx, offeringpostgres.CreateTrainParams{Code: "ROLLBACK_TRAIN", Name: "Rollback Train"})
	if err != nil {
		t.Fatal(err)
	}
	serviceDate := time.Now().UTC().AddDate(0, 0, 1)
	serviceDate = time.Date(serviceDate.Year(), serviceDate.Month(), serviceDate.Day(), 0, 0, 0, 0, time.UTC)
	run, err := store.CommissionTrainRun(ctx, offeringpostgres.CommissionTrainRunParams{TrainID: train.ID, RouteID: route.ID, ServiceDate: serviceDate, ScheduledDepartureAt: serviceDate.Add(5 * time.Minute)})
	if err != nil {
		t.Fatalf("seed train run: %v", err)
	}
	if _, err := conn.Exec(ctx, `DROP TABLE outbox_events`); err != nil {
		t.Fatalf("inject outbox failure: %v", err)
	}
	if _, err := store.UpdateTrainRunStatus(ctx, run.ID, domain.TrainRunStatusCancelled); err == nil {
		t.Fatal("cancellation succeeded without durable outbox table")
	}
	var status string
	if err := conn.QueryRow(ctx, `SELECT status FROM train_runs WHERE id = $1`, run.ID).Scan(&status); err != nil {
		t.Fatalf("read rolled-back train run: %v", err)
	}
	if status != domain.TrainRunStatusScheduled.String() {
		t.Fatalf("status after outbox failure = %q, want scheduled", status)
	}
}

func mustRoute(t *testing.T) domain.Route {
	t.Helper()
	stops := make([]domain.RouteStop, 0, 3)
	for index, stop := range []struct {
		code               string
		arrival, departure int
	}{
		{code: "TPE", arrival: 0, departure: 5},
		{code: "TXG", arrival: 60, departure: 65},
		{code: "KHH", arrival: 120, departure: 125},
	} {
		code, err := domain.NewStationCode(stop.code)
		if err != nil {
			t.Fatal(err)
		}
		routeStop, err := domain.NewRouteStop(code, index, stop.arrival, stop.departure)
		if err != nil {
			t.Fatal(err)
		}
		stops = append(stops, routeStop)
	}
	route, err := domain.NewRoute("WEST", "Western Line", stops)
	if err != nil {
		t.Fatal(err)
	}
	return route
}

func openTestStore(t *testing.T) (*offeringpostgres.Store, *pgx.Conn) {
	t.Helper()

	databaseURL := os.Getenv("DATABASE_URL")
	if strings.TrimSpace(databaseURL) == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect admin database: %v", err)
	}
	schema := "offering_test_" + randomHex(t, 16)
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		_ = admin.Close(ctx)
		t.Fatalf("create test schema: %v", err)
	}

	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse DATABASE_URL: %v", err)
	}
	config.RuntimeParams["search_path"] = schema
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = conn.Close(cleanupCtx)
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
		_ = admin.Close(cleanupCtx)
	})

	applyMigrations(t, ctx, conn)
	store, err := offeringpostgres.NewStore(conn)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	return store, conn
}

func applyMigrations(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test path")
	}
	root := filepath.Join(filepath.Dir(currentFile), "..", "..", "..")
	for _, name := range []string{"000001_accounts.up.sql", "000002_railway_offering.up.sql", "000003_booking.up.sql", "000004_idempotency_outbox.up.sql", "000005_inventory_and_route_integrity.up.sql"} {
		migration, err := os.ReadFile(filepath.Join(root, "migrations", name))
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err := conn.PgConn().Exec(ctx, string(migration)).ReadAll(); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}
}

func randomHex(t *testing.T, byteCount int) string {
	t.Helper()
	value := make([]byte, byteCount)
	if _, err := rand.Read(value); err != nil {
		t.Fatalf("generate schema suffix: %v", err)
	}
	return hex.EncodeToString(value)
}
