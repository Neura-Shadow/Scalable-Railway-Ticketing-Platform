package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	offeringpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/postgres"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/jackc/pgx/v5"
)

func TestStoreSearchAndAvailabilityUseDirectionOverlapAndRunFarePrecedence(t *testing.T) {
	conn := openQueryTestDatabase(t)
	fixture := seedOffering(t, conn)
	store, err := querypostgres.NewStore(conn)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	ctx := context.Background()
	stations, err := store.ListStations(ctx)
	if err != nil || len(stations) != 3 {
		t.Fatalf("ListStations() = %#v, error %v", stations, err)
	}

	results, err := store.SearchTrainRuns(ctx, querypostgres.SearchRequest{
		OriginCode: "TPE", DestinationCode: "KHH", ServiceDate: fixture.serviceDate,
		SeatClass: "standard", Page: 1, PageSize: 25, Sort: "fare_asc",
	})
	if err != nil {
		t.Fatalf("SearchTrainRuns() error = %v", err)
	}
	if len(results) != 1 || results[0].TrainRunID != fixture.trainRunID || results[0].FromStopIndex != 0 || results[0].ToStopIndex != 2 {
		t.Fatalf("SearchTrainRuns() = %#v", results)
	}
	if results[0].FareAmountMinor != 1200 || results[0].Currency != "TWD" {
		t.Fatalf("SearchTrainRuns() fare = %d %s, want run-specific 1200 TWD", results[0].FareAmountMinor, results[0].Currency)
	}
	if results[0].OriginDepartureOffsetMinutes != 5 || results[0].DestinationArrivalOffsetMinutes != 1500 {
		t.Fatalf("SearchTrainRuns() offsets = %d/%d", results[0].OriginDepartureOffsetMinutes, results[0].DestinationArrivalOffsetMinutes)
	}
	if !results[0].DepartureAt.Equal(time.Date(2026, time.July, 19, 16, 5, 0, 0, time.UTC)) ||
		!results[0].ArrivalAt.Equal(time.Date(2026, time.July, 20, 17, 0, 0, 0, time.UTC)) {
		t.Fatalf("SearchTrainRuns() journey times = %v/%v", results[0].DepartureAt, results[0].ArrivalAt)
	}

	reverse, err := store.SearchTrainRuns(ctx, querypostgres.SearchRequest{
		OriginCode: "KHH", DestinationCode: "TPE", ServiceDate: fixture.serviceDate,
		SeatClass: "standard",
	})
	if err != nil {
		t.Fatalf("reverse SearchTrainRuns() error = %v", err)
	}
	if len(reverse) != 0 {
		t.Fatalf("reverse SearchTrainRuns() = %#v, want empty", reverse)
	}

	availability, err := store.Availability(ctx, querypostgres.AvailabilityRequest{
		TrainRunID: fixture.trainRunID, OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard",
	})
	if err != nil {
		t.Fatalf("Availability() error = %v", err)
	}
	if availability.AvailableSeats != 2 || availability.FareAmountMinor != 1200 || availability.TrainCode != "TR200" {
		t.Fatalf("Availability() = %#v, want two seats and run fare", availability)
	}
	if !availability.DepartureAt.Equal(results[0].DepartureAt) || !availability.ArrivalAt.Equal(results[0].ArrivalAt) {
		t.Fatalf("Availability() journey times = %v/%v", availability.DepartureAt, availability.ArrivalAt)
	}

	journey, err := store.ResolveJourney(ctx, fixture.trainRunID, "TPE", "KHH")
	if err != nil {
		t.Fatalf("ResolveJourney() error = %v", err)
	}
	if journey.TrainRunID != fixture.trainRunID || journey.FromStopIndex != 0 || journey.ToStopIndex != 2 || journey.SegmentCount != 2 {
		t.Fatalf("ResolveJourney() = %#v", journey)
	}

	if _, err := conn.Exec(ctx, `
		UPDATE seat_inventory
		SET occupied_segments = B'10'
		WHERE train_run_id = $1
		  AND seat_id = (SELECT seat_id FROM seat_inventory WHERE train_run_id = $1 ORDER BY seat_id LIMIT 1)
	`, fixture.trainRunID); err != nil {
		t.Fatalf("seed authoritative occupied mask: %v", err)
	}
	availability, err = store.Availability(ctx, querypostgres.AvailabilityRequest{
		TrainRunID: fixture.trainRunID, OriginCode: "TPE", DestinationCode: "KHH", SeatClass: "standard",
	})
	if err != nil {
		t.Fatalf("Availability() after occupancy error = %v", err)
	}
	if availability.AvailableSeats != 1 {
		t.Fatalf("Availability() after occupancy = %d, want 1", availability.AvailableSeats)
	}
}

type offeringFixture struct {
	trainRunID  string
	serviceDate time.Time
}

func seedOffering(t *testing.T, conn *pgx.Conn) offeringFixture {
	t.Helper()
	ctx := context.Background()
	store, err := offeringpostgres.NewStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	for _, station := range []offeringpostgres.CreateStationParams{
		{Code: "TPE", Name: "Taipei", Timezone: "Asia/Taipei"},
		{Code: "TXG", Name: "Taichung", Timezone: "Asia/Taipei"},
		{Code: "KHH", Name: "Kaohsiung", Timezone: "Asia/Taipei"},
	} {
		if _, err := store.CreateStation(ctx, station); err != nil {
			t.Fatal(err)
		}
	}
	stopFixtures := []struct {
		code               string
		arrival, departure int
	}{
		{code: "TPE", arrival: 0, departure: 5},
		{code: "TXG", arrival: 60, departure: 65},
		{code: "KHH", arrival: 1500, departure: 1505},
	}
	stops := make([]domain.RouteStop, 0, len(stopFixtures))
	for index, fixture := range stopFixtures {
		code, err := domain.NewStationCode(fixture.code)
		if err != nil {
			t.Fatal(err)
		}
		stop, err := domain.NewRouteStop(code, index, fixture.arrival, fixture.departure)
		if err != nil {
			t.Fatal(err)
		}
		stops = append(stops, stop)
	}
	routeModel, err := domain.NewRoute("WEST", "Western Line", stops)
	if err != nil {
		t.Fatal(err)
	}
	route, err := store.CreateRoute(ctx, offeringpostgres.CreateRouteParams{Route: routeModel, OperatingTimezone: "Asia/Taipei"})
	if err != nil {
		t.Fatal(err)
	}
	train, err := store.CreateTrain(ctx, offeringpostgres.CreateTrainParams{Code: "TR200", Name: "Query Express"})
	if err != nil {
		t.Fatal(err)
	}
	coach, err := store.CreateCoach(ctx, offeringpostgres.CreateCoachParams{TrainID: train.ID, CoachNumber: "1", SeatClass: domain.SeatClassStandard})
	if err != nil {
		t.Fatal(err)
	}
	for _, number := range []string{"1A", "1B"} {
		if _, err := store.CreateSeat(ctx, offeringpostgres.CreateSeatParams{CoachID: coach.ID, SeatNumber: number, SeatType: "window"}); err != nil {
			t.Fatal(err)
		}
	}
	serviceDate := time.Date(2026, time.July, 20, 0, 0, 0, 0, time.UTC)
	run, err := store.CommissionTrainRun(ctx, offeringpostgres.CommissionTrainRunParams{
		TrainID: train.ID, RouteID: route.ID, ServiceDate: serviceDate,
		ScheduledDepartureAt: time.Date(2026, time.July, 19, 16, 5, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, fare := range []offeringpostgres.CreateFareParams{
		{RouteID: route.ID, FromStopIndex: 0, ToStopIndex: 2, SeatClass: domain.SeatClassStandard, AmountMinor: 1000, Currency: "TWD"},
		{TrainRunID: run.ID, FromStopIndex: 0, ToStopIndex: 2, SeatClass: domain.SeatClassStandard, AmountMinor: 1200, Currency: "TWD"},
	} {
		if _, err := store.CreateFare(ctx, fare); err != nil {
			t.Fatal(err)
		}
	}
	return offeringFixture{trainRunID: run.ID, serviceDate: serviceDate}
}

func openQueryTestDatabase(t *testing.T) *pgx.Conn {
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
	schema := "query_test_" + queryRandomHex(t, 16)
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.RuntimeParams["search_path"] = schema
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
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

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test path")
	}
	root := filepath.Join(filepath.Dir(currentFile), "..", "..", "..")
	for _, name := range []string{"000001_accounts.up.sql", "000002_railway_offering.up.sql", "000003_booking.up.sql", "000004_idempotency_outbox.up.sql", "000005_inventory_and_route_integrity.up.sql"} {
		migration, err := os.ReadFile(filepath.Join(root, "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.PgConn().Exec(ctx, string(migration)).ReadAll(); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	return conn
}

func queryRandomHex(t *testing.T, byteCount int) string {
	t.Helper()
	value := make([]byte, byteCount)
	if _, err := rand.Read(value); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(value)
}
