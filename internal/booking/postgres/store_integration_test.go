package postgres

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConcurrentOverlappingHoldsAllocateOneSeatOnce(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	const attempts = 100
	start := make(chan struct{})
	results := make(chan error, attempts)
	var wait sync.WaitGroup
	for index := 0; index < attempts; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			_, err := store.CreateHold(ctx, CreateHoldParams{
				UserID: fixture.userID, TrainRunID: fixture.trainRunID,
				FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
				PassengerIDs: fixture.passengerIDs[index%12 : index%12+1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
				IdempotencyKeyHash: hashWithByte(byte(index + 1)), RequestFingerprint: hashWithByte(0x91),
				IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
			})
			results <- err
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)

	successes := 0
	conflicts := 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrInsufficientInventory), errors.Is(err, ErrPassengerConflict):
			conflicts++
		default:
			t.Fatalf("concurrent hold error = %v", err)
		}
	}
	if successes != 1 || conflicts != attempts-1 {
		t.Fatalf("concurrent results: successes=%d conflicts=%d", successes, conflicts)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("reconcile one-seat race: %v", err)
	}
}

func TestActiveReservationSeatPreventsInventoryDeletionAndReinitializationOversell(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	hold, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
		IdempotencyKeyHash: hashWithByte(0x31), RequestFingerprint: hashWithByte(0x32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}
	commandTag, err := fixture.pool.Exec(ctx, `
DELETE FROM seat_inventory AS si
USING reservation_seats AS rs
WHERE rs.reservation_id = $1
  AND si.train_run_id = $2
  AND si.seat_id = rs.seat_id`, hold.ReservationID, fixture.trainRunID)
	if err == nil || commandTag.RowsAffected() != 0 {
		t.Fatalf("delete referenced inventory: affected=%d error=%v, want foreign-key rejection", commandTag.RowsAffected(), err)
	}
	inserted, err := store.InitializeInventory(ctx, fixture.trainRunID)
	if err != nil {
		t.Fatalf("reinitialize inventory: %v", err)
	}
	if inserted != 0 {
		t.Fatalf("reinitialized inventory rows = %d, want 0", inserted)
	}
	_, err = store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: fixture.passengerIDs[1:2], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
		IdempotencyKeyHash: hashWithByte(0x33), RequestFingerprint: hashWithByte(0x34),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if !errors.Is(err, ErrInsufficientInventory) {
		t.Fatalf("overlapping hold after rejected delete error = %v, want %v", err, ErrInsufficientInventory)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("reconcile after rejected inventory deletion: %v", err)
	}
}

func TestVersionFiveMigrationAcceptsVersionFourReservationSeatInsert(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	reservationID := insertVersionFourReservationSeat(t, fixture.pool, fixture)

	var derivedTrainRunID uuid.UUID
	if err := fixture.pool.QueryRow(ctx, `
SELECT train_run_id
FROM reservation_seats
WHERE reservation_id = $1`, reservationID).Scan(&derivedTrainRunID); err != nil {
		t.Fatalf("read derived train-run ID: %v", err)
	}
	if derivedTrainRunID != fixture.trainRunID {
		t.Fatalf("derived train_run_id=%s want=%s", derivedTrainRunID, fixture.trainRunID)
	}
}

func TestVersionFiveMigrationBackfillsAndRoundTripsVersionFourData(t *testing.T) {
	pool := newIsolatedDatabaseThrough(t, "000004_idempotency_outbox.up.sql")
	fixture := seedFixture(t, pool, 1)
	fixture.pool = pool
	store := New(pool)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize version-4 inventory: %v", err)
	}
	reservationID := insertVersionFourReservationSeat(t, pool, fixture)

	applyMigrationFile(t, ctx, pool, "000005_inventory_and_route_integrity.up.sql")
	assertReservationSeatTrainRun(t, pool, reservationID, fixture.trainRunID)
	applyMigrationFile(t, ctx, pool, "000005_inventory_and_route_integrity.down.sql")
	var legacyColumnCount int
	if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM information_schema.columns
WHERE table_schema = current_schema()
  AND table_name = 'reservation_seats'
  AND column_name = 'train_run_id'`).Scan(&legacyColumnCount); err != nil {
		t.Fatalf("inspect version-4 reservation-seat shape: %v", err)
	}
	if legacyColumnCount != 0 {
		t.Fatalf("train_run_id columns after down=%d want=0", legacyColumnCount)
	}
	applyMigrationFile(t, ctx, pool, "000005_inventory_and_route_integrity.up.sql")
	assertReservationSeatTrainRun(t, pool, reservationID, fixture.trainRunID)
}

func TestVersionFiveMigrationRejectsInvalidVersionFourRouteMove(t *testing.T) {
	pool := newIsolatedDatabaseThrough(t, "000004_idempotency_outbox.up.sql")
	fixture := seedFixture(t, pool, 1)
	ctx := context.Background()
	var sourceRouteID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT route_id FROM train_runs WHERE id = $1`, fixture.trainRunID).Scan(&sourceRouteID); err != nil {
		t.Fatalf("load source route: %v", err)
	}
	targetRouteID := uuid.New()
	targetStationA, targetStationB := uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `
INSERT INTO stations (id, code, name, timezone)
VALUES ($1, 'B0', 'Target Station 0', 'Asia/Taipei'),
       ($2, 'B1', 'Target Station 1', 'Asia/Taipei')`,
		targetStationA, targetStationB); err != nil {
		t.Fatalf("seed target stations: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO routes (id, code, name, operating_timezone)
VALUES ($1, 'R2', 'Target Route', 'Asia/Taipei')`, targetRouteID); err != nil {
		t.Fatalf("seed target route: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO route_stops (route_id, station_id, stop_index, arrival_offset_minutes, departure_offset_minutes)
VALUES ($1, $2, 0, 0, 0), ($1, $3, 1, 10, 10)`,
		targetRouteID, targetStationA, targetStationB); err != nil {
		t.Fatalf("seed target route stops: %v", err)
	}
	// Version 4 validates only NEW.route_id on a move, so the target remains
	// contiguous while the source is silently left with a gap.
	if _, err := pool.Exec(ctx, `
UPDATE route_stops
SET route_id = $1, stop_index = 2
WHERE route_id = $2 AND stop_index = 1`, targetRouteID, sourceRouteID); err != nil {
		t.Fatalf("create version-4 route gap: %v", err)
	}
	if err := execMigrationFile(ctx, pool, "000005_inventory_and_route_integrity.up.sql"); err == nil {
		t.Fatal("migration 5 accepted an existing non-contiguous route")
	}
}

func TestVersionFiveMigrationRejectsExistingCrossTrainInventory(t *testing.T) {
	pool := newIsolatedDatabaseThrough(t, "000004_idempotency_outbox.up.sql")
	fixture := seedFixture(t, pool, 1)
	ctx := context.Background()
	otherTrainID, otherCoachID, otherSeatID := uuid.New(), uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO trains (id, code, name) VALUES ($1, 'T2', 'Other Train')`, otherTrainID); err != nil {
		t.Fatalf("seed other train: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO coaches (id, train_id, coach_number, seat_class) VALUES ($2, $1, '1', 'standard')`, otherTrainID, otherCoachID); err != nil {
		t.Fatalf("seed other coach: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO seats (id, coach_id, seat_number) VALUES ($2, $1, '01A')`, otherCoachID, otherSeatID); err != nil {
		t.Fatalf("seed other seat: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO seat_inventory (
    train_run_id, segment_count, seat_id, seat_class, occupied_segments
) VALUES ($1, 3, $2, 'standard', B'000')`, fixture.trainRunID, otherSeatID); err != nil {
		t.Fatalf("seed version-4 cross-train inventory: %v", err)
	}
	if err := execMigrationFile(ctx, pool, "000005_inventory_and_route_integrity.up.sql"); err == nil {
		t.Fatal("migration 5 accepted existing cross-train inventory")
	}
}

func insertVersionFourReservationSeat(t *testing.T, pool *pgxpool.Pool, fixture integrationFixture) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin legacy writer transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	reservationID := uuid.New()
	var seatID uuid.UUID
	if err := tx.QueryRow(ctx, `
UPDATE seat_inventory
SET occupied_segments = B'110'
WHERE train_run_id = $1
RETURNING seat_id`, fixture.trainRunID).Scan(&seatID); err != nil {
		t.Fatalf("allocate legacy seat: %v", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO reservations (
    id, user_id, train_run_id, segment_count, from_stop_index, to_stop_index,
    seat_class, status, expires_at, total_amount_minor, currency
) VALUES ($1, $2, $3, 3, 0, 2, 'standard', 'held', clock_timestamp() + interval '10 minutes', 1250, 'TWD')`,
		reservationID, fixture.userID, fixture.trainRunID); err != nil {
		t.Fatalf("insert legacy reservation: %v", err)
	}
	// Exact version-4 column list: version 5 must backfill or derive the new
	// train_run_id without interrupting a rolling deployment.
	if _, err := tx.Exec(ctx, `
INSERT INTO reservation_seats (
    reservation_id, segment_count, seat_id, passenger_id, segment_mask,
    fare_amount_minor, currency
) VALUES ($1, 3, $2, $3, B'110', 1250, 'TWD')`,
		reservationID, seatID, fixture.passengerIDs[0]); err != nil {
		t.Fatalf("insert version-4 reservation seat: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit legacy writer transaction: %v", err)
	}
	return reservationID
}

func assertReservationSeatTrainRun(t *testing.T, pool *pgxpool.Pool, reservationID, want uuid.UUID) {
	t.Helper()
	var got uuid.UUID
	if err := pool.QueryRow(context.Background(), `
SELECT train_run_id
FROM reservation_seats
WHERE reservation_id = $1`, reservationID).Scan(&got); err != nil {
		t.Fatalf("read derived train-run ID: %v", err)
	}
	if got != want {
		t.Fatalf("derived train_run_id=%s want=%s", got, want)
	}
}

func TestReconciliationDetectsPrivilegedInventoryOrphanBeforeAndAfterReinitialization(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	hold, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
		IdempotencyKeyHash: hashWithByte(0x35), RequestFingerprint: hashWithByte(0x36),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}
	conn, err := fixture.pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire privileged connection: %v", err)
	}
	if _, err := conn.Exec(ctx, `SET session_replication_role = replica`); err != nil {
		conn.Release()
		t.Skipf("database role cannot exercise privileged-deletion reconciliation path: %v", err)
	}
	_, deleteErr := conn.Exec(ctx, `
DELETE FROM seat_inventory AS si
USING reservation_seats AS rs
WHERE rs.reservation_id = $1
  AND si.train_run_id = $2
  AND si.seat_id = rs.seat_id`, hold.ReservationID, fixture.trainRunID)
	_, restoreErr := conn.Exec(ctx, `SET session_replication_role = origin`)
	conn.Release()
	if deleteErr != nil || restoreErr != nil {
		t.Fatalf("inject privileged inventory orphan: delete=%v restore=%v", deleteErr, restoreErr)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); !errors.Is(err, ErrPersistenceInvariant) {
		t.Fatalf("reconcile orphan error = %v, want %v", err, ErrPersistenceInvariant)
	}
	inserted, err := store.InitializeInventory(ctx, fixture.trainRunID)
	if err != nil || inserted != 1 {
		t.Fatalf("reinitialize orphaned inventory: inserted=%d error=%v", inserted, err)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); !errors.Is(err, ErrPersistenceInvariant) {
		t.Fatalf("reconcile zero-mask recreation error = %v, want %v", err, ErrPersistenceInvariant)
	}
}

func TestDeactivatedSeatIsNotAllocatableFromExistingInventory(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, `
		UPDATE seats AS s
		SET active = false
		FROM seat_inventory AS si
		WHERE si.train_run_id = $1 AND s.id = si.seat_id
	`, fixture.trainRunID); err != nil {
		t.Fatalf("deactivate seat: %v", err)
	}
	_, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
		IdempotencyKeyHash: hashWithByte(0x37), RequestFingerprint: hashWithByte(0x38),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if !errors.Is(err, ErrInsufficientInventory) {
		t.Fatalf("hold using deactivated seat error = %v, want %v", err, ErrInsufficientInventory)
	}
}

func TestInventoryRejectsSeatFromDifferentTrain(t *testing.T) {
	_, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	otherTrainID, otherCoachID, otherSeatID := uuid.New(), uuid.New(), uuid.New()
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO trains (id, code, name) VALUES ($1, 'OTHER_TRAIN', 'Other Train')`, otherTrainID); err != nil {
		t.Fatalf("seed other train seat: %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO coaches (id, train_id, coach_number, seat_class) VALUES ($1, $2, '1', 'standard')`, otherCoachID, otherTrainID); err != nil {
		t.Fatalf("seed other train coach: %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO seats (id, coach_id, seat_number) VALUES ($1, $2, '01A')`, otherSeatID, otherCoachID); err != nil {
		t.Fatalf("seed other train seat: %v", err)
	}
	_, err := fixture.pool.Exec(ctx, `
		INSERT INTO seat_inventory (train_run_id, segment_count, seat_id, seat_class, occupied_segments)
		VALUES ($1, 3, $2, 'standard', B'000')
	`, fixture.trainRunID, otherSeatID)
	if err == nil {
		t.Fatal("inventory accepted a seat belonging to a different train")
	}
}

func TestConcurrentHoldsNeverExceedSeatCapacity(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 10)
	// This test isolates the seat-capacity invariant. Quota concurrency has
	// dedicated tests, so keep its independent bound above the attempt count.
	store = NewWithReservationQuotaLimits(store.pool, ReservationQuotaLimits{
		MaxActiveHoldsPerUser:            200,
		MaxActiveHoldsPerUserPerTrainRun: 200,
		MaxActivePassengersPerUser:       200,
	})
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	const attempts = 100
	start := make(chan struct{})
	results := make(chan error, attempts)
	var wait sync.WaitGroup
	for index := 0; index < attempts; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			_, err := store.CreateHold(ctx, CreateHoldParams{
				UserID: fixture.userID, TrainRunID: fixture.trainRunID,
				FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
				PassengerIDs: fixture.passengerIDs[index%12 : index%12+1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
				IdempotencyKeyHash: hashWithByte(byte(index + 101)), RequestFingerprint: hashWithByte(0x92),
				IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
			})
			results <- err
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)

	successes := 0
	for err := range results {
		if err == nil {
			successes++
			continue
		}
		if !errors.Is(err, ErrInsufficientInventory) && !errors.Is(err, ErrPassengerConflict) {
			t.Fatalf("concurrent capacity error = %v", err)
		}
	}
	if successes != 10 {
		t.Fatalf("successful holds = %d, want exact seat capacity 10", successes)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("reconcile capacity race: %v", err)
	}
}

func TestConcurrentSameIdempotencyKeyReturnsOneReservation(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 2)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	params := CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
		IdempotencyKeyHash: hashWithByte(0xa1), RequestFingerprint: hashWithByte(0xa2),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	}
	const attempts = 100
	start := make(chan struct{})
	results := make(chan CreateHoldResult, attempts)
	errorsFound := make(chan error, attempts)
	var wait sync.WaitGroup
	for range attempts {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := store.CreateHold(ctx, params)
			if err != nil {
				errorsFound <- err
				return
			}
			results <- result
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("same-key hold failed: %v", err)
	}

	var reservationID uuid.UUID
	created := 0
	count := 0
	for result := range results {
		count++
		if reservationID == uuid.Nil {
			reservationID = result.ReservationID
		}
		if result.ReservationID != reservationID {
			t.Fatalf("same key returned reservation %s and %s", reservationID, result.ReservationID)
		}
		if !result.Replayed {
			created++
		}
	}
	if count != attempts || created != 1 {
		t.Fatalf("same-key results: count=%d newly-created=%d", count, created)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("reconcile same-key race: %v", err)
	}
}

func TestConcurrentDifferentKeysCannotDoubleBookSamePassengerOnTrainRun(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 2)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for index := range 2 {
		go func(index int) {
			<-start
			_, err := store.CreateHold(ctx, CreateHoldParams{
				UserID: fixture.userID, TrainRunID: fixture.trainRunID,
				FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
				PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
				IdempotencyKeyHash: hashWithByte(byte(0xa5 + index)), RequestFingerprint: hashWithByte(byte(0xa7 + index)),
				IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
			})
			results <- err
		}(index)
	}
	close(start)
	successes, conflicts := 0, 0
	for range 2 {
		switch err := <-results; {
		case err == nil:
			successes++
		case errors.Is(err, ErrPassengerConflict):
			conflicts++
		default:
			t.Fatalf("same-passenger race error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("same-passenger race: successes=%d conflicts=%d", successes, conflicts)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("reconcile same-passenger race: %v", err)
	}
}

func TestOneSeatSupportsNonOverlappingIntervalsButRejectsOverlap(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, `
INSERT INTO fares (train_run_id, from_stop_index, to_stop_index, seat_class, amount_minor, currency)
VALUES ($1, 2, 3, 'standard', 500, 'TWD'), ($1, 1, 3, 'standard', 900, 'TWD')`, fixture.trainRunID); err != nil {
		t.Fatalf("seed interval fares: %v", err)
	}

	first, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard", PassengerIDs: fixture.passengerIDs[:1],
		HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute), IdempotencyKeyHash: hashWithByte(0xb1),
		RequestFingerprint: hashWithByte(0xb2), IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create A-C hold: %v", err)
	}
	if first.ReservationID == uuid.Nil {
		t.Fatal("A-C hold returned nil reservation")
	}
	_, err = store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 2, ToStopIndex: 3, SeatClass: "standard", PassengerIDs: fixture.passengerIDs[1:2],
		HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute), IdempotencyKeyHash: hashWithByte(0xb3),
		RequestFingerprint: hashWithByte(0xb4), IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create C-D non-overlapping hold: %v", err)
	}
	_, err = store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 1, ToStopIndex: 3, SeatClass: "standard", PassengerIDs: fixture.passengerIDs[2:3],
		HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute), IdempotencyKeyHash: hashWithByte(0xb5),
		RequestFingerprint: hashWithByte(0xb6), IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if !errors.Is(err, ErrInsufficientInventory) {
		t.Fatalf("overlapping B-D hold error = %v, want %v", err, ErrInsufficientInventory)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("reconcile non-overlapping holds: %v", err)
	}
}

func TestIdempotencyAcquisitionReplaysCompletedResourceAndRejectsChangedRequest(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	keyHash := bytes.Repeat([]byte{0x11}, 32)
	fingerprint := bytes.Repeat([]byte{0x22}, 32)
	resourceID := uuid.New()

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin owner transaction: %v", err)
	}
	acquisition, err := tx.AcquireIdempotency(ctx, IdempotencyInput{
		UserID:             fixture.userID,
		Operation:          OperationReservationCreate,
		KeyHash:            keyHash,
		RequestFingerprint: fingerprint,
		ExpiresAt:          time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("acquire owner: %v", err)
	}
	if !acquisition.Owned || acquisition.Replayed {
		t.Fatalf("owner acquisition = %#v", acquisition)
	}
	if err := tx.CompleteIdempotency(ctx, acquisition.RecordID, resourceID); err != nil {
		t.Fatalf("complete idempotency: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit owner: %v", err)
	}

	replayTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin replay transaction: %v", err)
	}
	defer func() { _ = replayTx.Rollback(context.Background()) }()
	replay, err := replayTx.AcquireIdempotency(ctx, IdempotencyInput{
		UserID:             fixture.userID,
		Operation:          OperationReservationCreate,
		KeyHash:            keyHash,
		RequestFingerprint: fingerprint,
		ExpiresAt:          time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("acquire replay: %v", err)
	}
	if !replay.Replayed || replay.Owned || replay.ResourceID != resourceID {
		t.Fatalf("replay acquisition = %#v, want resource %s", replay, resourceID)
	}
	_ = replayTx.Rollback(ctx)

	conflictTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin conflict transaction: %v", err)
	}
	defer func() { _ = conflictTx.Rollback(context.Background()) }()
	_, err = conflictTx.AcquireIdempotency(ctx, IdempotencyInput{
		UserID:             fixture.userID,
		Operation:          OperationReservationCreate,
		KeyHash:            keyHash,
		RequestFingerprint: bytes.Repeat([]byte{0x33}, 32),
		ExpiresAt:          time.Now().UTC().Add(24 * time.Hour),
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed fingerprint error = %v, want %v", err, ErrIdempotencyConflict)
	}
}

func TestExpiredIdempotencyRecordCanBeReacquiredWithNewFingerprint(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	keyHash := hashWithByte(0x41)
	oldFingerprint := hashWithByte(0x42)
	oldRecordID, resourceID := uuid.New(), uuid.New()
	if _, err := fixture.pool.Exec(ctx, `
		INSERT INTO idempotency_records (
			id, user_id, operation, key_hash, request_fingerprint, status,
			resource_type, resource_id, expires_at, created_at
		)
		VALUES ($1, $2, 'reservation.create', $3, $4, 'completed',
		        'reservation', $5, clock_timestamp() - interval '1 hour',
		        clock_timestamp() - interval '2 hours')
	`, oldRecordID, fixture.userID, keyHash, oldFingerprint, resourceID); err != nil {
		t.Fatalf("seed expired idempotency record: %v", err)
	}
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	acquisition, err := tx.AcquireIdempotency(ctx, IdempotencyInput{
		UserID: fixture.userID, Operation: OperationReservationCreate,
		KeyHash: keyHash, RequestFingerprint: hashWithByte(0x43),
		ExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("reacquire expired key: %v", err)
	}
	if !acquisition.Owned || acquisition.Replayed || acquisition.RecordID == oldRecordID {
		t.Fatalf("expired key acquisition = %#v", acquisition)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestExpirationPassCleansExpiredIdempotencyRecordsInBoundedBatch(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	for index := 0; index < 3; index++ {
		if _, err := fixture.pool.Exec(ctx, `
			INSERT INTO idempotency_records (
				user_id, operation, key_hash, request_fingerprint, expires_at, created_at
			) VALUES ($1, 'reservation.create', $2, $3,
			          clock_timestamp() - interval '1 hour', clock_timestamp() - interval '2 hours')
		`, fixture.userID, hashWithByte(byte(0x50+index)), hashWithByte(byte(0x60+index))); err != nil {
			t.Fatal(err)
		}
	}
	if expired, err := store.ExpireDue(ctx, time.Now().UTC(), 10); err != nil || len(expired) != 0 {
		t.Fatalf("expiration cleanup pass: expired=%v error=%v", expired, err)
	}
	var remaining int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*)::integer FROM idempotency_records WHERE expires_at <= clock_timestamp()`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("expired idempotency records remaining = %d", remaining)
	}
}

func TestCreateHoldRollsBackInventoryReservationAndIdempotencyWhenOutboxInsertFails(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, `DROP TABLE outbox_events`); err != nil {
		t.Fatalf("inject outbox failure: %v", err)
	}
	_, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
		IdempotencyKeyHash: hashWithByte(0x44), RequestFingerprint: hashWithByte(0x45),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err == nil {
		t.Fatal("create hold succeeded without durable outbox table")
	}
	var reservations, idempotency, occupiedBits int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*)::integer FROM reservations WHERE train_run_id = $1`, fixture.trainRunID).Scan(&reservations); err != nil {
		t.Fatal(err)
	}
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*)::integer FROM idempotency_records WHERE user_id = $1`, fixture.userID).Scan(&idempotency); err != nil {
		t.Fatal(err)
	}
	if err := fixture.pool.QueryRow(ctx, `SELECT coalesce(sum(bit_count(occupied_segments)), 0)::integer FROM seat_inventory WHERE train_run_id = $1`, fixture.trainRunID).Scan(&occupiedBits); err != nil {
		t.Fatal(err)
	}
	if reservations != 0 || idempotency != 0 || occupiedBits != 0 {
		t.Fatalf("rollback state: reservations=%d idempotency=%d occupied_bits=%d", reservations, idempotency, occupiedBits)
	}
}

func TestMigrationConstraintsRejectInvalidReservationAndOutboxStates(t *testing.T) {
	_, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	_, err := fixture.pool.Exec(ctx, `
		INSERT INTO reservations (
			user_id, train_run_id, segment_count, from_stop_index, to_stop_index,
			seat_class, status, expires_at, total_amount_minor, currency
		) VALUES ($1, $2, 3, 0, 2, 'standard', 'paid', clock_timestamp() + interval '10 minutes', 100, 'TWD')
	`, fixture.userID, fixture.trainRunID)
	if err == nil {
		t.Fatal("reservation status constraint accepted paid")
	}
	_, err = fixture.pool.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
		VALUES ('reservation', $1, 'reservation.unknown', '{}'::jsonb)
	`, uuid.New())
	if err == nil {
		t.Fatal("outbox event-type constraint accepted unknown event")
	}
}

func TestCreateHoldCommitsAllocationIdempotencyAndOutboxTogether(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 2)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	params := CreateHoldParams{
		UserID:               fixture.userID,
		TrainRunID:           fixture.trainRunID,
		FromStopIndex:        0,
		ToStopIndex:          2,
		SeatClass:            "STANDARD",
		PassengerIDs:         fixture.passengerIDs[:2],
		HoldExpiresAt:        time.Now().UTC().Add(10 * time.Minute),
		IdempotencyKeyHash:   bytes.Repeat([]byte{0x41}, 32),
		RequestFingerprint:   bytes.Repeat([]byte{0x42}, 32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	}
	created, err := store.CreateHold(ctx, params)
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}
	if created.Replayed || created.ReservationID == uuid.Nil || created.SeatCount != 2 {
		t.Fatalf("created hold = %#v", created)
	}
	if created.TotalAmountMinor != 2*fixture.standardFare || created.Currency != "TWD" {
		t.Fatalf("created total = %d %s, want 2500 TWD", created.TotalAmountMinor, created.Currency)
	}

	replayed, err := store.CreateHold(ctx, params)
	if err != nil {
		t.Fatalf("replay hold: %v", err)
	}
	if !replayed.Replayed || replayed.ReservationID != created.ReservationID {
		t.Fatalf("replayed hold = %#v, want reservation %s", replayed, created.ReservationID)
	}

	record, err := store.GetReservation(ctx, fixture.userID, created.ReservationID)
	if err != nil {
		t.Fatalf("get reservation: %v", err)
	}
	if record.Status != "held" || record.SeatCount != 2 || record.OutboxEventCount != 1 {
		t.Fatalf("reservation record = %#v", record)
	}
}

func TestConfirmThenCancelCreatesTicketsOnceAndReleasesExactMasks(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	hold, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
		IdempotencyKeyHash: bytes.Repeat([]byte{0x51}, 32), RequestFingerprint: bytes.Repeat([]byte{0x52}, 32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}

	confirmed, err := store.ConfirmReservation(ctx, ReservationCommandParams{
		UserID: fixture.userID, ReservationID: hold.ReservationID, Now: time.Now().UTC(),
		IdempotencyKeyHash: bytes.Repeat([]byte{0x61}, 32), RequestFingerprint: bytes.Repeat([]byte{0x62}, 32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("confirm reservation: %v", err)
	}
	if confirmed.TicketOrderID == uuid.Nil || confirmed.TicketCount != 1 || confirmed.Replayed {
		t.Fatalf("confirmed result = %#v", confirmed)
	}

	cancelled, err := store.CancelReservation(ctx, ReservationCommandParams{
		UserID: fixture.userID, ReservationID: hold.ReservationID, Now: time.Now().UTC(),
		IdempotencyKeyHash: bytes.Repeat([]byte{0x71}, 32), RequestFingerprint: bytes.Repeat([]byte{0x72}, 32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("cancel reservation: %v", err)
	}
	if cancelled.ReleasedSeatCount != 1 || cancelled.Replayed {
		t.Fatalf("cancelled result = %#v", cancelled)
	}
	confirmationReplay, err := store.ConfirmReservation(ctx, ReservationCommandParams{
		UserID: fixture.userID, ReservationID: hold.ReservationID, Now: time.Now().UTC(),
		IdempotencyKeyHash: bytes.Repeat([]byte{0x61}, 32), RequestFingerprint: bytes.Repeat([]byte{0x62}, 32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("replay confirmation after cancellation: %v", err)
	}
	if !confirmationReplay.Replayed || confirmationReplay.TicketOrderID != confirmed.TicketOrderID {
		t.Fatalf("confirmation replay = %#v, want order %s", confirmationReplay, confirmed.TicketOrderID)
	}
	cancellationReplay, err := store.CancelReservation(ctx, ReservationCommandParams{
		UserID: fixture.userID, ReservationID: hold.ReservationID, Now: time.Now().UTC(),
		IdempotencyKeyHash: bytes.Repeat([]byte{0x71}, 32), RequestFingerprint: bytes.Repeat([]byte{0x72}, 32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("replay cancellation: %v", err)
	}
	if !cancellationReplay.Replayed || cancellationReplay.ReleasedSeatCount != cancelled.ReleasedSeatCount {
		t.Fatalf("cancellation replay = %#v, want released count %d", cancellationReplay, cancelled.ReleasedSeatCount)
	}

	record, err := store.GetReservation(ctx, fixture.userID, hold.ReservationID)
	if err != nil {
		t.Fatalf("get cancelled reservation: %v", err)
	}
	if record.Status != "cancelled" || record.ActiveTicketCount != 0 || record.OutboxEventCount != 3 {
		t.Fatalf("cancelled reservation record = %#v", record)
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin inventory check: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	masks, err := tx.InventoryMasks(ctx, fixture.trainRunID)
	if err != nil {
		t.Fatalf("get inventory masks: %v", err)
	}
	if len(masks) != 1 || !masks[0].Mask.IsZero() {
		t.Fatalf("inventory after cancel = %#v, want exact mask released", masks)
	}
}

func TestExpireDueTransitionsOnceAndReleasesInventory(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	expiresAt := time.Now().UTC().Add(time.Minute)
	hold, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: expiresAt,
		IdempotencyKeyHash: bytes.Repeat([]byte{0x81}, 32), RequestFingerprint: bytes.Repeat([]byte{0x82}, 32),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}
	forceReservationsDue(t, fixture.pool, hold.ReservationID)

	type expiryResult struct {
		ids []uuid.UUID
		err error
	}
	start := make(chan struct{})
	results := make(chan expiryResult, 2)
	for range 2 {
		go func() {
			<-start
			ids, err := store.ExpireDue(ctx, expiresAt.Add(time.Second), 10)
			results <- expiryResult{ids: ids, err: err}
		}()
	}
	close(start)
	var expired []uuid.UUID
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("expire worker: %v", result.err)
		}
		expired = append(expired, result.ids...)
	}
	if len(expired) != 1 || expired[0] != hold.ReservationID {
		t.Fatalf("expired IDs across workers = %v, want %s once", expired, hold.ReservationID)
	}
	expiredAgain, err := store.ExpireDue(ctx, expiresAt.Add(2*time.Second), 10)
	if err != nil {
		t.Fatalf("expire due again: %v", err)
	}
	if len(expiredAgain) != 0 {
		t.Fatalf("second expiry returned %v, want none", expiredAgain)
	}
}

func TestConfirmRejectsDatabaseExpiredHoldDespiteLaggingCallerClock(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	hold, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
		PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: time.Now().UTC().Add(time.Minute),
		IdempotencyKeyHash: hashWithByte(0x83), RequestFingerprint: hashWithByte(0x84),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}
	forceReservationsDue(t, fixture.pool, hold.ReservationID)

	_, err = store.ConfirmReservation(ctx, ReservationCommandParams{
		UserID: fixture.userID, ReservationID: hold.ReservationID,
		Now:                  time.Now().UTC().Add(-24 * time.Hour),
		IdempotencyKeyHash:   hashWithByte(0x85),
		RequestFingerprint:   hashWithByte(0x86),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if !errors.Is(err, ErrReservationExpired) {
		t.Fatalf("confirm expired hold error = %v, want %v", err, ErrReservationExpired)
	}
	record, err := store.GetReservation(ctx, fixture.userID, hold.ReservationID)
	if err != nil {
		t.Fatalf("get expired hold: %v", err)
	}
	if record.Status != "held" || record.ActiveTicketCount != 0 {
		t.Fatalf("expired confirmation changed state: %#v", record)
	}
}

func TestExpireDueIsolatesOneCorruptReservationAndContinues(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 2)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	expiresAt := time.Now().UTC().Add(time.Minute)
	create := func(index int) CreateHoldResult {
		t.Helper()
		result, err := store.CreateHold(ctx, CreateHoldParams{
			UserID: fixture.userID, TrainRunID: fixture.trainRunID,
			FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
			PassengerIDs: fixture.passengerIDs[index : index+1], HoldExpiresAt: expiresAt,
			IdempotencyKeyHash: hashWithByte(byte(0xc1 + index)), RequestFingerprint: hashWithByte(byte(0xd1 + index)),
			IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		})
		if err != nil {
			t.Fatalf("create hold %d: %v", index, err)
		}
		return result
	}
	corrupt := create(0)
	healthy := create(1)
	forceReservationsDue(t, fixture.pool, corrupt.ReservationID, healthy.ReservationID)
	if _, err := fixture.pool.Exec(ctx, `
UPDATE seat_inventory AS si
SET occupied_segments = repeat('0', bit_length(si.occupied_segments))::bit varying
FROM reservations AS r
JOIN reservation_seats AS rs ON rs.reservation_id = r.id
WHERE r.id = $1
  AND si.train_run_id = r.train_run_id
  AND si.seat_id = rs.seat_id`, corrupt.ReservationID); err != nil {
		t.Fatalf("inject inventory mismatch: %v", err)
	}

	expired, err := store.ExpireDue(ctx, expiresAt.Add(time.Second), 10)
	if !errors.Is(err, ErrPersistenceInvariant) {
		t.Fatalf("expire batch error = %v, want persistence invariant", err)
	}
	if len(expired) != 1 || expired[0] != healthy.ReservationID {
		t.Fatalf("expired IDs = %v, want only healthy %s", expired, healthy.ReservationID)
	}
	corruptRecord, err := store.GetReservation(ctx, fixture.userID, corrupt.ReservationID)
	if err != nil {
		t.Fatalf("get corrupt reservation: %v", err)
	}
	healthyRecord, err := store.GetReservation(ctx, fixture.userID, healthy.ReservationID)
	if err != nil {
		t.Fatalf("get healthy reservation: %v", err)
	}
	if corruptRecord.Status != "held" || healthyRecord.Status != "expired" {
		t.Fatalf("statuses after isolated failure: corrupt=%s healthy=%s", corruptRecord.Status, healthyRecord.Status)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); !errors.Is(err, ErrPersistenceInvariant) {
		t.Fatalf("reconciliation error = %v, want persistence invariant", err)
	}
}

func TestConfirmVersusExpireHasOneValidOutcome(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	expiresAt := time.Now().UTC().Add(time.Minute)
	hold, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard", PassengerIDs: fixture.passengerIDs[:1],
		HoldExpiresAt: expiresAt, IdempotencyKeyHash: hashWithByte(0xe1), RequestFingerprint: hashWithByte(0xe2),
		IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}
	forceReservationsDue(t, fixture.pool, hold.ReservationID)

	start := make(chan struct{})
	confirmResult := make(chan error, 1)
	expireResult := make(chan expiryCallResult, 1)
	go func() {
		<-start
		_, err := store.ConfirmReservation(ctx, ReservationCommandParams{
			UserID: fixture.userID, ReservationID: hold.ReservationID, Now: expiresAt.Add(-time.Second),
			IdempotencyKeyHash: hashWithByte(0xe3), RequestFingerprint: hashWithByte(0xe4),
			IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		})
		confirmResult <- err
	}()
	go func() {
		<-start
		ids, err := store.ExpireDue(ctx, expiresAt.Add(time.Second), 1)
		expireResult <- expiryCallResult{ids: ids, err: err}
	}()
	close(start)
	confirmErr := <-confirmResult
	expiry := <-expireResult
	if expiry.err != nil {
		t.Fatalf("expire race: %v", expiry.err)
	}
	if confirmErr != nil && !errors.Is(confirmErr, ErrInvalidState) && !errors.Is(confirmErr, ErrReservationExpired) {
		t.Fatalf("confirm race error = %v", confirmErr)
	}
	record, err := store.GetReservation(ctx, fixture.userID, hold.ReservationID)
	if err != nil {
		t.Fatalf("get raced reservation: %v", err)
	}
	switch record.Status {
	case "confirmed":
		if confirmErr != nil || len(expiry.ids) != 0 || record.ActiveTicketCount != 1 {
			t.Fatalf("confirmed outcome: confirm=%v expired=%v record=%#v", confirmErr, expiry.ids, record)
		}
	case "expired":
		if (!errors.Is(confirmErr, ErrInvalidState) && !errors.Is(confirmErr, ErrReservationExpired)) || len(expiry.ids) != 1 || record.ActiveTicketCount != 0 {
			t.Fatalf("expired outcome: confirm=%v expired=%v record=%#v", confirmErr, expiry.ids, record)
		}
	default:
		t.Fatalf("invalid raced status %s", record.Status)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("reconcile confirm/expire race: %v", err)
	}
}

func TestCancelVersusConfirmEndsCancelledWithoutInventoryLeak(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	hold, err := store.CreateHold(ctx, CreateHoldParams{
		UserID: fixture.userID, TrainRunID: fixture.trainRunID,
		FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard", PassengerIDs: fixture.passengerIDs[:1],
		HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute), IdempotencyKeyHash: hashWithByte(0xf1),
		RequestFingerprint: hashWithByte(0xf2), IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("create hold: %v", err)
	}
	start := make(chan struct{})
	confirmResult := make(chan error, 1)
	cancelResult := make(chan error, 1)
	go func() {
		<-start
		_, err := store.ConfirmReservation(ctx, ReservationCommandParams{
			UserID: fixture.userID, ReservationID: hold.ReservationID, Now: time.Now().UTC(),
			IdempotencyKeyHash: hashWithByte(0xf3), RequestFingerprint: hashWithByte(0xf4),
			IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		})
		confirmResult <- err
	}()
	go func() {
		<-start
		_, err := store.CancelReservation(ctx, ReservationCommandParams{
			UserID: fixture.userID, ReservationID: hold.ReservationID, Now: time.Now().UTC(),
			IdempotencyKeyHash: hashWithByte(0xf5), RequestFingerprint: hashWithByte(0xf6),
			IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		})
		cancelResult <- err
	}()
	close(start)
	confirmErr := <-confirmResult
	cancelErr := <-cancelResult
	if cancelErr != nil {
		t.Fatalf("cancel race: %v", cancelErr)
	}
	if confirmErr != nil && !errors.Is(confirmErr, ErrInvalidState) {
		t.Fatalf("confirm race: %v", confirmErr)
	}
	record, err := store.GetReservation(ctx, fixture.userID, hold.ReservationID)
	if err != nil {
		t.Fatalf("get raced reservation: %v", err)
	}
	if record.Status != "cancelled" || record.ActiveTicketCount != 0 {
		t.Fatalf("cancel/confirm outcome = %#v", record)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("reconcile cancel/confirm race: %v", err)
	}
}

func TestTrainRunCancellationBecomesAuthoritativeBeforeNewHold(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin operator transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var currentStatus string
	if err := tx.QueryRow(ctx, `SELECT status FROM train_runs WHERE id = $1 FOR UPDATE`, fixture.trainRunID).Scan(&currentStatus); err != nil {
		t.Fatalf("lock train run: %v", err)
	}

	attempting := make(chan struct{})
	holdResult := make(chan error, 1)
	go func() {
		close(attempting)
		_, holdErr := store.CreateHold(ctx, CreateHoldParams{
			UserID: fixture.userID, TrainRunID: fixture.trainRunID,
			FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
			PassengerIDs: fixture.passengerIDs[:1], HoldExpiresAt: time.Now().UTC().Add(10 * time.Minute),
			IdempotencyKeyHash: hashWithByte(0xc7), RequestFingerprint: hashWithByte(0xc8),
			IdempotencyExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		})
		holdResult <- holdErr
	}()
	<-attempting
	if _, err := tx.Exec(ctx, `UPDATE train_runs SET status = 'cancelled' WHERE id = $1`, fixture.trainRunID); err != nil {
		t.Fatalf("cancel train run: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit train-run cancellation: %v", err)
	}
	if err := <-holdResult; !errors.Is(err, ErrNotBookable) {
		t.Fatalf("hold after authoritative cancellation error = %v, want %v", err, ErrNotBookable)
	}
}

type expiryCallResult struct {
	ids []uuid.UUID
	err error
}

func TestPostgreSQLVarbitAllocationPreservesMoreThan64Segments(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := fixture.pool.Exec(ctx, `UPDATE train_runs SET segment_count = 129 WHERE id = $1`, fixture.trainRunID); err != nil {
		t.Fatalf("extend train-run segments: %v", err)
	}
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	requested, err := domain.NewSegmentMask(129, 64, 129)
	if err != nil {
		t.Fatalf("new long segment mask: %v", err)
	}
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin allocation: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.AllocateSeat(ctx, fixture.trainRunID, "standard", requested); err != nil {
		t.Fatalf("allocate long segment mask: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit long segment allocation: %v", err)
	}

	checkTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin mask check: %v", err)
	}
	defer func() { _ = checkTx.Rollback(context.Background()) }()
	masks, err := checkTx.InventoryMasks(ctx, fixture.trainRunID)
	if err != nil {
		t.Fatalf("load long inventory mask: %v", err)
	}
	if len(masks) != 1 || !masks[0].Mask.Equal(requested) {
		t.Fatalf("stored long mask = %#v, want %s", masks, requested.String())
	}
}

func TestAllocateSeatsIsDeterministicAndAllOrNothing(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 2)
	ctx := context.Background()

	inserted, err := store.InitializeInventory(ctx, fixture.trainRunID)
	if err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	if inserted != 2 {
		t.Fatalf("inserted inventory = %d, want 2", inserted)
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })

	requested, err := domain.NewSegmentMask(3, 0, 2)
	if err != nil {
		t.Fatalf("new mask: %v", err)
	}
	_, err = tx.AllocateSeats(ctx, fixture.trainRunID, "standard", requested, 3)
	if !errors.Is(err, ErrInsufficientInventory) {
		t.Fatalf("allocate three seats error = %v, want %v", err, ErrInsufficientInventory)
	}

	masks, err := tx.InventoryMasks(ctx, fixture.trainRunID)
	if err != nil {
		t.Fatalf("load masks after shortfall: %v", err)
	}
	for _, item := range masks {
		if !item.Mask.IsZero() {
			t.Fatalf("seat %s was partially allocated: %s", item.SeatID, item.Mask.String())
		}
	}

	allocated, err := tx.AllocateSeats(ctx, fixture.trainRunID, "standard", requested, 2)
	if err != nil {
		t.Fatalf("allocate two seats: %v", err)
	}
	if len(allocated) != 2 {
		t.Fatalf("allocated seats = %d, want 2", len(allocated))
	}
	if allocated[0].CoachNumber != "1" || allocated[0].SeatNumber != "01A" ||
		allocated[1].CoachNumber != "1" || allocated[1].SeatNumber != "02A" {
		t.Fatalf("allocation order = %#v, want coach/seat order", allocated)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

type integrationFixture struct {
	pool          *pgxpool.Pool
	userID        uuid.UUID
	passengerIDs  []uuid.UUID
	trainRunID    uuid.UUID
	segmentCount  int
	standardFare  int64
	standardClass string
}

func newIntegrationFixture(t *testing.T, seatCount int) (*Store, integrationFixture) {
	t.Helper()
	pool := newIsolatedDatabase(t)
	fixture := seedFixture(t, pool, seatCount)
	fixture.pool = pool
	return New(pool), fixture
}

func newIsolatedDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return newIsolatedDatabaseThrough(t, "")
}

func newIsolatedDatabaseThrough(t *testing.T, lastMigration string) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect admin pool: %v", err)
	}

	random := make([]byte, 12)
	if _, err := rand.Read(random); err != nil {
		admin.Close()
		t.Fatalf("create schema suffix: %v", err)
	}
	schema := "booking_test_" + hex.EncodeToString(random)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatalf("create isolated schema: %v", err)
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		admin.Close()
		t.Fatalf("parse database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
		t.Fatalf("connect isolated pool: %v", err)
	}

	applyMigrationsThrough(t, ctx, pool, lastMigration)
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	return pool
}

func applyMigrationsThrough(t *testing.T, ctx context.Context, pool *pgxpool.Pool, lastMigration string) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "migrations", "*.up.sql"))
	if err != nil {
		t.Fatalf("glob migrations: %v", err)
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		t.Fatal("no migrations found")
	}
	for _, path := range paths {
		if lastMigration != "" && filepath.Base(path) > lastMigration {
			break
		}
		migrationName := filepath.Base(path)
		if migrationName == "000009_physical_shard_control_plane.up.sql" {
			var installed bool
			if err := pool.QueryRow(ctx, `SELECT to_regclass('public.physical_shard_migrations') IS NOT NULL`).Scan(&installed); err != nil {
				t.Fatalf("inspect control-plane migration state: %v", err)
			}
			if installed {
				continue
			}
		}
		if migrationName == "000010_payment_control_plane.up.sql" {
			var installed bool
			if err := pool.QueryRow(ctx, `SELECT to_regclass('public.payment_intents') IS NOT NULL`).Scan(&installed); err != nil {
				t.Fatalf("inspect payment control-plane migration state: %v", err)
			}
			if installed {
				continue
			}
		}
		applyMigrationPath(t, ctx, pool, path)
	}
}

func applyMigrationFile(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string) {
	t.Helper()
	if err := execMigrationFile(ctx, pool, name); err != nil {
		t.Fatalf("apply migration %s: %v", name, err)
	}
}

func execMigrationFile(ctx context.Context, pool *pgxpool.Pool, name string) error {
	path := filepath.Join("..", "..", "..", "migrations", name)
	sql, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read migration %s: %w", path, err)
	}
	if _, err := pool.Exec(ctx, string(sql)); err != nil {
		return fmt.Errorf("apply migration %s: %w", path, err)
	}
	return nil
}

func applyMigrationPath(t *testing.T, ctx context.Context, pool *pgxpool.Pool, path string) {
	t.Helper()
	sql, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration %s: %v", path, err)
	}
	if _, err := pool.Exec(ctx, string(sql)); err != nil {
		t.Fatalf("apply migration %s: %v", path, err)
	}
}

func seedFixture(t *testing.T, pool *pgxpool.Pool, seatCount int) integrationFixture {
	t.Helper()
	ctx := context.Background()
	userID := uuid.New()
	stationIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	routeID, trainID, coachID, trainRunID := uuid.New(), uuid.New(), uuid.New(), uuid.New()

	batch := &pgx.Batch{}
	batch.Queue(`INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)`, userID, userID.String()+"@example.test", "$2a$12$abcdefghijklmnopqrstuv012345678901234567890123456789")
	for index, stationID := range stationIDs {
		batch.Queue(`INSERT INTO stations (id, code, name, timezone) VALUES ($1, $2, $3, 'Asia/Taipei')`, stationID, fmt.Sprintf("S%d", index), fmt.Sprintf("Station %d", index))
	}
	batch.Queue(`INSERT INTO routes (id, code, name, operating_timezone) VALUES ($1, 'R1', 'Test Route', 'Asia/Taipei')`, routeID)
	for index, stationID := range stationIDs {
		batch.Queue(`INSERT INTO route_stops (route_id, station_id, stop_index, arrival_offset_minutes, departure_offset_minutes) VALUES ($1, $2, $3, $4, $4)`, routeID, stationID, index, index*10)
	}
	batch.Queue(`INSERT INTO trains (id, code, name) VALUES ($1, 'T1', 'Test Train')`, trainID)
	batch.Queue(`INSERT INTO coaches (id, train_id, coach_number, seat_class) VALUES ($1, $2, '1', 'standard')`, coachID, trainID)
	for index := 0; index < seatCount; index++ {
		batch.Queue(`INSERT INTO seats (id, coach_id, seat_number) VALUES ($1, $2, $3)`, uuid.New(), coachID, fmt.Sprintf("%02dA", index+1))
	}
	batch.Queue(`INSERT INTO train_runs (id, train_id, route_id, service_date, scheduled_departure_at, segment_count) VALUES ($1, $2, $3, CURRENT_DATE + 1, clock_timestamp() + interval '1 day', 3)`, trainRunID, trainID, routeID)
	batch.Queue(`INSERT INTO fares (train_run_id, from_stop_index, to_stop_index, seat_class, amount_minor, currency) VALUES ($1, 0, 2, 'standard', 1250, 'TWD')`, trainRunID)

	passengerCount := seatCount + 1
	if passengerCount < 12 {
		passengerCount = 12
	}
	passengerIDs := make([]uuid.UUID, passengerCount)
	for index := range passengerIDs {
		passengerIDs[index] = uuid.New()
		batch.Queue(`INSERT INTO passengers (id, user_id, display_name) VALUES ($1, $2, $3)`, passengerIDs[index], userID, fmt.Sprintf("Passenger %d", index+1))
	}

	results := pool.SendBatch(ctx, batch)
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			t.Fatalf("seed fixture: %v", err)
		}
	}
	if err := results.Close(); err != nil {
		t.Fatalf("close seed batch: %v", err)
	}

	return integrationFixture{
		userID:        userID,
		passengerIDs:  passengerIDs,
		trainRunID:    trainRunID,
		segmentCount:  3,
		standardFare:  1250,
		standardClass: "standard",
	}
}

func hashWithByte(value byte) []byte {
	return bytes.Repeat([]byte{value}, 32)
}

func forceReservationsDue(t *testing.T, pool *pgxpool.Pool, ids ...uuid.UUID) {
	t.Helper()
	textIDs := make([]string, len(ids))
	for index, id := range ids {
		textIDs[index] = id.String()
	}
	commandTag, err := pool.Exec(context.Background(), `
UPDATE reservations
SET expires_at = created_at + interval '1 microsecond'
WHERE id = ANY($1::uuid[])`, textIDs)
	if err != nil {
		t.Fatalf("make reservations due: %v", err)
	}
	if commandTag.RowsAffected() != int64(len(ids)) {
		t.Fatalf("made %d reservations due, want %d", commandTag.RowsAffected(), len(ids))
	}
}
