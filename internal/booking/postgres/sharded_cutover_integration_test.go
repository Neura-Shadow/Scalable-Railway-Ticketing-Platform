package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const concurrentBookingCutoverAttempts = 100

func TestConcurrentCreateHoldAcrossCutoverUsesOnlyTargetWriterIntegration(t *testing.T) {
	pool := openShardedBookingIntegrationPool(t)
	fixture := seedShardedBookingCutoverFixture(t, pool)
	ctx := context.Background()

	bootstrapRouter, err := shardingpostgres.NewRouter(pool, nil)
	if err != nil {
		t.Fatalf("create bootstrap router: %v", err)
	}
	bootstrapStore, err := NewShardedWithReservationQuotaLimits(pool, bootstrapRouter, cutoverQuotaLimits())
	if err != nil {
		t.Fatalf("create bootstrap store: %v", err)
	}
	if inserted, err := bootstrapStore.InitializeInventory(ctx, fixture.trainRunID); err != nil || inserted != 1 {
		t.Fatalf("initialize legacy inventory: inserted=%d err=%v", inserted, err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO booking_shard_0.seat_inventory (
    train_run_id, segment_count, seat_id, seat_class, occupied_segments,
    version, created_at, updated_at
)
SELECT train_run_id, segment_count, seat_id, seat_class, occupied_segments,
       version, created_at, updated_at
FROM public.seat_inventory
WHERE train_run_id = $1`, fixture.trainRunID); err != nil {
		t.Fatalf("copy target inventory: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO booking_shard_0.train_run_write_fences (
    train_run_id, assignment_generation, write_enabled
) VALUES ($1, 2, false)`, fixture.trainRunID); err != nil {
		t.Fatalf("seed disabled target fence: %v", err)
	}

	barrier := newBookingCutoverBeginBarrier(pool, concurrentBookingCutoverAttempts)
	defer barrier.Release()
	const replicaCount = 3
	stores := make([]*Store, replicaCount)
	caches := make([]*bookingCutoverRouteCache, replicaCount)
	for replica := range replicaCount {
		cache := &bookingCutoverRouteCache{}
		router, err := shardingpostgres.NewRouter(barrier, cache)
		if err != nil {
			t.Fatalf("create replica %d router: %v", replica+1, err)
		}
		route, err := router.ResolveTrainRun(ctx, fixture.trainRunID)
		if err != nil {
			t.Fatalf("prime replica %d route: %v", replica+1, err)
		}
		if route.ShardID() != sharding.ShardLegacy || route.Generation().Int64() != 1 {
			t.Fatalf("replica %d initial route = %s/%d, want legacy/1", replica+1, route.ShardID(), route.Generation().Int64())
		}
		stores[replica], err = NewShardedWithReservationQuotaLimits(pool, router, cutoverQuotaLimits())
		if err != nil {
			t.Fatalf("create replica %d store: %v", replica+1, err)
		}
		caches[replica] = cache
	}

	type holdResult struct {
		reservationID uuid.UUID
		err           error
	}
	results := make(chan holdResult, concurrentBookingCutoverAttempts)
	start := make(chan struct{})
	requestsCtx, cancelRequests := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	defer func() {
		cancelRequests()
		barrier.Release()
		wait.Wait()
	}()
	expiresAt := time.Now().UTC().Add(10 * time.Minute)
	for attempt := range concurrentBookingCutoverAttempts {
		wait.Add(1)
		go func(attempt int) {
			defer wait.Done()
			<-start
			requestCtx, cancel := context.WithTimeout(requestsCtx, 60*time.Second)
			defer cancel()
			result, err := stores[attempt%replicaCount].CreateHold(requestCtx, CreateHoldParams{
				UserID: fixture.userIDs[attempt], TrainRunID: fixture.trainRunID,
				FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard",
				PassengerIDs:         fixture.passengerIDs[attempt : attempt+1],
				HoldExpiresAt:        expiresAt,
				IdempotencyKeyHash:   bookingCutoverDigest("key", attempt),
				RequestFingerprint:   bookingCutoverDigest("request", attempt),
				IdempotencyExpiresAt: expiresAt.Add(24 * time.Hour),
			})
			results <- holdResult{reservationID: result.ReservationID, err: err}
		}(attempt)
	}
	close(start)

	select {
	case <-barrier.allArrived:
	case <-time.After(20 * time.Second):
		t.Fatalf("only %d of %d booking requests reached the cutover barrier", barrier.arrived.Load(), concurrentBookingCutoverAttempts)
	}
	commitBookingCutover(t, pool, fixture.trainRunID)
	barrier.Release()

	done := make(chan struct{})
	go func() {
		wait.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(75 * time.Second):
		t.Fatal("concurrent booking requests did not finish after cutover")
	}
	close(results)

	var createdReservationID uuid.UUID
	successes, conflicts := 0, 0
	for result := range results {
		switch {
		case result.err == nil:
			successes++
			if createdReservationID != uuid.Nil && createdReservationID != result.reservationID {
				t.Fatalf("successful cutover requests returned reservations %s and %s", createdReservationID, result.reservationID)
			}
			createdReservationID = result.reservationID
		case errors.Is(result.err, ErrInsufficientInventory):
			conflicts++
		default:
			t.Fatalf("concurrent cutover CreateHold error = %v", result.err)
		}
	}
	if successes != 1 || conflicts != concurrentBookingCutoverAttempts-1 || createdReservationID == uuid.Nil {
		t.Fatalf("cutover booking results: successes=%d conflicts=%d reservation=%s", successes, conflicts, createdReservationID)
	}

	for replica, cache := range caches {
		route, found, invalidations := cache.Snapshot()
		expectedInvalidations := concurrentBookingCutoverAttempts / replicaCount
		if replica < concurrentBookingCutoverAttempts%replicaCount {
			expectedInvalidations++
		}
		if !found || route.TrainRunID() != fixture.trainRunID || route.ShardID() != sharding.ShardZero ||
			route.Generation().Int64() != 2 || invalidations != expectedInvalidations {
			t.Fatalf("replica %d cache after cutover: route=%+v found=%t invalidations=%d want=%d", replica+1, route, found, invalidations, expectedInvalidations)
		}
	}
	assertShardedBookingCutoverState(t, pool, fixture.trainRunID, createdReservationID)
	for replica, store := range stores {
		if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
			t.Fatalf("replica %d reconcile target booking after cutover: %v", replica+1, err)
		}
	}
}

type bookingCutoverBeginBarrier struct {
	db          shardingpostgres.DB
	want        int32
	arrived     atomic.Int32
	allArrived  chan struct{}
	release     chan struct{}
	arriveOnce  sync.Once
	releaseOnce sync.Once
}

func newBookingCutoverBeginBarrier(db shardingpostgres.DB, want int) *bookingCutoverBeginBarrier {
	return &bookingCutoverBeginBarrier{
		db: db, want: int32(want), allArrived: make(chan struct{}), release: make(chan struct{}),
	}
}

func (db *bookingCutoverBeginBarrier) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	if db.arrived.Add(1) == db.want {
		db.arriveOnce.Do(func() { close(db.allArrived) })
	}
	select {
	case <-db.release:
		return db.db.BeginTx(ctx, options)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (db *bookingCutoverBeginBarrier) Query(ctx context.Context, sql string, arguments ...any) (pgx.Rows, error) {
	return db.db.Query(ctx, sql, arguments...)
}

func (db *bookingCutoverBeginBarrier) QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row {
	return db.db.QueryRow(ctx, sql, arguments...)
}

func (db *bookingCutoverBeginBarrier) Release() {
	db.releaseOnce.Do(func() { close(db.release) })
}

type bookingCutoverRouteCache struct {
	mu            sync.RWMutex
	route         sharding.ShardRoute
	found         bool
	invalidations int
}

func (cache *bookingCutoverRouteCache) Get(trainRunID uuid.UUID) (sharding.ShardRoute, bool) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cache.route, cache.found && cache.route.TrainRunID() == trainRunID
}

func (cache *bookingCutoverRouteCache) Put(route sharding.ShardRoute) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.route = route
	cache.found = true
	return nil
}

func (cache *bookingCutoverRouteCache) Invalidate(trainRunID uuid.UUID) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.found && cache.route.TrainRunID() == trainRunID {
		cache.found = false
	}
	cache.invalidations++
}

func (cache *bookingCutoverRouteCache) Snapshot() (sharding.ShardRoute, bool, int) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cache.route, cache.found, cache.invalidations
}

type shardedBookingCutoverFixture struct {
	userIDs      []uuid.UUID
	passengerIDs []uuid.UUID
	trainRunID   uuid.UUID
	trainID      uuid.UUID
	coachID      uuid.UUID
	seatID       uuid.UUID
	routeID      uuid.UUID
	stationIDs   []uuid.UUID
}

func openShardedBookingIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse PostgreSQL integration configuration: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = "public"
	if config.MaxConns < 24 {
		config.MaxConns = 24
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("open sharded booking integration pool: %v", err)
	}
	t.Cleanup(pool.Close)
	var ready bool
	if err := pool.QueryRow(context.Background(), `
SELECT to_regclass('public.train_run_shard_assignments') IS NOT NULL
   AND to_regclass('booking_shard_0.reservations') IS NOT NULL`).Scan(&ready); err != nil || !ready {
		t.Fatalf("Milestone 4 schema is not ready: ready=%t err=%v", ready, err)
	}
	return pool
}

func seedShardedBookingCutoverFixture(t *testing.T, pool *pgxpool.Pool) shardedBookingCutoverFixture {
	t.Helper()
	fixture := shardedBookingCutoverFixture{
		userIDs:    make([]uuid.UUID, concurrentBookingCutoverAttempts),
		trainRunID: uuid.New(), trainID: uuid.New(), coachID: uuid.New(),
		seatID: uuid.New(), routeID: uuid.New(), stationIDs: make([]uuid.UUID, 4),
		passengerIDs: make([]uuid.UUID, concurrentBookingCutoverAttempts),
	}
	for index := range fixture.stationIDs {
		fixture.stationIDs[index] = uuid.New()
	}
	for index := range fixture.passengerIDs {
		fixture.userIDs[index] = uuid.New()
		fixture.passengerIDs[index] = uuid.New()
	}
	t.Cleanup(func() { cleanupShardedBookingCutoverFixture(t, pool, fixture) })

	suffix := strings.ToUpper(strings.ReplaceAll(fixture.trainRunID.String(), "-", ""))[:8]
	tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin sharded booking fixture seed: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	for index, stationID := range fixture.stationIDs {
		mustCutoverSeedExec(t, tx, `
INSERT INTO public.stations (id, code, name, timezone)
VALUES ($1, $2, $3, 'UTC')`, stationID, fmt.Sprintf("M4C%s%d", suffix, index), fmt.Sprintf("M4 cutover station %d", index))
	}
	mustCutoverSeedExec(t, tx, `
INSERT INTO public.routes (id, code, name, operating_timezone)
VALUES ($1, $2, 'Milestone 4 booking cutover route', 'UTC')`, fixture.routeID, "M4R"+suffix)
	for index, stationID := range fixture.stationIDs {
		mustCutoverSeedExec(t, tx, `
INSERT INTO public.route_stops (
    route_id, station_id, stop_index, arrival_offset_minutes, departure_offset_minutes
) VALUES ($1, $2, $3, $4, $4)`, fixture.routeID, stationID, index, index*10)
	}
	mustCutoverSeedExec(t, tx, `
INSERT INTO public.trains (id, code, name)
VALUES ($1, $2, 'Milestone 4 booking cutover train')`, fixture.trainID, "M4T"+suffix)
	mustCutoverSeedExec(t, tx, `
INSERT INTO public.coaches (id, train_id, coach_number, seat_class)
VALUES ($1, $2, '1', 'standard')`, fixture.coachID, fixture.trainID)
	mustCutoverSeedExec(t, tx, `
INSERT INTO public.seats (id, coach_id, seat_number)
VALUES ($1, $2, '01A')`, fixture.seatID, fixture.coachID)
	mustCutoverSeedExec(t, tx, `
INSERT INTO public.train_runs (
    id, train_id, route_id, service_date, scheduled_departure_at, segment_count
) VALUES ($1, $2, $3, CURRENT_DATE + 365, clock_timestamp() + interval '365 days', 3)`,
		fixture.trainRunID, fixture.trainID, fixture.routeID)
	mustCutoverSeedExec(t, tx, `
INSERT INTO public.fares (
    train_run_id, from_stop_index, to_stop_index, seat_class, amount_minor, currency
) VALUES ($1, 0, 2, 'standard', 1250, 'TWD')`, fixture.trainRunID)
	for index, passengerID := range fixture.passengerIDs {
		mustCutoverSeedExec(t, tx, `
INSERT INTO public.users (id, email, password_hash)
VALUES ($1, $2, $3)`, fixture.userIDs[index], fmt.Sprintf("%s-%03d@cutover.example.test", strings.ToLower(suffix), index), "$2a$12$abcdefghijklmnopqrstuv012345678901234567890123456789")
		mustCutoverSeedExec(t, tx, `
INSERT INTO public.passengers (id, user_id, display_name)
VALUES ($1, $2, $3)`, passengerID, fixture.userIDs[index], fmt.Sprintf("Cutover passenger %d", index+1))
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit sharded booking fixture seed: %v", err)
	}
	return fixture
}

func mustCutoverSeedExec(t *testing.T, tx pgx.Tx, sql string, arguments ...any) {
	t.Helper()
	if _, err := tx.Exec(context.Background(), sql, arguments...); err != nil {
		t.Fatalf("seed sharded booking cutover fixture: %v", err)
	}
}

func cleanupShardedBookingCutoverFixture(t *testing.T, pool *pgxpool.Pool, fixture shardedBookingCutoverFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Errorf("begin sharded booking fixture cleanup: %v", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	execCleanup := func(label, statement string, arguments ...any) bool {
		if _, err := tx.Exec(ctx, statement, arguments...); err != nil {
			t.Errorf("clean sharded booking fixture %s: %v", label, err)
			return false
		}
		return true
	}
	execRunScoped := func(label, statement string) bool {
		return execCleanup(label, statement, fixture.trainRunID)
	}
	for index, statement := range []string{
		`DELETE FROM public.outbox_events WHERE train_run_id = $1`,
		`DELETE FROM public.reservation_quota_claims WHERE train_run_id = $1`,
		`DELETE FROM public.booking_idempotency_key_claims WHERE train_run_id = $1`,
		`DELETE FROM public.reservation_shard_locators WHERE train_run_id = $1`,
		`DELETE FROM public.ticket_order_shard_locators WHERE train_run_id = $1`,
		`DELETE FROM public.ticket_shard_locators WHERE train_run_id = $1`,
		`DELETE FROM public.train_run_generation_writes WHERE train_run_id = $1`,
	} {
		if !execRunScoped(fmt.Sprintf("global dependency %d", index+1), statement) {
			return
		}
	}

	var currentGeneration int64
	assignmentErr := tx.QueryRow(ctx, `
SELECT assignment_generation
FROM public.train_run_shard_assignments
WHERE train_run_id = $1`, fixture.trainRunID).Scan(&currentGeneration)
	if assignmentErr != nil && !errors.Is(assignmentErr, pgx.ErrNoRows) {
		t.Errorf("read sharded booking fixture cleanup authority: %v", assignmentErr)
		return
	}
	if assignmentErr == nil {
		targetGeneration := currentGeneration + 1
		if !execRunScoped("disable legacy writer", `UPDATE public.train_run_write_fences SET write_enabled = false WHERE train_run_id = $1`) ||
			!execRunScoped("disable shard-1 writer", `UPDATE booking_shard_1.train_run_write_fences SET write_enabled = false WHERE train_run_id = $1`) ||
			!execCleanup("enable target writer", `
INSERT INTO booking_shard_0.train_run_write_fences (
    train_run_id, assignment_generation, write_enabled
) VALUES ($1, $2, true)
ON CONFLICT (train_run_id) DO UPDATE
SET assignment_generation = EXCLUDED.assignment_generation,
    write_enabled = true`, fixture.trainRunID, targetGeneration) ||
			!execCleanup("move authority to target", `
UPDATE public.train_run_shard_assignments
SET shard_id = 'shard-0', assignment_generation = $2,
    assignment_state = 'stable', active_migration_id = NULL,
    availability_generation = availability_generation + 1
WHERE train_run_id = $1`, fixture.trainRunID, targetGeneration) {
			return
		}
		for index, statement := range []string{
			`DELETE FROM booking_shard_0.reservation_seats WHERE train_run_id = $1`,
			`DELETE FROM booking_shard_0.reservations WHERE train_run_id = $1`,
			`DELETE FROM booking_shard_0.idempotency_records WHERE train_run_id = $1`,
			`DELETE FROM booking_shard_0.seat_inventory WHERE train_run_id = $1`,
			`DELETE FROM booking_shard_1.reservation_seats WHERE train_run_id = $1`,
			`DELETE FROM booking_shard_1.reservations WHERE train_run_id = $1`,
			`DELETE FROM booking_shard_1.idempotency_records WHERE train_run_id = $1`,
			`DELETE FROM booking_shard_1.seat_inventory WHERE train_run_id = $1`,
		} {
			if !execRunScoped(fmt.Sprintf("target row set %d", index+1), statement) {
				return
			}
		}

		legacyGeneration := targetGeneration + 1
		if !execRunScoped("disable target writer", `UPDATE booking_shard_0.train_run_write_fences SET write_enabled = false WHERE train_run_id = $1`) ||
			!execRunScoped("keep shard-1 writer disabled", `UPDATE booking_shard_1.train_run_write_fences SET write_enabled = false WHERE train_run_id = $1`) ||
			!execCleanup("enable legacy writer", `
INSERT INTO public.train_run_write_fences (
    train_run_id, assignment_generation, write_enabled
) VALUES ($1, $2, true)
ON CONFLICT (train_run_id) DO UPDATE
SET assignment_generation = EXCLUDED.assignment_generation,
    write_enabled = true`, fixture.trainRunID, legacyGeneration) ||
			!execCleanup("restore authority to legacy", `
UPDATE public.train_run_shard_assignments
SET shard_id = 'legacy', assignment_generation = $2,
    assignment_state = 'stable', active_migration_id = NULL,
    availability_generation = availability_generation + 1
WHERE train_run_id = $1`, fixture.trainRunID, legacyGeneration) {
			return
		}
		for index, statement := range []string{
			`DELETE FROM public.reservation_seats WHERE train_run_id = $1`,
			`DELETE FROM public.reservations WHERE train_run_id = $1`,
			`DELETE FROM public.idempotency_records WHERE train_run_id = $1`,
			`DELETE FROM public.seat_inventory WHERE train_run_id = $1`,
		} {
			if !execRunScoped(fmt.Sprintf("legacy row set %d", index+1), statement) {
				return
			}
		}
	}
	for index, statement := range []string{
		`DELETE FROM public.fares WHERE train_run_id = $1`,
		`DELETE FROM public.train_runs WHERE id = $1`,
	} {
		if !execRunScoped(fmt.Sprintf("train-run identity %d", index+1), statement) {
			return
		}
	}
	for _, item := range []struct {
		sql string
		id  uuid.UUID
	}{
		{`DELETE FROM public.seats WHERE id = $1`, fixture.seatID},
		{`DELETE FROM public.coaches WHERE id = $1`, fixture.coachID},
		{`DELETE FROM public.trains WHERE id = $1`, fixture.trainID},
		{`DELETE FROM public.route_stops WHERE route_id = $1`, fixture.routeID},
		{`DELETE FROM public.routes WHERE id = $1`, fixture.routeID},
	} {
		if _, err := tx.Exec(ctx, item.sql, item.id); err != nil {
			t.Errorf("clean sharded booking fixture identity: %v", err)
			return
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM public.passengers WHERE id = ANY($1::uuid[])`, fixture.passengerIDs); err != nil {
		t.Errorf("clean sharded booking fixture passengers: %v", err)
		return
	}
	if _, err := tx.Exec(ctx, `DELETE FROM public.users WHERE id = ANY($1::uuid[])`, fixture.userIDs); err != nil {
		t.Errorf("clean sharded booking fixture users: %v", err)
		return
	}
	if _, err := tx.Exec(ctx, `DELETE FROM public.stations WHERE id = ANY($1::uuid[])`, fixture.stationIDs); err != nil {
		t.Errorf("clean sharded booking fixture stations: %v", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		t.Errorf("commit sharded booking fixture cleanup: %v", err)
	}
}

func cutoverQuotaLimits() ReservationQuotaLimits {
	return ReservationQuotaLimits{
		MaxActiveHoldsPerUser:            200,
		MaxActiveHoldsPerUserPerTrainRun: 200,
		MaxActivePassengersPerUser:       200,
	}
}

func bookingCutoverDigest(kind string, attempt int) []byte {
	digest := sha256.Sum256([]byte(fmt.Sprintf("milestone-4-cutover-%s-%d", kind, attempt)))
	return append([]byte(nil), digest[:]...)
}

func commitBookingCutover(t *testing.T, pool *pgxpool.Pool, trainRunID uuid.UUID) {
	t.Helper()
	tx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin booking cutover: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(context.Background(), `
UPDATE public.train_run_write_fences
SET write_enabled = false
WHERE train_run_id = $1 AND assignment_generation = 1`, trainRunID); err != nil {
		t.Fatalf("disable legacy writer: %v", err)
	}
	if _, err := tx.Exec(context.Background(), `
UPDATE booking_shard_0.train_run_write_fences
SET write_enabled = true
WHERE train_run_id = $1 AND assignment_generation = 2`, trainRunID); err != nil {
		t.Fatalf("enable target writer: %v", err)
	}
	if _, err := tx.Exec(context.Background(), `
UPDATE public.train_run_shard_assignments
SET shard_id = 'shard-0',
    assignment_generation = 2,
    assignment_state = 'stable',
    availability_generation = availability_generation + 1
WHERE train_run_id = $1`, trainRunID); err != nil {
		t.Fatalf("move booking assignment: %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit booking cutover: %v", err)
	}
}

func assertShardedBookingCutoverState(t *testing.T, pool *pgxpool.Pool, trainRunID, reservationID uuid.UUID) {
	t.Helper()
	var sourceReservations, sourceReservationSeats, targetReservations, distinctTargetReservations, targetReservationSeats int
	var shardOneReservations, shardOneReservationSeats, allReservations, distinctAllReservations int
	if err := pool.QueryRow(context.Background(), `
SELECT (SELECT count(*)::integer FROM public.reservations WHERE train_run_id = $1),
       (SELECT count(*)::integer FROM public.reservation_seats WHERE train_run_id = $1),
       (SELECT count(*)::integer FROM booking_shard_0.reservations WHERE train_run_id = $1),
       (SELECT count(DISTINCT id)::integer FROM booking_shard_0.reservations WHERE train_run_id = $1),
       (SELECT count(*)::integer FROM booking_shard_0.reservation_seats WHERE train_run_id = $1),
       (SELECT count(*)::integer FROM booking_shard_1.reservations WHERE train_run_id = $1),
       (SELECT count(*)::integer FROM booking_shard_1.reservation_seats WHERE train_run_id = $1),
       (SELECT count(*)::integer FROM (
            SELECT id FROM public.reservations WHERE train_run_id = $1
            UNION ALL SELECT id FROM booking_shard_0.reservations WHERE train_run_id = $1
            UNION ALL SELECT id FROM booking_shard_1.reservations WHERE train_run_id = $1
        ) AS all_reservations),
       (SELECT count(DISTINCT id)::integer FROM (
            SELECT id FROM public.reservations WHERE train_run_id = $1
            UNION ALL SELECT id FROM booking_shard_0.reservations WHERE train_run_id = $1
            UNION ALL SELECT id FROM booking_shard_1.reservations WHERE train_run_id = $1
        ) AS all_reservations)`, trainRunID).Scan(
		&sourceReservations, &sourceReservationSeats, &targetReservations,
		&distinctTargetReservations, &targetReservationSeats, &shardOneReservations,
		&shardOneReservationSeats, &allReservations, &distinctAllReservations,
	); err != nil {
		t.Fatalf("read cutover booking row counts: %v", err)
	}
	if sourceReservations != 0 || sourceReservationSeats != 0 || targetReservations != 1 ||
		distinctTargetReservations != 1 || targetReservationSeats != 1 || shardOneReservations != 0 ||
		shardOneReservationSeats != 0 || allReservations != 1 || distinctAllReservations != 1 {
		t.Fatalf("cutover booking rows: legacy=%d/%d shard-0=%d/%d/%d shard-1=%d/%d all/distinct=%d/%d",
			sourceReservations, sourceReservationSeats, targetReservations, distinctTargetReservations,
			targetReservationSeats, shardOneReservations, shardOneReservationSeats, allReservations, distinctAllReservations)
	}

	var storedReservationID uuid.UUID
	if err := pool.QueryRow(context.Background(), `
SELECT id FROM booking_shard_0.reservations WHERE train_run_id = $1`, trainRunID).Scan(&storedReservationID); err != nil {
		t.Fatalf("read target reservation identity: %v", err)
	}
	if storedReservationID != reservationID {
		t.Fatalf("target reservation = %s, want successful result %s", storedReservationID, reservationID)
	}

	var sourceMask, targetMask string
	var sourceVersion, targetVersion int64
	if err := pool.QueryRow(context.Background(), `
SELECT source.occupied_segments::text, source.version,
       target.occupied_segments::text, target.version
FROM public.seat_inventory AS source
JOIN booking_shard_0.seat_inventory AS target
  ON target.train_run_id = source.train_run_id
 AND target.seat_id = source.seat_id
WHERE source.train_run_id = $1`, trainRunID).Scan(&sourceMask, &sourceVersion, &targetMask, &targetVersion); err != nil {
		t.Fatalf("read source and target inventory masks: %v", err)
	}
	if sourceMask != "000" || sourceVersion != 0 || targetMask != "110" || targetVersion != 1 {
		t.Fatalf("cutover masks: source=%s/v%d target=%s/v%d, want 000/v0 and 110/v1", sourceMask, sourceVersion, targetMask, targetVersion)
	}

	var overlappingPairs int
	if err := pool.QueryRow(context.Background(), `
WITH allocations AS (
    SELECT 0 AS storage_order, rs.id, rs.train_run_id, rs.seat_id, rs.segment_mask
    FROM public.reservation_seats AS rs
    JOIN public.reservations AS r ON r.id = rs.reservation_id
    WHERE rs.train_run_id = $1 AND r.status IN ('held', 'confirmed')
    UNION ALL
    SELECT 1, rs.id, rs.train_run_id, rs.seat_id, rs.segment_mask
    FROM booking_shard_0.reservation_seats AS rs
    JOIN booking_shard_0.reservations AS r ON r.id = rs.reservation_id
    WHERE rs.train_run_id = $1 AND r.status IN ('held', 'confirmed')
    UNION ALL
    SELECT 2, rs.id, rs.train_run_id, rs.seat_id, rs.segment_mask
    FROM booking_shard_1.reservation_seats AS rs
    JOIN booking_shard_1.reservations AS r ON r.id = rs.reservation_id
    WHERE rs.train_run_id = $1 AND r.status IN ('held', 'confirmed')
)
SELECT count(*)::integer
FROM allocations AS left_seat
JOIN allocations AS right_seat
  ON ROW(left_seat.storage_order, left_seat.id) < ROW(right_seat.storage_order, right_seat.id)
 AND left_seat.train_run_id = right_seat.train_run_id
 AND left_seat.seat_id = right_seat.seat_id
WHERE CASE
    WHEN bit_length(left_seat.segment_mask) = bit_length(right_seat.segment_mask)
    THEN bit_count(left_seat.segment_mask & right_seat.segment_mask) > 0
    ELSE true
END`, trainRunID).Scan(&overlappingPairs); err != nil {
		t.Fatalf("check target seat overlaps: %v", err)
	}
	if overlappingPairs != 0 {
		t.Fatalf("cross-shard overlapping seat allocations = %d, want 0", overlappingPairs)
	}

	var locatorShard string
	var locatorGeneration int64
	if err := pool.QueryRow(context.Background(), `
SELECT shard_id, assignment_generation
FROM public.reservation_shard_locators
WHERE reservation_id = $1 AND train_run_id = $2`, reservationID, trainRunID).Scan(&locatorShard, &locatorGeneration); err != nil {
		t.Fatalf("read reservation locator: %v", err)
	}
	if locatorShard != "shard-0" || locatorGeneration != 2 {
		t.Fatalf("reservation locator = %s/%d, want shard-0/2", locatorShard, locatorGeneration)
	}

	var sourceIdempotency, targetIdempotency, globalClaims, targetOutbox int
	if err := pool.QueryRow(context.Background(), `
SELECT (SELECT count(*)::integer FROM public.idempotency_records WHERE train_run_id = $1),
       (SELECT count(*)::integer FROM booking_shard_0.idempotency_records WHERE train_run_id = $1),
       (SELECT count(*)::integer FROM public.booking_idempotency_key_claims WHERE train_run_id = $1),
       (SELECT count(*)::integer FROM public.outbox_events
        WHERE train_run_id = $1 AND aggregate_id = $2 AND event_type = 'reservation.held'
          AND shard_id = 'shard-0' AND assignment_generation = 2)`, trainRunID, reservationID).Scan(
		&sourceIdempotency, &targetIdempotency, &globalClaims, &targetOutbox,
	); err != nil {
		t.Fatalf("read cutover idempotency and outbox state: %v", err)
	}
	if sourceIdempotency != 0 || targetIdempotency != 1 || globalClaims != 1 || targetOutbox != 1 {
		t.Fatalf("cutover idempotency/outbox: source=%d target=%d claims=%d outbox=%d", sourceIdempotency, targetIdempotency, globalClaims, targetOutbox)
	}

	var assignedShard string
	var assignedGeneration int64
	var enabledWriters int
	if err := pool.QueryRow(context.Background(), `
SELECT assignment.shard_id,
       assignment.assignment_generation,
       (
           SELECT count(*)::integer
           FROM (
               SELECT write_enabled FROM public.train_run_write_fences WHERE train_run_id = $1
               UNION ALL
               SELECT write_enabled FROM booking_shard_0.train_run_write_fences WHERE train_run_id = $1
               UNION ALL
               SELECT write_enabled FROM booking_shard_1.train_run_write_fences WHERE train_run_id = $1
           ) AS fences
           WHERE write_enabled
       )
FROM public.train_run_shard_assignments AS assignment
WHERE assignment.train_run_id = $1`, trainRunID).Scan(&assignedShard, &assignedGeneration, &enabledWriters); err != nil {
		t.Fatalf("read final cutover authority: %v", err)
	}
	if assignedShard != "shard-0" || assignedGeneration != 2 || enabledWriters != 1 {
		t.Fatalf("final cutover authority = %s/%d with %d enabled writers", assignedShard, assignedGeneration, enabledWriters)
	}
}
