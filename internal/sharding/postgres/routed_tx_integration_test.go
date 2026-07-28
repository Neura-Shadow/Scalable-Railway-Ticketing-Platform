package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const concurrentCutoverWriteAttempts = 100

func TestFixedSearchPathIsTransactionLocalIntegration(t *testing.T) {
	pool := openRoutingIntegrationPool(t)

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal("acquire PostgreSQL integration connection")
	}
	defer conn.Release()
	baseline := currentSearchPath(t, conn)

	t.Run("rollback", func(t *testing.T) {
		tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{})
		if err != nil {
			t.Fatal("begin rollback transaction")
		}
		assertTransactionPath(t, tx, sharding.ShardZero, "pg_catalog, booking_shard_0, public, pg_temp")
		if err := tx.Rollback(context.Background()); err != nil {
			t.Fatal("rollback routed transaction")
		}
		if got := currentSearchPath(t, conn); got != baseline {
			t.Fatalf("search_path after rollback = %q, want baseline %q", got, baseline)
		}
	})

	t.Run("commit", func(t *testing.T) {
		tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{})
		if err != nil {
			t.Fatal("begin commit transaction")
		}
		assertTransactionPath(t, tx, sharding.ShardOne, "pg_catalog, booking_shard_1, public, pg_temp")
		if err := tx.Commit(context.Background()); err != nil {
			t.Fatal("commit routed transaction")
		}
		if got := currentSearchPath(t, conn); got != baseline {
			t.Fatalf("search_path after commit = %q, want baseline %q", got, baseline)
		}
	})

	t.Run("canceled request cleanup", func(t *testing.T) {
		tx, err := conn.BeginTx(context.Background(), pgx.TxOptions{})
		if err != nil {
			t.Fatal("begin cancellation transaction")
		}
		assertTransactionPath(t, tx, sharding.ShardZero, "pg_catalog, booking_shard_0, public, pg_temp")
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := tx.Exec(canceled, "SELECT 1"); err == nil {
			t.Fatal("canceled transaction command unexpectedly succeeded")
		}
		if err := tx.Rollback(context.Background()); err != nil {
			t.Fatal("rollback canceled routed transaction")
		}
		if got := currentSearchPath(t, conn); got != baseline {
			t.Fatalf("search_path after canceled request = %q, want baseline %q", got, baseline)
		}
	})
}

func TestFixedSearchPathIsolatedAcrossConcurrentTransactionsIntegration(t *testing.T) {
	pool := openRoutingIntegrationPool(t)
	first, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal("acquire first PostgreSQL integration connection")
	}
	defer first.Release()
	second, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal("acquire second PostgreSQL integration connection")
	}
	defer second.Release()
	firstBaseline := currentSearchPath(t, first)
	secondBaseline := currentSearchPath(t, second)

	firstTx, err := first.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Fatal("begin first concurrent transaction")
	}
	defer func() { _ = firstTx.Rollback(context.Background()) }()
	secondTx, err := second.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Fatal("begin second concurrent transaction")
	}
	defer func() { _ = secondTx.Rollback(context.Background()) }()

	assertTransactionPath(t, firstTx, sharding.ShardZero, "pg_catalog, booking_shard_0, public, pg_temp")
	assertTransactionPath(t, secondTx, sharding.ShardOne, "pg_catalog, booking_shard_1, public, pg_temp")
	if got := currentSearchPath(t, firstTx); got != "pg_catalog, booking_shard_0, public, pg_temp" {
		t.Fatalf("first concurrent transaction search_path = %q", got)
	}
	if got := currentSearchPath(t, secondTx); got != "pg_catalog, booking_shard_1, public, pg_temp" {
		t.Fatalf("second concurrent transaction search_path = %q", got)
	}
	if err := firstTx.Rollback(context.Background()); err != nil {
		t.Fatal("rollback first concurrent transaction")
	}
	if err := secondTx.Rollback(context.Background()); err != nil {
		t.Fatal("rollback second concurrent transaction")
	}
	if got := currentSearchPath(t, first); got != firstBaseline {
		t.Fatalf("first connection search_path after rollback = %q, want %q", got, firstBaseline)
	}
	if got := currentSearchPath(t, second); got != secondBaseline {
		t.Fatalf("second connection search_path after rollback = %q, want %q", got, secondBaseline)
	}
}

func TestCutoverRejectsOneHundredConcurrentStaleWritesAndRefreshesEveryReplicaIntegration(t *testing.T) {
	pool := openRoutingIntegrationPool(t)
	trainRunID := seedRoutingIntegrationTrainRun(t, pool)
	staleRoute := mustRoute(t, trainRunID, sharding.ShardLegacy, 1)

	barrier := newBeginBarrierDB(pool, concurrentCutoverWriteAttempts)
	defer barrier.Release()
	const replicaCount = 3
	replicaRouters := make([]*Router, replicaCount)
	replicaCaches := make([]*fakeRouteCache, replicaCount)
	replicaRoutes := make([]sharding.ShardRoute, replicaCount)
	for replica := range replicaCount {
		cache := &fakeRouteCache{route: staleRoute, found: true}
		router, err := NewRouter(barrier, cache)
		if err != nil {
			t.Fatalf("replica %d router: %v", replica+1, err)
		}
		cached, err := router.ResolveTrainRun(context.Background(), trainRunID)
		if err != nil || cached != staleRoute {
			t.Fatalf("replica %d did not start from the stale cached route: route=%+v err=%v", replica+1, cached, err)
		}
		replicaRouters[replica] = router
		replicaCaches[replica] = cache
		replicaRoutes[replica] = cached
	}

	cutover, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Fatal("begin cutover transaction")
	}
	defer func() { _ = cutover.Rollback(context.Background()) }()
	if _, err := cutover.Exec(context.Background(), `
UPDATE public.train_run_write_fences
SET write_enabled = false
WHERE train_run_id = $1`, trainRunID); err != nil {
		t.Fatalf("fence legacy writer: %v", err)
	}
	if _, err := cutover.Exec(context.Background(), `
INSERT INTO booking_shard_0.train_run_write_fences (
    train_run_id, assignment_generation, write_enabled
) VALUES ($1, 2, true)`, trainRunID); err != nil {
		t.Fatalf("enable target writer: %v", err)
	}
	if _, err := cutover.Exec(context.Background(), `
UPDATE public.train_run_shard_assignments
SET shard_id = 'shard-0',
    assignment_generation = 2,
    assignment_state = 'stable',
    availability_generation = availability_generation + 1
WHERE train_run_id = $1`, trainRunID); err != nil {
		t.Fatalf("move train-run assignment: %v", err)
	}

	type staleWriteResult struct {
		replica int
		err     error
	}
	results := make(chan staleWriteResult, concurrentCutoverWriteAttempts)
	wantStaleByReplica := [replicaCount]int{}
	for attempt := range concurrentCutoverWriteAttempts {
		replica := attempt % replicaCount
		wantStaleByReplica[replica]++
		go func(replica int) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			tx, err := replicaRouters[replica].BeginTrainRunWrite(ctx, replicaRoutes[replica])
			if tx != nil {
				_ = tx.Rollback(context.Background())
			}
			results <- staleWriteResult{replica: replica, err: err}
		}(replica)
	}

	select {
	case <-barrier.allArrived:
	case <-time.After(10 * time.Second):
		t.Fatalf("only %d of %d stale writes reached the deterministic cutover barrier", barrier.arrived.Load(), concurrentCutoverWriteAttempts)
	}
	if err := cutover.Commit(context.Background()); err != nil {
		t.Fatalf("commit cutover transaction: %v", err)
	}
	barrier.Release()

	gotStaleByReplica := [replicaCount]int{}
	for attempt := 0; attempt < concurrentCutoverWriteAttempts; attempt++ {
		result := <-results
		if !errors.Is(result.err, sharding.ErrAssignmentStale) {
			t.Fatalf("replica %d stale write %d error = %v, want %v", result.replica+1, attempt+1, result.err, sharding.ErrAssignmentStale)
		}
		gotStaleByReplica[result.replica]++
	}
	for replica := range replicaCount {
		if gotStaleByReplica[replica] != wantStaleByReplica[replica] {
			t.Fatalf("replica %d stale rejections = %d, want %d", replica+1, gotStaleByReplica[replica], wantStaleByReplica[replica])
		}

		// This is intentionally routing/fencing evidence. The accepted target
		// transaction below does not claim reservation or seat-mutation coverage.
		tx, err := replicaRouters[replica].BeginTrainRunWriteWithRefresh(
			context.Background(), replicaRoutes[replica],
		)
		if err != nil {
			t.Fatalf("replica %d did not refresh onto the target writer: %v", replica+1, err)
		}
		if tx.Route().ShardID() != sharding.ShardZero || tx.Route().Generation().Int64() != 2 {
			_ = tx.Rollback(context.Background())
			t.Fatalf("replica %d refreshed route = %s/%d, want shard-0/2", replica+1, tx.Route().ShardID(), tx.Route().Generation().Int64())
		}
		cache := replicaCaches[replica]
		if cache.invalidated != trainRunID || !cache.found || cache.route != tx.Route() {
			_ = tx.Rollback(context.Background())
			t.Fatalf("replica %d cache did not refresh to target route: %+v", replica+1, cache)
		}
		var targetWriteEnabled bool
		if err := tx.PGXTx().QueryRow(context.Background(), `
UPDATE train_run_write_fences
SET write_enabled = write_enabled
WHERE train_run_id = $1 AND assignment_generation = 2
RETURNING write_enabled`, trainRunID).Scan(&targetWriteEnabled); err != nil || !targetWriteEnabled {
			_ = tx.Rollback(context.Background())
			t.Fatalf("replica %d target transaction was not accepted: enabled=%t err=%v", replica+1, targetWriteEnabled, err)
		}
		if err := tx.Commit(context.Background()); err != nil {
			t.Fatalf("replica %d commit target transaction: %v", replica+1, err)
		}
	}

	assertExactlyOneCutoverWriter(t, pool, trainRunID)
}

func TestAssignmentLockCancellationPreservesAuthorityAndRetrySucceedsIntegration(t *testing.T) {
	pool := openRoutingIntegrationPool(t)
	trainRunID := seedRoutingIntegrationTrainRun(t, pool)
	route := mustRoute(t, trainRunID, sharding.ShardLegacy, 1)
	wantAuthority := readRoutingAuthority(t, pool, trainRunID)

	blocker, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Fatal("begin assignment-lock blocker")
	}
	blockerOpen := true
	defer func() {
		if blockerOpen {
			_ = blocker.Rollback(context.Background())
		}
	}()
	var blockerPID int32
	if err := blocker.QueryRow(context.Background(), "SELECT pg_backend_pid()").Scan(&blockerPID); err != nil {
		t.Fatal("read assignment-lock blocker backend")
	}
	if err := blocker.QueryRow(context.Background(), `
SELECT train_run_id
FROM public.train_run_shard_assignments
WHERE train_run_id = $1
FOR UPDATE`, trainRunID).Scan(new(uuid.UUID)); err != nil {
		t.Fatalf("lock assignment authority: %v", err)
	}

	router, err := NewRouter(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	requestContext, cancel := context.WithCancel(context.Background())
	requestDone := make(chan error, 1)
	go func() {
		transaction, beginErr := router.BeginTrainRunWrite(requestContext, route)
		if transaction != nil {
			_ = transaction.Rollback(context.Background())
		}
		requestDone <- beginErr
	}()

	waitUntilAssignmentQueryBlocked(t, pool, blockerPID)
	cancel()
	select {
	case err := <-requestDone:
		if !errors.Is(err, sharding.ErrShardUnavailable) {
			t.Fatalf("canceled assignment-lock wait error = %v, want %v", err, sharding.ErrShardUnavailable)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("canceled assignment-lock wait did not return")
	}
	if got := readRoutingAuthority(t, pool, trainRunID); got != wantAuthority {
		t.Fatalf("authority changed after cancellation: got=%+v want=%+v", got, wantAuthority)
	}

	if err := blocker.Rollback(context.Background()); err != nil {
		t.Fatalf("release assignment-lock blocker: %v", err)
	}
	blockerOpen = false
	retry, err := router.BeginTrainRunWrite(context.Background(), route)
	if err != nil {
		t.Fatalf("retry after assignment-lock cancellation: %v", err)
	}
	if retry.Route() != route {
		_ = retry.Rollback(context.Background())
		t.Fatalf("retry route = %+v, want unchanged legacy generation 1", retry.Route())
	}
	if err := retry.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback retry transaction: %v", err)
	}
	if got := readRoutingAuthority(t, pool, trainRunID); got != wantAuthority {
		t.Fatalf("authority changed after safe retry: got=%+v want=%+v", got, wantAuthority)
	}
}

type routingAuthority struct {
	shardID             string
	generation          int64
	assignmentState     string
	hasActiveMigration  bool
	sourceGeneration    int64
	sourceWriteEnabled  bool
	targetZeroFenceRows int
}

func readRoutingAuthority(t *testing.T, pool *pgxpool.Pool, trainRunID uuid.UUID) routingAuthority {
	t.Helper()
	var authority routingAuthority
	if err := pool.QueryRow(context.Background(), `
SELECT assignment.shard_id,
       assignment.assignment_generation,
       assignment.assignment_state,
       assignment.active_migration_id IS NOT NULL,
       source.assignment_generation,
       source.write_enabled,
       (SELECT count(*)::integer
        FROM booking_shard_0.train_run_write_fences AS target
        WHERE target.train_run_id = assignment.train_run_id)
FROM public.train_run_shard_assignments AS assignment
JOIN public.train_run_write_fences AS source USING (train_run_id)
WHERE assignment.train_run_id = $1`, trainRunID).Scan(
		&authority.shardID,
		&authority.generation,
		&authority.assignmentState,
		&authority.hasActiveMigration,
		&authority.sourceGeneration,
		&authority.sourceWriteEnabled,
		&authority.targetZeroFenceRows,
	); err != nil {
		t.Fatalf("read routing authority: %v", err)
	}
	return authority
}

func waitUntilAssignmentQueryBlocked(t *testing.T, pool *pgxpool.Pool, blockerPID int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM pg_stat_activity AS activity
    WHERE $1 = ANY(pg_blocking_pids(activity.pid))
      AND activity.wait_event_type = 'Lock'
      AND activity.query LIKE '%public.train_run_shard_assignments%'
      AND activity.query LIKE '%FOR UPDATE OF assignment%'
)`, blockerPID).Scan(&blocked); err != nil {
			if ctx.Err() != nil {
				t.Fatal("timed out waiting for the assignment query to block")
			}
			t.Fatalf("inspect blocked assignment query: %v", err)
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for the assignment query to block")
		case <-ticker.C:
		}
	}
}

type beginBarrierDB struct {
	db          DB
	want        int32
	arrived     atomic.Int32
	allArrived  chan struct{}
	release     chan struct{}
	arriveOnce  sync.Once
	releaseOnce sync.Once
}

func newBeginBarrierDB(db DB, want int) *beginBarrierDB {
	return &beginBarrierDB{
		db:         db,
		want:       int32(want),
		allArrived: make(chan struct{}),
		release:    make(chan struct{}),
	}
}

func (db *beginBarrierDB) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	arrived := db.arrived.Add(1)
	if arrived == db.want {
		db.arriveOnce.Do(func() { close(db.allArrived) })
	}
	select {
	case <-db.release:
		return db.db.BeginTx(ctx, options)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (db *beginBarrierDB) Query(ctx context.Context, sql string, arguments ...any) (pgx.Rows, error) {
	return db.db.Query(ctx, sql, arguments...)
}

func (db *beginBarrierDB) QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row {
	return db.db.QueryRow(ctx, sql, arguments...)
}

func (db *beginBarrierDB) Release() {
	db.releaseOnce.Do(func() { close(db.release) })
}

func seedRoutingIntegrationTrainRun(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	trainRunID := uuid.New()
	routeID := uuid.New()
	trainID := uuid.New()
	stationIDs := [2]uuid.UUID{uuid.New(), uuid.New()}
	suffix := strings.ToUpper(strings.ReplaceAll(trainRunID.String(), "-", ""))[:12]
	seedTx, err := pool.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin routing integration seed: %v", err)
	}
	defer func() { _ = seedTx.Rollback(context.Background()) }()
	if _, err := seedTx.Exec(context.Background(), `
INSERT INTO public.routes (id, code, name, operating_timezone)
VALUES ($1, $2, 'Milestone 4 routing integration route', 'UTC')`,
		routeID, "M4R"+suffix,
	); err != nil {
		t.Fatalf("seed routing integration route: %v", err)
	}
	for index, stationID := range stationIDs {
		if _, err := seedTx.Exec(context.Background(), `
INSERT INTO public.stations (id, code, name, timezone)
VALUES ($1, $2, $3, 'UTC')`, stationID, fmt.Sprintf("M4S%d%s", index, suffix[:8]), fmt.Sprintf("Milestone 4 routing station %d", index)); err != nil {
			t.Fatalf("seed routing integration station %d: %v", index, err)
		}
		if _, err := seedTx.Exec(context.Background(), `
INSERT INTO public.route_stops (
    route_id, station_id, stop_index, arrival_offset_minutes, departure_offset_minutes
) VALUES ($1, $2, $3, $4, $4)`, routeID, stationID, index, index*10); err != nil {
			t.Fatalf("seed routing integration route stop %d: %v", index, err)
		}
	}
	if _, err := seedTx.Exec(context.Background(), `
INSERT INTO public.trains (id, code, name)
VALUES ($1, $2, 'Milestone 4 routing integration train')`,
		trainID, "M4T"+suffix,
	); err != nil {
		t.Fatalf("seed routing integration train: %v", err)
	}
	if _, err := seedTx.Exec(context.Background(), `
INSERT INTO public.train_runs (
    id, train_id, route_id, service_date, scheduled_departure_at, segment_count
) VALUES ($1, $2, $3, CURRENT_DATE + 365, clock_timestamp() + interval '365 days', 1)`,
		trainRunID, trainID, routeID,
	); err != nil {
		t.Fatalf("seed routing integration train run: %v", err)
	}
	if err := seedTx.Commit(context.Background()); err != nil {
		t.Fatalf("commit routing integration seed: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, `DELETE FROM public.train_runs WHERE id = $1`, trainRunID)
		_, _ = pool.Exec(ctx, `DELETE FROM public.trains WHERE id = $1`, trainID)
		_, _ = pool.Exec(ctx, `DELETE FROM public.routes WHERE id = $1`, routeID)
		_, _ = pool.Exec(ctx, `DELETE FROM public.stations WHERE id = ANY($1::uuid[])`, []uuid.UUID{stationIDs[0], stationIDs[1]})
	})
	return trainRunID
}

func assertExactlyOneCutoverWriter(t *testing.T, pool *pgxpool.Pool, trainRunID uuid.UUID) {
	t.Helper()
	var assignedShard string
	var generation int64
	var sourceGeneration int64
	var sourceEnabled bool
	var targetGeneration int64
	var targetEnabled bool
	if err := pool.QueryRow(context.Background(), `
SELECT assignment.shard_id,
       assignment.assignment_generation,
       source.assignment_generation,
       source.write_enabled,
       target.assignment_generation,
       target.write_enabled
FROM public.train_run_shard_assignments AS assignment
JOIN public.train_run_write_fences AS source USING (train_run_id)
JOIN booking_shard_0.train_run_write_fences AS target USING (train_run_id)
WHERE assignment.train_run_id = $1`, trainRunID).Scan(
		&assignedShard,
		&generation,
		&sourceGeneration,
		&sourceEnabled,
		&targetGeneration,
		&targetEnabled,
	); err != nil {
		t.Fatalf("read cutover writer state: %v", err)
	}
	if assignedShard != "shard-0" || generation != 2 || sourceGeneration != 1 || sourceEnabled || targetGeneration != 2 || !targetEnabled {
		t.Fatalf("cutover writer state = assignment %s/%d, source %d/%t, target %d/%t", assignedShard, generation, sourceGeneration, sourceEnabled, targetGeneration, targetEnabled)
	}
	var enabledWriters int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*)
FROM (
    SELECT write_enabled FROM public.train_run_write_fences WHERE train_run_id = $1
    UNION ALL
    SELECT write_enabled FROM booking_shard_0.train_run_write_fences WHERE train_run_id = $1
    UNION ALL
    SELECT write_enabled FROM booking_shard_1.train_run_write_fences WHERE train_run_id = $1
) AS fences
WHERE write_enabled`, trainRunID).Scan(&enabledWriters); err != nil {
		t.Fatalf("count enabled writers: %v", err)
	}
	if enabledWriters != 1 {
		t.Fatalf("enabled writers = %d, want exactly one", enabledWriters)
	}
}

func openRoutingIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal("parse PostgreSQL integration configuration")
	}
	if config.MaxConns < 4 {
		// Lock-recovery integration tests need one blocker, one blocked routed
		// transaction, and an independent observer/retry connection.
		config.MaxConns = 4
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal("open PostgreSQL integration pool")
	}
	t.Cleanup(pool.Close)
	return pool
}

func assertTransactionPath(t *testing.T, tx pgx.Tx, shardID sharding.ShardID, want string) {
	t.Helper()
	statement, ok := fixedSearchPath(shardID)
	if !ok {
		t.Fatal("fixed shard did not map to a search path")
	}
	if _, err := tx.Exec(context.Background(), statement); err != nil {
		t.Fatal("set transaction-local search path")
	}
	var got string
	if err := tx.QueryRow(context.Background(), "SHOW search_path").Scan(&got); err != nil {
		t.Fatal("read transaction-local search path")
	}
	if got != want {
		t.Fatalf("transaction search_path = %q, want %q", got, want)
	}
}

type searchPathReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func currentSearchPath(t *testing.T, reader searchPathReader) string {
	t.Helper()
	var path string
	if err := reader.QueryRow(context.Background(), "SHOW search_path").Scan(&path); err != nil {
		t.Fatal("read connection search path")
	}
	return path
}
