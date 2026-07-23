package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type fakeShardRouter struct {
	resolveTrainRun    func(context.Context, uuid.UUID) (sharding.ShardRoute, error)
	refreshTrainRun    func(context.Context, uuid.UUID) (sharding.ShardRoute, error)
	resolveReservation func(context.Context, uuid.UUID) (sharding.ShardRoute, error)
	resolveForOwner    func(context.Context, uuid.UUID, uuid.UUID) (sharding.ShardRoute, error)
	beginWrite         func(context.Context, sharding.ShardRoute) (bookingRoutedTx, error)
	beginRead          func(context.Context, sharding.ShardRoute) (bookingRoutedTx, error)
	listShards         func(context.Context) ([]sharding.ShardID, error)
}

func (router *fakeShardRouter) ResolveTrainRun(ctx context.Context, id uuid.UUID) (sharding.ShardRoute, error) {
	return router.resolveTrainRun(ctx, id)
}
func (router *fakeShardRouter) RefreshTrainRun(ctx context.Context, id uuid.UUID) (sharding.ShardRoute, error) {
	return router.refreshTrainRun(ctx, id)
}
func (router *fakeShardRouter) ResolveReservation(ctx context.Context, id uuid.UUID) (sharding.ShardRoute, error) {
	return router.resolveReservation(ctx, id)
}
func (router *fakeShardRouter) ResolveReservationForOwner(
	ctx context.Context,
	id, ownerUserID uuid.UUID,
) (sharding.ShardRoute, error) {
	return router.resolveForOwner(ctx, id, ownerUserID)
}
func (router *fakeShardRouter) BeginTrainRunWrite(ctx context.Context, route sharding.ShardRoute) (bookingRoutedTx, error) {
	return router.beginWrite(ctx, route)
}
func (router *fakeShardRouter) BeginTrainRunRead(ctx context.Context, route sharding.ShardRoute) (bookingRoutedTx, error) {
	return router.beginRead(ctx, route)
}
func (router *fakeShardRouter) ListEnabledShards(ctx context.Context) ([]sharding.ShardID, error) {
	return router.listShards(ctx)
}

type fakeBookingRoutedTx struct {
	pgx.Tx
	route sharding.ShardRoute
}

func (tx *fakeBookingRoutedTx) PGXTx() pgx.Tx                  { return tx.Tx }
func (tx *fakeBookingRoutedTx) Route() sharding.ShardRoute     { return tx.route }
func (tx *fakeBookingRoutedTx) Commit(context.Context) error   { return nil }
func (tx *fakeBookingRoutedTx) Rollback(context.Context) error { return nil }

type embeddedPGXTx struct{ pgx.Tx }

type generationWritePGXTx struct {
	pgx.Tx
	migrationID      uuid.UUID
	state            string
	trainID          uuid.UUID
	segmentCount     int
	execCalls        int
	inventoryInserts int
}

func (tx *generationWritePGXTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	if strings.Contains(sql, "FROM train_runs") {
		return inventoryTrainRow{trainID: tx.trainID, segmentCount: tx.segmentCount}
	}
	return generationWriteRow{migrationID: tx.migrationID, state: tx.state}
}

func (tx *generationWritePGXTx) Exec(_ context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "INSERT INTO seat_inventory") {
		tx.inventoryInserts++
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	}
	if !strings.Contains(sql, "public.train_run_generation_writes") || len(arguments) != 4 {
		return pgconn.CommandTag{}, errors.New("unexpected generation-write statement")
	}
	tx.execCalls++
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

type inventoryTrainRow struct {
	trainID      uuid.UUID
	segmentCount int
}

func (row inventoryTrainRow) Scan(destinations ...any) error {
	if len(destinations) != 2 {
		return errors.New("unexpected inventory train scan")
	}
	*(destinations[0].(*uuid.UUID)) = row.trainID
	*(destinations[1].(*int)) = row.segmentCount
	return nil
}

type generationWriteRow struct {
	migrationID uuid.UUID
	state       string
}

func (row generationWriteRow) Scan(destinations ...any) error {
	if len(destinations) != 2 {
		return errors.New("unexpected generation-write scan")
	}
	*(destinations[0].(*pgtype.UUID)) = pgtype.UUID{Bytes: row.migrationID, Valid: row.migrationID != uuid.Nil}
	*(destinations[1].(*string)) = row.state
	return nil
}

func mustBookingRoute(t *testing.T, run uuid.UUID, shardID sharding.ShardID, generation int64) sharding.ShardRoute {
	t.Helper()
	gen, err := sharding.NewAssignmentGeneration(generation)
	if err != nil {
		t.Fatal(err)
	}
	route, err := sharding.NewShardRoute(run, shardID, gen)
	if err != nil {
		t.Fatal(err)
	}
	return route
}

func TestBeginTrainRunWriteUsesOpaqueResolvedRoute(t *testing.T) {
	t.Parallel()

	run := uuid.New()
	route := mustBookingRoute(t, run, sharding.ShardOne, 4)
	underlying := &embeddedPGXTx{}
	var began sharding.ShardRoute
	store := &Store{shards: &fakeShardRouter{
		resolveTrainRun: func(_ context.Context, got uuid.UUID) (sharding.ShardRoute, error) {
			if got != run {
				t.Fatalf("resolved train run = %s, want %s", got, run)
			}
			return route, nil
		},
		beginWrite: func(_ context.Context, got sharding.ShardRoute) (bookingRoutedTx, error) {
			began = got
			return &fakeBookingRoutedTx{Tx: underlying, route: got}, nil
		},
	}}

	tx, err := store.beginTrainRunWrite(context.Background(), run)
	if err != nil {
		t.Fatalf("beginTrainRunWrite() error = %v", err)
	}
	if began != route || tx.tx != underlying || tx.route != route || tx.routed == nil {
		t.Fatalf("routed transaction = %+v, began=%+v", tx, began)
	}
}

func TestInitializeInventoryUsesRoutedAuthorityAndRecordsRollbackEvidence(t *testing.T) {
	t.Parallel()

	run, trainID, migrationID := uuid.New(), uuid.New(), uuid.New()
	route := mustBookingRoute(t, run, sharding.ShardLegacy, 8)
	underlying := &generationWritePGXTx{
		migrationID:  migrationID,
		state:        "rollback_window",
		trainID:      trainID,
		segmentCount: 2,
	}
	store := &Store{shards: &fakeShardRouter{
		resolveTrainRun: func(_ context.Context, got uuid.UUID) (sharding.ShardRoute, error) {
			if got != run {
				t.Fatalf("resolved train run = %s, want %s", got, run)
			}
			return route, nil
		},
		beginWrite: func(_ context.Context, got sharding.ShardRoute) (bookingRoutedTx, error) {
			return &fakeBookingRoutedTx{Tx: underlying, route: got}, nil
		},
	}}

	inserted, err := store.InitializeInventory(context.Background(), run)
	if err != nil {
		t.Fatalf("InitializeInventory() error = %v", err)
	}
	if inserted != 1 || underlying.inventoryInserts != 1 || underlying.execCalls != 1 {
		t.Fatalf("inventory result = inserted %d inventory statements %d evidence statements %d", inserted, underlying.inventoryInserts, underlying.execCalls)
	}
}

func TestBeginReservationReadRefreshesOneStaleLocatorGeneration(t *testing.T) {
	t.Parallel()

	reservationID, ownerUserID, run := uuid.New(), uuid.New(), uuid.New()
	stale := mustBookingRoute(t, run, sharding.ShardZero, 3)
	fresh := mustBookingRoute(t, run, sharding.ShardOne, 4)
	underlying := &embeddedPGXTx{}
	beginCalls, refreshCalls := 0, 0
	store := &Store{shards: &fakeShardRouter{
		resolveForOwner: func(_ context.Context, got, gotOwner uuid.UUID) (sharding.ShardRoute, error) {
			if got != reservationID || gotOwner != ownerUserID {
				t.Fatalf("reservation owner lookup = (%s, %s), want (%s, %s)", got, gotOwner, reservationID, ownerUserID)
			}
			return stale, nil
		},
		refreshTrainRun: func(_ context.Context, got uuid.UUID) (sharding.ShardRoute, error) {
			refreshCalls++
			if got != run {
				t.Fatalf("refresh train run = %s, want %s", got, run)
			}
			return fresh, nil
		},
		beginRead: func(_ context.Context, got sharding.ShardRoute) (bookingRoutedTx, error) {
			beginCalls++
			if beginCalls == 1 {
				if got != stale {
					t.Fatalf("first route = %+v, want stale", got)
				}
				return nil, sharding.ErrAssignmentStale
			}
			if got != fresh {
				t.Fatalf("second route = %+v, want fresh", got)
			}
			return &fakeBookingRoutedTx{Tx: underlying, route: fresh}, nil
		},
	}}

	tx, err := store.beginReservationRead(context.Background(), reservationID, ownerUserID)
	if err != nil {
		t.Fatalf("beginReservationRead() error = %v", err)
	}
	if beginCalls != 2 || refreshCalls != 1 || tx.route != fresh {
		t.Fatalf("calls = begin %d refresh %d, route=%+v", beginCalls, refreshCalls, tx.route)
	}
}

func TestBeginReservationWriteDoesNotProbeAfterMissingLocator(t *testing.T) {
	t.Parallel()

	beginCalls := 0
	store := &Store{shards: &fakeShardRouter{
		resolveForOwner: func(context.Context, uuid.UUID, uuid.UUID) (sharding.ShardRoute, error) {
			return sharding.ShardRoute{}, sharding.ErrLocatorNotFound
		},
		beginWrite: func(context.Context, sharding.ShardRoute) (bookingRoutedTx, error) {
			beginCalls++
			return nil, errors.New("must not begin")
		},
	}}

	if _, err := store.beginReservationWrite(context.Background(), uuid.New(), uuid.New()); !errors.Is(err, sharding.ErrLocatorNotFound) {
		t.Fatalf("beginReservationWrite() error = %v", err)
	}
	if beginCalls != 0 {
		t.Fatalf("begin calls = %d, want zero", beginCalls)
	}
}

func TestDueReservationQueriesSelectOnlyAuthoritativeWritableLocatorRows(t *testing.T) {
	for _, shardID := range []sharding.ShardID{sharding.ShardLegacy, sharding.ShardZero, sharding.ShardOne} {
		query, ok := dueReservationQuery(shardID)
		if !ok {
			t.Fatalf("dueReservationQuery(%s) rejected fixed shard", shardID)
		}
		for _, fragment := range []string{
			"public.reservation_shard_locators",
			"public.train_run_shard_assignments",
			"assignment.assignment_generation = locator.assignment_generation",
			"assignment.assignment_state IN ('stable', 'rollback_window')",
			"catalog.write_enabled",
			"catalog.minimum_fencing_protocol_version <= $2",
			"locator.shard_id = '" + shardID.String() + "'",
		} {
			if !strings.Contains(query, fragment) {
				t.Fatalf("dueReservationQuery(%s) missing %q", shardID, fragment)
			}
		}
	}
}

func TestRecordSuccessfulGenerationWritePersistsRollbackEvidence(t *testing.T) {
	t.Parallel()

	run, migrationID := uuid.New(), uuid.New()
	route := mustBookingRoute(t, run, sharding.ShardZero, 2)
	underlying := &generationWritePGXTx{migrationID: migrationID, state: "rollback_window"}
	tx := &Tx{
		tx: underlying, route: route,
		routed: &fakeBookingRoutedTx{Tx: underlying, route: route},
	}

	if err := tx.recordSuccessfulGenerationWrite(context.Background()); err != nil {
		t.Fatalf("recordSuccessfulGenerationWrite() error = %v", err)
	}
	if underlying.execCalls != 1 {
		t.Fatalf("generation-write inserts = %d, want 1", underlying.execCalls)
	}
}

func TestRecordSuccessfulGenerationWriteRejectsRollbackWindowWithoutMigration(t *testing.T) {
	t.Parallel()

	run := uuid.New()
	route := mustBookingRoute(t, run, sharding.ShardOne, 4)
	underlying := &generationWritePGXTx{state: "rollback_window"}
	tx := &Tx{
		tx: underlying, route: route,
		routed: &fakeBookingRoutedTx{Tx: underlying, route: route},
	}

	if err := tx.recordSuccessfulGenerationWrite(context.Background()); !errors.Is(err, ErrPersistenceInvariant) {
		t.Fatalf("recordSuccessfulGenerationWrite() error = %v", err)
	}
	if underlying.execCalls != 0 {
		t.Fatalf("generation-write inserts = %d, want 0", underlying.execCalls)
	}
}

func TestExpiredIdempotencyCleanupOnlyMutatesStableAuthoritativeShard(t *testing.T) {
	t.Parallel()

	for _, shardID := range []sharding.ShardID{
		sharding.ShardLegacy,
		sharding.ShardZero,
		sharding.ShardOne,
	} {
		query, ok := expiredIdempotencyCleanupQuery(shardID)
		if !ok {
			t.Fatalf("expiredIdempotencyCleanupQuery(%s) unavailable", shardID)
		}
		for _, required := range []string{
			"public.train_run_shard_assignments",
			"assignment.assignment_state = 'stable'",
			"assignment.active_migration_id IS NULL",
			"assignment.shard_id = '",
		} {
			if !strings.Contains(query, required) {
				t.Fatalf("cleanup query for %s lacks rollback-safe authority guard %q: %s", shardID, required, query)
			}
		}
	}
}
