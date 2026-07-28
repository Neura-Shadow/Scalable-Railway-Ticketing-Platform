package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestResolveTrainRunReturnsAllowlistedCatalogRoute(t *testing.T) {
	trainRunID := uuid.New()
	db := &fakeDB{
		queryRow: func(context.Context, string, ...any) pgx.Row {
			return fakeRow{values: []any{"shard-0", int64(7), true, "active", sharding.SupportedFencingProtocolVersion}}
		},
	}
	router, err := NewRouter(db, nil)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	got, err := router.ResolveTrainRun(context.Background(), trainRunID)
	if err != nil {
		t.Fatalf("ResolveTrainRun() error = %v", err)
	}
	wantGeneration, err := sharding.NewAssignmentGeneration(7)
	if err != nil {
		t.Fatal(err)
	}
	if got.TrainRunID() != trainRunID || got.ShardID() != sharding.ShardZero || got.Generation() != wantGeneration {
		t.Fatalf("ResolveTrainRun() = (%s, %s, %d), want (%s, %s, 7)", got.TrainRunID(), got.ShardID(), got.Generation().Int64(), trainRunID, sharding.ShardZero)
	}
}

func TestNewRouterRejectsTypedNilDatabase(t *testing.T) {
	var db *fakeDB
	router, err := NewRouter(db, nil)
	if router != nil || !errors.Is(err, sharding.ErrShardUnavailable) {
		t.Fatalf("NewRouter() = (%v, %v), want nil, %v", router, err, sharding.ErrShardUnavailable)
	}
}

func TestNewRouterTreatsTypedNilOptionalCacheAsDisabled(t *testing.T) {
	trainRunID := uuid.New()
	db := &fakeDB{
		queryRow: func(context.Context, string, ...any) pgx.Row {
			return fakeRow{values: []any{"legacy", int64(1), true, "active", sharding.SupportedFencingProtocolVersion}}
		},
	}
	var cache *fakeRouteCache
	router, err := NewRouter(db, cache)
	if err != nil {
		t.Fatal(err)
	}

	got, err := router.ResolveTrainRun(context.Background(), trainRunID)
	if err != nil {
		t.Fatalf("ResolveTrainRun() error = %v", err)
	}
	if got.ShardID() != sharding.ShardLegacy {
		t.Fatalf("ResolveTrainRun() shard = %s, want legacy", got.ShardID())
	}
}

func TestNewRouterRejectsInvalidConfiguredShardSubset(t *testing.T) {
	for name, option := range map[string]Option{
		"empty":     WithAllowedShards(),
		"duplicate": WithAllowedShards(sharding.ShardLegacy, sharding.ShardLegacy),
		"unknown":   WithAllowedShards(sharding.ShardID("shard-9")),
	} {
		t.Run(name, func(t *testing.T) {
			router, err := NewRouter(&fakeDB{}, nil, option)
			if router != nil || !errors.Is(err, sharding.ErrShardUnavailable) {
				t.Fatalf("NewRouter() = (%v, %v), want nil, %v", router, err, sharding.ErrShardUnavailable)
			}
		})
	}
}

func TestConfiguredShardSubsetRejectsExcludedCatalogLocatorCacheAndTransactions(t *testing.T) {
	trainRunID := uuid.New()
	excluded := mustRoute(t, trainRunID, sharding.ShardOne, 9)
	cache := &fakeRouteCache{route: excluded, found: true}
	db := &fakeDB{queryRow: func(_ context.Context, sql string, _ ...any) pgx.Row {
		if strings.Contains(sql, "_shard_locators") {
			return fakeRow{values: []any{trainRunID, "shard-1", int64(9)}}
		}
		return fakeRow{values: []any{"shard-1", int64(9), true, "active", sharding.SupportedFencingProtocolVersion}}
	}}
	router, err := NewRouter(
		db,
		cache,
		WithAllowedShards(sharding.ShardLegacy, sharding.ShardZero),
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := router.ResolveTrainRun(context.Background(), trainRunID); !errors.Is(err, sharding.ErrShardUnavailable) {
		t.Fatalf("ResolveTrainRun() error = %v, want %v", err, sharding.ErrShardUnavailable)
	}
	if cache.invalidated != trainRunID {
		t.Fatalf("excluded cached route was not invalidated: %s", cache.invalidated)
	}
	if _, err := router.ResolveReservation(context.Background(), uuid.New()); !errors.Is(err, sharding.ErrShardUnavailable) {
		t.Fatalf("ResolveReservation() error = %v, want %v", err, sharding.ErrShardUnavailable)
	}
	if _, err := router.BeginTrainRunRead(context.Background(), excluded); !errors.Is(err, sharding.ErrShardUnavailable) {
		t.Fatalf("BeginTrainRunRead() error = %v, want %v", err, sharding.ErrShardUnavailable)
	}
	if _, err := router.BeginTrainRunWrite(context.Background(), excluded); !errors.Is(err, sharding.ErrShardUnavailable) {
		t.Fatalf("BeginTrainRunWrite() error = %v, want %v", err, sharding.ErrShardUnavailable)
	}
}

func TestListEnabledShardsIntersectsConfiguredSubset(t *testing.T) {
	db := &fakeDB{query: func(context.Context, string, ...any) (pgx.Rows, error) {
		return &fakeRows{values: [][]any{
			{"legacy", sharding.SupportedFencingProtocolVersion},
			{"shard-0", sharding.SupportedFencingProtocolVersion},
			{"shard-1", sharding.SupportedFencingProtocolVersion},
		}, index: -1}, nil
	}}
	router, err := NewRouter(
		db,
		nil,
		WithAllowedShards(sharding.ShardLegacy, sharding.ShardOne),
	)
	if err != nil {
		t.Fatal(err)
	}
	got, err := router.ListEnabledShards(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []sharding.ShardID{sharding.ShardLegacy, sharding.ShardOne}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListEnabledShards() = %#v, want %#v", got, want)
	}
}

func TestListEnabledShardsIgnoresExcludedShardProtocolDuringRollingUpgrade(t *testing.T) {
	db := &fakeDB{query: func(context.Context, string, ...any) (pgx.Rows, error) {
		return &fakeRows{values: [][]any{
			{"legacy", sharding.SupportedFencingProtocolVersion},
			{"shard-0", sharding.SupportedFencingProtocolVersion},
			{"shard-1", sharding.SupportedFencingProtocolVersion + 1},
		}, index: -1}, nil
	}}
	router, err := NewRouter(
		db,
		nil,
		WithAllowedShards(sharding.ShardLegacy, sharding.ShardZero),
	)
	if err != nil {
		t.Fatal(err)
	}
	got, err := router.ListEnabledShards(context.Background())
	if err != nil {
		t.Fatalf("ListEnabledShards() error = %v", err)
	}
	want := []sharding.ShardID{sharding.ShardLegacy, sharding.ShardZero}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListEnabledShards() = %#v, want %#v", got, want)
	}
}

func TestResolveReservationUsesOneGlobalLocator(t *testing.T) {
	reservationID := uuid.New()
	trainRunID := uuid.New()
	db := &fakeDB{
		queryRow: func(_ context.Context, sql string, args ...any) pgx.Row {
			if !strings.Contains(sql, "public.reservation_shard_locators") {
				t.Fatalf("locator query = %q", sql)
			}
			if len(args) != 1 || args[0] != reservationID {
				t.Fatalf("locator args = %#v, want reservation ID only", args)
			}
			return fakeRow{values: []any{trainRunID, "shard-1", int64(9)}}
		},
	}
	router, err := NewRouter(db, nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := router.ResolveReservation(context.Background(), reservationID)
	if err != nil {
		t.Fatalf("ResolveReservation() error = %v", err)
	}
	if got.TrainRunID() != trainRunID || got.ShardID() != sharding.ShardOne || got.Generation().Int64() != 9 {
		t.Fatalf("ResolveReservation() = (%s, %s, %d)", got.TrainRunID(), got.ShardID(), got.Generation().Int64())
	}
}

func TestResolveReservationForOwnerBindsEarlyAuthorizationPredicate(t *testing.T) {
	reservationID, ownerUserID, trainRunID := uuid.New(), uuid.New(), uuid.New()
	db := &fakeDB{queryRow: func(_ context.Context, sql string, args ...any) pgx.Row {
		if !strings.Contains(sql, "owner_user_id = $2") {
			t.Fatalf("owner-scoped locator query = %q", sql)
		}
		if len(args) != 2 || args[0] != reservationID || args[1] != ownerUserID {
			t.Fatalf("owner-scoped locator args = %#v", args)
		}
		return fakeRow{values: []any{trainRunID, "legacy", int64(3)}}
	}}
	router, err := NewRouter(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := router.ResolveReservationForOwner(context.Background(), reservationID, ownerUserID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TrainRunID() != trainRunID || got.ShardID() != sharding.ShardLegacy || got.Generation().Int64() != 3 {
		t.Fatalf("ResolveReservationForOwner() = %+v", got)
	}
}

func TestResolveTicketOrderUsesOneGlobalLocator(t *testing.T) {
	ticketOrderID := uuid.New()
	trainRunID := uuid.New()
	db := &fakeDB{
		queryRow: func(_ context.Context, sql string, args ...any) pgx.Row {
			if !strings.Contains(sql, "public.ticket_order_shard_locators") {
				t.Fatalf("locator query = %q", sql)
			}
			if len(args) != 1 || args[0] != ticketOrderID {
				t.Fatalf("locator args = %#v, want ticket-order ID only", args)
			}
			return fakeRow{values: []any{trainRunID, "legacy", int64(4)}}
		},
	}
	router, err := NewRouter(db, nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := router.ResolveTicketOrder(context.Background(), ticketOrderID)
	if err != nil {
		t.Fatalf("ResolveTicketOrder() error = %v", err)
	}
	if got.TrainRunID() != trainRunID || got.ShardID() != sharding.ShardLegacy || got.Generation().Int64() != 4 {
		t.Fatalf("ResolveTicketOrder() = (%s, %s, %d)", got.TrainRunID(), got.ShardID(), got.Generation().Int64())
	}
}

func TestResolveTicketUsesOneGlobalLocator(t *testing.T) {
	ticketID := uuid.New()
	trainRunID := uuid.New()
	db := &fakeDB{
		queryRow: func(_ context.Context, sql string, args ...any) pgx.Row {
			if !strings.Contains(sql, "public.ticket_shard_locators") {
				t.Fatalf("locator query = %q", sql)
			}
			if len(args) != 1 || args[0] != ticketID {
				t.Fatalf("locator args = %#v, want ticket ID only", args)
			}
			return fakeRow{values: []any{trainRunID, "shard-0", int64(5)}}
		},
	}
	router, err := NewRouter(db, nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := router.ResolveTicket(context.Background(), ticketID)
	if err != nil {
		t.Fatalf("ResolveTicket() error = %v", err)
	}
	if got.TrainRunID() != trainRunID || got.ShardID() != sharding.ShardZero || got.Generation().Int64() != 5 {
		t.Fatalf("ResolveTicket() = (%s, %s, %d)", got.TrainRunID(), got.ShardID(), got.Generation().Int64())
	}
}

func TestResolveLocatorReturnsOnlyTypedBoundedFailures(t *testing.T) {
	resourceID := uuid.New()
	trainRunID := uuid.New()
	tests := []struct {
		name string
		row  pgx.Row
		want error
	}{
		{
			name: "missing locator",
			row:  fakeRow{err: pgx.ErrNoRows},
			want: sharding.ErrLocatorNotFound,
		},
		{
			name: "database detail is discarded",
			row:  fakeRow{err: errors.New("postgres://operator:secret@db/internal_schema")},
			want: sharding.ErrShardUnavailable,
		},
		{
			name: "unknown catalog shard is rejected",
			row:  fakeRow{values: []any{trainRunID, "booking_shard_0; DROP SCHEMA public", int64(1)}},
			want: sharding.ErrShardUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := &fakeDB{queryRow: func(context.Context, string, ...any) pgx.Row { return test.row }}
			router, err := NewRouter(db, nil)
			if err != nil {
				t.Fatal(err)
			}

			_, err = router.ResolveReservation(context.Background(), resourceID)
			if !errors.Is(err, test.want) {
				t.Fatalf("ResolveReservation() error = %v, want %v", err, test.want)
			}
			var typed *sharding.RouteError
			if !errors.As(err, &typed) {
				t.Fatalf("error type = %T, want *sharding.RouteError", err)
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "schema") {
				t.Fatalf("error leaks backend or topology detail: %q", err)
			}
		})
	}
}

func TestRefreshTrainRunInvalidatesCachedRouteAndReloadsCatalog(t *testing.T) {
	trainRunID := uuid.New()
	cache := &fakeRouteCache{route: mustRoute(t, trainRunID, sharding.ShardZero, 1), found: true}
	db := &fakeDB{
		queryRow: func(context.Context, string, ...any) pgx.Row {
			return fakeRow{values: []any{"shard-1", int64(2), true, "active", sharding.SupportedFencingProtocolVersion}}
		},
	}
	router, err := NewRouter(db, cache)
	if err != nil {
		t.Fatal(err)
	}

	got, err := router.RefreshTrainRun(context.Background(), trainRunID)
	if err != nil {
		t.Fatalf("RefreshTrainRun() error = %v", err)
	}
	if cache.invalidated != trainRunID {
		t.Fatalf("Invalidate() ID = %s, want %s", cache.invalidated, trainRunID)
	}
	if got.ShardID() != sharding.ShardOne || got.Generation().Int64() != 2 {
		t.Fatalf("RefreshTrainRun() = (%s, %d)", got.ShardID(), got.Generation().Int64())
	}
}

func TestResolveTrainRunDoesNotPromoteAdvisoryCacheFailureToAuthorityFailure(t *testing.T) {
	trainRunID := uuid.New()
	cache := &fakeRouteCache{putErr: errors.New("cache unavailable")}
	db := &fakeDB{
		queryRow: func(context.Context, string, ...any) pgx.Row {
			return fakeRow{values: []any{"legacy", int64(1), true, "active", sharding.SupportedFencingProtocolVersion}}
		},
	}
	router, err := NewRouter(db, cache)
	if err != nil {
		t.Fatal(err)
	}

	got, err := router.ResolveTrainRun(context.Background(), trainRunID)
	if err != nil {
		t.Fatalf("ResolveTrainRun() error = %v", err)
	}
	if got.ShardID() != sharding.ShardLegacy || got.Generation().Int64() != 1 {
		t.Fatalf("ResolveTrainRun() = (%s, %d)", got.ShardID(), got.Generation().Int64())
	}
}

func TestResolveTrainRunRejectsInvalidCacheEntryAndReloadsCatalog(t *testing.T) {
	trainRunID := uuid.New()
	cache := &fakeRouteCache{found: true}
	dbCalls := 0
	db := &fakeDB{
		queryRow: func(context.Context, string, ...any) pgx.Row {
			dbCalls++
			return fakeRow{values: []any{"shard-0", int64(6), true, "active", sharding.SupportedFencingProtocolVersion}}
		},
	}
	router, err := NewRouter(db, cache)
	if err != nil {
		t.Fatal(err)
	}

	got, err := router.ResolveTrainRun(context.Background(), trainRunID)
	if err != nil {
		t.Fatalf("ResolveTrainRun() error = %v", err)
	}
	if dbCalls != 1 || cache.invalidated != trainRunID {
		t.Fatalf("catalog calls = %d, invalidated = %s", dbCalls, cache.invalidated)
	}
	if got.ShardID() != sharding.ShardZero || got.Generation().Int64() != 6 {
		t.Fatalf("ResolveTrainRun() = (%s, %d)", got.ShardID(), got.Generation().Int64())
	}
}

func TestResolveTrainRunFailsClosedForNonServingCatalogState(t *testing.T) {
	for _, state := range []string{"degraded", "disabled", "unknown"} {
		t.Run(state, func(t *testing.T) {
			db := &fakeDB{
				queryRow: func(context.Context, string, ...any) pgx.Row {
					return fakeRow{values: []any{"shard-0", int64(6), true, state, sharding.SupportedFencingProtocolVersion}}
				},
			}
			router, err := NewRouter(db, nil)
			if err != nil {
				t.Fatal(err)
			}

			_, err = router.ResolveTrainRun(context.Background(), uuid.New())
			if !errors.Is(err, sharding.ErrShardUnavailable) {
				t.Fatalf("ResolveTrainRun() error = %v, want %v", err, sharding.ErrShardUnavailable)
			}
		})
	}
}

func TestResolveTrainRunRejectsUnsupportedFencingProtocol(t *testing.T) {
	db := &fakeDB{
		queryRow: func(context.Context, string, ...any) pgx.Row {
			return fakeRow{values: []any{"shard-0", int64(6), true, "active", sharding.SupportedFencingProtocolVersion + 1}}
		},
	}
	router, err := NewRouter(db, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = router.ResolveTrainRun(context.Background(), uuid.New())
	if !errors.Is(err, sharding.ErrShardUnavailable) {
		t.Fatalf("ResolveTrainRun() error = %v, want %v", err, sharding.ErrShardUnavailable)
	}
}

func TestListEnabledShardsReturnsBoundedAllowlistedWorkset(t *testing.T) {
	db := &fakeDB{
		query: func(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
			if !strings.Contains(sql, "public.booking_shards") || len(args) != 0 {
				t.Fatalf("enabled-shard query = %q, args = %#v", sql, args)
			}
			return &fakeRows{values: [][]any{
				{"legacy", sharding.SupportedFencingProtocolVersion},
				{"shard-0", sharding.SupportedFencingProtocolVersion},
				{"shard-1", sharding.SupportedFencingProtocolVersion},
			}, index: -1}, nil
		},
	}
	router, err := NewRouter(db, nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := router.ListEnabledShards(context.Background())
	if err != nil {
		t.Fatalf("ListEnabledShards() error = %v", err)
	}
	want := []sharding.ShardID{sharding.ShardLegacy, sharding.ShardZero, sharding.ShardOne}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListEnabledShards() = %#v, want %#v", got, want)
	}
}

func TestBeginTrainRunWriteEstablishesFixedContextAndLocksBothAuthorities(t *testing.T) {
	trainRunID := uuid.New()
	route := mustRoute(t, trainRunID, sharding.ShardZero, 7)
	var statements []string
	tx := &fakeTx{
		exec: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
			statements = append(statements, strings.TrimSpace(sql))
			return pgconn.NewCommandTag("SET"), nil
		},
		queryRow: func(_ context.Context, sql string, args ...any) pgx.Row {
			statements = append(statements, strings.TrimSpace(sql))
			if len(args) != 1 || args[0] != trainRunID {
				t.Fatalf("lock args = %#v, want train-run ID", args)
			}
			if strings.Contains(sql, "public.train_run_shard_assignments") {
				return fakeRow{values: []any{"shard-0", int64(7), true, true, "active", sharding.SupportedFencingProtocolVersion, "stable", false}}
			}
			if strings.Contains(sql, "train_run_write_fences") {
				return fakeRow{values: []any{int64(7), true}}
			}
			t.Fatalf("unexpected transaction query %q", sql)
			return fakeRow{}
		},
	}
	db := &fakeDB{
		beginTx: func(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
			if options.IsoLevel != pgx.ReadCommitted || options.AccessMode == pgx.ReadOnly {
				t.Fatalf("BeginTx options = %#v", options)
			}
			return tx, nil
		},
	}
	router, err := NewRouter(db, nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := router.BeginTrainRunWrite(context.Background(), route)
	if err != nil {
		t.Fatalf("BeginTrainRunWrite() error = %v", err)
	}
	if got.PGXTx() != tx || got.Route() != route {
		t.Fatal("routed transaction did not retain the pgx transaction and route")
	}
	if len(statements) != 3 {
		t.Fatalf("statements = %#v, want search path plus two authority locks", statements)
	}
	if statements[0] != "SET LOCAL search_path TO pg_catalog, booking_shard_0, public, pg_temp" {
		t.Fatalf("search-path statement = %q", statements[0])
	}
	if !strings.Contains(statements[1], "FOR UPDATE OF assignment") || !strings.Contains(statements[1], "FOR SHARE OF shard") {
		t.Fatalf("assignment/catalog query does not lock both authorities: %q", statements[1])
	}
	if !strings.Contains(statements[2], "FOR UPDATE") {
		t.Fatalf("fence query does not lock the row: %q", statements[2])
	}
}

func TestBeginTrainRunReadRevalidatesRouteWithoutWriteAuthority(t *testing.T) {
	trainRunID := uuid.New()
	route := mustRoute(t, trainRunID, sharding.ShardOne, 11)
	var statements []string
	tx := &fakeTx{
		exec: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
			statements = append(statements, strings.TrimSpace(sql))
			return pgconn.NewCommandTag("SET"), nil
		},
		queryRow: func(_ context.Context, sql string, args ...any) pgx.Row {
			statements = append(statements, strings.TrimSpace(sql))
			if len(args) != 1 || args[0] != trainRunID {
				t.Fatalf("assignment args = %#v", args)
			}
			return fakeRow{values: []any{"shard-1", int64(11), true, "draining", sharding.SupportedFencingProtocolVersion}}
		},
	}
	db := &fakeDB{
		beginTx: func(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
			if options.IsoLevel != pgx.RepeatableRead || options.AccessMode != pgx.ReadOnly {
				t.Fatalf("BeginTx options = %#v", options)
			}
			return tx, nil
		},
	}
	router, err := NewRouter(db, nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := router.BeginTrainRunRead(context.Background(), route)
	if err != nil {
		t.Fatalf("BeginTrainRunRead() error = %v", err)
	}
	if got.PGXTx() != tx || got.Route() != route {
		t.Fatal("read transaction did not retain the pgx transaction and route")
	}
	if len(statements) != 2 {
		t.Fatalf("read statements = %#v, want only search path and assignment validation", statements)
	}
	if statements[0] != "SET LOCAL search_path TO pg_catalog, booking_shard_1, public, pg_temp" {
		t.Fatalf("search-path statement = %q", statements[0])
	}
	if strings.Contains(statements[1], "write_enabled") || strings.Contains(statements[1], "train_run_write_fences") {
		t.Fatalf("read validation unexpectedly requires write authority: %q", statements[1])
	}
}

func TestFixedSearchPathMapsOnlyCompiledShardHandles(t *testing.T) {
	tests := []struct {
		shardID sharding.ShardID
		want    string
		ok      bool
	}{
		{shardID: sharding.ShardLegacy, want: "SET LOCAL search_path TO pg_catalog, public, pg_temp", ok: true},
		{shardID: sharding.ShardZero, want: "SET LOCAL search_path TO pg_catalog, booking_shard_0, public, pg_temp", ok: true},
		{shardID: sharding.ShardOne, want: "SET LOCAL search_path TO pg_catalog, booking_shard_1, public, pg_temp", ok: true},
		{shardID: sharding.ShardID("booking_shard_0, public; DROP SCHEMA public"), ok: false},
	}
	for _, test := range tests {
		got, ok := fixedSearchPath(test.shardID)
		if got != test.want || ok != test.ok {
			t.Fatalf("fixedSearchPath(%q) = (%q, %t), want (%q, %t)", test.shardID, got, ok, test.want, test.ok)
		}
	}
}

func TestBeginTrainRunWriteRejectsStaleAssignmentBeforeFenceAccess(t *testing.T) {
	trainRunID := uuid.New()
	route := mustRoute(t, trainRunID, sharding.ShardZero, 7)
	fenceQueried := false
	tx := &fakeTx{
		exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("SET"), nil
		},
		queryRow: func(_ context.Context, sql string, _ ...any) pgx.Row {
			if strings.Contains(sql, "public.train_run_shard_assignments") {
				return fakeRow{values: []any{"shard-1", int64(8), true, true, "active", sharding.SupportedFencingProtocolVersion, "stable", false}}
			}
			fenceQueried = true
			return fakeRow{values: []any{int64(7), true}}
		},
	}
	db := &fakeDB{beginTx: func(context.Context, pgx.TxOptions) (pgx.Tx, error) { return tx, nil }}
	router, err := NewRouter(db, nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := router.BeginTrainRunWrite(context.Background(), route)
	if got != nil || !errors.Is(err, sharding.ErrAssignmentStale) {
		t.Fatalf("BeginTrainRunWrite() = (%v, %v), want nil, %v", got, err, sharding.ErrAssignmentStale)
	}
	if tx.rollbacks != 1 || fenceQueried {
		t.Fatalf("rollbacks = %d, fence queried = %t", tx.rollbacks, fenceQueried)
	}
}

func TestBeginTrainRunWriteCancellationRollsBackAndAllowsSafeRetry(t *testing.T) {
	trainRunID := uuid.New()
	route := mustRoute(t, trainRunID, sharding.ShardLegacy, 1)
	firstQuery := make(chan string, 1)
	firstTx := &fakeTx{
		exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("SET"), nil
		},
		queryRow: func(ctx context.Context, sql string, _ ...any) pgx.Row {
			firstQuery <- sql
			if !strings.Contains(sql, "public.train_run_shard_assignments") {
				return fakeRow{err: errors.New("unexpected query before assignment authority")}
			}
			<-ctx.Done()
			return fakeRow{err: ctx.Err()}
		},
	}
	secondTx := fakeAuthorityTx("legacy", 1, true, "active", "stable", false, 1, true)
	beginCount := 0
	db := &fakeDB{beginTx: func(context.Context, pgx.TxOptions) (pgx.Tx, error) {
		beginCount++
		if beginCount == 1 {
			return firstTx, nil
		}
		if beginCount == 2 {
			return secondTx, nil
		}
		return nil, errors.New("unexpected extra BeginTx call")
	}}
	router, err := NewRouter(db, nil)
	if err != nil {
		t.Fatal(err)
	}

	requestContext, cancel := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		transaction, beginErr := router.BeginTrainRunWrite(requestContext, route)
		if transaction != nil {
			_ = transaction.Rollback(context.Background())
		}
		firstResult <- beginErr
	}()
	observedQuery := <-firstQuery
	cancel()
	firstErr := <-firstResult
	if !strings.Contains(observedQuery, "public.train_run_shard_assignments") {
		t.Fatalf("canceled attempt queried %q before assignment authority", observedQuery)
	}
	if !errors.Is(firstErr, sharding.ErrShardUnavailable) {
		t.Fatalf("canceled attempt error = %v, want %v", firstErr, sharding.ErrShardUnavailable)
	}
	if firstTx.commits != 0 || firstTx.rollbacks != 1 {
		t.Fatalf("canceled attempt commits=%d rollbacks=%d, want 0/1", firstTx.commits, firstTx.rollbacks)
	}

	retry, err := router.BeginTrainRunWrite(context.Background(), route)
	if err != nil {
		t.Fatalf("retry after cancellation error = %v", err)
	}
	if retry.Route() != route {
		_ = retry.Rollback(context.Background())
		t.Fatalf("retry route = %+v, want unchanged legacy generation 1", retry.Route())
	}
	if err := retry.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback successful retry: %v", err)
	}
	if secondTx.commits != 0 || secondTx.rollbacks != 1 {
		t.Fatalf("retry commits=%d rollbacks=%d, want 0/1", secondTx.commits, secondTx.rollbacks)
	}
	if beginCount != 2 {
		t.Fatalf("BeginTx calls = %d, want exactly two bounded attempts", beginCount)
	}
}

func TestBeginTrainRunWriteTreatsUnknownCatalogShardAsUnavailable(t *testing.T) {
	trainRunID := uuid.New()
	route := mustRoute(t, trainRunID, sharding.ShardZero, 7)
	tx := fakeAuthorityTx("booking_shard_0; DROP SCHEMA public", 7, true, "active", "stable", false, 7, true)
	db := &fakeDB{beginTx: func(context.Context, pgx.TxOptions) (pgx.Tx, error) { return tx, nil }}
	router, err := NewRouter(db, nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := router.BeginTrainRunWrite(context.Background(), route)
	if got != nil || !errors.Is(err, sharding.ErrShardUnavailable) {
		t.Fatalf("BeginTrainRunWrite() = (%v, %v), want nil, %v", got, err, sharding.ErrShardUnavailable)
	}
	if tx.rollbacks != 1 {
		t.Fatalf("rollbacks = %d, want 1", tx.rollbacks)
	}
}

func TestBeginTrainRunWriteWithRefreshRetriesStaleRouteExactlyOnce(t *testing.T) {
	trainRunID := uuid.New()
	staleRoute := mustRoute(t, trainRunID, sharding.ShardZero, 1)
	first := fakeAuthorityTx("shard-0", 2, true, "active", "stable", false, 2, true)
	second := fakeAuthorityTx("shard-0", 2, true, "active", "stable", false, 2, true)
	beginCount := 0
	cache := &fakeRouteCache{route: staleRoute, found: true}
	db := &fakeDB{
		beginTx: func(context.Context, pgx.TxOptions) (pgx.Tx, error) {
			beginCount++
			if beginCount == 1 {
				return first, nil
			}
			return second, nil
		},
		queryRow: func(context.Context, string, ...any) pgx.Row {
			return fakeRow{values: []any{"shard-0", int64(2), true, "active", sharding.SupportedFencingProtocolVersion}}
		},
	}
	router, err := NewRouter(db, cache)
	if err != nil {
		t.Fatal(err)
	}

	got, err := router.BeginTrainRunWriteWithRefresh(context.Background(), staleRoute)
	if err != nil {
		t.Fatalf("BeginTrainRunWriteWithRefresh() error = %v", err)
	}
	if got.Route().Generation().Int64() != 2 || got.PGXTx() != second {
		t.Fatalf("retried transaction route = (%d, %p), want generation 2 on second tx", got.Route().Generation().Int64(), got.PGXTx())
	}
	if beginCount != 2 || first.rollbacks != 1 || second.rollbacks != 0 {
		t.Fatalf("begin count = %d, rollbacks = (%d, %d)", beginCount, first.rollbacks, second.rollbacks)
	}
	if cache.invalidated != trainRunID {
		t.Fatalf("cache invalidation ID = %s, want %s", cache.invalidated, trainRunID)
	}
}

func TestBeginTrainRunWriteFailsClosedForControlState(t *testing.T) {
	trainRunID := uuid.New()
	route := mustRoute(t, trainRunID, sharding.ShardLegacy, 3)
	tests := []struct {
		name            string
		catalogWrite    bool
		catalogState    string
		assignmentState string
		activeMigration bool
		fenceGeneration int64
		fenceWrite      bool
		want            error
	}{
		{
			name:            "draining assignment",
			catalogWrite:    true,
			catalogState:    "active",
			assignmentState: "draining",
			fenceGeneration: 3,
			fenceWrite:      true,
			want:            sharding.ErrTrainRunMigrating,
		},
		{
			name:            "active migration on stable assignment",
			catalogWrite:    true,
			catalogState:    "active",
			assignmentState: "stable",
			activeMigration: true,
			fenceGeneration: 3,
			fenceWrite:      true,
			want:            sharding.ErrTrainRunMigrating,
		},
		{
			name:            "catalog write disabled",
			catalogState:    "active",
			assignmentState: "stable",
			fenceGeneration: 3,
			fenceWrite:      true,
			want:            sharding.ErrWriteFenced,
		},
		{
			name:            "local fence disabled",
			catalogWrite:    true,
			catalogState:    "active",
			assignmentState: "stable",
			fenceGeneration: 3,
			want:            sharding.ErrWriteFenced,
		},
		{
			name:            "local fence generation mismatch",
			catalogWrite:    true,
			catalogState:    "active",
			assignmentState: "stable",
			fenceGeneration: 4,
			fenceWrite:      true,
			want:            sharding.ErrWriteFenced,
		},
		{
			name:            "degraded catalog",
			catalogWrite:    true,
			catalogState:    "degraded",
			assignmentState: "stable",
			fenceGeneration: 3,
			fenceWrite:      true,
			want:            sharding.ErrShardUnavailable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx := fakeAuthorityTx(
				"legacy",
				3,
				test.catalogWrite,
				test.catalogState,
				test.assignmentState,
				test.activeMigration,
				test.fenceGeneration,
				test.fenceWrite,
			)
			db := &fakeDB{beginTx: func(context.Context, pgx.TxOptions) (pgx.Tx, error) { return tx, nil }}
			router, err := NewRouter(db, nil)
			if err != nil {
				t.Fatal(err)
			}

			got, err := router.BeginTrainRunWrite(context.Background(), route)
			if got != nil || !errors.Is(err, test.want) {
				t.Fatalf("BeginTrainRunWrite() = (%v, %v), want nil, %v", got, err, test.want)
			}
			if tx.rollbacks != 1 {
				t.Fatalf("rollbacks = %d, want 1", tx.rollbacks)
			}
		})
	}
}

func TestBeginTrainRunWriteAllowsAuthoritativeRollbackWindowTarget(t *testing.T) {
	trainRunID := uuid.New()
	route := mustRoute(t, trainRunID, sharding.ShardOne, 12)
	tx := fakeAuthorityTx("shard-1", 12, true, "active", "rollback_window", true, 12, true)
	db := &fakeDB{beginTx: func(context.Context, pgx.TxOptions) (pgx.Tx, error) { return tx, nil }}
	router, err := NewRouter(db, nil)
	if err != nil {
		t.Fatal(err)
	}

	got, err := router.BeginTrainRunWrite(context.Background(), route)
	if err != nil {
		t.Fatalf("BeginTrainRunWrite() error = %v", err)
	}
	if got.PGXTx() != tx || got.Route() != route {
		t.Fatal("rollback-window target was not returned as the fenced authority")
	}
}

type fakeDB struct {
	queryRow func(context.Context, string, ...any) pgx.Row
	query    func(context.Context, string, ...any) (pgx.Rows, error)
	beginTx  func(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type fakeRouteCache struct {
	route       sharding.ShardRoute
	found       bool
	invalidated uuid.UUID
	putErr      error
}

func (cache *fakeRouteCache) Get(uuid.UUID) (sharding.ShardRoute, bool) {
	return cache.route, cache.found
}

func (cache *fakeRouteCache) Put(route sharding.ShardRoute) error {
	cache.route = route
	cache.found = true
	return cache.putErr
}

func (cache *fakeRouteCache) Invalidate(trainRunID uuid.UUID) {
	cache.invalidated = trainRunID
	cache.found = false
}

func (db *fakeDB) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	if db.beginTx == nil {
		panic("unexpected BeginTx call")
	}
	return db.beginTx(ctx, options)
}

func (db *fakeDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if db.query == nil {
		panic("unexpected Query call")
	}
	return db.query(ctx, sql, args...)
}

func (db *fakeDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return db.queryRow(ctx, sql, args...)
}

type fakeRow struct {
	values []any
	err    error
}

type fakeTx struct {
	pgx.Tx
	exec      func(context.Context, string, ...any) (pgconn.CommandTag, error)
	queryRow  func(context.Context, string, ...any) pgx.Row
	commits   int
	rollbacks int
}

func (tx *fakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return tx.exec(ctx, sql, args...)
}

func (tx *fakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return tx.queryRow(ctx, sql, args...)
}

func (tx *fakeTx) Commit(context.Context) error {
	tx.commits++
	return nil
}

func (tx *fakeTx) Rollback(context.Context) error {
	tx.rollbacks++
	return nil
}

type fakeRows struct {
	pgx.Rows
	values [][]any
	index  int
	err    error
}

func (rows *fakeRows) Close() {}

func (rows *fakeRows) Err() error { return rows.err }

func (rows *fakeRows) Next() bool {
	if rows.index+1 >= len(rows.values) {
		return false
	}
	rows.index++
	return true
}

func (rows *fakeRows) Scan(dest ...any) error {
	for index := range dest {
		reflect.ValueOf(dest[index]).Elem().Set(reflect.ValueOf(rows.values[rows.index][index]))
	}
	return nil
}

func (row fakeRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	for index := range dest {
		reflect.ValueOf(dest[index]).Elem().Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}

func mustRoute(t *testing.T, trainRunID uuid.UUID, shardID sharding.ShardID, generationValue int64) sharding.ShardRoute {
	t.Helper()
	generation, err := sharding.NewAssignmentGeneration(generationValue)
	if err != nil {
		t.Fatal(err)
	}
	route, err := sharding.NewShardRoute(trainRunID, shardID, generation)
	if err != nil {
		t.Fatal(err)
	}
	return route
}

func fakeAuthorityTx(
	shardID string,
	generation int64,
	catalogWriteEnabled bool,
	catalogState string,
	assignmentState string,
	hasActiveMigration bool,
	fenceGeneration int64,
	fenceWriteEnabled bool,
) *fakeTx {
	return &fakeTx{
		exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("SET"), nil
		},
		queryRow: func(_ context.Context, sql string, _ ...any) pgx.Row {
			if strings.Contains(sql, "public.train_run_shard_assignments") {
				return fakeRow{values: []any{shardID, generation, true, catalogWriteEnabled, catalogState, sharding.SupportedFencingProtocolVersion, assignmentState, hasActiveMigration}}
			}
			return fakeRow{values: []any{fenceGeneration, fenceWriteEnabled}}
		},
	}
}
