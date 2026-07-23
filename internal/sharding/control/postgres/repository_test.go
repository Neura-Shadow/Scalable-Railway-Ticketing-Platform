package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestWithinTransactionCommitsOnlyAfterSuccessfulCallback(t *testing.T) {
	pgxTx := &fakePGXTx{}
	db := &fakeDB{beginTx: func(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
		if options.IsoLevel != pgx.ReadCommitted {
			t.Fatalf("isolation = %q, want read committed", options.IsoLevel)
		}
		return pgxTx, nil
	}}
	repository, err := NewRepository(db)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	called := false
	err = repository.WithinTransaction(context.Background(), func(_ context.Context, tx control.Transaction) error {
		called = true
		if tx == nil {
			t.Fatal("callback transaction is nil")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithinTransaction() error = %v", err)
	}
	if !called || pgxTx.commits != 1 || pgxTx.rollbacks != 0 {
		t.Fatalf("called=%t commits=%d rollbacks=%d", called, pgxTx.commits, pgxTx.rollbacks)
	}
}

func TestWithinTransactionRollsBackCallbackFailure(t *testing.T) {
	pgxTx := &fakePGXTx{}
	db := &fakeDB{beginTx: func(context.Context, pgx.TxOptions) (pgx.Tx, error) { return pgxTx, nil }}
	repository, err := NewRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("bounded callback failure")

	err = repository.WithinTransaction(context.Background(), func(context.Context, control.Transaction) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("WithinTransaction() error = %v, want %v", err, want)
	}
	if pgxTx.commits != 0 || pgxTx.rollbacks != 1 {
		t.Fatalf("commits=%d rollbacks=%d", pgxTx.commits, pgxTx.rollbacks)
	}
}

func TestFindMigrationForUpdateReconstructsDurableRecord(t *testing.T) {
	migrationID, trainRunID := uuid.New(), uuid.New()
	createdAt := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	updatedAt := createdAt.Add(time.Minute)
	cutoverAt := updatedAt.Add(time.Minute)
	deadline := cutoverAt.Add(5 * time.Minute)
	validation := []byte(`{"Snapshot":{"Source":{"Tables":[{"Name":"reservations","Rows":1,"Checksum":"abc"}]},"Target":{"Tables":[{"Name":"reservations","Rows":1,"Checksum":"abc"}]},"InvariantViolations":0,"MissingReservationLocators":0,"MissingTicketOrderLocators":0,"MissingTicketLocators":0,"RowsExamined":2,"Truncated":false},"Passed":true,"CheckedAt":"2026-07-23T12:01:00Z"}`)
	pgxTx := &fakePGXTx{queryRow: func(_ context.Context, sql string, args ...any) pgx.Row {
		if len(args) != 1 || args[0] != migrationID || !strings.Contains(sql, "FOR UPDATE") {
			t.Fatalf("FindMigrationForUpdate query=%q args=%#v", sql, args)
		}
		return fakeRow{values: []any{
			migrationID, trainRunID, "legacy", "shard-0", int64(1), int64(2), int64(300),
			"rollback_window", "complete", int64(9), true,
			pgtype.Int8{Int64: 7, Valid: true}, validation,
			createdAt, updatedAt,
			pgtype.Timestamptz{Time: cutoverAt, Valid: true},
			pgtype.Timestamptz{Time: deadline, Valid: true},
		}}
	}}
	tx := &Transaction{tx: pgxTx}

	got, found, err := tx.FindMigrationForUpdate(context.Background(), migrationID)
	if err != nil {
		t.Fatalf("FindMigrationForUpdate() error = %v", err)
	}
	if !found {
		t.Fatal("FindMigrationForUpdate() found = false")
	}
	if got.MigrationID != migrationID || got.TrainRunID != trainRunID || got.SourceShard != sharding.ShardLegacy ||
		got.TargetShard != sharding.ShardZero || got.SourceGeneration.Int64() != 1 || got.TargetGeneration.Int64() != 2 ||
		got.RollbackWindow != 5*time.Minute || got.State != migration.StateRollbackWindow || got.Checkpoint != "complete" ||
		got.CopiedRows != 9 || !got.CopyComplete || got.RollbackGeneration == nil || got.RollbackGeneration.Int64() != 7 ||
		got.LastValidation == nil || !got.LastValidation.Passed || got.CutoverAt == nil || !got.CutoverAt.Equal(cutoverAt) ||
		got.RollbackDeadline == nil || !got.RollbackDeadline.Equal(deadline) {
		t.Fatalf("FindMigrationForUpdate() = %+v", got)
	}
}

func TestInsertMigrationPersistsPlanAndClaimsAssignment(t *testing.T) {
	migrationID, trainRunID := uuid.New(), uuid.New()
	createdAt := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	record := control.Record{
		MigrationID:      migrationID,
		TrainRunID:       trainRunID,
		SourceShard:      sharding.ShardLegacy,
		TargetShard:      sharding.ShardOne,
		SourceGeneration: mustGeneration(t, 1),
		TargetGeneration: mustGeneration(t, 2),
		RollbackWindow:   5 * time.Minute,
		State:            migration.StatePlanned,
		CreatedAt:        createdAt,
		UpdatedAt:        createdAt,
	}
	var statements []string
	pgxTx := &fakePGXTx{exec: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
		statements = append(statements, sql)
		if strings.Contains(sql, "INSERT INTO public.train_run_shard_migrations") {
			if args[0] != migrationID || args[1] != trainRunID || args[2] != "legacy" || args[3] != "shard-1" {
				t.Fatalf("migration insert args = %#v", args)
			}
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		}
		if strings.Contains(sql, "UPDATE public.train_run_shard_assignments") {
			if args[0] != trainRunID || args[1] != migrationID || args[2] != "legacy" || args[3] != int64(1) {
				t.Fatalf("assignment claim args = %#v", args)
			}
			return pgconn.NewCommandTag("UPDATE 1"), nil
		}
		t.Fatalf("unexpected statement %q", sql)
		return pgconn.CommandTag{}, nil
	}}
	tx := &Transaction{tx: pgxTx}

	if err := tx.InsertMigration(context.Background(), record); err != nil {
		t.Fatalf("InsertMigration() error = %v", err)
	}
	if len(statements) != 2 || !strings.Contains(statements[1], "assignment_state = 'draining'") {
		t.Fatalf("statements = %#v", statements)
	}
}

func TestSaveMigrationBridgesCollapsedServiceTransitions(t *testing.T) {
	migrationID, trainRunID := uuid.New(), uuid.New()
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	record := control.Record{
		MigrationID: migrationID, TrainRunID: trainRunID,
		SourceShard: sharding.ShardLegacy, TargetShard: sharding.ShardZero,
		SourceGeneration: mustGeneration(t, 1), TargetGeneration: mustGeneration(t, 2),
		RollbackWindow: 5 * time.Minute, State: migration.StateValidating,
		Checkpoint: "complete", CopyComplete: true, CreatedAt: now, UpdatedAt: now,
	}
	var states []string
	pgxTx := &fakePGXTx{
		queryRow: func(_ context.Context, sql string, _ ...any) pgx.Row {
			if !strings.Contains(sql, "FOR UPDATE") {
				t.Fatalf("query = %q", sql)
			}
			return fakeRow{values: []any{"planned"}}
		},
		exec: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			if strings.Contains(sql, "SET state = $2") {
				states = append(states, args[1].(string))
			}
			if strings.Contains(sql, "SET state = $3") {
				states = append(states, args[2].(string))
			}
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
	if err := (&Transaction{tx: pgxTx}).SaveMigration(context.Background(), record); err != nil {
		t.Fatalf("SaveMigration() error = %v", err)
	}
	want := []string{"draining", "copying", "validating"}
	if !reflect.DeepEqual(states, want) {
		t.Fatalf("states = %v, want %v", states, want)
	}
}

func TestFenceQueriesUseOnlyFixedAllowlistedSchema(t *testing.T) {
	trainRunID := uuid.New()
	route, err := sharding.NewShardRoute(trainRunID, sharding.ShardOne, mustGeneration(t, 9))
	if err != nil {
		t.Fatal(err)
	}
	pgxTx := &fakePGXTx{queryRow: func(_ context.Context, sql string, args ...any) pgx.Row {
		if !strings.Contains(sql, "FROM booking_shard_1.train_run_write_fences") || !strings.Contains(sql, "FOR UPDATE") {
			t.Fatalf("query = %q", sql)
		}
		return fakeRow{values: []any{int64(9), true}}
	}}
	enabled, err := (&Transaction{tx: pgxTx}).WriteFenceEnabledForUpdate(context.Background(), route)
	if err != nil || !enabled {
		t.Fatalf("enabled=%t error=%v", enabled, err)
	}

	badRoute, _ := sharding.NewShardRoute(trainRunID, sharding.ShardID("public;DROP SCHEMA public"), mustGeneration(t, 9))
	if _, err := (&Transaction{tx: pgxTx}).WriteFenceEnabledForUpdate(context.Background(), badRoute); !errors.Is(err, control.ErrInvalidInput) {
		t.Fatalf("invalid shard error = %v", err)
	}
}

func TestRequireShardWritableForUpdateLocksCatalogAndFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name            string
		enabled         bool
		writeEnabled    bool
		state           string
		minimumProtocol int32
		wantErr         error
	}{
		{name: "eligible", enabled: true, writeEnabled: true, state: "active", minimumProtocol: sharding.SupportedFencingProtocolVersion},
		{name: "disabled", writeEnabled: true, state: "active", minimumProtocol: sharding.SupportedFencingProtocolVersion, wantErr: control.ErrShardNotWritable},
		{name: "write disabled", enabled: true, state: "active", minimumProtocol: sharding.SupportedFencingProtocolVersion, wantErr: control.ErrShardNotWritable},
		{name: "degraded", enabled: true, writeEnabled: true, state: "degraded", minimumProtocol: sharding.SupportedFencingProtocolVersion, wantErr: control.ErrShardNotWritable},
		{name: "unsupported fencing protocol", enabled: true, writeEnabled: true, state: "active", minimumProtocol: sharding.SupportedFencingProtocolVersion + 1, wantErr: control.ErrShardNotWritable},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			pgxTx := &fakePGXTx{queryRow: func(_ context.Context, sql string, arguments ...any) pgx.Row {
				if !strings.Contains(sql, "FROM public.booking_shards") || !strings.Contains(sql, "FOR UPDATE") ||
					len(arguments) != 1 || arguments[0] != "shard-0" {
					t.Fatalf("catalog eligibility query=%q args=%#v", sql, arguments)
				}
				return fakeRow{values: []any{test.enabled, test.writeEnabled, test.state, test.minimumProtocol}}
			}}
			err := (&Transaction{tx: pgxTx}).RequireShardWritableForUpdate(context.Background(), sharding.ShardZero)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("RequireShardWritableForUpdate() error=%v want=%v", err, test.wantErr)
			}
		})
	}

	if err := (&Transaction{tx: &fakePGXTx{}}).RequireShardWritableForUpdate(
		context.Background(), sharding.ShardID("untrusted"),
	); !errors.Is(err, control.ErrInvalidInput) {
		t.Fatalf("untrusted shard error=%v", err)
	}
}

func TestCopyBatchUsesDeterministicInventoryCheckpointWithoutOutbox(t *testing.T) {
	migrationID, trainRunID, seatID := uuid.New(), uuid.New(), uuid.New()
	source, _ := sharding.NewShardRoute(trainRunID, sharding.ShardLegacy, mustGeneration(t, 1))
	target, _ := sharding.NewShardRoute(trainRunID, sharding.ShardZero, mustGeneration(t, 2))
	var copySQL string
	pgxTx := &fakePGXTx{
		queryRow: func(_ context.Context, sql string, args ...any) pgx.Row {
			copySQL = sql
			if !strings.Contains(sql, "FROM public.seat_inventory AS s") ||
				!strings.Contains(sql, "INSERT INTO booking_shard_0.seat_inventory") ||
				!strings.Contains(sql, "ORDER BY s.seat_id") || strings.Contains(strings.ToLower(sql), "outbox") {
				t.Fatalf("copy query = %q", sql)
			}
			if args[0] != trainRunID || args[2] != 25 {
				t.Fatalf("copy args = %#v", args)
			}
			return fakeRow{values: []any{int64(1), pgtype.UUID{Bytes: seatID, Valid: true}, true}}
		},
		exec: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			if strings.Contains(sql, "set_config") {
				if args[0] != migrationID.String() {
					t.Fatalf("guard args = %#v", args)
				}
				return pgconn.NewCommandTag("SELECT 1"), nil
			}
			if !strings.Contains(sql, "inventory_rows_copied") {
				t.Fatalf("audit query = %q", sql)
			}
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
	result, err := (&Transaction{tx: pgxTx}).CopyBatch(context.Background(), control.CopyBatchRequest{
		MigrationID: migrationID, TrainRunID: trainRunID, Source: source, Target: target, Limit: 25,
	})
	if err != nil {
		t.Fatalf("CopyBatch() error = %v", err)
	}
	if result.RowsCopied != 1 || result.Done || result.NextCheckpoint != "inventory:"+seatID.String() {
		t.Fatalf("CopyBatch() = %+v", result)
	}
	if copySQL == "" {
		t.Fatal("copy query was not executed")
	}
}

func TestParseCopyCheckpointRejectsUntrustedPhase(t *testing.T) {
	if _, _, err := parseCopyCheckpoint("public.outbox_events:" + uuid.NewString()); !errors.Is(err, control.ErrInvalidInput) {
		t.Fatalf("parseCopyCheckpoint() error = %v", err)
	}
}

func TestValidateStreamsCanonicalRowsWithinOneTotalCap(t *testing.T) {
	migrationID, trainRunID := uuid.New(), uuid.New()
	source, _ := sharding.NewShardRoute(trainRunID, sharding.ShardLegacy, mustGeneration(t, 1))
	target, _ := sharding.NewShardRoute(trainRunID, sharding.ShardZero, mustGeneration(t, 2))
	queryCount := 0
	pgxTx := &fakePGXTx{
		query: func(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
			queryCount++
			if !strings.Contains(sql, "row_to_json(scoped)::text") || args[0] != trainRunID {
				t.Fatalf("digest query=%q args=%#v", sql, args)
			}
			return newFakeRows([][]any{{`{"stable":"row"}`}}), nil
		},
		queryRow: func(_ context.Context, sql string, args ...any) pgx.Row {
			if !strings.Contains(sql, "reservation_seat_violations") || args[1] != "legacy" || args[2] != int64(1) {
				t.Fatalf("invariant query=%q args=%#v", sql, args)
			}
			return fakeRow{values: []any{int64(0), int64(0), int64(0), int64(0)}}
		},
	}
	snapshot, err := (&Transaction{tx: pgxTx}).Validate(context.Background(), control.ValidationRequest{
		MigrationID: migrationID, TrainRunID: trainRunID, Source: source, Target: target, RowCap: 12,
	})
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if snapshot.Truncated || snapshot.RowsExamined != 12 || queryCount != 12 ||
		!reflect.DeepEqual(snapshot.Source, snapshot.Target) || len(snapshot.Source.Tables) != 6 {
		t.Fatalf("Validate() = %+v queryCount=%d", snapshot, queryCount)
	}
}

func TestActivateRouteUpdatesProvenanceAndAppendsCutoverEvent(t *testing.T) {
	migrationID, trainRunID := uuid.New(), uuid.New()
	expected, _ := sharding.NewShardRoute(trainRunID, sharding.ShardLegacy, mustGeneration(t, 1))
	next, _ := sharding.NewShardRoute(trainRunID, sharding.ShardZero, mustGeneration(t, 2))
	queryRows := 0
	var statements []string
	pgxTx := &fakePGXTx{
		queryRow: func(_ context.Context, sql string, _ ...any) pgx.Row {
			queryRows++
			if queryRows == 1 {
				return fakeRow{values: []any{migrationID, "legacy", "shard-0", int64(1), int64(2)}}
			}
			if !strings.Contains(sql, "booking_idempotency_key_claims") {
				t.Fatalf("stale query = %q", sql)
			}
			return fakeRow{values: []any{false}}
		},
		exec: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			statements = append(statements, sql)
			if strings.Contains(sql, "INSERT INTO public.outbox_events") {
				if args[0] != trainRunID || args[1] != "shard_cutover" || args[2] != "shard-0" || args[3] != int64(2) {
					t.Fatalf("outbox args = %#v", args)
				}
				if !strings.Contains(sql, "'trainRunId'") || !strings.Contains(sql, "'reason'") {
					t.Fatalf("outbox sql = %q", sql)
				}
			}
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
	if err := (&Transaction{tx: pgxTx}).ActivateRoute(context.Background(), expected, next); err != nil {
		t.Fatalf("ActivateRoute() error = %v", err)
	}
	joined := strings.Join(statements, "\n")
	for _, required := range []string{"reservation_shard_locators", "ticket_order_shard_locators", "ticket_shard_locators", "booking_idempotency_key_claims", "train_run_shard_assignments", "train_run_generation_writes", "outbox_events"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("missing %s in statements", required)
		}
	}
}

type fakeDB struct {
	beginTx func(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

func (db *fakeDB) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	return db.beginTx(ctx, options)
}

type fakePGXTx struct {
	pgx.Tx
	commits   int
	rollbacks int
	queryRow  func(context.Context, string, ...any) pgx.Row
	query     func(context.Context, string, ...any) (pgx.Rows, error)
	exec      func(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func (tx *fakePGXTx) Commit(context.Context) error {
	tx.commits++
	return nil
}

func (tx *fakePGXTx) Rollback(context.Context) error {
	tx.rollbacks++
	return nil
}

func (tx *fakePGXTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return tx.queryRow(ctx, sql, args...)
}

func (tx *fakePGXTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return tx.query(ctx, sql, args...)
}

func (tx *fakePGXTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return tx.exec(ctx, sql, args...)
}

type fakeRow struct {
	values []any
	err    error
}

type fakeRows struct {
	pgx.Rows
	values [][]any
	index  int
	err    error
}

func newFakeRows(values [][]any) *fakeRows { return &fakeRows{values: values, index: -1} }

func (rows *fakeRows) Next() bool {
	rows.index++
	return rows.index < len(rows.values)
}

func (rows *fakeRows) Scan(dest ...any) error {
	return fakeRow{values: rows.values[rows.index]}.Scan(dest...)
}

func (rows *fakeRows) Err() error { return rows.err }
func (rows *fakeRows) Close()     {}

func (row fakeRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	for index := range dest {
		value := reflect.ValueOf(dest[index]).Elem()
		if row.values[index] == nil {
			value.SetZero()
			continue
		}
		value.Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}

func mustGeneration(t *testing.T, value int64) sharding.AssignmentGeneration {
	t.Helper()
	generation, err := sharding.NewAssignmentGeneration(value)
	if err != nil {
		t.Fatal(err)
	}
	return generation
}
