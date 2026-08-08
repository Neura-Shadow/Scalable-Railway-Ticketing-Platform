package controlsource

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestReverseAdapterRejectsPhysicalV2RatherThanDroppingPaymentEvidence(t *testing.T) {
	t.Parallel()

	adapter, err := NewReverse(&fakeDB{}, &fakeDB{}, SourceLegacy)
	if err != nil {
		t.Fatalf("NewReverse() error = %v", err)
	}
	record := physicalmigration.Record{
		MigrationID: uuid.New(), TrainRunID: uuid.New(), ReverseMigration: true,
		SourceShardID: "physical-shard-0", TargetShardID: SourceLegacy,
		SourceGeneration: 7, TargetGeneration: 8,
		SourceProtocolVersion: 1, SourceSchemaVersion: 2,
		TargetProtocolVersion: 1, TargetSchemaVersion: 8,
	}
	if err := adapter.Preflight(context.Background(), record); !errors.Is(err, physicalmigration.ErrCheckpointConflict) {
		t.Fatalf("Preflight() error = %v, want fail-closed checkpoint conflict", err)
	}
}

func TestFareSnapshotUsesDurableControlSourceVersion(t *testing.T) {
	t.Parallel()
	if !strings.Contains(sourceFareSnapshotSQL, "fare.source_version") {
		t.Fatal("fare snapshot does not copy public.fares.source_version")
	}
	if strings.Contains(sourceFareSnapshotSQL, "extract(epoch FROM fare.updated_at)") {
		t.Fatal("fare snapshot still derives a version from wall-clock time")
	}
}

func TestBookingShardFareVersionUniquenessAppliesOnlyToActiveSnapshots(t *testing.T) {
	t.Parallel()
	path := filepath.Join("..", "..", "..", "..", "migrations", "booking-shard", "000001_booking_shard.up.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read booking shard migration: %v", err)
	}
	sql := string(raw)
	if !strings.Contains(sql, "booking_fare_snapshots_active_version_unique_idx") ||
		!strings.Contains(sql, "WHERE active") {
		t.Fatal("inactive historical reservation fares cannot coexist during base copy")
	}
}

func TestNewAcceptsOnlyTheFixedControlSourceIDs(t *testing.T) {
	t.Parallel()

	for _, sourceID := range []string{SourceLegacy, SourceZero, SourceOne} {
		if _, err := New(&fakeDB{}, &fakeDB{}, sourceID); err != nil {
			t.Fatalf("New(%q) error = %v", sourceID, err)
		}
	}
	for _, sourceID := range []string{"", "public", "booking_shard_0", "physical-shard-0", "postgres://host/db"} {
		if _, err := New(&fakeDB{}, &fakeDB{}, sourceID); err == nil {
			t.Fatalf("New(%q) unexpectedly accepted an unapproved source", sourceID)
		}
	}
}

func TestNewReverseAcceptsOnlyFixedControlTargets(t *testing.T) {
	t.Parallel()

	for _, targetID := range []string{SourceLegacy, SourceZero, SourceOne} {
		if _, err := NewReverse(&fakeDB{}, &fakeDB{}, targetID); err != nil {
			t.Fatalf("NewReverse(%q) error = %v", targetID, err)
		}
	}
	for _, targetID := range []string{"", "public", "booking_shard_0", "physical-shard-0", "postgres://host/db"} {
		if _, err := NewReverse(&fakeDB{}, &fakeDB{}, targetID); err == nil {
			t.Fatalf("NewReverse(%q) unexpectedly accepted an unapproved target", targetID)
		}
	}
}

func TestReverseTargetStatementsUseOnlyFixedAllowlistedRelations(t *testing.T) {
	t.Parallel()

	relations := map[string]string{
		SourceLegacy: "public.reservations",
		SourceZero:   "booking_shard_0.reservations",
		SourceOne:    "booking_shard_1.reservations",
	}
	for targetID, relation := range relations {
		statements := []string{
			reverseUpsertSQL(targetID, "reservations"),
			reverseDeleteSQL(targetID, "reservations"),
			reverseCleanupCountSQL(targetID),
			reverseTargetValidationSQL(targetID, "reservations"),
			reverseTargetFencePrepareSQL(targetID),
		}
		for _, statement := range statements {
			if statement == "" || strings.Contains(statement, "%") || strings.Contains(statement, "postgres://") {
				t.Fatalf("target %s statement is not fixed: %s", targetID, statement)
			}
		}
		if !strings.Contains(reverseUpsertSQL(targetID, "reservations"), relation) {
			t.Fatalf("target %s did not select %s", targetID, relation)
		}
	}
}

func TestReversePayloadDropsPhysicalOnlyColumnsAndRebindsOutbox(t *testing.T) {
	t.Parallel()

	record := physicalmigration.Record{
		TrainRunID: uuid.New(), SourceGeneration: 8,
		TargetGeneration: 9, TargetShardID: SourceZero,
	}
	id := uuid.New()
	raw := []byte(`{"id":"` + id.String() + `","train_run_id":"` + record.TrainRunID.String() +
		`","assignment_generation":8,"aggregate_type":"reservation","lease_token":"` + uuid.New().String() +
		`","updated_at":"2026-01-01T00:00:00Z"}`)
	normalized, err := normalizeReversePayload(raw, "outbox_events", record)
	if err != nil {
		t.Fatalf("normalizeReversePayload() error = %v", err)
	}
	text := string(normalized)
	for _, forbidden := range []string{"lease_token", "updated_at"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("normalized payload retained %s: %s", forbidden, text)
		}
	}
	for _, required := range []string{`"shard_id":"shard-0"`, `"assignment_generation":9`} {
		if !strings.Contains(text, required) {
			t.Fatalf("normalized payload omitted %s: %s", required, text)
		}
	}
}

func TestReadBaseBatchUsesTheTransformedBoundaryAndOpaqueCursor(t *testing.T) {
	t.Parallel()

	trainRunID := uuid.New()
	rowID := uuid.New()
	control := &fakeDB{rows: []pgx.Rows{&fakeRows{values: [][]any{{
		rowID, []byte(`{"id":"` + rowID.String() + `","train_run_id":"` + trainRunID.String() + `"}`),
	}}}}}
	adapter, err := New(control, &fakeDB{}, SourceLegacy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	record := physicalmigration.Record{TrainRunID: trainRunID, SourceShardID: SourceLegacy, SourceGeneration: 7}
	batch, err := adapter.ReadBaseBatch(context.Background(), physicalmigration.BaseCopyRequest{Migration: record, Limit: 10})
	if err != nil {
		t.Fatalf("ReadBaseBatch() error = %v", err)
	}
	if batch.ObjectName != "train_run_booking_snapshots" || batch.Rows != 1 ||
		batch.NextCursor != "0:"+rowID.String() || batch.Fingerprint == ([32]byte{}) {
		t.Fatalf("ReadBaseBatch() = %+v", batch)
	}
	if !strings.Contains(control.querySQL[0], "physical_source_entity_id") ||
		strings.Contains(strings.ToLower(control.querySQL[0]), "password") {
		t.Fatalf("unexpected source query: %s", control.querySQL[0])
	}
	if got := control.queryArgs[0][1]; got != SourceLegacy {
		t.Fatalf("source argument = %v, want %s", got, SourceLegacy)
	}
}

func TestSourceFenceStatementsAreFixedAndNeverUseAnIdentifierArgument(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		SourceLegacy: "public.train_run_write_fences",
		SourceZero:   "booking_shard_0.train_run_write_fences",
		SourceOne:    "booking_shard_1.train_run_write_fences",
	}
	for sourceID, relation := range tests {
		for _, statement := range []string{
			sourceFenceReadSQL(sourceID), sourceFenceDisableSQL(sourceID),
			sourceFenceRebindSQL(sourceID), sourceFenceRetainedSQL(sourceID),
		} {
			if !strings.Contains(statement, relation) || strings.Contains(statement, "%") || strings.Contains(statement, "+") {
				t.Fatalf("source %s statement is not fixed: %s", sourceID, statement)
			}
		}
	}
}

func TestControlMigrationDefinesBoundedPayloadFreeTransactionalCapture(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "..", "..", "migrations", "000009_physical_shard_control_plane.up.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(raw)
	for _, fragment := range []string{
		"physical_source_migration_capture_state",
		"physical_source_train_run_mutation_journal",
		"UNIQUE (migration_id, mutation_sequence)",
		"octet_length(primary_key::text) <= 512",
		"append_physical_source_mutation",
		"capture_physical_source_booking_mutation",
		"physical_source_capture_train_run",
		"physical_source_capture_fare",
		"physical_source_capture_seat",
		"booking_shard_0.seat_inventory",
		"booking_shard_1.seat_inventory",
		"physical_control_target_apply_receipts",
		"apply_fingerprint bytea NOT NULL",
		"assigned_state IN ('migrating', 'draining')",
		"draining migration lacks a draining source assignment",
		"draining migration has a stale or duplicate control-local source fence",
		"final validation has a stale or duplicate control-local target fence",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("control migration omitted %q", fragment)
		}
	}
	for _, forbidden := range []string{"EXECUTE format", "row_to_json", "raw_idempotency_key'::"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("control-source capture contains forbidden fragment %q", forbidden)
		}
	}
	captureStart := strings.Index(sql, "CREATE TABLE public.physical_source_train_run_mutation_journal")
	captureEnd := strings.Index(sql[captureStart:], "CREATE INDEX physical_source_mutation_journal_replay_idx")
	if captureStart < 0 || captureEnd < 0 || strings.Contains(sql[captureStart:captureStart+captureEnd], "payload") {
		t.Fatal("control-source journal persists a row payload")
	}
}

func TestControlMigrationPreservesLogicalMigrationZeroWriterInvariant(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "..", "..", "migrations", "000009_physical_shard_control_plane.up.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(raw)
	guardStart := strings.Index(sql, "CREATE OR REPLACE FUNCTION public.assert_train_run_fence_invariant")
	if guardStart < 0 {
		t.Fatal("control migration omitted the train-run fence invariant")
	}
	guardEnd := strings.Index(sql[guardStart:], "\n$$;")
	if guardEnd < 0 {
		t.Fatal("control migration omitted the train-run fence invariant terminator")
	}
	guardSQL := sql[guardStart : guardStart+guardEnd+len("\n$$;")]
	for _, required := range []string{
		"assignment.active_migration_id",
		"FROM public.train_run_shard_migrations AS migration",
		"migration.id = active_migration_id",
		"migration.train_run_id = checked_train_run_id",
		"logical migration must remain in a zero-writer state",
	} {
		if !strings.Contains(guardSQL, required) {
			t.Fatalf("logical migration fence guard omitted %q", required)
		}
	}
}

func TestControlMigrationScopesReverseApplyAuthorizationToTheExactTransaction(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "..", "..", "migrations", "000009_physical_shard_control_plane.up.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(raw)
	tableStart := strings.Index(sql, "CREATE TABLE public.physical_control_target_apply_authorizations")
	if tableStart < 0 {
		t.Fatal("control migration omitted the reverse-apply authorization table")
	}
	tableEnd := strings.Index(sql[tableStart:], "\n);")
	if tableEnd < 0 {
		t.Fatal("control migration omitted the reverse-apply authorization table")
	}
	tableSQL := sql[tableStart : tableStart+tableEnd+len("\n);")]
	for _, required := range []string{
		"migration_id uuid NOT NULL",
		"train_run_id uuid NOT NULL",
		"target_shard_id text NOT NULL",
		"target_generation bigint NOT NULL",
		"transaction_id bigint NOT NULL",
		"PRIMARY KEY (migration_id, transaction_id)",
	} {
		if !strings.Contains(tableSQL, required) {
			t.Fatalf("reverse-apply authorization omitted %q", required)
		}
	}
	for _, forbidden := range []string{"payload", "token", "secret", "dsn", "password"} {
		if strings.Contains(strings.ToLower(tableSQL), forbidden) {
			t.Fatalf("reverse-apply authorization persists forbidden field %q", forbidden)
		}
	}

	guardStart := strings.Index(sql, "CREATE OR REPLACE FUNCTION public.assert_legacy_train_run_writable")
	if guardStart < 0 {
		t.Fatal("control migration omitted the legacy reverse-apply guard")
	}
	guardEnd := strings.Index(sql[guardStart:], "\n$$;")
	if guardEnd < 0 {
		t.Fatal("control migration omitted the legacy reverse-apply guard")
	}
	guardSQL := sql[guardStart : guardStart+guardEnd+len("\n$$;")]
	for _, required := range []string{
		"apply_auth.transaction_id = txid_current()",
		"apply_auth.train_run_id = checked_train_run_id",
		"apply_auth.target_shard_id = 'legacy'",
		"apply_auth.target_generation = migration.target_generation",
		"migration.reverse_migration",
		"migration.source_shard_id IN",
		"migration.target_shard_id = 'legacy'",
		"assignment.shard_id = migration.source_shard_id",
		"assignment.assignment_generation = migration.source_generation",
		"assignment.active_physical_migration_id = migration.migration_id",
		"assignment.assignment_state IN ('migrating', 'draining')",
	} {
		if !strings.Contains(guardSQL, required) {
			t.Fatalf("legacy reverse-apply guard omitted %q", required)
		}
	}
	for _, forbidden := range []string{"current_setting('app.", "set_config(", "session_user"} {
		if strings.Contains(strings.ToLower(guardSQL), forbidden) {
			t.Fatalf("legacy reverse-apply guard uses session capability %q", forbidden)
		}
	}
}

type fakeDB struct {
	rows      []pgx.Rows
	querySQL  []string
	queryArgs [][]any
}

func (*fakeDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	panic("unexpected BeginTx")
}
func (db *fakeDB) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	db.querySQL = append(db.querySQL, sql)
	db.queryArgs = append(db.queryArgs, args)
	if len(db.rows) == 0 {
		panic("unexpected Query")
	}
	rows := db.rows[0]
	db.rows = db.rows[1:]
	return rows, nil
}
func (*fakeDB) QueryRow(context.Context, string, ...any) pgx.Row {
	panic("unexpected QueryRow")
}

type fakeRows struct {
	pgx.Rows
	values [][]any
	index  int
}

func (rows *fakeRows) Next() bool { return rows.index < len(rows.values) }
func (rows *fakeRows) Scan(destinations ...any) error {
	values := rows.values[rows.index]
	rows.index++
	for index, value := range values {
		reflect.ValueOf(destinations[index]).Elem().Set(reflect.ValueOf(value))
	}
	return nil
}
func (*fakeRows) Err() error { return nil }
func (*fakeRows) Close()     {}
