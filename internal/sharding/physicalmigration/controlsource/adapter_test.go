package controlsource

import (
	"context"
	"encoding/json"
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

func TestReverseAdapterRejectsPhysicalV1RatherThanGuessingItsShape(t *testing.T) {
	t.Parallel()

	adapter, err := NewReverse(&fakeDB{}, &fakeDB{}, SourceLegacy)
	if err != nil {
		t.Fatalf("NewReverse() error = %v", err)
	}
	record := physicalmigration.Record{
		MigrationID: uuid.New(), TrainRunID: uuid.New(), ReverseMigration: true,
		SourceShardID: "physical-shard-0", TargetShardID: SourceLegacy,
		SourceGeneration: 7, TargetGeneration: 8,
		SourceProtocolVersion: 1, SourceSchemaVersion: 1,
		TargetProtocolVersion: 1, TargetSchemaVersion: 8,
	}
	if err := adapter.Preflight(context.Background(), record); !errors.Is(err, physicalmigration.ErrCheckpointConflict) {
		t.Fatalf("Preflight() error = %v, want fail-closed checkpoint conflict", err)
	}
}

func TestV3CopyAndReversePreservePaymentFieldsAndReceiptTables(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		"reservation.payment_intent_id", "reservation.payment_amount_minor",
		"reservation.payment_currency", "reservation.payment_grace_expires_at",
		"orders.payment_intent_id", "orders.authorized_amount_minor",
		"orders.captured_amount_minor", "orders.refunded_amount_minor",
	} {
		if !strings.Contains(sourceReservationSQL+sourceTicketOrderSQL, fragment) {
			t.Fatalf("control source transform omitted %q", fragment)
		}
	}
	for _, table := range []string{
		"booking_command_receipts", "payment_command_receipts",
		"ticket_issuance_receipts", "payment_refund_receipts",
		"payment_compensation_receipts", "ticket_refund_compensation_receipts",
		"ticket_refund_prepare_receipts",
		"selected_ticket_refund_receipts",
	} {
		if _, ok := sourceQuery(table); !ok {
			t.Fatalf("sourceQuery(%q) is not supported", table)
		}
		if _, ok := targetQuery(table); !ok {
			t.Fatalf("targetQuery(%q) is not supported", table)
		}
		if !reverseSupportedTable(table) || reverseUpsertSQL(SourceLegacy, table) == "" ||
			reverseDeleteSQL(SourceLegacy, table) == "" {
			t.Fatalf("reverse v3 relation %q is incomplete", table)
		}
	}
	for _, fragment := range []string{
		"payment_intent_id=EXCLUDED.payment_intent_id",
		"authorized_amount_minor=EXCLUDED.authorized_amount_minor",
		"captured_amount_minor=EXCLUDED.captured_amount_minor",
		"refunded_amount_minor=EXCLUDED.refunded_amount_minor",
	} {
		if !strings.Contains(reverseLegacyReservationUpsertSQL+reverseLegacyTicketOrderUpsertSQL, fragment) {
			t.Fatalf("reverse upsert omitted %q", fragment)
		}
	}
}

func TestForwardV3PreflightRequiresControlV11AndPartialRefundCaptureReadiness(t *testing.T) {
	t.Parallel()

	control := &fakeDB{rowResults: []pgx.Row{
		fakeRow{values: []any{true}}, fakeRow{values: []any{true}},
	}}
	target := &fakeDB{rowResults: []pgx.Row{fakeRow{values: []any{true}}}}
	adapter, err := New(control, target, SourceLegacy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	record := physicalmigration.Record{
		MigrationID: uuid.New(), TrainRunID: uuid.New(),
		SourceShardID: SourceLegacy, TargetShardID: "physical-shard-0",
		SourceGeneration: 7, TargetGeneration: 8,
		SourceProtocolVersion: 1, SourceSchemaVersion: 8,
		TargetProtocolVersion: 1, TargetSchemaVersion: 3,
	}
	if err := adapter.Preflight(context.Background(), record); err != nil {
		t.Fatalf("Preflight() error = %v", err)
	}
	joinedControl := strings.Join(control.queryRowSQL, "\n")
	for _, fragment := range []string{
		"version=11 AND NOT dirty",
		"regional_write_authority", "state='active'", "writes_enabled",
		"physical_source_ticket_refund_compensation_receipt_rows",
		"physical_source_selected_ticket_refund_receipt_rows",
		"guard_control_ticket_refund_evidence_mutation",
		"ticket_refund_prepare_receipts_guard_evidence",
		"ticket_refund_compensation_receipts_guard_evidence",
		"selected_ticket_refund_receipts_guard_evidence",
		"ticket_refund_compensation_receipts",
		"selected_ticket_refund_receipts",
	} {
		if !strings.Contains(joinedControl, fragment) {
			t.Fatalf("control-source preflight omitted %q: %s", fragment, joinedControl)
		}
	}
	joinedTarget := strings.Join(target.queryRowSQL, "\n")
	for _, fragment := range []string{
		"version = 3", "regional_write_authority", "state = 'active'", "writes_enabled",
		"migration_evidence_mutation_authorizations",
		"ticket_refund_compensation_receipts_capture_mutation",
		"selected_ticket_refund_receipts_capture_mutation",
	} {
		if !strings.Contains(joinedTarget, fragment) {
			t.Fatalf("physical-target preflight omitted %q: %s", fragment, joinedTarget)
		}
	}
}

func TestReverseV3PreflightRequiresSourceAndTargetReceiptCaptureReadiness(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		"version=3 AND NOT dirty",
		"regional_write_authority", "state='active'", "writes_enabled",
		"migration_evidence_mutation_authorizations",
		"payment_command_receipts_capture_mutation",
		"ticket_issuance_receipts_capture_mutation",
		"payment_refund_receipts_capture_mutation",
		"payment_compensation_receipts_capture_mutation",
		"ticket_refund_prepare_receipts_capture_mutation",
		"ticket_refund_compensation_receipts_capture_mutation",
		"selected_ticket_refund_receipts_capture_mutation",
	} {
		if !strings.Contains(reverseSourcePreflightSQL, fragment) {
			t.Fatalf("reverse source preflight omitted %q", fragment)
		}
	}
	for _, fragment := range []string{
		"version=11 AND NOT dirty",
		"regional_write_authority", "state='active'", "writes_enabled",
		"public.guard_control_booking_receipt_write()",
		"public.capture_physical_source_receipt_mutation()",
		"public.guard_control_ticket_refund_evidence_mutation()",
		"ticket_refund_prepare_receipts_guard_evidence",
		"ticket_refund_compensation_receipts_guard_evidence",
		"selected_ticket_refund_receipts_guard_evidence",
		"physical_target_write_guard",
		"physical_source_capture",
		"reservations_guard_payment_snapshot",
		"ticket_orders_guard_payment_snapshot",
	} {
		if !strings.Contains(reverseTargetPreflightSQL, fragment) {
			t.Fatalf("reverse target preflight omitted %q", fragment)
		}
	}
	for _, schema := range []string{"public", "booking_shard_0", "booking_shard_1"} {
		for _, table := range []string{
			"booking_command_receipts", "payment_command_receipts",
			"ticket_issuance_receipts", "payment_refund_receipts",
			"payment_compensation_receipts", "ticket_refund_prepare_receipts", "ticket_refund_compensation_receipts",
			"selected_ticket_refund_receipts",
		} {
			if !strings.Contains(reverseTargetPreflightSQL, schema+"."+table) {
				t.Fatalf("reverse target preflight omitted %s.%s", schema, table)
			}
		}
	}
}

func TestReverseV3ValidationAccountsForEveryPhysicalTable(t *testing.T) {
	t.Parallel()

	if len(tableOrder) != 18 {
		t.Fatalf("physical table contract has %d tables, want 18", len(tableOrder))
	}
	sourceRows := make([]pgx.Rows, len(tableOrder))
	targetRows := make([]pgx.Rows, len(tableOrder))
	for index, table := range tableOrder {
		sourceRows[index] = &fakeRows{}
		targetRows[index] = &fakeRows{}
		if reverseSupportedTable(table) == reverseIgnoredTable(table) {
			t.Fatalf("table %q must be either copied or derived, but not both", table)
		}
	}
	adapter, err := NewReverse(&fakeDB{rows: targetRows}, &fakeDB{rows: sourceRows}, SourceOne)
	if err != nil {
		t.Fatalf("NewReverse() error = %v", err)
	}
	result, err := adapter.Validate(context.Background(), physicalmigration.ValidationRequest{
		Migration: physicalmigration.Record{
			TrainRunID: uuid.New(), SourceGeneration: 7, TargetGeneration: 8,
			TargetShardID: SourceOne,
		},
		MaxRows: 1, MaxTables: len(tableOrder),
	})
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if !result.Passed || result.Truncated || result.Tables != len(tableOrder) {
		t.Fatalf("Validate() = %+v, want all 17 tables passed", result)
	}
}

func TestReverseDerivedValidationUsesTheStillAuthoritativeSourceAssignment(t *testing.T) {
	t.Parallel()

	rowID := uuid.New()
	control := &fakeDB{rows: []pgx.Rows{&fakeRows{values: [][]any{{
		rowID, []byte(`{"id":"` + rowID.String() + `"}`),
	}}}}}
	adapter, err := NewReverse(control, &fakeDB{}, SourceLegacy)
	if err != nil {
		t.Fatalf("NewReverse() error = %v", err)
	}
	record := physicalmigration.Record{
		TrainRunID: uuid.New(), SourceShardID: "physical-shard-0", TargetShardID: SourceLegacy,
		SourceGeneration: 7, TargetGeneration: 8,
	}
	if _, err := adapter.reverseDerivedTargetRows(context.Background(), record,
		"train_run_booking_snapshots", 2); err != nil {
		t.Fatalf("reverseDerivedTargetRows() error = %v", err)
	}
	if len(control.queryArgs) != 1 || len(control.queryArgs[0]) != 6 ||
		control.queryArgs[0][1] != record.SourceShardID ||
		control.queryArgs[0][2] != record.SourceGeneration {
		t.Fatalf("derived validation args = %#v, want current source assignment", control.queryArgs)
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

func TestReverseV3CleanupRemovesSelectedTicketEvidenceInForeignKeyOrder(t *testing.T) {
	t.Parallel()

	for _, targetID := range []string{SourceLegacy, SourceZero, SourceOne} {
		countSQL := reverseCleanupCountSQL(targetID)
		statements := strings.Join(reverseCleanupStatements(targetID), "\n")
		for _, table := range []string{
			"selected_ticket_refund_receipts", "ticket_refund_compensation_receipts",
		} {
			if !strings.Contains(countSQL, table) {
				t.Fatalf("target %s cleanup count omits %s", targetID, table)
			}
			if !strings.Contains(statements, "DELETE FROM ") || !strings.Contains(statements, table) {
				t.Fatalf("target %s cleanup omits %s: %s", targetID, table, statements)
			}
		}
		selected := strings.Index(statements, "selected_ticket_refund_receipts")
		compensation := strings.Index(statements, "ticket_refund_compensation_receipts")
		tickets := strings.Index(statements, ".tickets")
		if selected < 0 || compensation <= selected || tickets <= compensation {
			t.Fatalf("target %s unsafe v3 cleanup order: %s", targetID, statements)
		}
	}
}

func TestReverseV3RetriesImmutableEvidenceWithoutUpdatingIt(t *testing.T) {
	t.Parallel()

	for _, targetID := range []string{SourceLegacy, SourceZero, SourceOne} {
		for _, table := range []string{
			"ticket_refund_compensation_receipts", "selected_ticket_refund_receipts",
		} {
			sql := reverseUpsertSQL(targetID, table)
			if !strings.Contains(sql, "ON CONFLICT (id) DO NOTHING") || strings.Contains(sql, "DO UPDATE") {
				t.Fatalf("target %s immutable %s retry can mutate evidence: %s", targetID, table, sql)
			}
		}
	}
}

func TestReverseV3ReplaysPrepareResolutionAfterBaseCopy(t *testing.T) {
	t.Parallel()
	for _, targetID := range []string{SourceLegacy, SourceZero, SourceOne} {
		sql := reverseUpsertSQL(targetID, "ticket_refund_prepare_receipts")
		for _, fragment := range []string{
			"state=EXCLUDED.state", "resolved_at=EXCLUDED.resolved_at",
			".state='prepared'", "EXCLUDED.state IN ('released','applied')",
			"IS DISTINCT FROM", ".request_fingerprint",
		} {
			if !strings.Contains(sql, fragment) {
				t.Fatalf("target %s prepare merge omits %q: %s", targetID, fragment, sql)
			}
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

func TestReversePrepareReceiptRebindsTargetGeneration(t *testing.T) {
	t.Parallel()

	record := physicalmigration.Record{
		TrainRunID: uuid.New(), SourceGeneration: 8,
		TargetGeneration: 9, TargetShardID: SourceLegacy,
	}
	id := uuid.New()
	raw := []byte(`{"id":"` + id.String() + `","train_run_id":"` + record.TrainRunID.String() +
		`","assignment_generation":8}`)
	normalized, err := normalizeReversePayload(raw, "ticket_refund_prepare_receipts", record)
	if err != nil {
		t.Fatalf("normalizeReversePayload() error = %v", err)
	}
	var payload struct {
		AssignmentGeneration int64 `json:"assignment_generation"`
	}
	if err := json.Unmarshal(normalized, &payload); err != nil {
		t.Fatalf("decode normalized payload: %v", err)
	}
	if payload.AssignmentGeneration != record.TargetGeneration {
		t.Fatalf("assignment_generation = %d, want target generation %d", payload.AssignmentGeneration, record.TargetGeneration)
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

func TestV11ControlRefundReceiptEvidenceRequiresExactCleanupAuthorization(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "..", "..", "migrations", "000011_payment_ops_dr.up.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(raw)
	for _, fragment := range []string{
		"guard_control_ticket_refund_evidence_mutation",
		"apply_auth.transaction_id = txid_current()",
		"apply_auth.train_run_id = (source_row ->> 'train_run_id')::uuid",
		"apply_auth.target_generation = migration.target_generation",
		"migration.reverse_migration",
		"ticket_refund_prepare_receipts_guard_evidence",
		"ticket_refund_compensation_receipts_guard_evidence",
		"selected_ticket_refund_receipts_guard_evidence",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("v11 refund evidence guard omitted %q", fragment)
		}
	}
}

func TestV11AllowsAReleasedFailedTicketToBeSelectedByANewRefundRequest(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "..", "..", "migrations", "000011_payment_ops_dr.up.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(raw)
	start := strings.Index(sql, "CREATE TABLE public.ticket_refund_request_items")
	end := strings.Index(sql[start:], "CREATE TABLE public.ticket_refund_sagas")
	if start < 0 || end < 0 {
		t.Fatal("v11 refund request-item schema is incomplete")
	}
	itemSQL := sql[start : start+end]
	if strings.Contains(itemSQL, "UNIQUE (ticket_id)") ||
		!strings.Contains(itemSQL, "ticket_refund_request_items_active_ticket_idx") ||
		!strings.Contains(itemSQL, "WHERE state <> 'failed'") {
		t.Fatalf("v11 request-item uniqueness does not release failed selections: %s", itemSQL)
	}
}

func TestV3MarksPopulatedSnapshotsUnverifiedUntilExactDepartureRematerialization(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "..", "..", "migrations", "booking-shard", "000003_payment_ops_dr.up.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(raw)
	for _, fragment := range []string{
		"scheduled_departure_at timestamptz NOT NULL",
		"DEFAULT '-infinity'::timestamptz",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("v3 populated-snapshot marker omitted %q", fragment)
		}
	}
	for _, preflight := range []string{reverseSourcePreflightSQL} {
		if !strings.Contains(preflight, "isfinite(scheduled_departure_at)") {
			t.Fatal("physical source readiness does not require an exact rematerialized departure")
		}
	}
}

type fakeDB struct {
	rows        []pgx.Rows
	querySQL    []string
	queryArgs   [][]any
	rowResults  []pgx.Row
	queryRowSQL []string
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
func (db *fakeDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	db.queryRowSQL = append(db.queryRowSQL, sql)
	if len(db.rowResults) == 0 {
		panic("unexpected QueryRow")
	}
	row := db.rowResults[0]
	db.rowResults = db.rowResults[1:]
	return row
}

type fakeRow struct{ values []any }

func (row fakeRow) Scan(destinations ...any) error {
	for index, value := range row.values {
		reflect.ValueOf(destinations[index]).Elem().Set(reflect.ValueOf(value))
	}
	return nil
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
