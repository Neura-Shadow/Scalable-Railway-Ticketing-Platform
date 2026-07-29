package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	physicalpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestApplyJournalCommitsMutationAndReceiptTogetherAndSkipsAReplay(t *testing.T) {
	t.Parallel()

	fingerprint := [32]byte{1, 2, 3}
	record := testRecord()
	entry := physicalmigration.JournalEntry{
		ID: uuid.New(), Sequence: 17, TableName: "reservations", Operation: "UPDATE", EntityID: uuid.New(),
		ApplyFingerprint: fingerprint,
	}
	first := &fakeTx{rowsAffected: 1}
	replay := &fakeTx{rowsAffected: 0, row: fakeRow{values: []any{
		record.TrainRunID, record.TargetGeneration, entry.Sequence, fingerprint[:],
	}}}
	target := &fakeDB{transactions: []pgx.Tx{first, replay}}
	applier := &fakeApplier{}
	shards := newTestShards(t, &fakeDB{}, target, applier)

	alreadyApplied, err := shards.ApplyJournal(context.Background(), record, entry)
	if err != nil || alreadyApplied {
		t.Fatalf("first ApplyJournal() = (%v, %v), want newly applied", alreadyApplied, err)
	}
	if applier.calls != 1 || !first.committed || first.rolledBack {
		t.Fatalf("first apply calls=%d committed=%v rolledBack=%v", applier.calls, first.committed, first.rolledBack)
	}
	alreadyApplied, err = shards.ApplyJournal(context.Background(), record, entry)
	if err != nil || !alreadyApplied {
		t.Fatalf("replay ApplyJournal() = (%v, %v), want already applied", alreadyApplied, err)
	}
	if applier.calls != 1 || !replay.committed {
		t.Fatalf("replay called applier=%d committed=%v", applier.calls, replay.committed)
	}
}

func TestApplyJournalRejectsAConflictingDuplicateReceipt(t *testing.T) {
	t.Parallel()

	record := testRecord()
	entry := physicalmigration.JournalEntry{
		ID: uuid.New(), Sequence: 17, TableName: "reservations", Operation: "UPDATE", EntityID: uuid.New(),
		ApplyFingerprint: [32]byte{1},
	}
	tx := &fakeTx{rowsAffected: 0, row: fakeRow{values: []any{
		record.TrainRunID, record.TargetGeneration, entry.Sequence, []byte{9, 9, 9},
	}}}
	applier := &fakeApplier{}
	shards := newTestShards(t, &fakeDB{}, &fakeDB{transactions: []pgx.Tx{tx}}, applier)

	alreadyApplied, err := shards.ApplyJournal(context.Background(), record, entry)
	if err != physicalpostgres.ErrApplyReceiptConflict || alreadyApplied {
		t.Fatalf("ApplyJournal() = (%v, %v), want receipt conflict", alreadyApplied, err)
	}
	if applier.calls != 0 || tx.committed || !tx.rolledBack {
		t.Fatalf("conflict calls=%d committed=%v rolledBack=%v", applier.calls, tx.committed, tx.rolledBack)
	}
}

func TestControlLoadReadsTheMigrationAndDurableBaseCopyCheckpoint(t *testing.T) {
	t.Parallel()

	migrationID := uuid.New()
	parentID := uuid.New()
	trainRunID := uuid.New()
	db := &fakeDB{row: fakeRow{values: []any{
		migrationID, parentID.String(), trainRunID, "physical-shard-0", "physical-shard-1",
		int64(7), int64(8), int64(0), 1, 1, 1, 1,
		migration.PhysicalStateBaseCopying, "reservations:100",
		int64(100), int64(12), int64(12), int64(0), int64(0), int64(2), int64(0), false,
	}}}
	control, err := physicalpostgres.NewControl(db)
	if err != nil {
		t.Fatalf("NewControl() error = %v", err)
	}

	got, err := control.Load(context.Background(), migrationID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.ParentMigrationID != parentID || got.BaseCopyCursor != "reservations:100" || got.RowsCopied != 100 {
		t.Fatalf("Load() = %+v", got)
	}
	if !strings.Contains(db.lastSQL, "physical_shard_migration_checkpoints") || strings.Contains(strings.ToLower(db.lastSQL), "password") {
		t.Fatalf("control query does not use the bounded checkpoint ledger: %s", db.lastSQL)
	}
}

func TestJSONBaseCopierReturnsAnOpaqueResumableCursorForTheFixedTableOrder(t *testing.T) {
	t.Parallel()

	rowID := uuid.New()
	source := &fakeDB{rows: []pgx.Rows{&fakeRows{values: [][]any{{
		rowID, []byte(`{"id":"` + rowID.String() + `"}`),
	}}}}}
	record := testRecord()

	batch, err := (physicalpostgres.JSONBaseCopier{}).Read(context.Background(), source, physicalmigration.BaseCopyRequest{
		Migration: record, Limit: 10,
	})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if batch.ObjectName != "train_run_booking_snapshots" || batch.Rows != 1 ||
		batch.NextCursor != "0:"+rowID.String() || batch.Fingerprint == ([32]byte{}) {
		t.Fatalf("Read() = %+v", batch)
	}
}

func TestTargetWriteCountFailsClosedWhenThePreparedEvidenceRowIsMissing(t *testing.T) {
	t.Parallel()

	target := &fakeDB{row: errorRow{err: pgx.ErrNoRows}}
	shards := newTestShards(t, &fakeDB{}, target, &fakeApplier{})

	_, err := shards.TargetWriteCount(context.Background(), testRecord())
	if !errors.Is(err, physicalmigration.ErrTargetEvidenceMissing) {
		t.Fatalf("TargetWriteCount() error = %v, want ErrTargetEvidenceMissing", err)
	}
}

func TestPreflightChecksActualSchemaHistoryAndMutationTriggerCoverage(t *testing.T) {
	t.Parallel()

	source := &fakeDB{row: fakeRow{values: []any{true}}}
	target := &fakeDB{row: fakeRow{values: []any{true}}}
	shards := newTestShards(t, source, target, &fakeApplier{})

	if err := shards.Preflight(context.Background(), testRecord()); err != nil {
		t.Fatalf("Preflight() error = %v", err)
	}
	for name, sql := range map[string]string{"source": source.lastSQL, "target": target.lastSQL} {
		for _, fragment := range []string{"schema_migrations", "NOT dirty", "pg_trigger", "booking_command_receipts_capture_mutation"} {
			if !strings.Contains(sql, fragment) {
				t.Fatalf("%s preflight omitted %q: %s", name, fragment, sql)
			}
		}
	}
}

func TestRollbackVerifiesTargetBaselinesBeforeRebindingSourceToANewerGeneration(t *testing.T) {
	t.Parallel()

	record := testRecord()
	targetTx := &fakeTx{rowsAffected: 1, rows: []pgx.Row{
		fakeRow{values: []any{"active", true}},
		fakeRow{values: []any{int64(0), true, int64(4), int64(4), int64(7)}},
		fakeRow{values: []any{int64(4), int64(4), int64(7)}},
	}}
	sourceTx := &fakeTx{rowsAffected: 1}
	shards := newTestShards(t, &fakeDB{transactions: []pgx.Tx{sourceTx}},
		&fakeDB{transactions: []pgx.Tx{targetTx}}, &fakeApplier{})

	if err := shards.RollbackBeforeTargetWrites(context.Background(), record, 9); err != nil {
		t.Fatalf("RollbackBeforeTargetWrites() error = %v", err)
	}
	if !targetTx.committed || !sourceTx.committed {
		t.Fatalf("target committed=%v source committed=%v", targetTx.committed, sourceTx.committed)
	}
	targetSQL := strings.Join(append(targetTx.querySQL, targetTx.execSQL...), "\n")
	for _, fragment := range []string{"baseline_reservation_count", "write_enabled = false", "state = 'disabled'"} {
		if !strings.Contains(targetSQL, fragment) {
			t.Fatalf("target rollback omitted %q: %s", fragment, targetSQL)
		}
	}
	sourceSQL := strings.Join(sourceTx.execSQL, "\n")
	for _, fragment := range []string{"migration_capture_state", "assignment_generation = $2", "write_enabled = true"} {
		if !strings.Contains(sourceSQL, fragment) {
			t.Fatalf("source rebind omitted %q: %s", fragment, sourceSQL)
		}
	}
}

func TestRollbackRejectsTargetRowsBeyondTheEnabledBaseline(t *testing.T) {
	t.Parallel()

	record := testRecord()
	targetTx := &fakeTx{rowsAffected: 1, rows: []pgx.Row{
		fakeRow{values: []any{"active", true}},
		fakeRow{values: []any{int64(0), true, int64(4), int64(4), int64(7)}},
		fakeRow{values: []any{int64(5), int64(4), int64(7)}},
	}}
	shards := newTestShards(t, &fakeDB{}, &fakeDB{transactions: []pgx.Tx{targetTx}}, &fakeApplier{})

	err := shards.RollbackBeforeTargetWrites(context.Background(), record, 9)
	if !errors.Is(err, physicalmigration.ErrReverseMigrationRequired) || !targetTx.rolledBack || targetTx.committed {
		t.Fatalf("error=%v committed=%v rolledBack=%v", err, targetTx.committed, targetTx.rolledBack)
	}
}

func TestRollbackInitializesAStandbyTargetBaselineBeforeDisablingIt(t *testing.T) {
	t.Parallel()

	record := testRecord()
	targetTx := &fakeTx{rowsAffected: 1, rows: []pgx.Row{
		fakeRow{values: []any{"standby", false}},
		fakeRow{values: []any{int64(0), false, int64(0), int64(0), int64(0)}},
		fakeRow{values: []any{int64(4), int64(4), int64(7)}},
	}}
	sourceTx := &fakeTx{rowsAffected: 1}
	shards := newTestShards(t, &fakeDB{transactions: []pgx.Tx{sourceTx}},
		&fakeDB{transactions: []pgx.Tx{targetTx}}, &fakeApplier{})

	if err := shards.RollbackBeforeTargetWrites(context.Background(), record, 9); err != nil {
		t.Fatalf("RollbackBeforeTargetWrites() error = %v", err)
	}
	joined := strings.Join(targetTx.execSQL, "\n")
	if !strings.Contains(joined, "baseline_initialized = true") ||
		!strings.Contains(joined, "state = 'disabled'") {
		t.Fatalf("standby rollback did not initialize evidence before disabling target: %s", joined)
	}
}

func TestPrepareTargetCreatesSnapshotFenceAndExactZeroEvidenceInOneTransaction(t *testing.T) {
	t.Parallel()

	record := testRecord()
	source := &fakeDB{row: fakeRow{values: []any{[]byte(`{
        "id":"10000000-0000-0000-0000-000000000001",
        "assignment_generation":7
    }`)}}}
	tx := &fakeTx{rowsAffected: 1, row: fakeRow{values: []any{int64(0)}}}
	target := &fakeDB{row: fakeRow{values: []any{int64(0)}}, transactions: []pgx.Tx{tx}}
	shards, err := physicalpostgres.NewDefaultShards(source, target)
	if err != nil {
		t.Fatalf("NewDefaultShards() error = %v", err)
	}

	if err := shards.PrepareTarget(context.Background(), record); err != nil {
		t.Fatalf("PrepareTarget() error = %v", err)
	}
	if !tx.committed || tx.rolledBack || len(tx.execSQL) != 3 {
		t.Fatalf("committed=%v rolledBack=%v execs=%d", tx.committed, tx.rolledBack, len(tx.execSQL))
	}
	joined := strings.Join(tx.execSQL, "\n")
	for _, table := range []string{"train_run_booking_snapshots", "train_run_write_fences", "train_run_target_write_evidence"} {
		if !strings.Contains(joined, table) {
			t.Fatalf("target authority transaction omitted %s: %s", table, joined)
		}
	}
}

func TestCaptureOutboxResetsOnlyTransientRelayLeaseState(t *testing.T) {
	t.Parallel()

	record := testRecord()
	eventID := uuid.New()
	sourceTx := &fakeTx{
		row: fakeRow{values: []any{1}},
		resultRows: []pgx.Rows{&fakeRows{values: [][]any{{eventID, []byte(`{
            "id":"` + eventID.String() + `",
            "train_run_id":"` + record.TrainRunID.String() + `",
            "assignment_generation":7,
            "status":"processing",
            "locked_at":"2026-07-29T00:00:00Z",
            "locked_by":"relay-1",
            "lease_token":"10000000-0000-0000-0000-000000000001"
		}`)}}}},
	}
	source := &fakeDB{transactions: []pgx.Tx{sourceTx}}
	tx := &fakeTx{rowsAffected: 1}
	shards := newTestShards(t, source, &fakeDB{transactions: []pgx.Tx{tx}}, &fakeApplier{})

	if err := shards.CaptureOutbox(context.Background(), record, 10); err != nil {
		t.Fatalf("CaptureOutbox() error = %v", err)
	}
	if !sourceTx.committed {
		t.Fatal("source outbox snapshot transaction was not committed")
	}
	if len(tx.execArgs) != 2 {
		t.Fatalf("target exec calls = %d, want delete plus insert", len(tx.execArgs))
	}
	data := string(tx.execArgs[1][0].([]byte))
	for _, want := range []string{`"assignment_generation":8`, `"status":"pending"`, `"locked_at":null`, `"locked_by":null`, `"lease_token":null`} {
		if !strings.Contains(data, want) {
			t.Fatalf("normalized outbox %s does not contain %s", data, want)
		}
	}
}

func TestBoundedValidatorChecksInventoryAndReferentialSemantics(t *testing.T) {
	t.Parallel()

	rows := func() []pgx.Row {
		result := []pgx.Row{fakeRow{values: []any{0}}}
		for range 11 {
			result = append(result, fakeRow{values: []any{0, "", ""}})
		}
		return result
	}
	source := &fakeDB{queryRows: rows()}
	target := &fakeDB{queryRows: rows()}
	record := testRecord()
	result, err := (physicalpostgres.BoundedValidator{}).Validate(context.Background(), source, target,
		physicalmigration.ValidationRequest{Migration: record, MaxRows: 100, MaxTables: 11})
	if err != nil || !result.Passed {
		t.Fatalf("Validate() result=%+v error=%v", result, err)
	}
	semanticSQL := source.querySQL[0]
	for _, fragment := range []string{"bit_or", "idempotency_records", "booking_command_receipts", "outbox_events"} {
		if !strings.Contains(semanticSQL, fragment) {
			t.Fatalf("semantic validation omitted %q: %s", fragment, semanticSQL)
		}
	}
}

func TestReverseTargetPreparationCleansChildrenButRetainsExactSnapshotAndFenceMarker(t *testing.T) {
	t.Parallel()

	record := testRecord()
	record.ReverseMigration = true
	record.RetainedTargetGeneration = 7
	record.SourceGeneration = 8
	record.TargetGeneration = 9
	tx := &fakeTx{rowsAffected: 1, rows: []pgx.Row{
		fakeRow{values: []any{1, 1, 0, 0, 0}},
		fakeRow{values: []any{5}},
	}}
	target := &fakeDB{transactions: []pgx.Tx{tx}}

	if err := (physicalpostgres.DefaultTargetPreparer{MaxCleanupRows: 10}).Prepare(context.Background(), target, record); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	joined := strings.Join(tx.execSQL, "\n")
	if strings.Contains(joined, "DELETE FROM public.train_run_booking_snapshots") ||
		strings.Contains(joined, "DELETE FROM public.train_run_write_fences") {
		t.Fatalf("preparation deleted the retained snapshot/fence marker: %s", joined)
	}
	outbox := strings.Index(joined, "DELETE FROM public.outbox_events")
	tickets := strings.Index(joined, "DELETE FROM public.tickets")
	snapshot := strings.LastIndex(joined, "UPDATE public.train_run_booking_snapshots")
	fence := strings.LastIndex(joined, "UPDATE public.train_run_write_fences")
	if outbox < 0 || tickets <= outbox || snapshot <= tickets || fence <= snapshot {
		t.Fatalf("unsafe reverse cleanup order: %s", joined)
	}
}

func TestReverseTargetPreparationAcceptsAnExactlyPreparedNewGenerationAfterCrash(t *testing.T) {
	t.Parallel()

	record := testRecord()
	record.ReverseMigration = true
	record.RetainedTargetGeneration = 7
	record.SourceGeneration = 8
	record.TargetGeneration = 9
	tx := &fakeTx{rowsAffected: 1, rows: []pgx.Row{
		fakeRow{values: []any{0, 0, 1, 1, 0}},
	}}

	err := (physicalpostgres.DefaultTargetPreparer{MaxCleanupRows: 10}).Prepare(
		context.Background(), &fakeDB{transactions: []pgx.Tx{tx}}, record)
	if err != nil {
		t.Fatalf("Prepare() retry error = %v", err)
	}
	if !tx.committed || len(tx.execSQL) != 0 {
		t.Fatalf("prepared retry committed=%v mutations=%d", tx.committed, len(tx.execSQL))
	}
}

func TestBeginCleanupRejectsAnExistingReverseChildUnderTheParentLock(t *testing.T) {
	t.Parallel()

	tx := &fakeTx{rowsAffected: 1, rows: []pgx.Row{
		fakeRow{values: []any{migration.PhysicalStateCompleted, "eligible", true, []byte(nil)}},
		fakeRow{values: []any{true}},
	}}
	control, err := physicalpostgres.NewControl(&fakeDB{transactions: []pgx.Tx{tx}})
	if err != nil {
		t.Fatalf("NewControl() error = %v", err)
	}

	err = control.BeginCleanup(context.Background(), uuid.New(), [32]byte{1})
	if !errors.Is(err, physicalmigration.ErrCleanupConflict) || !tx.rolledBack || tx.committed {
		t.Fatalf("BeginCleanup() error=%v committed=%v rolledBack=%v", err, tx.committed, tx.rolledBack)
	}
}

func TestBeginCleanupRejectsAChangedConfirmationWhileAlreadyRunning(t *testing.T) {
	t.Parallel()

	tx := &fakeTx{rowsAffected: 1, rows: []pgx.Row{
		fakeRow{values: []any{migration.PhysicalStateCompleted, "running", true, bytes.Repeat([]byte{1}, 32)}},
	}}
	control, err := physicalpostgres.NewControl(&fakeDB{transactions: []pgx.Tx{tx}})
	if err != nil {
		t.Fatalf("NewControl() error = %v", err)
	}

	err = control.BeginCleanup(context.Background(), uuid.New(), [32]byte{2})
	if !errors.Is(err, physicalmigration.ErrCleanupConflict) || !tx.rolledBack || tx.committed {
		t.Fatalf("BeginCleanup() error=%v committed=%v rolledBack=%v", err, tx.committed, tx.rolledBack)
	}
}

func TestCreateReverseRejectsCleanupAlreadyRunningUnderTheParentLock(t *testing.T) {
	t.Parallel()

	record := testRecord()
	record.State = migration.PhysicalStateCompleted
	tx := &fakeTx{rowsAffected: 1, rows: []pgx.Row{
		fakeRow{values: controlRecordValues(record)},
		fakeRow{values: []any{"running"}},
	}}
	control, err := physicalpostgres.NewControl(&fakeDB{transactions: []pgx.Tx{tx}})
	if err != nil {
		t.Fatalf("NewControl() error = %v", err)
	}

	_, err = control.CreateReverse(context.Background(), physicalmigration.ReversePlan{
		OriginalMigrationID: record.MigrationID,
		MigrationID:         uuid.New(),
		TargetGeneration:    9,
	})
	if !errors.Is(err, physicalmigration.ErrCleanupConflict) || !tx.rolledBack || tx.committed {
		t.Fatalf("CreateReverse() error=%v committed=%v rolledBack=%v", err, tx.committed, tx.rolledBack)
	}
}

func TestCreateReverseMovesTheActiveTargetAssignmentBackToMigrating(t *testing.T) {
	t.Parallel()

	original := testRecord()
	original.State = migration.PhysicalStateRollbackWindow
	reverseID := uuid.New()
	reverse := original
	reverse.MigrationID = reverseID
	reverse.ParentMigrationID = original.MigrationID
	reverse.SourceShardID, reverse.TargetShardID = original.TargetShardID, original.SourceShardID
	reverse.SourceGeneration = original.TargetGeneration
	reverse.TargetGeneration = 9
	reverse.RetainedTargetGeneration = original.SourceGeneration
	reverse.SourceSchemaVersion, reverse.TargetSchemaVersion = original.TargetSchemaVersion, original.SourceSchemaVersion
	reverse.State = migration.PhysicalStatePlanned
	reverse.ReverseMigration = true
	tx := &fakeTx{rowsAffected: 1, rows: []pgx.Row{
		fakeRow{values: controlRecordValues(original)},
		fakeRow{values: []any{"pending"}},
		fakeRow{values: controlRecordValues(reverse)},
	}}
	control, err := physicalpostgres.NewControl(&fakeDB{transactions: []pgx.Tx{tx}})
	if err != nil {
		t.Fatalf("NewControl() error = %v", err)
	}

	got, err := control.CreateReverse(context.Background(), physicalmigration.ReversePlan{
		OriginalMigrationID:  original.MigrationID,
		MigrationID:          reverseID,
		TargetGeneration:     9,
		ObservedTargetWrites: 1,
	})
	if err != nil || got.MigrationID != reverseID || !tx.committed {
		t.Fatalf("CreateReverse() = (%+v,%v), committed=%v", got, err, tx.committed)
	}
	joined := strings.Join(tx.execSQL, "\n")
	if !strings.Contains(joined, "assignment_state = 'migrating'") ||
		!strings.Contains(joined, "assignment_state IN ('rollback_window', 'migrating')") {
		t.Fatalf("CreateReverse omitted assignment-state rotation: %s", joined)
	}
}

func TestControlRollbackMovesPostSwitchAssignmentAndLocatorsToTheNewGeneration(t *testing.T) {
	t.Parallel()

	record := testRecord()
	record.State = migration.PhysicalStateRollbackWindow
	tx := &fakeTx{rowsAffected: 1, rows: []pgx.Row{
		fakeRow{values: controlRecordValues(record)},
		fakeRow{values: []any{record.TargetShardID, record.TargetGeneration, record.MigrationID}},
	}}
	control, err := physicalpostgres.NewControl(&fakeDB{transactions: []pgx.Tx{tx}})
	if err != nil {
		t.Fatalf("NewControl() error = %v", err)
	}

	_, err = control.Rollback(context.Background(), physicalmigration.Change{
		MigrationID: record.MigrationID, ExpectedState: record.State,
		NextState: migration.PhysicalStateRolledBack, RollbackGeneration: 9,
	})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	joined := strings.Join(tx.execSQL, "\n")
	for _, fragment := range []string{"assignment_generation = $3", "reservation_directory", "booking_commands", "physical_shard_migration"} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("control rollback omitted %q: %s", fragment, joined)
		}
	}
}

func TestControlCutoverRotatesEveryGlobalLocatorInTheAssignmentTransaction(t *testing.T) {
	t.Parallel()

	record := testRecord()
	record.State = migration.PhysicalStateTargetEnabled
	tx := &fakeTx{rowsAffected: 1, rows: []pgx.Row{
		fakeRow{values: controlRecordValues(record)},
		fakeRow{values: []any{record.SourceShardID, record.SourceGeneration, record.MigrationID}},
		fakeRow{values: []any{int64(0)}},
	}}
	control, err := physicalpostgres.NewControl(&fakeDB{transactions: []pgx.Tx{tx}})
	if err != nil {
		t.Fatalf("NewControl() error = %v", err)
	}

	_, err = control.SwitchAssignment(context.Background(), physicalmigration.Change{
		MigrationID: record.MigrationID, ExpectedState: record.State,
		NextState: migration.PhysicalStateSwitchingAssignment,
	})
	if err != nil {
		t.Fatalf("SwitchAssignment() error = %v", err)
	}
	joined := strings.Join(tx.execSQL, "\n")
	for _, table := range []string{"reservation_shard_locators", "ticket_order_shard_locators", "ticket_shard_locators"} {
		if !strings.Contains(joined, "UPDATE public."+table) {
			t.Fatalf("cutover omitted %s: %s", table, joined)
		}
	}
}

func controlRecordValues(record physicalmigration.Record) []any {
	parent := ""
	if record.ParentMigrationID != uuid.Nil {
		parent = record.ParentMigrationID.String()
	}
	return []any{
		record.MigrationID, parent, record.TrainRunID, record.SourceShardID, record.TargetShardID,
		record.SourceGeneration, record.TargetGeneration, record.RetainedTargetGeneration,
		record.SourceProtocolVersion, record.SourceSchemaVersion,
		record.TargetProtocolVersion, record.TargetSchemaVersion,
		record.State, record.BaseCopyCursor, record.RowsCopied, record.SourceJournalStart,
		record.LastReplayedSequence, record.FinalSourceSequence, record.RowsReplayed,
		record.ValidationVersion, record.TargetWriteCount, record.ReverseMigration,
	}
}

func newTestShards(t *testing.T, source, target physicalpostgres.DB, applier physicalpostgres.MutationApplier) *physicalpostgres.Shards {
	t.Helper()
	shards, err := physicalpostgres.NewShards(source, target, fakeCopier{}, fakePreparer{}, applier, fakeValidator{})
	if err != nil {
		t.Fatalf("NewShards() error = %v", err)
	}
	return shards
}

func testRecord() physicalmigration.Record {
	return physicalmigration.Record{
		MigrationID: uuid.New(), TrainRunID: uuid.New(), SourceShardID: "physical-shard-0",
		TargetShardID: "physical-shard-1", SourceGeneration: 7, TargetGeneration: 8,
		SourceProtocolVersion: 1, SourceSchemaVersion: 1,
		TargetProtocolVersion: 1, TargetSchemaVersion: 1,
		State: migration.PhysicalStateCatchingUp,
	}
}

type fakeDB struct {
	transactions []pgx.Tx
	row          pgx.Row
	lastSQL      string
	querySQL     []string
	queryRows    []pgx.Row
	rows         []pgx.Rows
}

func (db *fakeDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	tx := db.transactions[0]
	db.transactions = db.transactions[1:]
	return tx, nil
}
func (db *fakeDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	if len(db.rows) == 0 {
		panic("unexpected Query")
	}
	rows := db.rows[0]
	db.rows = db.rows[1:]
	return rows, nil
}
func (db *fakeDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	db.lastSQL = sql
	db.querySQL = append(db.querySQL, sql)
	if len(db.queryRows) > 0 {
		row := db.queryRows[0]
		db.queryRows = db.queryRows[1:]
		return row
	}
	if db.row == nil {
		panic("unexpected QueryRow")
	}
	return db.row
}

type fakeTx struct {
	pgx.Tx
	rowsAffected int64
	committed    bool
	rolledBack   bool
	row          pgx.Row
	rows         []pgx.Row
	execSQL      []string
	execArgs     [][]any
	resultRows   []pgx.Rows
	querySQL     []string
}

func (tx *fakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.execSQL = append(tx.execSQL, sql)
	tx.execArgs = append(tx.execArgs, args)
	return pgconn.NewCommandTag("INSERT 0 " + string(rune('0'+tx.rowsAffected))), nil
}
func (tx *fakeTx) Commit(context.Context) error {
	tx.committed = true
	return nil
}
func (tx *fakeTx) Rollback(context.Context) error {
	tx.rolledBack = true
	return nil
}
func (tx *fakeTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	tx.querySQL = append(tx.querySQL, sql)
	if len(tx.rows) > 0 {
		row := tx.rows[0]
		tx.rows = tx.rows[1:]
		return row
	}
	if tx.row == nil {
		panic("unexpected QueryRow")
	}
	return tx.row
}
func (tx *fakeTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	if len(tx.resultRows) == 0 {
		panic("unexpected Query")
	}
	rows := tx.resultRows[0]
	tx.resultRows = tx.resultRows[1:]
	return rows, nil
}

type fakeRow struct{ values []any }

func (row fakeRow) Scan(destinations ...any) error {
	for index, value := range row.values {
		reflect.ValueOf(destinations[index]).Elem().Set(reflect.ValueOf(value))
	}
	return nil
}

type errorRow struct{ err error }

func (row errorRow) Scan(...any) error { return row.err }

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

type fakeApplier struct{ calls int }

func (applier *fakeApplier) Apply(context.Context, pgx.Tx, physicalmigration.Record, physicalmigration.JournalEntry) error {
	applier.calls++
	return nil
}

type fakeCopier struct{}

func (fakeCopier) Read(context.Context, physicalpostgres.DB, physicalmigration.BaseCopyRequest) (physicalmigration.BaseBatch, error) {
	return physicalmigration.BaseBatch{}, nil
}
func (fakeCopier) Apply(context.Context, physicalpostgres.DB, physicalmigration.Record, physicalmigration.BaseBatch) error {
	return nil
}

type fakePreparer struct{}

func (fakePreparer) Prepare(context.Context, physicalpostgres.DB, physicalmigration.Record) error {
	return nil
}

type fakeValidator struct{}

func (fakeValidator) Validate(context.Context, physicalpostgres.DB, physicalpostgres.DB, physicalmigration.ValidationRequest) (physicalmigration.ValidationResult, error) {
	return physicalmigration.ValidationResult{}, nil
}
