package physicalmigration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	"github.com/google/uuid"
)

func TestAdvanceResumesBaseCopyFromTheDurableCursor(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateBaseCopying)
	record.BaseCopyCursor = "reservations:000004"
	control := &fakeControl{record: record}
	shards := &fakeShards{baseBatches: []physicalmigration.BaseBatch{{
		ObjectName: "reservations",
		Cursor:     "reservations:000004",
		NextCursor: "reservations:000008",
		Rows:       4,
	}}}
	engine := newTestEngine(t, control, shards)

	got, err := engine.Advance(context.Background(), record.MigrationID)
	if err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	if got.BaseCopyCursor != "reservations:000008" || got.RowsCopied != 4 {
		t.Fatalf("Advance() = %+v, want durable next cursor and four copied rows", got)
	}
	if shards.baseRequests[0].Cursor != "reservations:000004" {
		t.Fatalf("source cursor = %q, want durable cursor", shards.baseRequests[0].Cursor)
	}
	if shards.appliedBase[0].NextCursor != "reservations:000008" {
		t.Fatalf("applied target batch = %+v", shards.appliedBase[0])
	}
}

func TestAdvanceReplaysJournalInOrderAndCheckpointsAppliedReceipts(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateCatchingUp)
	record.LastReplayedSequence = 40
	control := &fakeControl{record: record}
	shards := &fakeShards{journalBatches: []physicalmigration.JournalBatch{{
		SourceSequence: 42,
		Entries: []physicalmigration.JournalEntry{
			{ID: uuid.New(), Sequence: 41, TableName: "reservations", Operation: "UPDATE", EntityID: uuid.New()},
			{ID: uuid.New(), Sequence: 42, TableName: "reservation_seats", Operation: "INSERT", EntityID: uuid.New()},
		},
	}}}
	engine := newTestEngine(t, control, shards)

	got, err := engine.Advance(context.Background(), record.MigrationID)
	if err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	if got.LastReplayedSequence != 42 || got.RowsReplayed != 2 || got.State != migration.PhysicalStateValidatingOnline {
		t.Fatalf("Advance() = %+v, want replay checkpoint 42 and online validation", got)
	}
	if len(shards.appliedJournal) != 2 || shards.appliedJournal[0].Sequence != 41 || shards.appliedJournal[1].Sequence != 42 {
		t.Fatalf("applied journal = %+v, want ordered 41 then 42", shards.appliedJournal)
	}
}

func TestAdvanceEnforcesFinalQuiesceBeforeTheControlAssignmentSwitch(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateDraining)
	record.LastReplayedSequence = 50
	control := &fakeControl{record: record}
	shards := &fakeShards{
		fenceSequence: 52,
		journalBatches: []physicalmigration.JournalBatch{{
			SourceSequence: 52,
			Entries: []physicalmigration.JournalEntry{
				{ID: uuid.New(), Sequence: 51, TableName: "reservations", Operation: "UPDATE", EntityID: uuid.New()},
				{ID: uuid.New(), Sequence: 52, TableName: "reservation_seats", Operation: "INSERT", EntityID: uuid.New()},
			},
		}},
		validation: physicalmigration.ValidationResult{Passed: true, RowsExamined: 40, Tables: 10, Version: 9},
	}
	control.events = &shards.events
	engine := newTestEngine(t, control, shards)

	for control.record.State != migration.PhysicalStateRollbackWindow {
		if _, err := engine.Advance(context.Background(), record.MigrationID); err != nil {
			t.Fatalf("Advance() from %q error = %v", control.record.State, err)
		}
	}
	want := []string{"source-fence", "journal-read", "journal-apply:51", "journal-apply:52", "validate-final", "target-enable", "control-switch"}
	if len(shards.events) != len(want) {
		t.Fatalf("events = %#v, want %#v", shards.events, want)
	}
	for index := range want {
		if shards.events[index] != want[index] {
			t.Fatalf("events = %#v, want %#v", shards.events, want)
		}
	}
}

func TestRollbackRejectsDirectReassignmentAfterTheTargetAcceptedWrites(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateRollbackWindow)
	control := &fakeControl{record: record}
	shards := &fakeShards{targetWriteCount: 1}
	engine := newTestEngine(t, control, shards)

	_, err := engine.Rollback(context.Background(), record.MigrationID)
	if !errors.Is(err, physicalmigration.ErrReverseMigrationRequired) {
		t.Fatalf("Rollback() error = %v, want ErrReverseMigrationRequired", err)
	}
	if shards.rollbackCalled {
		t.Fatal("direct rollback touched shard fences after target-write evidence")
	}
	if control.record.State != migration.PhysicalStateRollbackWindow {
		t.Fatalf("state = %q, want rollback_window", control.record.State)
	}
}

func TestRollbackRejectsPersistedWriteEvidenceBeforeTouchingEitherShard(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateRollbackWindow)
	record.TargetWriteCount = 1
	control := &fakeControl{record: record}
	shards := &fakeShards{}
	engine := newTestEngine(t, control, shards)

	_, err := engine.Rollback(context.Background(), record.MigrationID)
	if !errors.Is(err, physicalmigration.ErrReverseMigrationRequired) {
		t.Fatalf("Rollback() error = %v, want ErrReverseMigrationRequired", err)
	}
	if shards.rollbackCalled || len(shards.events) != 0 {
		t.Fatalf("persisted write evidence touched shards: rollback=%v events=%v",
			shards.rollbackCalled, shards.events)
	}
}

func TestRollbackAfterTheControlSwitchUsesANewerGenerationWhenAllEvidenceIsZero(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateRollbackWindow)
	control := &fakeControl{record: record}
	shards := &fakeShards{}
	engine := newTestEngine(t, control, shards)

	got, err := engine.Rollback(context.Background(), record.MigrationID)
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if !shards.rollbackCalled || shards.rollbackGeneration != record.TargetGeneration+1 ||
		got.State != migration.PhysicalStateRolledBack {
		t.Fatalf("rollback called=%v generation=%d state=%q", shards.rollbackCalled,
			shards.rollbackGeneration, got.State)
	}
}

func TestRollbackWithoutTargetWritesRestoresTheSourceAndControlRoute(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateTargetEnabled)
	control := &fakeControl{record: record}
	shards := &fakeShards{}
	engine := newTestEngine(t, control, shards)

	got, err := engine.Rollback(context.Background(), record.MigrationID)
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if !shards.rollbackCalled || got.State != migration.PhysicalStateRolledBack {
		t.Fatalf("Rollback() = %+v, shard rollback called=%v", got, shards.rollbackCalled)
	}
	if shards.rollbackGeneration != record.TargetGeneration+1 || shards.captureDisableCalls != 1 {
		t.Fatalf("rollback generation=%d capture disables=%d", shards.rollbackGeneration,
			shards.captureDisableCalls)
	}
}

func TestRollbackRejectsContradictoryCommandOrOutboxEvidence(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateTargetEnabled)
	control := &fakeControl{record: record}
	shards := &fakeShards{targetCommandEvidence: 1}
	engine := newTestEngine(t, control, shards)

	_, err := engine.Rollback(context.Background(), record.MigrationID)
	if !errors.Is(err, physicalmigration.ErrReverseMigrationRequired) {
		t.Fatalf("Rollback() error = %v, want ErrReverseMigrationRequired", err)
	}
	if shards.rollbackCalled || shards.captureDisableCalls != 0 {
		t.Fatal("contradictory target evidence changed either shard")
	}
}

func TestRollbackCanAbortAnUnpreparedPlanWithoutTargetEvidence(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStatePlanned)
	control := &fakeControl{record: record}
	shards := &fakeShards{}
	engine := newTestEngine(t, control, shards)

	got, err := engine.Rollback(context.Background(), record.MigrationID)
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if got.State != migration.PhysicalStateRolledBack || shards.rollbackCalled || len(shards.events) != 0 {
		t.Fatalf("state=%q rollbackCalled=%v events=%v", got.State, shards.rollbackCalled, shards.events)
	}
}

func TestPlanReverseRequiresANewerGenerationAndSwapsSourceWithTarget(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateRollbackWindow)
	control := &fakeControl{record: record}
	shards := &fakeShards{targetWriteCount: 3, targetCommandEvidence: 1}
	engine := newTestEngine(t, control, shards)
	reverseID := uuid.MustParse("30000000-0000-0000-0000-000000000001")

	if _, err := engine.PlanReverse(context.Background(), record.MigrationID, reverseID, record.TargetGeneration); !errors.Is(err, physicalmigration.ErrGenerationNotNewer) {
		t.Fatalf("PlanReverse(same generation) error = %v, want ErrGenerationNotNewer", err)
	}
	got, err := engine.PlanReverse(context.Background(), record.MigrationID, reverseID, 9)
	if err != nil {
		t.Fatalf("PlanReverse() error = %v", err)
	}
	if got.ParentMigrationID != record.MigrationID || got.MigrationID != reverseID ||
		got.SourceShardID != record.TargetShardID || got.TargetShardID != record.SourceShardID ||
		got.SourceGeneration != record.TargetGeneration || got.TargetGeneration != 9 ||
		got.RetainedTargetGeneration != record.SourceGeneration ||
		!got.ReverseMigration || got.State != migration.PhysicalStatePlanned {
		t.Fatalf("reverse record = %+v", got)
	}
}

func TestOnlineValidationBeginsControlDrainBeforeSourceFencing(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateValidatingOnline)
	control := &fakeControl{record: record}
	shards := &fakeShards{validation: physicalmigration.ValidationResult{Passed: true, Tables: 11, Version: 1}}
	control.events = &shards.events
	engine := newTestEngine(t, control, shards)

	got, err := engine.Advance(context.Background(), record.MigrationID)
	if err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	if got.State != migration.PhysicalStateDraining || len(shards.events) != 3 ||
		shards.events[0] != "journal-read" || shards.events[1] != "validate-online" || shards.events[2] != "control-drain" {
		t.Fatalf("state=%q events=%v", got.State, shards.events)
	}
}

func TestOnlineValidationReplaysANewSourceMutationBeforeValidatingAgain(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateValidatingOnline)
	record.LastReplayedSequence = 40
	entry := physicalmigration.JournalEntry{
		ID: uuid.New(), Sequence: 41, TableName: "reservations", Operation: "UPDATE", EntityID: uuid.New(),
	}
	control := &fakeControl{record: record}
	shards := &fakeShards{journalBatches: []physicalmigration.JournalBatch{{
		SourceSequence: 41, Entries: []physicalmigration.JournalEntry{entry},
	}}}
	engine := newTestEngine(t, control, shards)

	got, err := engine.Advance(context.Background(), record.MigrationID)
	if err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	if got.State != migration.PhysicalStateValidatingOnline || got.LastReplayedSequence != 41 ||
		len(shards.events) != 2 || shards.events[0] != "journal-read" || shards.events[1] != "journal-apply:41" {
		t.Fatalf("state=%q sequence=%d events=%v", got.State, got.LastReplayedSequence, shards.events)
	}
}

func TestCompleteDisablesCaptureAndRetainsTheSourceBeforeControlCompletion(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateRollbackWindow)
	control := &fakeControl{record: record}
	shards := &fakeShards{}
	control.events = &shards.events
	engine := newTestEngine(t, control, shards)

	got, err := engine.Complete(context.Background(), record.MigrationID)
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	want := []string{"target-evidence", "capture-disable", "source-retain", "control-complete"}
	if got.State != migration.PhysicalStateCompleted || len(shards.events) != len(want) {
		t.Fatalf("state=%q events=%v", got.State, shards.events)
	}
	for index := range want {
		if shards.events[index] != want[index] {
			t.Fatalf("events=%v want=%v", shards.events, want)
		}
	}
}

func TestAdvanceRetriesAnAppliedBaseBatchAfterCheckpointCrash(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateBaseCopying)
	control := &fakeControl{record: record, failPersistOnce: true}
	batch := physicalmigration.BaseBatch{ObjectName: "reservations", NextCursor: "reservations:000010", Rows: 10}
	shards := &fakeShards{baseBatches: []physicalmigration.BaseBatch{batch, batch}}
	engine := newTestEngine(t, control, shards)

	if _, err := engine.Advance(context.Background(), record.MigrationID); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("first Advance() error = %v, want simulated checkpoint crash", err)
	}
	if control.record.BaseCopyCursor != "" {
		t.Fatalf("cursor after failed checkpoint = %q, want durable prior cursor", control.record.BaseCopyCursor)
	}
	got, err := engine.Advance(context.Background(), record.MigrationID)
	if err != nil {
		t.Fatalf("retry Advance() error = %v", err)
	}
	if got.BaseCopyCursor != "reservations:000010" || len(shards.appliedBase) != 2 {
		t.Fatalf("retry result = %+v, applied batches = %d", got, len(shards.appliedBase))
	}
}

func TestAdvanceRejectsTruncatedValidationWithoutFencingTheSource(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateValidatingOnline)
	control := &fakeControl{record: record}
	shards := &fakeShards{validation: physicalmigration.ValidationResult{
		Passed: true, Truncated: true, RowsExamined: 1000, Tables: 10, Version: 1,
	}}
	engine := newTestEngine(t, control, shards)

	_, err := engine.Advance(context.Background(), record.MigrationID)
	if !errors.Is(err, physicalmigration.ErrValidationFailed) {
		t.Fatalf("Advance() error = %v, want ErrValidationFailed", err)
	}
	if control.record.State != migration.PhysicalStateValidatingOnline || len(shards.events) != 2 ||
		shards.events[0] != "journal-read" || shards.events[1] != "validate-online" {
		t.Fatalf("failed validation changed state=%q or performed events=%v", control.record.State, shards.events)
	}
}

func TestAdvanceRejectsAJournalGapBeforeApplyingAnyMutation(t *testing.T) {
	t.Parallel()

	record := testRecord(migration.PhysicalStateCatchingUp)
	record.LastReplayedSequence = 10
	control := &fakeControl{record: record}
	shards := &fakeShards{journalBatches: []physicalmigration.JournalBatch{{
		SourceSequence: 12,
		Entries: []physicalmigration.JournalEntry{{
			ID: uuid.New(), Sequence: 12, TableName: "reservations", Operation: "UPDATE", EntityID: uuid.New(),
		}},
	}}}
	engine := newTestEngine(t, control, shards)

	_, err := engine.Advance(context.Background(), record.MigrationID)
	if !errors.Is(err, physicalmigration.ErrJournalGap) {
		t.Fatalf("Advance() error = %v, want ErrJournalGap", err)
	}
	if len(shards.appliedJournal) != 0 || control.record.LastReplayedSequence != 10 {
		t.Fatalf("gap applied %d mutations or changed checkpoint to %d", len(shards.appliedJournal), control.record.LastReplayedSequence)
	}
}

func TestAdvanceAcceptsOnlyFixedSourceEndpointSchemaContracts(t *testing.T) {
	t.Parallel()

	for _, sourceID := range []string{"legacy", "shard-0", "shard-1"} {
		record := testRecord(migration.PhysicalStatePlanned)
		record.SourceShardID = sourceID
		record.SourceSchemaVersion = 8
		control := &fakeControl{record: record}
		engine := newTestEngine(t, control, &fakeShards{})
		got, err := engine.Advance(context.Background(), record.MigrationID)
		if err != nil || got.State != migration.PhysicalStatePreparingTarget {
			t.Fatalf("source %s Advance() = (%s,%v)", sourceID, got.State, err)
		}
	}
	for _, targetID := range []string{"legacy", "shard-0", "shard-1"} {
		record := testRecord(migration.PhysicalStatePlanned)
		record.TargetShardID = targetID
		record.TargetSchemaVersion = 8
		record.TargetGeneration = 9
		record.RetainedTargetGeneration = 7
		record.ReverseMigration = true
		control := &fakeControl{record: record}
		engine := newTestEngine(t, control, &fakeShards{})
		got, err := engine.Advance(context.Background(), record.MigrationID)
		if err != nil || got.State != migration.PhysicalStatePreparingTarget {
			t.Fatalf("reverse target %s Advance() = (%s,%v)", targetID, got.State, err)
		}
	}

	tests := []struct {
		name         string
		sourceID     string
		sourceSchema int
		targetID     string
		targetSchema int
	}{
		{name: "legacy wrong schema", sourceID: "legacy", sourceSchema: 1, targetID: "physical-shard-1", targetSchema: 1},
		{name: "unknown source", sourceID: "tenant-schema", sourceSchema: 8, targetID: "physical-shard-1", targetSchema: 1},
		{name: "physical wrong schema", sourceID: "physical-shard-0", sourceSchema: 8, targetID: "physical-shard-1", targetSchema: 1},
		{name: "non physical target", sourceID: "legacy", sourceSchema: 8, targetID: "shard-1", targetSchema: 8},
		{name: "non reverse physical to control", sourceID: "physical-shard-0", sourceSchema: 1, targetID: "legacy", targetSchema: 8},
	}
	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			record := testRecord(migration.PhysicalStatePlanned)
			record.SourceShardID = testCase.sourceID
			record.SourceSchemaVersion = testCase.sourceSchema
			record.TargetShardID = testCase.targetID
			record.TargetSchemaVersion = testCase.targetSchema
			control := &fakeControl{record: record}
			engine := newTestEngine(t, control, &fakeShards{})
			_, err := engine.Advance(context.Background(), record.MigrationID)
			if !errors.Is(err, physicalmigration.ErrInvalidInput) || control.record.State != migration.PhysicalStatePlanned {
				t.Fatalf("Advance() error=%v state=%s", err, control.record.State)
			}
		})
	}
}

var errSimulatedCrash = errors.New("simulated checkpoint crash")

func testRecord(state migration.PhysicalState) physicalmigration.Record {
	return physicalmigration.Record{
		MigrationID:           uuid.MustParse("10000000-0000-0000-0000-000000000001"),
		TrainRunID:            uuid.MustParse("20000000-0000-0000-0000-000000000001"),
		SourceShardID:         "physical-shard-0",
		TargetShardID:         "physical-shard-1",
		SourceGeneration:      7,
		TargetGeneration:      8,
		SourceProtocolVersion: 1,
		SourceSchemaVersion:   1,
		TargetProtocolVersion: 1,
		TargetSchemaVersion:   1,
		State:                 state,
	}
}

func newTestEngine(t *testing.T, control physicalmigration.ControlStore, shards physicalmigration.ShardOperations) *physicalmigration.Engine {
	t.Helper()
	engine, err := physicalmigration.NewEngine(control, shards, physicalmigration.Limits{
		OperationTimeout: 5 * time.Second,
		BaseCopyBatch:    100,
		JournalBatch:     100,
		ValidationRows:   1000,
		ValidationTables: 16,
	})
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	return engine
}

type fakeControl struct {
	record          physicalmigration.Record
	events          *[]string
	reverse         physicalmigration.Record
	failPersistOnce bool
}

func (control *fakeControl) Load(context.Context, uuid.UUID) (physicalmigration.Record, error) {
	return control.record, nil
}

func (control *fakeControl) Persist(_ context.Context, change physicalmigration.Change) (physicalmigration.Record, error) {
	if control.failPersistOnce {
		control.failPersistOnce = false
		return physicalmigration.Record{}, errSimulatedCrash
	}
	if control.record.State != change.ExpectedState {
		return physicalmigration.Record{}, physicalmigration.ErrCheckpointConflict
	}
	control.record = physicalmigration.ApplyChange(control.record, change)
	return control.record, nil
}

func (control *fakeControl) BeginDrain(_ context.Context, change physicalmigration.Change) (physicalmigration.Record, error) {
	if control.events != nil {
		*control.events = append(*control.events, "control-drain")
	}
	return control.Persist(context.Background(), change)
}

func (control *fakeControl) SwitchAssignment(_ context.Context, change physicalmigration.Change) (physicalmigration.Record, error) {
	if control.events != nil {
		*control.events = append(*control.events, "control-switch")
	}
	return control.Persist(context.Background(), change)
}

func (control *fakeControl) Rollback(_ context.Context, change physicalmigration.Change) (physicalmigration.Record, error) {
	return control.Persist(context.Background(), change)
}

func (*fakeControl) CompletionEligible(context.Context, uuid.UUID) error { return nil }

func (control *fakeControl) Complete(_ context.Context, change physicalmigration.Change) (physicalmigration.Record, error) {
	if control.events != nil {
		*control.events = append(*control.events, "control-complete")
	}
	return control.Persist(context.Background(), change)
}

func (control *fakeControl) CreateReverse(_ context.Context, plan physicalmigration.ReversePlan) (physicalmigration.Record, error) {
	control.record.State = migration.PhysicalStateReverseMigrationRequired
	control.reverse = physicalmigration.Record{
		MigrationID:              plan.MigrationID,
		ParentMigrationID:        control.record.MigrationID,
		TrainRunID:               control.record.TrainRunID,
		SourceShardID:            control.record.TargetShardID,
		TargetShardID:            control.record.SourceShardID,
		SourceGeneration:         control.record.TargetGeneration,
		TargetGeneration:         plan.TargetGeneration,
		RetainedTargetGeneration: control.record.SourceGeneration,
		SourceProtocolVersion:    control.record.TargetProtocolVersion,
		SourceSchemaVersion:      control.record.TargetSchemaVersion,
		TargetProtocolVersion:    control.record.SourceProtocolVersion,
		TargetSchemaVersion:      control.record.SourceSchemaVersion,
		State:                    migration.PhysicalStatePlanned,
		ReverseMigration:         true,
	}
	return control.reverse, nil
}

type fakeShards struct {
	baseBatches           []physicalmigration.BaseBatch
	baseRequests          []physicalmigration.BaseCopyRequest
	appliedBase           []physicalmigration.BaseBatch
	journalBatches        []physicalmigration.JournalBatch
	appliedJournal        []physicalmigration.JournalEntry
	fenceSequence         int64
	validation            physicalmigration.ValidationResult
	events                []string
	targetWriteCount      int64
	targetCommandEvidence int64
	rollbackCalled        bool
	rollbackGeneration    int64
	captureDisableCalls   int
}

func (*fakeShards) Preflight(context.Context, physicalmigration.Record) error            { return nil }
func (shards *fakeShards) PrepareTarget(context.Context, physicalmigration.Record) error { return nil }
func (shards *fakeShards) EnableCapture(context.Context, physicalmigration.Record) (int64, error) {
	return 0, nil
}
func (shards *fakeShards) ReadBaseBatch(_ context.Context, request physicalmigration.BaseCopyRequest) (physicalmigration.BaseBatch, error) {
	shards.baseRequests = append(shards.baseRequests, request)
	batch := shards.baseBatches[0]
	shards.baseBatches = shards.baseBatches[1:]
	return batch, nil
}
func (shards *fakeShards) ApplyBaseBatch(_ context.Context, _ physicalmigration.Record, batch physicalmigration.BaseBatch) error {
	shards.appliedBase = append(shards.appliedBase, batch)
	return nil
}
func (shards *fakeShards) ReadJournal(_ context.Context, request physicalmigration.JournalRequest) (physicalmigration.JournalBatch, error) {
	shards.events = append(shards.events, "journal-read")
	if len(shards.journalBatches) == 0 {
		return physicalmigration.JournalBatch{SourceSequence: request.AfterSequence}, nil
	}
	batch := shards.journalBatches[0]
	shards.journalBatches = shards.journalBatches[1:]
	return batch, nil
}
func (shards *fakeShards) ApplyJournal(_ context.Context, _ physicalmigration.Record, entry physicalmigration.JournalEntry) (bool, error) {
	shards.appliedJournal = append(shards.appliedJournal, entry)
	shards.events = append(shards.events, "journal-apply:"+fmt.Sprint(entry.Sequence))
	return false, nil
}
func (*fakeShards) CaptureOutbox(context.Context, physicalmigration.Record, int) error { return nil }
func (shards *fakeShards) Validate(_ context.Context, request physicalmigration.ValidationRequest) (physicalmigration.ValidationResult, error) {
	if request.Final {
		shards.events = append(shards.events, "validate-final")
	} else {
		shards.events = append(shards.events, "validate-online")
	}
	return shards.validation, nil
}
func (shards *fakeShards) FenceSource(context.Context, physicalmigration.Record) (int64, error) {
	shards.events = append(shards.events, "source-fence")
	return shards.fenceSequence, nil
}
func (shards *fakeShards) EnableTarget(context.Context, physicalmigration.Record) error {
	shards.events = append(shards.events, "target-enable")
	return nil
}
func (shards *fakeShards) TargetWriteCount(context.Context, physicalmigration.Record) (int64, error) {
	shards.events = append(shards.events, "target-evidence")
	return shards.targetWriteCount, nil
}
func (shards *fakeShards) TargetCommandOutboxEvidence(context.Context, physicalmigration.Record) (int64, error) {
	return shards.targetCommandEvidence, nil
}
func (shards *fakeShards) RollbackBeforeTargetWrites(_ context.Context, _ physicalmigration.Record, generation int64) error {
	shards.rollbackCalled = true
	shards.rollbackGeneration = generation
	shards.captureDisableCalls++
	return nil
}
func (shards *fakeShards) DisableCapture(context.Context, physicalmigration.Record) error {
	shards.captureDisableCalls++
	shards.events = append(shards.events, "capture-disable")
	return nil
}
func (shards *fakeShards) RetainSource(context.Context, physicalmigration.Record) error {
	shards.events = append(shards.events, "source-retain")
	return nil
}
