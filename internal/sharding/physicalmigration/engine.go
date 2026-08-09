package physicalmigration

import (
	"context"
	"fmt"
	"math"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/google/uuid"
)

type Engine struct {
	control ControlStore
	shards  ShardOperations
	limits  Limits
}

func NewEngine(control ControlStore, shards ShardOperations, limits Limits) (*Engine, error) {
	if control == nil || shards == nil {
		return nil, ErrInvalidInput
	}
	if limits.OperationTimeout <= 0 || limits.BaseCopyBatch <= 0 ||
		limits.JournalBatch <= 0 || limits.ValidationRows <= 0 ||
		limits.ValidationTables <= 0 {
		return nil, ErrInvalidLimits
	}
	return &Engine{control: control, shards: shards, limits: limits}, nil
}

func (engine *Engine) Advance(ctx context.Context, migrationID uuid.UUID) (Record, error) {
	if ctx == nil || migrationID == uuid.Nil {
		return Record{}, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, engine.limits.OperationTimeout)
	defer cancel()
	record, err := engine.control.Load(ctx, migrationID)
	if err != nil {
		return Record{}, err
	}
	if err := validateRecord(record); err != nil {
		return Record{}, err
	}
	switch record.State {
	case migration.PhysicalStatePlanned:
		return engine.transition(ctx, record, migration.PhysicalStatePreparingTarget, Change{})
	case migration.PhysicalStatePreparingTarget:
		if err := engine.shards.Preflight(ctx, record); err != nil {
			return Record{}, err
		}
		if err := engine.shards.PrepareTarget(ctx, record); err != nil {
			return Record{}, err
		}
		return engine.transition(ctx, record, migration.PhysicalStateCaptureEnabled, Change{})
	case migration.PhysicalStateCaptureEnabled:
		sequence, err := engine.shards.EnableCapture(ctx, record)
		if err != nil {
			return Record{}, err
		}
		if sequence < 0 {
			return Record{}, ErrInvalidBatch
		}
		return engine.transition(ctx, record, migration.PhysicalStateBaseCopying, Change{
			SourceJournalStart:     int64Ptr(sequence),
			LastReplayedSequence:   int64Ptr(sequence),
			ExpectedReplaySequence: int64Ptr(record.LastReplayedSequence),
		})
	case migration.PhysicalStateBaseCopying:
		return engine.copyOneBatch(ctx, record)
	case migration.PhysicalStateCatchingUp, migration.PhysicalStateSourceFenced:
		return engine.replayOneBatch(ctx, record)
	case migration.PhysicalStateValidatingOnline:
		batch, last, err := engine.readAndApplyJournal(ctx, record, 0)
		if err != nil {
			return Record{}, err
		}
		if last != record.LastReplayedSequence || batch.SourceSequence != record.LastReplayedSequence {
			if last == record.LastReplayedSequence {
				return Record{}, ErrJournalGap
			}
			return engine.control.Persist(ctx, Change{
				MigrationID:            record.MigrationID,
				ExpectedState:          record.State,
				NextState:              record.State,
				ExpectedReplaySequence: int64Ptr(record.LastReplayedSequence),
				LastReplayedSequence:   int64Ptr(last),
				RowsReplayedDelta:      int64(len(batch.Entries)),
			})
		}
		result, err := engine.validateResult(ctx, record, false)
		if err != nil {
			return Record{}, err
		}
		change, err := engine.makeTransition(record, migration.PhysicalStateDraining, Change{
			ValidationVersionDelta: result.Version - record.ValidationVersion,
		})
		if err != nil {
			return Record{}, err
		}
		return engine.control.BeginDrain(ctx, change)
	case migration.PhysicalStateDraining:
		sequence, err := engine.shards.FenceSource(ctx, record)
		if err != nil {
			return Record{}, err
		}
		if sequence < record.LastReplayedSequence {
			return Record{}, ErrInvalidBatch
		}
		return engine.transition(ctx, record, migration.PhysicalStateSourceFenced, Change{FinalSourceSequence: int64Ptr(sequence)})
	case migration.PhysicalStateFinalCatchup:
		return engine.validate(ctx, record, true, migration.PhysicalStateFinalValidating)
	case migration.PhysicalStateFinalValidating:
		if err := engine.shards.EnableTarget(ctx, record); err != nil {
			return Record{}, err
		}
		return engine.transition(ctx, record, migration.PhysicalStateTargetEnabled, Change{})
	case migration.PhysicalStateTargetEnabled:
		change, err := engine.makeTransition(record, migration.PhysicalStateSwitchingAssignment, Change{})
		if err != nil {
			return Record{}, err
		}
		return engine.control.SwitchAssignment(ctx, change)
	case migration.PhysicalStateSwitchingAssignment:
		return engine.transition(ctx, record, migration.PhysicalStateRollbackWindow, Change{})
	default:
		return Record{}, ErrInvalidState
	}
}

// Rollback is the direct pre-write path only. Once the target has accepted a
// command, callers must create a forward, newer-generation reverse migration.
func (engine *Engine) Rollback(ctx context.Context, migrationID uuid.UUID) (Record, error) {
	ctx, cancel, err := engine.operationContext(ctx, migrationID)
	if err != nil {
		return Record{}, err
	}
	defer cancel()
	record, err := engine.control.Load(ctx, migrationID)
	if err != nil {
		return Record{}, err
	}
	if err := validateRecord(record); err != nil {
		return Record{}, err
	}
	if record.State == migration.PhysicalStateCompleted ||
		record.State == migration.PhysicalStateReverseMigrationRequired {
		return Record{}, ErrReverseMigrationRequired
	}
	// Persisted evidence is durable proof that the direct path is no longer
	// eligible. Reject it before querying or mutating either shard so a stale
	// live counter cannot erase the stronger control-plane fact.
	if record.TargetWriteCount > 0 {
		return Record{}, ErrReverseMigrationRequired
	}
	if record.State == migration.PhysicalStatePlanned || record.State == migration.PhysicalStatePreparingTarget {
		change, err := engine.makeTransition(record, migration.PhysicalStateRolledBack, Change{})
		if err != nil {
			return Record{}, err
		}
		return engine.control.Rollback(ctx, change)
	}
	writes, err := engine.shards.TargetWriteCount(ctx, record)
	if err != nil {
		return Record{}, err
	}
	if writes < 0 {
		return Record{}, ErrInvalidBatch
	}
	if writes > 0 {
		return Record{}, ErrReverseMigrationRequired
	}
	if record.State == migration.PhysicalStateTargetEnabled ||
		record.State == migration.PhysicalStateSwitchingAssignment ||
		record.State == migration.PhysicalStateRollbackWindow {
		commandEvidence, err := engine.shards.TargetCommandOutboxEvidence(ctx, record)
		if err != nil {
			return Record{}, err
		}
		if commandEvidence < 0 {
			return Record{}, ErrInvalidBatch
		}
		if commandEvidence > 0 {
			return Record{}, ErrReverseMigrationRequired
		}
	}
	if record.TargetGeneration == math.MaxInt64 {
		return Record{}, ErrGenerationNotNewer
	}
	rollbackGeneration := record.TargetGeneration + 1
	if err := engine.shards.RollbackBeforeTargetWrites(ctx, record, rollbackGeneration); err != nil {
		return Record{}, err
	}
	change, err := engine.makeTransition(record, migration.PhysicalStateRolledBack, Change{
		TargetWriteCount:   int64Ptr(0),
		RollbackGeneration: rollbackGeneration,
	})
	if err != nil {
		return Record{}, err
	}
	return engine.control.Rollback(ctx, change)
}

// Complete disables capture and leaves the predecessor read-only. The
// control repository also enforces that the durable rollback deadline elapsed.
func (engine *Engine) Complete(ctx context.Context, migrationID uuid.UUID) (Record, error) {
	ctx, cancel, err := engine.operationContext(ctx, migrationID)
	if err != nil {
		return Record{}, err
	}
	defer cancel()
	record, err := engine.control.Load(ctx, migrationID)
	if err != nil {
		return Record{}, err
	}
	if record.State != migration.PhysicalStateRollbackWindow {
		return Record{}, ErrInvalidState
	}
	if err := engine.control.CompletionEligible(ctx, migrationID); err != nil {
		return Record{}, err
	}
	if _, err := engine.shards.TargetWriteCount(ctx, record); err != nil {
		return Record{}, err
	}
	if err := engine.shards.DisableCapture(ctx, record); err != nil {
		return Record{}, err
	}
	if err := engine.shards.RetainSource(ctx, record); err != nil {
		return Record{}, err
	}
	change, err := engine.makeTransition(record, migration.PhysicalStateCompleted, Change{})
	if err != nil {
		return Record{}, err
	}
	return engine.control.Complete(ctx, change)
}

// PlanReverse creates a new forward migration with swapped shard roles. The
// control repository marks the original reverse_migration_required and inserts
// the new plan in one control-database transaction.
func (engine *Engine) PlanReverse(ctx context.Context, originalID, reverseID uuid.UUID, generation int64) (Record, error) {
	ctx, cancel, err := engine.operationContext(ctx, originalID)
	if err != nil || reverseID == uuid.Nil || reverseID == originalID {
		if err != nil {
			return Record{}, err
		}
		return Record{}, ErrInvalidInput
	}
	defer cancel()
	record, err := engine.control.Load(ctx, originalID)
	if err != nil {
		return Record{}, err
	}
	if err := validateRecord(record); err != nil {
		return Record{}, err
	}
	if generation <= record.TargetGeneration {
		return Record{}, ErrGenerationNotNewer
	}
	if record.State != migration.PhysicalStateRollbackWindow && record.State != migration.PhysicalStateCompleted &&
		record.State != migration.PhysicalStateReverseMigrationRequired {
		return Record{}, ErrInvalidState
	}
	writes, err := engine.shards.TargetWriteCount(ctx, record)
	if err != nil {
		return Record{}, err
	}
	commandEvidence, err := engine.shards.TargetCommandOutboxEvidence(ctx, record)
	if err != nil {
		return Record{}, err
	}
	if commandEvidence < 0 {
		return Record{}, ErrInvalidBatch
	}
	if (writes == 0) != (commandEvidence == 0) {
		return Record{}, ErrTargetEvidenceMissing
	}
	reverse, err := engine.control.CreateReverse(ctx, ReversePlan{
		OriginalMigrationID:  originalID,
		MigrationID:          reverseID,
		TargetGeneration:     generation,
		ObservedTargetWrites: writes,
	})
	if err != nil {
		return Record{}, err
	}
	if err := validateRecord(reverse); err != nil {
		return Record{}, err
	}
	if reverse.ParentMigrationID != originalID || reverse.SourceShardID != record.TargetShardID ||
		reverse.TargetShardID != record.SourceShardID || reverse.SourceGeneration != record.TargetGeneration ||
		reverse.TargetGeneration != generation || !reverse.ReverseMigration || reverse.State != migration.PhysicalStatePlanned {
		return Record{}, ErrCheckpointConflict
	}
	return reverse, nil
}

func (engine *Engine) operationContext(ctx context.Context, migrationID uuid.UUID) (context.Context, context.CancelFunc, error) {
	if ctx == nil || migrationID == uuid.Nil {
		return nil, nil, ErrInvalidInput
	}
	bounded, cancel := context.WithTimeout(ctx, engine.limits.OperationTimeout)
	return bounded, cancel, nil
}

func (engine *Engine) validate(ctx context.Context, record Record, final bool, next migration.PhysicalState) (Record, error) {
	result, err := engine.validateResult(ctx, record, final)
	if err != nil {
		return Record{}, err
	}
	return engine.transition(ctx, record, next, Change{ValidationVersionDelta: result.Version - record.ValidationVersion})
}

func (engine *Engine) validateResult(ctx context.Context, record Record, final bool) (ValidationResult, error) {
	if err := engine.shards.CaptureOutbox(ctx, record, engine.limits.ValidationRows); err != nil {
		return ValidationResult{}, err
	}
	result, err := engine.shards.Validate(ctx, ValidationRequest{
		Migration: record,
		MaxRows:   engine.limits.ValidationRows,
		MaxTables: engine.limits.ValidationTables,
		Final:     final,
	})
	if err != nil {
		return ValidationResult{}, err
	}
	if !result.Passed || result.Truncated || result.RowsExamined < 0 ||
		result.RowsExamined > engine.limits.ValidationRows || result.Tables < 0 ||
		result.Tables > engine.limits.ValidationTables || result.Version <= record.ValidationVersion {
		return ValidationResult{}, ErrValidationFailed
	}
	return result, nil
}

func (engine *Engine) transition(ctx context.Context, record Record, next migration.PhysicalState, patch Change) (Record, error) {
	change, err := engine.makeTransition(record, next, patch)
	if err != nil {
		return Record{}, err
	}
	return engine.control.Persist(ctx, change)
}

func (engine *Engine) makeTransition(record Record, next migration.PhysicalState, patch Change) (Change, error) {
	machine, err := migration.RestorePhysical(migration.PhysicalSnapshot{
		State:               record.State,
		Generation:          record.TargetGeneration,
		TargetWriteEvidence: record.TargetWriteCount > 0,
	})
	if err != nil {
		return Change{}, err
	}
	if err := machine.Transition(next); err != nil {
		return Change{}, err
	}
	patch.MigrationID = record.MigrationID
	patch.ExpectedState = record.State
	patch.NextState = next
	return patch, nil
}

func (engine *Engine) replayOneBatch(ctx context.Context, record Record) (Record, error) {
	through := int64(0)
	if record.State == migration.PhysicalStateSourceFenced {
		through = record.FinalSourceSequence
	}
	batch, last, err := engine.readAndApplyJournal(ctx, record, through)
	if err != nil {
		return Record{}, err
	}
	next := record.State
	if record.State == migration.PhysicalStateCatchingUp && last == batch.SourceSequence {
		next = migration.PhysicalStateValidatingOnline
	}
	if record.State == migration.PhysicalStateSourceFenced && last == record.FinalSourceSequence {
		next = migration.PhysicalStateFinalCatchup
	}
	return engine.control.Persist(ctx, Change{
		MigrationID:            record.MigrationID,
		ExpectedState:          record.State,
		NextState:              next,
		ExpectedReplaySequence: int64Ptr(record.LastReplayedSequence),
		LastReplayedSequence:   int64Ptr(last),
		RowsReplayedDelta:      int64(len(batch.Entries)),
	})
}

func (engine *Engine) readAndApplyJournal(ctx context.Context, record Record, through int64) (JournalBatch, int64, error) {
	batch, err := engine.shards.ReadJournal(ctx, JournalRequest{
		Migration:       record,
		AfterSequence:   record.LastReplayedSequence,
		ThroughSequence: through,
		Limit:           engine.limits.JournalBatch,
	})
	if err != nil {
		return JournalBatch{}, 0, err
	}
	if len(batch.Entries) > engine.limits.JournalBatch || batch.SourceSequence < record.LastReplayedSequence {
		return JournalBatch{}, 0, ErrInvalidBatch
	}
	expected := record.LastReplayedSequence + 1
	for _, entry := range batch.Entries {
		if entry.ID == uuid.Nil || entry.EntityID == uuid.Nil || entry.Sequence != expected ||
			entry.TableName == "" || (entry.Operation != "INSERT" && entry.Operation != "UPDATE" && entry.Operation != "DELETE") {
			return JournalBatch{}, 0, ErrJournalGap
		}
		if _, err := engine.shards.ApplyJournal(ctx, record, entry); err != nil {
			return JournalBatch{}, 0, err
		}
		expected++
	}
	last := record.LastReplayedSequence
	if len(batch.Entries) > 0 {
		last = batch.Entries[len(batch.Entries)-1].Sequence
	}
	if last > batch.SourceSequence || (record.State == migration.PhysicalStateSourceFenced && last > record.FinalSourceSequence) {
		return JournalBatch{}, 0, ErrInvalidBatch
	}
	return batch, last, nil
}

func int64Ptr(value int64) *int64 { return &value }

func (engine *Engine) copyOneBatch(ctx context.Context, record Record) (Record, error) {
	batch, err := engine.shards.ReadBaseBatch(ctx, BaseCopyRequest{
		Migration: record,
		Cursor:    record.BaseCopyCursor,
		Limit:     engine.limits.BaseCopyBatch,
	})
	if err != nil {
		return Record{}, err
	}
	if batch.Cursor != record.BaseCopyCursor || batch.Rows < 0 ||
		batch.Rows > engine.limits.BaseCopyBatch || batch.NextCursor == "" ||
		(!batch.Done && (batch.Rows == 0 || batch.NextCursor == batch.Cursor)) {
		return Record{}, ErrInvalidBatch
	}
	if err := engine.shards.ApplyBaseBatch(ctx, record, batch); err != nil {
		return Record{}, err
	}
	nextState := record.State
	if batch.Done {
		nextState = migration.PhysicalStateCatchingUp
	}
	return engine.control.Persist(ctx, Change{
		MigrationID:            record.MigrationID,
		ExpectedState:          record.State,
		NextState:              nextState,
		ExpectedBaseCopyCursor: record.BaseCopyCursor,
		BaseCopyCursor:         batch.NextCursor,
		RowsCopiedDelta:        int64(batch.Rows),
	})
}

func validateRecord(record Record) error {
	if record.MigrationID == uuid.Nil || record.TrainRunID == uuid.Nil ||
		record.SourceShardID == "" || record.TargetShardID == "" ||
		record.SourceShardID == record.TargetShardID || record.SourceGeneration <= 0 ||
		record.TargetGeneration <= record.SourceGeneration || record.State == "" {
		return fmt.Errorf("%w: malformed durable record", ErrInvalidInput)
	}
	if !validEndpointVersion(record.SourceShardID, record.SourceProtocolVersion, record.SourceSchemaVersion) ||
		!validEndpointVersion(record.TargetShardID, record.TargetProtocolVersion, record.TargetSchemaVersion) ||
		(!isPhysicalEndpoint(record.SourceShardID) && !isPhysicalEndpoint(record.TargetShardID)) ||
		(!isPhysicalEndpoint(record.TargetShardID) && !record.ReverseMigration) {
		return fmt.Errorf("%w: incompatible shard protocol or schema", ErrInvalidInput)
	}
	if record.ReverseMigration && (record.RetainedTargetGeneration <= 0 ||
		record.RetainedTargetGeneration >= record.TargetGeneration) {
		return fmt.Errorf("%w: invalid retained predecessor generation", ErrInvalidInput)
	}
	return nil
}

func validEndpointVersion(shardID string, protocolVersion, schemaVersion int) bool {
	if protocolVersion != 1 {
		return false
	}
	switch shardID {
	case "legacy", "shard-0", "shard-1":
		return schemaVersion == 8
	case "physical-shard-0", "physical-shard-1":
		return schemaVersion == 2
	default:
		return false
	}
}

func isPhysicalEndpoint(shardID string) bool {
	return shardID == "physical-shard-0" || shardID == "physical-shard-1"
}
