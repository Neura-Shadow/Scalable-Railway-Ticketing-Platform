package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	maxPersistedCheckpointBytes = 128
	maxPersistedValidationBytes = 8192
	maxRollbackWindowSeconds    = 86400
)

func (tx *Transaction) InsertMigration(ctx context.Context, record control.Record) error {
	if tx == nil || tx.tx == nil || record.State != migration.StatePlanned {
		return control.ErrInvalidRecord
	}
	lastValidation, rollbackGeneration, rollbackWindowSeconds, err := encodeRecord(record)
	if err != nil {
		return err
	}
	commandTag, err := tx.tx.Exec(ctx, `
INSERT INTO public.train_run_shard_migrations (
    id,
    train_run_id,
    source_shard_id,
    target_shard_id,
    source_generation,
    target_generation,
    state,
    copy_checkpoint,
    copied_rows,
    copy_complete,
    rollback_window_seconds,
    rollback_generation,
    last_validation,
    created_at,
    updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
)`,
		record.MigrationID,
		record.TrainRunID,
		record.SourceShard.String(),
		record.TargetShard.String(),
		record.SourceGeneration.Int64(),
		record.TargetGeneration.Int64(),
		string(record.State),
		record.Checkpoint,
		record.CopiedRows,
		record.CopyComplete,
		rollbackWindowSeconds,
		rollbackGeneration,
		lastValidation,
		record.CreatedAt,
		record.UpdatedAt,
	)
	if err != nil || commandTag.RowsAffected() != 1 {
		return ErrPersistence
	}

	commandTag, err = tx.tx.Exec(ctx, `
UPDATE public.train_run_shard_assignments
SET active_migration_id = $2,
    assignment_state = 'draining',
    updated_at = clock_timestamp()
WHERE train_run_id = $1
  AND shard_id = $3
  AND assignment_generation = $4
  AND active_migration_id IS NULL`,
		record.TrainRunID,
		record.MigrationID,
		record.SourceShard.String(),
		record.SourceGeneration.Int64(),
	)
	if err != nil {
		return ErrPersistence
	}
	if commandTag.RowsAffected() != 1 {
		return control.ErrActiveRouteMismatch
	}
	return nil
}

func (tx *Transaction) SaveMigration(ctx context.Context, record control.Record) error {
	if tx == nil || tx.tx == nil {
		return control.ErrInvalidRecord
	}
	lastValidation, rollbackGeneration, rollbackWindowSeconds, err := encodeRecord(record)
	if err != nil {
		return err
	}

	var rawCurrent string
	err = tx.tx.QueryRow(ctx, `
SELECT state
FROM public.train_run_shard_migrations
WHERE id = $1 AND train_run_id = $2
FOR UPDATE`, record.MigrationID, record.TrainRunID).Scan(&rawCurrent)
	if errors.Is(err, pgx.ErrNoRows) {
		return control.ErrMigrationNotFound
	}
	if err != nil {
		return ErrPersistence
	}
	current := migration.State(rawCurrent)
	if !knownMigrationState(current) {
		return control.ErrInvalidRecord
	}
	for _, intermediate := range persistenceBridge(current, record.State) {
		commandTag, err := tx.tx.Exec(ctx, `
UPDATE public.train_run_shard_migrations
SET state = $2
WHERE id = $1`, record.MigrationID, string(intermediate))
		if err != nil || commandTag.RowsAffected() != 1 {
			return ErrPersistence
		}
	}

	validationStatus := "pending"
	var validatedAt any
	if record.LastValidation != nil {
		validationStatus = "failed"
		if record.LastValidation.Passed {
			validationStatus = "passed"
		}
		validatedAt = record.LastValidation.CheckedAt
	}
	commandTag, err := tx.tx.Exec(ctx, `
UPDATE public.train_run_shard_migrations
SET state = $3,
    copy_checkpoint = $4,
    copied_rows = $5,
    copy_complete = $6,
    rollback_window_seconds = $7,
    rollback_generation = $8,
    last_validation = $9,
    validation_status = $10,
    validated_at = $11,
    cutover_at = $12,
    rollback_deadline_at = $13,
    completed_at = CASE WHEN $3 IN ('completed', 'rolled_back') THEN $14 ELSE completed_at END,
    updated_at = $14
WHERE id = $1 AND train_run_id = $2`,
		record.MigrationID,
		record.TrainRunID,
		string(record.State),
		record.Checkpoint,
		record.CopiedRows,
		record.CopyComplete,
		rollbackWindowSeconds,
		rollbackGeneration,
		lastValidation,
		validationStatus,
		validatedAt,
		record.CutoverAt,
		record.RollbackDeadline,
		record.UpdatedAt,
	)
	if err != nil {
		return ErrPersistence
	}
	if commandTag.RowsAffected() != 1 {
		return control.ErrMigrationNotFound
	}

	assignmentState, terminal := assignmentStateForMigration(record.State)
	if terminal {
		commandTag, err = tx.tx.Exec(ctx, `
UPDATE public.train_run_shard_assignments
SET assignment_state = 'stable',
    active_migration_id = NULL,
    updated_at = clock_timestamp()
WHERE train_run_id = $1 AND active_migration_id = $2`, record.TrainRunID, record.MigrationID)
	} else {
		commandTag, err = tx.tx.Exec(ctx, `
UPDATE public.train_run_shard_assignments
SET assignment_state = $3,
    updated_at = clock_timestamp()
WHERE train_run_id = $1 AND active_migration_id = $2`, record.TrainRunID, record.MigrationID, assignmentState)
	}
	if err != nil {
		return ErrPersistence
	}
	if commandTag.RowsAffected() != 1 {
		return control.ErrActiveRouteMismatch
	}
	return nil
}

func persistenceBridge(current, desired migration.State) []migration.State {
	switch {
	case current == migration.StatePlanned && desired == migration.StateCopying:
		return []migration.State{migration.StateDraining}
	case current == migration.StatePlanned && desired == migration.StateValidating:
		return []migration.State{migration.StateDraining, migration.StateCopying}
	case current == migration.StateDraining && desired == migration.StateValidating:
		return []migration.State{migration.StateCopying}
	case current == migration.StateCutoverReady && desired == migration.StateRollbackWindow:
		return []migration.State{migration.StateCuttingOver}
	default:
		return nil
	}
}

func assignmentStateForMigration(state migration.State) (string, bool) {
	switch state {
	case migration.StatePlanned, migration.StateDraining:
		return "draining", false
	case migration.StateCopying, migration.StateValidating, migration.StateCutoverReady, migration.StateCuttingOver:
		return "migrating", false
	case migration.StateRollbackWindow:
		return "rollback_window", false
	case migration.StateCompleted, migration.StateFailed, migration.StateRolledBack:
		return "stable", true
	default:
		return "", false
	}
}

func (tx *Transaction) FindMigrationForUpdate(ctx context.Context, migrationID uuid.UUID) (control.Record, bool, error) {
	if tx == nil || tx.tx == nil || migrationID == uuid.Nil {
		return control.Record{}, false, control.ErrInvalidInput
	}
	var (
		record                      control.Record
		rawSourceShard              string
		rawTargetShard              string
		rawSourceGeneration         int64
		rawTargetGeneration         int64
		rollbackWindowSeconds       int64
		rawState                    string
		rollbackGeneration          pgtype.Int8
		lastValidation              []byte
		cutoverAt, rollbackDeadline pgtype.Timestamptz
	)
	err := tx.tx.QueryRow(ctx, `
SELECT id,
       train_run_id,
       source_shard_id,
       target_shard_id,
       source_generation,
       target_generation,
       rollback_window_seconds::bigint,
       state,
       copy_checkpoint,
       copied_rows,
       copy_complete,
       rollback_generation,
       last_validation,
       created_at,
       updated_at,
       cutover_at,
       rollback_deadline_at
FROM public.train_run_shard_migrations
WHERE id = $1
FOR UPDATE`, migrationID).Scan(
		&record.MigrationID,
		&record.TrainRunID,
		&rawSourceShard,
		&rawTargetShard,
		&rawSourceGeneration,
		&rawTargetGeneration,
		&rollbackWindowSeconds,
		&rawState,
		&record.Checkpoint,
		&record.CopiedRows,
		&record.CopyComplete,
		&rollbackGeneration,
		&lastValidation,
		&record.CreatedAt,
		&record.UpdatedAt,
		&cutoverAt,
		&rollbackDeadline,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return control.Record{}, false, nil
	}
	if err != nil {
		return control.Record{}, false, ErrPersistence
	}

	sourceShard, err := sharding.ParseShardID(rawSourceShard)
	if err != nil {
		return control.Record{}, false, control.ErrInvalidRecord
	}
	targetShard, err := sharding.ParseShardID(rawTargetShard)
	if err != nil || sourceShard == targetShard {
		return control.Record{}, false, control.ErrInvalidRecord
	}
	sourceGeneration, err := sharding.NewAssignmentGeneration(rawSourceGeneration)
	if err != nil {
		return control.Record{}, false, control.ErrInvalidRecord
	}
	targetGeneration, err := sharding.NewAssignmentGeneration(rawTargetGeneration)
	if err != nil || targetGeneration <= sourceGeneration {
		return control.Record{}, false, control.ErrInvalidRecord
	}
	state := migration.State(rawState)
	if !knownMigrationState(state) || rollbackWindowSeconds <= 0 {
		return control.Record{}, false, control.ErrInvalidRecord
	}

	record.SourceShard = sourceShard
	record.TargetShard = targetShard
	record.SourceGeneration = sourceGeneration
	record.TargetGeneration = targetGeneration
	record.RollbackWindow = time.Duration(rollbackWindowSeconds) * time.Second
	record.State = state
	if rollbackGeneration.Valid {
		generation, err := sharding.NewAssignmentGeneration(rollbackGeneration.Int64)
		if err != nil || generation <= targetGeneration {
			return control.Record{}, false, control.ErrInvalidRecord
		}
		record.RollbackGeneration = &generation
	}
	if len(lastValidation) > 0 {
		var outcome control.ValidationOutcome
		if err := json.Unmarshal(lastValidation, &outcome); err != nil {
			return control.Record{}, false, control.ErrInvalidRecord
		}
		record.LastValidation = &outcome
	}
	if cutoverAt.Valid {
		value := cutoverAt.Time
		record.CutoverAt = &value
	}
	if rollbackDeadline.Valid {
		value := rollbackDeadline.Time
		record.RollbackDeadline = &value
	}
	return record, true, nil
}

func knownMigrationState(state migration.State) bool {
	switch state {
	case migration.StatePlanned,
		migration.StateDraining,
		migration.StateCopying,
		migration.StateValidating,
		migration.StateCutoverReady,
		migration.StateCuttingOver,
		migration.StateRollbackWindow,
		migration.StateCompleted,
		migration.StateFailed,
		migration.StateRolledBack:
		return true
	default:
		return false
	}
}

func encodeRecord(record control.Record) ([]byte, any, int64, error) {
	if record.MigrationID == uuid.Nil || record.TrainRunID == uuid.Nil ||
		record.SourceShard == record.TargetShard || record.SourceGeneration.Int64() <= 0 ||
		record.TargetGeneration.Int64() <= record.SourceGeneration.Int64() ||
		!knownMigrationState(record.State) || record.RollbackWindow <= 0 ||
		record.RollbackWindow%time.Second != 0 || record.CopiedRows < 0 ||
		len(record.Checkpoint) > maxPersistedCheckpointBytes || record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() {
		return nil, nil, 0, control.ErrInvalidRecord
	}
	if _, err := sharding.ParseShardID(record.SourceShard.String()); err != nil {
		return nil, nil, 0, control.ErrInvalidRecord
	}
	if _, err := sharding.ParseShardID(record.TargetShard.String()); err != nil {
		return nil, nil, 0, control.ErrInvalidRecord
	}
	rollbackWindowSeconds := int64(record.RollbackWindow / time.Second)
	if rollbackWindowSeconds <= 0 || rollbackWindowSeconds > maxRollbackWindowSeconds {
		return nil, nil, 0, control.ErrInvalidRecord
	}
	var rollbackGeneration any
	if record.RollbackGeneration != nil {
		if record.RollbackGeneration.Int64() <= record.TargetGeneration.Int64() {
			return nil, nil, 0, control.ErrInvalidRecord
		}
		rollbackGeneration = record.RollbackGeneration.Int64()
	}
	var lastValidation []byte
	if record.LastValidation != nil {
		encoded, err := json.Marshal(record.LastValidation)
		if err != nil || len(encoded) > maxPersistedValidationBytes {
			return nil, nil, 0, control.ErrInvalidRecord
		}
		lastValidation = encoded
	}
	return lastValidation, rollbackGeneration, rollbackWindowSeconds, nil
}
