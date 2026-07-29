package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Control struct{ db DB }

func NewControl(db DB) (*Control, error) {
	if db == nil {
		return nil, physicalmigration.ErrInvalidInput
	}
	return &Control{db: db}, nil
}

func (control *Control) Load(ctx context.Context, migrationID uuid.UUID) (physicalmigration.Record, error) {
	if migrationID == uuid.Nil {
		return physicalmigration.Record{}, physicalmigration.ErrInvalidInput
	}
	return scanRecord(control.db.QueryRow(ctx, loadMigrationSQL, migrationID))
}

func (control *Control) Persist(ctx context.Context, change physicalmigration.Change) (physicalmigration.Record, error) {
	if change.MigrationID == uuid.Nil || change.ExpectedState == "" || change.NextState == "" {
		return physicalmigration.Record{}, physicalmigration.ErrInvalidInput
	}
	tx, err := control.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return physicalmigration.Record{}, fmt.Errorf("%w: begin control checkpoint", physicalmigration.ErrCheckpointConflict)
	}
	rollback := func(result error) (physicalmigration.Record, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return physicalmigration.Record{}, result
	}
	record, err := scanRecord(tx.QueryRow(ctx, loadMigrationForUpdateSQL, change.MigrationID))
	if err != nil {
		return rollback(err)
	}
	if record.State != change.ExpectedState ||
		(record.State == migration.PhysicalStateBaseCopying && record.BaseCopyCursor != change.ExpectedBaseCopyCursor) ||
		(change.ExpectedReplaySequence != nil && record.LastReplayedSequence != *change.ExpectedReplaySequence) {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	record = physicalmigration.ApplyChange(record, change)
	if err := saveRecord(ctx, tx, record, change.ExpectedState); err != nil {
		return rollback(err)
	}
	if change.BaseCopyCursor != "" {
		status := "running"
		if record.State != migration.PhysicalStateBaseCopying {
			status = "completed"
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO public.physical_shard_migration_checkpoints (
    migration_id, checkpoint_kind, object_name, cursor_value,
    rows_processed, status, completed_at
) VALUES ($1, 'base_copy', 'migration_engine', $2, $3, $4,
          CASE WHEN $4 = 'completed' THEN clock_timestamp() END)
ON CONFLICT (migration_id, checkpoint_kind, object_name) DO UPDATE
SET cursor_value = EXCLUDED.cursor_value,
    rows_processed = EXCLUDED.rows_processed,
    status = EXCLUDED.status,
    completed_at = EXCLUDED.completed_at,
    bounded_error_category = NULL`, record.MigrationID, record.BaseCopyCursor, record.RowsCopied, status); err != nil {
			return rollback(fmt.Errorf("%w: save base-copy checkpoint", physicalmigration.ErrCheckpointConflict))
		}
	}
	if record.State == migration.PhysicalStateCaptureEnabled && change.ExpectedState == migration.PhysicalStatePreparingTarget {
		if _, err := tx.Exec(ctx, `
INSERT INTO public.physical_shard_migration_checkpoints (
    migration_id, checkpoint_kind, object_name, cursor_value,
    rows_processed, status, completed_at
) VALUES ($1, 'target_prepare', 'migration_engine', 'prepared', 0, 'completed', clock_timestamp())
ON CONFLICT (migration_id, checkpoint_kind, object_name) DO UPDATE
SET cursor_value = 'prepared', status = 'completed',
    completed_at = EXCLUDED.completed_at, bounded_error_category = NULL`, record.MigrationID); err != nil {
			return rollback(fmt.Errorf("%w: save target-prepare checkpoint", physicalmigration.ErrCheckpointConflict))
		}
	}
	if change.LastReplayedSequence != nil {
		status := "running"
		if record.State == migration.PhysicalStateFinalCatchup ||
			record.State == migration.PhysicalStateFinalValidating ||
			record.State == migration.PhysicalStateTargetEnabled ||
			record.State == migration.PhysicalStateSwitchingAssignment ||
			record.State == migration.PhysicalStateRollbackWindow ||
			record.State == migration.PhysicalStateCompleted {
			status = "completed"
		}
		sourceSequence := record.LastReplayedSequence
		if record.FinalSourceSequence > 0 {
			sourceSequence = record.FinalSourceSequence
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO public.physical_shard_migration_checkpoints (
    migration_id, checkpoint_kind, object_name, cursor_value,
    source_sequence, target_sequence, rows_processed, status, completed_at
) VALUES ($1, 'journal_replay', 'migration_engine', $2, $3, $4, $5, $6,
          CASE WHEN $6 = 'completed' THEN clock_timestamp() END)
ON CONFLICT (migration_id, checkpoint_kind, object_name) DO UPDATE
SET cursor_value = EXCLUDED.cursor_value,
    source_sequence = EXCLUDED.source_sequence,
    target_sequence = EXCLUDED.target_sequence,
    rows_processed = EXCLUDED.rows_processed,
    status = EXCLUDED.status,
    completed_at = EXCLUDED.completed_at,
    bounded_error_category = NULL`, record.MigrationID, fmt.Sprint(record.LastReplayedSequence),
			sourceSequence, record.LastReplayedSequence, record.RowsReplayed, status); err != nil {
			return rollback(fmt.Errorf("%w: save journal checkpoint", physicalmigration.ErrCheckpointConflict))
		}
	}
	if change.ValidationVersionDelta > 0 {
		kind := "online_validation"
		if record.State == migration.PhysicalStateFinalValidating {
			kind = "final_validation"
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO public.physical_shard_migration_checkpoints (
    migration_id, checkpoint_kind, object_name, cursor_value,
    rows_processed, status, completed_at
) VALUES ($1, $2, 'migration_engine', $3, 0, 'completed', clock_timestamp())
ON CONFLICT (migration_id, checkpoint_kind, object_name) DO UPDATE
SET cursor_value = EXCLUDED.cursor_value,
    status = 'completed', completed_at = EXCLUDED.completed_at,
    bounded_error_category = NULL`, record.MigrationID, kind, fmt.Sprint(record.ValidationVersion)); err != nil {
			return rollback(fmt.Errorf("%w: save validation checkpoint", physicalmigration.ErrCheckpointConflict))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return physicalmigration.Record{}, fmt.Errorf("%w: commit control checkpoint", physicalmigration.ErrCheckpointConflict)
	}
	return record, nil
}

func (control *Control) BeginDrain(ctx context.Context, change physicalmigration.Change) (physicalmigration.Record, error) {
	if change.ExpectedState != migration.PhysicalStateValidatingOnline || change.NextState != migration.PhysicalStateDraining {
		return physicalmigration.Record{}, physicalmigration.ErrInvalidInput
	}
	tx, err := control.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return physicalmigration.Record{}, physicalmigration.ErrCheckpointConflict
	}
	rollback := func(result error) (physicalmigration.Record, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return physicalmigration.Record{}, result
	}
	record, err := scanRecord(tx.QueryRow(ctx, loadMigrationForUpdateSQL, change.MigrationID))
	if err != nil || record.State != change.ExpectedState {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.train_run_shard_assignments
SET assignment_state = 'draining'
WHERE train_run_id = $1
  AND shard_id = $2
  AND assignment_generation = $3
  AND active_physical_migration_id = $4
  AND assignment_state IN ('migrating', 'draining')`, record.TrainRunID,
		record.SourceShardID, record.SourceGeneration, record.MigrationID)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	record = physicalmigration.ApplyChange(record, change)
	if err := saveRecord(ctx, tx, record, change.ExpectedState); err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.physical_shard_migration_checkpoints (
    migration_id, checkpoint_kind, object_name, cursor_value,
    rows_processed, status, completed_at
) VALUES ($1, 'online_validation', 'migration_engine', $2, 0, 'completed', clock_timestamp())
ON CONFLICT (migration_id, checkpoint_kind, object_name) DO UPDATE
SET cursor_value = EXCLUDED.cursor_value, status = 'completed',
    completed_at = EXCLUDED.completed_at, bounded_error_category = NULL`, record.MigrationID,
		fmt.Sprint(record.ValidationVersion)); err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return physicalmigration.Record{}, physicalmigration.ErrCheckpointConflict
	}
	return record, nil
}

func (control *Control) SwitchAssignment(ctx context.Context, change physicalmigration.Change) (physicalmigration.Record, error) {
	tx, err := control.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return physicalmigration.Record{}, fmt.Errorf("%w: begin assignment switch", physicalmigration.ErrCheckpointConflict)
	}
	rollback := func(result error) (physicalmigration.Record, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return physicalmigration.Record{}, result
	}
	record, err := scanRecord(tx.QueryRow(ctx, loadMigrationForUpdateSQL, change.MigrationID))
	if err != nil {
		return rollback(err)
	}
	if record.State != change.ExpectedState || change.NextState != migration.PhysicalStateSwitchingAssignment {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	var shardID string
	var generation int64
	var activeID uuid.UUID
	if err := tx.QueryRow(ctx, `
SELECT shard_id, assignment_generation, active_physical_migration_id
FROM public.train_run_shard_assignments
WHERE train_run_id = $1
FOR UPDATE`, record.TrainRunID).Scan(&shardID, &generation, &activeID); err != nil {
		return rollback(fmt.Errorf("%w: lock assignment", physicalmigration.ErrCheckpointConflict))
	}
	if shardID != record.SourceShardID || generation != record.SourceGeneration || activeID != record.MigrationID {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	var directoryMismatches int64
	if err := tx.QueryRow(ctx, `
SELECT count(*)
FROM (
    SELECT directory.reservation_id
    FROM public.reservation_directory AS directory
    WHERE directory.train_run_id = $1
      AND directory.state IN ('pending', 'active', 'moving')
      AND (directory.last_known_shard_id IS DISTINCT FROM $2
           OR directory.last_known_generation IS DISTINCT FROM $3)
    UNION ALL
    SELECT command.reservation_id
    FROM public.booking_commands AS command
    LEFT JOIN public.reservation_directory AS directory
      ON directory.command_id = command.command_id
     AND directory.reservation_id = command.reservation_id
    WHERE command.train_run_id = $1
      AND command.state NOT IN ('failed', 'expired')
      AND directory.reservation_id IS NULL
	UNION ALL
	SELECT locator.reservation_id
	FROM public.reservation_shard_locators AS locator
	WHERE locator.train_run_id = $1
	  AND (locator.shard_id IS DISTINCT FROM $2
	       OR locator.assignment_generation IS DISTINCT FROM $3)
	UNION ALL
	SELECT locator.ticket_order_id
	FROM public.ticket_order_shard_locators AS locator
	WHERE locator.train_run_id = $1
	  AND (locator.shard_id IS DISTINCT FROM $2
	       OR locator.assignment_generation IS DISTINCT FROM $3)
	UNION ALL
	SELECT locator.ticket_id
	FROM public.ticket_shard_locators AS locator
	WHERE locator.train_run_id = $1
	  AND (locator.shard_id IS DISTINCT FROM $2
	       OR locator.assignment_generation IS DISTINCT FROM $3)
) AS mismatches`, record.TrainRunID,
		record.SourceShardID, record.SourceGeneration).Scan(&directoryMismatches); err != nil || directoryMismatches != 0 {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.reservation_directory
SET last_known_shard_id = $2,
    last_known_generation = $3,
    state = CASE WHEN state = 'moving' THEN 'active' ELSE state END,
    bounded_error_category = NULL
WHERE train_run_id = $1
  AND state IN ('pending', 'active', 'moving')`, record.TrainRunID, record.TargetShardID,
		record.TargetGeneration); err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.booking_commands
SET target_shard_id = $2,
    assignment_generation = $3
WHERE train_run_id = $1
  AND state NOT IN ('failed', 'expired')`, record.TrainRunID, record.TargetShardID,
		record.TargetGeneration); err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	for _, table := range []string{
		"reservation_shard_locators", "ticket_order_shard_locators", "ticket_shard_locators",
	} {
		statement := fmt.Sprintf(`
UPDATE public.%s
SET shard_id = $2, assignment_generation = $3
WHERE train_run_id = $1`, table)
		if _, err := tx.Exec(ctx, statement, record.TrainRunID, record.TargetShardID,
			record.TargetGeneration); err != nil {
			return rollback(physicalmigration.ErrCheckpointConflict)
		}
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.train_run_shard_assignments
SET shard_id = $2,
    assignment_generation = $3,
    assignment_state = 'rollback_window',
    availability_generation = availability_generation + 1
WHERE train_run_id = $1
  AND shard_id = $4
  AND assignment_generation = $5
  AND active_physical_migration_id = $6`, record.TrainRunID, record.TargetShardID,
		record.TargetGeneration, record.SourceShardID, record.SourceGeneration, record.MigrationID)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	record = physicalmigration.ApplyChange(record, change)
	if err := saveRecord(ctx, tx, record, change.ExpectedState); err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.physical_shard_migrations
SET assignment_switched_at = COALESCE(assignment_switched_at, clock_timestamp()),
    rollback_deadline_at = COALESCE(
        rollback_deadline_at,
        clock_timestamp() + make_interval(secs => rollback_window_seconds)
    ),
    source_retention_until = COALESCE(
        source_retention_until,
        clock_timestamp() + make_interval(secs => rollback_window_seconds)
    )
WHERE migration_id = $1`, record.MigrationID); err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	eventType := "physical_shard_migration.cutover"
	if record.ReverseMigration {
		eventType = "physical_shard_migration.reverse_cutover"
	}
	eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(record.MigrationID.String()+":"+eventType))
	if _, err := tx.Exec(ctx, `
INSERT INTO public.outbox_events (
    id, aggregate_type, aggregate_id, event_type, event_version,
    payload, status, attempts, next_attempt_at
) VALUES (
    $1, 'physical_shard_migration', $2, $3, 1,
    jsonb_build_object(
        'migration_id', $2::uuid,
        'train_run_id', $4::uuid,
        'source_shard_id', $5::text,
        'target_shard_id', $6::text,
        'source_generation', $7::bigint,
        'target_generation', $8::bigint
    ),
    'pending', 0, clock_timestamp()
)
ON CONFLICT (id) DO NOTHING`, eventID, record.MigrationID, eventType,
		record.TrainRunID, record.SourceShardID, record.TargetShardID,
		record.SourceGeneration, record.TargetGeneration); err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return physicalmigration.Record{}, physicalmigration.ErrCheckpointConflict
	}
	return record, nil
}

// Rollback restores the control route only after the shard adapter has
// committed target-disable followed by source-enable. This control transaction
// cannot create a second writer even if it is retried after a crash.
func (control *Control) Rollback(ctx context.Context, change physicalmigration.Change) (physicalmigration.Record, error) {
	if change.NextState != migration.PhysicalStateRolledBack {
		return physicalmigration.Record{}, physicalmigration.ErrInvalidInput
	}
	tx, err := control.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return physicalmigration.Record{}, physicalmigration.ErrCheckpointConflict
	}
	rollback := func(result error) (physicalmigration.Record, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return physicalmigration.Record{}, result
	}
	record, err := scanRecord(tx.QueryRow(ctx, loadMigrationForUpdateSQL, change.MigrationID))
	if err != nil {
		return rollback(err)
	}
	if record.State != change.ExpectedState {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	if record.State == migration.PhysicalStatePlanned || record.State == migration.PhysicalStatePreparingTarget {
		if change.RollbackGeneration != 0 {
			return rollback(physicalmigration.ErrCheckpointConflict)
		}
		tag, err := tx.Exec(ctx, `
UPDATE public.train_run_shard_assignments
SET assignment_state = 'stable', active_physical_migration_id = NULL
WHERE train_run_id = $1
  AND shard_id = $2
  AND assignment_generation = $3
  AND active_physical_migration_id = $4`, record.TrainRunID, record.SourceShardID,
			record.SourceGeneration, record.MigrationID)
		if err != nil || tag.RowsAffected() != 1 {
			return rollback(physicalmigration.ErrCheckpointConflict)
		}
		record = physicalmigration.ApplyChange(record, change)
		if err := saveRecord(ctx, tx, record, change.ExpectedState); err != nil {
			return rollback(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return physicalmigration.Record{}, physicalmigration.ErrCheckpointConflict
		}
		return record, nil
	}
	var shardID string
	var generation int64
	var activeID uuid.UUID
	if err := tx.QueryRow(ctx, `
SELECT shard_id, assignment_generation, active_physical_migration_id
FROM public.train_run_shard_assignments
WHERE train_run_id = $1
FOR UPDATE`, record.TrainRunID).Scan(&shardID, &generation, &activeID); err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	expectedShardID := record.SourceShardID
	expectedGeneration := record.SourceGeneration
	if record.State == migration.PhysicalStateSwitchingAssignment || record.State == migration.PhysicalStateRollbackWindow {
		expectedShardID = record.TargetShardID
		expectedGeneration = record.TargetGeneration
	}
	if change.RollbackGeneration <= record.TargetGeneration || activeID != record.MigrationID ||
		shardID != expectedShardID || generation != expectedGeneration {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.train_run_shard_assignments
SET shard_id = $2,
    assignment_generation = $3,
    assignment_state = 'stable',
    active_physical_migration_id = NULL,
    availability_generation = availability_generation + 1
WHERE train_run_id = $1
  AND shard_id = $5
  AND assignment_generation = $6
  AND active_physical_migration_id = $4`, record.TrainRunID, record.SourceShardID,
		change.RollbackGeneration, record.MigrationID, expectedShardID, expectedGeneration)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	record = physicalmigration.ApplyChange(record, change)
	if err := saveRecord(ctx, tx, record, change.ExpectedState); err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.physical_shard_migrations
SET rollback_assignment_generation = $2
WHERE migration_id = $1`, record.MigrationID, change.RollbackGeneration); err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.reservation_directory
SET last_known_shard_id = $2,
    last_known_generation = $3,
    state = CASE WHEN state = 'moving' THEN 'active' ELSE state END,
    bounded_error_category = NULL
WHERE train_run_id = $1`, record.TrainRunID, record.SourceShardID,
		change.RollbackGeneration); err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.booking_commands
SET target_shard_id = $2,
    assignment_generation = $3
WHERE train_run_id = $1`, record.TrainRunID, record.SourceShardID,
		change.RollbackGeneration); err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	for _, table := range []string{
		"reservation_shard_locators", "ticket_order_shard_locators", "ticket_shard_locators",
	} {
		statement := fmt.Sprintf(`
UPDATE public.%s
SET shard_id = $2, assignment_generation = $3
WHERE train_run_id = $1`, table)
		if _, err := tx.Exec(ctx, statement, record.TrainRunID, record.SourceShardID,
			change.RollbackGeneration); err != nil {
			return rollback(physicalmigration.ErrCheckpointConflict)
		}
	}
	eventType := "physical_shard_migration.rolled_back"
	eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(record.MigrationID.String()+":"+eventType))
	if _, err := tx.Exec(ctx, `
INSERT INTO public.outbox_events (
    id, aggregate_type, aggregate_id, event_type, event_version,
    payload, status, attempts, next_attempt_at
) VALUES (
    $1, 'physical_shard_migration', $2, $3, 1,
    jsonb_build_object(
        'migration_id', $2::uuid,
        'train_run_id', $4::uuid,
        'source_shard_id', $5::text,
        'rollback_generation', $6::bigint
    ),
    'pending', 0, clock_timestamp()
)
ON CONFLICT (id) DO NOTHING`, eventID, record.MigrationID, eventType,
		record.TrainRunID, record.SourceShardID, change.RollbackGeneration); err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return physicalmigration.Record{}, physicalmigration.ErrCheckpointConflict
	}
	return record, nil
}

func (control *Control) CompletionEligible(ctx context.Context, migrationID uuid.UUID) error {
	var eligible bool
	if err := control.db.QueryRow(ctx, `
SELECT state = 'rollback_window'
   AND rollback_deadline_at IS NOT NULL
   AND rollback_deadline_at <= clock_timestamp()
   AND source_retention_until IS NOT NULL
   AND source_retention_until >= rollback_deadline_at
FROM public.physical_shard_migrations
WHERE migration_id = $1`, migrationID).Scan(&eligible); err != nil {
		return physicalmigration.ErrCheckpointConflict
	}
	if !eligible {
		return physicalmigration.ErrRetentionWindowOpen
	}
	return nil
}

func (control *Control) Complete(ctx context.Context, change physicalmigration.Change) (physicalmigration.Record, error) {
	if change.ExpectedState != migration.PhysicalStateRollbackWindow || change.NextState != migration.PhysicalStateCompleted {
		return physicalmigration.Record{}, physicalmigration.ErrInvalidInput
	}
	tx, err := control.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return physicalmigration.Record{}, physicalmigration.ErrCheckpointConflict
	}
	rollback := func(result error) (physicalmigration.Record, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return physicalmigration.Record{}, result
	}
	record, err := scanRecord(tx.QueryRow(ctx, loadMigrationForUpdateSQL, change.MigrationID))
	if err != nil || record.State != change.ExpectedState {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.train_run_shard_assignments
SET assignment_state = 'stable', active_physical_migration_id = NULL
WHERE train_run_id = $1
  AND shard_id = $2
  AND assignment_generation = $3
  AND active_physical_migration_id = $4
  AND EXISTS (
      SELECT 1 FROM public.physical_shard_migrations
      WHERE migration_id = $4
        AND rollback_deadline_at <= clock_timestamp()
  )`, record.TrainRunID, record.TargetShardID, record.TargetGeneration, record.MigrationID)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(physicalmigration.ErrRetentionWindowOpen)
	}
	record = physicalmigration.ApplyChange(record, change)
	if err := saveRecord(ctx, tx, record, change.ExpectedState); err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.physical_shard_migrations
SET completed_at = COALESCE(completed_at, clock_timestamp())
WHERE migration_id = $1`, record.MigrationID); err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	eventType := "physical_shard_migration.completed"
	eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(record.MigrationID.String()+":"+eventType))
	if _, err := tx.Exec(ctx, `
INSERT INTO public.outbox_events (
    id, aggregate_type, aggregate_id, event_type, event_version,
    payload, status, attempts, next_attempt_at
) VALUES (
    $1, 'physical_shard_migration', $2, $3, 1,
    jsonb_build_object('migration_id', $2::uuid, 'train_run_id', $4::uuid),
    'pending', 0, clock_timestamp()
)
ON CONFLICT (id) DO NOTHING`, eventID, record.MigrationID, eventType, record.TrainRunID); err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return physicalmigration.Record{}, physicalmigration.ErrCheckpointConflict
	}
	return record, nil
}

func (control *Control) CreateReverse(ctx context.Context, plan physicalmigration.ReversePlan) (physicalmigration.Record, error) {
	if plan.OriginalMigrationID == uuid.Nil || plan.MigrationID == uuid.Nil ||
		plan.TargetGeneration <= 0 || plan.ObservedTargetWrites < 0 {
		return physicalmigration.Record{}, physicalmigration.ErrInvalidInput
	}
	tx, err := control.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return physicalmigration.Record{}, physicalmigration.ErrCheckpointConflict
	}
	rollback := func(result error) (physicalmigration.Record, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return physicalmigration.Record{}, result
	}
	original, err := scanRecord(tx.QueryRow(ctx, loadMigrationForUpdateSQL, plan.OriginalMigrationID))
	if err != nil {
		return rollback(err)
	}
	var cleanupState string
	if err := tx.QueryRow(ctx, `
SELECT cleanup_state
FROM public.physical_shard_migrations
WHERE migration_id = $1
FOR UPDATE`, original.MigrationID).Scan(&cleanupState); err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	if cleanupState == "running" || cleanupState == "completed" {
		return rollback(physicalmigration.ErrCleanupConflict)
	}
	if plan.TargetGeneration <= original.TargetGeneration ||
		(original.State != migration.PhysicalStateRollbackWindow && original.State != migration.PhysicalStateCompleted && original.State != migration.PhysicalStateReverseMigrationRequired) {
		return rollback(physicalmigration.ErrGenerationNotNewer)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.physical_shard_migrations
SET state = 'reverse_migration_required',
    target_successful_write_count = GREATEST(target_successful_write_count, $2)
WHERE migration_id = $1`, original.MigrationID, plan.ObservedTargetWrites); err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	_, err = tx.Exec(ctx, `
INSERT INTO public.physical_shard_migrations (
    migration_id, parent_migration_id, train_run_id,
    source_shard_id, target_shard_id, source_generation, target_generation,
    reverse_migration, state, rollback_window_seconds
) VALUES ($1, $2, $3, $4, $5, $6, $7, true, 'planned', 300)
ON CONFLICT (migration_id) DO NOTHING`, plan.MigrationID,
		original.MigrationID, original.TrainRunID, original.TargetShardID, original.SourceShardID,
		original.TargetGeneration, plan.TargetGeneration)
	if err != nil {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	reverse, err := scanRecord(tx.QueryRow(ctx, loadMigrationForUpdateSQL, plan.MigrationID))
	if err != nil || reverse.ParentMigrationID != original.MigrationID || reverse.TrainRunID != original.TrainRunID ||
		reverse.SourceShardID != original.TargetShardID || reverse.TargetShardID != original.SourceShardID ||
		reverse.SourceGeneration != original.TargetGeneration || reverse.TargetGeneration != plan.TargetGeneration ||
		!reverse.ReverseMigration || reverse.State != migration.PhysicalStatePlanned {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.train_run_shard_assignments
SET active_physical_migration_id = $2,
    assignment_state = 'migrating'
WHERE train_run_id = $1
  AND shard_id = $3
  AND assignment_generation = $4
  AND (active_physical_migration_id IS NULL
       OR active_physical_migration_id IN ($5, $2))
  AND assignment_state IN ('rollback_window', 'migrating')`, original.TrainRunID, plan.MigrationID,
		original.TargetShardID, original.TargetGeneration, original.MigrationID)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return physicalmigration.Record{}, physicalmigration.ErrCheckpointConflict
	}
	return reverse, nil
}

// BeginCleanup serializes retained-source deletion against reverse planning by
// locking the same parent row CreateReverse locks. Any existing reverse child
// keeps the retained predecessor unavailable for destructive cleanup.
func (control *Control) BeginCleanup(ctx context.Context, migrationID uuid.UUID, confirmationHash [32]byte) error {
	if migrationID == uuid.Nil || confirmationHash == ([32]byte{}) {
		return physicalmigration.ErrInvalidInput
	}
	tx, err := control.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return physicalmigration.ErrCleanupConflict
	}
	rollback := func(result error) error {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	var state migration.PhysicalState
	var cleanupState string
	var eligible bool
	var storedConfirmation []byte
	if err := tx.QueryRow(ctx, `
SELECT state, cleanup_state,
       source_retention_until IS NOT NULL
       AND source_retention_until <= clock_timestamp(),
	   cleanup_confirmation_hash
FROM public.physical_shard_migrations
WHERE migration_id = $1
FOR UPDATE`, migrationID).Scan(&state, &cleanupState, &eligible, &storedConfirmation); err != nil {
		return rollback(physicalmigration.ErrCleanupConflict)
	}
	if state != migration.PhysicalStateCompleted || !eligible ||
		(cleanupState != "not_requested" && cleanupState != "eligible" && cleanupState != "confirmed" && cleanupState != "running") {
		return rollback(physicalmigration.ErrCleanupConflict)
	}
	if cleanupState == "running" && !bytes.Equal(storedConfirmation, confirmationHash[:]) {
		return rollback(physicalmigration.ErrCleanupConflict)
	}
	var reverseExists bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM public.physical_shard_migrations
    WHERE parent_migration_id = $1
      AND reverse_migration
      AND state NOT IN ('failed', 'rolled_back')
)`, migrationID).Scan(&reverseExists); err != nil || reverseExists {
		return rollback(physicalmigration.ErrCleanupConflict)
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.physical_shard_migrations
SET cleanup_state = 'running', cleanup_confirmation_hash = $2
WHERE migration_id = $1
  AND cleanup_state = $3`, migrationID, confirmationHash[:], cleanupState)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(physicalmigration.ErrCleanupConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return physicalmigration.ErrCleanupConflict
	}
	return nil
}

const loadMigrationSQL = `
SELECT migration.migration_id,
       COALESCE(migration.parent_migration_id::text, ''),
       migration.train_run_id, migration.source_shard_id,
       migration.target_shard_id, migration.source_generation,
       migration.target_generation,
       COALESCE(parent.source_generation, 0),
       source_shard.protocol_version, source_shard.schema_version,
       target_shard.protocol_version, target_shard.schema_version,
       migration.state,
       COALESCE(checkpoint.cursor_value, ''), migration.rows_copied,
       COALESCE(migration.source_journal_start_sequence, 0),
       COALESCE(migration.last_replayed_sequence, 0),
       COALESCE(migration.final_source_sequence, 0), migration.rows_replayed,
       migration.validation_version, migration.target_successful_write_count,
       migration.reverse_migration
FROM public.physical_shard_migrations AS migration
LEFT JOIN public.physical_shard_migrations AS parent
  ON parent.migration_id = migration.parent_migration_id
JOIN public.booking_shards AS source_shard
  ON source_shard.shard_id = migration.source_shard_id
JOIN public.booking_shards AS target_shard
  ON target_shard.shard_id = migration.target_shard_id
LEFT JOIN public.physical_shard_migration_checkpoints AS checkpoint
  ON checkpoint.migration_id = migration.migration_id
 AND checkpoint.checkpoint_kind = 'base_copy'
 AND checkpoint.object_name = 'migration_engine'
WHERE migration.migration_id = $1`

const loadMigrationForUpdateSQL = loadMigrationSQL + `
FOR UPDATE OF migration`

type rowScanner interface{ Scan(...any) error }

func scanRecord(row rowScanner) (physicalmigration.Record, error) {
	var record physicalmigration.Record
	var parent string
	if err := row.Scan(&record.MigrationID, &parent, &record.TrainRunID, &record.SourceShardID,
		&record.TargetShardID, &record.SourceGeneration, &record.TargetGeneration,
		&record.RetainedTargetGeneration, &record.SourceProtocolVersion,
		&record.SourceSchemaVersion, &record.TargetProtocolVersion,
		&record.TargetSchemaVersion, &record.State,
		&record.BaseCopyCursor, &record.RowsCopied, &record.SourceJournalStart,
		&record.LastReplayedSequence, &record.FinalSourceSequence, &record.RowsReplayed,
		&record.ValidationVersion, &record.TargetWriteCount, &record.ReverseMigration); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return physicalmigration.Record{}, physicalmigration.ErrCheckpointConflict
		}
		return physicalmigration.Record{}, fmt.Errorf("%w: load control record", physicalmigration.ErrCheckpointConflict)
	}
	if parent != "" {
		parsed, err := uuid.Parse(parent)
		if err != nil {
			return physicalmigration.Record{}, physicalmigration.ErrCheckpointConflict
		}
		record.ParentMigrationID = parsed
	}
	return record, nil
}

func saveRecord(ctx context.Context, tx pgx.Tx, record physicalmigration.Record, expected migration.PhysicalState) error {
	tag, err := tx.Exec(ctx, `
UPDATE public.physical_shard_migrations
SET state = $3,
    rows_copied = $4,
    source_journal_start_sequence = $5,
    last_replayed_sequence = $6,
    final_source_sequence = NULLIF($7, 0),
    rows_replayed = $8,
    validation_version = $9,
    target_successful_write_count = GREATEST(target_successful_write_count, $10),
    source_fenced_at = CASE WHEN $3 = 'source_fenced' THEN COALESCE(source_fenced_at, clock_timestamp()) ELSE source_fenced_at END,
    target_enabled_at = CASE WHEN $3 = 'target_enabled' THEN COALESCE(target_enabled_at, clock_timestamp()) ELSE target_enabled_at END
WHERE migration_id = $1
  AND state = $2`, record.MigrationID, expected, record.State, record.RowsCopied,
		record.SourceJournalStart, record.LastReplayedSequence, record.FinalSourceSequence,
		record.RowsReplayed, record.ValidationVersion, record.TargetWriteCount)
	if err != nil || tag.RowsAffected() != 1 {
		return physicalmigration.ErrCheckpointConflict
	}
	return nil
}
