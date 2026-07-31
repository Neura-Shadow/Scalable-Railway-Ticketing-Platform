package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	captureOutboxBatchRows     = 64
	captureOutboxBatchBytes    = 5 * 1024 * 1024
	captureOutboxTotalBytes    = 64 * 1024 * 1024
	captureOutboxGlobalBytes   = 256 * 1024 * 1024
	captureOutboxBudgetLockKey = int64(0x4D354F5554424F58) // "M5OUTBOX"
)

var (
	ErrShardOperation       = errors.New("physical migration shard operation failed")
	ErrApplyReceiptConflict = errors.New("physical migration apply receipt conflict")
)

type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type BaseCopier interface {
	Read(context.Context, DB, physicalmigration.BaseCopyRequest) (physicalmigration.BaseBatch, error)
	Apply(context.Context, DB, physicalmigration.Record, physicalmigration.BaseBatch) error
}

type TargetPreparer interface {
	Prepare(context.Context, DB, physicalmigration.Record) error
}

type MutationApplier interface {
	Apply(context.Context, pgx.Tx, physicalmigration.Record, physicalmigration.JournalEntry) error
}

type Validator interface {
	Validate(context.Context, DB, DB, physicalmigration.ValidationRequest) (physicalmigration.ValidationResult, error)
}

// Shards binds one source and one target database for one engine. Every
// mutation method opens and commits at most one database-local transaction.
type Shards struct {
	source   DB
	target   DB
	copy     BaseCopier
	prepare  TargetPreparer
	apply    MutationApplier
	validate Validator
}

func NewShards(source, target DB, copier BaseCopier, preparer TargetPreparer, applier MutationApplier, validator Validator) (*Shards, error) {
	if source == nil || target == nil || copier == nil || preparer == nil || applier == nil || validator == nil {
		return nil, physicalmigration.ErrInvalidInput
	}
	return &Shards{source: source, target: target, copy: copier, prepare: preparer, apply: applier, validate: validator}, nil
}

// NewDefaultShards installs the concrete fixed-table PostgreSQL data plane.
func NewDefaultShards(source, target DB) (*Shards, error) {
	return NewShards(source, target, JSONBaseCopier{}, DefaultTargetPreparer{}, JSONMutationApplier{}, BoundedValidator{})
}

func (shards *Shards) Preflight(ctx context.Context, record physicalmigration.Record) error {
	if record.SourceProtocolVersion != 1 || record.TargetProtocolVersion != 1 ||
		record.SourceSchemaVersion != 1 || record.TargetSchemaVersion != 1 {
		return physicalmigration.ErrCheckpointConflict
	}
	var sourceReady bool
	if err := shards.source.QueryRow(ctx, `
SELECT current_setting('server_version_num')::integer >= 160000
	AND EXISTS (
	    SELECT 1 FROM public.schema_migrations
	    WHERE version = 1 AND NOT dirty
	)
   AND to_regclass('public.train_run_booking_snapshots') IS NOT NULL
   AND to_regclass('public.train_run_mutation_journal') IS NOT NULL
   AND to_regclass('public.outbox_events') IS NOT NULL
	AND NOT EXISTS (
	    SELECT 1 FROM (VALUES
	        ('train_run_booking_snapshots_capture_mutation'),
	        ('booking_seat_catalog_capture_mutation'),
	        ('booking_fare_snapshots_capture_mutation'),
	        ('seat_inventory_capture_mutation'),
	        ('reservations_capture_mutation'),
	        ('reservation_seats_capture_mutation'),
	        ('ticket_orders_capture_mutation'),
	        ('tickets_capture_mutation'),
	        ('idempotency_records_capture_mutation'),
	        ('booking_command_receipts_capture_mutation')
	    ) AS required(trigger_name)
	    WHERE NOT EXISTS (
	        SELECT 1 FROM pg_catalog.pg_trigger
	        WHERE tgname = required.trigger_name AND NOT tgisinternal
	    )
	)
   AND EXISTS (
       SELECT 1 FROM public.train_run_booking_snapshots
       WHERE train_run_id = $1 AND assignment_generation = $2
   )
   AND EXISTS (
       SELECT 1 FROM public.train_run_write_fences
       WHERE train_run_id = $1 AND assignment_generation = $2
         AND state = 'active' AND write_enabled
   )`, record.TrainRunID, record.SourceGeneration).Scan(&sourceReady); err != nil || !sourceReady {
		return physicalmigration.ErrCheckpointConflict
	}
	var targetReady bool
	if err := shards.target.QueryRow(ctx, `
SELECT current_setting('server_version_num')::integer >= 160000
	AND EXISTS (
	    SELECT 1 FROM public.schema_migrations
	    WHERE version = 1 AND NOT dirty
	)
   AND to_regclass('public.train_run_booking_snapshots') IS NOT NULL
   AND to_regclass('public.migration_apply_receipts') IS NOT NULL
   AND to_regclass('public.outbox_events') IS NOT NULL
	AND NOT EXISTS (
	    SELECT 1 FROM (VALUES
	        ('train_run_booking_snapshots_capture_mutation'),
	        ('booking_seat_catalog_capture_mutation'),
	        ('booking_fare_snapshots_capture_mutation'),
	        ('seat_inventory_capture_mutation'),
	        ('reservations_capture_mutation'),
	        ('reservation_seats_capture_mutation'),
	        ('ticket_orders_capture_mutation'),
	        ('tickets_capture_mutation'),
	        ('idempotency_records_capture_mutation'),
	        ('booking_command_receipts_capture_mutation')
	    ) AS required(trigger_name)
	    WHERE NOT EXISTS (
	        SELECT 1 FROM pg_catalog.pg_trigger
	        WHERE tgname = required.trigger_name AND NOT tgisinternal
	    )
	)
   AND NOT EXISTS (
       SELECT 1 FROM public.train_run_write_fences
       WHERE train_run_id = $1 AND write_enabled
   )`, record.TrainRunID).Scan(&targetReady); err != nil || !targetReady {
		return physicalmigration.ErrCheckpointConflict
	}
	return nil
}

func (shards *Shards) PrepareTarget(ctx context.Context, record physicalmigration.Record) error {
	if err := shards.prepare.Prepare(ctx, shards.target, record); err != nil {
		return err
	}
	var snapshot []byte
	if err := shards.source.QueryRow(ctx, `
SELECT to_jsonb(source_snapshot)
FROM public.train_run_booking_snapshots AS source_snapshot
WHERE train_run_id = $1 AND assignment_generation = $2`, record.TrainRunID,
		record.SourceGeneration).Scan(&snapshot); err != nil {
		return fmt.Errorf("%w: read source authority snapshot", ErrShardOperation)
	}
	normalized, err := normalizeRow(snapshot, "train_run_booking_snapshots", record.TargetGeneration)
	if err != nil {
		return physicalmigration.ErrInvalidBatch
	}
	tx, err := shards.target.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("%w: begin target authority prep", ErrShardOperation)
	}
	rollback := func(result error) error {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	spec, _ := findTable("train_run_booking_snapshots")
	if _, err := tx.Exec(ctx, upsertSQL(spec), normalized); err != nil {
		return rollback(fmt.Errorf("%w: prepare target snapshot", ErrShardOperation))
	}
	fenceTag, err := tx.Exec(ctx, `
INSERT INTO public.train_run_write_fences (
    train_run_id, assignment_generation, state, write_enabled
) VALUES ($1, $2, 'standby', false)
ON CONFLICT (train_run_id) DO UPDATE
SET assignment_generation = EXCLUDED.assignment_generation,
    state = 'standby', write_enabled = false
WHERE NOT train_run_write_fences.write_enabled`, record.TrainRunID, record.TargetGeneration)
	if err != nil || fenceTag.RowsAffected() != 1 {
		return rollback(fmt.Errorf("%w: prepare target fence", ErrShardOperation))
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.train_run_target_write_evidence (
    train_run_id, assignment_generation, successful_write_count
) VALUES ($1, $2, 0)
ON CONFLICT (train_run_id, assignment_generation) DO NOTHING`, record.TrainRunID, record.TargetGeneration); err != nil {
		return rollback(fmt.Errorf("%w: prepare target evidence", ErrShardOperation))
	}
	var evidence int64
	if err := tx.QueryRow(ctx, `
SELECT successful_write_count
FROM public.train_run_target_write_evidence
WHERE train_run_id = $1 AND assignment_generation = $2
FOR UPDATE`, record.TrainRunID, record.TargetGeneration).Scan(&evidence); err != nil || evidence != 0 {
		return rollback(physicalmigration.ErrReverseMigrationRequired)
	}
	if record.ReverseMigration {
		if _, err := tx.Exec(ctx, `
DELETE FROM public.train_run_booking_snapshots
WHERE train_run_id = $1 AND assignment_generation = $2`, record.TrainRunID,
			record.RetainedTargetGeneration); err != nil {
			return rollback(fmt.Errorf("%w: retire retained predecessor snapshot", ErrShardOperation))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit target authority prep", ErrShardOperation)
	}
	return nil
}

func (shards *Shards) EnableCapture(ctx context.Context, record physicalmigration.Record) (int64, error) {
	tx, err := shards.source.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("%w: begin capture", ErrShardOperation)
	}
	rollback := func(result error) (int64, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return 0, result
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.migration_capture_state (
    train_run_id, migration_id, source_generation, capture_enabled
) VALUES ($1, $2, $3, false)
ON CONFLICT (train_run_id) DO NOTHING`, record.TrainRunID, record.MigrationID, record.SourceGeneration); err != nil {
		return rollback(fmt.Errorf("%w: install capture state", ErrShardOperation))
	}
	// A different completed migration may reuse the stable row only through the
	// schema's disabled-and-zero reset state.
	if _, err := tx.Exec(ctx, `
UPDATE public.migration_capture_state
SET migration_id = $2,
    source_generation = $3,
    next_sequence = 0,
    enabled_at = NULL,
    disabled_at = NULL
WHERE train_run_id = $1
  AND NOT capture_enabled
  AND migration_id <> $2`, record.TrainRunID, record.MigrationID, record.SourceGeneration); err != nil {
		return rollback(fmt.Errorf("%w: reset capture state", ErrShardOperation))
	}
	var currentSequence int64
	if err := tx.QueryRow(ctx, `
UPDATE public.migration_capture_state
SET capture_enabled = true,
    enabled_at = COALESCE(enabled_at, clock_timestamp()),
    disabled_at = NULL
WHERE train_run_id = $1
  AND migration_id = $2
  AND source_generation = $3
RETURNING next_sequence`, record.TrainRunID, record.MigrationID, record.SourceGeneration).Scan(&currentSequence); err != nil {
		return rollback(fmt.Errorf("%w: enable capture", ErrShardOperation))
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("%w: commit capture", ErrShardOperation)
	}
	// Sequences are per migration and reset to zero. Returning the stable origin
	// instead of currentSequence prevents a crash after activation from skipping
	// mutations that arrived before the control checkpoint was persisted.
	_ = currentSequence
	return 0, nil
}

func (shards *Shards) ReadBaseBatch(ctx context.Context, request physicalmigration.BaseCopyRequest) (physicalmigration.BaseBatch, error) {
	return shards.copy.Read(ctx, shards.source, request)
}

func (shards *Shards) ApplyBaseBatch(ctx context.Context, record physicalmigration.Record, batch physicalmigration.BaseBatch) error {
	return shards.copy.Apply(ctx, shards.target, record, batch)
}

func (shards *Shards) ReadJournal(ctx context.Context, request physicalmigration.JournalRequest) (physicalmigration.JournalBatch, error) {
	var sourceSequence int64
	if err := shards.source.QueryRow(ctx, `
SELECT next_sequence
FROM public.migration_capture_state
WHERE train_run_id = $1
  AND migration_id = $2
  AND source_generation = $3`, request.Migration.TrainRunID, request.Migration.MigrationID, request.Migration.SourceGeneration).Scan(&sourceSequence); err != nil {
		return physicalmigration.JournalBatch{}, fmt.Errorf("%w: read capture sequence", ErrShardOperation)
	}
	through := sourceSequence
	if request.ThroughSequence > 0 && request.ThroughSequence < through {
		through = request.ThroughSequence
	}
	rows, err := shards.source.Query(ctx, `
SELECT id, mutation_sequence, table_name, operation, entity_id,
       primary_key, metadata
FROM public.train_run_mutation_journal
WHERE migration_id = $1
  AND train_run_id = $2
  AND source_generation = $3
  AND mutation_sequence > $4
  AND mutation_sequence <= $5
ORDER BY mutation_sequence, id
LIMIT $6`, request.Migration.MigrationID, request.Migration.TrainRunID, request.Migration.SourceGeneration,
		request.AfterSequence, through, request.Limit)
	if err != nil {
		return physicalmigration.JournalBatch{}, fmt.Errorf("%w: read journal", ErrShardOperation)
	}
	defer rows.Close()
	batch := physicalmigration.JournalBatch{SourceSequence: through}
	for rows.Next() {
		var entry physicalmigration.JournalEntry
		if err := rows.Scan(&entry.ID, &entry.Sequence, &entry.TableName, &entry.Operation,
			&entry.EntityID, &entry.PrimaryKey, &entry.Metadata); err != nil {
			return physicalmigration.JournalBatch{}, fmt.Errorf("%w: scan journal", ErrShardOperation)
		}
		entry.ApplyFingerprint = journalFingerprint(entry)
		batch.Entries = append(batch.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return physicalmigration.JournalBatch{}, fmt.Errorf("%w: iterate journal", ErrShardOperation)
	}
	rows.Close()
	for index := range batch.Entries {
		if batch.Entries[index].Operation == "DELETE" {
			continue
		}
		payload, err := loadMutationPayload(ctx, shards.source, batch.Entries[index],
			request.Migration.TrainRunID, request.Migration.SourceGeneration)
		if err != nil {
			return physicalmigration.JournalBatch{}, err
		}
		batch.Entries[index].Payload = payload
	}
	return batch, nil
}

func (shards *Shards) ApplyJournal(ctx context.Context, record physicalmigration.Record, entry physicalmigration.JournalEntry) (bool, error) {
	tx, err := shards.target.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, fmt.Errorf("%w: begin target apply", ErrShardOperation)
	}
	rollback := func(result error) (bool, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return false, result
	}
	fingerprint := entry.ApplyFingerprint
	if fingerprint == ([32]byte{}) {
		fingerprint = journalFingerprint(entry)
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO public.migration_apply_receipts (
    migration_id, source_journal_id, train_run_id, target_generation,
    mutation_sequence, apply_fingerprint
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (migration_id, source_journal_id) DO NOTHING`, record.MigrationID, entry.ID,
		record.TrainRunID, record.TargetGeneration, entry.Sequence, fingerprint[:])
	if err != nil {
		return rollback(fmt.Errorf("%w: reserve apply receipt", ErrShardOperation))
	}
	if tag.RowsAffected() == 0 {
		var (
			storedTrainRunID  uuid.UUID
			storedGeneration  int64
			storedSequence    int64
			storedFingerprint []byte
		)
		if err := tx.QueryRow(ctx, `
SELECT train_run_id, target_generation, mutation_sequence, apply_fingerprint
FROM public.migration_apply_receipts
WHERE migration_id = $1
  AND source_journal_id = $2
FOR UPDATE`, record.MigrationID, entry.ID).Scan(&storedTrainRunID, &storedGeneration, &storedSequence, &storedFingerprint); err != nil {
			return rollback(ErrApplyReceiptConflict)
		}
		if storedTrainRunID != record.TrainRunID || storedGeneration != record.TargetGeneration ||
			storedSequence != entry.Sequence || !bytes.Equal(storedFingerprint, fingerprint[:]) {
			return rollback(ErrApplyReceiptConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("%w: commit replay receipt", ErrShardOperation)
		}
		return true, nil
	}
	if err := shards.apply.Apply(ctx, tx, record, entry); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("%w: commit journal apply", ErrShardOperation)
	}
	return false, nil
}

func (shards *Shards) CaptureOutbox(ctx context.Context, record physicalmigration.Record, maxRows int) error {
	if maxRows <= 0 {
		return physicalmigration.ErrInvalidInput
	}
	resetTx, err := shards.target.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("%w: begin target outbox staging reset", ErrShardOperation)
	}
	if err := lockOutboxStagingBudget(ctx, resetTx); err != nil {
		_ = resetTx.Rollback(context.WithoutCancel(ctx))
		return err
	}
	if _, err := resetTx.Exec(ctx, `DELETE FROM public.migration_outbox_staging WHERE migration_id=$1`,
		record.MigrationID); err != nil {
		_ = resetTx.Rollback(context.WithoutCancel(ctx))
		return fmt.Errorf("%w: reset target outbox staging", ErrShardOperation)
	}
	if err := enforceOutboxStagingBudget(ctx, resetTx); err != nil {
		_ = resetTx.Rollback(context.WithoutCancel(ctx))
		return err
	}
	if err := resetTx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit target outbox staging reset", ErrShardOperation)
	}
	sourceTx, err := shards.source.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("%w: begin source outbox snapshot", ErrShardOperation)
	}
	rollbackSource := func(result error) error {
		_ = sourceTx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	var cursor pgtype.UUID
	total := 0
	var totalBytes int64
	for {
		rows, queryErr := sourceTx.Query(ctx, `
SELECT id, to_jsonb(source_outbox)
FROM public.outbox_events AS source_outbox
WHERE train_run_id = $1 AND assignment_generation = $2
  AND ($3::uuid IS NULL OR id > $3)
ORDER BY id
LIMIT $4`, record.TrainRunID, record.SourceGeneration, cursor, captureOutboxBatchRows)
		if queryErr != nil {
			return rollbackSource(fmt.Errorf("%w: capture source outbox", ErrShardOperation))
		}
		payload := make([]JSONRow, 0, captureOutboxBatchRows)
		batchBytes := 0
		for rows.Next() {
			var row JSONRow
			row.Table = "outbox_events"
			if err := rows.Scan(&row.ID, &row.Data); err != nil {
				rows.Close()
				return rollbackSource(fmt.Errorf("%w: scan source outbox", ErrShardOperation))
			}
			normalized, normalizeErr := normalizeRow(row.Data, row.Table, record.TargetGeneration)
			if normalizeErr != nil {
				rows.Close()
				return rollbackSource(physicalmigration.ErrInvalidBatch)
			}
			row.Data = normalized
			batchBytes += len(normalized)
			totalBytes, normalizeErr = boundedCaptureBytes(totalBytes, len(normalized))
			if normalizeErr != nil {
				rows.Close()
				return rollbackSource(normalizeErr)
			}
			if batchBytes > captureOutboxBatchBytes {
				rows.Close()
				return rollbackSource(physicalmigration.ErrCleanupLimitExceeded)
			}
			payload = append(payload, row)
		}
		iterationErr := rows.Err()
		rows.Close()
		if iterationErr != nil {
			return rollbackSource(fmt.Errorf("%w: iterate source outbox", ErrShardOperation))
		}
		if len(payload) == 0 {
			break
		}
		total += len(payload)
		if total > maxRows {
			return rollbackSource(physicalmigration.ErrCleanupLimitExceeded)
		}
		stageTx, beginErr := shards.target.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if beginErr != nil {
			return rollbackSource(fmt.Errorf("%w: begin target outbox staging batch", ErrShardOperation))
		}
		if lockErr := lockOutboxStagingBudget(ctx, stageTx); lockErr != nil {
			_ = stageTx.Rollback(context.WithoutCancel(ctx))
			return rollbackSource(lockErr)
		}
		for _, row := range payload {
			if _, stageErr := stageTx.Exec(ctx, `
INSERT INTO public.migration_outbox_staging(migration_id,source_event_id,row_data)
VALUES($1,$2,$3)
ON CONFLICT(migration_id,source_event_id) DO UPDATE SET row_data=EXCLUDED.row_data`,
				record.MigrationID, row.ID, row.Data); stageErr != nil {
				_ = stageTx.Rollback(context.WithoutCancel(ctx))
				return rollbackSource(fmt.Errorf("%w: stage target outbox batch", ErrShardOperation))
			}
		}
		if budgetErr := enforceOutboxStagingBudget(ctx, stageTx); budgetErr != nil {
			_ = stageTx.Rollback(context.WithoutCancel(ctx))
			return rollbackSource(budgetErr)
		}
		if commitErr := stageTx.Commit(ctx); commitErr != nil {
			return rollbackSource(fmt.Errorf("%w: commit target outbox staging batch", ErrShardOperation))
		}
		cursor = pgtype.UUID{Bytes: payload[len(payload)-1].ID, Valid: true}
		if len(payload) < captureOutboxBatchRows {
			break
		}
	}
	if err := sourceTx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit source outbox snapshot", ErrShardOperation)
	}
	tx, err := shards.target.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("%w: begin target outbox capture", ErrShardOperation)
	}
	rollback := func(result error) error {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	if _, err := tx.Exec(ctx, `
DELETE FROM public.outbox_events
WHERE train_run_id = $1 AND assignment_generation = $2`, record.TrainRunID, record.TargetGeneration); err != nil {
		return rollback(fmt.Errorf("%w: replace target outbox", ErrShardOperation))
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO public.outbox_events
SELECT (jsonb_populate_record(NULL::public.outbox_events, staged.row_data)).*
FROM public.migration_outbox_staging AS staged
WHERE staged.migration_id=$1
ORDER BY staged.source_event_id`, record.MigrationID)
	if err != nil || tag.RowsAffected() != int64(total) {
		return rollback(fmt.Errorf("%w: promote target outbox capture", ErrShardOperation))
	}
	if _, err := tx.Exec(ctx, `DELETE FROM public.migration_outbox_staging WHERE migration_id=$1`,
		record.MigrationID); err != nil {
		return rollback(fmt.Errorf("%w: clear promoted outbox staging", ErrShardOperation))
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit target outbox capture", ErrShardOperation)
	}
	return nil
}

func boundedCaptureBytes(total int64, next int) (int64, error) {
	if total < 0 || next < 0 || total > captureOutboxTotalBytes-int64(next) {
		return total, physicalmigration.ErrCleanupLimitExceeded
	}
	return total + int64(next), nil
}

func lockOutboxStagingBudget(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, captureOutboxBudgetLockKey); err != nil {
		return fmt.Errorf("%w: lock target outbox staging budget", ErrShardOperation)
	}
	return nil
}

func enforceOutboxStagingBudget(ctx context.Context, tx pgx.Tx) error {
	tag, err := tx.Exec(ctx, `
SELECT 1
WHERE (
    SELECT COALESCE(sum(octet_length(row_data::text)), 0)
    FROM public.migration_outbox_staging
) <= $1`, int64(captureOutboxGlobalBytes))
	if err != nil {
		return fmt.Errorf("%w: inspect target outbox staging budget", ErrShardOperation)
	}
	if tag.RowsAffected() != 1 {
		return physicalmigration.ErrCleanupLimitExceeded
	}
	return nil
}

func (shards *Shards) Validate(ctx context.Context, request physicalmigration.ValidationRequest) (physicalmigration.ValidationResult, error) {
	return shards.validate.Validate(ctx, shards.source, shards.target, request)
}

func (shards *Shards) FenceSource(ctx context.Context, record physicalmigration.Record) (int64, error) {
	tx, err := shards.source.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("%w: begin source fence", ErrShardOperation)
	}
	rollback := func(result error) (int64, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return 0, result
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.train_run_write_fences
SET write_enabled = false, state = 'quiescing'
WHERE train_run_id = $1
  AND assignment_generation = $2
  AND state IN ('active', 'quiescing')`, record.TrainRunID, record.SourceGeneration)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(fmt.Errorf("%w: fence source", ErrShardOperation))
	}
	var sequence int64
	if err := tx.QueryRow(ctx, `
SELECT next_sequence
FROM public.migration_capture_state
WHERE train_run_id = $1
  AND migration_id = $2
  AND source_generation = $3
  AND capture_enabled
FOR UPDATE`, record.TrainRunID, record.MigrationID, record.SourceGeneration).Scan(&sequence); err != nil {
		return rollback(fmt.Errorf("%w: lock final source sequence", ErrShardOperation))
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("%w: commit source fence", ErrShardOperation)
	}
	return sequence, nil
}

func (shards *Shards) EnableTarget(ctx context.Context, record physicalmigration.Record) error {
	tx, err := shards.target.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("%w: begin target enable", ErrShardOperation)
	}
	rollback := func(result error) error {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	var count int64
	var baselineInitialized bool
	if err := tx.QueryRow(ctx, `
SELECT successful_write_count, baseline_initialized
FROM public.train_run_target_write_evidence
WHERE train_run_id = $1 AND assignment_generation = $2
FOR UPDATE`, record.TrainRunID, record.TargetGeneration).Scan(&count, &baselineInitialized); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return rollback(physicalmigration.ErrTargetEvidenceMissing)
		}
		return rollback(fmt.Errorf("%w: lock target evidence", ErrShardOperation))
	}
	if count != 0 {
		return rollback(physicalmigration.ErrReverseMigrationRequired)
	}
	if !baselineInitialized {
		tag, err := tx.Exec(ctx, `
UPDATE public.train_run_target_write_evidence AS evidence
SET baseline_initialized = true,
    baseline_reservation_count = (
        SELECT count(*) FROM public.reservations
        WHERE train_run_id = $1 AND assignment_generation = $2
    ),
    baseline_command_receipt_count = (
        SELECT count(*) FROM public.booking_command_receipts
        WHERE train_run_id = $1 AND assignment_generation = $2
    ),
    baseline_outbox_count = (
        SELECT count(*) FROM public.outbox_events
        WHERE train_run_id = $1 AND assignment_generation = $2
    )
WHERE evidence.train_run_id = $1
  AND evidence.assignment_generation = $2
  AND evidence.successful_write_count = 0
  AND NOT evidence.baseline_initialized`, record.TrainRunID, record.TargetGeneration)
		if err != nil || tag.RowsAffected() != 1 {
			return rollback(physicalmigration.ErrTargetEvidenceMissing)
		}
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.train_run_write_fences
SET write_enabled = true, state = 'active'
WHERE train_run_id = $1
  AND assignment_generation = $2
  AND state IN ('standby', 'active')`, record.TrainRunID, record.TargetGeneration)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(fmt.Errorf("%w: enable target", ErrShardOperation))
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit target enable", ErrShardOperation)
	}
	return nil
}

func (shards *Shards) TargetWriteCount(ctx context.Context, record physicalmigration.Record) (int64, error) {
	var count int64
	if err := shards.target.QueryRow(ctx, `
SELECT successful_write_count
FROM public.train_run_target_write_evidence
WHERE train_run_id = $1
  AND assignment_generation = $2`, record.TrainRunID, record.TargetGeneration).Scan(&count); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, physicalmigration.ErrTargetEvidenceMissing
		}
		return 0, fmt.Errorf("%w: read target-write evidence", ErrShardOperation)
	}
	return count, nil
}

func (shards *Shards) TargetCommandOutboxEvidence(ctx context.Context, record physicalmigration.Record) (int64, error) {
	var count int64
	if err := shards.target.QueryRow(ctx, `
SELECT
	CASE WHEN evidence.baseline_initialized THEN
	    GREATEST(0, (SELECT count(*) FROM public.reservations
	                 WHERE train_run_id = $1 AND assignment_generation = $2)
	                 - evidence.baseline_reservation_count)
	  + GREATEST(0, (SELECT count(*) FROM public.booking_command_receipts
	                 WHERE train_run_id = $1 AND assignment_generation = $2)
	                 - evidence.baseline_command_receipt_count)
	  + GREATEST(0, (SELECT count(*) FROM public.outbox_events
	                 WHERE train_run_id = $1 AND assignment_generation = $2)
	                 - evidence.baseline_outbox_count)
	ELSE -1 END
FROM public.train_run_target_write_evidence AS evidence
WHERE evidence.train_run_id = $1 AND evidence.assignment_generation = $2`, record.TrainRunID,
		record.TargetGeneration).Scan(&count); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, physicalmigration.ErrTargetEvidenceMissing
		}
		return 0, fmt.Errorf("%w: inspect target command/outbox evidence", ErrShardOperation)
	}
	if count < 0 {
		return 0, physicalmigration.ErrTargetEvidenceMissing
	}
	return count, nil
}

// RollbackBeforeTargetWrites is deliberately ordered. The target is disabled
// and committed before the source can be re-enabled; a crash can cause an
// outage, never two writers.
func (shards *Shards) RollbackBeforeTargetWrites(ctx context.Context, record physicalmigration.Record, rollbackGeneration int64) error {
	if rollbackGeneration <= record.TargetGeneration {
		return physicalmigration.ErrGenerationNotNewer
	}
	if err := shards.disableZeroWriteTarget(ctx, record); err != nil {
		return err
	}
	return shards.rebindRollbackSource(ctx, record, rollbackGeneration)
}

func (shards *Shards) disableZeroWriteTarget(ctx context.Context, record physicalmigration.Record) error {
	tx, err := shards.target.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("%w: begin rollback target", ErrShardOperation)
	}
	rollback := func(result error) error {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	var fenceState string
	var fenceWriteEnabled bool
	if err := tx.QueryRow(ctx, `
SELECT state, write_enabled
FROM public.train_run_write_fences
WHERE train_run_id = $1 AND assignment_generation = $2
FOR UPDATE`, record.TrainRunID, record.TargetGeneration).Scan(&fenceState, &fenceWriteEnabled); err != nil {
		return rollback(physicalmigration.ErrTargetEvidenceMissing)
	}
	var writes, reservations, receipts, outbox int64
	var initialized bool
	if err := tx.QueryRow(ctx, `
SELECT successful_write_count, baseline_initialized,
       baseline_reservation_count, baseline_command_receipt_count,
       baseline_outbox_count
FROM public.train_run_target_write_evidence
WHERE train_run_id = $1 AND assignment_generation = $2
FOR UPDATE`, record.TrainRunID, record.TargetGeneration).Scan(
		&writes, &initialized, &reservations, &receipts, &outbox); err != nil {
		return rollback(physicalmigration.ErrTargetEvidenceMissing)
	}
	if writes != 0 {
		return rollback(physicalmigration.ErrReverseMigrationRequired)
	}
	var currentReservations, currentReceipts, currentOutbox int64
	if err := tx.QueryRow(ctx, `
SELECT
    (SELECT count(*) FROM public.reservations
     WHERE train_run_id = $1 AND assignment_generation = $2),
    (SELECT count(*) FROM public.booking_command_receipts
     WHERE train_run_id = $1 AND assignment_generation = $2),
    (SELECT count(*) FROM public.outbox_events
     WHERE train_run_id = $1 AND assignment_generation = $2)`, record.TrainRunID,
		record.TargetGeneration).Scan(&currentReservations, &currentReceipts, &currentOutbox); err != nil {
		return rollback(physicalmigration.ErrTargetEvidenceMissing)
	}
	if !initialized {
		if fenceWriteEnabled || (fenceState != "standby" && fenceState != "disabled") {
			return rollback(physicalmigration.ErrTargetEvidenceMissing)
		}
		tag, err := tx.Exec(ctx, `
UPDATE public.train_run_target_write_evidence
SET baseline_initialized = true,
    baseline_reservation_count = $3,
    baseline_command_receipt_count = $4,
    baseline_outbox_count = $5
WHERE train_run_id = $1 AND assignment_generation = $2
  AND successful_write_count = 0 AND NOT baseline_initialized`, record.TrainRunID,
			record.TargetGeneration, currentReservations, currentReceipts, currentOutbox)
		if err != nil || tag.RowsAffected() != 1 {
			return rollback(physicalmigration.ErrTargetEvidenceMissing)
		}
		reservations, receipts, outbox = currentReservations, currentReceipts, currentOutbox
	}
	if currentReservations != reservations || currentReceipts != receipts || currentOutbox != outbox {
		return rollback(physicalmigration.ErrReverseMigrationRequired)
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.train_run_write_fences
SET write_enabled = false, state = 'disabled'
WHERE train_run_id = $1
  AND assignment_generation = $2
  AND state IN ('standby', 'active', 'disabled')`, record.TrainRunID, record.TargetGeneration)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(fmt.Errorf("%w: disable rollback target", ErrShardOperation))
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit rollback target", ErrShardOperation)
	}
	return nil
}

func (shards *Shards) rebindRollbackSource(ctx context.Context, record physicalmigration.Record, rollbackGeneration int64) error {
	tx, err := shards.source.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("%w: begin rollback source", ErrShardOperation)
	}
	rollback := func(result error) error {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.migration_capture_state
SET capture_enabled = false,
    disabled_at = COALESCE(disabled_at, clock_timestamp())
WHERE train_run_id = $1
  AND migration_id = $2
  AND source_generation IN ($3, $4)`, record.TrainRunID, record.MigrationID,
		record.SourceGeneration, rollbackGeneration); err != nil {
		return rollback(fmt.Errorf("%w: disable rollback capture", ErrShardOperation))
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.train_run_booking_snapshots
SET assignment_generation = $2
WHERE train_run_id = $1
  AND assignment_generation = $3`, record.TrainRunID, rollbackGeneration, record.SourceGeneration)
	if err != nil {
		return rollback(fmt.Errorf("%w: rebind rollback generation", ErrShardOperation))
	}
	if tag.RowsAffected() == 0 {
		var prepared int64
		if err := tx.QueryRow(ctx, `
SELECT count(*) FROM public.train_run_booking_snapshots
WHERE train_run_id = $1 AND assignment_generation = $2`, record.TrainRunID,
			rollbackGeneration).Scan(&prepared); err != nil || prepared != 1 {
			return rollback(physicalmigration.ErrCheckpointConflict)
		}
	} else if tag.RowsAffected() != 1 {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	tag, err = tx.Exec(ctx, `
UPDATE public.train_run_write_fences
SET write_enabled = true, state = 'active'
WHERE train_run_id = $1
  AND assignment_generation = $2
  AND state IN ('quiescing', 'retained', 'active')`, record.TrainRunID, rollbackGeneration)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(fmt.Errorf("%w: enable rollback source", ErrShardOperation))
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit rollback source", ErrShardOperation)
	}
	return nil
}

func (shards *Shards) DisableCapture(ctx context.Context, record physicalmigration.Record) error {
	tag, err := beginExecCommit(ctx, shards.source, `
UPDATE public.migration_capture_state
SET capture_enabled = false,
    disabled_at = COALESCE(disabled_at, clock_timestamp())
WHERE train_run_id = $1
  AND migration_id = $2
  AND source_generation = $3
  AND (capture_enabled OR disabled_at IS NOT NULL)`, record.TrainRunID, record.MigrationID, record.SourceGeneration)
	if err != nil || tag != 1 {
		return fmt.Errorf("%w: disable source capture", ErrShardOperation)
	}
	return nil
}

func (shards *Shards) RetainSource(ctx context.Context, record physicalmigration.Record) error {
	tag, err := beginExecCommit(ctx, shards.source, `
UPDATE public.train_run_write_fences
SET write_enabled = false, state = 'retained'
WHERE train_run_id = $1
  AND assignment_generation = $2
  AND state IN ('quiescing', 'retained')`, record.TrainRunID, record.SourceGeneration)
	if err != nil || tag != 1 {
		return fmt.Errorf("%w: retain source", ErrShardOperation)
	}
	return nil
}

func beginExecCommit(ctx context.Context, db DB, sql string, args ...any) (int64, error) {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, err
	}
	tag, err := tx.Exec(ctx, sql, args...)
	if err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func journalFingerprint(entry physicalmigration.JournalEntry) [32]byte {
	hash := sha256.New()
	hash.Write(entry.ID[:])
	var sequence [8]byte
	binary.BigEndian.PutUint64(sequence[:], uint64(entry.Sequence))
	hash.Write(sequence[:])
	hash.Write([]byte(entry.TableName))
	hash.Write([]byte{0})
	hash.Write([]byte(entry.Operation))
	hash.Write(entry.EntityID[:])
	hash.Write(entry.PrimaryKey)
	hash.Write(entry.Metadata)
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}
