// Package controlsource adapts the fixed legacy/logical-schema booking stores
// in the control PostgreSQL database to the physical migration engine.
package controlsource

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	physicalpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	SourceLegacy = "legacy"
	SourceZero   = "shard-0"
	SourceOne    = "shard-1"
)

var tableOrder = []string{
	"train_run_booking_snapshots",
	"booking_seat_catalog",
	"booking_fare_snapshots",
	"seat_inventory",
	"reservations",
	"reservation_seats",
	"ticket_orders",
	"tickets",
	"idempotency_records",
	"booking_command_receipts",
	"outbox_events",
}

// Adapter performs each source or target mutation in one database-local
// transaction. It never owns two live transactions and never dual-writes.
type Adapter struct {
	sourceID  string
	control   physicalpostgres.DB
	target    physicalpostgres.DB
	targetOps *physicalpostgres.Shards
}

func New(control, target physicalpostgres.DB, sourceID string) (*Adapter, error) {
	if control == nil || target == nil || !validSource(sourceID) {
		return nil, physicalmigration.ErrInvalidInput
	}
	targetOps, err := physicalpostgres.NewDefaultShards(control, target)
	if err != nil {
		return nil, err
	}
	return &Adapter{sourceID: sourceID, control: control, target: target, targetOps: targetOps}, nil
}

func validSource(value string) bool {
	return value == SourceLegacy || value == SourceZero || value == SourceOne
}

func (adapter *Adapter) Preflight(ctx context.Context, record physicalmigration.Record) error {
	if adapter == nil || record.SourceShardID != adapter.sourceID || !validSource(record.SourceShardID) ||
		record.SourceProtocolVersion != 1 || record.SourceSchemaVersion != 8 ||
		record.TargetProtocolVersion != 1 || record.TargetSchemaVersion != 1 {
		return physicalmigration.ErrCheckpointConflict
	}
	var sourceReady bool
	if err := adapter.control.QueryRow(ctx, `
SELECT current_setting('server_version_num')::integer >= 160000
   AND to_regclass('public.physical_source_migration_capture_state') IS NOT NULL
   AND to_regclass('public.physical_source_train_run_mutation_journal') IS NOT NULL
   AND assignment.shard_id = $2
   AND assignment.assignment_generation = $3
   AND assignment.assignment_state = 'migrating'
   AND assignment.active_physical_migration_id = $4
   AND shard.storage_kind IN ('legacy_schema', 'logical_schema')
   AND shard.enabled AND shard.write_enabled
FROM public.train_run_shard_assignments AS assignment
JOIN public.booking_shards AS shard ON shard.shard_id = assignment.shard_id
WHERE assignment.train_run_id = $1`, record.TrainRunID, adapter.sourceID,
		record.SourceGeneration, record.MigrationID).Scan(&sourceReady); err != nil || !sourceReady {
		return physicalmigration.ErrCheckpointConflict
	}
	var fenceReady bool
	if err := adapter.control.QueryRow(ctx, sourceFenceReadSQL(adapter.sourceID),
		record.TrainRunID, record.SourceGeneration).Scan(&fenceReady); err != nil || !fenceReady {
		return physicalmigration.ErrCheckpointConflict
	}
	var targetReady bool
	if err := adapter.target.QueryRow(ctx, `
SELECT current_setting('server_version_num')::integer >= 160000
   AND to_regclass('public.train_run_booking_snapshots') IS NOT NULL
   AND to_regclass('public.migration_apply_receipts') IS NOT NULL
   AND to_regclass('public.outbox_events') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM public.train_run_write_fences
       WHERE train_run_id = $1 AND write_enabled
   )`, record.TrainRunID).Scan(&targetReady); err != nil || !targetReady {
		return physicalmigration.ErrCheckpointConflict
	}
	return nil
}

func sourceFenceReadSQL(sourceID string) string {
	switch sourceID {
	case SourceLegacy:
		return `SELECT EXISTS (SELECT 1 FROM public.train_run_write_fences WHERE train_run_id=$1 AND assignment_generation=$2 AND write_enabled)`
	case SourceZero:
		return `SELECT EXISTS (SELECT 1 FROM booking_shard_0.train_run_write_fences WHERE train_run_id=$1 AND assignment_generation=$2 AND write_enabled)`
	case SourceOne:
		return `SELECT EXISTS (SELECT 1 FROM booking_shard_1.train_run_write_fences WHERE train_run_id=$1 AND assignment_generation=$2 AND write_enabled)`
	default:
		return `SELECT false`
	}
}

func (adapter *Adapter) PrepareTarget(ctx context.Context, record physicalmigration.Record) error {
	if err := (physicalpostgres.DefaultTargetPreparer{}).Prepare(ctx, adapter.target, record); err != nil {
		return err
	}
	batch, err := adapter.readExact(ctx, record, "train_run_booking_snapshots", uuid.Nil)
	if err != nil {
		return err
	}
	if batch.Rows != 1 {
		return physicalmigration.ErrCheckpointConflict
	}
	return adapter.targetOps.ApplyBaseBatch(ctx, record, batch)
}

func (adapter *Adapter) EnableCapture(ctx context.Context, record physicalmigration.Record) (int64, error) {
	tx, err := adapter.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return 0, fmt.Errorf("%w: begin control-source capture", physicalpostgres.ErrShardOperation)
	}
	rollback := func(result error) (int64, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return 0, result
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.physical_source_migration_capture_state (
    train_run_id, migration_id, source_shard_id, source_generation,
    capture_enabled
) VALUES ($1,$2,$3,$4,false)
ON CONFLICT (train_run_id) DO NOTHING`, record.TrainRunID, record.MigrationID,
		adapter.sourceID, record.SourceGeneration); err != nil {
		return rollback(fmt.Errorf("%w: install control-source capture", physicalpostgres.ErrShardOperation))
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.physical_source_migration_capture_state
SET migration_id=$2, source_shard_id=$3, source_generation=$4,
    next_sequence=0, enabled_at=NULL, disabled_at=NULL
WHERE train_run_id=$1 AND NOT capture_enabled AND migration_id<>$2`, record.TrainRunID,
		record.MigrationID, adapter.sourceID, record.SourceGeneration); err != nil {
		return rollback(fmt.Errorf("%w: reset control-source capture", physicalpostgres.ErrShardOperation))
	}
	var sequence int64
	if err := tx.QueryRow(ctx, `
UPDATE public.physical_source_migration_capture_state
SET capture_enabled=true, enabled_at=COALESCE(enabled_at,clock_timestamp()),
    disabled_at=NULL
WHERE train_run_id=$1 AND migration_id=$2 AND source_shard_id=$3
  AND source_generation=$4
RETURNING next_sequence`, record.TrainRunID, record.MigrationID, adapter.sourceID,
		record.SourceGeneration).Scan(&sequence); err != nil {
		return rollback(fmt.Errorf("%w: enable control-source capture", physicalpostgres.ErrShardOperation))
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("%w: commit control-source capture", physicalpostgres.ErrShardOperation)
	}
	// Always replay from the durable per-migration origin after an activation
	// crash; returning the observed high watermark could skip committed rows.
	_ = sequence
	return 0, nil
}

func (adapter *Adapter) ReadBaseBatch(ctx context.Context, request physicalmigration.BaseCopyRequest) (physicalmigration.BaseBatch, error) {
	index, after, err := parseCursor(request.Cursor)
	if err != nil || request.Limit <= 0 || request.Migration.SourceShardID != adapter.sourceID {
		return physicalmigration.BaseBatch{}, physicalmigration.ErrInvalidInput
	}
	for index < len(tableOrder) {
		table := tableOrder[index]
		if table == "booking_command_receipts" {
			index++
			after = uuid.Nil
			continue
		}
		batch, err := adapter.readRows(ctx, request.Migration, table, after, uuid.Nil, request.Limit)
		if err != nil {
			return physicalmigration.BaseBatch{}, err
		}
		if batch.Rows > 0 {
			payload := batch.Payload.(physicalpostgres.BasePayload)
			last := payload.Rows[len(payload.Rows)-1].ID
			batch.Cursor = request.Cursor
			batch.NextCursor = encodeCursor(index, last)
			return batch, nil
		}
		index++
		after = uuid.Nil
	}
	empty := physicalpostgres.BasePayload{}
	return physicalmigration.BaseBatch{ObjectName: "complete", Cursor: request.Cursor,
		NextCursor: "complete", Done: true, Fingerprint: payloadFingerprint(empty), Payload: empty}, nil
}

func (adapter *Adapter) ApplyBaseBatch(ctx context.Context, record physicalmigration.Record, batch physicalmigration.BaseBatch) error {
	return adapter.targetOps.ApplyBaseBatch(ctx, record, batch)
}

func (adapter *Adapter) ReadJournal(ctx context.Context, request physicalmigration.JournalRequest) (physicalmigration.JournalBatch, error) {
	if request.Limit <= 0 || request.AfterSequence < 0 || request.Migration.SourceShardID != adapter.sourceID {
		return physicalmigration.JournalBatch{}, physicalmigration.ErrInvalidInput
	}
	var highWatermark int64
	if err := adapter.control.QueryRow(ctx, `
SELECT next_sequence
FROM public.physical_source_migration_capture_state
WHERE train_run_id=$1 AND migration_id=$2 AND source_shard_id=$3
  AND source_generation=$4`, request.Migration.TrainRunID, request.Migration.MigrationID,
		adapter.sourceID, request.Migration.SourceGeneration).Scan(&highWatermark); err != nil {
		return physicalmigration.JournalBatch{}, fmt.Errorf("%w: read control-source sequence", physicalpostgres.ErrShardOperation)
	}
	through := highWatermark
	if request.ThroughSequence > 0 && request.ThroughSequence < through {
		through = request.ThroughSequence
	}
	rows, err := adapter.control.Query(ctx, `
SELECT journal_id, mutation_sequence, table_name, operation, entity_id,
       primary_key, metadata
FROM public.physical_source_train_run_mutation_journal
WHERE migration_id=$1 AND train_run_id=$2 AND source_shard_id=$3
  AND source_generation=$4 AND mutation_sequence>$5
  AND mutation_sequence<=$6
ORDER BY mutation_sequence,journal_id
LIMIT $7`, request.Migration.MigrationID, request.Migration.TrainRunID,
		adapter.sourceID, request.Migration.SourceGeneration, request.AfterSequence,
		through, request.Limit)
	if err != nil {
		return physicalmigration.JournalBatch{}, fmt.Errorf("%w: read control-source journal", physicalpostgres.ErrShardOperation)
	}
	defer rows.Close()
	batch := physicalmigration.JournalBatch{SourceSequence: through}
	for rows.Next() {
		var entry physicalmigration.JournalEntry
		if err := rows.Scan(&entry.ID, &entry.Sequence, &entry.TableName, &entry.Operation,
			&entry.EntityID, &entry.PrimaryKey, &entry.Metadata); err != nil {
			return physicalmigration.JournalBatch{}, fmt.Errorf("%w: scan control-source journal", physicalpostgres.ErrShardOperation)
		}
		if !knownTable(entry.TableName) || entry.TableName == "booking_command_receipts" {
			return physicalmigration.JournalBatch{}, physicalmigration.ErrInvalidBatch
		}
		entry.ApplyFingerprint = entryFingerprint(entry)
		batch.Entries = append(batch.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return physicalmigration.JournalBatch{}, fmt.Errorf("%w: iterate control-source journal", physicalpostgres.ErrShardOperation)
	}
	rows.Close()
	for index := range batch.Entries {
		entry := &batch.Entries[index]
		if entry.Operation == "DELETE" {
			continue
		}
		rowBatch, err := adapter.readExact(ctx, request.Migration, entry.TableName, entry.EntityID)
		if err != nil {
			return physicalmigration.JournalBatch{}, err
		}
		if rowBatch.Rows == 0 {
			// A later delete can make an earlier insert/update disappear before
			// replay. Applying the final tombstone here is convergent and safe.
			entry.Operation = "DELETE"
			entry.ApplyFingerprint = entryFingerprint(*entry)
			continue
		}
		payload := rowBatch.Payload.(physicalpostgres.BasePayload)
		entry.Payload = payload.Rows[0].Data
	}
	return batch, nil
}

func (adapter *Adapter) ApplyJournal(ctx context.Context, record physicalmigration.Record, entry physicalmigration.JournalEntry) (bool, error) {
	return adapter.targetOps.ApplyJournal(ctx, record, entry)
}

// Outbox rows are part of the online base copy and trigger journal. This gate
// only proves the source set remains within the configured validation bound;
// it performs no second source-to-target write.
func (adapter *Adapter) CaptureOutbox(ctx context.Context, record physicalmigration.Record, maxRows int) error {
	if maxRows <= 0 {
		return physicalmigration.ErrInvalidInput
	}
	var count int
	if err := adapter.control.QueryRow(ctx, `
SELECT count(*)
FROM public.physical_source_outbox_rows
WHERE source_shard_id=$1 AND train_run_id=$2 AND assignment_generation=$3`,
		adapter.sourceID, record.TrainRunID, record.SourceGeneration).Scan(&count); err != nil {
		return fmt.Errorf("%w: count control-source outbox", physicalpostgres.ErrShardOperation)
	}
	if count > maxRows {
		return physicalmigration.ErrCleanupLimitExceeded
	}
	return nil
}

func (adapter *Adapter) Validate(ctx context.Context, request physicalmigration.ValidationRequest) (physicalmigration.ValidationResult, error) {
	if request.MaxRows <= 0 || request.MaxTables < len(tableOrder) {
		return physicalmigration.ValidationResult{}, physicalmigration.ErrInvalidInput
	}
	result := physicalmigration.ValidationResult{Passed: true, Version: request.Migration.ValidationVersion + 1}
	remaining := request.MaxRows
	for _, table := range tableOrder {
		if remaining <= 0 {
			result.Passed = false
			result.Truncated = true
			return result, nil
		}
		sourceRows, err := adapter.allSourceRows(ctx, request.Migration, table, remaining+1)
		if err != nil {
			return physicalmigration.ValidationResult{}, fmt.Errorf("validate source %s: %w", table, err)
		}
		targetRows, err := adapter.allTargetRows(ctx, request.Migration, table, remaining+1)
		if err != nil {
			return physicalmigration.ValidationResult{}, fmt.Errorf("validate target %s: %w", table, err)
		}
		result.Tables++
		if len(sourceRows)+len(targetRows) > remaining {
			result.Passed = false
			result.Truncated = true
			return result, nil
		}
		result.RowsExamined += len(sourceRows) + len(targetRows)
		remaining -= len(sourceRows) + len(targetRows)
		if digestRows(sourceRows) != digestRows(targetRows) {
			result.Passed = false
		}
	}
	return result, nil
}

func (adapter *Adapter) FenceSource(ctx context.Context, record physicalmigration.Record) (int64, error) {
	tx, err := adapter.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return 0, fmt.Errorf("%w: begin control-source fence", physicalpostgres.ErrShardOperation)
	}
	rollback := func(result error) (int64, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return 0, result
	}
	tag, err := tx.Exec(ctx, sourceFenceDisableSQL(adapter.sourceID), record.TrainRunID, record.SourceGeneration)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(fmt.Errorf("%w: disable control-source writer", physicalpostgres.ErrShardOperation))
	}
	var sequence int64
	if err := tx.QueryRow(ctx, `
SELECT next_sequence
FROM public.physical_source_migration_capture_state
WHERE train_run_id=$1 AND migration_id=$2 AND source_shard_id=$3
  AND source_generation=$4 AND capture_enabled
FOR UPDATE`, record.TrainRunID, record.MigrationID, adapter.sourceID,
		record.SourceGeneration).Scan(&sequence); err != nil {
		return rollback(fmt.Errorf("%w: lock control-source sequence", physicalpostgres.ErrShardOperation))
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("%w: commit control-source fence", physicalpostgres.ErrShardOperation)
	}
	return sequence, nil
}

func sourceFenceDisableSQL(sourceID string) string {
	switch sourceID {
	case SourceLegacy:
		return `UPDATE public.train_run_write_fences SET write_enabled=false WHERE train_run_id=$1 AND assignment_generation=$2 AND write_enabled`
	case SourceZero:
		return `UPDATE booking_shard_0.train_run_write_fences SET write_enabled=false WHERE train_run_id=$1 AND assignment_generation=$2 AND write_enabled`
	case SourceOne:
		return `UPDATE booking_shard_1.train_run_write_fences SET write_enabled=false WHERE train_run_id=$1 AND assignment_generation=$2 AND write_enabled`
	default:
		return `SELECT 0 WHERE false`
	}
}

func (adapter *Adapter) EnableTarget(ctx context.Context, record physicalmigration.Record) error {
	return adapter.targetOps.EnableTarget(ctx, record)
}

func (adapter *Adapter) TargetWriteCount(ctx context.Context, record physicalmigration.Record) (int64, error) {
	return adapter.targetOps.TargetWriteCount(ctx, record)
}

func (adapter *Adapter) TargetCommandOutboxEvidence(ctx context.Context, record physicalmigration.Record) (int64, error) {
	return adapter.targetOps.TargetCommandOutboxEvidence(ctx, record)
}

func (adapter *Adapter) RollbackBeforeTargetWrites(ctx context.Context, record physicalmigration.Record, rollbackGeneration int64) error {
	if rollbackGeneration <= record.TargetGeneration {
		return physicalmigration.ErrGenerationNotNewer
	}
	if err := adapter.disableZeroWriteTarget(ctx, record); err != nil {
		return err
	}
	tx, err := adapter.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("%w: begin control-source rebind", physicalpostgres.ErrShardOperation)
	}
	rollback := func(result error) error {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.physical_source_migration_capture_state
SET capture_enabled=false, disabled_at=COALESCE(disabled_at,clock_timestamp())
WHERE train_run_id=$1 AND migration_id=$2 AND source_shard_id=$3
  AND source_generation=$4`, record.TrainRunID, record.MigrationID, adapter.sourceID,
		record.SourceGeneration); err != nil {
		return rollback(fmt.Errorf("%w: disable rollback capture", physicalpostgres.ErrShardOperation))
	}
	tag, err := tx.Exec(ctx, sourceFenceRebindSQL(adapter.sourceID), record.TrainRunID,
		rollbackGeneration, record.SourceGeneration)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(fmt.Errorf("%w: rebind control-source writer", physicalpostgres.ErrShardOperation))
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit control-source rebind", physicalpostgres.ErrShardOperation)
	}
	return nil
}

func (adapter *Adapter) disableZeroWriteTarget(ctx context.Context, record physicalmigration.Record) error {
	tx, err := adapter.target.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("%w: begin rollback target", physicalpostgres.ErrShardOperation)
	}
	rollback := func(result error) error {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	var writes, baselineReservations, baselineReceipts, baselineOutbox int64
	var initialized bool
	if err := tx.QueryRow(ctx, `
SELECT successful_write_count, baseline_initialized,
       baseline_reservation_count, baseline_command_receipt_count,
       baseline_outbox_count
FROM public.train_run_target_write_evidence
WHERE train_run_id=$1 AND assignment_generation=$2
FOR UPDATE`, record.TrainRunID, record.TargetGeneration).Scan(&writes, &initialized,
		&baselineReservations, &baselineReceipts, &baselineOutbox); err != nil {
		return rollback(physicalmigration.ErrTargetEvidenceMissing)
	}
	if !initialized || writes != 0 {
		return rollback(physicalmigration.ErrReverseMigrationRequired)
	}
	var reservations, receipts, outbox int64
	if err := tx.QueryRow(ctx, `
SELECT
 (SELECT count(*) FROM public.reservations WHERE train_run_id=$1 AND assignment_generation=$2),
 (SELECT count(*) FROM public.booking_command_receipts WHERE train_run_id=$1 AND assignment_generation=$2),
 (SELECT count(*) FROM public.outbox_events WHERE train_run_id=$1 AND assignment_generation=$2)`,
		record.TrainRunID, record.TargetGeneration).Scan(&reservations, &receipts, &outbox); err != nil {
		return rollback(physicalmigration.ErrTargetEvidenceMissing)
	}
	if reservations != baselineReservations || receipts != baselineReceipts || outbox != baselineOutbox {
		return rollback(physicalmigration.ErrReverseMigrationRequired)
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.train_run_write_fences SET write_enabled=false,state='disabled'
WHERE train_run_id=$1 AND assignment_generation=$2
  AND state IN ('standby','active','disabled')`, record.TrainRunID, record.TargetGeneration)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(fmt.Errorf("%w: disable rollback target", physicalpostgres.ErrShardOperation))
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit rollback target", physicalpostgres.ErrShardOperation)
	}
	return nil
}

func sourceFenceRebindSQL(sourceID string) string {
	switch sourceID {
	case SourceLegacy:
		return `UPDATE public.train_run_write_fences SET assignment_generation=$2,write_enabled=true WHERE train_run_id=$1 AND assignment_generation=$3 AND NOT write_enabled`
	case SourceZero:
		return `UPDATE booking_shard_0.train_run_write_fences SET assignment_generation=$2,write_enabled=true WHERE train_run_id=$1 AND assignment_generation=$3 AND NOT write_enabled`
	case SourceOne:
		return `UPDATE booking_shard_1.train_run_write_fences SET assignment_generation=$2,write_enabled=true WHERE train_run_id=$1 AND assignment_generation=$3 AND NOT write_enabled`
	default:
		return `SELECT 0 WHERE false`
	}
}

func (adapter *Adapter) DisableCapture(ctx context.Context, record physicalmigration.Record) error {
	tag, err := execOne(ctx, adapter.control, `
UPDATE public.physical_source_migration_capture_state
SET capture_enabled=false,disabled_at=COALESCE(disabled_at,clock_timestamp())
WHERE train_run_id=$1 AND migration_id=$2 AND source_shard_id=$3
  AND source_generation=$4 AND (capture_enabled OR disabled_at IS NOT NULL)`,
		record.TrainRunID, record.MigrationID, adapter.sourceID, record.SourceGeneration)
	if err != nil || tag != 1 {
		return fmt.Errorf("%w: disable control-source capture", physicalpostgres.ErrShardOperation)
	}
	return nil
}

func (adapter *Adapter) RetainSource(ctx context.Context, record physicalmigration.Record) error {
	var retained bool
	if err := adapter.control.QueryRow(ctx, sourceFenceRetainedSQL(adapter.sourceID),
		record.TrainRunID, record.SourceGeneration).Scan(&retained); err != nil || !retained {
		return fmt.Errorf("%w: retain control source", physicalpostgres.ErrShardOperation)
	}
	return nil
}

func sourceFenceRetainedSQL(sourceID string) string {
	switch sourceID {
	case SourceLegacy:
		return `SELECT EXISTS (SELECT 1 FROM public.train_run_write_fences WHERE train_run_id=$1 AND assignment_generation=$2 AND NOT write_enabled)`
	case SourceZero:
		return `SELECT EXISTS (SELECT 1 FROM booking_shard_0.train_run_write_fences WHERE train_run_id=$1 AND assignment_generation=$2 AND NOT write_enabled)`
	case SourceOne:
		return `SELECT EXISTS (SELECT 1 FROM booking_shard_1.train_run_write_fences WHERE train_run_id=$1 AND assignment_generation=$2 AND NOT write_enabled)`
	default:
		return `SELECT false`
	}
}

func execOne(ctx context.Context, db physicalpostgres.DB, sql string, args ...any) (int64, error) {
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

func (adapter *Adapter) readExact(ctx context.Context, record physicalmigration.Record, table string, id uuid.UUID) (physicalmigration.BaseBatch, error) {
	return adapter.readRows(ctx, record, table, uuid.Nil, id, 2)
}

func (adapter *Adapter) readRows(ctx context.Context, record physicalmigration.Record, table string, after, exact uuid.UUID, limit int) (physicalmigration.BaseBatch, error) {
	query, ok := sourceQuery(table)
	if !ok || limit <= 0 {
		return physicalmigration.BaseBatch{}, physicalmigration.ErrInvalidInput
	}
	rows, err := adapter.control.Query(ctx, query, record.TrainRunID, adapter.sourceID,
		record.SourceGeneration, after, nullableUUID(exact), limit)
	if err != nil {
		return physicalmigration.BaseBatch{}, fmt.Errorf("%w: read transformed control source", physicalpostgres.ErrShardOperation)
	}
	defer rows.Close()
	payload := physicalpostgres.BasePayload{}
	for rows.Next() {
		var row physicalpostgres.JSONRow
		row.Table = table
		if err := rows.Scan(&row.ID, &row.Data); err != nil {
			return physicalmigration.BaseBatch{}, fmt.Errorf("%w: scan transformed control source", physicalpostgres.ErrShardOperation)
		}
		payload.Rows = append(payload.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return physicalmigration.BaseBatch{}, fmt.Errorf("%w: iterate transformed control source", physicalpostgres.ErrShardOperation)
	}
	return physicalmigration.BaseBatch{ObjectName: table, Rows: len(payload.Rows),
		Fingerprint: payloadFingerprint(payload), Payload: payload}, nil
}

func nullableUUID(id uuid.UUID) any {
	if id == uuid.Nil {
		return nil
	}
	return id
}

func payloadFingerprint(payload physicalpostgres.BasePayload) [32]byte {
	hash := sha256.New()
	for _, row := range payload.Rows {
		_, _ = hash.Write([]byte(row.Table))
		_, _ = hash.Write(row.ID[:])
		_, _ = hash.Write(row.Data)
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func entryFingerprint(entry physicalmigration.JournalEntry) [32]byte {
	hash := sha256.New()
	_, _ = hash.Write(entry.ID[:])
	var sequence [8]byte
	binary.BigEndian.PutUint64(sequence[:], uint64(entry.Sequence))
	_, _ = hash.Write(sequence[:])
	_, _ = hash.Write([]byte(entry.TableName))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(entry.Operation))
	_, _ = hash.Write(entry.EntityID[:])
	_, _ = hash.Write(entry.PrimaryKey)
	_, _ = hash.Write(entry.Metadata)
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func knownTable(name string) bool {
	for _, candidate := range tableOrder {
		if candidate == name {
			return true
		}
	}
	return false
}

func parseCursor(cursor string) (int, uuid.UUID, error) {
	if cursor == "" {
		return 0, uuid.Nil, nil
	}
	if cursor == "complete" {
		return len(tableOrder), uuid.Nil, nil
	}
	parts := strings.Split(cursor, ":")
	if len(parts) != 2 {
		return 0, uuid.Nil, physicalmigration.ErrInvalidInput
	}
	index, err := strconv.Atoi(parts[0])
	if err != nil || index < 0 || index >= len(tableOrder) {
		return 0, uuid.Nil, physicalmigration.ErrInvalidInput
	}
	id, err := uuid.Parse(parts[1])
	if err != nil || id == uuid.Nil {
		return 0, uuid.Nil, physicalmigration.ErrInvalidInput
	}
	return index, id, nil
}

func encodeCursor(index int, id uuid.UUID) string {
	return strconv.Itoa(index) + ":" + id.String()
}

type canonicalRow struct {
	ID   uuid.UUID
	Data []byte
}

func (adapter *Adapter) allSourceRows(ctx context.Context, record physicalmigration.Record, table string, limit int) ([]canonicalRow, error) {
	if table == "booking_command_receipts" {
		return nil, nil
	}
	batch, err := adapter.readRows(ctx, record, table, uuid.Nil, uuid.Nil, limit)
	if err != nil {
		return nil, err
	}
	payload := batch.Payload.(physicalpostgres.BasePayload)
	result := make([]canonicalRow, 0, len(payload.Rows))
	for _, row := range payload.Rows {
		data, err := canonicalJSON(row.Data)
		if err != nil {
			return nil, physicalmigration.ErrInvalidBatch
		}
		result = append(result, canonicalRow{ID: row.ID, Data: data})
	}
	return result, nil
}

func (adapter *Adapter) allTargetRows(ctx context.Context, record physicalmigration.Record, table string, limit int) ([]canonicalRow, error) {
	query, ok := targetQuery(table)
	if !ok {
		return nil, physicalmigration.ErrInvalidInput
	}
	rows, err := adapter.target.Query(ctx, query, record.TrainRunID, record.TargetGeneration, limit)
	if err != nil {
		return nil, fmt.Errorf("%w: read validation target", physicalpostgres.ErrShardOperation)
	}
	defer rows.Close()
	result := make([]canonicalRow, 0)
	for rows.Next() {
		var row canonicalRow
		if err := rows.Scan(&row.ID, &row.Data); err != nil {
			return nil, fmt.Errorf("%w: scan validation target", physicalpostgres.ErrShardOperation)
		}
		row.Data, err = canonicalJSON(row.Data)
		if err != nil {
			return nil, physicalmigration.ErrInvalidBatch
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate validation target", physicalpostgres.ErrShardOperation)
	}
	return result, nil
}

func canonicalJSON(data []byte) ([]byte, error) {
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	delete(value, "assignment_generation")
	delete(value, "updated_at")
	if _, ok := value["locked_at"]; ok {
		value["locked_at"] = nil
		value["locked_by"] = nil
		value["lease_token"] = nil
		if value["status"] == "processing" {
			value["status"] = "pending"
		}
	}
	return json.Marshal(value)
}

func digestRows(rows []canonicalRow) [32]byte {
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].ID[:], rows[j].ID[:]) < 0 })
	hash := sha256.New()
	for _, row := range rows {
		_, _ = hash.Write(row.ID[:])
		_, _ = hash.Write(row.Data)
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

// Compile-time assertion keeps the adapter aligned with the migration engine.
var _ physicalmigration.ShardOperations = (*Adapter)(nil)
