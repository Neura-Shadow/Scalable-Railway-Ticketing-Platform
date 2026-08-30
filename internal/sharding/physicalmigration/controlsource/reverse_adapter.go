package controlsource

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	physicalpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ReverseAdapter migrates a physical booking-shard v3 source back into one of
// the three fixed control-database booking layouts after control migration 11
// has installed partial-refund-compatible columns and durable receipt relations.
type ReverseAdapter struct {
	targetID  string
	source    physicalpostgres.DB
	control   physicalpostgres.DB
	sourceOps *physicalpostgres.Shards
}

const reverseSourcePreflightSQL = `
SELECT current_setting('server_version_num')::integer >= 160000
   AND EXISTS (
       SELECT 1 FROM public.schema_migrations WHERE version=3 AND NOT dirty
   )
   AND to_regclass('public.train_run_booking_snapshots') IS NOT NULL
   AND to_regclass('public.train_run_mutation_journal') IS NOT NULL
   AND to_regclass('public.migration_capture_state') IS NOT NULL
   AND to_regclass('public.booking_command_receipts') IS NOT NULL
   AND to_regclass('public.payment_command_receipts') IS NOT NULL
   AND to_regclass('public.ticket_issuance_receipts') IS NOT NULL
   AND to_regclass('public.payment_refund_receipts') IS NOT NULL
   AND to_regclass('public.payment_compensation_receipts') IS NOT NULL
   AND to_regclass('public.ticket_refund_prepare_receipts') IS NOT NULL
   AND to_regclass('public.ticket_refund_compensation_receipts') IS NOT NULL
   AND to_regclass('public.selected_ticket_refund_receipts') IS NOT NULL
   AND to_regclass('public.migration_evidence_mutation_authorizations') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM public.regional_write_authority
       WHERE singleton AND state='active' AND writes_enabled
   )
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
           ('booking_command_receipts_capture_mutation'),
           ('payment_command_receipts_capture_mutation'),
           ('ticket_issuance_receipts_capture_mutation'),
           ('payment_refund_receipts_capture_mutation'),
           ('payment_compensation_receipts_capture_mutation'),
           ('ticket_refund_prepare_receipts_capture_mutation'),
           ('ticket_refund_compensation_receipts_capture_mutation'),
           ('selected_ticket_refund_receipts_capture_mutation')
       ) AS required(trigger_name)
       WHERE NOT EXISTS (
           SELECT 1 FROM pg_catalog.pg_trigger
           WHERE tgname=required.trigger_name AND NOT tgisinternal
       )
   )
   AND EXISTS (
       SELECT 1 FROM public.train_run_booking_snapshots
       WHERE train_run_id=$1 AND assignment_generation=$2
         AND isfinite(scheduled_departure_at)
   )
   AND EXISTS (
       SELECT 1 FROM public.train_run_write_fences
       WHERE train_run_id=$1 AND assignment_generation=$2
         AND state='active' AND write_enabled
   )`

const reverseTargetPreflightSQL = `
SELECT current_setting('server_version_num')::integer >= 160000
   AND EXISTS (SELECT 1 FROM public.schema_migrations WHERE version=11 AND NOT dirty)
   AND to_regclass('public.physical_control_target_apply_receipts') IS NOT NULL
   AND to_regprocedure('public.guard_control_booking_receipt_write()') IS NOT NULL
   AND to_regprocedure('public.capture_physical_source_receipt_mutation()') IS NOT NULL
   AND to_regprocedure('public.guard_control_ticket_refund_evidence_mutation()') IS NOT NULL
   AND to_regclass('public.physical_source_ticket_refund_prepare_receipt_rows') IS NOT NULL
   AND to_regclass('public.physical_source_ticket_refund_compensation_receipt_rows') IS NOT NULL
   AND to_regclass('public.physical_source_selected_ticket_refund_receipt_rows') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM public.regional_write_authority
       WHERE singleton AND state='active' AND writes_enabled
   )
   AND CASE $5
       WHEN 'legacy' THEN
           to_regclass('public.booking_command_receipts') IS NOT NULL
           AND to_regclass('public.payment_command_receipts') IS NOT NULL
           AND to_regclass('public.ticket_issuance_receipts') IS NOT NULL
           AND to_regclass('public.payment_refund_receipts') IS NOT NULL
           AND to_regclass('public.payment_compensation_receipts') IS NOT NULL
           AND to_regclass('public.ticket_refund_prepare_receipts') IS NOT NULL
           AND to_regclass('public.ticket_refund_compensation_receipts') IS NOT NULL
           AND to_regclass('public.selected_ticket_refund_receipts') IS NOT NULL
       WHEN 'shard-0' THEN
           to_regclass('booking_shard_0.booking_command_receipts') IS NOT NULL
           AND to_regclass('booking_shard_0.payment_command_receipts') IS NOT NULL
           AND to_regclass('booking_shard_0.ticket_issuance_receipts') IS NOT NULL
           AND to_regclass('booking_shard_0.payment_refund_receipts') IS NOT NULL
           AND to_regclass('booking_shard_0.payment_compensation_receipts') IS NOT NULL
           AND to_regclass('booking_shard_0.ticket_refund_prepare_receipts') IS NOT NULL
           AND to_regclass('booking_shard_0.ticket_refund_compensation_receipts') IS NOT NULL
           AND to_regclass('booking_shard_0.selected_ticket_refund_receipts') IS NOT NULL
       WHEN 'shard-1' THEN
           to_regclass('booking_shard_1.booking_command_receipts') IS NOT NULL
           AND to_regclass('booking_shard_1.payment_command_receipts') IS NOT NULL
           AND to_regclass('booking_shard_1.ticket_issuance_receipts') IS NOT NULL
           AND to_regclass('booking_shard_1.payment_refund_receipts') IS NOT NULL
           AND to_regclass('booking_shard_1.payment_compensation_receipts') IS NOT NULL
           AND to_regclass('booking_shard_1.ticket_refund_prepare_receipts') IS NOT NULL
           AND to_regclass('booking_shard_1.ticket_refund_compensation_receipts') IS NOT NULL
           AND to_regclass('booking_shard_1.selected_ticket_refund_receipts') IS NOT NULL
       ELSE false
   END
   AND NOT EXISTS (
       SELECT 1
       FROM (VALUES
           ('booking_command_receipts'), ('payment_command_receipts'),
           ('ticket_issuance_receipts'), ('payment_refund_receipts'),
           ('payment_compensation_receipts'),
           ('ticket_refund_prepare_receipts'),
           ('ticket_refund_compensation_receipts'),
           ('selected_ticket_refund_receipts')
       ) AS required_table(table_name)
       CROSS JOIN (VALUES
           ('physical_target_write_guard'), ('physical_source_capture')
       ) AS required_trigger(trigger_name)
       WHERE NOT EXISTS (
           SELECT 1
           FROM pg_catalog.pg_trigger AS trigger_row
           JOIN pg_catalog.pg_class AS table_row ON table_row.oid=trigger_row.tgrelid
           JOIN pg_catalog.pg_namespace AS schema_row ON schema_row.oid=table_row.relnamespace
           WHERE NOT trigger_row.tgisinternal
             AND schema_row.nspname=CASE $5
                 WHEN 'legacy' THEN 'public'
                 WHEN 'shard-0' THEN 'booking_shard_0'
                 WHEN 'shard-1' THEN 'booking_shard_1'
             END
             AND table_row.relname=required_table.table_name
             AND trigger_row.tgname=required_trigger.trigger_name
       )
   )
   AND NOT EXISTS (
       SELECT 1
       FROM (VALUES
           ('ticket_refund_prepare_receipts','ticket_refund_prepare_receipts_guard_evidence'),
           ('ticket_refund_compensation_receipts','ticket_refund_compensation_receipts_guard_evidence'),
           ('selected_ticket_refund_receipts','selected_ticket_refund_receipts_guard_evidence')
       ) AS required(table_name,trigger_name)
       WHERE NOT EXISTS (
           SELECT 1 FROM pg_catalog.pg_trigger AS trigger_row
           JOIN pg_catalog.pg_class AS table_row ON table_row.oid=trigger_row.tgrelid
           JOIN pg_catalog.pg_namespace AS schema_row ON schema_row.oid=table_row.relnamespace
           WHERE NOT trigger_row.tgisinternal
             AND schema_row.nspname=CASE $5
                 WHEN 'legacy' THEN 'public'
                 WHEN 'shard-0' THEN 'booking_shard_0'
                 WHEN 'shard-1' THEN 'booking_shard_1'
             END
             AND table_row.relname=required.table_name
             AND trigger_row.tgname=required.trigger_name
       )
   )
   AND NOT EXISTS (
       SELECT 1 FROM (VALUES
           ('reservations','reservations_guard_payment_snapshot'),
           ('ticket_orders','ticket_orders_guard_payment_snapshot')
       ) AS required(table_name,trigger_name)
       WHERE NOT EXISTS (
           SELECT 1
           FROM pg_catalog.pg_trigger AS trigger_row
           JOIN pg_catalog.pg_class AS table_row ON table_row.oid=trigger_row.tgrelid
           JOIN pg_catalog.pg_namespace AS schema_row ON schema_row.oid=table_row.relnamespace
           WHERE NOT trigger_row.tgisinternal
             AND schema_row.nspname=CASE $5
                 WHEN 'legacy' THEN 'public'
                 WHEN 'shard-0' THEN 'booking_shard_0'
                 WHEN 'shard-1' THEN 'booking_shard_1'
             END
             AND table_row.relname=required.table_name
             AND trigger_row.tgname=required.trigger_name
       )
   )
   AND assignment.shard_id=$2
   AND assignment.assignment_generation=$3
   AND assignment.active_physical_migration_id=$4
   AND shard.storage_kind='postgres'
   AND target.storage_kind IN ('legacy_schema','logical_schema')
   AND target.enabled AND target.write_enabled
FROM public.train_run_shard_assignments AS assignment
JOIN public.booking_shards AS shard ON shard.shard_id=assignment.shard_id
JOIN public.booking_shards AS target ON target.shard_id=$5
WHERE assignment.train_run_id=$1`

func NewReverse(control, source physicalpostgres.DB, targetID string) (*ReverseAdapter, error) {
	if control == nil || source == nil || !validSource(targetID) {
		return nil, physicalmigration.ErrInvalidInput
	}
	sourceOps, err := physicalpostgres.NewDefaultShards(source, control)
	if err != nil {
		return nil, err
	}
	return &ReverseAdapter{targetID: targetID, source: source, control: control, sourceOps: sourceOps}, nil
}

func (adapter *ReverseAdapter) Preflight(ctx context.Context, record physicalmigration.Record) error {
	if adapter == nil || !record.ReverseMigration || record.TargetShardID != adapter.targetID ||
		record.SourceProtocolVersion != 1 || record.SourceSchemaVersion != 3 ||
		record.TargetProtocolVersion != 1 || record.TargetSchemaVersion != 8 {
		return physicalmigration.ErrCheckpointConflict
	}
	var sourceReady bool
	if err := adapter.source.QueryRow(ctx, reverseSourcePreflightSQL,
		record.TrainRunID, record.SourceGeneration).Scan(&sourceReady); err != nil || !sourceReady {
		return physicalmigration.ErrCheckpointConflict
	}
	var targetReady bool
	if err := adapter.control.QueryRow(ctx, reverseTargetPreflightSQL,
		record.TrainRunID, record.SourceShardID,
		record.SourceGeneration, record.MigrationID, adapter.targetID).Scan(&targetReady); err != nil || !targetReady {
		return physicalmigration.ErrCheckpointConflict
	}
	return nil
}

func (adapter *ReverseAdapter) PrepareTarget(ctx context.Context, record physicalmigration.Record) error {
	statements := reverseCleanupStatements(adapter.targetID)
	if len(statements) == 0 {
		return physicalmigration.ErrInvalidInput
	}
	tx, err := adapter.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("%w: begin reverse control-target prep", physicalpostgres.ErrShardOperation)
	}
	rollback := func(result error) error {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	if err := adapter.authorizeTargetApply(ctx, tx, record); err != nil {
		return rollback(err)
	}
	var rows int
	if err := tx.QueryRow(ctx, reverseCleanupCountSQL(adapter.targetID), record.TrainRunID,
		adapter.targetID).Scan(&rows); err != nil {
		return rollback(fmt.Errorf("%w: count reverse control target", physicalpostgres.ErrShardOperation))
	}
	if rows > 10000 {
		return rollback(physicalmigration.ErrCleanupLimitExceeded)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.physical_source_migration_capture_state
SET capture_enabled=false,disabled_at=COALESCE(disabled_at,clock_timestamp())
WHERE train_run_id=$1 AND source_shard_id=$2 AND capture_enabled`, record.TrainRunID,
		adapter.targetID); err != nil {
		return rollback(fmt.Errorf("%w: disable retained control capture", physicalpostgres.ErrShardOperation))
	}
	for _, statement := range statements {
		args := []any{record.TrainRunID}
		if strings.Contains(statement, "$2") {
			args = append(args, adapter.targetID)
		}
		if _, err := tx.Exec(ctx, statement, args...); err != nil {
			return rollback(fmt.Errorf("%w: clear reverse control target", physicalpostgres.ErrShardOperation))
		}
	}
	tag, err := tx.Exec(ctx, reverseTargetFencePrepareSQL(adapter.targetID), record.TrainRunID,
		record.TargetGeneration)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(fmt.Errorf("%w: prepare reverse control fence", physicalpostgres.ErrShardOperation))
	}
	if err := adapter.releaseTargetApply(ctx, tx, record); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit reverse control-target prep", physicalpostgres.ErrShardOperation)
	}
	return nil
}

func (adapter *ReverseAdapter) EnableCapture(ctx context.Context, record physicalmigration.Record) (int64, error) {
	return adapter.sourceOps.EnableCapture(ctx, record)
}

func (adapter *ReverseAdapter) ReadBaseBatch(ctx context.Context, request physicalmigration.BaseCopyRequest) (physicalmigration.BaseBatch, error) {
	return adapter.sourceOps.ReadBaseBatch(ctx, request)
}

func (adapter *ReverseAdapter) ApplyBaseBatch(ctx context.Context, record physicalmigration.Record, batch physicalmigration.BaseBatch) error {
	payload, ok := batch.Payload.(physicalpostgres.BasePayload)
	if !ok || len(payload.Rows) != batch.Rows || reverseBaseFingerprint(payload) != batch.Fingerprint {
		return physicalmigration.ErrInvalidBatch
	}
	tx, err := adapter.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("%w: begin reverse base apply", physicalpostgres.ErrShardOperation)
	}
	rollback := func(result error) error {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	if err := adapter.authorizeTargetApply(ctx, tx, record); err != nil {
		return rollback(err)
	}
	for _, row := range payload.Rows {
		if reverseIgnoredTable(row.Table) {
			continue
		}
		if row.ID == uuid.Nil || len(row.Data) == 0 || !reverseSupportedTable(row.Table) {
			return rollback(physicalmigration.ErrInvalidBatch)
		}
		normalized, err := normalizeReversePayload(row.Data, row.Table, record)
		if err != nil {
			return rollback(physicalmigration.ErrInvalidBatch)
		}
		statement := reverseUpsertSQL(adapter.targetID, row.Table)
		if statement == "" {
			return rollback(physicalmigration.ErrInvalidBatch)
		}
		if _, err := tx.Exec(ctx, statement, normalized); err != nil {
			return rollback(fmt.Errorf("%w: apply reverse base row: %w", physicalpostgres.ErrShardOperation, err))
		}
	}
	if err := adapter.releaseTargetApply(ctx, tx, record); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit reverse base apply", physicalpostgres.ErrShardOperation)
	}
	return nil
}

func (adapter *ReverseAdapter) ReadJournal(ctx context.Context, request physicalmigration.JournalRequest) (physicalmigration.JournalBatch, error) {
	return adapter.sourceOps.ReadJournal(ctx, request)
}

func (adapter *ReverseAdapter) ApplyJournal(ctx context.Context, record physicalmigration.Record, entry physicalmigration.JournalEntry) (bool, error) {
	if entry.ID == uuid.Nil || entry.EntityID == uuid.Nil || entry.Sequence <= 0 ||
		(!reverseIgnoredTable(entry.TableName) && !reverseSupportedTable(entry.TableName)) {
		return false, physicalmigration.ErrInvalidBatch
	}
	tx, err := adapter.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, fmt.Errorf("%w: begin reverse journal apply", physicalpostgres.ErrShardOperation)
	}
	rollback := func(result error) (bool, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return false, result
	}
	if err := adapter.authorizeTargetApply(ctx, tx, record); err != nil {
		return rollback(err)
	}
	fingerprint := entry.ApplyFingerprint
	if fingerprint == ([32]byte{}) {
		fingerprint = entryFingerprint(entry)
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO public.physical_control_target_apply_receipts (
 migration_id,source_journal_id,train_run_id,target_shard_id,
 target_generation,mutation_sequence,apply_fingerprint
) VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (migration_id,source_journal_id) DO NOTHING`, record.MigrationID, entry.ID,
		record.TrainRunID, adapter.targetID, record.TargetGeneration, entry.Sequence, fingerprint[:])
	if err != nil {
		return rollback(fmt.Errorf("%w: reserve reverse apply receipt", physicalpostgres.ErrShardOperation))
	}
	if tag.RowsAffected() == 0 {
		var trainRun uuid.UUID
		var target string
		var generation, sequence int64
		var stored []byte
		if err := tx.QueryRow(ctx, `
SELECT train_run_id,target_shard_id,target_generation,mutation_sequence,apply_fingerprint
FROM public.physical_control_target_apply_receipts
WHERE migration_id=$1 AND source_journal_id=$2 FOR UPDATE`, record.MigrationID,
			entry.ID).Scan(&trainRun, &target, &generation, &sequence, &stored); err != nil ||
			trainRun != record.TrainRunID || target != adapter.targetID ||
			generation != record.TargetGeneration || sequence != entry.Sequence ||
			!bytes.Equal(stored, fingerprint[:]) {
			return rollback(physicalpostgres.ErrApplyReceiptConflict)
		}
		if err := adapter.releaseTargetApply(ctx, tx, record); err != nil {
			return rollback(err)
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("%w: commit reverse replay receipt", physicalpostgres.ErrShardOperation)
		}
		return true, nil
	}
	if !reverseIgnoredTable(entry.TableName) {
		if entry.Operation == "DELETE" {
			statement := reverseDeleteSQL(adapter.targetID, entry.TableName)
			if statement == "" {
				return rollback(physicalmigration.ErrInvalidBatch)
			}
			var applyErr error
			if entry.TableName == "outbox_events" {
				_, applyErr = tx.Exec(ctx, statement, record.TrainRunID, entry.EntityID,
					adapter.targetID)
			} else {
				_, applyErr = tx.Exec(ctx, statement, record.TrainRunID, entry.EntityID)
			}
			if applyErr != nil {
				return rollback(fmt.Errorf("%w: apply reverse tombstone", physicalpostgres.ErrShardOperation))
			}
		} else if entry.Operation == "INSERT" || entry.Operation == "UPDATE" {
			payload, ok := entry.Payload.([]byte)
			if !ok {
				return rollback(physicalmigration.ErrInvalidBatch)
			}
			normalized, err := normalizeReversePayload(payload, entry.TableName, record)
			if err != nil {
				return rollback(physicalmigration.ErrInvalidBatch)
			}
			if _, err := tx.Exec(ctx, reverseUpsertSQL(adapter.targetID, entry.TableName), normalized); err != nil {
				return rollback(fmt.Errorf("%w: apply reverse journal row", physicalpostgres.ErrShardOperation))
			}
		} else {
			return rollback(physicalmigration.ErrInvalidBatch)
		}
	}
	if err := adapter.releaseTargetApply(ctx, tx, record); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("%w: commit reverse journal apply", physicalpostgres.ErrShardOperation)
	}
	return false, nil
}

func (adapter *ReverseAdapter) CaptureOutbox(ctx context.Context, record physicalmigration.Record, maxRows int) error {
	if maxRows <= 0 {
		return physicalmigration.ErrInvalidInput
	}
	rows, err := adapter.source.Query(ctx, `
SELECT to_jsonb(source_row)
FROM public.outbox_events AS source_row
WHERE train_run_id=$1 AND assignment_generation=$2
ORDER BY id LIMIT $3`, record.TrainRunID, record.SourceGeneration, maxRows+1)
	if err != nil {
		return fmt.Errorf("%w: read reverse source outbox", physicalpostgres.ErrShardOperation)
	}
	var payloads [][]byte
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			rows.Close()
			return fmt.Errorf("%w: scan reverse source outbox", physicalpostgres.ErrShardOperation)
		}
		payloads = append(payloads, payload)
	}
	iterationErr := rows.Err()
	rows.Close()
	if iterationErr != nil {
		return fmt.Errorf("%w: iterate reverse source outbox", physicalpostgres.ErrShardOperation)
	}
	if len(payloads) > maxRows {
		return physicalmigration.ErrCleanupLimitExceeded
	}
	tx, err := adapter.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("%w: begin reverse outbox apply", physicalpostgres.ErrShardOperation)
	}
	rollback := func(result error) error {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	if err := adapter.authorizeTargetApply(ctx, tx, record); err != nil {
		return rollback(err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM public.outbox_events
WHERE train_run_id=$1 AND shard_id=$2 AND assignment_generation=$3`, record.TrainRunID,
		adapter.targetID, record.TargetGeneration); err != nil {
		return rollback(fmt.Errorf("%w: replace reverse target outbox", physicalpostgres.ErrShardOperation))
	}
	statement := reverseUpsertSQL(adapter.targetID, "outbox_events")
	for _, payload := range payloads {
		normalized, err := normalizeReversePayload(payload, "outbox_events", record)
		if err != nil {
			return rollback(physicalmigration.ErrInvalidBatch)
		}
		if _, err := tx.Exec(ctx, statement, normalized); err != nil {
			return rollback(fmt.Errorf("%w: apply reverse target outbox", physicalpostgres.ErrShardOperation))
		}
	}
	if err := adapter.releaseTargetApply(ctx, tx, record); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit reverse outbox apply", physicalpostgres.ErrShardOperation)
	}
	return nil
}

func (adapter *ReverseAdapter) Validate(ctx context.Context, request physicalmigration.ValidationRequest) (physicalmigration.ValidationResult, error) {
	tables := tableOrder
	if request.MaxRows <= 0 || request.MaxTables < len(tables) {
		return physicalmigration.ValidationResult{}, physicalmigration.ErrInvalidInput
	}
	result := physicalmigration.ValidationResult{Passed: true, Version: request.Migration.ValidationVersion + 1}
	remaining := request.MaxRows
	for _, table := range tables {
		sourceRows, err := adapter.reverseValidationRows(ctx, adapter.source,
			reverseSourceValidationSQL(table), request.Migration, table, remaining+1, true)
		if err != nil {
			return physicalmigration.ValidationResult{}, err
		}
		var targetRows []canonicalRow
		if reverseIgnoredTable(table) {
			targetRows, err = adapter.reverseDerivedTargetRows(ctx, request.Migration, table, remaining+1)
		} else {
			targetRows, err = adapter.reverseValidationRows(ctx, adapter.control,
				reverseTargetValidationSQL(adapter.targetID, table), request.Migration, table, remaining+1, false)
		}
		if err != nil {
			return physicalmigration.ValidationResult{}, err
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

func (adapter *ReverseAdapter) reverseDerivedTargetRows(ctx context.Context, record physicalmigration.Record,
	table string, limit int) ([]canonicalRow, error) {
	query, ok := sourceQuery(table)
	if !ok || !reverseIgnoredTable(table) || limit <= 0 {
		return nil, physicalmigration.ErrInvalidInput
	}
	// The assignment remains bound to the physical source until cutover. Derive
	// catalog-backed target rows through that current assignment; canonical
	// validation deliberately removes the generation and shard identity.
	rows, err := adapter.control.Query(ctx, query, record.TrainRunID, record.SourceShardID,
		record.SourceGeneration, uuid.Nil, nil, limit)
	if err != nil {
		return nil, fmt.Errorf("%w: derive reverse validation target", physicalpostgres.ErrShardOperation)
	}
	defer rows.Close()
	result := make([]canonicalRow, 0)
	for rows.Next() {
		var row canonicalRow
		if err := rows.Scan(&row.ID, &row.Data); err != nil {
			return nil, fmt.Errorf("%w: scan reverse derived target", physicalpostgres.ErrShardOperation)
		}
		row.Data, err = reverseCanonicalJSON(row.Data, table)
		if err != nil {
			return nil, physicalmigration.ErrInvalidBatch
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate reverse derived target", physicalpostgres.ErrShardOperation)
	}
	return result, nil
}

func (adapter *ReverseAdapter) reverseValidationRows(ctx context.Context, db physicalpostgres.DB, query string,
	record physicalmigration.Record, table string, limit int, source bool) ([]canonicalRow, error) {
	if query == "" || limit <= 0 {
		return nil, physicalmigration.ErrInvalidInput
	}
	var rows pgx.Rows
	var err error
	if source {
		rows, err = db.Query(ctx, query, record.TrainRunID, record.SourceGeneration, limit)
	} else if table == "outbox_events" {
		rows, err = db.Query(ctx, query, record.TrainRunID, adapter.targetID,
			record.TargetGeneration, limit)
	} else {
		rows, err = db.Query(ctx, query, record.TrainRunID, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: read reverse validation", physicalpostgres.ErrShardOperation)
	}
	defer rows.Close()
	result := make([]canonicalRow, 0)
	for rows.Next() {
		var row canonicalRow
		if err := rows.Scan(&row.ID, &row.Data); err != nil {
			return nil, fmt.Errorf("%w: scan reverse validation", physicalpostgres.ErrShardOperation)
		}
		row.Data, err = reverseCanonicalJSON(row.Data, table)
		if err != nil {
			return nil, physicalmigration.ErrInvalidBatch
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate reverse validation", physicalpostgres.ErrShardOperation)
	}
	return result, nil
}

func (adapter *ReverseAdapter) FenceSource(ctx context.Context, record physicalmigration.Record) (int64, error) {
	return adapter.sourceOps.FenceSource(ctx, record)
}

func (adapter *ReverseAdapter) EnableTarget(ctx context.Context, record physicalmigration.Record) error {
	tag, err := execOne(ctx, adapter.control, reverseTargetFenceEnableSQL(adapter.targetID),
		record.TrainRunID, record.TargetGeneration)
	if err != nil || tag != 1 {
		return fmt.Errorf("%w: enable reverse control target", physicalpostgres.ErrShardOperation)
	}
	return nil
}

func (adapter *ReverseAdapter) TargetWriteCount(ctx context.Context, record physicalmigration.Record) (int64, error) {
	return adapter.controlCommandCount(ctx, record)
}

func (adapter *ReverseAdapter) TargetCommandOutboxEvidence(ctx context.Context, record physicalmigration.Record) (int64, error) {
	return adapter.controlCommandCount(ctx, record)
}

func (adapter *ReverseAdapter) controlCommandCount(ctx context.Context, record physicalmigration.Record) (int64, error) {
	var count int64
	if err := adapter.control.QueryRow(ctx, `
SELECT count(*) FROM public.booking_commands
WHERE train_run_id=$1 AND target_shard_id=$2 AND assignment_generation=$3
  AND state IN ('committed_on_shard','finalized')`, record.TrainRunID, adapter.targetID,
		record.TargetGeneration).Scan(&count); err != nil {
		return 0, fmt.Errorf("%w: inspect reverse target writes", physicalpostgres.ErrShardOperation)
	}
	return count, nil
}

func (adapter *ReverseAdapter) RollbackBeforeTargetWrites(ctx context.Context, record physicalmigration.Record, rollbackGeneration int64) error {
	if rollbackGeneration <= record.TargetGeneration {
		return physicalmigration.ErrGenerationNotNewer
	}
	writes, err := adapter.controlCommandCount(ctx, record)
	if err != nil {
		return err
	}
	if writes != 0 {
		return physicalmigration.ErrReverseMigrationRequired
	}
	if tag, err := execOne(ctx, adapter.control, reverseTargetFenceDisableSQL(adapter.targetID),
		record.TrainRunID, record.TargetGeneration); err != nil || tag != 1 {
		return fmt.Errorf("%w: disable reverse target", physicalpostgres.ErrShardOperation)
	}
	tx, err := adapter.source.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("%w: begin reverse source rollback", physicalpostgres.ErrShardOperation)
	}
	rollback := func(result error) error {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	if _, err := tx.Exec(ctx, `UPDATE public.migration_capture_state
SET capture_enabled=false,disabled_at=COALESCE(disabled_at,clock_timestamp())
WHERE train_run_id=$1 AND migration_id=$2 AND source_generation=$3`, record.TrainRunID,
		record.MigrationID, record.SourceGeneration); err != nil {
		return rollback(fmt.Errorf("%w: disable reverse source capture", physicalpostgres.ErrShardOperation))
	}
	tag, err := tx.Exec(ctx, `UPDATE public.train_run_booking_snapshots
SET assignment_generation=$2 WHERE train_run_id=$1 AND assignment_generation=$3`,
		record.TrainRunID, rollbackGeneration, record.SourceGeneration)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(fmt.Errorf("%w: rebind reverse source snapshot", physicalpostgres.ErrShardOperation))
	}
	tag, err = tx.Exec(ctx, `UPDATE public.train_run_write_fences
SET assignment_generation=$2,write_enabled=true,state='active'
WHERE train_run_id=$1 AND assignment_generation=$3
  AND state IN ('quiescing','retained','active')`, record.TrainRunID, rollbackGeneration,
		record.SourceGeneration)
	if err != nil || tag.RowsAffected() != 1 {
		return rollback(fmt.Errorf("%w: rebind reverse source fence", physicalpostgres.ErrShardOperation))
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit reverse source rollback", physicalpostgres.ErrShardOperation)
	}
	return nil
}

func (adapter *ReverseAdapter) DisableCapture(ctx context.Context, record physicalmigration.Record) error {
	return adapter.sourceOps.DisableCapture(ctx, record)
}

func (adapter *ReverseAdapter) RetainSource(ctx context.Context, record physicalmigration.Record) error {
	return adapter.sourceOps.RetainSource(ctx, record)
}

func (adapter *ReverseAdapter) authorizeTargetApply(ctx context.Context, tx pgx.Tx, record physicalmigration.Record) error {
	tag, err := tx.Exec(ctx, `
INSERT INTO public.physical_control_target_apply_authorizations (
 migration_id,train_run_id,target_shard_id,target_generation,transaction_id
)
SELECT $1,$2,$3,$4,txid_current()
WHERE EXISTS (
 SELECT 1 FROM public.physical_shard_migrations
 WHERE migration_id=$1 AND train_run_id=$2 AND target_shard_id=$3
   AND target_generation=$4 AND reverse_migration
   AND state IN ('preparing_target','capture_enabled','base_copying',
                 'catching_up','validating_online','draining','source_fenced',
                 'final_catchup','final_validating')
) ON CONFLICT (migration_id,transaction_id) DO NOTHING`, record.MigrationID,
		record.TrainRunID, adapter.targetID, record.TargetGeneration)
	if err != nil || tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: authorize reverse control-target apply", physicalpostgres.ErrShardOperation)
	}
	return nil
}

func (adapter *ReverseAdapter) releaseTargetApply(ctx context.Context, tx pgx.Tx, record physicalmigration.Record) error {
	tag, err := tx.Exec(ctx, `
DELETE FROM public.physical_control_target_apply_authorizations
WHERE migration_id=$1 AND train_run_id=$2 AND target_shard_id=$3
  AND target_generation=$4 AND transaction_id=txid_current()`, record.MigrationID,
		record.TrainRunID, adapter.targetID, record.TargetGeneration)
	if err != nil || tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: release reverse control-target apply", physicalpostgres.ErrShardOperation)
	}
	return nil
}

func normalizeReversePayload(data []byte, table string, record physicalmigration.Record) ([]byte, error) {
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if value["train_run_id"] != record.TrainRunID.String() {
		return nil, physicalmigration.ErrInvalidBatch
	}
	if generation, ok := value["assignment_generation"].(json.Number); !ok || generation.String() != fmt.Sprint(record.SourceGeneration) {
		return nil, physicalmigration.ErrInvalidBatch
	}
	rowID, rowHasID := value["id"].(string)
	delete(value, "assignment_generation")
	delete(value, "id")
	if table != "seat_inventory" {
		if !rowHasID || rowID == "" {
			return nil, physicalmigration.ErrInvalidBatch
		}
		value["id"] = rowID
	}
	switch table {
	case "reservation_seats":
		delete(value, "fare_snapshot_id")
		delete(value, "updated_at")
	case "ticket_orders", "tickets":
		delete(value, "train_run_id")
	case "ticket_refund_prepare_receipts":
		// Control v11 keeps this fence column on each logical target.
		value["assignment_generation"] = record.TargetGeneration
	case "outbox_events":
		delete(value, "lease_token")
		delete(value, "updated_at")
		value["shard_id"] = record.TargetShardID
		value["assignment_generation"] = record.TargetGeneration
	}
	return json.Marshal(value)
}

func reverseCanonicalJSON(data []byte, table string) ([]byte, error) {
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	delete(value, "assignment_generation")
	delete(value, "shard_id")
	delete(value, "id")
	delete(value, "updated_at")
	switch table {
	case "reservation_seats":
		delete(value, "fare_snapshot_id")
	case "ticket_orders", "tickets":
		delete(value, "train_run_id")
	case "outbox_events":
		delete(value, "lease_token")
	}
	return json.Marshal(value)
}

func reverseBaseFingerprint(payload physicalpostgres.BasePayload) [32]byte {
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

func reverseSupportedTable(table string) bool {
	switch table {
	case "seat_inventory", "reservations", "reservation_seats", "ticket_orders",
		"tickets", "idempotency_records", "booking_command_receipts",
		"payment_command_receipts", "ticket_issuance_receipts",
		"payment_refund_receipts", "payment_compensation_receipts",
		"ticket_refund_prepare_receipts",
		"ticket_refund_compensation_receipts", "selected_ticket_refund_receipts",
		"outbox_events":
		return true
	default:
		return false
	}
}

func reverseIgnoredTable(table string) bool {
	switch table {
	case "train_run_booking_snapshots", "booking_seat_catalog", "booking_fare_snapshots":
		return true
	default:
		return false
	}
}

var _ physicalmigration.ShardOperations = (*ReverseAdapter)(nil)
