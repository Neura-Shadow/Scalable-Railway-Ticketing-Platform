package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type tableSpec struct {
	name    string
	columns []string
}

var migrationTables = []tableSpec{
	{name: "train_run_booking_snapshots", columns: fields("id train_run_id assignment_generation train_id route_id service_date segment_count route_version booking_policy_version source_version status bookable active source_updated_at created_at updated_at")},
	{name: "booking_seat_catalog", columns: fields("id train_run_id assignment_generation train_id coach_id seat_id coach_order seat_order seat_class active source_version source_updated_at created_at updated_at")},
	{name: "booking_fare_snapshots", columns: fields("id train_run_id assignment_generation segment_count from_stop_index to_stop_index seat_class amount_minor currency source_version active source_updated_at created_at updated_at")},
	{name: "seat_inventory", columns: fields("id train_run_id assignment_generation segment_count seat_id seat_class occupied_segments version created_at updated_at")},
	{name: "reservations", columns: fields("id user_id train_run_id assignment_generation segment_count from_stop_index to_stop_index seat_class status expires_at total_amount_minor currency payment_intent_id payment_amount_minor payment_currency payment_grace_expires_at created_at updated_at")},
	{name: "reservation_seats", columns: fields("id reservation_id train_run_id assignment_generation segment_count seat_id passenger_id fare_snapshot_id segment_mask fare_amount_minor currency created_at updated_at")},
	{name: "ticket_orders", columns: fields("id reservation_id user_id train_run_id assignment_generation status total_amount_minor currency payment_intent_id payment_currency authorized_amount_minor captured_amount_minor refunded_amount_minor created_at updated_at")},
	{name: "tickets", columns: fields("id ticket_order_id reservation_seat_id train_run_id assignment_generation ticket_code status created_at updated_at")},
	{name: "idempotency_records", columns: fields("id train_run_id assignment_generation user_id operation key_hash request_fingerprint status resource_type resource_id expires_at created_at updated_at")},
	{name: "booking_command_receipts", columns: fields("id command_id train_run_id assignment_generation command_type request_fingerprint status result_type result_id result_source_version result_booking_policy_version error_code started_at completed_at created_at updated_at")},
	{name: "payment_command_receipts", columns: fields("id command_id payment_intent_id reservation_id train_run_id assignment_generation operation request_fingerprint amount_minor currency status result_resource_id result_status error_code created_at committed_at updated_at")},
	{name: "ticket_issuance_receipts", columns: fields("id issuance_id payment_intent_id reservation_id payment_operation_id ticket_order_id train_run_id assignment_generation capture_proof_hash amount_minor currency issued_ticket_count created_at")},
	{name: "payment_refund_receipts", columns: fields("id refund_operation_id payment_intent_id reservation_id ticket_order_id train_run_id assignment_generation refund_proof_hash captured_amount_minor refunded_amount_minor currency refunded_at created_at")},
	{name: "payment_compensation_receipts", columns: fields("id compensation_id payment_intent_id reservation_id ticket_order_id refund_receipt_id train_run_id assignment_generation released_seat_count cancelled_ticket_count applied_at created_at")},
	{name: "outbox_events", columns: fields("id train_run_id assignment_generation aggregate_type aggregate_id event_type event_version payload status attempts next_attempt_at locked_at locked_by lease_token created_at updated_at published_at")},
}

type JSONRow struct {
	Table string
	ID    uuid.UUID
	Data  []byte
}

type BasePayload struct{ Rows []JSONRow }

// JSONBaseCopier streams fixed, allowlisted tables in dependency order. Rows
// are represented as JSON only between the two database-local transactions;
// neither DSNs nor row payloads are persisted in the control database.
type JSONBaseCopier struct{}

func (JSONBaseCopier) Read(ctx context.Context, source DB, request physicalmigration.BaseCopyRequest) (physicalmigration.BaseBatch, error) {
	index, after, err := parseBaseCursor(request.Cursor)
	if err != nil || request.Limit <= 0 {
		return physicalmigration.BaseBatch{}, physicalmigration.ErrInvalidInput
	}
	for index < len(migrationTables) {
		spec := migrationTables[index]
		query := fmt.Sprintf(`
SELECT id, to_jsonb(source_row)
FROM public.%s AS source_row
WHERE train_run_id = $1
  AND assignment_generation = $2
  AND ($3::uuid = '00000000-0000-0000-0000-000000000000'::uuid OR id > $3)
ORDER BY id
LIMIT $4`, spec.name)
		rows, err := source.Query(ctx, query, request.Migration.TrainRunID,
			request.Migration.SourceGeneration, after, request.Limit)
		if err != nil {
			return physicalmigration.BaseBatch{}, fmt.Errorf("%w: read base table", ErrShardOperation)
		}
		payload := BasePayload{}
		last := after
		for rows.Next() {
			var row JSONRow
			row.Table = spec.name
			if err := rows.Scan(&row.ID, &row.Data); err != nil {
				rows.Close()
				return physicalmigration.BaseBatch{}, fmt.Errorf("%w: scan base row", ErrShardOperation)
			}
			last = row.ID
			payload.Rows = append(payload.Rows, row)
		}
		iterationErr := rows.Err()
		rows.Close()
		if iterationErr != nil {
			return physicalmigration.BaseBatch{}, fmt.Errorf("%w: iterate base rows", ErrShardOperation)
		}
		if len(payload.Rows) > 0 {
			next := encodeBaseCursor(index, last)
			return physicalmigration.BaseBatch{
				ObjectName: spec.name, Cursor: request.Cursor, NextCursor: next,
				Rows: len(payload.Rows), Fingerprint: baseFingerprint(payload), Payload: payload,
			}, nil
		}
		index++
		after = uuid.Nil
	}
	return physicalmigration.BaseBatch{
		ObjectName: "complete", Cursor: request.Cursor, NextCursor: "complete", Done: true,
		Fingerprint: baseFingerprint(BasePayload{}), Payload: BasePayload{},
	}, nil
}

func (JSONBaseCopier) Apply(ctx context.Context, target DB, record physicalmigration.Record, batch physicalmigration.BaseBatch) error {
	payload, ok := batch.Payload.(BasePayload)
	if !ok || len(payload.Rows) != batch.Rows || baseFingerprint(payload) != batch.Fingerprint {
		return physicalmigration.ErrInvalidBatch
	}
	tx, err := target.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("%w: begin base apply", ErrShardOperation)
	}
	rollback := func(result error) error {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	for _, row := range payload.Rows {
		spec, found := findTable(row.Table)
		if !found || row.ID == uuid.Nil || len(row.Data) == 0 {
			return rollback(physicalmigration.ErrInvalidBatch)
		}
		normalized, err := normalizeRow(row.Data, spec.name, record.TargetGeneration)
		if err != nil {
			return rollback(physicalmigration.ErrInvalidBatch)
		}
		if _, err := tx.Exec(ctx, upsertSQL(spec), normalized); err != nil {
			return rollback(fmt.Errorf("%w: apply base row", ErrShardOperation))
		}
	}
	if batch.ObjectName == "train_run_booking_snapshots" && len(payload.Rows) > 0 {
		if _, err := tx.Exec(ctx, `
INSERT INTO public.train_run_write_fences (
    train_run_id, assignment_generation, state, write_enabled
) VALUES ($1, $2, 'standby', false)
ON CONFLICT (train_run_id) DO UPDATE
SET assignment_generation = EXCLUDED.assignment_generation,
    state = 'standby', write_enabled = false
WHERE NOT train_run_write_fences.write_enabled`, record.TrainRunID, record.TargetGeneration); err != nil {
			return rollback(fmt.Errorf("%w: prepare target fence", ErrShardOperation))
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO public.train_run_target_write_evidence (
    train_run_id, assignment_generation, successful_write_count
) VALUES ($1, $2, 0)
ON CONFLICT (train_run_id, assignment_generation) DO NOTHING`, record.TrainRunID, record.TargetGeneration); err != nil {
			return rollback(fmt.Errorf("%w: prepare target-write evidence", ErrShardOperation))
		}
		if record.ReverseMigration {
			if _, err := tx.Exec(ctx, `
DELETE FROM public.train_run_booking_snapshots
WHERE train_run_id = $1 AND assignment_generation = $2`, record.TrainRunID, record.RetainedTargetGeneration); err != nil {
				return rollback(fmt.Errorf("%w: retire retained predecessor snapshot", ErrShardOperation))
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit base apply", ErrShardOperation)
	}
	return nil
}

// DefaultTargetPreparer fails closed if the target already has a writer or a
// different generation for this train run.
type DefaultTargetPreparer struct{ MaxCleanupRows int }

func (preparer DefaultTargetPreparer) Prepare(ctx context.Context, target DB, record physicalmigration.Record) error {
	if record.ReverseMigration {
		limit := preparer.MaxCleanupRows
		if limit <= 0 {
			limit = 10000
		}
		return prepareRetainedPredecessor(ctx, target, record, limit)
	}
	var conflicts int64
	if err := target.QueryRow(ctx, `
SELECT (
    SELECT count(*) FROM public.train_run_write_fences
    WHERE train_run_id = $1
      AND (assignment_generation <> $2 OR write_enabled)
) + (
    SELECT count(*) FROM public.train_run_booking_snapshots
    WHERE train_run_id = $1 AND assignment_generation <> $2
)`, record.TrainRunID, record.TargetGeneration).Scan(&conflicts); err != nil {
		return fmt.Errorf("%w: inspect target fence", ErrShardOperation)
	}
	if conflicts != 0 {
		return physicalmigration.ErrCheckpointConflict
	}
	return nil
}

func prepareRetainedPredecessor(ctx context.Context, target DB, record physicalmigration.Record, limit int) error {
	if record.RetainedTargetGeneration <= 0 || record.RetainedTargetGeneration >= record.TargetGeneration {
		return physicalmigration.ErrInvalidInput
	}
	tx, err := target.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("%w: begin retained predecessor prep", ErrShardOperation)
	}
	rollback := func(result error) error {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return result
	}
	var snapshots, fences, preparedSnapshots, preparedFences, conflicts int
	if err := tx.QueryRow(ctx, `
SELECT
    (SELECT count(*) FROM public.train_run_booking_snapshots
     WHERE train_run_id = $1 AND assignment_generation = $2),
    (SELECT count(*) FROM public.train_run_write_fences
     WHERE train_run_id = $1 AND assignment_generation = $2
        AND NOT write_enabled AND state IN ('quiescing', 'retained')),
	(SELECT count(*) FROM public.train_run_booking_snapshots
	 WHERE train_run_id = $1 AND assignment_generation = $3),
	(SELECT count(*) FROM public.train_run_write_fences
	 WHERE train_run_id = $1 AND assignment_generation = $3
	   AND NOT write_enabled AND state IN ('standby', 'disabled')),
    (SELECT count(*) FROM public.train_run_booking_snapshots
     WHERE train_run_id = $1 AND assignment_generation NOT IN ($2, $3))
  + (SELECT count(*) FROM public.train_run_write_fences
     WHERE train_run_id = $1 AND assignment_generation NOT IN ($2, $3))`, record.TrainRunID,
		record.RetainedTargetGeneration, record.TargetGeneration).Scan(
		&snapshots, &fences, &preparedSnapshots, &preparedFences, &conflicts); err != nil {
		return rollback(fmt.Errorf("%w: verify retained predecessor", ErrShardOperation))
	}
	if snapshots == 0 && fences == 0 && preparedSnapshots == 1 && preparedFences == 1 && conflicts == 0 {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("%w: commit prepared predecessor inspection", ErrShardOperation)
		}
		return nil
	}
	if snapshots != 1 || fences != 1 || preparedSnapshots != 0 || preparedFences != 0 || conflicts != 0 {
		return rollback(physicalmigration.ErrCheckpointConflict)
	}
	var rows int
	if err := tx.QueryRow(ctx, `
SELECT
    (SELECT count(*) FROM public.outbox_events WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.payment_compensation_receipts WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.payment_refund_receipts WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.ticket_issuance_receipts WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.payment_command_receipts WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.tickets WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.ticket_orders WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.booking_command_receipts WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.idempotency_records WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.reservation_seats WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.reservations WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.seat_inventory WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.booking_fare_snapshots WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.booking_seat_catalog WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.train_run_target_write_evidence WHERE train_run_id = $1 AND assignment_generation = $2)
  + (SELECT count(*) FROM public.train_run_mutation_journal WHERE train_run_id = $1 AND source_generation = $2)
  + (SELECT count(*) FROM public.migration_apply_receipts WHERE train_run_id = $1 AND target_generation = $2)`,
		record.TrainRunID, record.RetainedTargetGeneration).Scan(&rows); err != nil {
		return rollback(fmt.Errorf("%w: count retained predecessor", ErrShardOperation))
	}
	if rows > limit {
		return rollback(physicalmigration.ErrCleanupLimitExceeded)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.migration_capture_state
SET capture_enabled = false, disabled_at = COALESCE(disabled_at, clock_timestamp())
WHERE train_run_id = $1 AND source_generation = $2`, record.TrainRunID, record.RetainedTargetGeneration); err != nil {
		return rollback(fmt.Errorf("%w: disable retained capture", ErrShardOperation))
	}
	deletes := []string{
		"DELETE FROM public.outbox_events WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.payment_compensation_receipts WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.payment_refund_receipts WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.ticket_issuance_receipts WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.payment_command_receipts WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.tickets WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.ticket_orders WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.booking_command_receipts WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.idempotency_records WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.reservation_seats WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.reservations WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.seat_inventory WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.booking_fare_snapshots WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.booking_seat_catalog WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.migration_apply_receipts WHERE train_run_id = $1 AND target_generation = $2",
		"DELETE FROM public.train_run_mutation_journal WHERE train_run_id = $1 AND source_generation = $2",
		"DELETE FROM public.train_run_target_write_evidence WHERE train_run_id = $1 AND assignment_generation = $2",
		"DELETE FROM public.migration_capture_state WHERE train_run_id = $1 AND source_generation = $2",
	}
	for _, statement := range deletes {
		if _, err := tx.Exec(ctx, statement, record.TrainRunID, record.RetainedTargetGeneration); err != nil {
			return rollback(fmt.Errorf("%w: clean retained predecessor", ErrShardOperation))
		}
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.train_run_booking_snapshots
SET active = false, bookable = false
WHERE train_run_id = $1 AND assignment_generation = $2`, record.TrainRunID, record.RetainedTargetGeneration); err != nil {
		return rollback(fmt.Errorf("%w: quiesce retained snapshot", ErrShardOperation))
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.train_run_write_fences
SET state = 'retained', write_enabled = false
WHERE train_run_id = $1 AND assignment_generation = $2`, record.TrainRunID, record.RetainedTargetGeneration); err != nil {
		return rollback(fmt.Errorf("%w: retain predecessor fence", ErrShardOperation))
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit retained predecessor prep", ErrShardOperation)
	}
	return nil
}

// JSONMutationApplier applies the current authoritative source row for an
// insert/update, or a delete tombstone, inside the receipt transaction.
type JSONMutationApplier struct{}

func (JSONMutationApplier) Apply(ctx context.Context, tx pgx.Tx, record physicalmigration.Record, entry physicalmigration.JournalEntry) error {
	spec, found := findTable(entry.TableName)
	if !found {
		return physicalmigration.ErrInvalidBatch
	}
	data, _ := entry.Payload.([]byte)
	if entry.Operation == "DELETE" || len(data) == 0 {
		query := fmt.Sprintf(`DELETE FROM public.%s WHERE id = $1 AND train_run_id = $2 AND assignment_generation = $3`, spec.name)
		_, err := tx.Exec(ctx, query, entry.EntityID, record.TrainRunID, record.TargetGeneration)
		return err
	}
	normalized, err := normalizeRow(data, spec.name, record.TargetGeneration)
	if err != nil {
		return physicalmigration.ErrInvalidBatch
	}
	_, err = tx.Exec(ctx, upsertSQL(spec), normalized)
	return err
}

// BoundedValidator compares row counts and content hashes without returning
// row values. Assignment generation and trigger-maintained updated_at are
// excluded because they intentionally differ on the target.
type BoundedValidator struct{}

func (BoundedValidator) Validate(ctx context.Context, source, target DB, request physicalmigration.ValidationRequest) (physicalmigration.ValidationResult, error) {
	if request.MaxRows <= 0 || request.MaxTables <= 0 || request.MaxTables < len(migrationTables) {
		return physicalmigration.ValidationResult{}, physicalmigration.ErrInvalidInput
	}
	result := physicalmigration.ValidationResult{Passed: true, Version: request.Migration.ValidationVersion + 1}
	remaining := request.MaxRows
	for _, side := range []struct {
		db         DB
		generation int64
	}{
		{db: source, generation: request.Migration.SourceGeneration},
		{db: target, generation: request.Migration.TargetGeneration},
	} {
		violations, err := semanticInvariantViolations(ctx, side.db, request.Migration.TrainRunID, side.generation)
		if err != nil {
			return physicalmigration.ValidationResult{}, err
		}
		if violations != 0 {
			result.Passed = false
		}
	}
	for _, spec := range migrationTables {
		if remaining <= 0 {
			result.Passed = false
			result.Truncated = true
			return result, nil
		}
		sourceCount, sourceHash, sourceIDs, err := boundedTableDigest(ctx, source, spec, request.Migration.TrainRunID, request.Migration.SourceGeneration, remaining+1)
		if err != nil {
			return physicalmigration.ValidationResult{}, err
		}
		targetCount, targetHash, targetIDs, err := boundedTableDigest(ctx, target, spec, request.Migration.TrainRunID, request.Migration.TargetGeneration, remaining+1)
		if err != nil {
			return physicalmigration.ValidationResult{}, err
		}
		result.Tables++
		if sourceCount+targetCount > remaining {
			result.Passed = false
			result.Truncated = true
			return result, nil
		}
		result.RowsExamined += sourceCount + targetCount
		remaining -= sourceCount + targetCount
		if sourceCount != targetCount || sourceHash != targetHash || sourceIDs != targetIDs {
			result.Passed = false
		}
	}
	return result, nil
}

func boundedTableDigest(ctx context.Context, db DB, spec tableSpec, trainRunID uuid.UUID, generation int64, limit int) (int, string, string, error) {
	expression := "to_jsonb(bounded_row) - 'assignment_generation' - 'updated_at'"
	if spec.name == "outbox_events" {
		expression = "(to_jsonb(bounded_row) - 'assignment_generation' - 'updated_at' - 'locked_at' - 'locked_by' - 'lease_token') || jsonb_build_object('status', CASE WHEN status = 'processing' THEN 'pending' ELSE status END)"
	}
	query := fmt.Sprintf(`
SELECT count(*),
       COALESCE(md5(string_agg(
           md5((%s)::text),
           '' ORDER BY id
       )), ''),
       COALESCE(string_agg(id::text, ',' ORDER BY id), '')
FROM (
    SELECT * FROM public.%s
    WHERE train_run_id = $1 AND assignment_generation = $2
    ORDER BY id
    LIMIT $3
) AS bounded_row`, expression, spec.name)
	var count int
	var digest string
	var ids string
	if err := db.QueryRow(ctx, query, trainRunID, generation, limit).Scan(&count, &digest, &ids); err != nil {
		return 0, "", "", fmt.Errorf("%w: validate table", ErrShardOperation)
	}
	return count, digest, ids, nil
}

func semanticInvariantViolations(ctx context.Context, db DB, trainRunID uuid.UUID, generation int64) (int, error) {
	parts := make([]string, 0, len(migrationTables)+2)
	for _, spec := range migrationTables {
		parts = append(parts, fmt.Sprintf(`SELECT 1 WHERE EXISTS (SELECT 1 FROM public.%s WHERE train_run_id = $1 AND assignment_generation <> $2)`, spec.name))
	}
	parts = append(parts,
		`SELECT 1 WHERE (SELECT count(*) FROM public.train_run_booking_snapshots WHERE train_run_id = $1 AND assignment_generation = $2) <> 1`,
		`SELECT 1 WHERE (SELECT count(*) FROM public.train_run_write_fences WHERE train_run_id = $1 AND assignment_generation = $2) <> 1`,
		`SELECT 1 WHERE EXISTS (
		    SELECT 1
		    FROM public.seat_inventory AS inventory
		    LEFT JOIN (
		        SELECT seat.train_run_id, seat.assignment_generation, seat.seat_id,
		               bit_or(seat.segment_mask) AS occupied_segments
		        FROM public.reservation_seats AS seat
		        JOIN public.reservations AS reservation
		          ON reservation.id = seat.reservation_id
		         AND reservation.train_run_id = seat.train_run_id
		         AND reservation.assignment_generation = seat.assignment_generation
		        WHERE reservation.status IN (
		            'held', 'payment_pending', 'payment_review', 'confirmed',
		            'refund_pending'
		        )
		        GROUP BY seat.train_run_id, seat.assignment_generation, seat.seat_id
		    ) AS expected
		      ON expected.train_run_id = inventory.train_run_id
		     AND expected.assignment_generation = inventory.assignment_generation
		     AND expected.seat_id = inventory.seat_id
		    WHERE inventory.train_run_id = $1
		      AND inventory.assignment_generation = $2
		      AND inventory.occupied_segments IS DISTINCT FROM COALESCE(
		          expected.occupied_segments,
		          repeat('0', inventory.segment_count)::bit varying
		      )
		)`,
		`SELECT 1 WHERE EXISTS (
		    SELECT 1 FROM public.reservations AS reservation
		    LEFT JOIN LATERAL (
		        SELECT COALESCE(sum(seat.fare_amount_minor), 0) AS amount,
		               count(DISTINCT seat.currency) AS currencies,
		               min(seat.currency) AS currency
		        FROM public.reservation_seats AS seat
		        WHERE seat.reservation_id = reservation.id
		    ) AS fares ON true
		    WHERE reservation.train_run_id = $1
		      AND reservation.assignment_generation = $2
		      AND (fares.amount <> reservation.total_amount_minor
		           OR fares.currencies <> 1 OR fares.currency <> reservation.currency)
		)`,
		`SELECT 1 WHERE EXISTS (
		    SELECT 1 FROM public.idempotency_records AS idempotency
		    LEFT JOIN public.reservations AS reservation
		      ON reservation.id = idempotency.resource_id
		     AND reservation.train_run_id = idempotency.train_run_id
		     AND reservation.assignment_generation = idempotency.assignment_generation
		    WHERE idempotency.train_run_id = $1
		      AND idempotency.assignment_generation = $2
		      AND idempotency.status = 'completed'
		      AND reservation.id IS NULL
		)`,
		`SELECT 1 WHERE EXISTS (
		    SELECT 1 FROM public.booking_command_receipts AS receipt
		    WHERE receipt.train_run_id = $1
		      AND receipt.assignment_generation = $2
		      AND receipt.status = 'succeeded'
		      AND NOT (
		          (receipt.result_type = 'reservation' AND EXISTS (
		              SELECT 1 FROM public.reservations WHERE id = receipt.result_id
		          ))
		          OR (receipt.result_type = 'train_run' AND receipt.result_id = receipt.train_run_id)
		          OR (receipt.result_type = 'fare' AND EXISTS (
		              SELECT 1 FROM public.booking_fare_snapshots WHERE id = receipt.result_id
		          ))
		          OR (receipt.result_type = 'seat' AND EXISTS (
		              SELECT 1 FROM public.booking_seat_catalog WHERE seat_id = receipt.result_id
		          ))
		      )
		)`,
		`SELECT 1 WHERE EXISTS (
		    SELECT 1 FROM public.outbox_events AS event
		    WHERE event.train_run_id = $1
		      AND event.assignment_generation = $2
		      AND ((event.aggregate_type = 'reservation' AND NOT EXISTS (
		               SELECT 1 FROM public.reservations WHERE id = event.aggregate_id
		           ))
		        OR (event.aggregate_type = 'ticket' AND NOT EXISTS (
		               SELECT 1 FROM public.tickets WHERE id = event.aggregate_id
		           ))
		        OR (event.aggregate_type = 'train_run' AND event.aggregate_id <> event.train_run_id)
		        OR (event.aggregate_type = 'booking_command' AND NOT EXISTS (
		               SELECT 1 FROM public.booking_command_receipts WHERE command_id = event.aggregate_id
		           )))
		)`,
	)
	query := "SELECT count(*) FROM (" + strings.Join(parts, " UNION ALL ") + ") AS violations"
	var count int
	if err := db.QueryRow(ctx, query, trainRunID, generation).Scan(&count); err != nil {
		return 0, fmt.Errorf("%w: validate shard invariants", ErrShardOperation)
	}
	return count, nil
}

func loadMutationPayload(ctx context.Context, source DB, entry physicalmigration.JournalEntry, trainRunID uuid.UUID, generation int64) ([]byte, error) {
	spec, found := findTable(entry.TableName)
	if !found {
		return nil, physicalmigration.ErrInvalidBatch
	}
	query := fmt.Sprintf(`
SELECT to_jsonb(source_row)
FROM public.%s AS source_row
WHERE id = $1 AND train_run_id = $2 AND assignment_generation = $3`, spec.name)
	var data []byte
	if err := source.QueryRow(ctx, query, entry.EntityID, trainRunID, generation).Scan(&data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: load mutation row", ErrShardOperation)
	}
	return data, nil
}

func upsertSQL(spec tableSpec) string {
	columns := strings.Join(spec.columns, ", ")
	updates := make([]string, 0, len(spec.columns)-1)
	for _, column := range spec.columns {
		if column != "id" {
			updates = append(updates, column+" = EXCLUDED."+column)
		}
	}
	return fmt.Sprintf(`
INSERT INTO public.%s (%s)
SELECT %s
FROM jsonb_populate_record(
    NULL::public.%s,
    $1::jsonb
)
ON CONFLICT (id) DO UPDATE SET %s`, spec.name, columns, columns, spec.name, strings.Join(updates, ", "))
}

func parseBaseCursor(cursor string) (int, uuid.UUID, error) {
	if cursor == "" {
		return 0, uuid.Nil, nil
	}
	if cursor == "complete" {
		return len(migrationTables), uuid.Nil, nil
	}
	parts := strings.Split(cursor, ":")
	if len(parts) != 2 {
		return 0, uuid.Nil, physicalmigration.ErrInvalidInput
	}
	index, err := strconv.Atoi(parts[0])
	if err != nil || index < 0 || index >= len(migrationTables) {
		return 0, uuid.Nil, physicalmigration.ErrInvalidInput
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return 0, uuid.Nil, physicalmigration.ErrInvalidInput
	}
	return index, id, nil
}

func encodeBaseCursor(index int, id uuid.UUID) string { return strconv.Itoa(index) + ":" + id.String() }

func baseFingerprint(payload BasePayload) [32]byte {
	hash := sha256.New()
	for _, row := range payload.Rows {
		hash.Write([]byte(row.Table))
		hash.Write(row.ID[:])
		hash.Write(row.Data)
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func findTable(name string) (tableSpec, bool) {
	for _, spec := range migrationTables {
		if spec.name == name {
			return spec, true
		}
	}
	return tableSpec{}, false
}

func fields(value string) []string { return strings.Fields(value) }

func normalizeRow(data []byte, table string, generation int64) ([]byte, error) {
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	value["assignment_generation"] = generation
	if table == "outbox_events" && value["status"] == "processing" {
		value["status"] = "pending"
		value["locked_at"] = nil
		value["locked_by"] = nil
		value["lease_token"] = nil
	}
	return json.Marshal(value)
}
