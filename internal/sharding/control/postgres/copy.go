package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

type copySpec struct {
	phase        string
	table        string
	key          string
	columns      []string
	conflict     string
	scopeJoins   string
	scopeClause  string
	auditCounter string
}

var copySpecs = []copySpec{
	{
		phase: "inventory", table: "seat_inventory", key: "seat_id",
		columns:  []string{"train_run_id", "segment_count", "seat_id", "seat_class", "occupied_segments", "version", "created_at", "updated_at"},
		conflict: "train_run_id, seat_id", scopeClause: "s.train_run_id = $1", auditCounter: "inventory_rows_copied",
	},
	{
		phase: "reservations", table: "reservations", key: "id",
		columns:  []string{"id", "user_id", "train_run_id", "segment_count", "from_stop_index", "to_stop_index", "seat_class", "status", "expires_at", "total_amount_minor", "currency", "created_at", "updated_at"},
		conflict: "id", scopeClause: "s.train_run_id = $1", auditCounter: "reservation_rows_copied",
	},
	{
		phase: "reservation_seats", table: "reservation_seats", key: "id",
		columns:  []string{"id", "reservation_id", "segment_count", "seat_id", "passenger_id", "segment_mask", "fare_amount_minor", "currency", "created_at", "train_run_id"},
		conflict: "id", scopeClause: "s.train_run_id = $1", auditCounter: "reservation_seat_rows_copied",
	},
	{
		phase: "ticket_orders", table: "ticket_orders", key: "id",
		columns:  []string{"id", "reservation_id", "user_id", "status", "total_amount_minor", "currency", "created_at", "updated_at"},
		conflict: "id", scopeJoins: "JOIN %s.reservations AS r ON r.id = s.reservation_id", scopeClause: "r.train_run_id = $1", auditCounter: "ticket_order_rows_copied",
	},
	{
		phase: "tickets", table: "tickets", key: "id",
		columns:  []string{"id", "ticket_order_id", "reservation_seat_id", "ticket_code", "status", "created_at", "updated_at"},
		conflict: "id", scopeJoins: "JOIN %s.ticket_orders AS o ON o.id = s.ticket_order_id JOIN %s.reservations AS r ON r.id = o.reservation_id", scopeClause: "r.train_run_id = $1", auditCounter: "ticket_rows_copied",
	},
	{
		phase: "idempotency_records", table: "idempotency_records", key: "id",
		columns:  []string{"id", "user_id", "operation", "key_hash", "request_fingerprint", "status", "resource_type", "resource_id", "expires_at", "created_at", "updated_at", "train_run_id"},
		conflict: "id", scopeClause: "s.train_run_id = $1", auditCounter: "idempotency_rows_copied",
	},
}

func (tx *Transaction) CopyBatch(ctx context.Context, request control.CopyBatchRequest) (control.CopyBatchResult, error) {
	if tx == nil || tx.tx == nil || request.MigrationID == uuid.Nil || request.TrainRunID == uuid.Nil ||
		request.Limit <= 0 || request.Source.TrainRunID() != request.TrainRunID ||
		request.Target.TrainRunID() != request.TrainRunID || request.Source.ShardID() == request.Target.ShardID() ||
		request.Target.Generation().Int64() <= request.Source.Generation().Int64() {
		return control.CopyBatchResult{}, control.ErrInvalidInput
	}
	sourceSchema, err := schemaForShard(request.Source.ShardID())
	if err != nil {
		return control.CopyBatchResult{}, control.ErrInvalidInput
	}
	targetSchema, err := schemaForShard(request.Target.ShardID())
	if err != nil {
		return control.CopyBatchResult{}, control.ErrInvalidInput
	}
	phaseIndex, cursor, err := parseCopyCheckpoint(request.Checkpoint)
	if err != nil {
		return control.CopyBatchResult{}, control.ErrInvalidInput
	}
	if phaseIndex == len(copySpecs) {
		return control.CopyBatchResult{NextCheckpoint: "complete", Done: true}, nil
	}
	if _, err := tx.tx.Exec(ctx, `SELECT set_config('railway.booking_migration_id', $1, true)`, request.MigrationID.String()); err != nil {
		return control.CopyBatchResult{}, ErrPersistence
	}

	for index := phaseIndex; index < len(copySpecs); index++ {
		spec := copySpecs[index]
		if index != phaseIndex {
			cursor = nil
		}
		rowsCopied, nextCursor, hasMore, err := tx.copyPage(ctx, sourceSchema, targetSchema, spec, request.TrainRunID, cursor, request.Limit)
		if err != nil {
			return control.CopyBatchResult{}, err
		}
		nextIndex := index
		var persistedCursor *uuid.UUID
		if hasMore {
			persistedCursor = nextCursor
		} else {
			nextIndex++
		}
		if err := tx.persistCopyProgress(ctx, request.MigrationID, spec, rowsCopied, nextIndex, persistedCursor); err != nil {
			return control.CopyBatchResult{}, err
		}
		if rowsCopied == 0 && !hasMore {
			continue
		}
		checkpoint := checkpointFor(nextIndex, persistedCursor)
		return control.CopyBatchResult{
			NextCheckpoint: checkpoint,
			RowsCopied:     rowsCopied,
			Done:           nextIndex == len(copySpecs),
		}, nil
	}
	return control.CopyBatchResult{NextCheckpoint: "complete", Done: true}, nil
}

func parseCopyCheckpoint(checkpoint string) (int, *uuid.UUID, error) {
	if checkpoint == "" {
		return 0, nil, nil
	}
	if checkpoint == "complete" {
		return len(copySpecs), nil, nil
	}
	parts := strings.Split(checkpoint, ":")
	if len(parts) > 2 || parts[0] == "" {
		return 0, nil, control.ErrInvalidInput
	}
	index := -1
	for candidate := range copySpecs {
		if copySpecs[candidate].phase == parts[0] {
			index = candidate
			break
		}
	}
	if index < 0 {
		return 0, nil, control.ErrInvalidInput
	}
	if len(parts) == 1 {
		return index, nil, nil
	}
	parsed, err := uuid.Parse(parts[1])
	if err != nil || parsed == uuid.Nil {
		return 0, nil, control.ErrInvalidInput
	}
	return index, &parsed, nil
}

func checkpointFor(index int, cursor *uuid.UUID) string {
	if index >= len(copySpecs) {
		return "complete"
	}
	if cursor == nil {
		return copySpecs[index].phase
	}
	return copySpecs[index].phase + ":" + cursor.String()
}

func (tx *Transaction) copyPage(
	ctx context.Context,
	sourceSchema, targetSchema string,
	spec copySpec,
	trainRunID uuid.UUID,
	cursor *uuid.UUID,
	limit int,
) (int, *uuid.UUID, bool, error) {
	columns := strings.Join(spec.columns, ", ")
	selectedColumns := make([]string, 0, len(spec.columns))
	updates := make([]string, 0, len(spec.columns))
	for _, column := range spec.columns {
		selectedColumns = append(selectedColumns, "s."+column)
		if column != spec.key && !strings.Contains(", "+spec.conflict+", ", ", "+column+", ") {
			updates = append(updates, column+" = EXCLUDED."+column)
		}
	}
	joins := ""
	if spec.scopeJoins != "" {
		count := strings.Count(spec.scopeJoins, "%s")
		values := make([]any, count)
		for index := range values {
			values[index] = sourceSchema
		}
		joins = fmt.Sprintf(spec.scopeJoins, values...)
	}
	query := fmt.Sprintf(`
WITH selected AS MATERIALIZED (
    SELECT %s
    FROM %s.%s AS s
    %s
    WHERE %s
      AND ($2::uuid IS NULL OR s.%s > $2)
    ORDER BY s.%s
    LIMIT $3
), upserted AS (
    INSERT INTO %s.%s (%s)
    SELECT %s FROM selected
    ON CONFLICT (%s) DO UPDATE SET %s
    RETURNING %s
)
SELECT count(*)::bigint,
       (SELECT s.%s FROM selected AS s ORDER BY s.%s DESC LIMIT 1),
       CASE WHEN count(*) = 0 THEN false ELSE EXISTS (
           SELECT 1
           FROM %s.%s AS s
           %s
           WHERE %s
             AND s.%s > (SELECT tail.%s FROM selected AS tail ORDER BY tail.%s DESC LIMIT 1)
       ) END
FROM selected`,
		strings.Join(selectedColumns, ", "), sourceSchema, spec.table, joins, spec.scopeClause,
		spec.key, spec.key, targetSchema, spec.table, columns, columns, spec.conflict,
		strings.Join(updates, ", "), spec.key, spec.key, spec.key,
		sourceSchema, spec.table, joins, spec.scopeClause, spec.key, spec.key, spec.key,
	)
	var rows int64
	var rawCursor pgtype.UUID
	var hasMore bool
	err := tx.tx.QueryRow(ctx, query, trainRunID, cursor, limit).Scan(&rows, &rawCursor, &hasMore)
	if err != nil {
		return 0, nil, false, ErrPersistence
	}
	if rows < 0 || rows > int64(limit) || (rows > 0 && !rawCursor.Valid) {
		return 0, nil, false, ErrPersistence
	}
	if rows == 0 {
		return 0, nil, false, nil
	}
	parsed := uuid.UUID(rawCursor.Bytes)
	return int(rows), &parsed, hasMore, nil
}

func (tx *Transaction) persistCopyProgress(
	ctx context.Context,
	migrationID uuid.UUID,
	spec copySpec,
	rows int,
	nextIndex int,
	cursor *uuid.UUID,
) error {
	if rows < 0 || nextIndex < 0 || nextIndex > len(copySpecs) {
		return control.ErrInvalidCopyResult
	}
	phase := "complete"
	if nextIndex < len(copySpecs) {
		phase = copySpecs[nextIndex].phase
	}
	query := fmt.Sprintf(`
UPDATE public.train_run_shard_migrations
SET copy_phase = $2,
    copy_cursor = $3,
    %s = %s + $4
WHERE id = $1`, spec.auditCounter, spec.auditCounter)
	commandTag, err := tx.tx.Exec(ctx, query, migrationID, phase, cursor, rows)
	if err != nil {
		return ErrPersistence
	}
	if commandTag.RowsAffected() != 1 {
		return control.ErrMigrationNotFound
	}
	return nil
}
