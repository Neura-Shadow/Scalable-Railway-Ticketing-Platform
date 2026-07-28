package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var errQuery = errors.New("shard reconciliation query failed")

type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type Inspector struct {
	db DB
}

func New(db DB) (*Inspector, error) {
	if db == nil {
		return nil, ErrInvalidInput
	}
	return &Inspector{db: db}, nil
}

func (inspector *Inspector) Assignments(ctx context.Context, limits Limits) (Report, error) {
	return inspector.inspect(ctx, ScopeAssignments, func(service *service) (Report, error) {
		return service.Assignments(ctx, limits)
	})
}

func (inspector *Inspector) Locators(
	ctx context.Context,
	filter LocatorFilter,
	limits Limits,
) (Report, error) {
	return inspector.inspect(ctx, ScopeLocators, func(service *service) (Report, error) {
		return service.Locators(ctx, filter, limits)
	})
}

func (inspector *Inspector) Migration(
	ctx context.Context,
	migrationID uuid.UUID,
	limits Limits,
) (Report, error) {
	return inspector.inspect(ctx, ScopeMigration, func(service *service) (Report, error) {
		return service.Migration(ctx, migrationID, limits)
	})
}

func (inspector *Inspector) inspect(
	ctx context.Context,
	scope string,
	callback func(*service) (Report, error),
) (Report, error) {
	if inspector == nil || inspector.db == nil || ctx == nil || callback == nil ||
		(scope != ScopeAssignments && scope != ScopeLocators && scope != ScopeMigration) {
		return Report{}, ErrInvalidInput
	}
	tx, err := inspector.db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		report := baseReport(scope, time.Now().UTC())
		report.Completeness = CompletenessUnavailable
		category := "transaction_begin_" + failureCategory(databaseError(ctx, err))
		report.Failures = append(report.Failures, category)
		for index := range report.Shards {
			markShardFailure(&report.Shards[index], category)
		}
		return report, errors.Join(ErrUnavailable, databaseError(ctx, err))
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	report, inspectErr := callback(newService(&postgresSource{tx: tx}))
	if commitErr := tx.Commit(ctx); commitErr != nil {
		report.Completeness = CompletenessUnavailable
		report.Failures = append(report.Failures, "transaction_commit_"+failureCategory(databaseError(ctx, commitErr)))
		if inspectErr != nil {
			return report, errors.Join(inspectErr, ErrUnavailable, databaseError(ctx, commitErr))
		}
		return report, errors.Join(ErrUnavailable, databaseError(ctx, commitErr))
	}
	return report, inspectErr
}

type postgresSource struct {
	tx pgx.Tx
}

// withQuery isolates each fixed-shard read behind a savepoint. PostgreSQL
// aborts a transaction after a statement error; rolling back only the nested
// transaction lets reconciliation retain healthy results from other shards.
func (source *postgresSource) withQuery(ctx context.Context, callback func(pgx.Tx) error) error {
	if source == nil || source.tx == nil || callback == nil {
		return ErrInvalidInput
	}
	nested, err := source.tx.Begin(ctx)
	if err != nil {
		return databaseError(ctx, err)
	}
	if err := callback(nested); err != nil {
		_ = nested.Rollback(context.Background())
		return databaseError(ctx, err)
	}
	if err := nested.Commit(ctx); err != nil {
		return databaseError(ctx, err)
	}
	return nil
}

func (source *postgresSource) Catalog(ctx context.Context) ([]catalogObservation, error) {
	var result []catalogObservation
	err := source.withQuery(ctx, func(query pgx.Tx) error {
		rows, err := query.Query(ctx, `
SELECT shard_id, storage_kind, enabled, write_enabled, state
FROM public.booking_shards
ORDER BY shard_id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row catalogObservation
			if err := rows.Scan(&row.ShardID, &row.StorageKind, &row.Enabled, &row.WriteEnabled, &row.State); err != nil {
				return err
			}
			result = append(result, row)
		}
		return rows.Err()
	})
	return result, err
}

func (source *postgresSource) AssignmentPage(
	ctx context.Context,
	after uuid.UUID,
	limit int,
) ([]assignmentObservation, bool, error) {
	if limit < 1 || limit > MaxPageSize {
		return nil, false, ErrInvalidInput
	}
	var result []assignmentObservation
	err := source.withQuery(ctx, func(query pgx.Tx) error {
		rows, err := query.Query(ctx, `
SELECT train_run.id,
       assignment.train_run_id,
       assignment.shard_id,
       assignment.assignment_generation,
       assignment.assignment_state,
       assignment.active_migration_id,
       shard.shard_id,
       shard.enabled,
       shard.write_enabled
FROM public.train_runs AS train_run
LEFT JOIN public.train_run_shard_assignments AS assignment
  ON assignment.train_run_id = train_run.id
LEFT JOIN public.booking_shards AS shard
  ON shard.shard_id = assignment.shard_id
WHERE train_run.id > $1
ORDER BY train_run.id
LIMIT ($2 + 1)`, after, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				row                                      assignmentObservation
				assignmentID, activeMigration            pgtype.UUID
				shardID, assignmentState, catalogShardID pgtype.Text
				generation                               pgtype.Int8
				catalogEnabled, catalogWriteEnabled      pgtype.Bool
			)
			if err := rows.Scan(
				&row.TrainRunID, &assignmentID, &shardID, &generation, &assignmentState,
				&activeMigration, &catalogShardID, &catalogEnabled, &catalogWriteEnabled,
			); err != nil {
				return err
			}
			row.AssignmentPresent = assignmentID.Valid
			if shardID.Valid {
				row.ShardID = shardID.String
			}
			if generation.Valid {
				row.Generation = generation.Int64
			}
			if assignmentState.Valid {
				row.State = assignmentState.String
			}
			if activeMigration.Valid {
				value := uuid.UUID(activeMigration.Bytes)
				row.ActiveMigrationID = &value
			}
			row.CatalogPresent = catalogShardID.Valid
			row.CatalogEnabled = catalogEnabled.Valid && catalogEnabled.Bool
			row.CatalogWriteEnabled = catalogWriteEnabled.Valid && catalogWriteEnabled.Bool
			result = append(result, row)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, false, err
	}
	more := len(result) > limit
	if more {
		result = result[:limit]
	}
	return result, more, nil
}

func (source *postgresSource) Fences(
	ctx context.Context,
	storage fixedStorage,
	trainRunIDs []uuid.UUID,
) ([]fenceObservation, error) {
	queryText, valid := fenceQuery(storage)
	if !valid || len(trainRunIDs) == 0 || len(trainRunIDs) > MaxPageSize {
		return nil, ErrInvalidInput
	}
	var result []fenceObservation
	err := source.withQuery(ctx, func(query pgx.Tx) error {
		rows, err := query.Query(ctx, queryText, trainRunIDs)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row fenceObservation
			if err := rows.Scan(&row.TrainRunID, &row.Generation, &row.Enabled); err != nil {
				return err
			}
			result = append(result, row)
		}
		return rows.Err()
	})
	return result, err
}

func fenceQuery(storage fixedStorage) (string, bool) {
	switch storage {
	case storageLegacy:
		return `SELECT train_run_id, assignment_generation, write_enabled
FROM public.train_run_write_fences WHERE train_run_id = ANY($1) ORDER BY train_run_id`, true
	case storageZero:
		return `SELECT train_run_id, assignment_generation, write_enabled
FROM booking_shard_0.train_run_write_fences WHERE train_run_id = ANY($1) ORDER BY train_run_id`, true
	case storageOne:
		return `SELECT train_run_id, assignment_generation, write_enabled
FROM booking_shard_1.train_run_write_fences WHERE train_run_id = ANY($1) ORDER BY train_run_id`, true
	default:
		return "", false
	}
}

func (source *postgresSource) LocatorPage(
	ctx context.Context,
	kind locatorKind,
	after uuid.UUID,
	filter LocatorFilter,
	limit int,
) ([]locatorObservation, bool, error) {
	queryText, valid := locatorPageQuery(kind)
	if !valid || !filter.valid() || limit < 1 || limit > MaxPageSize {
		return nil, false, ErrInvalidInput
	}
	var trainRun any
	if filter.TrainRunID != nil {
		trainRun = *filter.TrainRunID
	}
	var result []locatorObservation
	err := source.withQuery(ctx, func(query pgx.Tx) error {
		rows, err := query.Query(ctx, queryText, after, trainRun, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			row, err := scanLocator(rows, kind)
			if err != nil {
				return err
			}
			result = append(result, row)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, false, err
	}
	more := len(result) > limit
	if more {
		result = result[:limit]
	}
	return result, more, nil
}

func locatorPageQuery(kind locatorKind) (string, bool) {
	switch kind {
	case locatorReservation:
		return `
SELECT locator.reservation_id, locator.train_run_id, locator.shard_id,
       locator.assignment_generation, locator.owner_user_id,
       assignment.train_run_id, assignment.shard_id, assignment.assignment_generation
FROM public.reservation_shard_locators AS locator
LEFT JOIN public.train_run_shard_assignments AS assignment
  ON assignment.train_run_id = locator.train_run_id
WHERE locator.reservation_id > $1
  AND ($2::uuid IS NULL OR locator.train_run_id = $2)
ORDER BY locator.reservation_id
LIMIT ($3 + 1)`, true
	case locatorTicketOrder:
		return `
SELECT locator.ticket_order_id, locator.train_run_id, locator.shard_id,
       locator.assignment_generation, locator.owner_user_id,
       locator.reservation_id, locator.status, locator.total_amount_minor,
       locator.currency, locator.created_at,
       assignment.train_run_id, assignment.shard_id, assignment.assignment_generation
FROM public.ticket_order_shard_locators AS locator
LEFT JOIN public.train_run_shard_assignments AS assignment
  ON assignment.train_run_id = locator.train_run_id
WHERE locator.ticket_order_id > $1
  AND ($2::uuid IS NULL OR locator.train_run_id = $2)
ORDER BY locator.ticket_order_id
LIMIT ($3 + 1)`, true
	case locatorTicket:
		return `
SELECT locator.ticket_id, locator.train_run_id, locator.shard_id,
       locator.assignment_generation, locator.owner_user_id,
       locator.reservation_id, locator.ticket_order_id,
       assignment.train_run_id, assignment.shard_id, assignment.assignment_generation
FROM public.ticket_shard_locators AS locator
LEFT JOIN public.train_run_shard_assignments AS assignment
  ON assignment.train_run_id = locator.train_run_id
WHERE locator.ticket_id > $1
  AND ($2::uuid IS NULL OR locator.train_run_id = $2)
ORDER BY locator.ticket_id
LIMIT ($3 + 1)`, true
	default:
		return "", false
	}
}

type rowScanner interface {
	Scan(...any) error
}

func scanLocator(scanner rowScanner, kind locatorKind) (locatorObservation, error) {
	var row locatorObservation
	var assignmentID pgtype.UUID
	var assignmentShard pgtype.Text
	var assignmentGeneration pgtype.Int8
	switch kind {
	case locatorReservation:
		if err := scanner.Scan(
			&row.ID, &row.TrainRunID, &row.ShardID, &row.Generation, &row.OwnerID,
			&assignmentID, &assignmentShard, &assignmentGeneration,
		); err != nil {
			return locatorObservation{}, err
		}
	case locatorTicketOrder:
		if err := scanner.Scan(
			&row.ID, &row.TrainRunID, &row.ShardID, &row.Generation, &row.OwnerID,
			&row.ReservationID, &row.Status, &row.AmountMinor, &row.Currency, &row.CreatedAt,
			&assignmentID, &assignmentShard, &assignmentGeneration,
		); err != nil {
			return locatorObservation{}, err
		}
	case locatorTicket:
		if err := scanner.Scan(
			&row.ID, &row.TrainRunID, &row.ShardID, &row.Generation, &row.OwnerID,
			&row.ReservationID, &row.TicketOrderID,
			&assignmentID, &assignmentShard, &assignmentGeneration,
		); err != nil {
			return locatorObservation{}, err
		}
	default:
		return locatorObservation{}, ErrInvalidInput
	}
	row.AssignmentPresent = assignmentID.Valid
	if assignmentShard.Valid {
		row.AssignmentShardID = assignmentShard.String
	}
	if assignmentGeneration.Valid {
		row.AssignmentGeneration = assignmentGeneration.Int64
	}
	return row, nil
}

func (source *postgresSource) Resources(
	ctx context.Context,
	storage fixedStorage,
	kind locatorKind,
	ids []uuid.UUID,
) ([]resourceObservation, error) {
	queryText, valid := resourceQuery(storage, kind)
	if !valid || len(ids) == 0 || len(ids) > MaxPageSize {
		return nil, ErrInvalidInput
	}
	var result []resourceObservation
	err := source.withQuery(ctx, func(query pgx.Tx) error {
		rows, err := query.Query(ctx, queryText, ids)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row resourceObservation
			switch kind {
			case locatorReservation:
				err = rows.Scan(&row.ID, &row.TrainRunID, &row.OwnerID)
			case locatorTicketOrder:
				err = rows.Scan(
					&row.ID, &row.TrainRunID, &row.OwnerID, &row.ReservationID,
					&row.Status, &row.AmountMinor, &row.Currency, &row.CreatedAt,
				)
			case locatorTicket:
				err = rows.Scan(
					&row.ID, &row.TrainRunID, &row.OwnerID, &row.ReservationID, &row.TicketOrderID,
				)
			}
			if err != nil {
				return err
			}
			result = append(result, row)
		}
		return rows.Err()
	})
	return result, err
}

func resourceQuery(storage fixedStorage, kind locatorKind) (string, bool) {
	prefix, valid := storagePrefix(storage)
	if !valid {
		return "", false
	}
	// prefix is selected exclusively from the private fixedStorage allowlist.
	switch kind {
	case locatorReservation:
		return fmt.Sprintf(`SELECT id, train_run_id, user_id FROM %s.reservations
WHERE id = ANY($1) ORDER BY id`, prefix), true
	case locatorTicketOrder:
		return fmt.Sprintf(`
SELECT ticket_order.id, reservation.train_run_id, ticket_order.user_id,
       ticket_order.reservation_id, ticket_order.status, ticket_order.total_amount_minor,
       ticket_order.currency, ticket_order.created_at
FROM %[1]s.ticket_orders AS ticket_order
JOIN %[1]s.reservations AS reservation ON reservation.id = ticket_order.reservation_id
WHERE ticket_order.id = ANY($1)
ORDER BY ticket_order.id`, prefix), true
	case locatorTicket:
		return fmt.Sprintf(`
SELECT ticket.id, reservation.train_run_id, reservation.user_id,
       ticket_order.reservation_id, ticket.ticket_order_id
FROM %[1]s.tickets AS ticket
JOIN %[1]s.ticket_orders AS ticket_order ON ticket_order.id = ticket.ticket_order_id
JOIN %[1]s.reservations AS reservation ON reservation.id = ticket_order.reservation_id
WHERE ticket.id = ANY($1)
ORDER BY ticket.id`, prefix), true
	default:
		return "", false
	}
}

func storagePrefix(storage fixedStorage) (string, bool) {
	switch storage {
	case storageLegacy:
		return "public", true
	case storageZero:
		return "booking_shard_0", true
	case storageOne:
		return "booking_shard_1", true
	default:
		return "", false
	}
}

func (source *postgresSource) LocatorCoverage(
	ctx context.Context,
	storage fixedStorage,
	filter LocatorFilter,
	limit int64,
) (locatorCoverage, error) {
	prefix, valid := storagePrefix(storage)
	if !valid || !filter.valid() || limit < 1 || limit > MaxRows {
		return locatorCoverage{}, ErrInvalidInput
	}
	var trainRun any
	if filter.TrainRunID != nil {
		trainRun = *filter.TrainRunID
	}
	var result locatorCoverage
	queryText := fmt.Sprintf(locatorCoverageQuery, prefix)
	err := source.withQuery(ctx, func(query pgx.Tx) error {
		return query.QueryRow(ctx, queryText, trainRun, limit).Scan(
			&result.Counts.Reservations,
			&result.Counts.TicketOrders,
			&result.Counts.Tickets,
			&result.MissingReservationLocators,
			&result.InvalidReservationLocators,
			&result.MissingTicketOrderLocators,
			&result.InvalidTicketOrderLocators,
			&result.MissingTicketLocators,
			&result.InvalidTicketLocators,
			&result.Truncated,
		)
	})
	return result, err
}

const locatorCoverageQuery = `
WITH reservation_page AS MATERIALIZED (
    SELECT reservation.id, reservation.train_run_id, reservation.user_id
    FROM %[1]s.reservations AS reservation
    WHERE ($1::uuid IS NULL OR reservation.train_run_id = $1)
    ORDER BY reservation.id
    LIMIT ($2 + 1)
), reservation_scope AS (
    SELECT * FROM reservation_page ORDER BY id LIMIT $2
), order_page AS MATERIALIZED (
    SELECT ticket_order.id, ticket_order.reservation_id, reservation.train_run_id,
           ticket_order.user_id, ticket_order.status, ticket_order.total_amount_minor,
           ticket_order.currency, ticket_order.created_at
    FROM %[1]s.ticket_orders AS ticket_order
    JOIN %[1]s.reservations AS reservation ON reservation.id = ticket_order.reservation_id
    WHERE ($1::uuid IS NULL OR reservation.train_run_id = $1)
    ORDER BY ticket_order.id
    LIMIT ($2 + 1)
), order_scope AS (
    SELECT * FROM order_page ORDER BY id LIMIT $2
), ticket_page AS MATERIALIZED (
    SELECT ticket.id, ticket.ticket_order_id, ticket_order.reservation_id,
           reservation.train_run_id, reservation.user_id
    FROM %[1]s.tickets AS ticket
    JOIN %[1]s.ticket_orders AS ticket_order ON ticket_order.id = ticket.ticket_order_id
    JOIN %[1]s.reservations AS reservation ON reservation.id = ticket_order.reservation_id
    WHERE ($1::uuid IS NULL OR reservation.train_run_id = $1)
    ORDER BY ticket.id
    LIMIT ($2 + 1)
), ticket_scope AS (
    SELECT * FROM ticket_page ORDER BY id LIMIT $2
)
SELECT
    (SELECT count(*) FROM reservation_scope),
    (SELECT count(*) FROM order_scope),
    (SELECT count(*) FROM ticket_scope),
    (SELECT count(*) FROM reservation_scope AS resource
     LEFT JOIN public.reservation_shard_locators AS locator ON locator.reservation_id = resource.id
     WHERE locator.reservation_id IS NULL),
    (SELECT count(*) FROM reservation_scope AS resource
     JOIN public.reservation_shard_locators AS locator ON locator.reservation_id = resource.id
     LEFT JOIN public.train_run_shard_assignments AS assignment ON assignment.train_run_id = locator.train_run_id
     WHERE locator.train_run_id <> resource.train_run_id
        OR locator.owner_user_id <> resource.user_id
        OR assignment.train_run_id IS NULL
        OR locator.shard_id <> assignment.shard_id
        OR locator.assignment_generation <> assignment.assignment_generation),
    (SELECT count(*) FROM order_scope AS resource
     LEFT JOIN public.ticket_order_shard_locators AS locator ON locator.ticket_order_id = resource.id
     WHERE locator.ticket_order_id IS NULL),
    (SELECT count(*) FROM order_scope AS resource
     JOIN public.ticket_order_shard_locators AS locator ON locator.ticket_order_id = resource.id
     LEFT JOIN public.train_run_shard_assignments AS assignment ON assignment.train_run_id = locator.train_run_id
     WHERE locator.train_run_id <> resource.train_run_id
        OR locator.reservation_id <> resource.reservation_id
        OR locator.owner_user_id <> resource.user_id
        OR locator.status <> resource.status
        OR locator.total_amount_minor <> resource.total_amount_minor
        OR locator.currency <> resource.currency
        OR locator.created_at <> resource.created_at
        OR assignment.train_run_id IS NULL
        OR locator.shard_id <> assignment.shard_id
        OR locator.assignment_generation <> assignment.assignment_generation),
    (SELECT count(*) FROM ticket_scope AS resource
     LEFT JOIN public.ticket_shard_locators AS locator ON locator.ticket_id = resource.id
     WHERE locator.ticket_id IS NULL),
    (SELECT count(*) FROM ticket_scope AS resource
     JOIN public.ticket_shard_locators AS locator ON locator.ticket_id = resource.id
     LEFT JOIN public.train_run_shard_assignments AS assignment ON assignment.train_run_id = locator.train_run_id
     WHERE locator.train_run_id <> resource.train_run_id
        OR locator.reservation_id <> resource.reservation_id
        OR locator.ticket_order_id <> resource.ticket_order_id
        OR locator.owner_user_id <> resource.user_id
        OR assignment.train_run_id IS NULL
        OR locator.shard_id <> assignment.shard_id
        OR locator.assignment_generation <> assignment.assignment_generation),
    ((SELECT count(*) FROM reservation_page) > $2
      OR (SELECT count(*) FROM order_page) > $2
      OR (SELECT count(*) FROM ticket_page) > $2)`

func (source *postgresSource) Migration(
	ctx context.Context,
	migrationID uuid.UUID,
) (migrationObservation, bool, error) {
	if migrationID == uuid.Nil {
		return migrationObservation{}, false, ErrInvalidInput
	}
	var result migrationObservation
	found := true
	err := source.withQuery(ctx, func(query pgx.Tx) error {
		var (
			activeMigration                  pgtype.UUID
			assignmentID                     pgtype.UUID
			assignmentShard, assignmentState pgtype.Text
			assignmentGeneration             pgtype.Int8
			rollbackGeneration               pgtype.Int8
			sourceEnabled, targetEnabled     pgtype.Bool
		)
		err := query.QueryRow(ctx, `
SELECT migration.id, migration.train_run_id,
       migration.source_shard_id, migration.target_shard_id,
       migration.source_generation, migration.target_generation,
       migration.rollback_generation,
       migration.state, migration.copy_phase, migration.copy_complete,
       migration.copied_rows, migration.inventory_rows_copied,
       migration.reservation_rows_copied, migration.reservation_seat_rows_copied,
       migration.ticket_order_rows_copied, migration.ticket_rows_copied,
       migration.idempotency_rows_copied, migration.validation_status,
       migration.last_validation,
       assignment.train_run_id, assignment.shard_id,
       assignment.assignment_generation, assignment.assignment_state,
       assignment.active_migration_id,
       source_catalog.enabled, target_catalog.enabled
FROM public.train_run_shard_migrations AS migration
LEFT JOIN public.train_run_shard_assignments AS assignment
  ON assignment.train_run_id = migration.train_run_id
LEFT JOIN public.booking_shards AS source_catalog
  ON source_catalog.shard_id = migration.source_shard_id
LEFT JOIN public.booking_shards AS target_catalog
  ON target_catalog.shard_id = migration.target_shard_id
WHERE migration.id = $1`, migrationID).Scan(
			&result.ID, &result.TrainRunID, &result.SourceShardID, &result.TargetShardID,
			&result.SourceGeneration, &result.TargetGeneration, &rollbackGeneration,
			&result.State, &result.CopyPhase,
			&result.CopyComplete, &result.CopiedRows, &result.InventoryRowsCopied,
			&result.ReservationRowsCopied, &result.ReservationSeatRowsCopied,
			&result.TicketOrderRowsCopied, &result.TicketRowsCopied,
			&result.IdempotencyRowsCopied, &result.ValidationStatus, &result.LastValidation,
			&assignmentID, &assignmentShard, &assignmentGeneration,
			&assignmentState, &activeMigration,
			&sourceEnabled, &targetEnabled,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			found = false
			return nil
		}
		if err != nil {
			return err
		}
		result.AssignmentPresent = assignmentID.Valid
		if assignmentShard.Valid {
			result.AssignmentShardID = assignmentShard.String
		}
		if assignmentGeneration.Valid {
			result.AssignmentGeneration = assignmentGeneration.Int64
		}
		if rollbackGeneration.Valid {
			value := rollbackGeneration.Int64
			result.RollbackGeneration = &value
		}
		if assignmentState.Valid {
			result.AssignmentState = assignmentState.String
		}
		if activeMigration.Valid {
			value := uuid.UUID(activeMigration.Bytes)
			result.ActiveMigrationID = &value
		}
		result.SourceCatalogEnabled = sourceEnabled.Valid && sourceEnabled.Bool
		result.TargetCatalogEnabled = targetEnabled.Valid && targetEnabled.Bool
		return nil
	})
	return result, found, err
}

func (source *postgresSource) StorageSnapshot(
	ctx context.Context,
	storage fixedStorage,
	trainRunID uuid.UUID,
	limit int64,
) (storageSnapshot, error) {
	prefix, valid := storagePrefix(storage)
	if !valid || trainRunID == uuid.Nil || limit < 1 || limit > MaxRows {
		return storageSnapshot{}, ErrInvalidInput
	}
	var result storageSnapshot
	queryText := fmt.Sprintf(storageSnapshotQuery, prefix)
	err := source.withQuery(ctx, func(query pgx.Tx) error {
		return query.QueryRow(ctx, queryText, trainRunID, limit).Scan(
			&result.Counts.Inventory,
			&result.Counts.Reservations,
			&result.Counts.ReservationSeats,
			&result.Counts.TicketOrders,
			&result.Counts.Tickets,
			&result.Counts.IdempotencyRecords,
			&result.SeatMaskViolations,
			&result.OrphanActiveSeats,
			&result.QuotaStructuralViolations,
			&result.QuotaActivityViolations,
			&result.TicketViolations,
			&result.IdempotencyViolations,
			&result.Truncated,
		)
	})
	return result, err
}

const storageSnapshotQuery = `
WITH inventory_page AS MATERIALIZED (
    SELECT * FROM %[1]s.seat_inventory
    WHERE train_run_id = $1 ORDER BY seat_id LIMIT ($2 + 1)
), inventory AS (SELECT * FROM inventory_page ORDER BY seat_id LIMIT $2),
reservation_page AS MATERIALIZED (
    SELECT * FROM %[1]s.reservations
    WHERE train_run_id = $1 ORDER BY id LIMIT ($2 + 1)
), reservation AS (SELECT * FROM reservation_page ORDER BY id LIMIT $2),
reservation_seat_page AS MATERIALIZED (
    SELECT * FROM %[1]s.reservation_seats
    WHERE train_run_id = $1 ORDER BY id LIMIT ($2 + 1)
), reservation_seat AS (SELECT * FROM reservation_seat_page ORDER BY id LIMIT $2),
order_page AS MATERIALIZED (
    SELECT ticket_order.*
    FROM %[1]s.ticket_orders AS ticket_order
    JOIN %[1]s.reservations AS scoped_reservation
      ON scoped_reservation.id = ticket_order.reservation_id
    WHERE scoped_reservation.train_run_id = $1
    ORDER BY ticket_order.id LIMIT ($2 + 1)
), ticket_order AS (SELECT * FROM order_page ORDER BY id LIMIT $2),
ticket_page AS MATERIALIZED (
    SELECT ticket.*
    FROM %[1]s.tickets AS ticket
    JOIN %[1]s.ticket_orders AS scoped_order ON scoped_order.id = ticket.ticket_order_id
    JOIN %[1]s.reservations AS scoped_reservation ON scoped_reservation.id = scoped_order.reservation_id
    WHERE scoped_reservation.train_run_id = $1
    ORDER BY ticket.id LIMIT ($2 + 1)
), ticket AS (SELECT * FROM ticket_page ORDER BY id LIMIT $2),
idempotency_page AS MATERIALIZED (
    SELECT * FROM %[1]s.idempotency_records
    WHERE train_run_id = $1 ORDER BY id LIMIT ($2 + 1)
), idempotency AS (SELECT * FROM idempotency_page ORDER BY id LIMIT $2),
inventory_violations AS (
    SELECT inventory.seat_id
    FROM inventory
    LEFT JOIN LATERAL (
        SELECT count(*)::bigint AS total_masks,
               count(*) FILTER (
                   WHERE bit_length(reservation_seat.segment_mask) = bit_length(inventory.occupied_segments)
               )::bigint AS matching_masks,
               bit_or(reservation_seat.segment_mask) FILTER (
                   WHERE bit_length(reservation_seat.segment_mask) = bit_length(inventory.occupied_segments)
               ) AS expected_mask,
               sum(bit_count(reservation_seat.segment_mask)) FILTER (
                   WHERE bit_length(reservation_seat.segment_mask) = bit_length(inventory.occupied_segments)
               ) AS individual_bit_count
        FROM reservation_seat
        JOIN reservation ON reservation.id = reservation_seat.reservation_id
        WHERE reservation_seat.seat_id = inventory.seat_id
          AND reservation.status IN ('held', 'confirmed')
    ) AS active ON true
    WHERE NOT CASE
        WHEN active.total_masks = 0 THEN bit_count(inventory.occupied_segments) = 0
        WHEN active.total_masks = active.matching_masks
        THEN inventory.occupied_segments = active.expected_mask
         AND active.individual_bit_count = bit_count(active.expected_mask)
        ELSE false
    END
), orphan_active_seats AS (
    SELECT reservation_seat.id
    FROM reservation_seat
    JOIN reservation ON reservation.id = reservation_seat.reservation_id
    LEFT JOIN inventory
      ON inventory.train_run_id = reservation.train_run_id
     AND inventory.seat_id = reservation_seat.seat_id
    WHERE reservation.status IN ('held', 'confirmed') AND inventory.seat_id IS NULL
), quota_structural_violations AS (
    SELECT reservation.id
    FROM reservation
    LEFT JOIN public.reservation_quota_claims AS claim
      ON claim.reservation_id = reservation.id
     AND claim.user_id = reservation.user_id
     AND claim.train_run_id = reservation.train_run_id
    WHERE claim.reservation_id IS NULL
       OR claim.passenger_count <> (
           SELECT count(*)::integer FROM reservation_seat
           WHERE reservation_seat.reservation_id = reservation.id
       )
), quota_activity_violations AS (
    SELECT reservation.id
    FROM reservation
    JOIN public.reservation_quota_claims AS claim
      ON claim.reservation_id = reservation.id
     AND claim.user_id = reservation.user_id
     AND claim.train_run_id = reservation.train_run_id
    WHERE claim.active <> (reservation.status = 'held')
), ticket_violations AS (
    SELECT ticket.id
    FROM ticket
    LEFT JOIN ticket_order ON ticket_order.id = ticket.ticket_order_id
    LEFT JOIN reservation ON reservation.id = ticket_order.reservation_id
    LEFT JOIN reservation_seat ON reservation_seat.id = ticket.reservation_seat_id
    WHERE ticket_order.id IS NULL OR reservation.id IS NULL OR reservation_seat.id IS NULL
       OR reservation_seat.reservation_id <> reservation.id
       OR ticket_order.user_id <> reservation.user_id
       OR ticket_order.currency <> reservation.currency
       OR (ticket_order.status = 'confirmed' AND reservation.status <> 'confirmed')
       OR (ticket_order.status = 'cancelled' AND reservation.status <> 'cancelled')
       OR (ticket.status = 'active' AND ticket_order.status <> 'confirmed')
       OR (ticket.status = 'cancelled' AND ticket_order.status <> 'cancelled')
), idempotency_violations AS (
    SELECT local.id
    FROM idempotency AS local
    LEFT JOIN public.booking_idempotency_key_claims AS claim
      ON claim.user_id = local.user_id
     AND claim.operation = local.operation
     AND claim.key_hash = local.key_hash
     AND claim.request_fingerprint = local.request_fingerprint
     AND claim.train_run_id = local.train_run_id
     AND claim.expires_at = local.expires_at
    LEFT JOIN reservation AS resource
      ON local.status = 'completed'
     AND local.resource_type = 'reservation'
     AND resource.id = local.resource_id
    WHERE claim.id IS NULL
       OR (local.status = 'completed' AND resource.id IS NULL)
)
SELECT
    (SELECT count(*) FROM inventory),
    (SELECT count(*) FROM reservation),
    (SELECT count(*) FROM reservation_seat),
    (SELECT count(*) FROM ticket_order),
    (SELECT count(*) FROM ticket),
    (SELECT count(*) FROM idempotency),
    (SELECT count(*) FROM inventory_violations),
    (SELECT count(*) FROM orphan_active_seats),
    (SELECT count(*) FROM quota_structural_violations),
    (SELECT count(*) FROM quota_activity_violations),
    (SELECT count(*) FROM ticket_violations),
    (SELECT count(*) FROM idempotency_violations),
    ((SELECT count(*) FROM inventory_page) > $2
      OR (SELECT count(*) FROM reservation_page) > $2
      OR (SELECT count(*) FROM reservation_seat_page) > $2
      OR (SELECT count(*) FROM order_page) > $2
      OR (SELECT count(*) FROM ticket_page) > $2
      OR (SELECT count(*) FROM idempotency_page) > $2)`

func (source *postgresSource) CentralMigrationSnapshot(
	ctx context.Context,
	record migrationObservation,
	limit int64,
) (centralMigrationSnapshot, error) {
	if record.ID == uuid.Nil || record.TrainRunID == uuid.Nil || limit < 1 || limit > MaxRows {
		return centralMigrationSnapshot{}, ErrInvalidInput
	}
	var result centralMigrationSnapshot
	err := source.withQuery(ctx, func(query pgx.Tx) error {
		return query.QueryRow(ctx, centralMigrationSnapshotQuery, record.TrainRunID, limit, record.ID).Scan(
			&result.QuotaClaims,
			&result.IdempotencyClaims,
			&result.ReservationLocators,
			&result.TicketOrderLocators,
			&result.TicketLocators,
			&result.OutboxEvents,
			&result.MigrationsForTrainRun,
			&result.ActiveMigrations,
			&result.GenerationWriteRows,
			&result.TargetGenerationWriteRows,
			&result.TargetGenerationWrites,
			&result.LocatorRouteViolations,
			&result.IdempotencyRouteViolations,
			&result.OutboxProvenanceViolations,
			&result.GenerationWriteViolations,
			&result.Truncated,
		)
	})
	return result, err
}

const centralMigrationSnapshotQuery = `
WITH quota_page AS MATERIALIZED (
    SELECT * FROM public.reservation_quota_claims
    WHERE train_run_id = $1 ORDER BY reservation_id LIMIT ($2 + 1)
), quota AS (SELECT * FROM quota_page ORDER BY reservation_id LIMIT $2),
claim_page AS MATERIALIZED (
    SELECT * FROM public.booking_idempotency_key_claims
    WHERE train_run_id = $1 ORDER BY id LIMIT ($2 + 1)
), claim AS (SELECT * FROM claim_page ORDER BY id LIMIT $2),
reservation_locator_page AS MATERIALIZED (
    SELECT * FROM public.reservation_shard_locators
    WHERE train_run_id = $1 ORDER BY reservation_id LIMIT ($2 + 1)
), reservation_locator AS (SELECT * FROM reservation_locator_page ORDER BY reservation_id LIMIT $2),
order_locator_page AS MATERIALIZED (
    SELECT * FROM public.ticket_order_shard_locators
    WHERE train_run_id = $1 ORDER BY ticket_order_id LIMIT ($2 + 1)
), order_locator AS (SELECT * FROM order_locator_page ORDER BY ticket_order_id LIMIT $2),
ticket_locator_page AS MATERIALIZED (
    SELECT * FROM public.ticket_shard_locators
    WHERE train_run_id = $1 ORDER BY ticket_id LIMIT ($2 + 1)
), ticket_locator AS (SELECT * FROM ticket_locator_page ORDER BY ticket_id LIMIT $2),
outbox_page AS MATERIALIZED (
    SELECT * FROM public.outbox_events
    WHERE train_run_id = $1 ORDER BY id LIMIT ($2 + 1)
), outbox AS (SELECT * FROM outbox_page ORDER BY id LIMIT $2),
migration_page AS MATERIALIZED (
    SELECT * FROM public.train_run_shard_migrations
    WHERE train_run_id = $1 ORDER BY id LIMIT ($2 + 1)
), migration AS (SELECT * FROM migration_page ORDER BY id LIMIT $2),
generation_write_page AS MATERIALIZED (
    SELECT * FROM public.train_run_generation_writes
    WHERE train_run_id = $1 ORDER BY assignment_generation LIMIT ($2 + 1)
), generation_write AS (SELECT * FROM generation_write_page ORDER BY assignment_generation LIMIT $2),
locator_route_violations AS (
    SELECT reservation_id AS id, shard_id, assignment_generation FROM reservation_locator
    UNION ALL
    SELECT ticket_order_id, shard_id, assignment_generation FROM order_locator
    UNION ALL
    SELECT ticket_id, shard_id, assignment_generation FROM ticket_locator
), invalid_locator_routes AS (
    SELECT locator.id
    FROM locator_route_violations AS locator
    LEFT JOIN public.train_run_shard_assignments AS assignment ON assignment.train_run_id = $1
    WHERE assignment.train_run_id IS NULL
       OR locator.shard_id <> assignment.shard_id
       OR locator.assignment_generation <> assignment.assignment_generation
), invalid_claim_routes AS (
    SELECT claim.id
    FROM claim
    LEFT JOIN public.train_run_shard_assignments AS assignment ON assignment.train_run_id = claim.train_run_id
    WHERE assignment.train_run_id IS NULL
), invalid_outbox AS (
    SELECT event.id
    FROM outbox AS event
    WHERE event.shard_id NOT IN ('legacy', 'shard-0', 'shard-1')
       OR event.assignment_generation IS NULL OR event.assignment_generation <= 0
       OR (event.event_type LIKE 'reservation.%%' AND NOT EXISTS (
           SELECT 1 FROM reservation_locator WHERE reservation_id = event.aggregate_id
       ))
       OR (event.event_type = 'ticket.created' AND NOT EXISTS (
           SELECT 1 FROM ticket_locator WHERE ticket_id = event.aggregate_id
       ))
), invalid_generation_writes AS (
    SELECT evidence.assignment_generation
    FROM generation_write AS evidence
    LEFT JOIN migration
      ON migration.id = evidence.migration_id
     AND migration.target_shard_id = evidence.shard_id
     AND migration.target_generation = evidence.assignment_generation
    WHERE migration.id IS NULL
       OR evidence.successful_write_count < 0
       OR (evidence.successful_write_count = 0 AND (
           evidence.first_successful_write_at IS NOT NULL OR evidence.last_successful_write_at IS NOT NULL
       ))
       OR (evidence.successful_write_count > 0 AND (
           evidence.first_successful_write_at IS NULL OR evidence.last_successful_write_at IS NULL
       ))
), target_generation_write AS (
    SELECT evidence.*
    FROM generation_write AS evidence
    JOIN migration
      ON migration.id = $3
     AND migration.id = evidence.migration_id
     AND migration.target_shard_id = evidence.shard_id
     AND migration.target_generation = evidence.assignment_generation
)
SELECT
    (SELECT count(*) FROM quota),
    (SELECT count(*) FROM claim),
    (SELECT count(*) FROM reservation_locator),
    (SELECT count(*) FROM order_locator),
    (SELECT count(*) FROM ticket_locator),
    (SELECT count(*) FROM outbox),
    (SELECT count(*) FROM migration),
    (SELECT count(*) FROM migration WHERE state NOT IN ('completed', 'failed', 'rolled_back')),
    (SELECT count(*) FROM generation_write),
    (SELECT count(*) FROM target_generation_write),
    (SELECT count(*) FROM target_generation_write WHERE successful_write_count > 0),
    (SELECT count(*) FROM invalid_locator_routes),
    (SELECT count(*) FROM invalid_claim_routes),
    (SELECT count(*) FROM invalid_outbox),
    (SELECT count(*) FROM invalid_generation_writes),
    ((SELECT count(*) FROM quota_page) > $2
      OR (SELECT count(*) FROM claim_page) > $2
      OR (SELECT count(*) FROM reservation_locator_page) > $2
      OR (SELECT count(*) FROM order_locator_page) > $2
      OR (SELECT count(*) FROM ticket_locator_page) > $2
      OR (SELECT count(*) FROM outbox_page) > $2
      OR (SELECT count(*) FROM migration_page) > $2
      OR (SELECT count(*) FROM generation_write_page) > $2)`

func databaseError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return err
	}
	return errQuery
}
