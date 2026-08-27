package main

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control"
	controlpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control/postgres"
	shardreconcile "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/reconcile"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	maxAdminConnections       = 4
	maxCheckpointSummaryBytes = 128
)

type postgresBackend struct {
	pool       *pgxpool.Pool
	repository *controlpostgres.Repository
	service    *control.Service
	reconciler *shardreconcile.Inspector
}

type shardSummary struct {
	ShardID                       string `json:"shard_id"`
	StorageKind                   string `json:"storage_kind"`
	Enabled                       bool   `json:"enabled"`
	WriteEnabled                  bool   `json:"write_enabled"`
	State                         string `json:"state"`
	MinimumFencingProtocolVersion int32  `json:"minimum_fencing_protocol_version"`
}

type shardListResult struct {
	Items    []shardSummary `json:"items"`
	Complete bool           `json:"complete"`
}

type assignmentRecord struct {
	TrainRunID             uuid.UUID
	ShardID                sharding.ShardID
	AssignmentGeneration   sharding.AssignmentGeneration
	AssignmentState        string
	AvailabilityGeneration int64
	ActiveMigrationID      *uuid.UUID
}

type assignmentSummary struct {
	TrainRunID             uuid.UUID `json:"train_run_id"`
	ShardID                string    `json:"shard_id"`
	AssignmentGeneration   int64     `json:"assignment_generation"`
	AssignmentState        string    `json:"assignment_state"`
	AvailabilityGeneration int64     `json:"availability_generation"`
	ActiveMigration        bool      `json:"active_migration"`
}

type assignmentListResult struct {
	Items      []assignmentSummary `json:"items"`
	Complete   bool                `json:"complete"`
	NextCursor *uuid.UUID          `json:"next_cursor,omitempty"`
}

type fenceSummary struct {
	ShardID              string `json:"shard_id"`
	AssignmentGeneration int64  `json:"assignment_generation"`
	WriteEnabled         bool   `json:"write_enabled"`
	MatchesAssignment    bool   `json:"matches_assignment"`
}

type validationSummary struct {
	Present                    bool       `json:"present"`
	Passed                     bool       `json:"passed"`
	CheckedAt                  *time.Time `json:"checked_at,omitempty"`
	RowsExamined               int64      `json:"rows_examined"`
	Truncated                  bool       `json:"truncated"`
	InvariantViolations        int64      `json:"invariant_violations"`
	MissingReservationLocators int64      `json:"missing_reservation_locators"`
	MissingTicketOrderLocators int64      `json:"missing_ticket_order_locators"`
	MissingTicketLocators      int64      `json:"missing_ticket_locators"`
}

type migrationSummary struct {
	MigrationID           uuid.UUID         `json:"migration_id"`
	TrainRunID            uuid.UUID         `json:"train_run_id"`
	SourceShard           string            `json:"source_shard"`
	TargetShard           string            `json:"target_shard"`
	SourceGeneration      int64             `json:"source_generation"`
	TargetGeneration      int64             `json:"target_generation"`
	RollbackGeneration    *int64            `json:"rollback_generation,omitempty"`
	RollbackWindowSeconds int64             `json:"rollback_window_seconds"`
	State                 string            `json:"state"`
	CheckpointPresent     bool              `json:"checkpoint_present"`
	CopiedRows            int64             `json:"copied_rows"`
	CopyComplete          bool              `json:"copy_complete"`
	Validation            validationSummary `json:"validation"`
	CreatedAt             time.Time         `json:"created_at"`
	UpdatedAt             time.Time         `json:"updated_at"`
	CutoverAt             *time.Time        `json:"cutover_at,omitempty"`
	RollbackDeadline      *time.Time        `json:"rollback_deadline,omitempty"`
}

type trainRunInspection struct {
	Assignment      assignmentSummary `json:"assignment"`
	Fences          []fenceSummary    `json:"fences"`
	ActiveMigration *migrationSummary `json:"active_migration,omitempty"`
}

type planPreview struct {
	WouldPlan             bool      `json:"would_plan"`
	MigrationID           uuid.UUID `json:"migration_id"`
	TrainRunID            uuid.UUID `json:"train_run_id"`
	SourceShard           string    `json:"source_shard"`
	TargetShard           string    `json:"target_shard"`
	SourceGeneration      int64     `json:"source_generation"`
	TargetGeneration      int64     `json:"target_generation"`
	RollbackWindowSeconds int64     `json:"rollback_window_seconds"`
}

type validationResultSummary struct {
	Passed    bool             `json:"passed"`
	Migration migrationSummary `json:"migration"`
}

type healthResult struct {
	Ready                     bool  `json:"ready"`
	SchemaVersion             int64 `json:"schema_version"`
	SchemaDirty               bool  `json:"schema_dirty"`
	ShardCatalogEntries       int   `json:"shard_catalog_entries"`
	WritableActiveShards      int   `json:"writable_active_shards"`
	DegradedShards            int   `json:"degraded_shards"`
	ActiveMigrationsObserved  int64 `json:"active_migrations_observed"`
	ActiveMigrationsTruncated bool  `json:"active_migrations_truncated"`
}

func newPostgresBackend(ctx context.Context, databaseURL string, session postgresx.RegionalSession) (adminBackend, error) {
	if ctx == nil || databaseURL == "" {
		return nil, control.ErrInvalidInput
	}
	pool, err := postgresx.NewRegionalBoundedPool(ctx, databaseURL, maxAdminConnections, session)
	if err != nil {
		return nil, controlpostgres.ErrPersistence
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, controlpostgres.ErrPersistence
	}
	repository, err := controlpostgres.NewRepository(pool)
	if err != nil {
		pool.Close()
		return nil, err
	}
	service, err := control.NewService(repository, clock.RealClock{}, control.Limits{
		MaxBatchSize:        maxBatchSize,
		MaxCheckpointBytes:  maxCheckpointSummaryBytes,
		MaxOperationTimeout: maxCommandTimeout,
		MaxValidationRows:   maxRowCap,
		MaxLocatorRows:      maxRowCap,
		MaxRollbackWindow:   maxRollbackWindow,
	})
	if err != nil {
		pool.Close()
		return nil, err
	}
	reconciler, err := shardreconcile.New(pool)
	if err != nil {
		pool.Close()
		return nil, controlpostgres.ErrPersistence
	}
	return &postgresBackend{
		pool: pool, repository: repository, service: service, reconciler: reconciler,
	}, nil
}

func (backend *postgresBackend) Close() {
	if backend != nil && backend.pool != nil {
		backend.pool.Close()
	}
}

func (backend *postgresBackend) ListShards(ctx context.Context, limit int) (any, error) {
	if !backend.valid(ctx) || limit < 1 || limit > maxShardLimit {
		return nil, control.ErrInvalidInput
	}
	items, complete, err := backend.readShards(ctx, limit)
	if err != nil {
		return nil, err
	}
	return shardListResult{Items: items, Complete: complete}, nil
}

func (backend *postgresBackend) readShards(ctx context.Context, limit int) ([]shardSummary, bool, error) {
	rows, err := backend.pool.Query(ctx, `
SELECT shard_id,
       storage_kind,
       enabled,
       write_enabled,
       state,
       minimum_fencing_protocol_version
FROM public.booking_shards
WHERE storage_kind <> 'postgres'
ORDER BY shard_id
LIMIT $1`, limit+1)
	if err != nil {
		return nil, false, controlpostgres.ErrPersistence
	}
	defer rows.Close()

	items := make([]shardSummary, 0, limit+1)
	for rows.Next() {
		var item shardSummary
		if err := rows.Scan(
			&item.ShardID,
			&item.StorageKind,
			&item.Enabled,
			&item.WriteEnabled,
			&item.State,
			&item.MinimumFencingProtocolVersion,
		); err != nil {
			return nil, false, controlpostgres.ErrPersistence
		}
		if !validShardSummary(item) {
			return nil, false, control.ErrInvalidRecord
		}
		items = append(items, item)
	}
	if rows.Err() != nil {
		return nil, false, controlpostgres.ErrPersistence
	}
	complete := len(items) <= limit
	if !complete {
		items = items[:limit]
	}
	return items, complete, nil
}

func (backend *postgresBackend) ListAssignments(ctx context.Context, options assignmentListOptions) (any, error) {
	if !backend.valid(ctx) || options.Limit < 1 || options.Limit > maxAssignmentLimit {
		return nil, control.ErrInvalidInput
	}
	var after any
	if options.After != nil {
		if *options.After == uuid.Nil {
			return nil, control.ErrInvalidInput
		}
		after = *options.After
	}
	rows, err := backend.pool.Query(ctx, `
SELECT train_run_id,
       shard_id,
       assignment_generation,
       assignment_state,
       availability_generation,
       active_migration_id IS NOT NULL
FROM public.train_run_shard_assignments
WHERE ($1::uuid IS NULL OR train_run_id > $1)
ORDER BY train_run_id
LIMIT $2`, after, options.Limit+1)
	if err != nil {
		return nil, controlpostgres.ErrPersistence
	}
	defer rows.Close()

	items := make([]assignmentSummary, 0, options.Limit+1)
	for rows.Next() {
		var (
			trainRunID             uuid.UUID
			rawShard               string
			rawGeneration          int64
			state                  string
			availabilityGeneration int64
			activeMigration        bool
		)
		if err := rows.Scan(&trainRunID, &rawShard, &rawGeneration, &state, &availabilityGeneration, &activeMigration); err != nil {
			return nil, controlpostgres.ErrPersistence
		}
		record, err := newAssignmentRecord(trainRunID, rawShard, rawGeneration, state, availabilityGeneration, nil)
		if err != nil {
			return nil, err
		}
		item := summarizeAssignment(record)
		item.ActiveMigration = activeMigration
		items = append(items, item)
	}
	if rows.Err() != nil {
		return nil, controlpostgres.ErrPersistence
	}
	complete := len(items) <= options.Limit
	var nextCursor *uuid.UUID
	if !complete {
		items = items[:options.Limit]
		cursor := items[len(items)-1].TrainRunID
		nextCursor = &cursor
	}
	return assignmentListResult{Items: items, Complete: complete, NextCursor: nextCursor}, nil
}

func (backend *postgresBackend) InspectTrainRun(ctx context.Context, trainRunID uuid.UUID) (any, error) {
	if !backend.valid(ctx) || trainRunID == uuid.Nil {
		return nil, control.ErrInvalidInput
	}
	assignment, err := backend.assignment(ctx, trainRunID)
	if err != nil {
		return nil, err
	}
	fences, err := backend.fences(ctx, assignment)
	if err != nil {
		return nil, err
	}
	result := trainRunInspection{
		Assignment: summarizeAssignment(assignment),
		Fences:     fences,
	}
	if assignment.ActiveMigrationID != nil {
		record, err := backend.migration(ctx, *assignment.ActiveMigrationID)
		if err != nil {
			return nil, err
		}
		summary := summarizeMigration(record)
		result.ActiveMigration = &summary
	}
	return result, nil
}

func (backend *postgresBackend) PreviewPlan(ctx context.Context, options planOptions) (any, error) {
	input, err := backend.planInput(ctx, options)
	if err != nil {
		return nil, err
	}
	return planPreview{
		WouldPlan:             true,
		MigrationID:           input.MigrationID,
		TrainRunID:            input.TrainRunID,
		SourceShard:           input.SourceShard.String(),
		TargetShard:           input.TargetShard.String(),
		SourceGeneration:      input.SourceGeneration.Int64(),
		TargetGeneration:      input.TargetGeneration.Int64(),
		RollbackWindowSeconds: int64(input.RollbackWindow / time.Second),
	}, nil
}

func (backend *postgresBackend) Plan(ctx context.Context, options planOptions) (any, error) {
	input, err := backend.planInput(ctx, options)
	if err != nil {
		return nil, err
	}
	record, err := backend.service.Plan(ctx, input)
	if err != nil {
		return nil, err
	}
	return summarizeMigration(record), nil
}

func (backend *postgresBackend) planInput(ctx context.Context, options planOptions) (control.PlanInput, error) {
	if !backend.valid(ctx) || options.MigrationID == uuid.Nil || options.TrainRunID == uuid.Nil ||
		options.RollbackWindow <= 0 || options.RollbackWindow > maxRollbackWindow ||
		options.RollbackWindow%time.Second != 0 || !validTimeout(options.Timeout) {
		return control.PlanInput{}, control.ErrInvalidInput
	}
	if _, err := sharding.ParseShardID(options.TargetShard.String()); err != nil {
		return control.PlanInput{}, control.ErrInvalidInput
	}
	existing, err := backend.migration(ctx, options.MigrationID)
	if err == nil {
		return existingPlanInput(existing, options)
	}
	if !errors.Is(err, control.ErrMigrationNotFound) {
		return control.PlanInput{}, err
	}
	assignment, err := backend.assignment(ctx, options.TrainRunID)
	if err != nil {
		return control.PlanInput{}, err
	}
	if assignment.AssignmentState != "stable" || assignment.ActiveMigrationID != nil || assignment.ShardID == options.TargetShard {
		return control.PlanInput{}, control.ErrPlanConflict
	}
	if err := backend.requireWritableTarget(ctx, options.TargetShard); err != nil {
		return control.PlanInput{}, err
	}
	if assignment.AssignmentGeneration.Int64() == math.MaxInt64 {
		return control.PlanInput{}, control.ErrInvalidRecord
	}
	targetGeneration, err := sharding.NewAssignmentGeneration(assignment.AssignmentGeneration.Int64() + 1)
	if err != nil {
		return control.PlanInput{}, control.ErrInvalidRecord
	}
	return control.PlanInput{
		MigrationID:      options.MigrationID,
		TrainRunID:       options.TrainRunID,
		SourceShard:      assignment.ShardID,
		TargetShard:      options.TargetShard,
		SourceGeneration: assignment.AssignmentGeneration,
		TargetGeneration: targetGeneration,
		RollbackWindow:   options.RollbackWindow,
		OperationTimeout: options.Timeout,
	}, nil
}

func (backend *postgresBackend) InspectMigration(ctx context.Context, migrationID uuid.UUID) (any, error) {
	record, err := backend.migration(ctx, migrationID)
	if err != nil {
		return nil, err
	}
	return summarizeMigration(record), nil
}

func (backend *postgresBackend) CopyBatch(ctx context.Context, options copyOptions) (any, error) {
	if !backend.valid(ctx) {
		return nil, control.ErrInvalidInput
	}
	record, err := backend.service.CopyBatch(ctx, control.CopyBatchInput{
		MigrationID: options.MigrationID,
		BatchSize:   options.BatchSize,
		Timeout:     options.Timeout,
	})
	if err != nil {
		return nil, err
	}
	return summarizeMigration(record), nil
}

func (backend *postgresBackend) Validate(ctx context.Context, options validationOptions) (any, error) {
	if !backend.valid(ctx) {
		return nil, control.ErrInvalidInput
	}
	result, err := backend.service.Validate(ctx, control.ValidateInput{
		MigrationID: options.MigrationID,
		RowCap:      options.RowCap,
		Timeout:     options.Timeout,
	})
	if err != nil {
		return nil, err
	}
	return validationResultSummary{Passed: result.Passed, Migration: summarizeMigration(result.Record)}, nil
}

func (backend *postgresBackend) Cutover(ctx context.Context, options cutoverOptions) (any, error) {
	if !backend.valid(ctx) {
		return nil, control.ErrInvalidInput
	}
	record, err := backend.service.Cutover(ctx, control.CutoverInput{
		MigrationID:      options.MigrationID,
		ValidationRowCap: options.ValidationRowCap,
		LocatorRowCap:    options.LocatorRowCap,
		Timeout:          options.Timeout,
	})
	if err != nil {
		return nil, err
	}
	return summarizeMigration(record), nil
}

func (backend *postgresBackend) Rollback(ctx context.Context, options rollbackOptions) (any, error) {
	if !backend.valid(ctx) || options.MigrationID == uuid.Nil {
		return nil, control.ErrInvalidInput
	}
	record, err := backend.migration(ctx, options.MigrationID)
	if err != nil {
		return nil, err
	}
	generation, err := rollbackGenerationFor(record)
	if err != nil {
		return nil, err
	}
	record, err = backend.service.DirectRollback(ctx, control.DirectRollbackInput{
		MigrationID:        options.MigrationID,
		RollbackGeneration: generation,
		LocatorRowCap:      options.LocatorRowCap,
		Timeout:            options.Timeout,
	})
	if err != nil {
		return nil, err
	}
	return summarizeMigration(record), nil
}

func (backend *postgresBackend) CleanupEligibility(ctx context.Context, migrationID uuid.UUID) (cleanupEligibility, error) {
	if !backend.valid(ctx) || migrationID == uuid.Nil {
		return cleanupEligibility{}, control.ErrInvalidInput
	}
	result, err := backend.service.CleanupEligibility(ctx, control.CleanupEligibilityInput{
		MigrationID: migrationID,
		Timeout:     timeoutFromContext(ctx),
	})
	if err != nil {
		return cleanupEligibility{}, err
	}
	return cleanupEligibility{Eligible: result.Eligible, EligibleAt: utcTimePointer(result.EligibleAt), Reason: result.Reason}, nil
}

func (backend *postgresBackend) Reconcile(ctx context.Context, options reconcileOptions) (any, error) {
	if !backend.valid(ctx) || options.TrainRunID == uuid.Nil || !validRowCap(options.RowCap) {
		return nil, control.ErrInvalidInput
	}
	pageSize := shardreconcile.DefaultPageSize
	if options.RowCap < int64(pageSize) {
		pageSize = int(options.RowCap)
	}
	trainRunID := options.TrainRunID
	report, err := backend.reconciler.Locators(
		ctx,
		shardreconcile.LocatorFilter{TrainRunID: &trainRunID},
		shardreconcile.Limits{
			PageSize: pageSize, MaxPages: shardreconcile.DefaultMaxPages, MaxRows: options.RowCap,
		},
	)
	return report, mapReconciliationError(err)
}

func (backend *postgresBackend) InspectHealth(ctx context.Context) (any, error) {
	if !backend.valid(ctx) {
		return nil, control.ErrInvalidInput
	}
	var result healthResult
	if err := backend.pool.QueryRow(ctx, `
SELECT version::bigint, dirty
FROM public.schema_migrations
LIMIT 1`).Scan(&result.SchemaVersion, &result.SchemaDirty); err != nil {
		return nil, controlpostgres.ErrPersistence
	}
	shards, complete, err := backend.readShards(ctx, maxShardLimit)
	if err != nil {
		return nil, err
	}
	result.ShardCatalogEntries = len(shards)
	for _, shard := range shards {
		if shardWritableByCLI(shard) {
			result.WritableActiveShards++
		} else {
			result.DegradedShards++
		}
	}
	var activeMigrations int64
	if err := backend.pool.QueryRow(ctx, `
SELECT count(*)::bigint
FROM (
    SELECT 1
    FROM public.train_run_shard_migrations
    WHERE state NOT IN ('completed', 'failed', 'rolled_back')
    LIMIT $1
) AS bounded_active_migrations`, maxRowCap+1).Scan(&activeMigrations); err != nil {
		return nil, controlpostgres.ErrPersistence
	}
	if activeMigrations < 0 {
		return nil, control.ErrInvalidRecord
	}
	result.ActiveMigrationsTruncated = activeMigrations > maxRowCap
	if result.ActiveMigrationsTruncated {
		activeMigrations = maxRowCap
	}
	result.ActiveMigrationsObserved = activeMigrations
	result.Ready = result.SchemaVersion == 11 && !result.SchemaDirty && complete &&
		result.ShardCatalogEntries == maxShardLimit && result.WritableActiveShards == maxShardLimit &&
		!result.ActiveMigrationsTruncated
	if !result.Ready {
		return result, errControlStateInvalid
	}
	return result, nil
}

func (backend *postgresBackend) assignment(ctx context.Context, trainRunID uuid.UUID) (assignmentRecord, error) {
	if !backend.valid(ctx) || trainRunID == uuid.Nil {
		return assignmentRecord{}, control.ErrInvalidInput
	}
	var (
		rawShard               string
		rawGeneration          int64
		state                  string
		availabilityGeneration int64
		activeMigration        pgtype.UUID
	)
	err := backend.pool.QueryRow(ctx, `
SELECT shard_id,
       assignment_generation,
       assignment_state,
       availability_generation,
       active_migration_id
FROM public.train_run_shard_assignments
WHERE train_run_id = $1`, trainRunID).Scan(
		&rawShard,
		&rawGeneration,
		&state,
		&availabilityGeneration,
		&activeMigration,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return assignmentRecord{}, errResourceNotFound
	}
	if err != nil {
		return assignmentRecord{}, controlpostgres.ErrPersistence
	}
	var activeMigrationID *uuid.UUID
	if activeMigration.Valid {
		value := uuid.UUID(activeMigration.Bytes)
		if value == uuid.Nil {
			return assignmentRecord{}, control.ErrInvalidRecord
		}
		activeMigrationID = &value
	}
	return newAssignmentRecord(
		trainRunID,
		rawShard,
		rawGeneration,
		state,
		availabilityGeneration,
		activeMigrationID,
	)
}

func (backend *postgresBackend) fences(ctx context.Context, assignment assignmentRecord) ([]fenceSummary, error) {
	rows, err := backend.pool.Query(ctx, `
SELECT 'legacy'::text AS shard_id, assignment_generation, write_enabled
FROM public.train_run_write_fences
WHERE train_run_id = $1
UNION ALL
SELECT 'shard-0'::text AS shard_id, assignment_generation, write_enabled
FROM booking_shard_0.train_run_write_fences
WHERE train_run_id = $1
UNION ALL
SELECT 'shard-1'::text AS shard_id, assignment_generation, write_enabled
FROM booking_shard_1.train_run_write_fences
WHERE train_run_id = $1
ORDER BY shard_id`, assignment.TrainRunID)
	if err != nil {
		return nil, controlpostgres.ErrPersistence
	}
	defer rows.Close()
	items := make([]fenceSummary, 0, maxShardLimit)
	for rows.Next() {
		var item fenceSummary
		if err := rows.Scan(&item.ShardID, &item.AssignmentGeneration, &item.WriteEnabled); err != nil {
			return nil, controlpostgres.ErrPersistence
		}
		if _, err := sharding.ParseShardID(item.ShardID); err != nil || item.AssignmentGeneration <= 0 {
			return nil, control.ErrInvalidRecord
		}
		item.MatchesAssignment = item.ShardID == assignment.ShardID.String() &&
			item.AssignmentGeneration == assignment.AssignmentGeneration.Int64()
		items = append(items, item)
		if len(items) > maxShardLimit {
			return nil, control.ErrInvalidRecord
		}
	}
	if rows.Err() != nil {
		return nil, controlpostgres.ErrPersistence
	}
	return items, nil
}

func (backend *postgresBackend) migration(ctx context.Context, migrationID uuid.UUID) (control.Record, error) {
	if !backend.valid(ctx) || migrationID == uuid.Nil {
		return control.Record{}, control.ErrInvalidInput
	}
	var result control.Record
	err := backend.repository.WithinTransaction(ctx, func(ctx context.Context, tx control.Transaction) error {
		record, found, err := tx.FindMigrationForUpdate(ctx, migrationID)
		if err != nil {
			return err
		}
		if !found {
			return control.ErrMigrationNotFound
		}
		result = record
		return nil
	})
	if err != nil {
		return control.Record{}, err
	}
	return result, nil
}

func (backend *postgresBackend) requireWritableTarget(ctx context.Context, shardID sharding.ShardID) error {
	var enabled, writeEnabled bool
	var state string
	var minimumProtocol int32
	err := backend.pool.QueryRow(ctx, `
SELECT enabled, write_enabled, state, minimum_fencing_protocol_version
FROM public.booking_shards
WHERE shard_id = $1`, shardID.String()).Scan(&enabled, &writeEnabled, &state, &minimumProtocol)
	if errors.Is(err, pgx.ErrNoRows) {
		return control.ErrInvalidInput
	}
	if err != nil {
		return controlpostgres.ErrPersistence
	}
	if minimumProtocol <= 0 {
		return control.ErrInvalidRecord
	}
	if !enabled || !writeEnabled || state != "active" ||
		minimumProtocol > sharding.SupportedFencingProtocolVersion {
		return control.ErrShardNotWritable
	}
	return nil
}

func (backend *postgresBackend) valid(ctx context.Context) bool {
	return backend != nil && backend.pool != nil && backend.repository != nil && backend.service != nil &&
		backend.reconciler != nil && ctx != nil
}

func newAssignmentRecord(
	trainRunID uuid.UUID,
	rawShard string,
	rawGeneration int64,
	state string,
	availabilityGeneration int64,
	activeMigrationID *uuid.UUID,
) (assignmentRecord, error) {
	if trainRunID == uuid.Nil || availabilityGeneration <= 0 || !validAssignmentState(state) {
		return assignmentRecord{}, control.ErrInvalidRecord
	}
	shardID, err := sharding.ParseShardID(rawShard)
	if err != nil {
		return assignmentRecord{}, control.ErrInvalidRecord
	}
	generation, err := sharding.NewAssignmentGeneration(rawGeneration)
	if err != nil {
		return assignmentRecord{}, control.ErrInvalidRecord
	}
	if activeMigrationID != nil && *activeMigrationID == uuid.Nil {
		return assignmentRecord{}, control.ErrInvalidRecord
	}
	return assignmentRecord{
		TrainRunID:             trainRunID,
		ShardID:                shardID,
		AssignmentGeneration:   generation,
		AssignmentState:        state,
		AvailabilityGeneration: availabilityGeneration,
		ActiveMigrationID:      activeMigrationID,
	}, nil
}

func existingPlanInput(record control.Record, options planOptions) (control.PlanInput, error) {
	if record.MigrationID == uuid.Nil || record.TrainRunID == uuid.Nil ||
		record.SourceShard == record.TargetShard || record.SourceGeneration.Int64() <= 0 ||
		record.TargetGeneration.Int64() <= record.SourceGeneration.Int64() {
		return control.PlanInput{}, control.ErrInvalidRecord
	}
	if record.MigrationID != options.MigrationID || record.TrainRunID != options.TrainRunID ||
		record.TargetShard != options.TargetShard || record.RollbackWindow != options.RollbackWindow {
		return control.PlanInput{}, control.ErrPlanConflict
	}
	return control.PlanInput{
		MigrationID:      record.MigrationID,
		TrainRunID:       record.TrainRunID,
		SourceShard:      record.SourceShard,
		TargetShard:      record.TargetShard,
		SourceGeneration: record.SourceGeneration,
		TargetGeneration: record.TargetGeneration,
		RollbackWindow:   record.RollbackWindow,
		OperationTimeout: options.Timeout,
	}, nil
}

func rollbackGenerationFor(record control.Record) (sharding.AssignmentGeneration, error) {
	if record.RollbackGeneration != nil {
		if record.RollbackGeneration.Int64() <= record.TargetGeneration.Int64() {
			return 0, control.ErrInvalidRecord
		}
		return *record.RollbackGeneration, nil
	}
	if record.TargetGeneration.Int64() <= 0 || record.TargetGeneration.Int64() == math.MaxInt64 {
		return 0, control.ErrInvalidRecord
	}
	generation, err := sharding.NewAssignmentGeneration(record.TargetGeneration.Int64() + 1)
	if err != nil {
		return 0, control.ErrInvalidRecord
	}
	return generation, nil
}

func mapReconciliationError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, shardreconcile.ErrUnavailable):
		return controlpostgres.ErrPersistence
	case errors.Is(err, shardreconcile.ErrPartial), errors.Is(err, shardreconcile.ErrLimitReached):
		return errReconciliationIncomplete
	case errors.Is(err, shardreconcile.ErrViolations):
		return errReconciliationMismatch
	case errors.Is(err, shardreconcile.ErrInvalidInput):
		return control.ErrInvalidInput
	default:
		return controlpostgres.ErrPersistence
	}
}

func summarizeAssignment(record assignmentRecord) assignmentSummary {
	return assignmentSummary{
		TrainRunID:             record.TrainRunID,
		ShardID:                record.ShardID.String(),
		AssignmentGeneration:   record.AssignmentGeneration.Int64(),
		AssignmentState:        record.AssignmentState,
		AvailabilityGeneration: record.AvailabilityGeneration,
		ActiveMigration:        record.ActiveMigrationID != nil,
	}
}

func summarizeMigration(record control.Record) migrationSummary {
	result := migrationSummary{
		MigrationID:           record.MigrationID,
		TrainRunID:            record.TrainRunID,
		SourceShard:           record.SourceShard.String(),
		TargetShard:           record.TargetShard.String(),
		SourceGeneration:      record.SourceGeneration.Int64(),
		TargetGeneration:      record.TargetGeneration.Int64(),
		RollbackWindowSeconds: int64(record.RollbackWindow / time.Second),
		State:                 string(record.State),
		CheckpointPresent:     record.Checkpoint != "",
		CopiedRows:            record.CopiedRows,
		CopyComplete:          record.CopyComplete,
		CreatedAt:             record.CreatedAt.UTC(),
		UpdatedAt:             record.UpdatedAt.UTC(),
		CutoverAt:             utcTimePointer(record.CutoverAt),
		RollbackDeadline:      utcTimePointer(record.RollbackDeadline),
	}
	if record.RollbackGeneration != nil {
		value := record.RollbackGeneration.Int64()
		result.RollbackGeneration = &value
	}
	if record.LastValidation != nil {
		snapshot := record.LastValidation.Snapshot
		checkedAt := record.LastValidation.CheckedAt.UTC()
		result.Validation = validationSummary{
			Present:                    true,
			Passed:                     record.LastValidation.Passed,
			CheckedAt:                  &checkedAt,
			RowsExamined:               snapshot.RowsExamined,
			Truncated:                  snapshot.Truncated,
			InvariantViolations:        snapshot.InvariantViolations,
			MissingReservationLocators: snapshot.MissingReservationLocators,
			MissingTicketOrderLocators: snapshot.MissingTicketOrderLocators,
			MissingTicketLocators:      snapshot.MissingTicketLocators,
		}
	}
	return result
}

func validShardSummary(summary shardSummary) bool {
	shardID, err := sharding.ParseShardID(summary.ShardID)
	if err != nil || summary.MinimumFencingProtocolVersion <= 0 {
		return false
	}
	switch shardID {
	case sharding.ShardLegacy:
		if summary.StorageKind != "legacy" && summary.StorageKind != "legacy_schema" {
			return false
		}
	case sharding.ShardZero, sharding.ShardOne:
		if summary.StorageKind != "schema" && summary.StorageKind != "logical_schema" {
			return false
		}
	default:
		return false
	}
	switch summary.State {
	case "active", "degraded", "draining", "disabled":
		return true
	default:
		return false
	}
}

func shardWritableByCLI(summary shardSummary) bool {
	return summary.Enabled && summary.WriteEnabled && summary.State == "active" &&
		summary.MinimumFencingProtocolVersion <= sharding.SupportedFencingProtocolVersion
}

func validAssignmentState(state string) bool {
	switch state {
	case "stable", "draining", "migrating", "rollback_window":
		return true
	default:
		return false
	}
}

func utcTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	result := value.UTC()
	return &result
}

func timeoutFromContext(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return defaultCommandTimeout
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return time.Nanosecond
	}
	if remaining > maxCommandTimeout {
		return maxCommandTimeout
	}
	return remaining
}
