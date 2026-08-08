package main

import (
	"context"
	"crypto/sha256"
	"math"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	commandreconcile "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/reconcile"
	commandphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/reconcile/physical"
	commandpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/reconcile/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/controlsource"
	physicalpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	controlEnv = "CONTROL_DATABASE_URL"
	shard0Env  = "BOOKING_SHARD_0_DATABASE_URL"
	shard1Env  = "BOOKING_SHARD_1_DATABASE_URL"
)

type postgresBackend struct {
	control         controlDatabase
	registry        *physical.Registry
	shards          map[string]physicalpostgres.DB
	loadRecord      func(context.Context, uuid.UUID) (physicalmigration.Record, error)
	engineFactory   func(context.Context, uuid.UUID) (migrationEngine, physicalmigration.Record, error)
	beginCleanup    func(context.Context, uuid.UUID, [32]byte) error
	repairerFactory func() (commandRepairer, error)
}

type controlDatabase interface {
	physicalpostgres.DB
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Close()
}

type migrationEngine interface {
	Advance(context.Context, uuid.UUID) (physicalmigration.Record, error)
	Rollback(context.Context, uuid.UUID) (physicalmigration.Record, error)
	Complete(context.Context, uuid.UUID) (physicalmigration.Record, error)
	PlanReverse(context.Context, uuid.UUID, uuid.UUID, int64) (physicalmigration.Record, error)
}

type commandRepairer interface {
	Inspect(context.Context, commandreconcile.Candidate) (commandreconcile.Observation, error)
	Repair(context.Context, commandreconcile.Candidate) (commandreconcile.Outcome, error)
}

type migrationSummary struct {
	MigrationID         uuid.UUID `json:"migration_id"`
	ParentMigration     bool      `json:"parent_migration"`
	TrainRunID          uuid.UUID `json:"train_run_id"`
	SourceShardID       string    `json:"source_shard_id"`
	TargetShardID       string    `json:"target_shard_id"`
	SourceGeneration    int64     `json:"source_generation"`
	TargetGeneration    int64     `json:"target_generation"`
	State               string    `json:"state"`
	RowsCopied          int64     `json:"rows_copied"`
	RowsReplayed        int64     `json:"rows_replayed"`
	ReplaySequence      int64     `json:"replay_sequence"`
	FinalSourceSequence int64     `json:"final_source_sequence"`
	ValidationVersion   int64     `json:"validation_version"`
	TargetWriteCount    int64     `json:"target_write_count"`
	ReverseMigration    bool      `json:"reverse_migration"`
	BaseCopyCursorSet   bool      `json:"base_copy_cursor_set"`
}

func openBackend(ctx context.Context, lookup func(string) (string, bool)) (backend, error) {
	values := make(map[string]string, 3)
	for _, name := range []string{controlEnv, shard0Env, shard1Env} {
		value, ok := lookup(name)
		value = strings.TrimSpace(value)
		if !ok || value == "" {
			return nil, errArguments
		}
		values[name] = value
	}
	control, err := pgxpool.New(ctx, values[controlEnv])
	if err != nil {
		return nil, errUnavailable
	}
	if err := requireOperatorRole(ctx, control); err != nil {
		control.Close()
		return nil, err
	}
	registry, err := physical.NewRegistry(ctx, physical.RegistryConfig{
		Connections: map[string]physical.ConnectionConfig{
			"physical-shard-0": {ShardID: sharding.ShardPhysicalZero, DSN: values[shard0Env]},
			"physical-shard-1": {ShardID: sharding.ShardPhysicalOne, DSN: values[shard1Env]},
		},
		MaxCount: 2,
		Limits:   physical.PoolLimits{MaxOpenConns: 4, MaxIdleConns: 2, MaxLifetime: 30 * time.Minute, MaxIdleTime: 5 * time.Minute, ConnectTimeout: 3 * time.Second},
	}, physical.OpenPGXPool)
	if err != nil {
		control.Close()
		return nil, errUnavailable
	}
	result := &postgresBackend{control: control, registry: registry, shards: make(map[string]physicalpostgres.DB, 2)}
	for _, shardID := range []sharding.ShardID{sharding.ShardPhysicalZero, sharding.ShardPhysicalOne} {
		handle, resolveErr := registry.Resolve(physical.CatalogEntry{ShardID: shardID, StorageKind: physical.StoragePostgres, ConnectionRef: shardID.String(), ProtocolVersion: physical.SupportedProtocolVersion, SchemaVersion: physical.SupportedSchemaVersion, Enabled: true, WriteEnabled: true, HealthState: physical.HealthHealthy, State: physical.StateActive})
		if resolveErr != nil {
			result.Close()
			return nil, errUnavailable
		}
		db, ok := handle.Pool().(physicalpostgres.DB)
		if !ok || requireOperatorRole(ctx, db) != nil {
			result.Close()
			return nil, errRole
		}
		result.shards[shardID.String()] = db
	}
	return result, nil
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func requireOperatorRole(ctx context.Context, db queryRower) error {
	var allowed bool
	err := db.QueryRow(ctx, `
SELECT role.rolsuper
    OR role.rolname IN ('operator', 'admin')
    OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles AS permitted
        WHERE permitted.rolname IN ('operator', 'admin')
          AND pg_has_role(role.oid, permitted.oid, 'member')
    )
FROM pg_catalog.pg_roles AS role
WHERE role.rolname = current_user`).Scan(&allowed)
	if err != nil {
		return errUnavailable
	}
	if !allowed {
		return errRole
	}
	return nil
}

func (b *postgresBackend) Close() {
	if b == nil {
		return
	}
	if b.registry != nil {
		b.registry.Close()
	}
	if b.control != nil {
		b.control.Close()
	}
}

func (b *postgresBackend) Execute(ctx context.Context, req request) (any, error) {
	switch req.Command {
	case "list-shards":
		return b.listShards(ctx, req.Limit)
	case "inspect-shard":
		return b.inspectShard(ctx, req.ShardID)
	case "check-schema":
		return b.checkSchema(ctx, req.ShardID)
	case "bootstrap-shard":
		return b.bootstrapShard(ctx, req)
	case "inspect-train-run":
		return b.inspectTrainRun(ctx, req.TrainRunID)
	case "plan-migration":
		if req.DryRun {
			return b.previewPlan(ctx, req)
		}
		return b.plan(ctx, req)
	case "inspect-crash-window":
		return b.inspectCrashWindow(ctx, req.MigrationID)
	case "rollback":
		return b.rollback(ctx, req)
	case "cutover":
		return b.cutover(ctx, req)
	case "plan-reverse-migration":
		return b.planReverse(ctx, req)
	case "start-reverse-migration":
		return b.advance(ctx, req, expectedState(req.Command))
	case "cleanup-source":
		return b.cleanup(ctx, req)
	case "repair-command":
		return b.repairCommand(ctx, req)
	case "reconcile":
		return b.reconcile(ctx, req.MigrationID, req.Limit)
	default:
		return b.advance(ctx, req, expectedState(req.Command))
	}
}

func (b *postgresBackend) controlStore() (*physicalpostgres.Control, error) {
	return physicalpostgres.NewControl(b.control)
}

func (b *postgresBackend) load(ctx context.Context, id uuid.UUID) (physicalmigration.Record, error) {
	if b.loadRecord != nil {
		return b.loadRecord(ctx, id)
	}
	control, err := b.controlStore()
	if err != nil {
		return physicalmigration.Record{}, err
	}
	return control.Load(ctx, id)
}

func (b *postgresBackend) engine(ctx context.Context, id uuid.UUID) (migrationEngine, physicalmigration.Record, error) {
	if b.engineFactory != nil {
		return b.engineFactory(ctx, id)
	}
	record, err := b.load(ctx, id)
	if err != nil {
		return nil, physicalmigration.Record{}, err
	}
	shards, err := b.shardOperations(record)
	if err != nil {
		return nil, physicalmigration.Record{}, err
	}
	control, err := b.controlStore()
	if err != nil {
		return nil, physicalmigration.Record{}, err
	}
	operationTimeout := defaultTimeout
	if deadline, ok := ctx.Deadline(); ok {
		operationTimeout = time.Until(deadline)
		if operationTimeout <= 0 {
			return nil, physicalmigration.Record{}, context.DeadlineExceeded
		}
	}
	engine, err := physicalmigration.NewEngine(control, shards, physicalmigration.Limits{OperationTimeout: operationTimeout, BaseCopyBatch: 500, JournalBatch: 500, ValidationRows: 100000, ValidationTables: physicalMigrationValidationTableLimit})
	return engine, record, err
}

func (b *postgresBackend) shardOperations(record physicalmigration.Record) (physicalmigration.ShardOperations, error) {
	if validControlSource(record.TargetShardID) {
		if !record.ReverseMigration || !validShard(record.SourceShardID) {
			return nil, errUnsupportedMigrationSource
		}
		source, sourceOK := b.shards[record.SourceShardID]
		if !sourceOK || source == nil {
			return nil, errUnavailable
		}
		return controlsource.NewReverse(b.control, source, record.TargetShardID)
	}
	target, targetOK := b.shards[record.TargetShardID]
	if !targetOK || target == nil {
		return nil, errUnavailable
	}
	if validShard(record.SourceShardID) {
		source, sourceOK := b.shards[record.SourceShardID]
		if !sourceOK || source == nil || source == target {
			return nil, errUnavailable
		}
		return physicalpostgres.NewDefaultShards(source, target)
	}
	if !validControlSource(record.SourceShardID) {
		return nil, errUnsupportedMigrationSource
	}
	return controlsource.New(b.control, target, record.SourceShardID)
}

func (b *postgresBackend) advance(ctx context.Context, req request, allowed []migration.PhysicalState) (any, error) {
	engine, record, err := b.engine(ctx, req.MigrationID)
	if err != nil {
		return nil, err
	}
	if req.DryRun {
		return summarize(record), nil
	}
	if len(allowed) > 0 && !containsState(allowed, record.State) {
		if phaseRank(record.State) > maxStateRank(allowed) {
			// Resuming after the command's durable transition is an idempotent no-op.
			return summarize(record), nil
		}
		return summarize(record), errState
	}
	next, err := engine.Advance(ctx, req.MigrationID)
	if err != nil {
		return summarize(record), err
	}
	return summarize(next), nil
}

func phaseRank(state migration.PhysicalState) int {
	states := []migration.PhysicalState{migration.PhysicalStatePlanned, migration.PhysicalStatePreparingTarget, migration.PhysicalStateCaptureEnabled, migration.PhysicalStateBaseCopying, migration.PhysicalStateCatchingUp, migration.PhysicalStateValidatingOnline, migration.PhysicalStateDraining, migration.PhysicalStateSourceFenced, migration.PhysicalStateFinalCatchup, migration.PhysicalStateFinalValidating, migration.PhysicalStateTargetEnabled, migration.PhysicalStateSwitchingAssignment, migration.PhysicalStateRollbackWindow, migration.PhysicalStateCompleted}
	for index, candidate := range states {
		if candidate == state {
			return index
		}
	}
	return -1
}

func maxStateRank(states []migration.PhysicalState) int {
	result := -1
	for _, state := range states {
		if rank := phaseRank(state); rank > result {
			result = rank
		}
	}
	return result
}

func containsState(states []migration.PhysicalState, state migration.PhysicalState) bool {
	for _, candidate := range states {
		if candidate == state {
			return true
		}
	}
	return false
}

func summarize(record physicalmigration.Record) migrationSummary {
	return migrationSummary{MigrationID: record.MigrationID, ParentMigration: record.ParentMigrationID != uuid.Nil, TrainRunID: record.TrainRunID, SourceShardID: record.SourceShardID, TargetShardID: record.TargetShardID, SourceGeneration: record.SourceGeneration, TargetGeneration: record.TargetGeneration, State: string(record.State), RowsCopied: record.RowsCopied, RowsReplayed: record.RowsReplayed, ReplaySequence: record.LastReplayedSequence, FinalSourceSequence: record.FinalSourceSequence, ValidationVersion: record.ValidationVersion, TargetWriteCount: record.TargetWriteCount, ReverseMigration: record.ReverseMigration, BaseCopyCursorSet: record.BaseCopyCursor != ""}
}

func (b *postgresBackend) listShards(ctx context.Context, limit int) (any, error) {
	rows, err := b.control.Query(ctx, `SELECT shard_id, storage_kind, enabled, write_enabled, health_state, state, protocol_version, schema_version FROM public.booking_shards WHERE shard_id IN ('physical-shard-0','physical-shard-1') ORDER BY shard_id LIMIT $1`, limit)
	if err != nil {
		return nil, errUnavailable
	}
	defer rows.Close()
	type item struct {
		ShardID         string `json:"shard_id"`
		StorageKind     string `json:"storage_kind"`
		Enabled         bool   `json:"enabled"`
		WriteEnabled    bool   `json:"write_enabled"`
		HealthState     string `json:"health_state"`
		State           string `json:"state"`
		ProtocolVersion int32  `json:"protocol_version"`
		SchemaVersion   int32  `json:"schema_version"`
	}
	result := make([]item, 0, 2)
	for rows.Next() {
		var value item
		if rows.Scan(&value.ShardID, &value.StorageKind, &value.Enabled, &value.WriteEnabled, &value.HealthState, &value.State, &value.ProtocolVersion, &value.SchemaVersion) != nil {
			return nil, errUnavailable
		}
		result = append(result, value)
	}
	if rows.Err() != nil {
		return nil, errUnavailable
	}
	return result, nil
}

func (b *postgresBackend) inspectShard(ctx context.Context, shardID string) (any, error) {
	var result struct {
		ShardID         string `json:"shard_id"`
		Enabled         bool   `json:"enabled"`
		WriteEnabled    bool   `json:"write_enabled"`
		HealthState     string `json:"health_state"`
		State           string `json:"state"`
		ProtocolVersion int32  `json:"protocol_version"`
		SchemaVersion   int32  `json:"schema_version"`
	}
	if err := b.control.QueryRow(ctx, `SELECT shard_id,enabled,write_enabled,health_state,state,protocol_version,schema_version FROM public.booking_shards WHERE shard_id=$1 AND storage_kind='postgres'`, shardID).Scan(&result.ShardID, &result.Enabled, &result.WriteEnabled, &result.HealthState, &result.State, &result.ProtocolVersion, &result.SchemaVersion); err != nil {
		return nil, errUnavailable
	}
	return result, nil
}

func (b *postgresBackend) checkSchema(ctx context.Context, shardID string) (any, error) {
	db := b.shards[shardID]
	if db == nil {
		return nil, errArguments
	}
	var version int64
	var dirty bool
	if err := db.QueryRow(ctx, `SELECT version::bigint, dirty FROM public.schema_migrations LIMIT 1`).Scan(&version, &dirty); err != nil {
		return nil, errUnavailable
	}
	result := struct {
		ShardID string `json:"shard_id"`
		Version int64  `json:"version"`
		Dirty   bool   `json:"dirty"`
		Ready   bool   `json:"ready"`
	}{shardID, version, dirty, version == int64(physical.SupportedSchemaVersion) && !dirty}
	if !result.Ready {
		return result, errState
	}
	return result, nil
}

func (b *postgresBackend) bootstrapShard(ctx context.Context, req request) (any, error) {
	result, err := b.checkSchema(ctx, req.ShardID)
	if err != nil || req.DryRun {
		return result, err
	}
	tag, err := b.control.Exec(ctx, `
UPDATE public.booking_shards
SET enabled = true, write_enabled = true, health_state = 'healthy',
    state = 'active', last_health_checked_at = clock_timestamp(),
    write_disabled_reason = NULL
WHERE shard_id = $1
  AND storage_kind = 'postgres'
  AND connection_ref = $1
  AND protocol_version = $2
  AND schema_version = $3`, req.ShardID, physical.SupportedProtocolVersion,
		physical.SupportedSchemaVersion)
	if err != nil || tag.RowsAffected() != 1 {
		return result, errState
	}
	return b.inspectShard(ctx, req.ShardID)
}

func (b *postgresBackend) inspectTrainRun(ctx context.Context, id uuid.UUID) (any, error) {
	var result struct {
		TrainRunID      uuid.UUID `json:"train_run_id"`
		ShardID         string    `json:"shard_id"`
		Generation      int64     `json:"generation"`
		State           string    `json:"state"`
		ActiveMigration bool      `json:"active_migration"`
	}
	var active *uuid.UUID
	if err := b.control.QueryRow(ctx, `SELECT train_run_id,shard_id,assignment_generation,assignment_state,active_physical_migration_id FROM public.train_run_shard_assignments WHERE train_run_id=$1`, id).Scan(&result.TrainRunID, &result.ShardID, &result.Generation, &result.State, &active); err != nil {
		return nil, errUnavailable
	}
	result.ActiveMigration = active != nil
	return result, nil
}

func (b *postgresBackend) previewPlan(ctx context.Context, req request) (any, error) {
	var source string
	var generation int64
	var active *uuid.UUID
	if err := b.control.QueryRow(ctx, `SELECT shard_id,assignment_generation,active_physical_migration_id FROM public.train_run_shard_assignments WHERE train_run_id=$1`, req.TrainRunID).Scan(&source, &generation, &active); err != nil {
		return nil, errUnavailable
	}
	if !validShard(source) && !validControlSource(source) {
		return nil, errUnsupportedMigrationSource
	}
	if active != nil || source == req.TargetShardID || generation == math.MaxInt64 {
		return nil, errState
	}
	var writable bool
	if err := b.control.QueryRow(ctx, `SELECT enabled AND write_enabled AND state='active' AND storage_kind='postgres' FROM public.booking_shards WHERE shard_id=$1`, req.TargetShardID).Scan(&writable); err != nil || !writable {
		return nil, errUnavailable
	}
	return struct {
		TrainRunID       uuid.UUID `json:"train_run_id"`
		MigrationID      uuid.UUID `json:"migration_id"`
		SourceShardID    string    `json:"source_shard_id"`
		TargetShardID    string    `json:"target_shard_id"`
		SourceGeneration int64     `json:"source_generation"`
		TargetGeneration int64     `json:"target_generation"`
	}{req.TrainRunID, req.MigrationID, source, req.TargetShardID, generation, generation + 1}, nil
}

func (b *postgresBackend) plan(ctx context.Context, req request) (any, error) {
	tx, err := b.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, errUnavailable
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var source, state string
	var generation int64
	var active *uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT shard_id,assignment_generation,assignment_state,active_physical_migration_id FROM public.train_run_shard_assignments WHERE train_run_id=$1 FOR UPDATE`, req.TrainRunID).Scan(&source, &generation, &state, &active); err != nil {
		return nil, errUnavailable
	}
	if active != nil && *active == req.MigrationID {
		record, loadErr := b.load(ctx, req.MigrationID)
		if loadErr == nil && record.TrainRunID == req.TrainRunID &&
			record.SourceShardID == source && record.TargetShardID == req.TargetShardID &&
			record.SourceGeneration == generation && record.TargetGeneration == generation+1 {
			return summarize(record), nil
		}
	}
	if !validShard(source) && !validControlSource(source) {
		return nil, errUnsupportedMigrationSource
	}
	if active != nil || state != "stable" || source == req.TargetShardID || generation == math.MaxInt64 {
		return nil, errState
	}
	var writable bool
	if err = tx.QueryRow(ctx, `SELECT enabled AND write_enabled AND state='active' AND storage_kind='postgres' FROM public.booking_shards WHERE shard_id=$1`, req.TargetShardID).Scan(&writable); err != nil || !writable {
		return nil, errUnavailable
	}
	_, err = tx.Exec(ctx, `INSERT INTO public.physical_shard_migrations(migration_id,train_run_id,source_shard_id,target_shard_id,source_generation,target_generation,state) VALUES($1,$2,$3,$4,$5,$6,'planned')`, req.MigrationID, req.TrainRunID, source, req.TargetShardID, generation, generation+1)
	if err != nil {
		return nil, errState
	}
	tag, err := tx.Exec(ctx, `UPDATE public.train_run_shard_assignments SET assignment_state='migrating',active_physical_migration_id=$2 WHERE train_run_id=$1 AND assignment_generation=$3 AND active_physical_migration_id IS NULL`, req.TrainRunID, req.MigrationID, generation)
	if err != nil || tag.RowsAffected() != 1 {
		return nil, errState
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, errUnavailable
	}
	record, err := b.load(ctx, req.MigrationID)
	if err != nil {
		return nil, err
	}
	return summarize(record), nil
}

func (b *postgresBackend) rollback(ctx context.Context, req request) (any, error) {
	engine, record, err := b.engine(ctx, req.MigrationID)
	if err != nil {
		return nil, err
	}
	if req.DryRun {
		return summarize(record), nil
	}
	next, err := engine.Rollback(ctx, req.MigrationID)
	if err != nil {
		return summarize(record), err
	}
	return summarize(next), nil
}

func (b *postgresBackend) cutover(ctx context.Context, req request) (any, error) {
	engine, record, err := b.engine(ctx, req.MigrationID)
	if err != nil {
		return nil, err
	}
	if req.DryRun || record.State == migration.PhysicalStateCompleted {
		return summarize(record), nil
	}
	if record.State == migration.PhysicalStateRollbackWindow {
		next, completeErr := engine.Complete(ctx, req.MigrationID)
		if completeErr != nil {
			return summarize(record), completeErr
		}
		return summarize(next), nil
	}
	if !containsState(expectedState("cutover"), record.State) {
		return summarize(record), errState
	}
	next, advanceErr := engine.Advance(ctx, req.MigrationID)
	if advanceErr != nil {
		return summarize(record), advanceErr
	}
	return summarize(next), nil
}

func (b *postgresBackend) planReverse(ctx context.Context, req request) (any, error) {
	engine, record, err := b.engine(ctx, req.MigrationID)
	if err != nil {
		return nil, err
	}
	if req.DryRun {
		return summarize(record), nil
	}
	next, err := engine.PlanReverse(ctx, req.MigrationID, req.ReverseMigrationID, req.Generation)
	if err != nil {
		return summarize(record), err
	}
	return summarize(next), nil
}

func (b *postgresBackend) inspectCrashWindow(ctx context.Context, id uuid.UUID) (any, error) {
	record, err := b.load(ctx, id)
	if err != nil {
		return nil, err
	}
	target := b.shards[record.TargetShardID]
	if target == nil {
		return nil, errUnavailable
	}
	type fence struct {
		WriteEnabled bool   `json:"write_enabled"`
		Generation   int64  `json:"generation"`
		State        string `json:"state"`
	}
	var sf, tf fence
	if sf, err = b.inspectSourceFence(ctx, record); err != nil {
		return nil, errUnavailable
	}
	if err = target.QueryRow(ctx, `SELECT write_enabled,assignment_generation,state FROM public.train_run_write_fences WHERE train_run_id=$1`, record.TrainRunID).Scan(&tf.WriteEnabled, &tf.Generation, &tf.State); err != nil {
		return nil, errUnavailable
	}
	return struct {
		Migration   migrationSummary `json:"migration"`
		Source      fence            `json:"source"`
		Target      fence            `json:"target"`
		WriterCount int              `json:"writer_count"`
	}{summarize(record), sf, tf, boolInt(sf.WriteEnabled) + boolInt(tf.WriteEnabled)}, nil
}

func (b *postgresBackend) inspectSourceFence(ctx context.Context, record physicalmigration.Record) (struct {
	WriteEnabled bool   `json:"write_enabled"`
	Generation   int64  `json:"generation"`
	State        string `json:"state"`
}, error) {
	var result struct {
		WriteEnabled bool   `json:"write_enabled"`
		Generation   int64  `json:"generation"`
		State        string `json:"state"`
	}
	if source := b.shards[record.SourceShardID]; source != nil {
		err := source.QueryRow(ctx, `SELECT write_enabled,assignment_generation,state FROM public.train_run_write_fences WHERE train_run_id=$1`, record.TrainRunID).Scan(&result.WriteEnabled, &result.Generation, &result.State)
		return result, err
	}
	var sql string
	switch record.SourceShardID {
	case "legacy":
		sql = `SELECT write_enabled,assignment_generation,CASE WHEN write_enabled THEN 'active' ELSE 'retained' END FROM public.train_run_write_fences WHERE train_run_id=$1`
	case "shard-0":
		sql = `SELECT write_enabled,assignment_generation,CASE WHEN write_enabled THEN 'active' ELSE 'retained' END FROM booking_shard_0.train_run_write_fences WHERE train_run_id=$1`
	case "shard-1":
		sql = `SELECT write_enabled,assignment_generation,CASE WHEN write_enabled THEN 'active' ELSE 'retained' END FROM booking_shard_1.train_run_write_fences WHERE train_run_id=$1`
	default:
		return result, errUnsupportedMigrationSource
	}
	err := b.control.QueryRow(ctx, sql, record.TrainRunID).Scan(&result.WriteEnabled, &result.Generation, &result.State)
	return result, err
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (b *postgresBackend) reconcile(ctx context.Context, id uuid.UUID, limit int) (any, error) {
	_, record, err := b.engine(ctx, id)
	if err != nil {
		return nil, err
	}
	shards, err := b.shardOperations(record)
	if err != nil {
		return nil, err
	}
	result, err := shards.Validate(ctx, physicalmigration.ValidationRequest{Migration: record, MaxRows: limit, MaxTables: physicalMigrationValidationTableLimit, Final: false})
	if err != nil {
		return nil, err
	}
	summary := struct {
		Passed       bool `json:"passed"`
		RowsExamined int  `json:"rows_examined"`
		Tables       int  `json:"tables"`
		Truncated    bool `json:"truncated"`
	}{result.Passed, result.RowsExamined, result.Tables, result.Truncated}
	if !result.Passed || result.Truncated {
		return summary, errState
	}
	return summary, nil
}

func (b *postgresBackend) cleanup(ctx context.Context, req request) (any, error) {
	record, err := b.load(ctx, req.MigrationID)
	if err != nil {
		return nil, err
	}
	var retention *time.Time
	var state string
	if err = b.control.QueryRow(ctx, `SELECT source_retention_until,cleanup_state FROM public.physical_shard_migrations WHERE migration_id=$1`, req.MigrationID).Scan(&retention, &state); err != nil {
		return nil, errUnavailable
	}
	eligible := retention != nil && !time.Now().UTC().Before(retention.UTC()) && record.State == migration.PhysicalStateCompleted
	preview := struct {
		Eligible           bool   `json:"eligible"`
		CleanupState       string `json:"cleanup_state"`
		RetentionSatisfied bool   `json:"retention_satisfied"`
	}{eligible, state, retention != nil && !time.Now().UTC().Before(retention.UTC())}
	if req.DryRun {
		return preview, nil
	}
	if state == "completed" {
		return preview, nil
	}
	if !eligible {
		return preview, errState
	}
	resumingCleanup := state == "running"
	hash := sha256.Sum256([]byte(req.MigrationID.String() + ":cleanup-source"))
	if err := b.beginCleanupMigration(ctx, req.MigrationID, hash); err != nil {
		return preview, errState
	}
	if validControlSource(record.SourceShardID) {
		if _, err := b.cleanupControlSource(ctx, record, req.Limit); err != nil {
			return preview, err
		}
		if err := b.finishCleanup(ctx, req.MigrationID, hash); err != nil {
			return preview, err
		}
		preview.CleanupState = "completed"
		return preview, nil
	}
	source := b.shards[record.SourceShardID]
	if source == nil {
		return preview, errUnavailable
	}
	tx, err := source.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return preview, errUnavailable
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var retained, snapshotPresent bool
	if err := tx.QueryRow(ctx, `
SELECT COALESCE((
           SELECT NOT write_enabled AND state='retained' AND assignment_generation=$2
           FROM public.train_run_write_fences WHERE train_run_id=$1 FOR UPDATE
       ), false),
       EXISTS (
           SELECT 1 FROM public.train_run_booking_snapshots
           WHERE train_run_id=$1 AND assignment_generation=$2
       )`, record.TrainRunID, record.SourceGeneration).Scan(&retained, &snapshotPresent); err != nil || (!retained && (!resumingCleanup || snapshotPresent)) {
		return preview, errState
	}
	// Fixed dependency order only; there is no operator-provided relation or SQL.
	var deleted int64
	for _, statement := range fixedCleanupStatements() {
		deleteTag, deleteErr := tx.Exec(ctx, statement, record.TrainRunID, record.SourceGeneration)
		if deleteErr != nil {
			return preview, errUnavailable
		}
		deleted += deleteTag.RowsAffected()
	}
	if err := tx.Commit(ctx); err != nil {
		return preview, errUnavailable
	}
	if err := b.finishCleanup(ctx, req.MigrationID, hash); err != nil {
		return preview, err
	}
	preview.CleanupState = "completed"
	_ = deleted // Deliberately omit row counts that could leak booking volume.
	return preview, nil
}

func (b *postgresBackend) finishCleanup(ctx context.Context, migrationID uuid.UUID, hash [32]byte) error {
	tag, err := b.control.Exec(ctx, `UPDATE public.physical_shard_migrations SET cleanup_state='completed',cleanup_completed_at=COALESCE(cleanup_completed_at,clock_timestamp()) WHERE migration_id=$1 AND state='completed' AND cleanup_state='running' AND cleanup_confirmation_hash=$2 AND source_retention_until<=clock_timestamp()`, migrationID, hash[:])
	if err != nil || tag.RowsAffected() != 1 {
		return errUnavailable
	}
	return nil
}

func (b *postgresBackend) cleanupControlSource(ctx context.Context, record physicalmigration.Record, limit int) (int64, error) {
	if limit <= 0 || !validControlSource(record.SourceShardID) {
		return 0, errArguments
	}
	tx, err := b.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return 0, errUnavailable
	}
	rollback := func(result error) (int64, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return 0, result
	}
	var captureDisabled, fenceDisabled bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
    SELECT 1 FROM public.physical_source_migration_capture_state
    WHERE train_run_id=$1 AND migration_id=$2 AND source_shard_id=$3
      AND source_generation=$4 AND NOT capture_enabled
)`, record.TrainRunID, record.MigrationID, record.SourceShardID,
		record.SourceGeneration).Scan(&captureDisabled); err != nil || !captureDisabled {
		return rollback(errState)
	}
	if err := tx.QueryRow(ctx, controlSourceFenceDisabledSQL(record.SourceShardID),
		record.TrainRunID, record.SourceGeneration).Scan(&fenceDisabled); err != nil || !fenceDisabled {
		return rollback(errState)
	}
	var count int64
	if err := tx.QueryRow(ctx, `
SELECT
 (SELECT count(*) FROM public.physical_source_ticket_rows WHERE source_shard_id=$1 AND train_run_id=$2)
 +(SELECT count(*) FROM public.physical_source_ticket_order_rows WHERE source_shard_id=$1 AND train_run_id=$2)
 +(SELECT count(*) FROM public.physical_source_reservation_seat_rows WHERE source_shard_id=$1 AND train_run_id=$2)
 +(SELECT count(*) FROM public.physical_source_idempotency_rows WHERE source_shard_id=$1 AND train_run_id=$2)
 +(SELECT count(*) FROM public.physical_source_reservation_rows WHERE source_shard_id=$1 AND train_run_id=$2)
 +(SELECT count(*) FROM public.physical_source_seat_inventory_rows WHERE source_shard_id=$1 AND train_run_id=$2)
 +(SELECT count(*) FROM public.physical_source_outbox_rows WHERE source_shard_id=$1 AND train_run_id=$2 AND assignment_generation=$3)
 +(SELECT count(*) FROM public.physical_source_train_run_mutation_journal WHERE migration_id=$4)
 +1`, record.SourceShardID, record.TrainRunID, record.SourceGeneration,
		record.MigrationID).Scan(&count); err != nil {
		return rollback(errUnavailable)
	}
	if count > int64(limit) {
		return rollback(physicalmigration.ErrCleanupLimitExceeded)
	}
	statements := controlSourceCleanupSQL(record.SourceShardID)
	if len(statements) == 0 {
		return rollback(errUnsupportedMigrationSource)
	}
	var deleted int64
	for _, statement := range statements {
		tag, err := tx.Exec(ctx, statement, record.TrainRunID, record.SourceGeneration,
			record.MigrationID, record.SourceShardID)
		if err != nil {
			return rollback(errUnavailable)
		}
		deleted += tag.RowsAffected()
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, errUnavailable
	}
	return deleted, nil
}

func controlSourceFenceDisabledSQL(sourceID string) string {
	switch sourceID {
	case "legacy":
		return `SELECT EXISTS (SELECT 1 FROM public.train_run_write_fences WHERE train_run_id=$1 AND assignment_generation=$2 AND NOT write_enabled FOR UPDATE)`
	case "shard-0":
		return `SELECT EXISTS (SELECT 1 FROM booking_shard_0.train_run_write_fences WHERE train_run_id=$1 AND assignment_generation=$2 AND NOT write_enabled FOR UPDATE)`
	case "shard-1":
		return `SELECT EXISTS (SELECT 1 FROM booking_shard_1.train_run_write_fences WHERE train_run_id=$1 AND assignment_generation=$2 AND NOT write_enabled FOR UPDATE)`
	default:
		return `SELECT false`
	}
}

func controlSourceCleanupSQL(sourceID string) []string {
	commonTail := []string{
		`DELETE FROM public.outbox_events WHERE train_run_id=$1 AND assignment_generation=$2 AND shard_id=$4`,
		`DELETE FROM public.physical_source_train_run_mutation_journal WHERE migration_id=$3`,
		`DELETE FROM public.physical_source_migration_capture_state WHERE migration_id=$3 AND NOT capture_enabled`,
	}
	var statements []string
	switch sourceID {
	case "legacy":
		statements = []string{
			`DELETE FROM public.tickets AS ticket USING public.ticket_orders AS orders,public.reservations AS reservation WHERE ticket.ticket_order_id=orders.id AND orders.reservation_id=reservation.id AND reservation.train_run_id=$1`,
			`DELETE FROM public.ticket_orders AS orders USING public.reservations AS reservation WHERE orders.reservation_id=reservation.id AND reservation.train_run_id=$1`,
			`DELETE FROM public.reservation_seats WHERE train_run_id=$1`,
			`DELETE FROM public.idempotency_records WHERE train_run_id=$1`,
			`DELETE FROM public.reservations WHERE train_run_id=$1`,
			`DELETE FROM public.seat_inventory WHERE train_run_id=$1`,
			`DELETE FROM public.train_run_write_fences WHERE train_run_id=$1 AND assignment_generation=$2 AND NOT write_enabled`,
		}
	case "shard-0":
		statements = []string{
			`DELETE FROM booking_shard_0.tickets AS ticket USING booking_shard_0.ticket_orders AS orders,booking_shard_0.reservations AS reservation WHERE ticket.ticket_order_id=orders.id AND orders.reservation_id=reservation.id AND reservation.train_run_id=$1`,
			`DELETE FROM booking_shard_0.ticket_orders AS orders USING booking_shard_0.reservations AS reservation WHERE orders.reservation_id=reservation.id AND reservation.train_run_id=$1`,
			`DELETE FROM booking_shard_0.reservation_seats WHERE train_run_id=$1`,
			`DELETE FROM booking_shard_0.idempotency_records WHERE train_run_id=$1`,
			`DELETE FROM booking_shard_0.reservations WHERE train_run_id=$1`,
			`DELETE FROM booking_shard_0.seat_inventory WHERE train_run_id=$1`,
			`DELETE FROM booking_shard_0.train_run_write_fences WHERE train_run_id=$1 AND assignment_generation=$2 AND NOT write_enabled`,
		}
	case "shard-1":
		statements = []string{
			`DELETE FROM booking_shard_1.tickets AS ticket USING booking_shard_1.ticket_orders AS orders,booking_shard_1.reservations AS reservation WHERE ticket.ticket_order_id=orders.id AND orders.reservation_id=reservation.id AND reservation.train_run_id=$1`,
			`DELETE FROM booking_shard_1.ticket_orders AS orders USING booking_shard_1.reservations AS reservation WHERE orders.reservation_id=reservation.id AND reservation.train_run_id=$1`,
			`DELETE FROM booking_shard_1.reservation_seats WHERE train_run_id=$1`,
			`DELETE FROM booking_shard_1.idempotency_records WHERE train_run_id=$1`,
			`DELETE FROM booking_shard_1.reservations WHERE train_run_id=$1`,
			`DELETE FROM booking_shard_1.seat_inventory WHERE train_run_id=$1`,
			`DELETE FROM booking_shard_1.train_run_write_fences WHERE train_run_id=$1 AND assignment_generation=$2 AND NOT write_enabled`,
		}
	default:
		return nil
	}
	return append(statements, commonTail...)
}

func (b *postgresBackend) beginCleanupMigration(ctx context.Context, migrationID uuid.UUID, hash [32]byte) error {
	if b.beginCleanup != nil {
		return b.beginCleanup(ctx, migrationID, hash)
	}
	control, err := b.controlStore()
	if err != nil {
		return err
	}
	return control.BeginCleanup(ctx, migrationID, hash)
}

func fixedCleanupStatements() []string {
	return []string{
		`DELETE FROM public.tickets WHERE train_run_id=$1 AND assignment_generation=$2`,
		`DELETE FROM public.ticket_orders WHERE train_run_id=$1 AND assignment_generation=$2`,
		`DELETE FROM public.reservation_seats WHERE train_run_id=$1 AND assignment_generation=$2`,
		`DELETE FROM public.outbox_events WHERE train_run_id=$1 AND assignment_generation=$2`,
		`DELETE FROM public.migration_apply_receipts WHERE train_run_id=$1 AND target_generation=$2`,
		`DELETE FROM public.train_run_mutation_journal WHERE train_run_id=$1 AND source_generation=$2`,
		`DELETE FROM public.migration_capture_state WHERE train_run_id=$1 AND source_generation=$2 AND NOT capture_enabled`,
		`DELETE FROM public.train_run_target_write_evidence WHERE train_run_id=$1 AND assignment_generation=$2`,
		`DELETE FROM public.train_run_write_fences WHERE train_run_id=$1 AND assignment_generation=$2 AND NOT write_enabled AND state='retained'`,
		`DELETE FROM public.booking_command_receipts WHERE train_run_id=$1 AND assignment_generation=$2`,
		`DELETE FROM public.idempotency_records WHERE train_run_id=$1 AND assignment_generation=$2`,
		`DELETE FROM public.reservations WHERE train_run_id=$1 AND assignment_generation=$2`,
		`DELETE FROM public.seat_inventory WHERE train_run_id=$1 AND assignment_generation=$2`,
		`DELETE FROM public.booking_fare_snapshots WHERE train_run_id=$1 AND assignment_generation=$2`,
		`DELETE FROM public.booking_seat_catalog WHERE train_run_id=$1 AND assignment_generation=$2`,
		`DELETE FROM public.train_run_booking_snapshots WHERE train_run_id=$1 AND assignment_generation=$2`,
	}
}

func (b *postgresBackend) repairCommand(ctx context.Context, req request) (any, error) {
	var candidate commandreconcile.Candidate
	var rawShard, rawState string
	var rawGeneration int64
	var rawFingerprint []byte
	if err := b.control.QueryRow(ctx, `
SELECT command_row.command_id, command_row.operation, command_row.owner_user_id,
       command_row.train_run_id, command_row.reservation_id,
       command_row.target_shard_id, command_row.assignment_generation,
       command_row.request_fingerprint, command_row.state, quota.expires_at
FROM public.booking_commands AS command_row
JOIN public.booking_quota_leases AS quota ON quota.command_id = command_row.command_id
WHERE command_row.command_id = $1`, req.CommandID).Scan(
		&candidate.Command.ID, &candidate.Command.Operation, &candidate.Command.OwnerUserID,
		&candidate.Command.TrainRunID, &candidate.Command.ReservationID, &rawShard,
		&rawGeneration, &rawFingerprint, &rawState, &candidate.QuotaExpiresAt,
	); err != nil || len(rawFingerprint) != 32 {
		return nil, errUnavailable
	}
	shardID, err := sharding.ParseShardID(rawShard)
	if err != nil || (shardID != sharding.ShardPhysicalZero && shardID != sharding.ShardPhysicalOne) {
		return nil, errState
	}
	generation, err := sharding.NewAssignmentGeneration(rawGeneration)
	if err != nil {
		return nil, errState
	}
	candidate.Command.Route, err = sharding.NewShardRoute(candidate.Command.TrainRunID, shardID, generation)
	if err != nil {
		return nil, errState
	}
	copy(candidate.Command.RequestFingerprint[:], rawFingerprint)
	candidate.Command.State = command.State(rawState)
	result := struct {
		CommandID uuid.UUID `json:"command_id"`
		State     string    `json:"state"`
		Outcome   string    `json:"outcome,omitempty"`
	}{CommandID: req.CommandID, State: rawState}
	service, err := b.commandRepairer()
	if err != nil {
		return result, errUnavailable
	}
	if req.DryRun {
		observation, inspectErr := service.Inspect(ctx, candidate)
		result.Outcome = string(observation.Kind)
		return result, inspectErr
	}
	outcome, repairErr := service.Repair(ctx, candidate)
	result.Outcome = string(outcome)
	return result, repairErr
}

func (b *postgresBackend) commandRepairer() (commandRepairer, error) {
	if b.repairerFactory != nil {
		return b.repairerFactory()
	}
	resolver, err := commandphysical.NewCatalogHandleResolver(b.control, b.registry)
	if err != nil {
		return nil, errUnavailable
	}
	inspector, err := commandphysical.NewInspector(resolver)
	if err != nil {
		return nil, errUnavailable
	}
	store, err := commandpostgres.NewStore(b.control)
	if err != nil {
		return nil, errUnavailable
	}
	return commandreconcile.New(store, inspector, commandreconcile.Options{WorkerID: "physical-shard-admin", BatchSize: 1, LeaseTTL: time.Minute, InspectTimeout: 5 * time.Second})
}
