package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	commandreconcile "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/reconcile"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/controlsource"
	physicalpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestBootstrapRequiresTheFixedShardAndCleanV2Schema(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name    string
		shardID string
		version int64
		dirty   bool
		wantErr error
	}{
		{name: "clean v2", shardID: "physical-shard-0", version: 2},
		{name: "old schema", shardID: "physical-shard-0", version: 0, wantErr: errState},
		{name: "dirty schema", shardID: "physical-shard-0", version: 2, dirty: true, wantErr: errState},
		{name: "unknown shard", shardID: "attacker-shard", wantErr: errArguments},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			backend := &postgresBackend{shards: map[string]physicalpostgres.DB{}}
			if validShard(testCase.shardID) {
				backend.shards[testCase.shardID] = &scriptedShard{rows: []pgx.Row{valueRow{testCase.version, testCase.dirty}}}
			}
			_, err := backend.checkSchema(context.Background(), testCase.shardID)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("checkSchema() error = %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestBootstrapMutatesCatalogOnlyAfterCleanV2Check(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name      string
		version   int64
		dirty     bool
		wantExec  int
		wantError error
	}{
		{name: "clean v2", version: 2, wantExec: 1},
		{name: "dirty v2", version: 2, dirty: true, wantError: errState},
		{name: "unsupported v1", version: 1, wantError: errState},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			control := &scriptedControl{rows: []pgx.Row{valueRow{"physical-shard-0", true, true, "healthy", "active", int32(1), int32(2)}}}
			backend := &postgresBackend{control: control, shards: map[string]physicalpostgres.DB{
				"physical-shard-0": &scriptedShard{rows: []pgx.Row{valueRow{testCase.version, testCase.dirty}}},
			}}
			_, err := backend.bootstrapShard(context.Background(), request{ShardID: "physical-shard-0", Confirm: true})
			if !errors.Is(err, testCase.wantError) || len(control.execSQL) != testCase.wantExec {
				t.Fatalf("bootstrap = err:%v exec:%d", err, len(control.execSQL))
			}
		})
	}
}

func TestPlanIsIdempotentOnlyForTheExactActiveMigration(t *testing.T) {
	t.Parallel()
	migrationID := uuid.MustParse(testMigration)
	trainRunID := uuid.MustParse(testTrainRun)
	req := request{Command: "plan-migration", MigrationID: migrationID, TrainRunID: trainRunID, TargetShardID: "physical-shard-1", Confirm: true}
	record := migrationRecord(migration.PhysicalStatePlanned)
	record.MigrationID, record.TrainRunID = migrationID, trainRunID

	t.Run("exact replay", func(t *testing.T) {
		tx := &scriptedTx{rows: []pgx.Row{valueRow{"physical-shard-0", int64(7), "migrating", &migrationID}}}
		control := &scriptedControl{tx: tx}
		backend := &postgresBackend{control: control, loadRecord: func(context.Context, uuid.UUID) (physicalmigration.Record, error) { return record, nil }}
		if _, err := backend.plan(context.Background(), req); err != nil {
			t.Fatalf("plan exact replay: %v", err)
		}
		if len(tx.execSQL) != 0 {
			t.Fatalf("idempotent replay mutated control: %v", tx.execSQL)
		}
	})

	t.Run("conflicting active migration", func(t *testing.T) {
		other := uuid.MustParse("33333333-3333-4333-8333-333333333333")
		tx := &scriptedTx{rows: []pgx.Row{valueRow{"physical-shard-0", int64(7), "migrating", &other}}}
		backend := &postgresBackend{control: &scriptedControl{tx: tx}}
		if _, err := backend.plan(context.Background(), req); !errors.Is(err, errState) {
			t.Fatalf("conflicting plan error = %v", err)
		}
		if len(tx.execSQL) != 0 {
			t.Fatalf("conflicting plan mutated control: %v", tx.execSQL)
		}
	})
}

func TestLegacyAndLogicalMigrationSourcesUseTheBoundedControlSourceAdapter(t *testing.T) {
	t.Parallel()
	for _, source := range []string{"legacy", "shard-0", "shard-1"} {
		source := source
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			record := migrationRecord(migration.PhysicalStatePlanned)
			record.SourceShardID = source
			backend := &postgresBackend{
				control:    &scriptedControl{},
				loadRecord: func(context.Context, uuid.UUID) (physicalmigration.Record, error) { return record, nil },
				shards: map[string]physicalpostgres.DB{
					"physical-shard-1": &scriptedShard{},
				},
			}
			if _, _, err := backend.engine(context.Background(), record.MigrationID); err != nil {
				t.Fatalf("engine source %q error = %v", source, err)
			}

			backend.control = &scriptedControl{rows: []pgx.Row{
				valueRow{source, int64(7), (*uuid.UUID)(nil)}, valueRow{true},
			}}
			_, err := backend.previewPlan(context.Background(), request{
				MigrationID: uuid.MustParse(testMigration), TrainRunID: record.TrainRunID,
				TargetShardID: "physical-shard-1",
			})
			if err != nil {
				t.Fatalf("previewPlan source %q error = %v", source, err)
			}

			tx := &scriptedTx{rows: []pgx.Row{
				valueRow{source, int64(7), "stable", (*uuid.UUID)(nil)}, valueRow{true},
			}}
			backend.control = &scriptedControl{tx: tx}
			_, err = backend.plan(context.Background(), request{
				MigrationID: uuid.MustParse(testMigration), TrainRunID: record.TrainRunID,
				TargetShardID: "physical-shard-1", Confirm: true,
			})
			if err != nil || len(tx.execSQL) != 2 {
				t.Fatalf("plan source %q = error:%v writes:%d", source, err, len(tx.execSQL))
			}
		})
	}
}

func TestReversePhysicalMigrationUsesTheFixedControlTargetAdapter(t *testing.T) {
	t.Parallel()
	for _, target := range []string{"legacy", "shard-0", "shard-1"} {
		record := migrationRecord(migration.PhysicalStatePlanned)
		record.TargetShardID = target
		record.TargetSchemaVersion = 8
		record.TargetGeneration = 9
		record.RetainedTargetGeneration = 7
		record.ReverseMigration = true
		backend := &postgresBackend{
			control: &scriptedControl{},
			shards: map[string]physicalpostgres.DB{
				record.SourceShardID: &scriptedShard{},
			},
		}
		operations, err := backend.shardOperations(record)
		if err != nil {
			t.Fatalf("target %q shardOperations() error = %v", target, err)
		}
		if _, ok := operations.(*controlsource.ReverseAdapter); !ok {
			t.Fatalf("target %q operations type = %T", target, operations)
		}
	}
}

func TestReconcileValidationExecutesTheCompleteFixedTableSet(t *testing.T) {
	t.Parallel()
	record := migrationRecord(migration.PhysicalStateValidatingOnline)
	const physicalV2MigrationTables = 15
	rows := make([]pgx.Row, 0, physicalV2MigrationTables+1)
	rows = append(rows, valueRow{0})
	for range physicalV2MigrationTables {
		rows = append(rows, valueRow{0, "", ""})
	}
	sourceRows := append([]pgx.Row(nil), rows...)
	targetRows := append([]pgx.Row(nil), rows...)
	backend := backendWithEngine(&fakeMigrationEngine{}, record)
	backend.shards = map[string]physicalpostgres.DB{
		record.SourceShardID: &scriptedShard{rows: sourceRows},
		record.TargetShardID: &scriptedShard{rows: targetRows},
	}
	result, err := backend.reconcile(context.Background(), record.MigrationID, 100)
	if err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}
	summary, ok := result.(struct {
		Passed       bool `json:"passed"`
		RowsExamined int  `json:"rows_examined"`
		Tables       int  `json:"tables"`
		Truncated    bool `json:"truncated"`
	})
	if !ok || !summary.Passed || summary.Tables != physicalV2MigrationTables || summary.Truncated {
		t.Fatalf("reconcile() result = %#v", result)
	}
}

func TestCutoverAdvancesOnlyEvidenceBearingStates(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name         string
		state        migration.PhysicalState
		wantAdvance  int
		wantComplete int
		wantErr      error
	}{
		{name: "too early", state: migration.PhysicalStateBaseCopying, wantErr: errState},
		{name: "final catchup", state: migration.PhysicalStateFinalCatchup, wantAdvance: 1},
		{name: "final validation", state: migration.PhysicalStateFinalValidating, wantAdvance: 1},
		{name: "target enabled", state: migration.PhysicalStateTargetEnabled, wantAdvance: 1},
		{name: "assignment switching", state: migration.PhysicalStateSwitchingAssignment, wantAdvance: 1},
		{name: "rollback deadline completion", state: migration.PhysicalStateRollbackWindow, wantComplete: 1},
		{name: "already complete", state: migration.PhysicalStateCompleted},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			engine := &fakeMigrationEngine{}
			record := migrationRecord(testCase.state)
			backend := backendWithEngine(engine, record)
			_, err := backend.cutover(context.Background(), request{MigrationID: record.MigrationID, Confirm: true})
			if !errors.Is(err, testCase.wantErr) || engine.advanceCalls != testCase.wantAdvance || engine.completeCalls != testCase.wantComplete {
				t.Fatalf("cutover = err:%v advance:%d complete:%d", err, engine.advanceCalls, engine.completeCalls)
			}
		})
	}
}

func TestDirectRollbackPropagatesTargetWriteRejection(t *testing.T) {
	t.Parallel()
	engine := &fakeMigrationEngine{rollbackErr: physicalmigration.ErrReverseMigrationRequired}
	record := migrationRecord(migration.PhysicalStateRollbackWindow)
	backend := backendWithEngine(engine, record)
	_, err := backend.rollback(context.Background(), request{MigrationID: record.MigrationID, Confirm: true})
	if !errors.Is(err, physicalmigration.ErrReverseMigrationRequired) || engine.rollbackCalls != 1 {
		t.Fatalf("rollback = (%v), calls=%d", err, engine.rollbackCalls)
	}
}

func TestCleanupGuardsAndSafeResume(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	completed := migrationRecord(migration.PhysicalStateCompleted)

	t.Run("before retention", func(t *testing.T) {
		beginCalls := 0
		backend := cleanupBackend(completed, now.Add(time.Minute), "not_requested", nil)
		backend.beginCleanup = func(context.Context, uuid.UUID, [32]byte) error { beginCalls++; return nil }
		if _, err := backend.cleanup(context.Background(), request{MigrationID: completed.MigrationID, Confirm: true}); !errors.Is(err, errState) || beginCalls != 0 {
			t.Fatalf("cleanup before retention = %v, begin=%d", err, beginCalls)
		}
	})

	t.Run("non-completed migration", func(t *testing.T) {
		record := completed
		record.State = migration.PhysicalStateRollbackWindow
		backend := cleanupBackend(record, now.Add(-time.Minute), "not_requested", nil)
		if _, err := backend.cleanup(context.Background(), request{MigrationID: record.MigrationID, Confirm: true}); !errors.Is(err, errState) {
			t.Fatalf("cleanup non-completed = %v", err)
		}
	})

	t.Run("active reverse mutex", func(t *testing.T) {
		backend := cleanupBackend(completed, now.Add(-time.Minute), "not_requested", nil)
		backend.beginCleanup = func(context.Context, uuid.UUID, [32]byte) error { return physicalmigration.ErrCleanupConflict }
		if _, err := backend.cleanup(context.Background(), request{MigrationID: completed.MigrationID, Confirm: true}); !errors.Is(err, errState) {
			t.Fatalf("cleanup active reverse = %v", err)
		}
	})

	t.Run("source must be retained", func(t *testing.T) {
		tx := &scriptedTx{rows: []pgx.Row{valueRow{false, true}}}
		backend := cleanupBackend(completed, now.Add(-time.Minute), "not_requested", tx)
		backend.beginCleanup = func(context.Context, uuid.UUID, [32]byte) error { return nil }
		if _, err := backend.cleanup(context.Background(), request{MigrationID: completed.MigrationID, Confirm: true}); !errors.Is(err, errState) || len(tx.execSQL) != 0 {
			t.Fatalf("cleanup non-retained = %v, deletes=%d", err, len(tx.execSQL))
		}
	})

	t.Run("running cleanup resumes after local commit", func(t *testing.T) {
		tx := &scriptedTx{rows: []pgx.Row{valueRow{false, false}}}
		backend := cleanupBackend(completed, now.Add(-time.Minute), "running", tx)
		beginCalls := 0
		backend.beginCleanup = func(context.Context, uuid.UUID, [32]byte) error { beginCalls++; return nil }
		if _, err := backend.cleanup(context.Background(), request{MigrationID: completed.MigrationID, Confirm: true}); err != nil {
			t.Fatalf("cleanup resume: %v", err)
		}
		if beginCalls != 1 || !tx.committed || len(tx.execSQL) != len(fixedCleanupStatements()) {
			t.Fatalf("resume begin=%d committed=%v deletes=%d", beginCalls, tx.committed, len(tx.execSQL))
		}
	})
}

func TestCleanupUsesOnlyFixedParameterizedDeletes(t *testing.T) {
	t.Parallel()
	statements := fixedCleanupStatements()
	if len(statements) != 16 {
		t.Fatalf("cleanup statement count = %d", len(statements))
	}
	for _, statement := range statements {
		trimmed := strings.TrimSpace(statement)
		if !strings.HasPrefix(trimmed, "DELETE FROM public.") || !strings.Contains(trimmed, "$1") || !strings.Contains(trimmed, "$2") || strings.Contains(trimmed, ";") {
			t.Fatalf("unbounded cleanup SQL: %q", statement)
		}
	}
}

func TestRepairDryRunInspectsButNeverRepairsOrMutatesSeatState(t *testing.T) {
	t.Parallel()
	commandID := uuid.MustParse(testMigration)
	trainRunID := uuid.MustParse(testTrainRun)
	ownerID := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	reservationID := uuid.MustParse("44444444-4444-4444-8444-444444444444")
	fingerprint := make([]byte, 32)
	fingerprint[0] = 1
	control := &scriptedControl{rows: []pgx.Row{valueRow{commandID, command.OperationCreateReservation, ownerID, trainRunID, reservationID, "physical-shard-0", int64(3), fingerprint, string(command.StateNeedsRepair), time.Now().Add(time.Minute)}}}
	repairer := &fakeRepairer{observation: commandreconcile.Observation{Kind: commandreconcile.ObservationMissing}}
	backend := &postgresBackend{control: control, repairerFactory: func() (commandRepairer, error) { return repairer, nil }}
	if _, err := backend.repairCommand(context.Background(), request{CommandID: commandID, DryRun: true}); err != nil {
		t.Fatalf("repair dry-run: %v", err)
	}
	if repairer.inspectCalls != 1 || repairer.repairCalls != 0 || control.beginCalls != 0 || len(control.execSQL) != 0 {
		t.Fatalf("dry-run inspect=%d repair=%d begin=%d exec=%d", repairer.inspectCalls, repairer.repairCalls, control.beginCalls, len(control.execSQL))
	}
}

type fakeMigrationEngine struct {
	advanceCalls, rollbackCalls, completeCalls int
	advanceErr, rollbackErr, completeErr       error
}

func (engine *fakeMigrationEngine) Advance(context.Context, uuid.UUID) (physicalmigration.Record, error) {
	engine.advanceCalls++
	return physicalmigration.Record{}, engine.advanceErr
}
func (engine *fakeMigrationEngine) Rollback(context.Context, uuid.UUID) (physicalmigration.Record, error) {
	engine.rollbackCalls++
	return physicalmigration.Record{}, engine.rollbackErr
}
func (engine *fakeMigrationEngine) Complete(context.Context, uuid.UUID) (physicalmigration.Record, error) {
	engine.completeCalls++
	return physicalmigration.Record{}, engine.completeErr
}
func (*fakeMigrationEngine) PlanReverse(context.Context, uuid.UUID, uuid.UUID, int64) (physicalmigration.Record, error) {
	return physicalmigration.Record{}, nil
}

func backendWithEngine(engine migrationEngine, record physicalmigration.Record) *postgresBackend {
	return &postgresBackend{engineFactory: func(context.Context, uuid.UUID) (migrationEngine, physicalmigration.Record, error) {
		return engine, record, nil
	}}
}

func migrationRecord(state migration.PhysicalState) physicalmigration.Record {
	return physicalmigration.Record{MigrationID: uuid.MustParse(testMigration), TrainRunID: uuid.MustParse(testTrainRun), SourceShardID: "physical-shard-0", TargetShardID: "physical-shard-1", SourceGeneration: 7, TargetGeneration: 8, SourceProtocolVersion: 1, SourceSchemaVersion: 2, TargetProtocolVersion: 1, TargetSchemaVersion: 2, State: state}
}

func cleanupBackend(record physicalmigration.Record, retention time.Time, state string, tx *scriptedTx) *postgresBackend {
	control := &scriptedControl{rows: []pgx.Row{valueRow{&retention, state}}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 1")}}
	backend := &postgresBackend{control: control, loadRecord: func(context.Context, uuid.UUID) (physicalmigration.Record, error) { return record, nil }, shards: map[string]physicalpostgres.DB{}}
	if tx != nil {
		backend.shards[record.SourceShardID] = &scriptedShard{tx: tx}
	}
	return backend
}

type scriptedControl struct {
	controlDatabase
	rows       []pgx.Row
	tx         pgx.Tx
	execTags   []pgconn.CommandTag
	execSQL    []string
	beginCalls int
}

func (db *scriptedControl) QueryRow(context.Context, string, ...any) pgx.Row {
	row := db.rows[0]
	db.rows = db.rows[1:]
	return row
}
func (db *scriptedControl) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	db.beginCalls++
	return db.tx, nil
}
func (db *scriptedControl) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	db.execSQL = append(db.execSQL, sql)
	if len(db.execTags) == 0 {
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
	tag := db.execTags[0]
	db.execTags = db.execTags[1:]
	return tag, nil
}

type scriptedShard struct {
	physicalpostgres.DB
	rows []pgx.Row
	tx   pgx.Tx
}

func (db *scriptedShard) QueryRow(context.Context, string, ...any) pgx.Row {
	row := db.rows[0]
	db.rows = db.rows[1:]
	return row
}
func (db *scriptedShard) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) { return db.tx, nil }

type scriptedTx struct {
	pgx.Tx
	rows      []pgx.Row
	execSQL   []string
	committed bool
}

func (tx *scriptedTx) QueryRow(context.Context, string, ...any) pgx.Row {
	row := tx.rows[0]
	tx.rows = tx.rows[1:]
	return row
}
func (tx *scriptedTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	tx.execSQL = append(tx.execSQL, sql)
	return pgconn.NewCommandTag("DELETE 1"), nil
}
func (tx *scriptedTx) Commit(context.Context) error { tx.committed = true; return nil }
func (*scriptedTx) Rollback(context.Context) error  { return nil }

type valueRow []any

func (row valueRow) Scan(destinations ...any) error {
	if len(row) != len(destinations) {
		return errors.New("scan arity mismatch")
	}
	for index, value := range row {
		switch destination := destinations[index].(type) {
		case *bool:
			*destination = value.(bool)
		case *int64:
			*destination = value.(int64)
		case *int:
			*destination = value.(int)
		case *int32:
			*destination = value.(int32)
		case *string:
			*destination = value.(string)
		case *uuid.UUID:
			*destination = value.(uuid.UUID)
		case **uuid.UUID:
			*destination = value.(*uuid.UUID)
		case *[]byte:
			*destination = value.([]byte)
		case *time.Time:
			*destination = value.(time.Time)
		case **time.Time:
			*destination = value.(*time.Time)
		case *command.Operation:
			*destination = value.(command.Operation)
		default:
			return errors.New("unsupported scan destination")
		}
	}
	return nil
}

type fakeRepairer struct {
	observation               commandreconcile.Observation
	inspectCalls, repairCalls int
}

func (repairer *fakeRepairer) Inspect(context.Context, commandreconcile.Candidate) (commandreconcile.Observation, error) {
	repairer.inspectCalls++
	return repairer.observation, nil
}
func (repairer *fakeRepairer) Repair(context.Context, commandreconcile.Candidate) (commandreconcile.Outcome, error) {
	repairer.repairCalls++
	return commandreconcile.OutcomeDeferred, nil
}
