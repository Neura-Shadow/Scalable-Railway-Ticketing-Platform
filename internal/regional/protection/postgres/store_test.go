package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/protection"
	protectionpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/protection/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestMetadataStorePersistsValidatedEncryptedBackupEvidence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 16, 0, 0, 0, time.UTC)
	checksum := protection.HashEvidence([]byte("manifest"))
	position, _ := recovery.NewReplicationPosition(7, 900)
	service := mustMetadataProtectionService(t, protection.Result{
		Success: true, BackupSet: "20260811-160000F", Checksum: checksum, Encrypted: true,
		SourcePosition: position, CompletedAt: now,
	})
	evidence, err := service.Backup(context.Background(), protection.BackupRequest{
		Database: recovery.DatabaseControl, Repository: "repo-dr",
	})
	if err != nil {
		t.Fatal(err)
	}
	db := &metadataDB{tags: []pgconn.CommandTag{pgconn.NewCommandTag("INSERT 0 1")}}
	store, err := protectionpostgres.NewMetadataStore(db)
	if err != nil {
		t.Fatalf("NewMetadataStore() error = %v", err)
	}

	artifact, created, err := store.RecordBackup(context.Background(), evidence, now)
	if err != nil || !created || artifact.BackupID.String() == "" || artifact.BackupSet != evidence.BackupSet() {
		t.Fatalf("RecordBackup() = %+v/%v/%v", artifact, created, err)
	}
	if len(db.execSQL) != 1 || !strings.Contains(db.execSQL[0], "backup_artifacts") ||
		!strings.Contains(db.execSQL[0], "ON CONFLICT DO NOTHING") {
		t.Fatalf("record SQL = %v", db.execSQL)
	}
	if wal, ok := db.execArgs[0][7].(int64); !ok || wal != 900 {
		t.Fatalf("source WAL argument = %T/%v, want int64/900", db.execArgs[0][7], db.execArgs[0][7])
	}
}

func TestMetadataStorePersistsMutationIntentBeforeCompletion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 18, 0, 0, 0, time.UTC)
	operationID := uuid.New()
	backupID := uuid.New()
	db := &metadataDB{tags: []pgconn.CommandTag{
		pgconn.NewCommandTag("INSERT 0 1"),
		pgconn.NewCommandTag("UPDATE 1"),
		pgconn.NewCommandTag("INSERT 0 1"),
	}}
	store, err := protectionpostgres.NewMetadataStore(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BeginBackupOperation(context.Background(), operationID, recovery.DatabaseControl, "repo-dr", now); err != nil {
		t.Fatalf("BeginBackupOperation() error = %v", err)
	}
	artifact := protectionpostgres.Artifact{
		BackupID: backupID, Database: recovery.DatabaseControl, Repository: "repo-dr",
	}
	if err := store.CompleteBackupOperation(context.Background(), operationID, artifact, now.Add(time.Minute)); err != nil {
		t.Fatalf("CompleteBackupOperation() error = %v", err)
	}
	pitrTarget := now.Add(-time.Minute)
	if err := store.BeginRestoreValidation(
		context.Background(), uuid.New(), artifact, "validation-control", pitrTarget, now,
	); err != nil {
		t.Fatalf("BeginRestoreValidation() error = %v", err)
	}
	if len(db.execSQL) != 3 || !strings.Contains(db.execSQL[0], "state,requested_at") ||
		!strings.Contains(db.execSQL[1], "state='completed'") ||
		!strings.Contains(db.execSQL[2], "'running'") ||
		!strings.Contains(db.execSQL[2], "point_in_time") {
		t.Fatalf("intent SQL = %v", db.execSQL)
	}
}

func mustMetadataProtectionService(t *testing.T, result protection.Result) *protection.Service {
	t.Helper()
	policy, err := protection.NewPolicy(
		[]string{"repo-dr"},
		[]protection.ValidationTargetConfig{{ID: "validation-control", Database: recovery.DatabaseControl, Isolated: true}},
	)
	if err != nil {
		t.Fatal(err)
	}
	service, err := protection.NewService(policy, metadataRunner{result: result})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

type metadataRunner struct{ result protection.Result }

func (runner metadataRunner) Run(context.Context, protection.Invocation) (protection.Result, error) {
	return runner.result, nil
}

type metadataDB struct {
	tags     []pgconn.CommandTag
	row      pgx.Row
	execSQL  []string
	execArgs [][]any
}

func (db *metadataDB) Exec(_ context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	db.execSQL = append(db.execSQL, sql)
	db.execArgs = append(db.execArgs, arguments)
	tag := db.tags[0]
	db.tags = db.tags[1:]
	return tag, nil
}

func (db *metadataDB) QueryRow(context.Context, string, ...any) pgx.Row { return db.row }
