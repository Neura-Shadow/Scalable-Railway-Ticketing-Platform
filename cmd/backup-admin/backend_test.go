package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/protection"
	protectionpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/protection/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
)

func TestRuntimeBackupBackendExecutesBackupAndPersistsValidatedMetadata(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 17, 0, 0, 0, time.UTC)
	checksum := protection.HashEvidence([]byte("manifest"))
	position, _ := recovery.NewReplicationPosition(9, 1200)
	service := mustRuntimeProtectionService(t, runtimeProtectionRunner{result: protection.Result{
		Success: true, BackupSet: "20260811-170000F", Checksum: checksum, Encrypted: true,
		SourcePosition: position, CompletedAt: now,
	}})
	metadata := &runtimeMetadataStore{}
	backend := &runtimeBackupBackend{service: service, metadata: metadata, now: func() time.Time { return now }}

	got, err := backend.Execute(context.Background(), request{
		Command: "backup-control", Database: recovery.DatabaseControl, Repository: "repo-dr", OperationID: uuid.New(),
	})
	if err != nil || metadata.recordCalls != 1 || got.Database != recovery.DatabaseControl ||
		got.BackupSet != "20260811-170000F" || got.State != "retained" ||
		len(metadata.callOrder) != 3 || metadata.callOrder[0] != "begin" ||
		metadata.callOrder[1] != "artifact" || metadata.callOrder[2] != "complete" {
		t.Fatalf("Execute(backup) = %+v, recordCalls=%d, error=%v", got, metadata.recordCalls, err)
	}
}

func TestOpenBackendBuildsBoundedPgBackRestRuntimeWithoutConnecting(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	values := map[string]string{
		"BACKUP_ADMIN_ENABLED":         "true",
		"BACKUP_METADATA_DATABASE_URL": "postgres://runtime:secret@127.0.0.1:1/backup_metadata?sslmode=disable",
		"BACKUP_DEPLOYMENT_REGION":     "region-a",
		"BACKUP_REGION_EPOCH":          "7",
		"PGBACKREST_REPOSITORY_ID":     "repo-dr",
		"PGBACKREST_REPOSITORY_NUMBER": "1",
		"PGBACKREST_BINARY":            "pgbackrest.exe",
		"PGBACKREST_CONTROL_STANZA":    "railway-control",
		"PGBACKREST_SHARD_0_STANZA":    "railway-shard-0",
		"PGBACKREST_SHARD_1_STANZA":    "railway-shard-1",
		"RESTORE_VALIDATION_ROOT":      root,
		"RESTORE_CONTROL_TARGET_ID":    "validation-control",
		"RESTORE_CONTROL_TARGET_PATH":  filepath.Join(root, "control"),
		"RESTORE_SHARD_0_TARGET_ID":    "validation-shard-0",
		"RESTORE_SHARD_0_TARGET_PATH":  filepath.Join(root, "shard-0"),
		"RESTORE_SHARD_1_TARGET_ID":    "validation-shard-1",
		"RESTORE_SHARD_1_TARGET_PATH":  filepath.Join(root, "shard-1"),
	}
	lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	service, closeBackend, err := openBackend(context.Background(), lookup, request{
		Command: "backup-control", Database: recovery.DatabaseControl, Repository: "repo-dr",
	})
	if err != nil || service == nil || closeBackend == nil {
		t.Fatalf("openBackend() service/close/error = %T/%v/%v", service, closeBackend != nil, err)
	}
	closeBackend()
}

func TestOpenMetadataPoolInstallsActiveAuthorityRuntimeParameters(t *testing.T) {
	t.Parallel()

	region, err := authority.ParseRegion("region-a")
	if err != nil {
		t.Fatal(err)
	}
	epoch, _ := authority.NewEpoch(7)
	pool, err := openMetadataPool(
		context.Background(),
		"postgres://runtime:secret@127.0.0.1:1/control?sslmode=disable",
		region,
		epoch,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	parameters := pool.Config().ConnConfig.RuntimeParams
	for key, want := range map[string]string{
		"railway.deployment_region":       "region-a",
		"railway.deployment_role":         "active",
		"railway.region_epoch":            "7",
		"railway.regional_writes_enabled": "true",
	} {
		if got := parameters[key]; got != want {
			t.Fatalf("runtime parameter %s = %q, want %q", key, got, want)
		}
	}
}

func TestRuntimeBackupBackendExpiresOnlyFromPersistedMatchingDryRun(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 18, 0, 0, 0, time.UTC)
	checksum := protection.HashEvidence([]byte("manifest"))
	position, _ := recovery.NewReplicationPosition(9, 1200)
	artifact := protectionpostgres.Artifact{
		BackupID: uuid.New(), Database: recovery.DatabaseControl, Repository: "repo-dr",
		BackupSet: "20260811-170000F", Checksum: checksum, Encrypted: true,
		SourcePosition: position, RetentionState: "expiration_planned", CreatedAt: now.Add(-time.Hour),
	}
	expirationID := uuid.New()
	runner := &operationProtectionRunner{results: map[protection.Operation]protection.Result{
		protection.OperationVerify: {
			Success: true, BackupSet: artifact.BackupSet, Checksum: checksum, Encrypted: true,
			RepositoryVerified: true, CompletedAt: now,
		},
		protection.OperationExpireDryRun: {
			Success: true, BackupSet: artifact.BackupSet, Checksum: checksum, DryRun: true, CompletedAt: now,
		},
		protection.OperationExpireConfirmed: {
			Success: true, BackupSet: artifact.BackupSet, Checksum: checksum, Expired: true, CompletedAt: now,
		},
	}}
	service := mustRuntimeProtectionService(t, runner)
	metadata := &runtimeMetadataStore{expiration: protectionpostgres.ExpirationRecord{
		ExpirationID: expirationID, Artifact: artifact, Digest: expirationPlanDigest(artifact), State: "dry_run",
	}}
	backend := &runtimeBackupBackend{
		service: service, metadata: metadata, operator: "operator:test", now: func() time.Time { return now },
	}

	got, err := backend.Execute(context.Background(), request{
		Command: "expire-backup", Database: artifact.Database, Repository: artifact.Repository,
		BackupSet: artifact.BackupSet, ExpirationID: expirationID, Confirm: true,
	})
	if err != nil || got.State != "expired" || got.OperationID != expirationID ||
		metadata.executingCalls != 1 || metadata.completedCalls != 1 || runner.calls[protection.OperationVerify] != 1 ||
		runner.calls[protection.OperationExpireDryRun] != 1 || runner.calls[protection.OperationExpireConfirmed] != 1 {
		t.Fatalf("Execute(expire) = %+v, completed=%d, calls=%v, error=%v", got, metadata.completedCalls, runner.calls, err)
	}
}

func TestRuntimeBackupBackendReconcilesRanButJournalFailedWithoutRepeatingDelete(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 18, 30, 0, 0, time.UTC)
	checksum := protection.HashEvidence([]byte("manifest"))
	position, _ := recovery.NewReplicationPosition(9, 1200)
	artifact := protectionpostgres.Artifact{
		BackupID: uuid.New(), Database: recovery.DatabaseControl, Repository: "repo-dr",
		BackupSet: "20260811-170000F", Checksum: checksum, Encrypted: true,
		SourcePosition: position, RetentionState: "expiration_planned", CreatedAt: now.Add(-time.Hour),
	}
	expirationID := uuid.New()
	runner := &operationProtectionRunner{results: map[protection.Operation]protection.Result{
		protection.OperationVerify:          {Success: true, BackupSet: artifact.BackupSet, Checksum: checksum, Encrypted: true, RepositoryVerified: true, CompletedAt: now},
		protection.OperationExpireDryRun:    {Success: true, BackupSet: artifact.BackupSet, Checksum: checksum, DryRun: true, CompletedAt: now},
		protection.OperationExpireConfirmed: {Success: true, BackupSet: artifact.BackupSet, Expired: true, CompletedAt: now},
		protection.OperationExpireReconcile: {Success: true, BackupSet: artifact.BackupSet, Expired: true, CompletedAt: now},
	}}
	metadata := &runtimeMetadataStore{
		expiration:     protectionpostgres.ExpirationRecord{ExpirationID: expirationID, Artifact: artifact, Digest: expirationPlanDigest(artifact), State: "confirmed"},
		completeErrors: []error{errors.New("journal unavailable"), nil},
	}
	backend := &runtimeBackupBackend{service: mustRuntimeProtectionService(t, runner), metadata: metadata, operator: "operator:test", now: func() time.Time { return now }}
	req := request{Command: "expire-backup", Database: artifact.Database, Repository: artifact.Repository, BackupSet: artifact.BackupSet, ExpirationID: expirationID, Confirm: true}

	if _, err := backend.Execute(context.Background(), req); !errors.Is(err, errBackupPrecondition) {
		t.Fatalf("first Execute() error = %v, want journal failure", err)
	}
	if metadata.expiration.State != "executing" || runner.calls[protection.OperationExpireConfirmed] != 1 {
		t.Fatalf("first execution state/calls = %s/%v", metadata.expiration.State, runner.calls)
	}
	got, err := backend.Execute(context.Background(), req)
	if err != nil || got.State != "expired" || runner.calls[protection.OperationExpireConfirmed] != 1 ||
		runner.calls[protection.OperationExpireReconcile] != 1 || metadata.completedCalls != 2 {
		t.Fatalf("reconciled Execute() = %+v calls=%v completed=%d error=%v", got, runner.calls, metadata.completedCalls, err)
	}
}

func TestRuntimeBackupBackendResumesExecutingIntentWhenRepositoryStillContainsSet(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 11, 18, 45, 0, 0, time.UTC)
	checksum := protection.HashEvidence([]byte("manifest"))
	position, _ := recovery.NewReplicationPosition(9, 1200)
	artifact := protectionpostgres.Artifact{BackupID: uuid.New(), Database: recovery.DatabaseControl, Repository: "repo-dr", BackupSet: "20260811-170000F", Checksum: checksum, Encrypted: true, SourcePosition: position, RetentionState: "expiration_planned", CreatedAt: now.Add(-time.Hour)}
	expirationID := uuid.New()
	runner := &operationProtectionRunner{results: map[protection.Operation]protection.Result{
		protection.OperationExpireReconcile: {Success: true, BackupSet: artifact.BackupSet, BackupPresent: true, CompletedAt: now},
		protection.OperationVerify:          {Success: true, BackupSet: artifact.BackupSet, Checksum: checksum, Encrypted: true, RepositoryVerified: true, CompletedAt: now},
		protection.OperationExpireDryRun:    {Success: true, BackupSet: artifact.BackupSet, Checksum: checksum, DryRun: true, CompletedAt: now},
		protection.OperationExpireConfirmed: {Success: true, BackupSet: artifact.BackupSet, Expired: true, CompletedAt: now},
	}}
	metadata := &runtimeMetadataStore{expiration: protectionpostgres.ExpirationRecord{ExpirationID: expirationID, Artifact: artifact, Digest: expirationPlanDigest(artifact), State: "executing"}}
	backend := &runtimeBackupBackend{service: mustRuntimeProtectionService(t, runner), metadata: metadata, operator: "operator:test", now: func() time.Time { return now }}

	got, err := backend.Execute(context.Background(), request{Command: "expire-backup", Database: artifact.Database, Repository: artifact.Repository, BackupSet: artifact.BackupSet, ExpirationID: expirationID, Confirm: true})
	if err != nil || got.State != "expired" || runner.calls[protection.OperationExpireReconcile] != 1 || runner.calls[protection.OperationExpireConfirmed] != 1 {
		t.Fatalf("Execute(resume pending) = %+v calls=%v error=%v", got, runner.calls, err)
	}
}

func TestBoundedEnvRejectsTrimmedControlCharacters(t *testing.T) {
	t.Parallel()

	for name, raw := range map[string]string{
		"newline": "repo-dr\n",
		"nul":     "repo-dr\x00suffix",
	} {
		t.Run(name, func(t *testing.T) {
			lookup := func(string) (string, bool) { return raw, true }
			if _, ok := boundedEnv(lookup, "REPOSITORY"); ok {
				t.Fatalf("boundedEnv(%s) accepted unsafe value", name)
			}
		})
	}
}

func mustRuntimeProtectionService(t *testing.T, runner protection.Runner) *protection.Service {
	t.Helper()
	policy, err := protection.NewPolicy(
		[]string{"repo-dr"},
		[]protection.ValidationTargetConfig{{ID: "validation-control", Database: recovery.DatabaseControl, Isolated: true}},
	)
	if err != nil {
		t.Fatal(err)
	}
	service, err := protection.NewService(policy, runner)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

type runtimeProtectionRunner struct {
	result protection.Result
	err    error
}

func (runner runtimeProtectionRunner) Run(context.Context, protection.Invocation) (protection.Result, error) {
	return runner.result, runner.err
}

type operationProtectionRunner struct {
	results map[protection.Operation]protection.Result
	calls   map[protection.Operation]int
}

func (runner *operationProtectionRunner) Run(_ context.Context, invocation protection.Invocation) (protection.Result, error) {
	if runner.calls == nil {
		runner.calls = make(map[protection.Operation]int)
	}
	runner.calls[invocation.Operation()]++
	result := runner.results[invocation.Operation()]
	if invocation.Operation() == protection.OperationExpireConfirmed || invocation.Operation() == protection.OperationExpireReconcile {
		result.Checksum = invocation.Checksum()
		result.PlanDigest = invocation.PlanDigest()
	}
	return result, nil
}

type runtimeMetadataStore struct {
	recordCalls       int
	completedCalls    int
	executingCalls    int
	backupIntentCalls int
	callOrder         []string
	artifact          protectionpostgres.Artifact
	expiration        protectionpostgres.ExpirationRecord
	completeErrors    []error
}

func (store *runtimeMetadataStore) BeginBackupOperation(context.Context, uuid.UUID, recovery.Database, string, time.Time) error {
	store.backupIntentCalls++
	store.callOrder = append(store.callOrder, "begin")
	return nil
}

func (store *runtimeMetadataStore) CompleteBackupOperation(context.Context, uuid.UUID, protectionpostgres.Artifact, time.Time) error {
	store.callOrder = append(store.callOrder, "complete")
	return nil
}

func (store *runtimeMetadataStore) RecordBackup(_ context.Context, evidence protection.BackupEvidence, at time.Time) (protectionpostgres.Artifact, bool, error) {
	store.recordCalls++
	store.callOrder = append(store.callOrder, "artifact")
	store.artifact = protectionpostgres.Artifact{
		BackupID: uuid.New(), Database: evidence.Database(), Repository: evidence.Repository(),
		BackupSet: evidence.BackupSet(), Checksum: evidence.Checksum(), Encrypted: evidence.Encrypted(),
		SourcePosition: evidence.SourcePosition(), RetentionState: "retained", CreatedAt: at,
	}
	return store.artifact, true, nil
}

func (*runtimeMetadataStore) LoadArtifact(context.Context, recovery.Database, string, string) (protectionpostgres.Artifact, error) {
	return protectionpostgres.Artifact{}, nil
}

func (*runtimeMetadataStore) RecordVerification(context.Context, protectionpostgres.Artifact, protection.VerificationEvidence, time.Time) (uuid.UUID, error) {
	return uuid.New(), nil
}

func (*runtimeMetadataStore) BeginRestoreValidation(context.Context, uuid.UUID, protectionpostgres.Artifact, string, time.Time, time.Time) error {
	return nil
}

func (*runtimeMetadataStore) CompleteRestoreValidation(context.Context, uuid.UUID, protectionpostgres.Artifact, protection.RestoreEvidence, time.Time) error {
	return nil
}

func (*runtimeMetadataStore) CountArtifacts(context.Context, recovery.Database, string, string) (int, error) {
	return 0, nil
}

func (*runtimeMetadataStore) RecordExpirationPlan(context.Context, protectionpostgres.Artifact, protection.Digest, time.Time) (protectionpostgres.ExpirationRecord, error) {
	return protectionpostgres.ExpirationRecord{}, nil
}

func (store *runtimeMetadataStore) LoadExpiration(context.Context, uuid.UUID) (protectionpostgres.ExpirationRecord, error) {
	return store.expiration, nil
}

func (store *runtimeMetadataStore) RecordExpirationCompleted(context.Context, protectionpostgres.ExpirationRecord, protection.ExpirationEvidence, string, time.Time) error {
	store.completedCalls++
	if len(store.completeErrors) > 0 {
		err := store.completeErrors[0]
		store.completeErrors = store.completeErrors[1:]
		if err != nil {
			return err
		}
	}
	store.expiration.State = "expired"
	return nil
}

func (store *runtimeMetadataStore) RecordExpirationExecuting(_ context.Context, record protectionpostgres.ExpirationRecord, _ string, _ time.Time) error {
	store.executingCalls++
	store.expiration = record
	store.expiration.State = "executing"
	return nil
}

func (store *runtimeMetadataStore) RecordExpirationConfirmed(_ context.Context, record protectionpostgres.ExpirationRecord, _ string, _ time.Time) error {
	store.expiration = record
	store.expiration.State = "confirmed"
	return nil
}
