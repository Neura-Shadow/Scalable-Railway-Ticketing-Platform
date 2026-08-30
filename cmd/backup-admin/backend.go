package main

import (
	"context"
	"encoding/hex"
	"errors"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/protection"
	protectionpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/protection/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	errBackupPrecondition = errors.New("backup operation precondition failed")
	backupOperatorPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$`)
)

type backupProtectionService interface {
	Backup(context.Context, protection.BackupRequest) (protection.BackupEvidence, error)
	Verify(context.Context, protection.VerifyRequest) (protection.VerificationEvidence, error)
	RestoreValidation(context.Context, protection.RestoreRequest) (protection.RestoreEvidence, error)
	PlanExpiration(context.Context, protection.VerificationEvidence) (protection.ExpirationPlan, error)
	Expire(context.Context, protection.ExpirationPlan, protection.ExpirationConfirmation) (protection.ExpirationEvidence, error)
	ReconcileExpiration(context.Context, protection.ExpirationReconciliationRequest) (protection.ExpirationReconciliation, error)
}

type backupMetadataStore interface {
	BeginBackupOperation(context.Context, uuid.UUID, recovery.Database, string, time.Time) error
	CompleteBackupOperation(context.Context, uuid.UUID, protectionpostgres.Artifact, time.Time) error
	RecordBackup(context.Context, protection.BackupEvidence, time.Time) (protectionpostgres.Artifact, bool, error)
	LoadArtifact(context.Context, recovery.Database, string, string) (protectionpostgres.Artifact, error)
	RecordVerification(context.Context, protectionpostgres.Artifact, protection.VerificationEvidence, time.Time) (uuid.UUID, error)
	BeginRestoreValidation(context.Context, uuid.UUID, protectionpostgres.Artifact, string, time.Time, time.Time) error
	CompleteRestoreValidation(context.Context, uuid.UUID, protectionpostgres.Artifact, protection.RestoreEvidence, time.Time) error
	CountArtifacts(context.Context, recovery.Database, string, string) (int, error)
	RecordExpirationPlan(context.Context, protectionpostgres.Artifact, protection.Digest, time.Time) (protectionpostgres.ExpirationRecord, error)
	LoadExpiration(context.Context, uuid.UUID) (protectionpostgres.ExpirationRecord, error)
	RecordExpirationConfirmed(context.Context, protectionpostgres.ExpirationRecord, string, time.Time) error
	RecordExpirationExecuting(context.Context, protectionpostgres.ExpirationRecord, string, time.Time) error
	RecordExpirationCompleted(context.Context, protectionpostgres.ExpirationRecord, protection.ExpirationEvidence, string, time.Time) error
}

type repositoryChecker interface {
	Check(context.Context, recovery.Database, string) error
}

type runtimeBackupBackend struct {
	service  backupProtectionService
	metadata backupMetadataStore
	checker  repositoryChecker
	operator string
	now      func() time.Time
}

func (backend *runtimeBackupBackend) Execute(ctx context.Context, req request) (result, error) {
	if backend == nil || backend.metadata == nil || backend.now == nil || ctx == nil {
		return result{}, errRuntimeWiring
	}
	switch req.Command {
	case "backup-control", "backup-shard":
		if backend.service == nil {
			return result{}, errRuntimeWiring
		}
		if req.OperationID == uuid.Nil || backend.metadata.BeginBackupOperation(
			ctx, req.OperationID, req.Database, req.Repository, backend.now().UTC(),
		) != nil {
			return result{}, errBackupPrecondition
		}
		evidence, err := backend.service.Backup(ctx, protection.BackupRequest{Database: req.Database, Repository: req.Repository})
		if err != nil {
			return result{}, errBackupPrecondition
		}
		artifact, _, err := backend.metadata.RecordBackup(ctx, evidence, backend.now().UTC())
		if err != nil {
			return result{}, errBackupPrecondition
		}
		if backend.metadata.CompleteBackupOperation(ctx, req.OperationID, artifact, backend.now().UTC()) != nil {
			return result{}, errBackupPrecondition
		}
		value := artifactResult(req.Command, artifact, "retained")
		value.OperationID = req.OperationID
		return value, nil
	case "verify-backup":
		artifact, verification, err := backend.verifyArtifact(ctx, req)
		if err != nil {
			return result{}, err
		}
		if _, err := backend.metadata.RecordVerification(ctx, artifact, verification, backend.now().UTC()); err != nil {
			return result{}, errBackupPrecondition
		}
		return artifactResult(req.Command, artifact, "verified"), nil
	case "restore-validation":
		artifact, verification, err := backend.verifyArtifact(ctx, req)
		if err != nil {
			return result{}, err
		}
		if req.DryRun {
			return artifactResult(req.Command, artifact, "verified"), nil
		}
		if _, err := backend.metadata.RecordVerification(ctx, artifact, verification, backend.now().UTC()); err != nil {
			return result{}, errBackupPrecondition
		}
		startedAt := backend.now().UTC()
		if req.OperationID == uuid.Nil || backend.metadata.BeginRestoreValidation(
			ctx, req.OperationID, artifact, req.Target, req.PITRTarget, startedAt,
		) != nil {
			return result{}, errBackupPrecondition
		}
		restored, err := backend.service.RestoreValidation(ctx, protection.RestoreRequest{
			Verification: verification, Target: req.Target, PointInTime: req.PITRTarget,
		})
		if err != nil {
			return result{}, errBackupPrecondition
		}
		if err := backend.metadata.CompleteRestoreValidation(ctx, req.OperationID, artifact, restored, backend.now().UTC()); err != nil {
			return result{}, errBackupPrecondition
		}
		value := artifactResult(req.Command, artifact, "restored")
		value.OperationID = req.OperationID
		return value, nil
	case "list-backups":
		count, err := backend.metadata.CountArtifacts(ctx, req.Database, req.Repository, "")
		if err != nil {
			return result{}, errBackupPrecondition
		}
		return result{Database: req.Database, State: "metadata_inspected", Count: count}, nil
	case "inspect-retention":
		count, err := backend.metadata.CountArtifacts(ctx, req.Database, req.Repository, "retained")
		if err != nil {
			return result{}, errBackupPrecondition
		}
		return result{Database: req.Database, State: "retention_inspected", Count: count}, nil
	case "inspect-wal-archive":
		if backend.checker == nil || backend.checker.Check(ctx, req.Database, req.Repository) != nil {
			return result{}, errBackupPrecondition
		}
		return result{Database: req.Database, State: "archive_checked"}, nil
	case "expire-backup":
		return backend.executeExpiration(ctx, req)
	default:
		return result{}, errArguments
	}
}

func (backend *runtimeBackupBackend) verifyArtifact(
	ctx context.Context,
	req request,
) (protectionpostgres.Artifact, protection.VerificationEvidence, error) {
	if backend.service == nil {
		return protectionpostgres.Artifact{}, protection.VerificationEvidence{}, errRuntimeWiring
	}
	artifact, err := backend.metadata.LoadArtifact(ctx, req.Database, req.Repository, req.BackupSet)
	if err != nil || artifact.RetentionState == "expired" {
		return protectionpostgres.Artifact{}, protection.VerificationEvidence{}, errBackupPrecondition
	}
	verification, err := backend.service.Verify(ctx, protection.VerifyRequest{
		Database: artifact.Database, Repository: artifact.Repository,
		BackupSet: artifact.BackupSet, ExpectedChecksum: artifact.Checksum,
	})
	if err != nil {
		return protectionpostgres.Artifact{}, protection.VerificationEvidence{}, errBackupPrecondition
	}
	return artifact, verification, nil
}

func (backend *runtimeBackupBackend) executeExpiration(ctx context.Context, req request) (result, error) {
	if backend.service == nil {
		return result{}, errRuntimeWiring
	}
	if req.DryRun {
		artifact, verification, err := backend.verifyArtifact(ctx, req)
		if err != nil {
			return result{}, err
		}
		plan, err := backend.service.PlanExpiration(ctx, verification)
		if err != nil {
			return result{}, errBackupPrecondition
		}
		digest := expirationPlanDigest(artifact)
		record, err := backend.metadata.RecordExpirationPlan(ctx, artifact, digest, backend.now().UTC())
		if err != nil || record.ExpirationID == uuid.Nil || plan.BackupSet() != artifact.BackupSet {
			return result{}, errBackupPrecondition
		}
		return result{
			Database: artifact.Database, BackupSet: artifact.BackupSet,
			OperationID: record.ExpirationID, State: "dry_run",
		}, nil
	}
	if !backupOperatorPattern.MatchString(backend.operator) {
		return result{}, errBackupPrecondition
	}
	record, err := backend.metadata.LoadExpiration(ctx, req.ExpirationID)
	if err != nil || record.Artifact.Database != req.Database || record.Artifact.Repository != req.Repository ||
		record.Artifact.BackupSet != req.BackupSet ||
		(record.State != "dry_run" && record.State != "confirmed" && record.State != "executing" && record.State != "expired") ||
		record.Digest != expirationPlanDigest(record.Artifact) {
		return result{}, errBackupPrecondition
	}
	if record.State == "expired" {
		return expirationResult(record), nil
	}
	if record.State == "executing" {
		reconciled, reconcileErr := backend.service.ReconcileExpiration(ctx, expirationReconciliationRequest(record))
		if reconcileErr != nil {
			return result{}, errBackupPrecondition
		}
		if !reconciled.Present() {
			if err := backend.metadata.RecordExpirationCompleted(
				ctx, record, reconciled.Evidence(), backend.operator, backend.now().UTC(),
			); err != nil {
				return result{}, errBackupPrecondition
			}
			return expirationResult(record), nil
		}
	}
	verification, err := backend.service.Verify(ctx, protection.VerifyRequest{
		Database: record.Artifact.Database, Repository: record.Artifact.Repository,
		BackupSet: record.Artifact.BackupSet, ExpectedChecksum: record.Artifact.Checksum,
	})
	if err != nil {
		return result{}, errBackupPrecondition
	}
	plan, err := backend.service.PlanExpiration(ctx, verification)
	if err != nil || plan.BackupSet() != record.Artifact.BackupSet {
		return result{}, errBackupPrecondition
	}
	confirmation, err := protection.ConfirmExpiration(plan, "expire-backup:"+record.Artifact.BackupSet)
	if err != nil {
		return result{}, errBackupPrecondition
	}
	if record.State == "dry_run" {
		if err := backend.metadata.RecordExpirationConfirmed(ctx, record, backend.operator, backend.now().UTC()); err != nil {
			return result{}, errBackupPrecondition
		}
		record.State = "confirmed"
	}
	if record.State == "confirmed" {
		if err := backend.metadata.RecordExpirationExecuting(ctx, record, backend.operator, backend.now().UTC()); err != nil {
			return result{}, errBackupPrecondition
		}
		record.State = "executing"
	}
	evidence, err := backend.service.Expire(ctx, plan, confirmation)
	if err != nil || !evidence.Expired() || evidence.Checksum() != record.Artifact.Checksum ||
		evidence.PlanDigest() != record.Digest {
		return result{}, errBackupPrecondition
	}
	if err := backend.metadata.RecordExpirationCompleted(ctx, record, evidence, backend.operator, backend.now().UTC()); err != nil {
		return result{}, errBackupPrecondition
	}
	return expirationResult(record), nil
}

func expirationReconciliationRequest(record protectionpostgres.ExpirationRecord) protection.ExpirationReconciliationRequest {
	return protection.ExpirationReconciliationRequest{
		Database: record.Artifact.Database, Repository: record.Artifact.Repository,
		BackupSet: record.Artifact.BackupSet, ExpectedChecksum: record.Artifact.Checksum,
		PlanDigest: record.Digest,
	}
}

func expirationResult(record protectionpostgres.ExpirationRecord) result {
	return result{Database: record.Artifact.Database, BackupSet: record.Artifact.BackupSet,
		OperationID: record.ExpirationID, State: "expired"}
}

func expirationPlanDigest(artifact protectionpostgres.Artifact) protection.Digest {
	value := artifact.Database.String() + "|" + artifact.Repository + "|" + artifact.BackupSet + "|" +
		hex.EncodeToString(artifact.Checksum[:])
	return protection.HashEvidence([]byte(value))
}

func artifactResult(_ string, artifact protectionpostgres.Artifact, state string) result {
	return result{Database: artifact.Database, BackupSet: artifact.BackupSet, State: state}
}

func openBackend(ctx context.Context, lookup func(string) (string, bool), req request) (backendService, func(), error) {
	if ctx == nil || lookup == nil {
		return nil, func() {}, errRuntimeWiring
	}
	enabled, _ := lookup("BACKUP_ADMIN_ENABLED")
	if enabled != "true" {
		return nil, func() {}, errRuntimeWiring
	}
	dsn, ok := boundedEnv(lookup, "BACKUP_METADATA_DATABASE_URL")
	if !ok {
		return nil, func() {}, errRuntimeWiring
	}
	regionRaw, regionOK := boundedEnv(lookup, "BACKUP_DEPLOYMENT_REGION")
	epochRaw, epochOK := boundedEnv(lookup, "BACKUP_REGION_EPOCH")
	if !regionOK || !epochOK {
		return nil, func() {}, errRuntimeWiring
	}
	region, err := authority.ParseRegion(regionRaw)
	if err != nil {
		return nil, func() {}, errRuntimeWiring
	}
	epochValue, err := strconv.ParseUint(epochRaw, 10, 64)
	if err != nil {
		return nil, func() {}, errRuntimeWiring
	}
	epoch, err := authority.NewEpoch(epochValue)
	if err != nil {
		return nil, func() {}, errRuntimeWiring
	}
	pool, err := openMetadataPool(ctx, dsn, region, epoch)
	if err != nil {
		return nil, func() {}, errRuntimeWiring
	}
	metadata, err := protectionpostgres.NewMetadataStore(pool)
	if err != nil {
		pool.Close()
		return nil, func() {}, errRuntimeWiring
	}
	backend := &runtimeBackupBackend{metadata: metadata, now: time.Now}
	if operator, present := boundedEnv(lookup, "BACKUP_OPERATOR_ID"); present {
		backend.operator = operator
	}
	if req.Command == "list-backups" || req.Command == "inspect-retention" {
		return backend, pool.Close, nil
	}
	runner, service, err := openProtectionService(lookup, req.Repository)
	if err != nil {
		pool.Close()
		return nil, func() {}, errRuntimeWiring
	}
	backend.service = service
	backend.checker = runner
	return backend, pool.Close, nil
}

func openProtectionService(
	lookup func(string) (string, bool),
	repositoryID string,
) (*protectionpostgres.Runner, *protection.Service, error) {
	configuredRepository, ok := boundedEnv(lookup, "PGBACKREST_REPOSITORY_ID")
	if !ok || configuredRepository != repositoryID {
		return nil, nil, errRuntimeWiring
	}
	repositoryNumberRaw, ok := boundedEnv(lookup, "PGBACKREST_REPOSITORY_NUMBER")
	if !ok {
		return nil, nil, errRuntimeWiring
	}
	repositoryNumber, err := strconv.Atoi(repositoryNumberRaw)
	if err != nil || repositoryNumber < 1 || repositoryNumber > 4 {
		return nil, nil, errRuntimeWiring
	}
	binary, binaryOK := boundedEnv(lookup, "PGBACKREST_BINARY")
	root, rootOK := boundedEnv(lookup, "RESTORE_VALIDATION_ROOT")
	controlStanza, controlOK := boundedEnv(lookup, "PGBACKREST_CONTROL_STANZA")
	shard0Stanza, shard0OK := boundedEnv(lookup, "PGBACKREST_SHARD_0_STANZA")
	shard1Stanza, shard1OK := boundedEnv(lookup, "PGBACKREST_SHARD_1_STANZA")
	if !binaryOK || !rootOK || !controlOK || !shard0OK || !shard1OK || !filepath.IsAbs(root) {
		return nil, nil, errRuntimeWiring
	}
	targets := make([]protection.ValidationTargetConfig, 0, 3)
	targetPaths := make(map[string]string, 3)
	for _, member := range []struct {
		database recovery.Database
		prefix   string
	}{
		{recovery.DatabaseControl, "RESTORE_CONTROL"},
		{recovery.DatabaseShard0, "RESTORE_SHARD_0"},
		{recovery.DatabaseShard1, "RESTORE_SHARD_1"},
	} {
		id, idOK := boundedEnv(lookup, member.prefix+"_TARGET_ID")
		path, pathOK := boundedEnv(lookup, member.prefix+"_TARGET_PATH")
		if !idOK || !pathOK {
			return nil, nil, errRuntimeWiring
		}
		targets = append(targets, protection.ValidationTargetConfig{ID: id, Database: member.database, Isolated: true})
		targetPaths[id] = path
	}
	restoreVerifier, err := protectionpostgres.NewLocalRestoreVerifier("/usr/local/bin/postgres")
	if err != nil {
		return nil, nil, errRuntimeWiring
	}
	runner, err := protectionpostgres.New(protectionpostgres.Config{
		Binary:         binary,
		Stanzas:        recovery.NewDatabaseSet(controlStanza, shard0Stanza, shard1Stanza),
		Repositories:   map[string]int{repositoryID: repositoryNumber},
		ValidationRoot: root, ValidationTargets: targetPaths, MaxOutputBytes: 64 << 10,
		RestoreVerifier: restoreVerifier,
	}, protectionpostgres.OSExecutor{})
	if err != nil {
		return nil, nil, errRuntimeWiring
	}
	policy, err := protection.NewPolicy([]string{repositoryID}, targets)
	if err != nil {
		return nil, nil, errRuntimeWiring
	}
	service, err := protection.NewService(policy, runner)
	if err != nil {
		return nil, nil, errRuntimeWiring
	}
	return runner, service, nil
}

func openMetadataPool(ctx context.Context, dsn string, region authority.Region, epoch authority.Epoch) (*pgxpool.Pool, error) {
	if region.String() == "" || epoch.Uint64() == 0 {
		return nil, errRuntimeWiring
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errRuntimeWiring
	}
	config.MaxConns = 2
	config.MinConns = 0
	config.MaxConnLifetime = 5 * time.Minute
	config.ConnConfig.ConnectTimeout = 3 * time.Second
	config.ConnConfig.RuntimeParams["application_name"] = "railway-backup-admin"
	session, err := postgresx.ParseRegionalSession(
		region.String(), string(authority.RoleActive), strconv.FormatUint(epoch.Uint64(), 10), "true",
	)
	if err != nil || postgresx.ApplyRegionalSession(config.ConnConfig, session) != nil {
		return nil, errRuntimeWiring
	}
	return pgxpool.NewWithConfig(ctx, config)
}

func boundedEnv(lookup func(string) (string, bool), key string) (string, bool) {
	raw, ok := lookup(key)
	if !ok || len(raw) > 4096 || strings.ContainsAny(raw, "\x00\r\n") {
		return "", false
	}
	value := strings.TrimSpace(raw)
	return value, value != ""
}
