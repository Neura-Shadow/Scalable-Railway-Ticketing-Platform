package postgres

import (
	"bytes"
	"context"
	"errors"
	"math"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/protection"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrInvalidMetadataStore = errors.New("backup metadata store invalid")
	ErrMetadataConflict     = errors.New("backup metadata conflict")
)

type MetadataDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Artifact struct {
	BackupID       uuid.UUID
	Database       recovery.Database
	Repository     string
	BackupSet      string
	Checksum       protection.Digest
	Encrypted      bool
	SourcePosition recovery.ReplicationPosition
	RetentionState string
	CreatedAt      time.Time
}

type MetadataStore struct{ db MetadataDB }

func NewMetadataStore(db MetadataDB) (*MetadataStore, error) {
	if db == nil {
		return nil, ErrInvalidMetadataStore
	}
	return &MetadataStore{db: db}, nil
}

func (store *MetadataStore) BeginBackupOperation(
	ctx context.Context,
	operationID uuid.UUID,
	database recovery.Database,
	repository string,
	requestedAt time.Time,
) error {
	if store == nil || store.db == nil || ctx == nil || operationID == uuid.Nil ||
		repository == "" || requestedAt.IsZero() {
		return ErrInvalidMetadataStore
	}
	if _, err := recovery.ParseDatabase(database.String()); err != nil {
		return ErrInvalidMetadataStore
	}
	tag, err := store.db.Exec(ctx, insertBackupOperationSQL,
		operationID, database.String(), repository, requestedAt.UTC(),
	)
	if err != nil || tag.RowsAffected() != 1 {
		return ErrMetadataConflict
	}
	return nil
}

func (store *MetadataStore) CompleteBackupOperation(
	ctx context.Context,
	operationID uuid.UUID,
	artifact Artifact,
	completedAt time.Time,
) error {
	if store == nil || store.db == nil || ctx == nil || operationID == uuid.Nil ||
		artifact.BackupID == uuid.Nil || completedAt.IsZero() {
		return ErrInvalidMetadataStore
	}
	tag, err := store.db.Exec(ctx, completeBackupOperationSQL,
		operationID, artifact.BackupID, artifact.Database.String(), artifact.Repository, completedAt.UTC(),
	)
	if err != nil || tag.RowsAffected() != 1 {
		return ErrMetadataConflict
	}
	return nil
}

func (store *MetadataStore) RecordBackup(
	ctx context.Context,
	evidence protection.BackupEvidence,
	createdAt time.Time,
) (Artifact, bool, error) {
	if store == nil || store.db == nil || ctx == nil || createdAt.IsZero() ||
		evidence.BackupSet() == "" || evidence.Repository() == "" || !evidence.Encrypted() ||
		evidence.Checksum() == (protection.Digest{}) || evidence.SourcePosition().Timeline() == 0 ||
		evidence.SourcePosition().WAL() == 0 || evidence.SourcePosition().WAL() > math.MaxInt64 {
		return Artifact{}, false, ErrInvalidMetadataStore
	}
	if _, err := recovery.ParseDatabase(evidence.Database().String()); err != nil {
		return Artifact{}, false, ErrInvalidMetadataStore
	}
	backupID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(
		evidence.Repository()+"|"+evidence.Database().String()+"|"+evidence.BackupSet(),
	))
	checksum := evidence.Checksum()
	tag, err := store.db.Exec(ctx, insertArtifactSQL,
		backupID, evidence.Database().String(), evidence.Repository(), evidence.BackupSet(), checksum[:],
		true, int64(evidence.SourcePosition().Timeline()), int64(evidence.SourcePosition().WAL()), createdAt.UTC(),
	)
	if err != nil {
		return Artifact{}, false, err
	}
	artifact := Artifact{
		BackupID: backupID, Database: evidence.Database(), Repository: evidence.Repository(),
		BackupSet: evidence.BackupSet(), Checksum: checksum, Encrypted: true,
		SourcePosition: evidence.SourcePosition(), RetentionState: "retained", CreatedAt: createdAt.UTC(),
	}
	if tag.RowsAffected() == 1 {
		return artifact, true, nil
	}
	if tag.RowsAffected() != 0 {
		return Artifact{}, false, ErrMetadataConflict
	}
	existing, err := store.LoadArtifact(ctx, evidence.Database(), evidence.Repository(), evidence.BackupSet())
	if err != nil || !sameArtifact(existing, artifact) {
		return Artifact{}, false, ErrMetadataConflict
	}
	return existing, false, nil
}

func (store *MetadataStore) RecordVerification(
	ctx context.Context,
	artifact Artifact,
	evidence protection.VerificationEvidence,
	verifiedAt time.Time,
) (uuid.UUID, error) {
	if store == nil || store.db == nil || ctx == nil || artifact.BackupID == uuid.Nil || verifiedAt.IsZero() ||
		evidence.Database() != artifact.Database || evidence.Repository() != artifact.Repository ||
		evidence.BackupSet() != artifact.BackupSet || evidence.Checksum() != artifact.Checksum ||
		!evidence.Encrypted() || !evidence.RepositoryVerified() {
		return uuid.Nil, ErrInvalidMetadataStore
	}
	verificationID := uuid.New()
	checksum := evidence.Checksum()
	tag, err := store.db.Exec(ctx, insertVerificationSQL,
		verificationID, artifact.BackupID, checksum[:], verifiedAt.UTC(),
	)
	if err != nil || tag.RowsAffected() != 1 {
		return uuid.Nil, ErrMetadataConflict
	}
	return verificationID, nil
}

func (store *MetadataStore) BeginRestoreValidation(
	ctx context.Context,
	restoreID uuid.UUID,
	artifact Artifact,
	target string,
	pointInTime time.Time,
	startedAt time.Time,
) error {
	if store == nil || store.db == nil || ctx == nil || artifact.BackupID == uuid.Nil ||
		restoreID == uuid.Nil || target == "" || pointInTime.IsZero() || pointInTime.Location() != time.UTC || startedAt.IsZero() {
		return ErrInvalidMetadataStore
	}
	tag, err := store.db.Exec(ctx, insertRestoreValidationIntentSQL,
		restoreID, artifact.BackupID, target, artifact.Database.String(), pointInTime, startedAt.UTC(),
	)
	if err != nil || tag.RowsAffected() != 1 {
		return ErrMetadataConflict
	}
	return nil
}

func (store *MetadataStore) CompleteRestoreValidation(
	ctx context.Context,
	restoreID uuid.UUID,
	artifact Artifact,
	evidence protection.RestoreEvidence,
	completedAt time.Time,
) error {
	if store == nil || store.db == nil || ctx == nil || restoreID == uuid.Nil || artifact.BackupID == uuid.Nil ||
		completedAt.IsZero() || evidence.Database() != artifact.Database || evidence.BackupSet() != artifact.BackupSet ||
		evidence.Checksum() != artifact.Checksum || evidence.Target() == "" || evidence.PointInTime().IsZero() ||
		evidence.SchemaVersion() <= 0 || evidence.Timeline() == 0 || !evidence.Reconciled() {
		return ErrInvalidMetadataStore
	}
	tag, err := store.db.Exec(ctx, completeRestoreValidationSQL,
		restoreID, artifact.BackupID, evidence.Target(), evidence.PointInTime(), evidence.SchemaVersion(),
		int64(evidence.Timeline()), completedAt.UTC(),
	)
	if err != nil || tag.RowsAffected() != 1 {
		return ErrMetadataConflict
	}
	return nil
}

func (store *MetadataStore) CountArtifacts(
	ctx context.Context,
	database recovery.Database,
	repository, retentionState string,
) (int, error) {
	if store == nil || store.db == nil || ctx == nil || repository == "" ||
		(retentionState != "" && retentionState != "retained" && retentionState != "expiration_planned" && retentionState != "expired") {
		return 0, ErrInvalidMetadataStore
	}
	if _, err := recovery.ParseDatabase(database.String()); err != nil {
		return 0, ErrInvalidMetadataStore
	}
	var count int64
	if err := store.db.QueryRow(ctx, countArtifactsSQL, database.String(), repository, retentionState).Scan(&count); err != nil ||
		count < 0 || count > 1_000_000 {
		return 0, ErrInvalidMetadataStore
	}
	return int(count), nil
}

type ExpirationRecord struct {
	ExpirationID uuid.UUID
	Artifact     Artifact
	Digest       protection.Digest
	State        string
}

func (store *MetadataStore) RecordExpirationPlan(
	ctx context.Context,
	artifact Artifact,
	digest protection.Digest,
	plannedAt time.Time,
) (ExpirationRecord, error) {
	if store == nil || store.db == nil || ctx == nil || artifact.BackupID == uuid.Nil ||
		digest == (protection.Digest{}) || plannedAt.IsZero() || artifact.RetentionState != "retained" {
		return ExpirationRecord{}, ErrInvalidMetadataStore
	}
	expirationID := uuid.New()
	tag, err := store.db.Exec(ctx, insertExpirationPlanSQL,
		expirationID, artifact.BackupID, digest[:], plannedAt.UTC(),
	)
	if err != nil || tag.RowsAffected() != 1 {
		return ExpirationRecord{}, ErrMetadataConflict
	}
	return ExpirationRecord{ExpirationID: expirationID, Artifact: artifact, Digest: digest, State: "dry_run"}, nil
}

func (store *MetadataStore) LoadExpiration(ctx context.Context, expirationID uuid.UUID) (ExpirationRecord, error) {
	if store == nil || store.db == nil || ctx == nil || expirationID == uuid.Nil {
		return ExpirationRecord{}, ErrInvalidMetadataStore
	}
	var (
		record           ExpirationRecord
		rawDatabase      string
		checksum, digest []byte
		timeline, wal    int64
	)
	if err := store.db.QueryRow(ctx, loadExpirationSQL, expirationID).Scan(
		&record.ExpirationID, &record.Artifact.BackupID, &rawDatabase, &record.Artifact.Repository,
		&record.Artifact.BackupSet, &checksum, &record.Artifact.Encrypted, &timeline, &wal,
		&record.Artifact.RetentionState, &record.Artifact.CreatedAt, &digest, &record.State,
	); err != nil || record.ExpirationID != expirationID || len(checksum) != len(protection.Digest{}) ||
		len(digest) != len(protection.Digest{}) || !record.Artifact.Encrypted || timeline <= 0 ||
		timeline > math.MaxUint32 || wal <= 0 {
		return ExpirationRecord{}, ErrInvalidMetadataStore
	}
	database, err := recovery.ParseDatabase(rawDatabase)
	if err != nil {
		return ExpirationRecord{}, ErrInvalidMetadataStore
	}
	position, err := recovery.NewReplicationPosition(uint32(timeline), uint64(wal))
	if err != nil {
		return ExpirationRecord{}, ErrInvalidMetadataStore
	}
	record.Artifact.Database = database
	record.Artifact.SourcePosition = position
	copy(record.Artifact.Checksum[:], checksum)
	copy(record.Digest[:], digest)
	return record, nil
}

func (store *MetadataStore) RecordExpirationCompleted(
	ctx context.Context,
	record ExpirationRecord,
	evidence protection.ExpirationEvidence,
	operator string,
	completedAt time.Time,
) error {
	if store == nil || store.db == nil || ctx == nil || record.ExpirationID == uuid.Nil ||
		record.Artifact.BackupID == uuid.Nil || record.State != "executing" || operator == "" || completedAt.IsZero() ||
		!evidence.Expired() || evidence.Database() != record.Artifact.Database ||
		evidence.Repository() != record.Artifact.Repository || evidence.BackupSet() != record.Artifact.BackupSet ||
		evidence.Checksum() != record.Artifact.Checksum || evidence.PlanDigest() != record.Digest {
		return ErrInvalidMetadataStore
	}
	tag, err := store.db.Exec(ctx, completeExpirationSQL,
		record.ExpirationID, record.Artifact.BackupID, record.Digest[:], operator, completedAt.UTC(),
	)
	if err != nil || tag.RowsAffected() != 1 {
		return ErrMetadataConflict
	}
	return nil
}

func (store *MetadataStore) RecordExpirationExecuting(
	ctx context.Context,
	record ExpirationRecord,
	operator string,
	executingAt time.Time,
) error {
	if store == nil || store.db == nil || ctx == nil || record.ExpirationID == uuid.Nil ||
		record.Artifact.BackupID == uuid.Nil || record.State != "confirmed" || operator == "" || executingAt.IsZero() {
		return ErrInvalidMetadataStore
	}
	tag, err := store.db.Exec(ctx, executeExpirationSQL,
		record.ExpirationID, record.Artifact.BackupID, record.Digest[:], operator, executingAt.UTC(),
	)
	if err != nil || tag.RowsAffected() != 1 {
		return ErrMetadataConflict
	}
	return nil
}

func (store *MetadataStore) RecordExpirationConfirmed(
	ctx context.Context,
	record ExpirationRecord,
	operator string,
	confirmedAt time.Time,
) error {
	if store == nil || store.db == nil || ctx == nil || record.ExpirationID == uuid.Nil ||
		record.Artifact.BackupID == uuid.Nil || record.State != "dry_run" || operator == "" || confirmedAt.IsZero() {
		return ErrInvalidMetadataStore
	}
	tag, err := store.db.Exec(ctx, confirmExpirationSQL,
		record.ExpirationID, record.Artifact.BackupID, record.Digest[:], operator, confirmedAt.UTC(),
	)
	if err != nil || tag.RowsAffected() != 1 {
		return ErrMetadataConflict
	}
	return nil
}

func (store *MetadataStore) LoadArtifact(
	ctx context.Context,
	database recovery.Database,
	repository, backupSet string,
) (Artifact, error) {
	if store == nil || store.db == nil || ctx == nil || repository == "" || backupSet == "" {
		return Artifact{}, ErrInvalidMetadataStore
	}
	if _, err := recovery.ParseDatabase(database.String()); err != nil {
		return Artifact{}, ErrInvalidMetadataStore
	}
	var (
		artifact    Artifact
		rawDatabase string
		checksum    []byte
		timeline    int64
		wal         int64
	)
	if err := store.db.QueryRow(ctx, loadArtifactSQL, database.String(), repository, backupSet).Scan(
		&artifact.BackupID, &rawDatabase, &artifact.Repository, &artifact.BackupSet, &checksum,
		&artifact.Encrypted, &timeline, &wal, &artifact.RetentionState, &artifact.CreatedAt,
	); err != nil || artifact.BackupID == uuid.Nil || rawDatabase != database.String() ||
		len(checksum) != len(protection.Digest{}) || !artifact.Encrypted || timeline <= 0 ||
		timeline > math.MaxUint32 || wal <= 0 ||
		artifact.CreatedAt.IsZero() {
		return Artifact{}, ErrInvalidMetadataStore
	}
	copy(artifact.Checksum[:], checksum)
	position, err := recovery.NewReplicationPosition(uint32(timeline), uint64(wal))
	if err != nil {
		return Artifact{}, ErrInvalidMetadataStore
	}
	artifact.Database = database
	artifact.SourcePosition = position
	artifact.CreatedAt = artifact.CreatedAt.UTC()
	return artifact, nil
}

func sameArtifact(left, right Artifact) bool {
	return left.BackupID == right.BackupID && left.Database == right.Database &&
		left.Repository == right.Repository && left.BackupSet == right.BackupSet &&
		bytes.Equal(left.Checksum[:], right.Checksum[:]) && left.Encrypted == right.Encrypted &&
		left.SourcePosition == right.SourcePosition && left.RetentionState == right.RetentionState
}

const insertArtifactSQL = `
INSERT INTO public.backup_artifacts(
 backup_id,database_id,repository_id,backup_set,checksum,encrypted,
 source_timeline,source_wal,retention_state,created_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'retained',$9)
ON CONFLICT DO NOTHING`

const insertBackupOperationSQL = `
INSERT INTO public.backup_operations(
 operation_id,operation_kind,database_id,repository_id,state,requested_at
) VALUES($1,'backup',$2,$3,'planned',$4)`

const completeBackupOperationSQL = `
UPDATE public.backup_operations
   SET backup_id=$2,state='completed',completed_at=$5,bounded_error_category=NULL
 WHERE operation_id=$1 AND database_id=$3 AND repository_id=$4 AND state='planned'`

const loadArtifactSQL = `
SELECT backup_id,database_id,repository_id,backup_set,checksum,encrypted,
       source_timeline,source_wal,retention_state,created_at
FROM public.backup_artifacts
WHERE database_id=$1 AND repository_id=$2 AND backup_set=$3`

const insertVerificationSQL = `
INSERT INTO public.backup_verifications(
 verification_id,backup_id,state,checksum,verifier_kind,verified_at,bounded_error_category
) VALUES($1,$2,'passed',$3,'pgbackrest_verify',$4,NULL)`

const insertRestoreValidationIntentSQL = `
INSERT INTO public.restore_validations(
 restore_validation_id,backup_id,target_id,database_id,state,point_in_time,
 reconciled,started_at
) VALUES($1,$2,$3,$4,'running',$5,false,$6)`

const completeRestoreValidationSQL = `
UPDATE public.restore_validations
   SET state='passed',schema_version=$5,timeline=$6,reconciled=true,
       completed_at=$7,bounded_error_category=NULL
 WHERE restore_validation_id=$1 AND backup_id=$2 AND target_id=$3
   AND point_in_time=$4 AND state='running'`

const countArtifactsSQL = `
SELECT count(*)
FROM public.backup_artifacts
WHERE database_id=$1 AND repository_id=$2
  AND ($3='' OR retention_state=$3)`

const insertExpirationPlanSQL = `
WITH artifact AS (
    UPDATE public.backup_artifacts
       SET retention_state='expiration_planned'
     WHERE backup_id=$2 AND retention_state='retained'
     RETURNING backup_id
)
INSERT INTO public.backup_expiration_operations(
 expiration_id,backup_id,dry_run_digest,dry_run_at,state
)
SELECT $1,backup_id,$3,$4,'dry_run' FROM artifact`

const loadExpirationSQL = `
SELECT expiration.expiration_id,artifact.backup_id,artifact.database_id,
       artifact.repository_id,artifact.backup_set,artifact.checksum,artifact.encrypted,
       artifact.source_timeline,artifact.source_wal,artifact.retention_state,artifact.created_at,
       expiration.dry_run_digest,expiration.state
FROM public.backup_expiration_operations AS expiration
JOIN public.backup_artifacts AS artifact ON artifact.backup_id=expiration.backup_id
WHERE expiration.expiration_id=$1`

const completeExpirationSQL = `
WITH expiration AS (
    UPDATE public.backup_expiration_operations
       SET state='expired',completed_at=$5,bounded_error_category=NULL
     WHERE expiration_id=$1 AND backup_id=$2 AND dry_run_digest=$3
       AND confirmed_by=$4 AND state='executing'
     RETURNING backup_id
)
UPDATE public.backup_artifacts
   SET retention_state='expired',expired_at=$5
 WHERE backup_id=$2 AND backup_id IN (SELECT backup_id FROM expiration)
   AND retention_state='expiration_planned'`

const executeExpirationSQL = `
UPDATE public.backup_expiration_operations
   SET state='executing',execution_started_at=$5,bounded_error_category=NULL
 WHERE expiration_id=$1 AND backup_id=$2 AND dry_run_digest=$3
   AND state='confirmed' AND confirmed_by=$4`

const confirmExpirationSQL = `
UPDATE public.backup_expiration_operations
   SET confirmed_by=$4,confirmed_at=$5,state='confirmed',bounded_error_category=NULL
 WHERE expiration_id=$1 AND backup_id=$2 AND dry_run_digest=$3 AND state='dry_run'`
