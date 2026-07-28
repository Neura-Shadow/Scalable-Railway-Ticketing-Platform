// Package control coordinates bounded train-run shard migrations without
// depending on a particular database adapter.
package control

import (
	"context"
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/google/uuid"
)

var (
	ErrInvalidLimits            = errors.New("invalid migration control limits")
	ErrInvalidInput             = errors.New("invalid migration control input")
	ErrMigrationNotFound        = errors.New("shard migration not found")
	ErrPlanConflict             = errors.New("migration plan conflicts with persisted plan")
	ErrInvalidRecord            = errors.New("invalid persisted migration record")
	ErrInvalidState             = errors.New("operation is not valid in the migration state")
	ErrActiveRouteMismatch      = errors.New("active train-run route does not match migration plan")
	ErrWriteFenceMismatch       = errors.New("write fence does not match migration state")
	ErrShardNotWritable         = errors.New("logical shard is not eligible for assignment")
	ErrInvalidCopyResult        = errors.New("copy adapter returned an invalid bounded result")
	ErrValidationRowCapExceeded = errors.New("validation row cap exceeded")
	ErrInvalidValidation        = errors.New("validation adapter returned an invalid snapshot")
	ErrCutoverValidationFailed  = errors.New("immediate cutover validation failed")
	ErrLocatorRowCapExceeded    = errors.New("locator row cap exceeded")
	ErrTargetWriteEvidence      = errors.New("durable target-write evidence prevents direct rollback")
	ErrRollbackWindowOpen       = errors.New("rollback window is still open")
	ErrRollbackWindowExpired    = errors.New("rollback window has expired")
)

// Limits are hard process-side safety caps. Individual requests must select a
// positive value no larger than the corresponding cap.
type Limits struct {
	MaxBatchSize        int
	MaxCheckpointBytes  int
	MaxOperationTimeout time.Duration
	MaxValidationRows   int64
	MaxLocatorRows      int64
	MaxRollbackWindow   time.Duration
}

// Clock is injected so rollback-window decisions are deterministic and
// independently testable.
type Clock interface {
	Now() time.Time
}

// Record is the durable control-plane aggregate for one train run. Booking
// rows themselves are never represented or deleted by this package.
type Record struct {
	MigrationID        uuid.UUID
	TrainRunID         uuid.UUID
	SourceShard        sharding.ShardID
	TargetShard        sharding.ShardID
	SourceGeneration   sharding.AssignmentGeneration
	TargetGeneration   sharding.AssignmentGeneration
	RollbackGeneration *sharding.AssignmentGeneration
	RollbackWindow     time.Duration
	State              migration.State
	Checkpoint         string
	CopiedRows         int64
	CopyComplete       bool
	LastValidation     *ValidationOutcome
	CreatedAt          time.Time
	UpdatedAt          time.Time
	CutoverAt          *time.Time
	RollbackDeadline   *time.Time
}

func (record Record) SourceRoute() (sharding.ShardRoute, error) {
	return sharding.NewShardRoute(record.TrainRunID, record.SourceShard, record.SourceGeneration)
}

func (record Record) TargetRoute() (sharding.ShardRoute, error) {
	return sharding.NewShardRoute(record.TrainRunID, record.TargetShard, record.TargetGeneration)
}

// PlanInput describes one immutable source-to-target plan. MigrationID is the
// idempotency identity for retries.
type PlanInput struct {
	MigrationID      uuid.UUID
	TrainRunID       uuid.UUID
	SourceShard      sharding.ShardID
	TargetShard      sharding.ShardID
	SourceGeneration sharding.AssignmentGeneration
	TargetGeneration sharding.AssignmentGeneration
	RollbackWindow   time.Duration
	OperationTimeout time.Duration
}

type CopyBatchInput struct {
	MigrationID uuid.UUID
	BatchSize   int
	Timeout     time.Duration
}

// CopyBatchRequest is passed to the storage adapter inside the same
// transaction that advances the checkpoint. Implementations must use
// idempotent upserts and must not enable target writes.
type CopyBatchRequest struct {
	MigrationID uuid.UUID
	TrainRunID  uuid.UUID
	Source      sharding.ShardRoute
	Target      sharding.ShardRoute
	Checkpoint  string
	Limit       int
}

type CopyBatchResult struct {
	NextCheckpoint string
	RowsCopied     int
	Done           bool
}

type ValidateInput struct {
	MigrationID uuid.UUID
	RowCap      int64
	Timeout     time.Duration
}

type ValidationRequest struct {
	MigrationID uuid.UUID
	TrainRunID  uuid.UUID
	Source      sharding.ShardRoute
	Target      sharding.ShardRoute
	RowCap      int64
}

type TableDigest struct {
	Name     string
	Rows     int64
	Checksum string
}

type DatasetDigest struct {
	Tables []TableDigest
}

// ValidationSnapshot is one bounded source/target comparison. Truncated must
// be set whenever the adapter could not finish within RowCap.
type ValidationSnapshot struct {
	Source                     DatasetDigest
	Target                     DatasetDigest
	InvariantViolations        int64
	MissingReservationLocators int64
	MissingTicketOrderLocators int64
	MissingTicketLocators      int64
	RowsExamined               int64
	Truncated                  bool
}

type ValidationOutcome struct {
	Snapshot  ValidationSnapshot
	Passed    bool
	CheckedAt time.Time
}

type ValidateResult struct {
	Record Record
	Passed bool
}

type CutoverInput struct {
	MigrationID      uuid.UUID
	ValidationRowCap int64
	LocatorRowCap    int64
	Timeout          time.Duration
}

type DirectRollbackInput struct {
	MigrationID        uuid.UUID
	RollbackGeneration sharding.AssignmentGeneration
	LocatorRowCap      int64
	Timeout            time.Duration
}

type CompleteInput struct {
	MigrationID uuid.UUID
	Timeout     time.Duration
}

type CleanupEligibilityInput struct {
	MigrationID uuid.UUID
	Timeout     time.Duration
}

// CleanupEligibility is deliberately advisory. This package exposes no delete
// operation; a separately reviewed cleanup executor must consume the result.
type CleanupEligibility struct {
	Eligible   bool
	EligibleAt *time.Time
	Reason     string
}

// Repository must execute the callback in one atomic transaction and roll
// back every callback-side effect when the callback returns an error.
type Repository interface {
	WithinTransaction(context.Context, func(context.Context, Transaction) error) error
}

// Transaction is intentionally schema-agnostic. A PostgreSQL adapter maps the
// fixed ShardID values to allowlisted schema identifiers.
type Transaction interface {
	FindMigrationForUpdate(context.Context, uuid.UUID) (Record, bool, error)
	InsertMigration(context.Context, Record) error
	SaveMigration(context.Context, Record) error
	ActiveRouteForUpdate(context.Context, uuid.UUID) (sharding.ShardRoute, error)
	RequireShardWritableForUpdate(context.Context, sharding.ShardID) error
	WriteFenceEnabledForUpdate(context.Context, sharding.ShardRoute) (bool, error)
	SetWriteFence(context.Context, sharding.ShardRoute, bool) error
	QuiesceWrites(context.Context, sharding.ShardRoute) error
	CopyBatch(context.Context, CopyBatchRequest) (CopyBatchResult, error)
	Validate(context.Context, ValidationRequest) (ValidationSnapshot, error)
	LockLocatorsForUpdate(context.Context, uuid.UUID, int64) (int64, error)
	ActivateRoute(context.Context, sharding.ShardRoute, sharding.ShardRoute) error
	HasDurableTargetWrites(context.Context, sharding.ShardRoute) (bool, error)
}
