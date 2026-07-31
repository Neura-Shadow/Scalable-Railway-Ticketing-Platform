// Package physicalmigration coordinates a bounded, single-region train-run
// move between two independently transactional PostgreSQL booking shards.
package physicalmigration

import (
	"context"
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/google/uuid"
)

var (
	ErrInvalidInput             = errors.New("invalid physical migration input")
	ErrInvalidLimits            = errors.New("invalid physical migration limits")
	ErrInvalidState             = errors.New("invalid physical migration state")
	ErrInvalidBatch             = errors.New("invalid physical migration batch")
	ErrJournalGap               = errors.New("physical migration journal gap")
	ErrValidationFailed         = errors.New("physical migration validation failed")
	ErrCheckpointConflict       = errors.New("physical migration checkpoint conflict")
	ErrReverseMigrationRequired = errors.New("physical migration requires a reverse migration")
	ErrGenerationNotNewer       = errors.New("reverse migration generation must be newer")
	ErrTargetEvidenceMissing    = errors.New("physical migration target-write evidence missing")
	ErrRetentionWindowOpen      = errors.New("physical migration retention window is still open")
	ErrCleanupLimitExceeded     = errors.New("physical migration cleanup limit exceeded")
	ErrCleanupConflict          = errors.New("physical migration cleanup conflicts with reverse migration")
)

type Limits struct {
	OperationTimeout time.Duration
	BaseCopyBatch    int
	JournalBatch     int
	ValidationRows   int
	ValidationTables int
}

type Record struct {
	MigrationID              uuid.UUID
	ParentMigrationID        uuid.UUID
	TrainRunID               uuid.UUID
	SourceShardID            string
	TargetShardID            string
	SourceGeneration         int64
	TargetGeneration         int64
	RetainedTargetGeneration int64
	SourceProtocolVersion    int
	SourceSchemaVersion      int
	TargetProtocolVersion    int
	TargetSchemaVersion      int
	State                    migration.PhysicalState
	BaseCopyCursor           string
	RowsCopied               int64
	SourceJournalStart       int64
	LastReplayedSequence     int64
	FinalSourceSequence      int64
	RowsReplayed             int64
	ValidationVersion        int64
	TargetWriteCount         int64
	ReverseMigration         bool
}

type Change struct {
	MigrationID            uuid.UUID
	ExpectedState          migration.PhysicalState
	NextState              migration.PhysicalState
	ExpectedBaseCopyCursor string
	BaseCopyCursor         string
	RowsCopiedDelta        int64
	SourceJournalStart     *int64
	ExpectedReplaySequence *int64
	LastReplayedSequence   *int64
	RowsReplayedDelta      int64
	FinalSourceSequence    *int64
	ValidationVersionDelta int64
	TargetWriteCount       *int64
	RollbackGeneration     int64
}

type ReversePlan struct {
	OriginalMigrationID  uuid.UUID
	MigrationID          uuid.UUID
	TargetGeneration     int64
	ObservedTargetWrites int64
}

type BaseCopyRequest struct {
	Migration Record
	Cursor    string
	Limit     int
}

type BaseBatch struct {
	ObjectName  string
	Cursor      string
	NextCursor  string
	Rows        int
	Done        bool
	Fingerprint [32]byte
	Payload     any
}

type JournalRequest struct {
	Migration       Record
	AfterSequence   int64
	ThroughSequence int64
	Limit           int
}

type JournalEntry struct {
	ID               uuid.UUID
	Sequence         int64
	TableName        string
	Operation        string
	EntityID         uuid.UUID
	PrimaryKey       []byte
	Metadata         []byte
	ApplyFingerprint [32]byte
	Payload          any
}

type JournalBatch struct {
	Entries        []JournalEntry
	SourceSequence int64
}

type ValidationRequest struct {
	Migration Record
	MaxRows   int
	MaxTables int
	Final     bool
}

type ValidationResult struct {
	Passed       bool
	RowsExamined int
	Tables       int
	Truncated    bool
	Version      int64
}

type ControlStore interface {
	Load(context.Context, uuid.UUID) (Record, error)
	Persist(context.Context, Change) (Record, error)
	BeginDrain(context.Context, Change) (Record, error)
	SwitchAssignment(context.Context, Change) (Record, error)
	Rollback(context.Context, Change) (Record, error)
	CompletionEligible(context.Context, uuid.UUID) error
	Complete(context.Context, Change) (Record, error)
	CreateReverse(context.Context, ReversePlan) (Record, error)
}

// ShardOperations deliberately exposes only database-local operations. No
// method accepts two transactions, preventing XA or an accidental dual write.
type ShardOperations interface {
	Preflight(context.Context, Record) error
	PrepareTarget(context.Context, Record) error
	EnableCapture(context.Context, Record) (int64, error)
	ReadBaseBatch(context.Context, BaseCopyRequest) (BaseBatch, error)
	ApplyBaseBatch(context.Context, Record, BaseBatch) error
	ReadJournal(context.Context, JournalRequest) (JournalBatch, error)
	ApplyJournal(context.Context, Record, JournalEntry) (alreadyApplied bool, err error)
	CaptureOutbox(context.Context, Record, int) error
	Validate(context.Context, ValidationRequest) (ValidationResult, error)
	FenceSource(context.Context, Record) (finalSequence int64, err error)
	EnableTarget(context.Context, Record) error
	TargetWriteCount(context.Context, Record) (int64, error)
	TargetCommandOutboxEvidence(context.Context, Record) (int64, error)
	RollbackBeforeTargetWrites(context.Context, Record, int64) error
	DisableCapture(context.Context, Record) error
	RetainSource(context.Context, Record) error
}

func ApplyChange(record Record, change Change) Record {
	if change.NextState != "" {
		record.State = change.NextState
	}
	if change.BaseCopyCursor != "" {
		record.BaseCopyCursor = change.BaseCopyCursor
	}
	record.RowsCopied += change.RowsCopiedDelta
	if change.SourceJournalStart != nil {
		record.SourceJournalStart = *change.SourceJournalStart
	}
	if change.LastReplayedSequence != nil {
		record.LastReplayedSequence = *change.LastReplayedSequence
	}
	record.RowsReplayed += change.RowsReplayedDelta
	if change.FinalSourceSequence != nil {
		record.FinalSourceSequence = *change.FinalSourceSequence
	}
	record.ValidationVersion += change.ValidationVersionDelta
	if change.TargetWriteCount != nil {
		record.TargetWriteCount = *change.TargetWriteCount
	}
	return record
}
