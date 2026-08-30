// Package protection exposes one concrete pgBackRest-oriented protection
// interface for the fixed regional PostgreSQL topology.
package protection

import (
	"context"
	"crypto/sha256"
	"errors"
	"regexp"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
)

var (
	ErrInvalidPolicy        = errors.New("invalid pgBackRest protection policy")
	ErrInvalidService       = errors.New("invalid pgBackRest protection service")
	ErrRepositoryNotAllowed = errors.New("pgBackRest repository is not allowlisted")
	ErrTargetNotAllowed     = errors.New("restore validation target is not allowlisted")
	ErrTargetNotIsolated    = errors.New("restore validation target is not isolated")
	ErrInvalidResult        = errors.New("invalid pgBackRest normalized result")
	ErrExecution            = errors.New("pgBackRest execution failed")
)

var (
	identityPattern  = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
	backupSetPattern = regexp.MustCompile(`^[0-9]{8}-[0-9]{6}F(?:_[0-9]{8}-[0-9]{6}[DI])?$`)
)

type Digest [sha256.Size]byte

func HashEvidence(value []byte) Digest { return sha256.Sum256(value) }
func (digest Digest) valid() bool      { return digest != Digest{} }

type Operation string

const (
	OperationBackup            Operation = "backup"
	OperationVerify            Operation = "verify"
	OperationRestoreValidation Operation = "restore-validation"
	OperationExpireDryRun      Operation = "expire-dry-run"
	OperationExpireConfirmed   Operation = "expire-confirmed"
	OperationExpireReconcile   Operation = "expire-reconcile"
)

// Invocation is constructed only by Service after policy resolution. It is a
// closed pgBackRest operation, not a generic executable command.
type Invocation struct {
	operation   Operation
	database    recovery.Database
	repository  string
	target      string
	backupSet   string
	pointInTime time.Time
	checksum    Digest
	planDigest  Digest
}

func (invocation Invocation) Operation() Operation        { return invocation.operation }
func (invocation Invocation) Database() recovery.Database { return invocation.database }
func (invocation Invocation) Repository() string          { return invocation.repository }
func (invocation Invocation) Target() string              { return invocation.target }
func (invocation Invocation) BackupSet() string           { return invocation.backupSet }
func (invocation Invocation) PointInTime() time.Time      { return invocation.pointInTime }
func (invocation Invocation) Checksum() Digest            { return invocation.checksum }
func (invocation Invocation) PlanDigest() Digest          { return invocation.planDigest }

// Result is the normalized pgBackRest adapter result. Raw stdout, paths,
// credentials, hosts, and commands do not cross this seam.
type Result struct {
	Success            bool
	BackupSet          string
	Checksum           Digest
	Encrypted          bool
	SourcePosition     recovery.ReplicationPosition
	CompletedAt        time.Time
	SchemaVersion      int
	Timeline           uint32
	Reconciled         bool
	DryRun             bool
	Expired            bool
	RepositoryVerified bool
	PointInTime        time.Time
	PlanDigest         Digest
	BackupPresent      bool
	RestoreFacts       RestoreFacts
}

// RestoreFacts are independently observed from the booted isolated database;
// pgBackRest command output cannot assert any of these application invariants.
type RestoreFacts struct {
	SchemaCurrent bool
	Payment       bool
	Ticket        bool
	Refund        bool
	Ledger        bool
	Settlement    bool
	Regional      bool
}

func (facts RestoreFacts) validFor(database recovery.Database) bool {
	if !facts.SchemaCurrent || !facts.Payment || !facts.Ticket || !facts.Refund || !facts.Regional {
		return false
	}
	if database == recovery.DatabaseControl {
		return facts.Ledger && facts.Settlement
	}
	return database == recovery.DatabaseShard0 || database == recovery.DatabaseShard1
}

type Runner interface {
	Run(context.Context, Invocation) (Result, error)
}

type ValidationTargetConfig struct {
	ID       string
	Database recovery.Database
	Isolated bool
}

type validationTarget struct {
	database recovery.Database
	isolated bool
}

type Policy struct {
	repositories map[string]struct{}
	targets      map[string]validationTarget
}

func NewPolicy(repositories []string, targets []ValidationTargetConfig) (Policy, error) {
	if len(repositories) == 0 || len(repositories) > 4 || len(targets) == 0 || len(targets) > 6 {
		return Policy{}, ErrInvalidPolicy
	}
	policy := Policy{
		repositories: make(map[string]struct{}, len(repositories)),
		targets:      make(map[string]validationTarget, len(targets)),
	}
	for _, repository := range repositories {
		if !identityPattern.MatchString(repository) {
			return Policy{}, ErrInvalidPolicy
		}
		if _, duplicate := policy.repositories[repository]; duplicate {
			return Policy{}, ErrInvalidPolicy
		}
		policy.repositories[repository] = struct{}{}
	}
	for _, target := range targets {
		if !identityPattern.MatchString(target.ID) {
			return Policy{}, ErrInvalidPolicy
		}
		if _, err := recovery.ParseDatabase(target.Database.String()); err != nil {
			return Policy{}, ErrInvalidPolicy
		}
		if _, duplicate := policy.targets[target.ID]; duplicate {
			return Policy{}, ErrInvalidPolicy
		}
		policy.targets[target.ID] = validationTarget{database: target.Database, isolated: target.Isolated}
	}
	return policy, nil
}

type Service struct {
	policy Policy
	runner Runner
}

func NewService(policy Policy, runner Runner) (*Service, error) {
	if len(policy.repositories) == 0 || len(policy.targets) == 0 || runner == nil {
		return nil, ErrInvalidService
	}
	return &Service{policy: policy, runner: runner}, nil
}
