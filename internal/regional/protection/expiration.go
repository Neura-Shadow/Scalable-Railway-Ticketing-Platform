package protection

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
)

var ErrDestructiveConfirmation = errors.New("explicit backup expiration confirmation required")

type ExpirationPlan struct {
	verification VerificationEvidence
	bound        Digest
}

func (plan ExpirationPlan) BackupSet() string { return plan.verification.backupSet }

type ExpirationConfirmation struct{ bound Digest }

func ConfirmExpiration(plan ExpirationPlan, phrase string) (ExpirationConfirmation, error) {
	if !plan.verification.valid() || !plan.bound.valid() || phrase != "expire-backup:"+plan.verification.backupSet {
		return ExpirationConfirmation{}, ErrDestructiveConfirmation
	}
	return ExpirationConfirmation{bound: plan.bound}, nil
}

type ExpirationEvidence struct {
	database   recovery.Database
	repository string
	backupSet  string
	checksum   Digest
	planDigest Digest
	expired    bool
}

func (evidence ExpirationEvidence) BackupSet() string           { return evidence.backupSet }
func (evidence ExpirationEvidence) Expired() bool               { return evidence.expired }
func (evidence ExpirationEvidence) Database() recovery.Database { return evidence.database }
func (evidence ExpirationEvidence) Repository() string          { return evidence.repository }
func (evidence ExpirationEvidence) Checksum() Digest            { return evidence.checksum }
func (evidence ExpirationEvidence) PlanDigest() Digest          { return evidence.planDigest }

func (service *Service) PlanExpiration(
	ctx context.Context,
	verification VerificationEvidence,
) (ExpirationPlan, error) {
	if ctx == nil || !verification.valid() {
		return ExpirationPlan{}, ErrInvalidResult
	}
	if _, allowed := service.policy.repositories[verification.repository]; !allowed {
		return ExpirationPlan{}, ErrRepositoryNotAllowed
	}
	result, err := service.runner.Run(ctx, Invocation{
		operation:  OperationExpireDryRun,
		database:   verification.database,
		repository: verification.repository,
		backupSet:  verification.backupSet,
	})
	if err != nil {
		return ExpirationPlan{}, fmt.Errorf("%w: %v", ErrExecution, err)
	}
	if !result.Success || !result.DryRun || result.BackupSet != verification.backupSet ||
		result.Checksum != verification.checksum || result.CompletedAt.IsZero() {
		return ExpirationPlan{}, ErrInvalidResult
	}
	bound := expirationDigest(verification)
	return ExpirationPlan{verification: verification, bound: bound}, nil
}

func (service *Service) Expire(
	ctx context.Context,
	plan ExpirationPlan,
	confirmation ExpirationConfirmation,
) (ExpirationEvidence, error) {
	if ctx == nil || !plan.verification.valid() || !plan.bound.valid() || confirmation.bound != plan.bound {
		return ExpirationEvidence{}, ErrDestructiveConfirmation
	}
	result, err := service.runner.Run(ctx, Invocation{
		operation:  OperationExpireConfirmed,
		database:   plan.verification.database,
		repository: plan.verification.repository,
		backupSet:  plan.verification.backupSet,
		checksum:   plan.verification.checksum,
		planDigest: plan.bound,
	})
	if err != nil {
		return ExpirationEvidence{}, fmt.Errorf("%w: %v", ErrExecution, err)
	}
	if !result.Success || !result.Expired || result.BackupSet != plan.verification.backupSet ||
		result.Checksum != plan.verification.checksum || result.PlanDigest != plan.bound ||
		result.BackupPresent || result.CompletedAt.IsZero() {
		return ExpirationEvidence{}, ErrInvalidResult
	}
	return ExpirationEvidence{
		database:   plan.verification.database,
		repository: plan.verification.repository,
		backupSet:  plan.verification.backupSet,
		checksum:   plan.verification.checksum,
		planDigest: plan.bound,
		expired:    true,
	}, nil
}

// ExpirationReconciliationRequest binds crash recovery to the same artifact
// checksum and dry-run plan digest that authorized the destructive operation.
type ExpirationReconciliationRequest struct {
	Database         recovery.Database
	Repository       string
	BackupSet        string
	ExpectedChecksum Digest
	PlanDigest       Digest
}

// ExpirationReconciliation reports the independently observed repository
// postcondition. Present means the executing intent was durable but deletion
// did not run; Expired means deletion ran and only journal completion remains.
type ExpirationReconciliation struct {
	present  bool
	evidence ExpirationEvidence
}

func (reconciliation ExpirationReconciliation) Present() bool { return reconciliation.present }
func (reconciliation ExpirationReconciliation) Evidence() ExpirationEvidence {
	return reconciliation.evidence
}

func (service *Service) ReconcileExpiration(
	ctx context.Context,
	request ExpirationReconciliationRequest,
) (ExpirationReconciliation, error) {
	verification := VerificationEvidence{
		database: request.Database, repository: request.Repository,
		backupSet: request.BackupSet, checksum: request.ExpectedChecksum,
		encrypted: true, repositoryVerified: true,
	}
	if ctx == nil || !verification.valid() || !request.PlanDigest.valid() ||
		request.PlanDigest != expirationDigest(verification) {
		return ExpirationReconciliation{}, ErrInvalidResult
	}
	if _, allowed := service.policy.repositories[request.Repository]; !allowed {
		return ExpirationReconciliation{}, ErrRepositoryNotAllowed
	}
	result, err := service.runner.Run(ctx, Invocation{
		operation: OperationExpireReconcile, database: request.Database,
		repository: request.Repository, backupSet: request.BackupSet,
		checksum: request.ExpectedChecksum, planDigest: request.PlanDigest,
	})
	if err != nil {
		return ExpirationReconciliation{}, fmt.Errorf("%w: %v", ErrExecution, err)
	}
	if !result.Success || result.BackupSet != request.BackupSet ||
		result.Checksum != request.ExpectedChecksum || result.PlanDigest != request.PlanDigest ||
		result.CompletedAt.IsZero() || result.BackupPresent == result.Expired {
		return ExpirationReconciliation{}, ErrInvalidResult
	}
	evidence := ExpirationEvidence{
		database: request.Database, repository: request.Repository, backupSet: request.BackupSet,
		checksum: request.ExpectedChecksum, planDigest: request.PlanDigest, expired: result.Expired,
	}
	return ExpirationReconciliation{present: result.BackupPresent, evidence: evidence}, nil
}

func expirationDigest(verification VerificationEvidence) Digest {
	value := verification.database.String() + "|" + verification.repository + "|" +
		verification.backupSet + "|" + hex.EncodeToString(verification.checksum[:])
	return HashEvidence([]byte(value))
}
