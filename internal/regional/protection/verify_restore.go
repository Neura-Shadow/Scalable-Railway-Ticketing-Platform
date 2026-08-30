package protection

import (
	"context"
	"fmt"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
)

type VerifyRequest struct {
	Database         recovery.Database
	Repository       string
	BackupSet        string
	ExpectedChecksum Digest
}

type VerificationEvidence struct {
	database           recovery.Database
	repository         string
	backupSet          string
	checksum           Digest
	encrypted          bool
	repositoryVerified bool
}

func (evidence VerificationEvidence) Database() recovery.Database { return evidence.database }
func (evidence VerificationEvidence) Repository() string          { return evidence.repository }
func (evidence VerificationEvidence) BackupSet() string           { return evidence.backupSet }
func (evidence VerificationEvidence) Checksum() Digest            { return evidence.checksum }
func (evidence VerificationEvidence) Encrypted() bool             { return evidence.encrypted }
func (evidence VerificationEvidence) RepositoryVerified() bool    { return evidence.repositoryVerified }

func (evidence VerificationEvidence) valid() bool {
	return backupSetPattern.MatchString(evidence.backupSet) && evidence.checksum.valid() &&
		evidence.encrypted && evidence.repositoryVerified
}

func (service *Service) Verify(ctx context.Context, request VerifyRequest) (VerificationEvidence, error) {
	if ctx == nil {
		return VerificationEvidence{}, ErrInvalidService
	}
	if _, err := recovery.ParseDatabase(request.Database.String()); err != nil ||
		!backupSetPattern.MatchString(request.BackupSet) || !request.ExpectedChecksum.valid() {
		return VerificationEvidence{}, ErrInvalidResult
	}
	if _, allowed := service.policy.repositories[request.Repository]; !allowed {
		return VerificationEvidence{}, ErrRepositoryNotAllowed
	}
	result, err := service.runner.Run(ctx, Invocation{
		operation:  OperationVerify,
		database:   request.Database,
		repository: request.Repository,
		backupSet:  request.BackupSet,
	})
	if err != nil {
		return VerificationEvidence{}, fmt.Errorf("%w: %v", ErrExecution, err)
	}
	if !result.Success || result.BackupSet != request.BackupSet || result.Checksum != request.ExpectedChecksum ||
		!result.Encrypted || !result.RepositoryVerified || result.CompletedAt.IsZero() {
		return VerificationEvidence{}, ErrInvalidResult
	}
	return VerificationEvidence{
		database:           request.Database,
		repository:         request.Repository,
		backupSet:          request.BackupSet,
		checksum:           result.Checksum,
		encrypted:          true,
		repositoryVerified: true,
	}, nil
}

type RestoreRequest struct {
	Verification VerificationEvidence
	Target       string
	PointInTime  time.Time
}

type RestoreEvidence struct {
	database      recovery.Database
	target        string
	backupSet     string
	checksum      Digest
	schemaVersion int
	timeline      uint32
	reconciled    bool
	facts         RestoreFacts
	pointInTime   time.Time
}

func (evidence RestoreEvidence) Database() recovery.Database { return evidence.database }
func (evidence RestoreEvidence) Target() string              { return evidence.target }
func (evidence RestoreEvidence) BackupSet() string           { return evidence.backupSet }
func (evidence RestoreEvidence) Checksum() Digest            { return evidence.checksum }
func (evidence RestoreEvidence) SchemaVersion() int          { return evidence.schemaVersion }
func (evidence RestoreEvidence) Timeline() uint32            { return evidence.timeline }
func (evidence RestoreEvidence) Reconciled() bool            { return evidence.reconciled }
func (evidence RestoreEvidence) PointInTime() time.Time      { return evidence.pointInTime }
func (evidence RestoreEvidence) Facts() RestoreFacts         { return evidence.facts }

func (service *Service) RestoreValidation(ctx context.Context, request RestoreRequest) (RestoreEvidence, error) {
	if ctx == nil || !request.Verification.valid() || request.PointInTime.IsZero() ||
		request.PointInTime.Location() != time.UTC {
		return RestoreEvidence{}, ErrInvalidResult
	}
	target, allowed := service.policy.targets[request.Target]
	if !allowed {
		return RestoreEvidence{}, ErrTargetNotAllowed
	}
	if !target.isolated {
		return RestoreEvidence{}, ErrTargetNotIsolated
	}
	if target.database != request.Verification.database {
		return RestoreEvidence{}, ErrTargetNotAllowed
	}
	result, err := service.runner.Run(ctx, Invocation{
		operation:   OperationRestoreValidation,
		database:    request.Verification.database,
		repository:  request.Verification.repository,
		target:      request.Target,
		backupSet:   request.Verification.backupSet,
		pointInTime: request.PointInTime,
	})
	if err != nil {
		return RestoreEvidence{}, fmt.Errorf("%w: %v", ErrExecution, err)
	}
	if !result.Success || result.BackupSet != request.Verification.backupSet ||
		result.Checksum != request.Verification.checksum || !result.Encrypted ||
		result.CompletedAt.IsZero() || result.SchemaVersion <= 0 || result.Timeline == 0 || !result.Reconciled ||
		!result.RestoreFacts.validFor(request.Verification.database) ||
		!result.PointInTime.Equal(request.PointInTime) {
		return RestoreEvidence{}, ErrInvalidResult
	}
	return RestoreEvidence{
		database:      request.Verification.database,
		target:        request.Target,
		backupSet:     result.BackupSet,
		checksum:      result.Checksum,
		schemaVersion: result.SchemaVersion,
		timeline:      result.Timeline,
		reconciled:    true,
		facts:         result.RestoreFacts,
		pointInTime:   request.PointInTime,
	}, nil
}
