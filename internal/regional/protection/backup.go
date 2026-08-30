package protection

import (
	"context"
	"fmt"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
)

type BackupRequest struct {
	Database   recovery.Database
	Repository string
}

type BackupEvidence struct {
	database       recovery.Database
	repository     string
	backupSet      string
	checksum       Digest
	encrypted      bool
	sourcePosition recovery.ReplicationPosition
}

func (evidence BackupEvidence) Database() recovery.Database { return evidence.database }
func (evidence BackupEvidence) Repository() string          { return evidence.repository }
func (evidence BackupEvidence) BackupSet() string           { return evidence.backupSet }
func (evidence BackupEvidence) Checksum() Digest            { return evidence.checksum }
func (evidence BackupEvidence) Encrypted() bool             { return evidence.encrypted }
func (evidence BackupEvidence) SourcePosition() recovery.ReplicationPosition {
	return evidence.sourcePosition
}

func (service *Service) Backup(ctx context.Context, request BackupRequest) (BackupEvidence, error) {
	if ctx == nil {
		return BackupEvidence{}, ErrInvalidService
	}
	if _, err := recovery.ParseDatabase(request.Database.String()); err != nil {
		return BackupEvidence{}, ErrInvalidResult
	}
	if _, allowed := service.policy.repositories[request.Repository]; !allowed {
		return BackupEvidence{}, ErrRepositoryNotAllowed
	}
	result, err := service.runner.Run(ctx, Invocation{
		operation:  OperationBackup,
		database:   request.Database,
		repository: request.Repository,
	})
	if err != nil {
		return BackupEvidence{}, fmt.Errorf("%w: %v", ErrExecution, err)
	}
	if !result.Success || !backupSetPattern.MatchString(result.BackupSet) ||
		!result.Checksum.valid() || !result.Encrypted || result.CompletedAt.IsZero() ||
		result.SourcePosition.Timeline() == 0 || result.SourcePosition.WAL() == 0 {
		return BackupEvidence{}, ErrInvalidResult
	}
	return BackupEvidence{
		database:       request.Database,
		repository:     request.Repository,
		backupSet:      result.BackupSet,
		checksum:       result.Checksum,
		encrypted:      result.Encrypted,
		sourcePosition: result.SourcePosition,
	}, nil
}
