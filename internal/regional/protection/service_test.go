package protection_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/protection"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
)

func TestPgBackRestBackupRequiresAllowlistedRepositoryAndEncryptedChecksummedResult(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 8, 11, 7, 0, 0, 0, time.UTC)
	runner := &pgBackRestRunner{result: protection.Result{
		Success:        true,
		BackupSet:      "20260811-070000F",
		Checksum:       protection.HashEvidence([]byte("manifest")),
		Encrypted:      true,
		SourcePosition: mustProtectionPosition(t, 12, 400),
		CompletedAt:    createdAt,
	}}
	service := mustProtectionService(t, runner)

	evidence, err := service.Backup(context.Background(), protection.BackupRequest{
		Database:   recovery.DatabaseControl,
		Repository: "repo-dr",
	})
	if err != nil {
		t.Fatalf("Backup() error = %v", err)
	}
	if evidence.Database() != recovery.DatabaseControl || !evidence.Encrypted() ||
		evidence.Checksum() == (protection.Digest{}) || evidence.BackupSet() != "20260811-070000F" {
		t.Fatalf("Backup() evidence = %+v", evidence)
	}
	if runner.calls != 1 || runner.last.Operation() != protection.OperationBackup {
		t.Fatalf("runner calls/operation = %d/%s", runner.calls, runner.last.Operation())
	}

	_, err = service.Backup(context.Background(), protection.BackupRequest{
		Database:   recovery.DatabaseControl,
		Repository: "request-controlled-repo",
	})
	if !errors.Is(err, protection.ErrRepositoryNotAllowed) {
		t.Fatalf("Backup(unknown repo) error = %v, want ErrRepositoryNotAllowed", err)
	}
	if runner.calls != 1 {
		t.Fatalf("runner calls after rejected repo = %d, want 1", runner.calls)
	}
}

func TestPgBackRestRestoreRunsOnlyAgainstAllowlistedIsolatedValidationTarget(t *testing.T) {
	t.Parallel()

	checksum := protection.HashEvidence([]byte("manifest"))
	pitrTarget := time.Date(2026, 8, 11, 8, 20, 0, 0, time.UTC)
	runner := &pgBackRestRunner{result: protection.Result{
		Success:       true,
		BackupSet:     "20260811-080000F",
		Checksum:      checksum,
		Encrypted:     true,
		CompletedAt:   time.Date(2026, 8, 11, 8, 30, 0, 0, time.UTC),
		SchemaVersion: 11,
		Timeline:      15,
		Reconciled:    true,
		RestoreFacts: protection.RestoreFacts{
			SchemaCurrent: true, Payment: true, Ticket: true, Refund: true,
			Ledger: true, Settlement: true, Regional: true,
		},
		PointInTime: pitrTarget,
	}}
	service := mustProtectionService(t, runner)
	verified := mustVerification(t, service, runner, "20260811-080000F", checksum)
	runner.calls = 0

	_, err := service.RestoreValidation(context.Background(), protection.RestoreRequest{
		Verification: verified,
		Target:       "active-control",
		PointInTime:  pitrTarget,
	})
	if !errors.Is(err, protection.ErrTargetNotIsolated) {
		t.Fatalf("RestoreValidation(active) error = %v, want ErrTargetNotIsolated", err)
	}
	if runner.calls != 0 {
		t.Fatalf("runner calls for active target = %d, want 0", runner.calls)
	}

	evidence, err := service.RestoreValidation(context.Background(), protection.RestoreRequest{
		Verification: verified,
		Target:       "validation-control",
		PointInTime:  pitrTarget,
	})
	if err != nil {
		t.Fatalf("RestoreValidation(isolated) error = %v", err)
	}
	if !evidence.Reconciled() || evidence.SchemaVersion() != 11 || evidence.Target() != "validation-control" {
		t.Fatalf("restore evidence = %+v", evidence)
	}
}

func TestPgBackRestExpirationRequiresBoundDryRunAndExplicitConfirmation(t *testing.T) {
	t.Parallel()

	checksum := protection.HashEvidence([]byte("manifest"))
	runner := &pgBackRestRunner{result: protection.Result{
		Success:     true,
		BackupSet:   "20260811-090000F",
		Checksum:    checksum,
		DryRun:      true,
		CompletedAt: time.Date(2026, 8, 11, 9, 30, 0, 0, time.UTC),
	}}
	service := mustProtectionService(t, runner)
	verified := mustVerification(t, service, runner, "20260811-090000F", checksum)
	runner.calls = 0
	plan, err := service.PlanExpiration(context.Background(), verified)
	if err != nil {
		t.Fatalf("PlanExpiration() error = %v", err)
	}
	if runner.last.Operation() != protection.OperationExpireDryRun {
		t.Fatalf("dry-run operation = %s", runner.last.Operation())
	}

	_, err = service.Expire(context.Background(), plan, protection.ExpirationConfirmation{})
	if !errors.Is(err, protection.ErrDestructiveConfirmation) {
		t.Fatalf("Expire(unconfirmed) error = %v, want ErrDestructiveConfirmation", err)
	}
	if runner.calls != 1 {
		t.Fatalf("runner calls after unconfirmed expire = %d, want 1", runner.calls)
	}
	confirmation, err := protection.ConfirmExpiration(plan, "expire-backup:20260811-090000F")
	if err != nil {
		t.Fatalf("ConfirmExpiration() error = %v", err)
	}
	runner.result = protection.Result{
		Success:     true,
		BackupSet:   "20260811-090000F",
		Checksum:    checksum,
		Expired:     true,
		CompletedAt: time.Date(2026, 8, 11, 9, 31, 0, 0, time.UTC),
	}
	expired, err := service.Expire(context.Background(), plan, confirmation)
	if err != nil {
		t.Fatalf("Expire(confirmed) error = %v", err)
	}
	if !expired.Expired() || runner.last.Operation() != protection.OperationExpireConfirmed {
		t.Fatalf("expiration evidence/operation = %+v/%s", expired, runner.last.Operation())
	}
}

func mustProtectionService(t *testing.T, runner protection.Runner) *protection.Service {
	t.Helper()
	policy, err := protection.NewPolicy(
		[]string{"repo-dr"},
		[]protection.ValidationTargetConfig{
			{ID: "validation-control", Database: recovery.DatabaseControl, Isolated: true},
			{ID: "active-control", Database: recovery.DatabaseControl, Isolated: false},
		},
	)
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	service, err := protection.NewService(policy, runner)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service
}

func mustVerification(
	t *testing.T,
	service *protection.Service,
	runner *pgBackRestRunner,
	backupSet string,
	checksum protection.Digest,
) protection.VerificationEvidence {
	t.Helper()
	original := runner.result
	runner.result = protection.Result{
		Success:            true,
		BackupSet:          backupSet,
		Checksum:           checksum,
		Encrypted:          true,
		RepositoryVerified: true,
		CompletedAt:        time.Date(2026, 8, 11, 8, 15, 0, 0, time.UTC),
	}
	evidence, err := service.Verify(context.Background(), protection.VerifyRequest{
		Database:         recovery.DatabaseControl,
		Repository:       "repo-dr",
		BackupSet:        backupSet,
		ExpectedChecksum: checksum,
	})
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	runner.result = original
	return evidence
}

func mustProtectionPosition(t *testing.T, timeline uint32, wal uint64) recovery.ReplicationPosition {
	t.Helper()
	position, err := recovery.NewReplicationPosition(timeline, wal)
	if err != nil {
		t.Fatalf("NewReplicationPosition() error = %v", err)
	}
	return position
}

type pgBackRestRunner struct {
	result protection.Result
	err    error
	calls  int
	last   protection.Invocation
}

func (runner *pgBackRestRunner) Run(_ context.Context, invocation protection.Invocation) (protection.Result, error) {
	runner.calls++
	runner.last = invocation
	result := runner.result
	if invocation.Operation() == protection.OperationExpireConfirmed || invocation.Operation() == protection.OperationExpireReconcile {
		result.Checksum = invocation.Checksum()
		result.PlanDigest = invocation.PlanDigest()
	}
	return result, runner.err
}
