package postgres_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/protection"
	protectionpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/protection/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
)

func TestPgBackRestRunnerUsesFixedArgumentVectorAndNormalizesBoundedJSON(t *testing.T) {
	t.Parallel()

	executor := &fakeExecutor{outputs: [][]byte{nil, nativeInfo("20260811-120000F")}}
	runner := mustPgBackRestRunner(t, executor)
	policy, err := protection.NewPolicy(
		[]string{"repo-dr"},
		[]protection.ValidationTargetConfig{{ID: "validation-control", Database: recovery.DatabaseControl, Isolated: true}},
	)
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	service, err := protection.NewService(policy, runner)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	evidence, err := service.Backup(context.Background(), protection.BackupRequest{
		Database: recovery.DatabaseControl, Repository: "repo-dr",
	})
	if err != nil {
		t.Fatalf("Backup() error = %v", err)
	}
	if evidence.BackupSet() != "20260811-120000F" || evidence.Checksum() == (protection.Digest{}) || executor.calls != 2 {
		t.Fatalf("evidence/calls = %s/%d", evidence.BackupSet(), executor.calls)
	}
	backupArgs := strings.Join(executor.argumentVectors[0], " ")
	for _, required := range []string{"--stanza=railway-control", "--repo=1", "backup", "--type=full"} {
		if !strings.Contains(backupArgs, required) {
			t.Fatalf("backup arguments %q missing %q", backupArgs, required)
		}
	}
	if strings.Contains(backupArgs, "--output=") {
		t.Fatalf("backup uses unsupported output option: %q", backupArgs)
	}
	infoArgs := strings.Join(executor.argumentVectors[1], " ")
	for _, required := range []string{"--stanza=railway-control", "--repo=1", "info", "--output=json"} {
		if !strings.Contains(infoArgs, required) {
			t.Fatalf("info arguments %q missing %q", infoArgs, required)
		}
	}
	if strings.Contains(infoArgs, "sh -c") || strings.Contains(infoArgs, "cmd /c") || executor.binary != "pgbackrest" {
		t.Fatalf("unsafe execution binary/arguments = %q/%q", executor.binary, infoArgs)
	}
}

func TestPgBackRestRunnerBindsCanonicalPITRTargetToFixedRestoreArguments(t *testing.T) {
	t.Parallel()

	executor := &fakeExecutor{}
	runner := mustPgBackRestRunner(t, executor)
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
	verified := mustVerified(t, service, executor, protection.Digest{})
	executor.argumentVectors = nil
	executor.calls = 0
	executor.outputs = [][]byte{nil, nativeInfo("20260811-120000F")}
	target := time.Date(2026, 8, 11, 12, 3, 4, 500, time.UTC)
	evidence, err := service.RestoreValidation(context.Background(), protection.RestoreRequest{
		Verification: verified, Target: "validation-control", PointInTime: target,
	})
	if err != nil || !evidence.Reconciled() || evidence.SchemaVersion() != 11 || evidence.Timeline() != 17 {
		t.Fatalf("RestoreValidation() evidence=%+v error=%v", evidence, err)
	}
	if executor.calls != 2 {
		t.Fatalf("restore calls = %d, want 2", executor.calls)
	}
	joined := strings.Join(executor.argumentVectors[0], " ")
	for _, required := range []string{
		"restore", "--set=20260811-120000F", "--type=time",
		"--target=2026-08-11T12:03:04.0000005Z", "--target-action=promote",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("restore arguments %q missing %q", joined, required)
		}
	}
}

func TestPgBackRestRunnerRestoreFailsClosedWhenIndependentFactsAreMissing(t *testing.T) {
	t.Parallel()

	executor := &fakeExecutor{}
	runner, err := protectionpostgres.New(protectionpostgres.Config{
		Binary: "pgbackrest", Stanzas: recovery.NewDatabaseSet("railway-control", "railway-shard-0", "railway-shard-1"),
		Repositories: map[string]int{"repo-dr": 1}, ValidationRoot: validationRoot(),
		ValidationTargets: map[string]string{"validation-control": validationTarget()},
		MaxOutputBytes:    64 << 10, RestoreVerifier: invalidRestoreVerifier{},
	}, executor)
	if err != nil {
		t.Fatal(err)
	}
	policy, _ := protection.NewPolicy([]string{"repo-dr"}, []protection.ValidationTargetConfig{{ID: "validation-control", Database: recovery.DatabaseControl, Isolated: true}})
	service, _ := protection.NewService(policy, runner)
	verified := mustVerified(t, service, executor, protection.Digest{})
	executor.outputs = [][]byte{nil, nativeInfo("20260811-120000F")}
	_, err = service.RestoreValidation(context.Background(), protection.RestoreRequest{Verification: verified, Target: "validation-control", PointInTime: time.Date(2026, 8, 11, 12, 3, 4, 0, time.UTC)})
	if err == nil {
		t.Fatal("RestoreValidation() accepted missing ledger, settlement, and regional evidence")
	}
}

func TestPgBackRestRunnerConfirmsExpirationOnlyAfterRepositoryAbsence(t *testing.T) {
	t.Parallel()

	executor := &fakeExecutor{}
	runner := mustPgBackRestRunner(t, executor)
	policy, _ := protection.NewPolicy([]string{"repo-dr"}, []protection.ValidationTargetConfig{{ID: "validation-control", Database: recovery.DatabaseControl, Isolated: true}})
	service, _ := protection.NewService(policy, runner)
	verified := mustVerified(t, service, executor, protection.Digest{})
	executor.outputs = [][]byte{nil, nativeInfo("20260811-120000F")}
	plan, err := service.PlanExpiration(context.Background(), verified)
	if err != nil {
		t.Fatal(err)
	}
	confirmation, _ := protection.ConfirmExpiration(plan, "expire-backup:20260811-120000F")
	executor.argumentVectors = nil
	executor.calls = 0
	executor.outputs = [][]byte{nil, nativeInfoWithoutBackups()}
	evidence, err := service.Expire(context.Background(), plan, confirmation)
	if err != nil || !evidence.Expired() || evidence.Checksum() == (protection.Digest{}) || evidence.PlanDigest() == (protection.Digest{}) {
		t.Fatalf("Expire() evidence=%+v error=%v", evidence, err)
	}
	if executor.calls != 2 || !strings.Contains(strings.Join(executor.argumentVectors[0], " "), "expire --set=20260811-120000F") ||
		!strings.Contains(strings.Join(executor.argumentVectors[1], " "), "info --output=json") {
		t.Fatalf("expiration command/postcondition calls=%d args=%v", executor.calls, executor.argumentVectors)
	}
}

func TestPgBackRestRunnerAcceptsOnlyNamedBinaryOrSecretWrapper(t *testing.T) {
	t.Parallel()

	for _, binary := range []string{"/usr/bin/pgbackrest", "/etc/railway/pgbackrest-secret.sh"} {
		config := protectionpostgres.Config{
			Binary:       binary,
			Stanzas:      recovery.NewDatabaseSet("railway-control", "railway-shard-0", "railway-shard-1"),
			Repositories: map[string]int{"repo-dr": 1}, ValidationRoot: validationRoot(),
			ValidationTargets: map[string]string{"validation-control": validationTarget()},
			MaxOutputBytes:    64 << 10,
			RestoreVerifier:   validRestoreVerifier{},
		}
		if _, err := protectionpostgres.New(config, &fakeExecutor{}); err != nil {
			t.Fatalf("New(%q) error = %v", binary, err)
		}
	}
}

func TestPgBackRestRunnerRejectsValidationTargetOutsideItsConfiguredAllowlist(t *testing.T) {
	t.Parallel()

	executor := &fakeExecutor{}
	runner := mustPgBackRestRunner(t, executor)
	policy, err := protection.NewPolicy(
		[]string{"repo-dr"},
		[]protection.ValidationTargetConfig{{ID: "other-validation", Database: recovery.DatabaseControl, Isolated: true}},
	)
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	service, err := protection.NewService(policy, runner)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	checksum := protection.HashEvidence([]byte("manifest"))
	verified := mustVerified(t, service, executor, checksum)
	executor.calls = 0
	_, err = service.RestoreValidation(context.Background(), protection.RestoreRequest{
		Verification: verified,
		Target:       "other-validation",
	})
	if err == nil || executor.calls != 0 {
		t.Fatalf("RestoreValidation() error/calls = %v/%d, want rejected/0", err, executor.calls)
	}
}

func TestPgBackRestRunnerCheckUsesFixedBoundedArgumentVector(t *testing.T) {
	t.Parallel()

	executor := &fakeExecutor{outputs: [][]byte{[]byte(`{"ignored":true}`)}}
	runner := mustPgBackRestRunner(t, executor)
	if err := runner.Check(context.Background(), recovery.DatabaseShard0, "repo-dr"); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	joined := strings.Join(executor.argumentVectors[0], " ")
	for _, required := range []string{"--stanza=railway-shard-0", "--repo=1", "check"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("arguments %q missing %q", joined, required)
		}
	}
	if strings.Contains(joined, "--output=") || strings.Contains(joined, "sh -c") || strings.Contains(joined, "cmd /c") ||
		executor.binary != "pgbackrest" || executor.limit != 64<<10 {
		t.Fatalf("unsafe or unbounded execution binary/arguments/limit = %q/%q/%d", executor.binary, joined, executor.limit)
	}
}

func mustPgBackRestRunner(t *testing.T, executor protectionpostgres.Executor) *protectionpostgres.Runner {
	t.Helper()
	runner, err := protectionpostgres.New(protectionpostgres.Config{
		Binary:         "pgbackrest",
		Stanzas:        recovery.NewDatabaseSet("railway-control", "railway-shard-0", "railway-shard-1"),
		Repositories:   map[string]int{"repo-dr": 1},
		ValidationRoot: validationRoot(),
		ValidationTargets: map[string]string{
			"validation-control": validationTarget(),
		},
		MaxOutputBytes:  64 << 10,
		RestoreVerifier: validRestoreVerifier{},
	}, executor)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return runner
}

type validRestoreVerifier struct{}

func validationRoot() string {
	return filepath.Join(os.TempDir(), "pgbackrest-validation")
}

func validationTarget() string {
	return filepath.Join(validationRoot(), "control")
}

func (validRestoreVerifier) Observe(context.Context, protectionpostgres.RestoreObservationRequest) (protectionpostgres.RestoreObservation, error) {
	return protectionpostgres.RestoreObservation{
		SchemaVersion: 11, Timeline: 17,
		Facts: protection.RestoreFacts{SchemaCurrent: true, Payment: true, Ticket: true, Refund: true, Ledger: true, Settlement: true, Regional: true},
	}, nil
}

type invalidRestoreVerifier struct{}

func (invalidRestoreVerifier) Observe(context.Context, protectionpostgres.RestoreObservationRequest) (protectionpostgres.RestoreObservation, error) {
	return protectionpostgres.RestoreObservation{SchemaVersion: 11, Timeline: 17, Facts: protection.RestoreFacts{SchemaCurrent: true, Payment: true, Ticket: true, Refund: true}}, nil
}

func mustVerified(
	t *testing.T,
	service *protection.Service,
	executor *fakeExecutor,
	_ protection.Digest,
) protection.VerificationEvidence {
	t.Helper()
	executor.outputs = [][]byte{nil, nativeInfo("20260811-120000F")}
	backup, err := service.Backup(context.Background(), protection.BackupRequest{
		Database: recovery.DatabaseControl, Repository: "repo-dr",
	})
	if err != nil {
		t.Fatalf("Backup() error = %v", err)
	}
	executor.outputs = [][]byte{nil, nativeInfo("20260811-120000F")}
	evidence, err := service.Verify(context.Background(), protection.VerifyRequest{
		Database: recovery.DatabaseControl, Repository: "repo-dr",
		BackupSet: "20260811-120000F", ExpectedChecksum: backup.Checksum(),
	})
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	return evidence
}

type fakeExecutor struct {
	outputs         [][]byte
	err             error
	calls           int
	binary          string
	argumentVectors [][]string
	limit           int
}

func (executor *fakeExecutor) Execute(_ context.Context, binary string, arguments []string, limit int) ([]byte, error) {
	executor.calls++
	executor.binary = binary
	executor.argumentVectors = append(executor.argumentVectors, append([]string(nil), arguments...))
	executor.limit = limit
	if executor.err != nil {
		return nil, executor.err
	}
	if len(executor.outputs) == 0 {
		return nil, nil
	}
	output := executor.outputs[0]
	executor.outputs = executor.outputs[1:]
	return output, nil
}

func nativeInfo(label string) []byte {
	return []byte(`[{"archive":[{"database":{"id":1,"repo-key":1},"id":"17-1","max":"000000110000000000000001","min":"000000110000000000000001"}],"backup":[{"archive":{"start":"000000110000000000000001","stop":"000000110000000000000001"},"backrest":{"format":5,"version":"2.59.0"},"database":{"id":1,"repo-key":1},"error":false,"label":"` + label + `","lsn":{"start":"0/100","stop":"0/1F4"},"repo":{"cipher":"aes-256-cbc","key":1},"timestamp":{"start":1786449600,"stop":1786449660},"type":"full"}],"cipher":"aes-256-cbc","name":"railway-control","status":{"code":0,"message":"ok"}}]`)
}

func nativeInfoWithoutBackups() []byte {
	return []byte(`[{"archive":[],"backup":[],"cipher":"aes-256-cbc","name":"railway-control","status":{"code":0,"message":"ok"}}]`)
}
