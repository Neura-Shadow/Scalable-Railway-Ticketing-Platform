package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/google/uuid"
)

const (
	testTrainRunID  = "11111111-1111-4111-8111-111111111111"
	testMigrationID = "22222222-2222-4222-8222-222222222222"
)

func TestExactCommandSurfaceIsAccepted(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "list shards", args: []string{"list-shards"}},
		{name: "list assignments", args: []string{"list-assignments"}},
		{name: "inspect train run", args: []string{"inspect-train-run", "--train-run-id", testTrainRunID}},
		{name: "plan migration", args: []string{"plan-migration", "--train-run-id", testTrainRunID, "--target-shard", "shard-1", "--migration-id", testMigrationID, "--dry-run"}},
		{name: "start migration", args: []string{"start-migration", "--migration-id", testMigrationID, "--dry-run"}},
		{name: "resume migration", args: []string{"resume-migration", "--migration-id", testMigrationID, "--dry-run"}},
		{name: "validate migration", args: []string{"validate-migration", "--migration-id", testMigrationID, "--dry-run"}},
		{name: "cutover", args: []string{"cutover", "--migration-id", testMigrationID, "--dry-run"}},
		{name: "rollback", args: []string{"rollback", "--migration-id", testMigrationID, "--dry-run"}},
		{name: "cleanup source", args: []string{"cleanup-source", "--migration-id", testMigrationID, "--dry-run"}},
		{name: "reconcile", args: []string{"reconcile", "--train-run-id", testTrainRunID}},
		{name: "inspect health", args: []string{"inspect-health"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &fakeAdminBackend{}
			var stdout, stderr bytes.Buffer
			exitCode := runWithFactory(
				context.Background(),
				test.args,
				databaseLookup,
				&stdout,
				&stderr,
				fakeFactory(backend),
			)
			if exitCode != 0 || stderr.Len() != 0 {
				t.Fatalf("exit/stdout/stderr = %d/%q/%q", exitCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stdout.String(), `"command":"`+test.args[0]+`"`) ||
				!strings.Contains(stdout.String(), `"status":`) {
				t.Fatalf("unexpected envelope %q", stdout.String())
			}
		})
	}
}

func TestMissingCommandReturnsJSONEnvelope(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exitCode := runWithFactory(
		context.Background(),
		nil,
		databaseLookup,
		&stdout,
		&stderr,
		fakeFactory(&fakeAdminBackend{}),
	)
	if exitCode != 2 || !strings.Contains(stdout.String(), `"command":"shard-admin"`) ||
		!strings.Contains(stdout.String(), `"error":"invalid_arguments"`) {
		t.Fatalf("exit/stdout/stderr=%d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestMutatingCommandsRequireConfirmationBeforeBackendOpen(t *testing.T) {
	tests := [][]string{
		{"plan-migration", "--train-run-id", testTrainRunID, "--target-shard", "shard-0", "--migration-id", testMigrationID},
		{"start-migration", "--migration-id", testMigrationID},
		{"resume-migration", "--migration-id", testMigrationID},
		{"validate-migration", "--migration-id", testMigrationID},
		{"cutover", "--migration-id", testMigrationID},
		{"rollback", "--migration-id", testMigrationID},
		{"cleanup-source", "--migration-id", testMigrationID},
	}

	for _, args := range tests {
		var opened int
		factory := func(context.Context, string, postgresx.RegionalSession) (adminBackend, error) {
			opened++
			return &fakeAdminBackend{}, nil
		}
		var stdout, stderr bytes.Buffer
		exitCode := runWithFactory(context.Background(), args, databaseLookup, &stdout, &stderr, factory)
		if exitCode != 2 || opened != 0 || !strings.Contains(stdout.String(), `"error":"confirmation_required"`) {
			t.Fatalf("args=%v exit/opened/stdout/stderr=%d/%d/%q/%q", args, exitCode, opened, stdout.String(), stderr.String())
		}
	}
}

func TestDryRunAndConfirmAreMutuallyExclusive(t *testing.T) {
	var opened int
	factory := func(context.Context, string, postgresx.RegionalSession) (adminBackend, error) {
		opened++
		return &fakeAdminBackend{}, nil
	}
	var stdout, stderr bytes.Buffer
	exitCode := runWithFactory(context.Background(), []string{
		"cutover", "--migration-id", testMigrationID, "--dry-run", "--confirm",
	}, databaseLookup, &stdout, &stderr, factory)
	if exitCode != 2 || opened != 0 || !strings.Contains(stdout.String(), `"error":"invalid_arguments"`) {
		t.Fatalf("exit/opened/stdout/stderr=%d/%d/%q/%q", exitCode, opened, stdout.String(), stderr.String())
	}
}

func TestBoundsFailBeforeBackendOpen(t *testing.T) {
	tests := [][]string{
		{"list-shards", "--limit", "4"},
		{"list-assignments", "--limit", "1001"},
		{"start-migration", "--migration-id", testMigrationID, "--batch-size", "10001", "--confirm"},
		{"validate-migration", "--migration-id", testMigrationID, "--row-cap", "100001", "--confirm"},
		{"cutover", "--migration-id", testMigrationID, "--locator-row-cap", "100001", "--confirm"},
		{"reconcile", "--train-run-id", testTrainRunID, "--row-cap", "100001"},
		{"inspect-health", "--timeout", "6m"},
	}

	for _, args := range tests {
		var opened int
		factory := func(context.Context, string, postgresx.RegionalSession) (adminBackend, error) {
			opened++
			return &fakeAdminBackend{}, nil
		}
		var stdout, stderr bytes.Buffer
		exitCode := runWithFactory(context.Background(), args, databaseLookup, &stdout, &stderr, factory)
		if exitCode != 2 || opened != 0 || !strings.Contains(stdout.String(), `"error":"invalid_arguments"`) {
			t.Fatalf("args=%v exit/opened/stdout/stderr=%d/%d/%q/%q", args, exitCode, opened, stdout.String(), stderr.String())
		}
	}
}

func TestPlanRejectsNonAllowlistedShardBeforeBackendOpen(t *testing.T) {
	malicious := `booking_shard_0;DROP TABLE users`
	var opened int
	factory := func(context.Context, string, postgresx.RegionalSession) (adminBackend, error) {
		opened++
		return &fakeAdminBackend{}, nil
	}
	var stdout, stderr bytes.Buffer
	exitCode := runWithFactory(context.Background(), []string{
		"plan-migration", "--train-run-id", testTrainRunID, "--target-shard", malicious,
		"--migration-id", testMigrationID, "--dry-run",
	}, databaseLookup, &stdout, &stderr, factory)
	combined := stdout.String() + stderr.String()
	if exitCode != 2 || opened != 0 || !strings.Contains(stdout.String(), `"error":"invalid_arguments"`) ||
		strings.Contains(combined, malicious) || strings.Contains(combined, "DROP TABLE") {
		t.Fatalf("exit/opened/stdout/stderr=%d/%d/%q/%q", exitCode, opened, stdout.String(), stderr.String())
	}
}

func TestDependencyErrorsAreSanitized(t *testing.T) {
	secret := "postgres://operator:secret@example/private booking_shard_0 SELECT passenger_name"
	backend := &fakeAdminBackend{err: errors.New(secret)}
	var stdout, stderr bytes.Buffer
	exitCode := runWithFactory(
		context.Background(),
		[]string{"inspect-health"},
		databaseLookup,
		&stdout,
		&stderr,
		fakeFactory(backend),
	)
	combined := stdout.String() + stderr.String()
	if exitCode != 1 || !strings.Contains(stdout.String(), `"error":"operation_failed"`) ||
		strings.Contains(combined, "secret") || strings.Contains(combined, "booking_shard_0") ||
		strings.Contains(combined, "passenger_name") {
		t.Fatalf("exit/stdout/stderr=%d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestBackendFactoryErrorsAreSanitized(t *testing.T) {
	secret := "postgres://operator:secret@example/private booking_shard_1 SELECT owner_user_id"
	factory := func(context.Context, string, postgresx.RegionalSession) (adminBackend, error) {
		return nil, errors.New(secret)
	}
	var stdout, stderr bytes.Buffer
	exitCode := runWithFactory(
		context.Background(),
		[]string{"inspect-health"},
		databaseLookup,
		&stdout,
		&stderr,
		factory,
	)
	combined := stdout.String() + stderr.String()
	if exitCode != 1 || !strings.Contains(stdout.String(), `"error":"operation_failed"`) ||
		strings.Contains(combined, "secret") || strings.Contains(combined, "booking_shard_1") ||
		strings.Contains(combined, "owner_user_id") {
		t.Fatalf("exit/stdout/stderr=%d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestCanceledContextOverridesCollapsedDependencyError(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	backend := &fakeAdminBackend{err: errors.New("postgres://operator:secret@example/private")}
	var stdout, stderr bytes.Buffer
	exitCode := runWithFactory(
		parent,
		[]string{"inspect-health"},
		databaseLookup,
		&stdout,
		&stderr,
		fakeFactory(backend),
	)
	if exitCode != 1 || !strings.Contains(stdout.String(), `"error":"canceled"`) ||
		strings.Contains(stdout.String()+stderr.String(), "secret") {
		t.Fatalf("exit/stdout/stderr=%d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestReconciliationMismatchUsesViolationEnvelope(t *testing.T) {
	backend := &fakeAdminBackend{err: errReconciliationMismatch}
	var stdout, stderr bytes.Buffer
	exitCode := runWithFactory(
		context.Background(),
		[]string{"reconcile", "--train-run-id", testTrainRunID},
		databaseLookup,
		&stdout,
		&stderr,
		fakeFactory(backend),
	)
	if exitCode != 1 || !strings.Contains(stdout.String(), `"status":"violations"`) ||
		!strings.Contains(stdout.String(), `"error":"reconciliation_mismatch"`) ||
		!strings.Contains(stdout.String(), `"read_only":true`) {
		t.Fatalf("exit/stdout/stderr=%d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestCleanupConfirmRemainsDefaultDenyWithoutExecutor(t *testing.T) {
	backend := &fakeAdminBackend{cleanup: cleanupEligibility{Eligible: true, Reason: "eligible_for_separate_cleanup_executor"}}
	var stdout, stderr bytes.Buffer
	exitCode := runWithFactory(
		context.Background(),
		[]string{"cleanup-source", "--migration-id", testMigrationID, "--confirm"},
		databaseLookup,
		&stdout,
		&stderr,
		fakeFactory(backend),
	)
	if exitCode != 1 || backend.cleanupCalls != 1 ||
		!strings.Contains(stdout.String(), `"status":"blocked"`) ||
		!strings.Contains(stdout.String(), `"error":"cleanup_executor_unavailable"`) {
		t.Fatalf("exit/calls/stdout/stderr=%d/%d/%q/%q", exitCode, backend.cleanupCalls, stdout.String(), stderr.String())
	}
}

func TestMissingDatabaseURLDoesNotEchoConfiguration(t *testing.T) {
	lookup := func(string) (string, bool) { return "", false }
	var stdout, stderr bytes.Buffer
	exitCode := runWithFactory(context.Background(), []string{"list-shards"}, lookup, &stdout, &stderr, fakeFactory(&fakeAdminBackend{}))
	if exitCode != 2 || !strings.Contains(stdout.String(), `"error":"configuration_invalid"`) ||
		strings.Contains(stdout.String()+stderr.String(), "DATABASE_URL=") {
		t.Fatalf("exit/stdout/stderr=%d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestRegionalSessionIsRequiredBeforeBackendConstruction(t *testing.T) {
	lookup := func(key string) (string, bool) {
		if key == "DATABASE_URL" {
			return "postgres://user:do-not-leak@unused.invalid/railway", true
		}
		return "", false
	}
	called := false
	factory := func(context.Context, string, postgresx.RegionalSession) (adminBackend, error) {
		called = true
		return &fakeAdminBackend{}, nil
	}
	var stdout, stderr bytes.Buffer
	exitCode := runWithFactory(context.Background(), []string{"list-shards"}, lookup, &stdout, &stderr, factory)
	if exitCode != 2 || called || !strings.Contains(stdout.String(), `"error":"configuration_invalid"`) ||
		strings.Contains(stdout.String()+stderr.String(), "do-not-leak") {
		t.Fatalf("exit/called/stdout/stderr=%d/%t/%q/%q", exitCode, called, stdout.String(), stderr.String())
	}
}

func TestRegionalSessionIsPassedToBackendFactory(t *testing.T) {
	var got postgresx.RegionalSession
	factory := func(_ context.Context, _ string, session postgresx.RegionalSession) (adminBackend, error) {
		got = session
		return &fakeAdminBackend{}, nil
	}
	var stdout, stderr bytes.Buffer
	exitCode := runWithFactory(context.Background(), []string{"list-shards"}, databaseLookup, &stdout, &stderr, factory)
	want := postgresx.RegionalSession{Region: "region-a", Role: "active", Epoch: 1, WritesEnabled: true}
	if exitCode != 0 || got != want {
		t.Fatalf("exit/session/stdout/stderr=%d/%+v/%q/%q", exitCode, got, stdout.String(), stderr.String())
	}
}

func databaseLookup(key string) (string, bool) {
	values := map[string]string{
		"DATABASE_URL":            "postgres://unused.invalid/railway",
		"DEPLOYMENT_REGION":       "region-a",
		"DEPLOYMENT_ROLE":         "active",
		"REGION_EPOCH":            "1",
		"REGIONAL_WRITES_ENABLED": "true",
	}
	if value, ok := values[key]; ok {
		return value, true
	}
	return "", false
}

func fakeFactory(backend adminBackend) backendFactory {
	return func(context.Context, string, postgresx.RegionalSession) (adminBackend, error) { return backend, nil }
}

type fakeAdminBackend struct {
	err          error
	cleanup      cleanupEligibility
	cleanupCalls int
}

func (backend *fakeAdminBackend) Close() {}

func (backend *fakeAdminBackend) ListShards(context.Context, int) (any, error) {
	return map[string]any{"items": []any{}, "complete": true}, backend.err
}

func (backend *fakeAdminBackend) ListAssignments(context.Context, assignmentListOptions) (any, error) {
	return map[string]any{"items": []any{}, "complete": true}, backend.err
}

func (backend *fakeAdminBackend) InspectTrainRun(context.Context, uuid.UUID) (any, error) {
	return map[string]any{"found": true}, backend.err
}

func (backend *fakeAdminBackend) PreviewPlan(context.Context, planOptions) (any, error) {
	return map[string]any{"would_plan": true}, backend.err
}

func (backend *fakeAdminBackend) Plan(context.Context, planOptions) (any, error) {
	return map[string]any{"state": "planned"}, backend.err
}

func (backend *fakeAdminBackend) InspectMigration(context.Context, uuid.UUID) (any, error) {
	return map[string]any{"state": "planned"}, backend.err
}

func (backend *fakeAdminBackend) CopyBatch(context.Context, copyOptions) (any, error) {
	return map[string]any{"state": "copying"}, backend.err
}

func (backend *fakeAdminBackend) Validate(context.Context, validationOptions) (any, error) {
	return map[string]any{"passed": true}, backend.err
}

func (backend *fakeAdminBackend) Cutover(context.Context, cutoverOptions) (any, error) {
	return map[string]any{"state": "rollback_window"}, backend.err
}

func (backend *fakeAdminBackend) Rollback(context.Context, rollbackOptions) (any, error) {
	return map[string]any{"state": "rolled_back"}, backend.err
}

func (backend *fakeAdminBackend) CleanupEligibility(context.Context, uuid.UUID) (cleanupEligibility, error) {
	backend.cleanupCalls++
	return backend.cleanup, backend.err
}

func (backend *fakeAdminBackend) Reconcile(context.Context, reconcileOptions) (any, error) {
	return map[string]any{"complete": true, "mismatches": 0}, backend.err
}

func (backend *fakeAdminBackend) InspectHealth(context.Context) (any, error) {
	return map[string]any{"ready": true}, backend.err
}
