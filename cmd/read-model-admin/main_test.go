package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRebuildTrainRunDefaultsToReadOnlyDryRunWithoutDatabaseConnection(t *testing.T) {
	var stdout, stderr bytes.Buffer
	lookup := regionalAdminLookup
	exitCode := run(context.Background(), []string{
		"rebuild-train-run", "--train-run-id", "66666666-6666-4666-8666-666666666666",
	}, lookup, &stdout, &stderr)
	if exitCode != 0 || !strings.Contains(stdout.String(), `"status":"dry-run"`) ||
		!strings.Contains(stdout.String(), `"read_only":true`) {
		t.Fatalf("dry-run exit/stdout/stderr = %d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestRebuildAllRejectsUnboundedBatchBeforeOpeningDatabase(t *testing.T) {
	var stdout, stderr bytes.Buffer
	lookup := regionalAdminLookup
	exitCode := run(context.Background(), []string{"rebuild-all", "--batch-size", "101"}, lookup, &stdout, &stderr)
	if exitCode != 1 || !strings.Contains(stderr.String(), "admin command failed") {
		t.Fatalf("unbounded exit/stdout/stderr = %d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestReplayOutboxRejectsUnboundedBatchBeforeOpeningDatabase(t *testing.T) {
	var stdout, stderr bytes.Buffer
	lookup := regionalAdminLookup
	exitCode := run(context.Background(), []string{"replay-outbox", "--batch-size", "101"}, lookup, &stdout, &stderr)
	if exitCode != 1 || !strings.Contains(stderr.String(), "admin command failed") {
		t.Fatalf("unbounded replay exit/stdout/stderr = %d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestReconcileRejectsUnboundedBatchBeforeOpeningDatabase(t *testing.T) {
	var stdout, stderr bytes.Buffer
	lookup := regionalAdminLookup
	exitCode := run(context.Background(), []string{"reconcile", "--limit", "101"}, lookup, &stdout, &stderr)
	if exitCode != 1 || !strings.Contains(stderr.String(), "admin command failed") {
		t.Fatalf("unbounded reconcile exit/stdout/stderr = %d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestDatabaseCommandsRequireRegionalSessionWithoutLeakingConfiguration(t *testing.T) {
	lookup := func(key string) (string, bool) {
		values := map[string]string{
			"DATABASE_URL":      "postgres://user:do-not-leak@unused.example/railway",
			"DEPLOYMENT_REGION": "do-not-leak-regional-input",
		}
		value, ok := values[key]
		return value, ok
	}
	var stdout, stderr bytes.Buffer
	exitCode := run(context.Background(), []string{
		"rebuild-train-run", "--train-run-id", "66666666-6666-4666-8666-666666666666",
	}, lookup, &stdout, &stderr)
	if exitCode != 2 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "regional database session is required") {
		t.Fatalf("exit/stdout/stderr = %d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
	if strings.Contains(stderr.String(), "do-not-leak") {
		t.Fatal("configuration error leaked database or regional input")
	}
}

func regionalAdminLookup(key string) (string, bool) {
	values := map[string]string{
		"DATABASE_URL":            "postgres://unused.example/railway",
		"DEPLOYMENT_REGION":       "region-a",
		"DEPLOYMENT_ROLE":         "active",
		"REGION_EPOCH":            "1",
		"REGIONAL_WRITES_ENABLED": "true",
	}
	value, ok := values[key]
	return value, ok
}
