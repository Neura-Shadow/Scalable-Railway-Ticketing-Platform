package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRebuildTrainRunDefaultsToReadOnlyDryRunWithoutDatabaseConnection(t *testing.T) {
	var stdout, stderr bytes.Buffer
	lookup := func(key string) (string, bool) {
		if key == "DATABASE_URL" {
			return "postgres://unused.example/railway", true
		}
		return "", false
	}
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
	lookup := func(key string) (string, bool) { return "postgres://unused.example/railway", key == "DATABASE_URL" }
	exitCode := run(context.Background(), []string{"rebuild-all", "--batch-size", "101"}, lookup, &stdout, &stderr)
	if exitCode != 1 || !strings.Contains(stderr.String(), "admin command failed") {
		t.Fatalf("unbounded exit/stdout/stderr = %d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestReplayOutboxRejectsUnboundedBatchBeforeOpeningDatabase(t *testing.T) {
	var stdout, stderr bytes.Buffer
	lookup := func(key string) (string, bool) { return "postgres://unused.example/railway", key == "DATABASE_URL" }
	exitCode := run(context.Background(), []string{"replay-outbox", "--batch-size", "101"}, lookup, &stdout, &stderr)
	if exitCode != 1 || !strings.Contains(stderr.String(), "admin command failed") {
		t.Fatalf("unbounded replay exit/stdout/stderr = %d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestReconcileRejectsUnboundedBatchBeforeOpeningDatabase(t *testing.T) {
	var stdout, stderr bytes.Buffer
	lookup := func(key string) (string, bool) { return "postgres://unused.example/railway", key == "DATABASE_URL" }
	exitCode := run(context.Background(), []string{"reconcile", "--limit", "101"}, lookup, &stdout, &stderr)
	if exitCode != 1 || !strings.Contains(stderr.String(), "admin command failed") {
		t.Fatalf("unbounded reconcile exit/stdout/stderr = %d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
}
