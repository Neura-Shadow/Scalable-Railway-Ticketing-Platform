package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
)

const (
	testMigration = "11111111-1111-4111-8111-111111111111"
	testTrainRun  = "22222222-2222-4222-8222-222222222222"
)

func TestEveryRequiredCommandParses(t *testing.T) {
	tests := map[string][]string{
		"list-shards": {"list-shards"}, "inspect-shard": {"inspect-shard", "--shard-id", "physical-shard-0"},
		"check-schema": {"check-schema", "--shard-id", "physical-shard-1"}, "bootstrap-shard": {"bootstrap-shard", "--shard-id", "physical-shard-0", "--train-run-id", testTrainRun, "--dry-run"},
		"inspect-train-run": {"inspect-train-run", "--train-run-id", testTrainRun}, "plan-migration": {"plan-migration", "--migration-id", testMigration, "--train-run-id", testTrainRun, "--target-shard", "physical-shard-1", "--dry-run"},
		"repair-command": {"repair-command", "--command-id", testMigration, "--dry-run"},
	}
	for _, name := range []string{"enable-capture", "start-base-copy", "resume-base-copy", "replay-journal", "validate-online", "begin-quiesce", "final-catchup", "cutover", "rollback", "start-reverse-migration", "cleanup-source"} {
		tests[name] = []string{name, "--migration-id", testMigration, "--dry-run"}
	}
	tests["inspect-crash-window"] = []string{"inspect-crash-window", "--migration-id", testMigration}
	tests["reconcile"] = []string{"reconcile", "--migration-id", testMigration}
	tests["plan-reverse-migration"] = []string{"plan-reverse-migration", "--migration-id", testMigration, "--reverse-migration-id", testTrainRun, "--generation", "3", "--dry-run"}
	for name, args := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := parse(args); err != nil {
				t.Fatalf("parse(%s): %v", name, err)
			}
		})
	}
}

func TestMutationsRequireExactlyOneExecutionMode(t *testing.T) {
	base := []string{"cutover", "--migration-id", testMigration}
	if _, err := parse(base); err != errConfirmation {
		t.Fatalf("error=%v", err)
	}
	if _, err := parse(append(base, "--dry-run", "--confirm")); err != errConfirmation {
		t.Fatalf("error=%v", err)
	}
}

func TestEveryCommandEnforcesItsExecutionMode(t *testing.T) {
	readCommands := []string{"list-shards", "inspect-shard", "check-schema", "inspect-train-run", "inspect-crash-window", "reconcile"}
	mutationCommands := []string{"bootstrap-shard", "plan-migration", "enable-capture", "start-base-copy", "resume-base-copy", "replay-journal", "validate-online", "begin-quiesce", "final-catchup", "cutover", "rollback", "plan-reverse-migration", "start-reverse-migration", "cleanup-source", "repair-command"}
	for _, command := range readCommands {
		args := validArgsFor(command)
		if _, err := parse(args); err != nil {
			t.Fatalf("read %s rejected: %v", command, err)
		}
		if _, err := parse(append(args, "--confirm")); !errors.Is(err, errArguments) {
			t.Fatalf("read %s accepted --confirm: %v", command, err)
		}
	}
	for _, command := range mutationCommands {
		args := validArgsFor(command)
		if _, err := parse(args); !errors.Is(err, errConfirmation) {
			t.Fatalf("mutation %s without mode: %v", command, err)
		}
		if _, err := parse(append(args, "--dry-run")); err != nil {
			t.Fatalf("mutation %s dry-run rejected: %v", command, err)
		}
		if _, err := parse(append(args, "--confirm")); err != nil {
			t.Fatalf("mutation %s confirm rejected: %v", command, err)
		}
		if _, err := parse(append(args, "--dry-run", "--confirm")); !errors.Is(err, errConfirmation) {
			t.Fatalf("mutation %s accepted both modes: %v", command, err)
		}
	}
}

func validArgsFor(command string) []string {
	args := []string{command}
	switch command {
	case "inspect-shard", "check-schema", "bootstrap-shard":
		return append(args, "--shard-id", "physical-shard-0")
	case "inspect-train-run":
		return append(args, "--train-run-id", testTrainRun)
	case "plan-migration":
		return append(args, "--migration-id", testMigration, "--train-run-id", testTrainRun, "--target-shard", "physical-shard-1")
	case "repair-command":
		return append(args, "--command-id", testMigration)
	case "plan-reverse-migration":
		return append(args, "--migration-id", testMigration, "--reverse-migration-id", testTrainRun, "--generation", "9")
	case "list-shards":
		return args
	default:
		return append(args, "--migration-id", testMigration)
	}
}

func TestPhaseCommandsHaveBoundedPreconditions(t *testing.T) {
	if got := expectedState("begin-quiesce"); len(got) != 1 || got[0] != migration.PhysicalStateDraining {
		t.Fatalf("states=%v", got)
	}
	if got := expectedState("cutover"); len(got) != 4 {
		t.Fatalf("cutover states=%v", got)
	}
}

func TestRunDoesNotPrintConfigurationOrBackendError(t *testing.T) {
	secret := "postgres://operator:very-secret@db/control"
	lookup := func(name string) (string, bool) { return secret, true }
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"list-shards"}, lookup, &stdout, &stderr, func(context.Context, func(string) (string, bool)) (backend, error) { return nil, errUnavailable })
	if code != 1 {
		t.Fatalf("code=%d", code)
	}
	combined := stdout.String() + stderr.String()
	if strings.Contains(combined, secret) || strings.Contains(combined, "very-secret") {
		t.Fatalf("secret leaked: %s", combined)
	}
	var value envelope
	if err := json.Unmarshal(stdout.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	if value.Error != "unavailable" {
		t.Fatalf("error=%q", value.Error)
	}
}

func TestCancellationWinsOverAdapterFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := contextError(ctx, errUnavailable); got != context.Canceled {
		t.Fatalf("error=%v", got)
	}
}

func TestBoundsRejectOversizedOutputAndTimeout(t *testing.T) {
	if _, err := parse([]string{"list-shards", "--limit", "1001"}); err == nil {
		t.Fatal("accepted oversized limit")
	}
	if _, err := parse([]string{"list-shards", "--timeout", (maxTimeout + time.Second).String()}); err == nil {
		t.Fatal("accepted oversized timeout")
	}
}
