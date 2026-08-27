package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control"
	controlpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	shardreconcile "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/reconcile"
	"github.com/google/uuid"
)

func TestExistingPlanInputAllowsExactIdempotentReplay(t *testing.T) {
	record := control.Record{
		MigrationID:      uuid.MustParse(testMigrationID),
		TrainRunID:       uuid.MustParse(testTrainRunID),
		SourceShard:      sharding.ShardLegacy,
		TargetShard:      sharding.ShardOne,
		SourceGeneration: mustGeneration(t, 4),
		TargetGeneration: mustGeneration(t, 5),
		RollbackWindow:   10 * time.Minute,
		State:            migration.StateCopying,
	}
	input, err := existingPlanInput(record, planOptions{
		MigrationID:    record.MigrationID,
		TrainRunID:     record.TrainRunID,
		TargetShard:    record.TargetShard,
		RollbackWindow: record.RollbackWindow,
		Timeout:        defaultCommandTimeout,
	})
	if err != nil {
		t.Fatalf("existingPlanInput() error = %v", err)
	}
	if input.SourceGeneration != record.SourceGeneration || input.TargetGeneration != record.TargetGeneration ||
		input.SourceShard != record.SourceShard || input.TargetShard != record.TargetShard {
		t.Fatalf("existingPlanInput() = %+v, want persisted route", input)
	}
}

func TestExistingPlanInputRejectsChangedReplay(t *testing.T) {
	record := control.Record{
		MigrationID:      uuid.MustParse(testMigrationID),
		TrainRunID:       uuid.MustParse(testTrainRunID),
		SourceShard:      sharding.ShardLegacy,
		TargetShard:      sharding.ShardOne,
		SourceGeneration: mustGeneration(t, 1),
		TargetGeneration: mustGeneration(t, 2),
		RollbackWindow:   5 * time.Minute,
		State:            migration.StatePlanned,
	}
	_, err := existingPlanInput(record, planOptions{
		MigrationID:    record.MigrationID,
		TrainRunID:     record.TrainRunID,
		TargetShard:    sharding.ShardZero,
		RollbackWindow: record.RollbackWindow,
		Timeout:        defaultCommandTimeout,
	})
	if !errors.Is(err, control.ErrPlanConflict) {
		t.Fatalf("existingPlanInput() error = %v, want ErrPlanConflict", err)
	}
}

func TestRollbackGenerationUsesPersistedValueAndRejectsOverflow(t *testing.T) {
	persisted := mustGeneration(t, 9)
	record := control.Record{TargetGeneration: mustGeneration(t, 8), RollbackGeneration: &persisted}
	generation, err := rollbackGenerationFor(record)
	if err != nil || generation != persisted {
		t.Fatalf("rollbackGenerationFor() = %v, %v; want %v", generation, err, persisted)
	}

	record = control.Record{TargetGeneration: mustGeneration(t, math.MaxInt64)}
	if _, err := rollbackGenerationFor(record); !errors.Is(err, control.ErrInvalidRecord) {
		t.Fatalf("rollbackGenerationFor(max) error = %v, want ErrInvalidRecord", err)
	}
}

func TestReconciliationErrorsMapToBoundedOperatorCategories(t *testing.T) {
	tests := []struct {
		name  string
		input error
		want  error
	}{
		{name: "violations", input: shardreconcile.ErrViolations, want: errReconciliationMismatch},
		{name: "partial", input: errors.Join(shardreconcile.ErrPartial, shardreconcile.ErrViolations), want: errReconciliationIncomplete},
		{name: "unavailable", input: shardreconcile.ErrUnavailable, want: controlpostgres.ErrPersistence},
		{name: "invalid", input: shardreconcile.ErrInvalidInput, want: control.ErrInvalidInput},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := mapReconciliationError(test.input); !errors.Is(err, test.want) {
				t.Fatalf("mapReconciliationError(%v) = %v, want %v", test.input, err, test.want)
			}
		})
	}
}

func TestPlanRejectsSubsecondRollbackWindowBeforeBackendOpen(t *testing.T) {
	var opened int
	factory := func(_ context.Context, _ string, _ postgresx.RegionalSession) (adminBackend, error) {
		opened++
		return &fakeAdminBackend{}, nil
	}
	var stdout, stderr bytes.Buffer
	exitCode := runWithFactory(context.Background(), []string{
		"plan-migration", "--train-run-id", testTrainRunID, "--target-shard", "shard-1",
		"--migration-id", testMigrationID, "--rollback-window", "1500ms", "--dry-run",
	}, databaseLookup, &stdout, &stderr, factory)
	if exitCode != 2 || opened != 0 || !strings.Contains(stdout.String(), `"error":"invalid_arguments"`) {
		t.Fatalf("exit/opened/stdout/stderr=%d/%d/%q/%q", exitCode, opened, stdout.String(), stderr.String())
	}
}

func TestShardEligibilityFailureHasBoundedPublicCode(t *testing.T) {
	backend := &fakeAdminBackend{err: control.ErrShardNotWritable}
	var stdout, stderr bytes.Buffer
	exitCode := runWithFactory(
		context.Background(),
		[]string{"inspect-health"},
		databaseLookup,
		&stdout,
		&stderr,
		fakeFactory(backend),
	)
	if exitCode != 1 || !strings.Contains(stdout.String(), `"error":"shard_not_writable"`) ||
		strings.Contains(stdout.String()+stderr.String(), control.ErrShardNotWritable.Error()) {
		t.Fatalf("exit/stdout/stderr=%d/%q/%q", exitCode, stdout.String(), stderr.String())
	}
}

func TestShardEligibilityRejectsUnsupportedFencingProtocol(t *testing.T) {
	supported := shardSummary{
		ShardID:                       sharding.ShardOne.String(),
		StorageKind:                   "schema",
		Enabled:                       true,
		WriteEnabled:                  true,
		State:                         "active",
		MinimumFencingProtocolVersion: sharding.SupportedFencingProtocolVersion,
	}
	if !shardWritableByCLI(supported) {
		t.Fatal("supported active shard reported ineligible")
	}
	supported.MinimumFencingProtocolVersion++
	if shardWritableByCLI(supported) {
		t.Fatal("unsupported fencing protocol reported eligible")
	}
}

func TestValidShardSummaryAcceptsVersionEightAndNineLogicalStorageKinds(t *testing.T) {
	tests := []struct {
		name        string
		shardID     string
		storageKind string
		want        bool
	}{
		{name: "version eight legacy", shardID: "legacy", storageKind: "legacy", want: true},
		{name: "version nine legacy", shardID: "legacy", storageKind: "legacy_schema", want: true},
		{name: "version eight logical", shardID: "shard-0", storageKind: "schema", want: true},
		{name: "version nine logical", shardID: "shard-1", storageKind: "logical_schema", want: true},
		{name: "physical catalog belongs to physical admin", shardID: "physical-shard-0", storageKind: "postgres", want: false},
		{name: "legacy cannot claim logical storage", shardID: "legacy", storageKind: "logical_schema", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			summary := shardSummary{
				ShardID:                       test.shardID,
				StorageKind:                   test.storageKind,
				Enabled:                       true,
				WriteEnabled:                  true,
				State:                         "active",
				MinimumFencingProtocolVersion: 1,
			}
			if got := validShardSummary(summary); got != test.want {
				t.Fatalf("validShardSummary(%s, %s) = %t, want %t", test.shardID, test.storageKind, got, test.want)
			}
		})
	}
}

func TestMigrationSummaryOmitsCheckpointAndValidationDigests(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	record := control.Record{
		MigrationID:      uuid.MustParse(testMigrationID),
		TrainRunID:       uuid.MustParse(testTrainRunID),
		SourceShard:      sharding.ShardLegacy,
		TargetShard:      sharding.ShardOne,
		SourceGeneration: mustGeneration(t, 1),
		TargetGeneration: mustGeneration(t, 2),
		RollbackWindow:   5 * time.Minute,
		State:            migration.StateValidating,
		Checkpoint:       "reservations:33333333-3333-4333-8333-333333333333",
		CreatedAt:        now,
		UpdatedAt:        now,
		LastValidation: &control.ValidationOutcome{
			CheckedAt: now,
			Snapshot: control.ValidationSnapshot{
				Source: control.DatasetDigest{Tables: []control.TableDigest{{
					Name: "private_table", Rows: 1, Checksum: "private-checksum",
				}}},
			},
		},
	}
	encoded, err := json.Marshal(summarizeMigration(record))
	if err != nil {
		t.Fatalf("Marshal(summary): %v", err)
	}
	text := string(encoded)
	for _, forbidden := range []string{"33333333-3333-4333-8333-333333333333", "private_table", "private-checksum"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("summary leaked %q: %s", forbidden, text)
		}
	}
	if !strings.Contains(text, `"checkpoint_present":true`) {
		t.Fatalf("summary omitted safe checkpoint indicator: %s", text)
	}
}

func mustGeneration(t *testing.T, value int64) sharding.AssignmentGeneration {
	t.Helper()
	generation, err := sharding.NewAssignmentGeneration(value)
	if err != nil {
		t.Fatalf("NewAssignmentGeneration(%d): %v", value, err)
	}
	return generation
}
