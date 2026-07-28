package control_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
	"github.com/google/uuid"
)

var (
	migrationID = uuid.MustParse("10000000-0000-0000-0000-000000000001")
	trainRunID  = uuid.MustParse("20000000-0000-0000-0000-000000000002")
	baseTime    = time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
)

func TestNewServiceRejectsUnboundedLimits(t *testing.T) {
	t.Parallel()

	valid := testLimits()
	cases := []struct {
		name   string
		mutate func(*control.Limits)
	}{
		{"batch", func(v *control.Limits) { v.MaxBatchSize = 0 }},
		{"checkpoint", func(v *control.Limits) { v.MaxCheckpointBytes = 0 }},
		{"timeout", func(v *control.Limits) { v.MaxOperationTimeout = 0 }},
		{"row cap", func(v *control.Limits) { v.MaxValidationRows = 0 }},
		{"locator cap", func(v *control.Limits) { v.MaxLocatorRows = 0 }},
		{"rollback window", func(v *control.Limits) { v.MaxRollbackWindow = 0 }},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			limits := valid
			tc.mutate(&limits)
			if _, err := control.NewService(newFakeRepository(), fixedClock{baseTime}, limits); !errors.Is(err, control.ErrInvalidLimits) {
				t.Fatalf("NewService() error = %v, want ErrInvalidLimits", err)
			}
		})
	}
}

func TestPlanIsIdempotentAndInitializesDisabledTargetFence(t *testing.T) {
	t.Parallel()

	repo := newFakeRepository()
	repo.route = mustRoute(t, sharding.ShardLegacy, 7)
	service := newService(t, repo, baseTime)
	input := validPlanInput(t)

	first, err := service.Plan(context.Background(), input)
	if err != nil {
		t.Fatalf("Plan() returned error: %v", err)
	}
	second, err := service.Plan(context.Background(), input)
	if err != nil {
		t.Fatalf("second Plan() returned error: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("idempotent Plan() changed record:\nfirst=%+v\nsecond=%+v", first, second)
	}
	if first.State != migration.StatePlanned {
		t.Fatalf("State = %q, want planned", first.State)
	}
	if got, want := repo.committedCalls, []string{"active_route", "shard_eligible:shard-0", "fence_state:legacy:7", "fence:shard-0:8:false", "insert"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
}

func TestPlanRejectsConflictingReplayAndNonMonotonicGeneration(t *testing.T) {
	t.Parallel()

	repo := newFakeRepository()
	repo.route = mustRoute(t, sharding.ShardLegacy, 7)
	service := newService(t, repo, baseTime)
	input := validPlanInput(t)
	if _, err := service.Plan(context.Background(), input); err != nil {
		t.Fatalf("Plan() returned error: %v", err)
	}

	conflict := input
	conflict.TargetShard = sharding.ShardOne
	if _, err := service.Plan(context.Background(), conflict); !errors.Is(err, control.ErrPlanConflict) {
		t.Fatalf("conflicting Plan() error = %v, want ErrPlanConflict", err)
	}

	nonMonotonic := validPlanInput(t)
	nonMonotonic.MigrationID = uuid.New()
	nonMonotonic.TargetGeneration = mustGeneration(t, 7)
	if _, err := service.Plan(context.Background(), nonMonotonic); !errors.Is(err, control.ErrInvalidInput) {
		t.Fatalf("non-monotonic Plan() error = %v, want ErrInvalidInput", err)
	}
}

func TestPlanRejectsTargetThatIsNotAtomicallyWriteEligible(t *testing.T) {
	t.Parallel()

	repo := newFakeRepository()
	repo.route = mustRoute(t, sharding.ShardLegacy, 7)
	repo.failAt = "shard_eligible"
	service := newService(t, repo, baseTime)

	if _, err := service.Plan(context.Background(), validPlanInput(t)); err == nil {
		t.Fatal("Plan() with ineligible target unexpectedly succeeded")
	}
	if len(repo.records) != 0 || repo.fences[sharding.ShardZero] {
		t.Fatalf("ineligible target leaked plan state: records=%d fences=%+v", len(repo.records), repo.fences)
	}
}

func TestCopyBatchPersistsBoundedCheckpointAndResumesIdempotently(t *testing.T) {
	t.Parallel()

	repo := newFakeRepository()
	record := plannedRecord(t)
	repo.records[migrationID] = record
	repo.route = mustSourceRoute(t, record)
	repo.copyResults = []control.CopyBatchResult{
		{NextCheckpoint: "reservation:0100", RowsCopied: 2},
		{NextCheckpoint: "done", RowsCopied: 1, Done: true},
	}
	service := newService(t, repo, baseTime)
	input := control.CopyBatchInput{MigrationID: migrationID, BatchSize: 2, Timeout: time.Second}

	first, err := service.CopyBatch(context.Background(), input)
	if err != nil {
		t.Fatalf("first CopyBatch() returned error: %v", err)
	}
	if first.Checkpoint != "reservation:0100" || first.CopiedRows != 2 || first.State != migration.StateCopying {
		t.Fatalf("first CopyBatch() = %+v", first)
	}
	second, err := service.CopyBatch(context.Background(), input)
	if err != nil {
		t.Fatalf("second CopyBatch() returned error: %v", err)
	}
	if second.Checkpoint != "done" || second.CopiedRows != 3 || !second.CopyComplete || second.State != migration.StateValidating {
		t.Fatalf("second CopyBatch() = %+v", second)
	}
	third, err := service.CopyBatch(context.Background(), input)
	if err != nil {
		t.Fatalf("completed CopyBatch() replay returned error: %v", err)
	}
	if !reflect.DeepEqual(second, third) {
		t.Fatalf("completed replay changed record:\nsecond=%+v\nthird=%+v", second, third)
	}
	if got := repo.copyRequests; len(got) != 2 || got[0].Checkpoint != "" || got[1].Checkpoint != "reservation:0100" {
		t.Fatalf("copy checkpoints = %+v, want empty then reservation:0100", got)
	}
	if repo.fences[sharding.ShardLegacy] {
		t.Fatal("source remained write-enabled after the quiesced copy began")
	}
	wantCalls := []string{
		"active_route",
		"fence_state:legacy:7",
		"fence_state:shard-0:8",
		"quiesce:legacy:7",
		"fence:legacy:7:false",
		"save",
		"save",
	}
	if !reflect.DeepEqual(repo.committedCalls, wantCalls) {
		t.Fatalf("copy calls = %v, want %v", repo.committedCalls, wantCalls)
	}
}

func TestCopyBatchSaveFailureRollsBackQuiesceAndResumesFromDurableCheckpoint(t *testing.T) {
	t.Parallel()

	repo := newFakeRepository()
	record := plannedRecord(t)
	repo.records[migrationID] = record
	repo.route = mustSourceRoute(t, record)
	repo.copyResults = []control.CopyBatchResult{{NextCheckpoint: "next", RowsCopied: 1}}
	repo.failAt = "save"
	service := newService(t, repo, baseTime)
	input := control.CopyBatchInput{MigrationID: migrationID, BatchSize: 2, Timeout: time.Second}

	if _, err := service.CopyBatch(context.Background(), input); err == nil {
		t.Fatal("CopyBatch() with injected save failure unexpectedly succeeded")
	}
	if got := repo.records[migrationID]; got.State != migration.StatePlanned || got.Checkpoint != "" {
		t.Fatalf("failed copy committed checkpoint: %+v", got)
	}
	if !repo.fences[sharding.ShardLegacy] || len(repo.copyResults) != 1 {
		t.Fatalf("failed copy leaked transaction effects: fences=%+v results=%d", repo.fences, len(repo.copyResults))
	}

	repo.failAt = ""
	got, err := service.CopyBatch(context.Background(), input)
	if err != nil {
		t.Fatalf("CopyBatch() retry returned error: %v", err)
	}
	if got.Checkpoint != "next" || got.State != migration.StateCopying {
		t.Fatalf("CopyBatch() retry = %+v", got)
	}
}

func TestCopyBatchRejectsAdapterOverrunAndNoProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		result control.CopyBatchResult
	}{
		{"overrun", control.CopyBatchResult{NextCheckpoint: "next", RowsCopied: 3}},
		{"no progress", control.CopyBatchResult{}},
		{"same checkpoint", control.CopyBatchResult{RowsCopied: 1}},
		{"done without checkpoint", control.CopyBatchResult{Done: true}},
		{"oversized checkpoint", control.CopyBatchResult{NextCheckpoint: strings.Repeat("x", 257), RowsCopied: 1}},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo := newFakeRepository()
			record := plannedRecord(t)
			repo.records[migrationID] = record
			repo.route = mustSourceRoute(t, record)
			repo.copyResults = []control.CopyBatchResult{tc.result}
			service := newService(t, repo, baseTime)
			_, err := service.CopyBatch(context.Background(), control.CopyBatchInput{MigrationID: migrationID, BatchSize: 2, Timeout: time.Second})
			if !errors.Is(err, control.ErrInvalidCopyResult) {
				t.Fatalf("CopyBatch() error = %v, want ErrInvalidCopyResult", err)
			}
			if got := repo.records[migrationID]; got.State != migration.StatePlanned || got.Checkpoint != "" {
				t.Fatalf("failed batch persisted progress: %+v", got)
			}
		})
	}
}

func TestValidateRequiresMatchingDigestsAndCompleteLocators(t *testing.T) {
	t.Parallel()

	repo := newFakeRepository()
	record := plannedRecord(t)
	record.State = migration.StateValidating
	record.CopyComplete = true
	repo.records[migrationID] = record
	repo.validationResults = []control.ValidationSnapshot{
		{
			Source:                     digest(control.TableDigest{Name: "reservations", Rows: 4, Checksum: "aaa"}),
			Target:                     digest(control.TableDigest{Name: "reservations", Rows: 4, Checksum: "bbb"}),
			MissingReservationLocators: 1,
			RowsExamined:               8,
		},
		{
			Source:       digest(control.TableDigest{Name: "reservations", Rows: 4, Checksum: "aaa"}),
			Target:       digest(control.TableDigest{Name: "reservations", Rows: 4, Checksum: "aaa"}),
			RowsExamined: 8,
		},
	}
	service := newService(t, repo, baseTime)
	input := control.ValidateInput{MigrationID: migrationID, RowCap: 20, Timeout: time.Second}

	failed, err := service.Validate(context.Background(), input)
	if err != nil {
		t.Fatalf("first Validate() returned error: %v", err)
	}
	if failed.Passed || failed.Record.State != migration.StateValidating {
		t.Fatalf("failed validation = %+v", failed)
	}
	passed, err := service.Validate(context.Background(), input)
	if err != nil {
		t.Fatalf("second Validate() returned error: %v", err)
	}
	if !passed.Passed || passed.Record.State != migration.StateCutoverReady {
		t.Fatalf("passed validation = %+v", passed)
	}
	replayed, err := service.Validate(context.Background(), input)
	if err != nil {
		t.Fatalf("validation replay returned error: %v", err)
	}
	if !reflect.DeepEqual(passed, replayed) || len(repo.validationRequests) != 2 {
		t.Fatalf("validation replay was not idempotent: passed=%+v replayed=%+v calls=%d", passed, replayed, len(repo.validationRequests))
	}
}

func TestValidateRejectsTruncatedOrOverCapSnapshot(t *testing.T) {
	t.Parallel()

	for _, snapshot := range []control.ValidationSnapshot{
		{Truncated: true, RowsExamined: 10},
		{RowsExamined: 11},
	} {
		repo := newFakeRepository()
		record := plannedRecord(t)
		record.State = migration.StateValidating
		record.CopyComplete = true
		repo.records[migrationID] = record
		repo.validationResults = []control.ValidationSnapshot{snapshot}
		service := newService(t, repo, baseTime)
		_, err := service.Validate(context.Background(), control.ValidateInput{MigrationID: migrationID, RowCap: 10, Timeout: time.Second})
		if !errors.Is(err, control.ErrValidationRowCapExceeded) {
			t.Fatalf("Validate(%+v) error = %v, want ErrValidationRowCapExceeded", snapshot, err)
		}
	}
}

func TestValidateSaveFailureKeepsStateAndCanRetry(t *testing.T) {
	t.Parallel()

	repo := newFakeRepository()
	record := plannedRecord(t)
	record.State = migration.StateValidating
	record.CopyComplete = true
	repo.records[migrationID] = record
	repo.validationResults = []control.ValidationSnapshot{matchingSnapshot()}
	repo.failAt = "save"
	service := newService(t, repo, baseTime)
	input := control.ValidateInput{MigrationID: migrationID, RowCap: 10, Timeout: time.Second}

	if _, err := service.Validate(context.Background(), input); err == nil {
		t.Fatal("Validate() with injected save failure unexpectedly succeeded")
	}
	if got := repo.records[migrationID]; got.State != migration.StateValidating || got.LastValidation != nil {
		t.Fatalf("failed validation committed state: %+v", got)
	}
	if len(repo.validationResults) != 1 {
		t.Fatal("failed validation consumed the durable retry input")
	}

	repo.failAt = ""
	got, err := service.Validate(context.Background(), input)
	if err != nil || !got.Passed {
		t.Fatalf("Validate() retry = %+v, %v", got, err)
	}
}

func TestCutoverSequencesQuiesceAndFencesWithoutDualWrites(t *testing.T) {
	t.Parallel()

	repo := newFakeRepository()
	record := plannedRecord(t)
	record.State = migration.StateCutoverReady
	record.CopyComplete = true
	record.LastValidation = &control.ValidationOutcome{Passed: true, CheckedAt: baseTime}
	repo.records[migrationID] = record
	repo.route = mustSourceRoute(t, record)
	repo.fences[sharding.ShardLegacy] = false
	repo.validationResults = []control.ValidationSnapshot{matchingSnapshot()}
	service := newService(t, repo, baseTime)

	got, err := service.Cutover(context.Background(), validCutoverInput())
	if err != nil {
		t.Fatalf("Cutover() returned error: %v", err)
	}
	if got.State != migration.StateRollbackWindow || got.CutoverAt == nil || got.RollbackDeadline == nil {
		t.Fatalf("Cutover() record = %+v", got)
	}
	wantCalls := []string{
		"active_route",
		"shard_eligible:shard-0",
		"validate",
		"locators",
		"fence_state:legacy:7",
		"fence_state:shard-0:8",
		"quiesce:legacy:7",
		"fence:legacy:7:false",
		"fence:shard-0:8:true",
		"activate:legacy:7->shard-0:8",
		"save",
	}
	if !reflect.DeepEqual(repo.committedCalls, wantCalls) {
		t.Fatalf("cutover calls = %v, want %v", repo.committedCalls, wantCalls)
	}
	if repo.fences[sharding.ShardLegacy] || !repo.fences[sharding.ShardZero] {
		t.Fatalf("unexpected committed fences: %+v", repo.fences)
	}
	if gotRoute := repo.route; gotRoute.ShardID() != sharding.ShardZero || gotRoute.Generation().Int64() != 8 {
		t.Fatalf("active route = %s/%d", gotRoute.ShardID(), gotRoute.Generation().Int64())
	}

	before := append([]string(nil), repo.committedCalls...)
	if _, err := service.Cutover(context.Background(), validCutoverInput()); err != nil {
		t.Fatalf("Cutover replay returned error: %v", err)
	}
	if !reflect.DeepEqual(repo.committedCalls, before) {
		t.Fatalf("Cutover replay performed side effects: before=%v after=%v", before, repo.committedCalls)
	}
}

func TestCutoverFailureInjectionRollsBackEveryPartialSequence(t *testing.T) {
	t.Parallel()

	for _, failAt := range []string{"active_route", "shard_eligible", "fence_state", "validate", "locators", "quiesce", "source_fence", "target_fence", "activate", "save"} {
		failAt := failAt
		t.Run(failAt, func(t *testing.T) {
			t.Parallel()
			repo := newFakeRepository()
			record := plannedRecord(t)
			record.State = migration.StateCutoverReady
			record.CopyComplete = true
			record.LastValidation = &control.ValidationOutcome{Passed: true, CheckedAt: baseTime}
			repo.records[migrationID] = record
			repo.route = mustSourceRoute(t, record)
			repo.fences[sharding.ShardLegacy] = false
			repo.validationResults = []control.ValidationSnapshot{matchingSnapshot()}
			repo.failAt = failAt
			service := newService(t, repo, baseTime)

			_, err := service.Cutover(context.Background(), validCutoverInput())
			if err == nil {
				t.Fatalf("Cutover() with failure at %q unexpectedly succeeded", failAt)
			}
			if got := repo.records[migrationID]; got.State != migration.StateCutoverReady {
				t.Fatalf("failure at %q committed state %q", failAt, got.State)
			}
			if repo.route.ShardID() != sharding.ShardLegacy || repo.fences[sharding.ShardLegacy] || repo.fences[sharding.ShardZero] {
				t.Fatalf("failure at %q leaked authority changes: route=%s fences=%+v", failAt, repo.route.ShardID(), repo.fences)
			}

			repo.failAt = ""
			if _, err := service.Cutover(context.Background(), validCutoverInput()); err != nil {
				t.Fatalf("Cutover() resume after %q failure returned error: %v", failAt, err)
			}
		})
	}
}

func TestCutoverRevalidationOrLocatorCapFailureCannotChangeAuthority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		validation  control.ValidationSnapshot
		locatorRows int64
		want        error
	}{
		{
			name: "changed target checksum",
			validation: control.ValidationSnapshot{
				Source:       digest(control.TableDigest{Name: "reservations", Rows: 1, Checksum: "source"}),
				Target:       digest(control.TableDigest{Name: "reservations", Rows: 1, Checksum: "target"}),
				RowsExamined: 2,
			},
			want: control.ErrCutoverValidationFailed,
		},
		{name: "locator cap", validation: matchingSnapshot(), locatorRows: 101, want: control.ErrLocatorRowCapExceeded},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo := newFakeRepository()
			record := plannedRecord(t)
			record.State = migration.StateCutoverReady
			record.CopyComplete = true
			record.LastValidation = &control.ValidationOutcome{Passed: true, CheckedAt: baseTime}
			repo.records[migrationID] = record
			repo.route = mustSourceRoute(t, record)
			repo.fences[sharding.ShardLegacy] = false
			repo.validationResults = []control.ValidationSnapshot{tc.validation}
			repo.locatorRows = tc.locatorRows
			service := newService(t, repo, baseTime)

			_, err := service.Cutover(context.Background(), validCutoverInput())
			if !errors.Is(err, tc.want) {
				t.Fatalf("Cutover() error = %v, want %v", err, tc.want)
			}
			if repo.route.ShardID() != sharding.ShardLegacy || repo.fences[sharding.ShardZero] {
				t.Fatalf("failed cutover changed authority: route=%s fences=%+v", repo.route.ShardID(), repo.fences)
			}
		})
	}
}

func TestPreCutoverRollbackRestoresSourceFenceWithoutChangingAssignment(t *testing.T) {
	t.Parallel()

	repo := newFakeRepository()
	record := plannedRecord(t)
	record.State = migration.StateCopying
	repo.records[migrationID] = record
	repo.route = mustSourceRoute(t, record)
	repo.fences[sharding.ShardLegacy] = false
	service := newService(t, repo, baseTime)

	got, err := service.DirectRollback(context.Background(), control.DirectRollbackInput{
		MigrationID: migrationID,
		Timeout:     time.Second,
	})
	if err != nil {
		t.Fatalf("DirectRollback() returned error: %v", err)
	}
	if got.State != migration.StateRolledBack || repo.route.ShardID() != sharding.ShardLegacy || !repo.fences[sharding.ShardLegacy] {
		t.Fatalf("pre-cutover rollback = %+v, route=%s, fences=%+v", got, repo.route.ShardID(), repo.fences)
	}
	want := []string{"active_route", "shard_eligible:legacy", "fence:shard-0:8:false", "fence:legacy:7:true", "save"}
	if !reflect.DeepEqual(repo.committedCalls, want) {
		t.Fatalf("pre-cutover rollback calls = %v, want %v", repo.committedCalls, want)
	}
}

func TestDirectRollbackRequiresNoDurableTargetWriteAndUsesNewGeneration(t *testing.T) {
	t.Parallel()

	repo := newFakeRepository()
	record := cutoverRecord(t, baseTime)
	repo.records[migrationID] = record
	repo.route = mustTargetRoute(t, record)
	repo.fences[sharding.ShardLegacy] = false
	repo.fences[sharding.ShardZero] = true
	service := newService(t, repo, baseTime.Add(time.Minute))

	got, err := service.DirectRollback(context.Background(), control.DirectRollbackInput{
		MigrationID:        migrationID,
		RollbackGeneration: mustGeneration(t, 9),
		LocatorRowCap:      100,
		Timeout:            time.Second,
	})
	if err != nil {
		t.Fatalf("DirectRollback() returned error: %v", err)
	}
	if got.State != migration.StateRolledBack || got.RollbackGeneration == nil || got.RollbackGeneration.Int64() != 9 {
		t.Fatalf("rollback record = %+v", got)
	}
	want := []string{
		"active_route",
		"shard_eligible:legacy",
		"locators",
		"fence_state:shard-0:8",
		"fence_state:legacy:7",
		"quiesce:shard-0:8",
		"target_write_evidence",
		"fence:shard-0:8:false",
		"fence:legacy:9:true",
		"activate:shard-0:8->legacy:9",
		"save",
	}
	if !reflect.DeepEqual(repo.committedCalls, want) {
		t.Fatalf("rollback calls = %v, want %v", repo.committedCalls, want)
	}
	if repo.route.ShardID() != sharding.ShardLegacy || repo.route.Generation().Int64() != 9 {
		t.Fatalf("rollback route = %s/%d", repo.route.ShardID(), repo.route.Generation().Int64())
	}
}

func TestDirectRollbackRejectsTargetWriteEvidenceWithoutLeakingQuiesce(t *testing.T) {
	t.Parallel()

	repo := newFakeRepository()
	record := cutoverRecord(t, baseTime)
	repo.records[migrationID] = record
	repo.route = mustTargetRoute(t, record)
	repo.fences[sharding.ShardLegacy] = false
	repo.fences[sharding.ShardZero] = true
	repo.targetWriteEvidence = true
	service := newService(t, repo, baseTime.Add(time.Minute))

	_, err := service.DirectRollback(context.Background(), control.DirectRollbackInput{
		MigrationID:        migrationID,
		RollbackGeneration: mustGeneration(t, 9),
		LocatorRowCap:      100,
		Timeout:            time.Second,
	})
	if !errors.Is(err, control.ErrTargetWriteEvidence) {
		t.Fatalf("DirectRollback() error = %v, want ErrTargetWriteEvidence", err)
	}
	if got := repo.records[migrationID]; got.State != migration.StateRollbackWindow {
		t.Fatalf("rollback rejection persisted state %q", got.State)
	}
	if repo.route.ShardID() != sharding.ShardZero || !repo.fences[sharding.ShardZero] {
		t.Fatalf("rollback rejection leaked authority changes: route=%s fences=%+v", repo.route.ShardID(), repo.fences)
	}
}

func TestDirectRollbackRejectsExpiredWindowAndNonMonotonicGeneration(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		now        time.Time
		generation int64
		want       error
	}{
		"expired":       {baseTime.Add(31 * time.Minute), 9, control.ErrRollbackWindowExpired},
		"non_monotonic": {baseTime.Add(time.Minute), 8, control.ErrInvalidInput},
	} {
		t.Run(name, func(t *testing.T) {
			repo := newFakeRepository()
			record := cutoverRecord(t, baseTime)
			repo.records[migrationID] = record
			repo.route = mustTargetRoute(t, record)
			service := newService(t, repo, tc.now)
			_, err := service.DirectRollback(context.Background(), control.DirectRollbackInput{
				MigrationID:        migrationID,
				RollbackGeneration: mustGeneration(t, tc.generation),
				LocatorRowCap:      100,
				Timeout:            time.Second,
			})
			if !errors.Is(err, tc.want) {
				t.Fatalf("DirectRollback() error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestCompleteAndCleanupEligibilityWaitForRollbackWindow(t *testing.T) {
	t.Parallel()

	repo := newFakeRepository()
	record := cutoverRecord(t, baseTime)
	repo.records[migrationID] = record
	repo.route = mustTargetRoute(t, record)
	repo.fences[sharding.ShardLegacy] = false
	repo.fences[sharding.ShardZero] = true
	service := newService(t, repo, baseTime.Add(29*time.Minute))
	input := control.CompleteInput{MigrationID: migrationID, Timeout: time.Second}

	if _, err := service.Complete(context.Background(), input); !errors.Is(err, control.ErrRollbackWindowOpen) {
		t.Fatalf("early Complete() error = %v, want ErrRollbackWindowOpen", err)
	}
	early, err := service.CleanupEligibility(context.Background(), control.CleanupEligibilityInput{MigrationID: migrationID, Timeout: time.Second})
	if err != nil {
		t.Fatalf("early CleanupEligibility() returned error: %v", err)
	}
	if early.Eligible {
		t.Fatal("cleanup became eligible before completion/window expiry")
	}

	service = newService(t, repo, baseTime.Add(30*time.Minute))
	completed, err := service.Complete(context.Background(), input)
	if err != nil {
		t.Fatalf("Complete() returned error: %v", err)
	}
	if completed.State != migration.StateCompleted {
		t.Fatalf("Complete() state = %q", completed.State)
	}
	eligibility, err := service.CleanupEligibility(context.Background(), control.CleanupEligibilityInput{MigrationID: migrationID, Timeout: time.Second})
	if err != nil {
		t.Fatalf("CleanupEligibility() returned error: %v", err)
	}
	if !eligibility.Eligible {
		t.Fatalf("cleanup eligibility = %+v", eligibility)
	}
}

func TestOperationsRejectNonPositiveOrOverLimitBounds(t *testing.T) {
	t.Parallel()

	repo := newFakeRepository()
	repo.records[migrationID] = plannedRecord(t)
	service := newService(t, repo, baseTime)

	for _, batch := range []int{0, 101} {
		_, err := service.CopyBatch(context.Background(), control.CopyBatchInput{MigrationID: migrationID, BatchSize: batch, Timeout: time.Second})
		if !errors.Is(err, control.ErrInvalidInput) {
			t.Fatalf("CopyBatch(batch=%d) error = %v, want ErrInvalidInput", batch, err)
		}
	}
	for _, timeout := range []time.Duration{0, 6 * time.Second} {
		_, err := service.CopyBatch(context.Background(), control.CopyBatchInput{MigrationID: migrationID, BatchSize: 1, Timeout: timeout})
		if !errors.Is(err, control.ErrInvalidInput) {
			t.Fatalf("CopyBatch(timeout=%s) error = %v, want ErrInvalidInput", timeout, err)
		}
	}
	record := repo.records[migrationID]
	record.State = migration.StateValidating
	record.CopyComplete = true
	repo.records[migrationID] = record
	for _, cap := range []int64{0, 1001} {
		_, err := service.Validate(context.Background(), control.ValidateInput{MigrationID: migrationID, RowCap: cap, Timeout: time.Second})
		if !errors.Is(err, control.ErrInvalidInput) {
			t.Fatalf("Validate(row cap=%d) error = %v, want ErrInvalidInput", cap, err)
		}
	}
}

func validPlanInput(t *testing.T) control.PlanInput {
	t.Helper()
	return control.PlanInput{
		MigrationID:      migrationID,
		TrainRunID:       trainRunID,
		SourceShard:      sharding.ShardLegacy,
		TargetShard:      sharding.ShardZero,
		SourceGeneration: mustGeneration(t, 7),
		TargetGeneration: mustGeneration(t, 8),
		RollbackWindow:   30 * time.Minute,
		OperationTimeout: time.Second,
	}
}

func plannedRecord(t *testing.T) control.Record {
	t.Helper()
	input := validPlanInput(t)
	return control.Record{
		MigrationID:      input.MigrationID,
		TrainRunID:       input.TrainRunID,
		SourceShard:      input.SourceShard,
		TargetShard:      input.TargetShard,
		SourceGeneration: input.SourceGeneration,
		TargetGeneration: input.TargetGeneration,
		RollbackWindow:   input.RollbackWindow,
		State:            migration.StatePlanned,
		CreatedAt:        baseTime,
		UpdatedAt:        baseTime,
	}
}

func cutoverRecord(t *testing.T, cutoverAt time.Time) control.Record {
	t.Helper()
	record := plannedRecord(t)
	record.State = migration.StateRollbackWindow
	record.CopyComplete = true
	record.CutoverAt = timePointer(cutoverAt)
	record.RollbackDeadline = timePointer(cutoverAt.Add(record.RollbackWindow))
	return record
}

func newService(t *testing.T, repo *fakeRepository, now time.Time) *control.Service {
	t.Helper()
	service, err := control.NewService(repo, fixedClock{now}, testLimits())
	if err != nil {
		t.Fatalf("NewService() returned error: %v", err)
	}
	return service
}

func testLimits() control.Limits {
	return control.Limits{
		MaxBatchSize:        100,
		MaxCheckpointBytes:  256,
		MaxOperationTimeout: 5 * time.Second,
		MaxValidationRows:   1000,
		MaxLocatorRows:      1000,
		MaxRollbackWindow:   time.Hour,
	}
}

func validCutoverInput() control.CutoverInput {
	return control.CutoverInput{
		MigrationID:      migrationID,
		ValidationRowCap: 100,
		LocatorRowCap:    100,
		Timeout:          time.Second,
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

func mustRoute(t *testing.T, shardID sharding.ShardID, generation int64) sharding.ShardRoute {
	t.Helper()
	route, err := sharding.NewShardRoute(trainRunID, shardID, mustGeneration(t, generation))
	if err != nil {
		t.Fatalf("NewShardRoute(): %v", err)
	}
	return route
}

func mustSourceRoute(t *testing.T, record control.Record) sharding.ShardRoute {
	t.Helper()
	route, err := record.SourceRoute()
	if err != nil {
		t.Fatalf("SourceRoute(): %v", err)
	}
	return route
}

func mustTargetRoute(t *testing.T, record control.Record) sharding.ShardRoute {
	t.Helper()
	route, err := record.TargetRoute()
	if err != nil {
		t.Fatalf("TargetRoute(): %v", err)
	}
	return route
}

func digest(tables ...control.TableDigest) control.DatasetDigest {
	return control.DatasetDigest{Tables: tables}
}

func matchingSnapshot() control.ValidationSnapshot {
	table := control.TableDigest{Name: "reservations", Rows: 1, Checksum: "same"}
	return control.ValidationSnapshot{
		Source:       digest(table),
		Target:       digest(table),
		RowsExamined: 2,
	}
}

func timePointer(value time.Time) *time.Time { return &value }

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

type fakeRepository struct {
	records             map[uuid.UUID]control.Record
	route               sharding.ShardRoute
	fences              map[sharding.ShardID]bool
	copyResults         []control.CopyBatchResult
	validationResults   []control.ValidationSnapshot
	copyRequests        []control.CopyBatchRequest
	validationRequests  []control.ValidationRequest
	locatorRows         int64
	targetWriteEvidence bool
	failAt              string
	committedCalls      []string
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{
		records: make(map[uuid.UUID]control.Record),
		fences: map[sharding.ShardID]bool{
			sharding.ShardLegacy: true,
			sharding.ShardZero:   false,
			sharding.ShardOne:    false,
		},
	}
}

func (repo *fakeRepository) WithinTransaction(ctx context.Context, fn func(context.Context, control.Transaction) error) error {
	tx := &fakeTransaction{
		records:             cloneRecords(repo.records),
		route:               repo.route,
		fences:              cloneFences(repo.fences),
		copyResults:         append([]control.CopyBatchResult(nil), repo.copyResults...),
		validationResults:   append([]control.ValidationSnapshot(nil), repo.validationResults...),
		copyRequests:        append([]control.CopyBatchRequest(nil), repo.copyRequests...),
		validationRequests:  append([]control.ValidationRequest(nil), repo.validationRequests...),
		locatorRows:         repo.locatorRows,
		targetWriteEvidence: repo.targetWriteEvidence,
		failAt:              repo.failAt,
	}
	if err := fn(ctx, tx); err != nil {
		return err
	}
	repo.records = tx.records
	repo.route = tx.route
	repo.fences = tx.fences
	repo.copyResults = tx.copyResults
	repo.validationResults = tx.validationResults
	repo.copyRequests = tx.copyRequests
	repo.validationRequests = tx.validationRequests
	repo.locatorRows = tx.locatorRows
	repo.committedCalls = append(repo.committedCalls, tx.calls...)
	return nil
}

type fakeTransaction struct {
	records             map[uuid.UUID]control.Record
	route               sharding.ShardRoute
	fences              map[sharding.ShardID]bool
	copyResults         []control.CopyBatchResult
	validationResults   []control.ValidationSnapshot
	copyRequests        []control.CopyBatchRequest
	validationRequests  []control.ValidationRequest
	locatorRows         int64
	targetWriteEvidence bool
	failAt              string
	calls               []string
}

func (tx *fakeTransaction) FindMigrationForUpdate(_ context.Context, id uuid.UUID) (control.Record, bool, error) {
	record, ok := tx.records[id]
	return record, ok, nil
}

func (tx *fakeTransaction) InsertMigration(_ context.Context, record control.Record) error {
	if _, exists := tx.records[record.MigrationID]; exists {
		return errors.New("duplicate migration")
	}
	tx.calls = append(tx.calls, "insert")
	tx.records[record.MigrationID] = record
	return nil
}

func (tx *fakeTransaction) SaveMigration(_ context.Context, record control.Record) error {
	if err := tx.maybeFail("save"); err != nil {
		return err
	}
	tx.calls = append(tx.calls, "save")
	tx.records[record.MigrationID] = record
	return nil
}

func (tx *fakeTransaction) ActiveRouteForUpdate(_ context.Context, _ uuid.UUID) (sharding.ShardRoute, error) {
	if err := tx.maybeFail("active_route"); err != nil {
		return sharding.ShardRoute{}, err
	}
	tx.calls = append(tx.calls, "active_route")
	return tx.route, nil
}

func (tx *fakeTransaction) RequireShardWritableForUpdate(_ context.Context, shardID sharding.ShardID) error {
	if err := tx.maybeFail("shard_eligible"); err != nil {
		return err
	}
	tx.calls = append(tx.calls, "shard_eligible:"+shardID.String())
	return nil
}

func (tx *fakeTransaction) WriteFenceEnabledForUpdate(_ context.Context, route sharding.ShardRoute) (bool, error) {
	if err := tx.maybeFail("fence_state"); err != nil {
		return false, err
	}
	tx.calls = append(tx.calls, fmt.Sprintf("fence_state:%s:%d", route.ShardID(), route.Generation().Int64()))
	return tx.fences[route.ShardID()], nil
}

func (tx *fakeTransaction) SetWriteFence(_ context.Context, route sharding.ShardRoute, enabled bool) error {
	name := "target_fence"
	if route.ShardID() == sharding.ShardLegacy {
		name = "source_fence"
	}
	if err := tx.maybeFail(name); err != nil {
		return err
	}
	tx.calls = append(tx.calls, fmt.Sprintf("fence:%s:%d:%t", route.ShardID(), route.Generation().Int64(), enabled))
	tx.fences[route.ShardID()] = enabled
	return nil
}

func (tx *fakeTransaction) QuiesceWrites(_ context.Context, route sharding.ShardRoute) error {
	if err := tx.maybeFail("quiesce"); err != nil {
		return err
	}
	tx.calls = append(tx.calls, fmt.Sprintf("quiesce:%s:%d", route.ShardID(), route.Generation().Int64()))
	return nil
}

func (tx *fakeTransaction) CopyBatch(_ context.Context, request control.CopyBatchRequest) (control.CopyBatchResult, error) {
	if err := tx.maybeFail("copy"); err != nil {
		return control.CopyBatchResult{}, err
	}
	tx.copyRequests = append(tx.copyRequests, request)
	if len(tx.copyResults) == 0 {
		return control.CopyBatchResult{}, errors.New("no copy result")
	}
	result := tx.copyResults[0]
	tx.copyResults = tx.copyResults[1:]
	return result, nil
}

func (tx *fakeTransaction) Validate(_ context.Context, request control.ValidationRequest) (control.ValidationSnapshot, error) {
	if err := tx.maybeFail("validate"); err != nil {
		return control.ValidationSnapshot{}, err
	}
	tx.validationRequests = append(tx.validationRequests, request)
	tx.calls = append(tx.calls, "validate")
	if len(tx.validationResults) == 0 {
		return control.ValidationSnapshot{}, errors.New("no validation result")
	}
	result := tx.validationResults[0]
	tx.validationResults = tx.validationResults[1:]
	return result, nil
}

func (tx *fakeTransaction) LockLocatorsForUpdate(_ context.Context, _ uuid.UUID, _ int64) (int64, error) {
	if err := tx.maybeFail("locators"); err != nil {
		return 0, err
	}
	tx.calls = append(tx.calls, "locators")
	return tx.locatorRows, nil
}

func (tx *fakeTransaction) ActivateRoute(_ context.Context, expected, next sharding.ShardRoute) error {
	if err := tx.maybeFail("activate"); err != nil {
		return err
	}
	if !sameRoute(tx.route, expected) {
		return errors.New("compare-and-swap route mismatch")
	}
	tx.calls = append(tx.calls, fmt.Sprintf("activate:%s:%d->%s:%d", expected.ShardID(), expected.Generation().Int64(), next.ShardID(), next.Generation().Int64()))
	tx.route = next
	return nil
}

func (tx *fakeTransaction) HasDurableTargetWrites(_ context.Context, _ sharding.ShardRoute) (bool, error) {
	if err := tx.maybeFail("target_write_evidence"); err != nil {
		return false, err
	}
	tx.calls = append(tx.calls, "target_write_evidence")
	return tx.targetWriteEvidence, nil
}

func (tx *fakeTransaction) maybeFail(point string) error {
	if tx.failAt == point {
		return fmt.Errorf("injected %s failure", point)
	}
	return nil
}

func cloneRecords(source map[uuid.UUID]control.Record) map[uuid.UUID]control.Record {
	clone := make(map[uuid.UUID]control.Record, len(source))
	for id, record := range source {
		clone[id] = record
	}
	return clone
}

func cloneFences(source map[sharding.ShardID]bool) map[sharding.ShardID]bool {
	clone := make(map[sharding.ShardID]bool, len(source))
	for id, enabled := range source {
		clone[id] = enabled
	}
	return clone
}

func sameRoute(left, right sharding.ShardRoute) bool {
	return left.TrainRunID() == right.TrainRunID() && left.ShardID() == right.ShardID() && left.Generation() == right.Generation()
}
