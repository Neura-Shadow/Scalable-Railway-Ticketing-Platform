package migration_test

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
)

func TestPhysicalMigrationAdvancesThroughTheExplicitHappyPath(t *testing.T) {
	t.Parallel()

	machine, err := migration.NewPhysical(12)
	if err != nil {
		t.Fatalf("NewPhysical(12) returned error: %v", err)
	}
	for _, next := range []migration.PhysicalState{
		migration.PhysicalStatePreparingTarget,
		migration.PhysicalStateCaptureEnabled,
		migration.PhysicalStateBaseCopying,
		migration.PhysicalStateCatchingUp,
		migration.PhysicalStateValidatingOnline,
		migration.PhysicalStateDraining,
		migration.PhysicalStateSourceFenced,
		migration.PhysicalStateFinalCatchup,
		migration.PhysicalStateFinalValidating,
		migration.PhysicalStateTargetEnabled,
		migration.PhysicalStateSwitchingAssignment,
		migration.PhysicalStateRollbackWindow,
		migration.PhysicalStateCompleted,
	} {
		if err := machine.Transition(next); err != nil {
			t.Fatalf("Transition(%q) returned error: %v", next, err)
		}
		if got := machine.Snapshot().State; got != next {
			t.Fatalf("state after Transition(%q) = %q", next, got)
		}
	}
	if got := machine.Snapshot().Generation; got != 12 {
		t.Fatalf("generation = %d, want 12", got)
	}
}

func TestNewPhysicalRejectsNonPositiveGeneration(t *testing.T) {
	t.Parallel()

	for _, generation := range []int64{0, -1} {
		if _, err := migration.NewPhysical(generation); !errors.Is(err, migration.ErrInvalidGeneration) {
			t.Fatalf("NewPhysical(%d) error = %v, want ErrInvalidGeneration", generation, err)
		}
	}
}

func TestPhysicalMigrationRestoresEveryCrashCheckpoint(t *testing.T) {
	t.Parallel()

	states := []migration.PhysicalState{
		migration.PhysicalStatePlanned,
		migration.PhysicalStatePreparingTarget,
		migration.PhysicalStateCaptureEnabled,
		migration.PhysicalStateBaseCopying,
		migration.PhysicalStateCatchingUp,
		migration.PhysicalStateValidatingOnline,
		migration.PhysicalStateDraining,
		migration.PhysicalStateSourceFenced,
		migration.PhysicalStateFinalCatchup,
		migration.PhysicalStateFinalValidating,
		migration.PhysicalStateTargetEnabled,
		migration.PhysicalStateSwitchingAssignment,
		migration.PhysicalStateRollbackWindow,
		migration.PhysicalStateCompleted,
		migration.PhysicalStateReverseMigrationRequired,
		migration.PhysicalStateFailed,
		migration.PhysicalStateRolledBack,
	}
	for _, state := range states {
		state := state
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			want := migration.PhysicalSnapshot{State: state, Generation: 19}
			machine, err := migration.RestorePhysical(want)
			if err != nil {
				t.Fatalf("RestorePhysical(%q) returned error: %v", state, err)
			}
			if got := machine.Snapshot(); got != want {
				t.Fatalf("Snapshot() = %+v, want %+v", got, want)
			}
		})
	}

	if _, err := migration.RestorePhysical(migration.PhysicalSnapshot{
		State:      migration.PhysicalState("unknown"),
		Generation: 19,
	}); !errors.Is(err, migration.ErrInvalidPhysicalSnapshot) {
		t.Fatalf("unknown-state restore error = %v, want ErrInvalidPhysicalSnapshot", err)
	}
}

func TestPhysicalMigrationAllowsDirectRollbackWithoutTargetWrite(t *testing.T) {
	t.Parallel()

	machine, err := migration.RestorePhysical(migration.PhysicalSnapshot{
		State:      migration.PhysicalStateRollbackWindow,
		Generation: 23,
	})
	if err != nil {
		t.Fatalf("RestorePhysical returned error: %v", err)
	}
	if err := machine.Transition(migration.PhysicalStateRolledBack); err != nil {
		t.Fatalf("Transition(rolled_back) returned error: %v", err)
	}
	if got := machine.Snapshot().State; got != migration.PhysicalStateRolledBack {
		t.Fatalf("state = %q, want rolled_back", got)
	}
}

func TestPhysicalTargetWriteEvidenceForbidsDirectRollback(t *testing.T) {
	t.Parallel()

	machine, err := migration.RestorePhysical(migration.PhysicalSnapshot{
		State:      migration.PhysicalStateRollbackWindow,
		Generation: 29,
	})
	if err != nil {
		t.Fatalf("RestorePhysical returned error: %v", err)
	}
	if err := machine.RecordTargetWrite(29); err != nil {
		t.Fatalf("RecordTargetWrite(29) returned error: %v", err)
	}
	if !machine.Snapshot().TargetWriteEvidence {
		t.Fatal("target-write evidence was not persisted in the snapshot")
	}
	if err := machine.Transition(migration.PhysicalStateRolledBack); !errors.Is(err, migration.ErrPhysicalReverseMigrationRequired) {
		t.Fatalf("direct rollback error = %v, want ErrPhysicalReverseMigrationRequired", err)
	}
	if got := machine.Snapshot().State; got != migration.PhysicalStateRollbackWindow {
		t.Fatalf("state after rejected rollback = %q, want rollback_window", got)
	}
}

func TestPhysicalReverseMigrationRequiresANewerGeneration(t *testing.T) {
	t.Parallel()

	machine, err := migration.RestorePhysical(migration.PhysicalSnapshot{
		State:               migration.PhysicalStateRollbackWindow,
		Generation:          31,
		TargetWriteEvidence: true,
	})
	if err != nil {
		t.Fatalf("RestorePhysical returned error: %v", err)
	}
	for _, generation := range []int64{30, 31} {
		if _, err := machine.StartReverseMigration(generation); !errors.Is(err, migration.ErrPhysicalReverseGenerationNotNewer) {
			t.Fatalf("StartReverseMigration(%d) error = %v, want ErrPhysicalReverseGenerationNotNewer", generation, err)
		}
		if got := machine.Snapshot().State; got != migration.PhysicalStateRollbackWindow {
			t.Fatalf("state after rejected reverse generation = %q", got)
		}
	}

	reverse, err := machine.StartReverseMigration(32)
	if err != nil {
		t.Fatalf("StartReverseMigration(32) returned error: %v", err)
	}
	if got := machine.Snapshot().State; got != migration.PhysicalStateReverseMigrationRequired {
		t.Fatalf("original state = %q, want reverse_migration_required", got)
	}
	if got := reverse.Snapshot(); got.State != migration.PhysicalStatePlanned || got.Generation != 32 {
		t.Fatalf("reverse snapshot = %+v, want planned generation 32", got)
	}
}

func TestPhysicalMigrationCanRecordSafeRollbackFromAnyUnassignedCheckpoint(t *testing.T) {
	t.Parallel()

	states := []migration.PhysicalState{
		migration.PhysicalStatePlanned,
		migration.PhysicalStatePreparingTarget,
		migration.PhysicalStateCaptureEnabled,
		migration.PhysicalStateBaseCopying,
		migration.PhysicalStateCatchingUp,
		migration.PhysicalStateValidatingOnline,
		migration.PhysicalStateDraining,
		migration.PhysicalStateSourceFenced,
		migration.PhysicalStateFinalCatchup,
		migration.PhysicalStateFinalValidating,
		migration.PhysicalStateTargetEnabled,
		migration.PhysicalStateSwitchingAssignment,
	}
	for _, state := range states {
		state := state
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			machine, err := migration.RestorePhysical(migration.PhysicalSnapshot{
				State:      state,
				Generation: 37,
			})
			if err != nil {
				t.Fatalf("RestorePhysical returned error: %v", err)
			}
			if err := machine.Transition(migration.PhysicalStateRolledBack); err != nil {
				t.Fatalf("Transition(rolled_back) from %q returned error: %v", state, err)
			}
		})
	}
}

func TestPhysicalMigrationRecordsFailureWithoutResumingTerminalStates(t *testing.T) {
	t.Parallel()

	for _, state := range []migration.PhysicalState{
		migration.PhysicalStatePlanned,
		migration.PhysicalStateBaseCopying,
		migration.PhysicalStateSourceFenced,
		migration.PhysicalStateTargetEnabled,
		migration.PhysicalStateSwitchingAssignment,
		migration.PhysicalStateRollbackWindow,
	} {
		machine, err := migration.RestorePhysical(migration.PhysicalSnapshot{
			State:      state,
			Generation: 41,
		})
		if err != nil {
			t.Fatalf("RestorePhysical(%q) returned error: %v", state, err)
		}
		if err := machine.Transition(migration.PhysicalStateFailed); err != nil {
			t.Fatalf("Transition(failed) from %q returned error: %v", state, err)
		}
	}

	for _, state := range []migration.PhysicalState{
		migration.PhysicalStateCompleted,
		migration.PhysicalStateFailed,
		migration.PhysicalStateRolledBack,
		migration.PhysicalStateReverseMigrationRequired,
	} {
		machine, err := migration.RestorePhysical(migration.PhysicalSnapshot{
			State:      state,
			Generation: 43,
		})
		if err != nil {
			t.Fatalf("RestorePhysical(%q) returned error: %v", state, err)
		}
		if err := machine.Transition(migration.PhysicalStatePreparingTarget); !errors.Is(err, migration.ErrInvalidPhysicalTransition) {
			t.Fatalf("terminal %q resume error = %v, want ErrInvalidPhysicalTransition", state, err)
		}
	}
}

func TestPhysicalCutoverOrderingCannotBeSkipped(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		current migration.PhysicalState
		next    migration.PhysicalState
	}{
		{name: "target before source fence", current: migration.PhysicalStateDraining, next: migration.PhysicalStateTargetEnabled},
		{name: "target before final catchup", current: migration.PhysicalStateSourceFenced, next: migration.PhysicalStateTargetEnabled},
		{name: "assignment before target enable", current: migration.PhysicalStateFinalValidating, next: migration.PhysicalStateSwitchingAssignment},
		{name: "assignment completion before switch checkpoint", current: migration.PhysicalStateTargetEnabled, next: migration.PhysicalStateRollbackWindow},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			machine, err := migration.RestorePhysical(migration.PhysicalSnapshot{
				State:      testCase.current,
				Generation: 47,
			})
			if err != nil {
				t.Fatalf("RestorePhysical returned error: %v", err)
			}
			if err := machine.Transition(testCase.next); !errors.Is(err, migration.ErrInvalidPhysicalTransition) {
				t.Fatalf("Transition(%q) error = %v, want ErrInvalidPhysicalTransition", testCase.next, err)
			}
			if got := machine.Snapshot().State; got != testCase.current {
				t.Fatalf("state after rejected transition = %q, want %q", got, testCase.current)
			}
		})
	}
}

func TestRestorePhysicalRejectsTargetWriteEvidenceBeforeAssignmentSwitch(t *testing.T) {
	t.Parallel()

	_, err := migration.RestorePhysical(migration.PhysicalSnapshot{
		State:               migration.PhysicalStateTargetEnabled,
		Generation:          53,
		TargetWriteEvidence: true,
	})
	if !errors.Is(err, migration.ErrInvalidPhysicalSnapshot) {
		t.Fatalf("RestorePhysical error = %v, want ErrInvalidPhysicalSnapshot", err)
	}
}

func TestPhysicalTargetWriteEvidenceValidatesStateAndGeneration(t *testing.T) {
	t.Parallel()

	planned, err := migration.NewPhysical(59)
	if err != nil {
		t.Fatalf("NewPhysical returned error: %v", err)
	}
	if err := planned.RecordTargetWrite(59); !errors.Is(err, migration.ErrTargetWriteState) {
		t.Fatalf("planned RecordTargetWrite error = %v, want ErrTargetWriteState", err)
	}

	assigned, err := migration.RestorePhysical(migration.PhysicalSnapshot{
		State:      migration.PhysicalStateRollbackWindow,
		Generation: 59,
	})
	if err != nil {
		t.Fatalf("RestorePhysical returned error: %v", err)
	}
	if err := assigned.RecordTargetWrite(60); !errors.Is(err, migration.ErrTargetWriteGeneration) {
		t.Fatalf("wrong-generation error = %v, want ErrTargetWriteGeneration", err)
	}
	if err := assigned.RecordTargetWrite(0); !errors.Is(err, migration.ErrInvalidGeneration) {
		t.Fatalf("zero-generation error = %v, want ErrInvalidGeneration", err)
	}
}
