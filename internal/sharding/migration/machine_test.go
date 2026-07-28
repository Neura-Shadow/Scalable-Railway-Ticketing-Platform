package migration_test

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/migration"
)

func TestNewStartsPlannedAtRequestedGeneration(t *testing.T) {
	t.Parallel()

	machine, err := migration.New(1)
	if err != nil {
		t.Fatalf("New(1) returned error: %v", err)
	}
	if got := machine.State(); got != migration.StatePlanned {
		t.Fatalf("State() = %q, want %q", got, migration.StatePlanned)
	}
	if got := machine.Generation(); got != 1 {
		t.Fatalf("Generation() = %d, want 1", got)
	}
}

func TestNewRejectsNonPositiveGeneration(t *testing.T) {
	t.Parallel()

	for _, generation := range []int64{0, -1} {
		generation := generation
		t.Run("generation", func(t *testing.T) {
			t.Parallel()
			if _, err := migration.New(generation); err == nil {
				t.Fatalf("New(%d) unexpectedly succeeded", generation)
			}
		})
	}
}

func TestTransitionAdvancesAlongTheNormalMigrationPath(t *testing.T) {
	t.Parallel()

	machine, err := migration.New(3)
	if err != nil {
		t.Fatalf("New(3) returned error: %v", err)
	}
	for _, next := range []migration.State{
		migration.StateDraining,
		migration.StateCopying,
		migration.StateValidating,
		migration.StateCutoverReady,
		migration.StateCuttingOver,
		migration.StateRollbackWindow,
		migration.StateCompleted,
	} {
		if err := machine.Transition(next); err != nil {
			t.Fatalf("Transition(%q) from normal path returned error: %v", next, err)
		}
		if got := machine.State(); got != next {
			t.Fatalf("State() = %q, want %q", got, next)
		}
	}
}

func TestTransitionRejectsSkippingTheMigrationLifecycle(t *testing.T) {
	t.Parallel()

	machine, err := migration.New(1)
	if err != nil {
		t.Fatalf("New(1) returned error: %v", err)
	}
	if err := machine.Transition(migration.StateCopying); err == nil {
		t.Fatal("Transition(copying) from planned unexpectedly succeeded")
	}
	if got := machine.State(); got != migration.StatePlanned {
		t.Fatalf("State() after rejected transition = %q, want planned", got)
	}
}

func TestRollbackWindowAllowsDirectRollbackBeforeAnyTargetWrite(t *testing.T) {
	t.Parallel()

	machine, err := migration.New(4)
	if err != nil {
		t.Fatalf("New(4) returned error: %v", err)
	}
	for _, next := range []migration.State{
		migration.StateDraining,
		migration.StateCopying,
		migration.StateValidating,
		migration.StateCutoverReady,
		migration.StateCuttingOver,
		migration.StateRollbackWindow,
	} {
		if err := machine.Transition(next); err != nil {
			t.Fatalf("Transition(%q) returned error: %v", next, err)
		}
	}
	if err := machine.Transition(migration.StateRolledBack); err != nil {
		t.Fatalf("Transition(rolled_back) returned error: %v", err)
	}
	if got := machine.State(); got != migration.StateRolledBack {
		t.Fatalf("State() = %q, want rolled_back", got)
	}
}

func TestTargetWriteEvidencePreventsDirectRollback(t *testing.T) {
	t.Parallel()

	machine, err := migration.New(5)
	if err != nil {
		t.Fatalf("New(5) returned error: %v", err)
	}
	for _, next := range []migration.State{
		migration.StateDraining,
		migration.StateCopying,
		migration.StateValidating,
		migration.StateCutoverReady,
		migration.StateCuttingOver,
		migration.StateRollbackWindow,
	} {
		if err := machine.Transition(next); err != nil {
			t.Fatalf("Transition(%q) returned error: %v", next, err)
		}
	}
	if err := machine.RecordTargetWrite(5); err != nil {
		t.Fatalf("RecordTargetWrite(5) returned error: %v", err)
	}
	if err := machine.Transition(migration.StateRolledBack); err == nil {
		t.Fatal("Transition(rolled_back) with target write evidence unexpectedly succeeded")
	}
	if got := machine.State(); got != migration.StateRollbackWindow {
		t.Fatalf("State() after rejected rollback = %q, want rollback_window", got)
	}
}

func TestPreCutoverPhasesCanRollBackWithoutChangingAuthority(t *testing.T) {
	t.Parallel()

	phases := [][]migration.State{
		nil,
		{migration.StateDraining},
		{migration.StateDraining, migration.StateCopying},
		{migration.StateDraining, migration.StateCopying, migration.StateValidating},
		{migration.StateDraining, migration.StateCopying, migration.StateValidating, migration.StateCutoverReady},
	}
	for _, phasesToReach := range phases {
		machine := mustMachineAt(t, 11, phasesToReach...)
		if err := machine.Transition(migration.StateRolledBack); err != nil {
			t.Fatalf("Transition(rolled_back) from %q returned error: %v", machine.State(), err)
		}
	}
}

func TestRecordTargetWriteRejectsNonPositiveGeneration(t *testing.T) {
	t.Parallel()

	machine := mustMachineAt(t, 9,
		migration.StateDraining,
		migration.StateCopying,
		migration.StateValidating,
		migration.StateCutoverReady,
		migration.StateCuttingOver,
	)
	if err := machine.RecordTargetWrite(0); !errors.Is(err, migration.ErrInvalidGeneration) {
		t.Fatalf("RecordTargetWrite(0) error = %v, want ErrInvalidGeneration", err)
	}
}

func TestTerminalStatesCannotResume(t *testing.T) {
	t.Parallel()

	completed := mustMachineAt(t, 6,
		migration.StateDraining,
		migration.StateCopying,
		migration.StateValidating,
		migration.StateCutoverReady,
		migration.StateCuttingOver,
		migration.StateRollbackWindow,
		migration.StateCompleted,
	)
	failed := mustMachineAt(t, 7, migration.StateFailed)
	rolledBack := mustMachineAt(t, 8,
		migration.StateDraining,
		migration.StateCopying,
		migration.StateValidating,
		migration.StateCutoverReady,
		migration.StateCuttingOver,
		migration.StateRollbackWindow,
		migration.StateRolledBack,
	)

	for name, machine := range map[string]*migration.Machine{
		"completed":   completed,
		"failed":      failed,
		"rolled_back": rolledBack,
	} {
		machine := machine
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := machine.Transition(migration.StateDraining); err == nil {
				t.Fatalf("terminal %q state resumed", name)
			}
		})
	}
}

func mustMachineAt(t *testing.T, generation int64, states ...migration.State) *migration.Machine {
	t.Helper()
	machine, err := migration.New(generation)
	if err != nil {
		t.Fatalf("New(%d) returned error: %v", generation, err)
	}
	for _, state := range states {
		if err := machine.Transition(state); err != nil {
			t.Fatalf("Transition(%q) returned error: %v", state, err)
		}
	}
	return machine
}
