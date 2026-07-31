package migration

import "errors"

var (
	ErrInvalidPhysicalTransition         = errors.New("invalid physical migration state transition")
	ErrInvalidPhysicalSnapshot           = errors.New("invalid physical migration snapshot")
	ErrPhysicalReverseMigrationRequired  = errors.New("target write evidence requires reverse migration")
	ErrPhysicalReverseGenerationNotNewer = errors.New("reverse migration generation must be newer")
)

// PhysicalState is a durable checkpoint in a cross-database train-run move.
type PhysicalState string

const (
	PhysicalStatePlanned                  PhysicalState = "planned"
	PhysicalStatePreparingTarget          PhysicalState = "preparing_target"
	PhysicalStateCaptureEnabled           PhysicalState = "capture_enabled"
	PhysicalStateBaseCopying              PhysicalState = "base_copying"
	PhysicalStateCatchingUp               PhysicalState = "catching_up"
	PhysicalStateValidatingOnline         PhysicalState = "validating_online"
	PhysicalStateDraining                 PhysicalState = "draining"
	PhysicalStateSourceFenced             PhysicalState = "source_fenced"
	PhysicalStateFinalCatchup             PhysicalState = "final_catchup"
	PhysicalStateFinalValidating          PhysicalState = "final_validating"
	PhysicalStateTargetEnabled            PhysicalState = "target_enabled"
	PhysicalStateSwitchingAssignment      PhysicalState = "switching_assignment"
	PhysicalStateRollbackWindow           PhysicalState = "rollback_window"
	PhysicalStateCompleted                PhysicalState = "completed"
	PhysicalStateReverseMigrationRequired PhysicalState = "reverse_migration_required"
	PhysicalStateFailed                   PhysicalState = "failed"
	PhysicalStateRolledBack               PhysicalState = "rolled_back"
)

// PhysicalSnapshot is sufficient to persist and reconstruct the state machine.
type PhysicalSnapshot struct {
	State               PhysicalState
	Generation          int64
	TargetWriteEvidence bool
}

// PhysicalMachine owns the ordered control checkpoints for one physical move.
type PhysicalMachine struct {
	snapshot PhysicalSnapshot
}

// NewPhysical starts a physical migration at planned.
func NewPhysical(generation int64) (*PhysicalMachine, error) {
	if generation <= 0 {
		return nil, ErrInvalidGeneration
	}
	return &PhysicalMachine{snapshot: PhysicalSnapshot{
		State:      PhysicalStatePlanned,
		Generation: generation,
	}}, nil
}

// RestorePhysical reconstructs a durable checkpoint after a process crash.
func RestorePhysical(snapshot PhysicalSnapshot) (*PhysicalMachine, error) {
	if snapshot.Generation <= 0 {
		return nil, ErrInvalidGeneration
	}
	if !isPhysicalState(snapshot.State) {
		return nil, ErrInvalidPhysicalSnapshot
	}
	if snapshot.TargetWriteEvidence && !canContainTargetWriteEvidence(snapshot.State) {
		return nil, ErrInvalidPhysicalSnapshot
	}
	return &PhysicalMachine{snapshot: snapshot}, nil
}

// Snapshot returns the durable state needed after a process crash.
func (m *PhysicalMachine) Snapshot() PhysicalSnapshot { return m.snapshot }

// RecordTargetWrite records durable evidence that the assigned target accepted
// a non-replay write under this generation.
func (m *PhysicalMachine) RecordTargetWrite(generation int64) error {
	if generation <= 0 {
		return ErrInvalidGeneration
	}
	if generation != m.snapshot.Generation {
		return ErrTargetWriteGeneration
	}
	if m.snapshot.State != PhysicalStateSwitchingAssignment &&
		m.snapshot.State != PhysicalStateRollbackWindow &&
		m.snapshot.State != PhysicalStateCompleted &&
		m.snapshot.State != PhysicalStateReverseMigrationRequired {
		return ErrTargetWriteState
	}
	m.snapshot.TargetWriteEvidence = true
	return nil
}

func canContainTargetWriteEvidence(state PhysicalState) bool {
	switch state {
	case PhysicalStateSwitchingAssignment,
		PhysicalStateRollbackWindow,
		PhysicalStateCompleted,
		PhysicalStateReverseMigrationRequired,
		PhysicalStateFailed:
		return true
	default:
		return false
	}
}

// StartReverseMigration marks the current ownership as requiring a reverse
// move and returns a fresh machine with a strictly newer fencing generation.
func (m *PhysicalMachine) StartReverseMigration(generation int64) (*PhysicalMachine, error) {
	if generation <= m.snapshot.Generation {
		return nil, ErrPhysicalReverseGenerationNotNewer
	}
	if !m.snapshot.TargetWriteEvidence {
		return nil, ErrInvalidPhysicalTransition
	}
	if m.snapshot.State != PhysicalStateRollbackWindow &&
		m.snapshot.State != PhysicalStateCompleted &&
		m.snapshot.State != PhysicalStateReverseMigrationRequired {
		return nil, ErrInvalidPhysicalTransition
	}
	reverse, err := NewPhysical(generation)
	if err != nil {
		return nil, err
	}
	m.snapshot.State = PhysicalStateReverseMigrationRequired
	return reverse, nil
}

// Transition advances one explicit durable checkpoint.
func (m *PhysicalMachine) Transition(next PhysicalState) error {
	if next == PhysicalStateRolledBack && m.snapshot.TargetWriteEvidence {
		return ErrPhysicalReverseMigrationRequired
	}
	if next == PhysicalStateRolledBack && canDirectRollbackPhysical(m.snapshot.State) {
		m.snapshot.State = next
		return nil
	}
	if next == PhysicalStateFailed && canFailPhysical(m.snapshot.State) {
		m.snapshot.State = next
		return nil
	}
	if !canTransitionPhysical(m.snapshot.State, next) {
		return ErrInvalidPhysicalTransition
	}
	m.snapshot.State = next
	return nil
}

func canFailPhysical(state PhysicalState) bool {
	switch state {
	case PhysicalStatePlanned,
		PhysicalStatePreparingTarget,
		PhysicalStateCaptureEnabled,
		PhysicalStateBaseCopying,
		PhysicalStateCatchingUp,
		PhysicalStateValidatingOnline,
		PhysicalStateDraining,
		PhysicalStateSourceFenced,
		PhysicalStateFinalCatchup,
		PhysicalStateFinalValidating,
		PhysicalStateTargetEnabled,
		PhysicalStateSwitchingAssignment,
		PhysicalStateRollbackWindow:
		return true
	default:
		return false
	}
}

func canDirectRollbackPhysical(state PhysicalState) bool {
	switch state {
	case PhysicalStatePlanned,
		PhysicalStatePreparingTarget,
		PhysicalStateCaptureEnabled,
		PhysicalStateBaseCopying,
		PhysicalStateCatchingUp,
		PhysicalStateValidatingOnline,
		PhysicalStateDraining,
		PhysicalStateSourceFenced,
		PhysicalStateFinalCatchup,
		PhysicalStateFinalValidating,
		PhysicalStateTargetEnabled,
		PhysicalStateSwitchingAssignment,
		PhysicalStateRollbackWindow:
		return true
	default:
		return false
	}
}

func canTransitionPhysical(current, next PhysicalState) bool {
	switch current {
	case PhysicalStatePlanned:
		return next == PhysicalStatePreparingTarget
	case PhysicalStatePreparingTarget:
		return next == PhysicalStateCaptureEnabled
	case PhysicalStateCaptureEnabled:
		return next == PhysicalStateBaseCopying
	case PhysicalStateBaseCopying:
		return next == PhysicalStateCatchingUp
	case PhysicalStateCatchingUp:
		return next == PhysicalStateValidatingOnline
	case PhysicalStateValidatingOnline:
		return next == PhysicalStateDraining
	case PhysicalStateDraining:
		return next == PhysicalStateSourceFenced
	case PhysicalStateSourceFenced:
		return next == PhysicalStateFinalCatchup
	case PhysicalStateFinalCatchup:
		return next == PhysicalStateFinalValidating
	case PhysicalStateFinalValidating:
		return next == PhysicalStateTargetEnabled
	case PhysicalStateTargetEnabled:
		return next == PhysicalStateSwitchingAssignment
	case PhysicalStateSwitchingAssignment:
		return next == PhysicalStateRollbackWindow
	case PhysicalStateRollbackWindow:
		return next == PhysicalStateCompleted || next == PhysicalStateRolledBack
	default:
		return false
	}
}

func isPhysicalState(state PhysicalState) bool {
	switch state {
	case PhysicalStatePlanned,
		PhysicalStatePreparingTarget,
		PhysicalStateCaptureEnabled,
		PhysicalStateBaseCopying,
		PhysicalStateCatchingUp,
		PhysicalStateValidatingOnline,
		PhysicalStateDraining,
		PhysicalStateSourceFenced,
		PhysicalStateFinalCatchup,
		PhysicalStateFinalValidating,
		PhysicalStateTargetEnabled,
		PhysicalStateSwitchingAssignment,
		PhysicalStateRollbackWindow,
		PhysicalStateCompleted,
		PhysicalStateReverseMigrationRequired,
		PhysicalStateFailed,
		PhysicalStateRolledBack:
		return true
	default:
		return false
	}
}
