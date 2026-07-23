// Package migration models the bounded control-plane lifecycle of one
// train-run shard migration. It intentionally has no database dependency.
package migration

import "errors"

var (
	ErrInvalidGeneration     = errors.New("migration generation must be positive")
	ErrInvalidTransition     = errors.New("invalid migration state transition")
	ErrTargetWriteGeneration = errors.New("target write generation does not match migration generation")
	ErrTargetWriteState      = errors.New("target write evidence is only valid after cutover begins")
)

// State is the externally observable lifecycle of a migration.
type State string

const (
	StatePlanned        State = "planned"
	StateDraining       State = "draining"
	StateCopying        State = "copying"
	StateValidating     State = "validating"
	StateCutoverReady   State = "cutover_ready"
	StateCuttingOver    State = "cutting_over"
	StateRollbackWindow State = "rollback_window"
	StateCompleted      State = "completed"
	StateFailed         State = "failed"
	StateRolledBack     State = "rolled_back"
)

// Machine owns a single migration lifecycle.
type Machine struct {
	state               State
	generation          int64
	targetWriteEvidence bool
}

// New starts a migration at planned. Generations are positive fencing values.
func New(generation int64) (*Machine, error) {
	if generation <= 0 {
		return nil, ErrInvalidGeneration
	}
	return &Machine{state: StatePlanned, generation: generation}, nil
}

func (m *Machine) State() State { return m.state }

func (m *Machine) Generation() int64 { return m.generation }

// RecordTargetWrite records durable evidence that the destination accepted a
// write under this migration's fencing generation. Such a migration requires a
// reverse migration; it cannot use the direct rollback path.
func (m *Machine) RecordTargetWrite(generation int64) error {
	if generation <= 0 {
		return ErrInvalidGeneration
	}
	if generation != m.generation {
		return ErrTargetWriteGeneration
	}
	if m.state != StateCuttingOver && m.state != StateRollbackWindow {
		return ErrTargetWriteState
	}
	m.targetWriteEvidence = true
	return nil
}

// Transition moves the migration through its legal forward lifecycle.
func (m *Machine) Transition(next State) error {
	if m.state == StateRollbackWindow && next == StateRolledBack && m.targetWriteEvidence {
		return ErrInvalidTransition
	}
	if !canTransition(m.state, next) {
		return ErrInvalidTransition
	}
	m.state = next
	return nil
}

func canTransition(current, next State) bool {
	switch current {
	case StatePlanned:
		return next == StateDraining || next == StateFailed || next == StateRolledBack
	case StateDraining:
		return next == StateCopying || next == StateFailed || next == StateRolledBack
	case StateCopying:
		return next == StateValidating || next == StateFailed || next == StateRolledBack
	case StateValidating:
		return next == StateCutoverReady || next == StateFailed || next == StateRolledBack
	case StateCutoverReady:
		return next == StateCuttingOver || next == StateFailed || next == StateRolledBack
	case StateCuttingOver:
		return next == StateRollbackWindow || next == StateFailed
	case StateRollbackWindow:
		return next == StateCompleted || next == StateFailed || next == StateRolledBack
	default:
		return false
	}
}
