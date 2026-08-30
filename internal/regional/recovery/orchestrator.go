package recovery

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrInvalidOrchestrator = errors.New("regional recovery orchestrator invalid")

// CheckpointStore is the minimum durable CAS boundary used for one phase.
type CheckpointStore interface {
	Load(context.Context, uuid.UUID) (Failover, int64, error)
	Save(context.Context, int64, Failover, time.Time) (int64, error)
}

type FenceRefreshStore interface {
	CheckpointStore
	Refresh(context.Context, int64, Failover, time.Time) (int64, error)
}

// PhaseAction performs and verifies exactly one bounded external action. It
// returns typed evidence only after the underlying observation is complete.
type PhaseAction interface {
	Verify(context.Context, Failover) (Evidence, error)
}

// PhaseActionFunc adapts a verifier without weakening the typed boundary.
type PhaseActionFunc func(context.Context, Failover) (Evidence, error)

func (action PhaseActionFunc) Verify(ctx context.Context, operation Failover) (Evidence, error) {
	if action == nil {
		return nil, ErrInvalidOrchestrator
	}
	return action(ctx, operation)
}

type Orchestrator struct {
	store    CheckpointStore
	now      func() time.Time
	verifier FencingVerifier
}

func NewOrchestrator(store CheckpointStore, now func() time.Time, verifier FencingVerifier) (*Orchestrator, error) {
	if store == nil || now == nil || !verifier.valid() {
		return nil, ErrInvalidOrchestrator
	}
	return &Orchestrator{store: store, now: now, verifier: verifier}, nil
}

// AdvanceNext is crash-safe: it loads the latest checkpoint, verifies the
// action, advances exactly one fixed phase, then saves with the loaded version.
// An identical immediate replay does not allocate another checkpoint version.
func (orchestrator *Orchestrator) AdvanceNext(ctx context.Context, operationID uuid.UUID, action PhaseAction) (Failover, int64, error) {
	if orchestrator == nil || orchestrator.store == nil || orchestrator.now == nil || ctx == nil || operationID == uuid.Nil || action == nil {
		return Failover{}, 0, ErrInvalidOrchestrator
	}
	operation, version, err := orchestrator.store.Load(ctx, operationID)
	if err != nil || version <= 0 {
		return Failover{}, 0, err
	}
	if err := operation.ValidateFreshFence(orchestrator.verifier); err != nil {
		return operation, version, err
	}
	evidence, err := action.Verify(ctx, operation)
	if err != nil {
		return operation, version, err
	}
	advanced, err := Advance(operation, evidence)
	if err != nil {
		return operation, version, err
	}
	if advanced.Stage() == operation.Stage() {
		return advanced, version, nil
	}
	now := orchestrator.now().UTC()
	if now.IsZero() {
		return operation, version, ErrInvalidOrchestrator
	}
	nextVersion, err := orchestrator.store.Save(ctx, version, advanced, now)
	if err != nil {
		return operation, version, err
	}
	if nextVersion != version+1 {
		return operation, version, ErrInvalidOrchestrator
	}
	return advanced, nextVersion, nil
}

// RefreshFence CAS-persists a newly verified ongoing-source attestation while
// retaining the current phase marker, allowing a long-running operation to
// continue without accepting an expired checkpoint fence.
func (orchestrator *Orchestrator) RefreshFence(ctx context.Context, operationID uuid.UUID, attestation FencingAttestation) (Failover, int64, error) {
	if orchestrator == nil || orchestrator.store == nil || ctx == nil || operationID == uuid.Nil {
		return Failover{}, 0, ErrInvalidOrchestrator
	}
	if err := orchestrator.verifier.Verify(attestation); err != nil {
		return Failover{}, 0, err
	}
	operation, version, err := orchestrator.store.Load(ctx, operationID)
	if err != nil || version <= 0 {
		return Failover{}, 0, err
	}
	refreshed, err := RefreshFence(operation, attestation)
	if err != nil {
		return operation, version, err
	}
	refreshStore, ok := orchestrator.store.(FenceRefreshStore)
	if !ok {
		return operation, version, ErrInvalidOrchestrator
	}
	nextVersion, err := refreshStore.Refresh(ctx, version, refreshed, orchestrator.now().UTC())
	if err != nil {
		return operation, version, err
	}
	if nextVersion != version+1 {
		return operation, version, ErrInvalidOrchestrator
	}
	return refreshed, nextVersion, nil
}
