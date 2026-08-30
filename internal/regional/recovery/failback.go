package recovery

import (
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
)

var (
	ErrInvalidReseedProvenance = errors.New("invalid failback reseed provenance")
	ErrFailbackEpochNotNewer   = errors.New("failback epoch must be newer")
	ErrInvalidFailback         = errors.New("invalid failback plan")
)

// ReseedProvenance proves that one fresh standby was created from the current
// active region, caught up, and reconciled. It never permits reuse of divergent
// former-primary data.
type ReseedProvenance struct {
	sourceRegion     authority.Region
	sourceEpoch      authority.Epoch
	startedAt        time.Time
	completedAt      time.Time
	sourcePosition   ReplicationPosition
	replayedPosition ReplicationPosition
	reconciled       bool
}

func NewReseedProvenance(
	sourceRegion authority.Region,
	sourceEpoch authority.Epoch,
	startedAt time.Time,
	completedAt time.Time,
	sourcePosition ReplicationPosition,
	replayedPosition ReplicationPosition,
	reconciled bool,
) (ReseedProvenance, error) {
	_, regionErr := authority.ParseRegion(sourceRegion.String())
	if regionErr != nil || sourceEpoch.Uint64() == 0 || startedAt.IsZero() ||
		completedAt.Before(startedAt) || !sourcePosition.valid() || !replayedPosition.valid() ||
		replayedPosition.timeline < sourcePosition.timeline || replayedPosition.wal < sourcePosition.wal {
		return ReseedProvenance{}, ErrInvalidReseedProvenance
	}
	return ReseedProvenance{
		sourceRegion:     sourceRegion,
		sourceEpoch:      sourceEpoch,
		startedAt:        startedAt.UTC(),
		completedAt:      completedAt.UTC(),
		sourcePosition:   sourcePosition,
		replayedPosition: replayedPosition,
		reconciled:       reconciled,
	}, nil
}

func (provenance ReseedProvenance) SourceRegion() authority.Region { return provenance.sourceRegion }
func (provenance ReseedProvenance) SourceEpoch() authority.Epoch   { return provenance.sourceEpoch }
func (provenance ReseedProvenance) StartedAt() time.Time           { return provenance.startedAt }
func (provenance ReseedProvenance) CompletedAt() time.Time         { return provenance.completedAt }
func (provenance ReseedProvenance) SourcePosition() ReplicationPosition {
	return provenance.sourcePosition
}
func (provenance ReseedProvenance) ReplayedPosition() ReplicationPosition {
	return provenance.replayedPosition
}
func (provenance ReseedProvenance) Reconciled() bool { return provenance.reconciled }

// FailbackPlan is promotion-ready only after every bounded prerequisite has
// been validated. Execution still uses the same operator-controlled promotion
// and authority installation interfaces as failover.
type FailbackPlan struct {
	binding      FenceBinding
	target       authority.Region
	targetEpoch  authority.Epoch
	reseedAfter  time.Time
	reseeds      DatabaseSet[ReseedProvenance]
	currentFence FencingAttestation
}

func PrepareFailback(
	binding FenceBinding,
	target authority.Region,
	targetEpoch authority.Epoch,
	reseedAfter time.Time,
	reseeds DatabaseSet[ReseedProvenance],
	currentFence FencingAttestation,
) (FailbackPlan, error) {
	_, targetErr := authority.ParseRegion(target.String())
	if targetErr != nil || target == binding.source || reseedAfter.IsZero() || reseedAfter.Before(binding.declaredAt) {
		return FailbackPlan{}, ErrInvalidFailback
	}
	if err := authority.RequireNewerEpoch(binding.sourceEpoch, targetEpoch); err != nil {
		return FailbackPlan{}, ErrFailbackEpochNotNewer
	}
	latestCompletion := reseedAfter
	if err := reseeds.Visit(func(_ Database, provenance ReseedProvenance) error {
		if provenance.sourceRegion != binding.source || provenance.sourceEpoch != binding.sourceEpoch ||
			provenance.startedAt.Before(reseedAfter) || !provenance.reconciled ||
			!provenance.sourcePosition.valid() || !provenance.replayedPosition.valid() ||
			provenance.replayedPosition.timeline < provenance.sourcePosition.timeline ||
			provenance.replayedPosition.wal < provenance.sourcePosition.wal {
			return ErrInvalidReseedProvenance
		}
		if provenance.completedAt.After(latestCompletion) {
			latestCompletion = provenance.completedAt
		}
		return nil
	}); err != nil {
		return FailbackPlan{}, err
	}
	if err := currentFence.ValidateForPurpose(binding, FencingPurposeFailbackValidation); err != nil || !currentFence.ObservedAt().After(latestCompletion) {
		return FailbackPlan{}, ErrInvalidFailback
	}
	return FailbackPlan{
		binding:      binding,
		target:       target,
		targetEpoch:  targetEpoch,
		reseedAfter:  reseedAfter.UTC(),
		reseeds:      reseeds,
		currentFence: currentFence,
	}, nil
}

func (plan FailbackPlan) Ready() bool                            { return plan.binding.operationID != [16]byte{} }
func (plan FailbackPlan) CurrentActive() authority.Region        { return plan.binding.source }
func (plan FailbackPlan) Target() authority.Region               { return plan.target }
func (plan FailbackPlan) TargetEpoch() authority.Epoch           { return plan.targetEpoch }
func (plan FailbackPlan) Reseeds() DatabaseSet[ReseedProvenance] { return plan.reseeds }
