package recovery

import (
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/google/uuid"
)

// Failover is the complete resumable core state for one bounded promotion.
// Persistence adapters journal Snapshot() after every successful Advance.
type Failover struct {
	binding           FenceBinding
	target            authority.Region
	stage             Stage
	fencing           FencingAttestation
	positions         DatabaseSet[ReplicationPosition]
	promotions        DatabaseSet[ReplicationPosition]
	targetEpoch       authority.Epoch
	controlAuthority  authority.Snapshot
	shardAuthorities  ShardAuthoritySet
	rto               time.Duration
	rpo               DatabaseSet[Loss]
	targetAuthorities AuthoritySet
	activatedAt       time.Time
	retainedFence     FencingAttestation
	actions           ActionEvidenceSet
}

// Checkpoint is the complete typed durable record required to resume one
// failover. It contains normalized identities and evidence only.
type Checkpoint struct {
	Binding           FenceBinding
	Target            authority.Region
	Stage             Stage
	Fencing           FencingAttestation
	Positions         DatabaseSet[ReplicationPosition]
	Promotions        DatabaseSet[ReplicationPosition]
	TargetEpoch       authority.Epoch
	ControlAuthority  authority.Snapshot
	ShardAuthorities  ShardAuthoritySet
	RTO               time.Duration
	RPO               DatabaseSet[Loss]
	TargetAuthorities AuthoritySet
	ActivatedAt       time.Time
	RetainedFence     FencingAttestation
	Actions           ActionEvidenceSet
}

func NewFailover(
	operationID uuid.UUID,
	source authority.Region,
	target authority.Region,
	sourceEpoch authority.Epoch,
	incidentID uuid.UUID,
	operatorID string,
	declaredAt time.Time,
) (Failover, error) {
	if source == target {
		return Failover{}, ErrInvalidFailover
	}
	binding, err := NewFenceBinding(operationID, source, sourceEpoch, incidentID, operatorID, declaredAt)
	_, targetErr := authority.ParseRegion(target.String())
	if err != nil || targetErr != nil {
		return Failover{}, ErrInvalidFailover
	}
	return Failover{binding: binding, target: target, stage: StagePlanned}, nil
}

func (operation Failover) Binding() FenceBinding        { return operation.binding }
func (operation Failover) Target() authority.Region     { return operation.target }
func (operation Failover) Stage() Stage                 { return operation.stage }
func (operation Failover) TargetEpoch() authority.Epoch { return operation.targetEpoch }

func (operation Failover) Checkpoint() Checkpoint {
	return Checkpoint{
		Binding:           operation.binding,
		Target:            operation.target,
		Stage:             operation.stage,
		Fencing:           operation.fencing,
		Positions:         operation.positions,
		Promotions:        operation.promotions,
		TargetEpoch:       operation.targetEpoch,
		ControlAuthority:  operation.controlAuthority,
		ShardAuthorities:  operation.shardAuthorities,
		RTO:               operation.rto,
		RPO:               operation.rpo,
		TargetAuthorities: operation.targetAuthorities,
		ActivatedAt:       operation.activatedAt,
		RetainedFence:     operation.retainedFence,
		Actions:           operation.actions,
	}
}

// RestoreFailover validates a journal checkpoint before allowing the next
// phase. A stage marker without the evidence required up to that stage fails
// closed.
func RestoreFailover(checkpoint Checkpoint) (Failover, error) {
	operation := Failover{
		binding:           checkpoint.Binding,
		target:            checkpoint.Target,
		stage:             checkpoint.Stage,
		fencing:           checkpoint.Fencing,
		positions:         checkpoint.Positions,
		promotions:        checkpoint.Promotions,
		targetEpoch:       checkpoint.TargetEpoch,
		controlAuthority:  checkpoint.ControlAuthority,
		shardAuthorities:  checkpoint.ShardAuthorities,
		rto:               checkpoint.RTO,
		rpo:               checkpoint.RPO,
		targetAuthorities: checkpoint.TargetAuthorities,
		activatedAt:       checkpoint.ActivatedAt,
		retainedFence:     checkpoint.RetainedFence,
		actions:           checkpoint.Actions,
	}
	if err := operation.validateCheckpoint(); err != nil {
		return Failover{}, err
	}
	return operation, nil
}

// WriteReady becomes true at the atomic target-activation checkpoint. The
// initial external fence remains a prerequisite for every preceding phase;
// the terminal source-retention phase re-observes that fence after activation.
func (operation Failover) WriteReady() bool {
	return operation.stage >= StageTargetActive && operation.stage <= StageSourceRetainedFenced
}

func (operation Failover) validateCheckpoint() error {
	if !operation.stage.valid() || operation.target.String() == "" || operation.target == operation.binding.source {
		return ErrInvalidFailover
	}
	if _, err := NewFenceBinding(
		operation.binding.operationID,
		operation.binding.source,
		operation.binding.sourceEpoch,
		operation.binding.incidentID,
		operation.binding.operatorID,
		operation.binding.declaredAt,
	); err != nil {
		return ErrInvalidFailover
	}
	if operation.stage >= StageExternalFencingVerified {
		if err := operation.validateCurrentFenceStructure(); err != nil {
			return ErrInvalidFailover
		}
	}
	if operation.stage >= StagePositionsRecorded && !positionsValid(operation.positions) {
		return ErrInvalidFailover
	}
	if operation.stage >= StagePassiveReadinessRemoved && !operation.actions.PassiveReadiness.valid() {
		return ErrInvalidFailover
	}
	for _, member := range []struct {
		stage    Stage
		database Database
	}{
		{stage: StageControlPromoted, database: DatabaseControl},
		{stage: StageShard0Promoted, database: DatabaseShard0},
		{stage: StageShard1Promoted, database: DatabaseShard1},
	} {
		if operation.stage < member.stage {
			continue
		}
		source, _ := operation.positions.Value(member.database)
		promoted, _ := operation.promotions.Value(member.database)
		if !promoted.valid() || promoted.timeline <= source.timeline || promoted.wal < source.wal {
			return ErrInvalidFailover
		}
	}
	if operation.stage >= StageEpochAllocated {
		if err := authority.RequireNewerEpoch(operation.binding.sourceEpoch, operation.targetEpoch); err != nil {
			return ErrInvalidFailover
		}
	}
	if operation.stage >= StageControlRecoveryInstalled &&
		!matchesAuthority(operation.controlAuthority, operation.target, operation.targetEpoch, authority.StateRecovery, false) {
		return ErrInvalidFailover
	}
	if operation.stage >= StageShardAuthoritiesInstalled &&
		(!matchesAuthority(operation.shardAuthorities.shard0, operation.target, operation.targetEpoch, authority.StateRecovery, false) ||
			!matchesAuthority(operation.shardAuthorities.shard1, operation.target, operation.targetEpoch, authority.StateRecovery, false)) {
		return ErrInvalidFailover
	}
	for _, required := range []struct {
		stage Stage
		hash  ObservationHash
	}{
		{StageRecoveryAPIsStarted, operation.actions.RecoveryAPIs},
		{StageReconciled, operation.actions.Reconciliation},
		{StagePaymentWorkersEnabled, operation.actions.PaymentWorkers},
		{StageSettlementWorkersEnabled, operation.actions.SettlementWorkers},
		{StageIngressSwitched, operation.actions.Ingress},
		{StageCustomerWritesConfigured, operation.actions.CustomerWrites},
	} {
		if operation.stage >= required.stage && !required.hash.valid() {
			return ErrInvalidFailover
		}
	}
	if operation.stage >= StageTargetActive {
		if operation.activatedAt.IsZero() || operation.activatedAt.Before(operation.binding.declaredAt) {
			return ErrInvalidFailover
		}
		for _, snapshot := range []authority.Snapshot{
			operation.targetAuthorities.control,
			operation.targetAuthorities.shard0,
			operation.targetAuthorities.shard1,
		} {
			if !matchesAuthority(snapshot, operation.target, operation.targetEpoch, authority.StateActive, true) {
				return ErrInvalidFailover
			}
		}
	}
	if operation.stage >= StageRTORecorded && operation.rto <= 0 {
		return ErrInvalidFailover
	}
	if operation.stage >= StageRPORecorded && !lossesValid(operation.rpo) {
		return ErrInvalidFailover
	}
	if operation.stage >= StageSourceRetainedFenced {
		if err := operation.retainedFence.ValidateForPurpose(operation.binding, FencingPurposeRetainedSource); err != nil ||
			!operation.retainedFence.ObservedAt().After(operation.fencing.ObservedAt()) ||
			!operation.retainedFence.ObservedAt().After(operation.activatedAt) ||
			operation.retainedFence.Nonce() == operation.fencing.Nonce() {
			return ErrInvalidFailover
		}
	}
	return nil
}

func (operation Failover) validateCurrentFenceStructure() error {
	if operation.fencing.Purpose() != FencingPurposeInitial && operation.fencing.Purpose() != FencingPurposeOngoing {
		return ErrInvalidFailover
	}
	return operation.fencing.ValidateForPurpose(operation.binding, operation.fencing.Purpose())
}

// ValidateFreshFence revalidates the durable fence against the independent
// verifier clock before another phase can advance.
func (operation Failover) ValidateFreshFence(verifier FencingVerifier) error {
	if operation.stage < StageExternalFencingVerified || operation.stage >= StageSourceRetainedFenced {
		return nil
	}
	if err := operation.validateCurrentFenceStructure(); err != nil {
		return err
	}
	return verifier.Verify(operation.fencing)
}

// RefreshFence replaces only the current-source fence while preserving the
// phase marker. A same-purpose signed observation must be strictly newer.
func RefreshFence(operation Failover, attestation FencingAttestation) (Failover, error) {
	if operation.stage < StageExternalFencingVerified || operation.stage >= StageSourceRetainedFenced ||
		attestation.ValidateForPurpose(operation.binding, FencingPurposeOngoing) != nil ||
		!attestation.ObservedAt().After(operation.fencing.ObservedAt()) || attestation.Nonce() == operation.fencing.Nonce() {
		return operation, ErrInvalidEvidence
	}
	operation.fencing = attestation
	return operation, nil
}

// Advance consumes one typed observation for exactly the next fixed phase.
// Repeating the immediately completed phase with identical evidence is a
// no-op, enabling crash-safe command retries without allocating another epoch.
func Advance(operation Failover, evidence Evidence) (Failover, error) {
	if !operation.stage.valid() || evidence == nil {
		return operation, ErrInvalidFailover
	}
	targetStage, err := evidenceStage(evidence)
	if err != nil {
		return operation, err
	}
	if targetStage == operation.stage {
		if err := operation.validateReplay(evidence); err != nil {
			return operation, err
		}
		return operation, nil
	}
	if targetStage != operation.stage+1 {
		return operation, ErrFailoverOutOfOrder
	}
	if err := operation.apply(evidence); err != nil {
		return operation, err
	}
	operation.stage = targetStage
	return operation, nil
}

func (operation *Failover) apply(evidence Evidence) error {
	switch value := evidence.(type) {
	case ExternalFencingVerified:
		if err := value.Attestation.ValidateForPurpose(operation.binding, FencingPurposeInitial); err != nil {
			return err
		}
		operation.fencing = value.Attestation
	case PositionsRecorded:
		if !positionsValid(value.Positions) {
			return ErrInvalidEvidence
		}
		operation.positions = value.Positions
	case PassiveReadinessRemoved:
		if !value.Observation.valid() {
			return ErrInvalidEvidence
		}
		operation.actions.PassiveReadiness = value.Observation
	case RecoveryAPIsStarted:
		if !value.Observation.valid() {
			return ErrInvalidEvidence
		}
		operation.actions.RecoveryAPIs = value.Observation
	case PaymentWorkersEnabled:
		if !value.Observation.valid() {
			return ErrInvalidEvidence
		}
		operation.actions.PaymentWorkers = value.Observation
	case SettlementWorkersEnabled:
		if !value.Observation.valid() {
			return ErrInvalidEvidence
		}
		operation.actions.SettlementWorkers = value.Observation
	case DatabasePromoted:
		if !value.Position.valid() {
			return ErrInvalidEvidence
		}
		source, err := operation.positions.Value(value.Database)
		if err != nil || value.Position.timeline <= source.timeline || value.Position.wal < source.wal {
			return ErrInvalidEvidence
		}
		operation.setPromotion(value.Database, value.Position)
	case RolesAndTimelinesVerified:
		if !verificationsValid(value.Databases, operation.promotions) {
			return ErrInvalidVerification
		}
	case EpochAllocated:
		if err := authority.RequireNewerEpoch(operation.binding.sourceEpoch, value.Epoch); err != nil {
			return err
		}
		operation.targetEpoch = value.Epoch
	case ControlRecoveryInstalled:
		if !matchesAuthority(value.Authority, operation.target, operation.targetEpoch, authority.StateRecovery, false) {
			return ErrInvalidEvidence
		}
		operation.controlAuthority = value.Authority
	case ShardAuthoritiesInstalled:
		if !matchesAuthority(value.Authorities.shard0, operation.target, operation.targetEpoch, authority.StateRecovery, false) ||
			!matchesAuthority(value.Authorities.shard1, operation.target, operation.targetEpoch, authority.StateRecovery, false) {
			return ErrInvalidEvidence
		}
		operation.shardAuthorities = value.Authorities
	case ReconciliationPassed:
		if !value.Control || !value.Shards || !value.Payments || !value.Tickets || !value.Refunds || !value.Ledger || !value.Routing || !value.Observation.valid() {
			return ErrInvalidEvidence
		}
		operation.actions.Reconciliation = value.Observation
	case IngressSwitched:
		if !value.Webhook || !value.Global || !value.Observation.valid() {
			return ErrInvalidEvidence
		}
		operation.actions.Ingress = value.Observation
	case CustomerWritesConfigured:
		if !value.Enabled || !value.ReadinessGated || !value.Observation.valid() {
			return ErrInvalidEvidence
		}
		operation.actions.CustomerWrites = value.Observation
	case TargetActivated:
		if value.ObservedAt.IsZero() || value.ObservedAt.Before(operation.binding.declaredAt) {
			return ErrInvalidEvidence
		}
		for _, snapshot := range []authority.Snapshot{value.Authorities.control, value.Authorities.shard0, value.Authorities.shard1} {
			if !matchesAuthority(snapshot, operation.target, operation.targetEpoch, authority.StateActive, true) {
				return ErrInvalidEvidence
			}
		}
		operation.targetAuthorities = value.Authorities
		operation.activatedAt = value.ObservedAt.UTC()
	case RTORecorded:
		if value.Duration <= 0 {
			return ErrInvalidEvidence
		}
		operation.rto = value.Duration
	case RPORecorded:
		if !lossesValid(value.Loss) {
			return ErrInvalidEvidence
		}
		operation.rpo = value.Loss
	case SourceRetainedFenced:
		if err := value.Attestation.ValidateForPurpose(operation.binding, FencingPurposeRetainedSource); err != nil || !value.Attestation.ObservedAt().After(operation.fencing.ObservedAt()) || !value.Attestation.ObservedAt().After(operation.activatedAt) || value.Attestation.Nonce() == operation.fencing.Nonce() {
			return ErrInvalidEvidence
		}
		operation.retainedFence = value.Attestation
	default:
		return ErrInvalidEvidence
	}
	return nil
}

func (operation Failover) validateReplay(evidence Evidence) error {
	switch value := evidence.(type) {
	case ExternalFencingVerified:
		if !operation.fencing.equal(value.Attestation) {
			return ErrEvidenceConflict
		}
	case PositionsRecorded:
		if operation.positions != value.Positions {
			return ErrEvidenceConflict
		}
	case PassiveReadinessRemoved:
		if operation.actions.PassiveReadiness != value.Observation {
			return ErrEvidenceConflict
		}
	case DatabasePromoted:
		position, err := operation.promotions.Value(value.Database)
		if err != nil || position != value.Position {
			return ErrEvidenceConflict
		}
	case EpochAllocated:
		if operation.targetEpoch != value.Epoch {
			return ErrEvidenceConflict
		}
	case ControlRecoveryInstalled:
		if operation.controlAuthority != value.Authority {
			return ErrEvidenceConflict
		}
	case ShardAuthoritiesInstalled:
		if operation.shardAuthorities != value.Authorities {
			return ErrEvidenceConflict
		}
	case TargetActivated:
		if operation.targetAuthorities != value.Authorities || !operation.activatedAt.Equal(value.ObservedAt) {
			return ErrEvidenceConflict
		}
	case RTORecorded:
		if operation.rto != value.Duration {
			return ErrEvidenceConflict
		}
	case RPORecorded:
		if operation.rpo != value.Loss {
			return ErrEvidenceConflict
		}
	case SourceRetainedFenced:
		if !operation.retainedFence.equal(value.Attestation) {
			return ErrEvidenceConflict
		}
	case RolesAndTimelinesVerified:
		if !verificationsValid(value.Databases, operation.promotions) {
			return ErrEvidenceConflict
		}
	case ReconciliationPassed:
		if !value.Control || !value.Shards || !value.Payments || !value.Tickets ||
			!value.Refunds || !value.Ledger || !value.Routing || operation.actions.Reconciliation != value.Observation {
			return ErrEvidenceConflict
		}
	case RecoveryAPIsStarted:
		if operation.actions.RecoveryAPIs != value.Observation {
			return ErrEvidenceConflict
		}
	case PaymentWorkersEnabled:
		if operation.actions.PaymentWorkers != value.Observation {
			return ErrEvidenceConflict
		}
	case SettlementWorkersEnabled:
		if operation.actions.SettlementWorkers != value.Observation {
			return ErrEvidenceConflict
		}
	case IngressSwitched:
		if !value.Webhook || !value.Global || operation.actions.Ingress != value.Observation {
			return ErrEvidenceConflict
		}
	case CustomerWritesConfigured:
		if !value.Enabled || !value.ReadinessGated || operation.actions.CustomerWrites != value.Observation {
			return ErrEvidenceConflict
		}
	}
	return nil
}

func (operation *Failover) setPromotion(database Database, position ReplicationPosition) {
	switch database {
	case DatabaseControl:
		operation.promotions.control = position
	case DatabaseShard0:
		operation.promotions.shard0 = position
	case DatabaseShard1:
		operation.promotions.shard1 = position
	}
}

func positionsValid(positions DatabaseSet[ReplicationPosition]) bool {
	return positions.control.valid() && positions.shard0.valid() && positions.shard1.valid()
}

func verificationsValid(
	verifications DatabaseSet[DatabaseVerification],
	promotions DatabaseSet[ReplicationPosition],
) bool {
	return verifications.control.validPromoted(promotions.control) &&
		verifications.shard0.validPromoted(promotions.shard0) &&
		verifications.shard1.validPromoted(promotions.shard1)
}

func lossesValid(losses DatabaseSet[Loss]) bool {
	return losses.control.valid() && losses.shard0.valid() && losses.shard1.valid()
}

func matchesAuthority(
	snapshot authority.Snapshot,
	region authority.Region,
	epoch authority.Epoch,
	state authority.State,
	writesEnabled bool,
) bool {
	return snapshot.Region() == region && snapshot.Epoch() == epoch &&
		snapshot.State() == state && snapshot.WritesEnabled() == writesEnabled
}
