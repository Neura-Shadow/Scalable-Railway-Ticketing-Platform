package recovery

import (
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
)

// Evidence is a sealed set of typed failover observations. Callers cannot pass
// an arbitrary phase name, host, or command.
type Evidence interface{ failoverEvidence() }

type ExternalFencingVerified struct{ Attestation FencingAttestation }
type PositionsRecorded struct {
	Positions DatabaseSet[ReplicationPosition]
}
type PassiveReadinessRemoved struct{ Observation ObservationHash }
type DatabasePromoted struct {
	Database Database
	Position ReplicationPosition
}
type RolesAndTimelinesVerified struct {
	Databases DatabaseSet[DatabaseVerification]
}
type EpochAllocated struct{ Epoch authority.Epoch }
type ControlRecoveryInstalled struct{ Authority authority.Snapshot }
type ShardAuthoritiesInstalled struct{ Authorities ShardAuthoritySet }
type RecoveryAPIsStarted struct{ Observation ObservationHash }
type ReconciliationPassed struct {
	Control     bool
	Shards      bool
	Payments    bool
	Tickets     bool
	Refunds     bool
	Ledger      bool
	Routing     bool
	Observation ObservationHash
}
type PaymentWorkersEnabled struct{ Observation ObservationHash }
type SettlementWorkersEnabled struct{ Observation ObservationHash }
type IngressSwitched struct {
	Webhook     bool
	Global      bool
	Observation ObservationHash
}
type CustomerWritesConfigured struct {
	Enabled        bool
	ReadinessGated bool
	Observation    ObservationHash
}
type RTORecorded struct{ Duration time.Duration }
type RPORecorded struct{ Loss DatabaseSet[Loss] }
type TargetActivated struct {
	Authorities AuthoritySet
	ObservedAt  time.Time
}
type SourceRetainedFenced struct{ Attestation FencingAttestation }

func (ExternalFencingVerified) failoverEvidence()   {}
func (PositionsRecorded) failoverEvidence()         {}
func (PassiveReadinessRemoved) failoverEvidence()   {}
func (DatabasePromoted) failoverEvidence()          {}
func (RolesAndTimelinesVerified) failoverEvidence() {}
func (EpochAllocated) failoverEvidence()            {}
func (ControlRecoveryInstalled) failoverEvidence()  {}
func (ShardAuthoritiesInstalled) failoverEvidence() {}
func (RecoveryAPIsStarted) failoverEvidence()       {}
func (ReconciliationPassed) failoverEvidence()      {}
func (PaymentWorkersEnabled) failoverEvidence()     {}
func (SettlementWorkersEnabled) failoverEvidence()  {}
func (IngressSwitched) failoverEvidence()           {}
func (CustomerWritesConfigured) failoverEvidence()  {}
func (RTORecorded) failoverEvidence()               {}
func (RPORecorded) failoverEvidence()               {}
func (TargetActivated) failoverEvidence()           {}
func (SourceRetainedFenced) failoverEvidence()      {}

func evidenceStage(evidence Evidence) (Stage, error) {
	switch value := evidence.(type) {
	case ExternalFencingVerified:
		return StageExternalFencingVerified, nil
	case PositionsRecorded:
		return StagePositionsRecorded, nil
	case PassiveReadinessRemoved:
		return StagePassiveReadinessRemoved, nil
	case DatabasePromoted:
		switch value.Database {
		case DatabaseControl:
			return StageControlPromoted, nil
		case DatabaseShard0:
			return StageShard0Promoted, nil
		case DatabaseShard1:
			return StageShard1Promoted, nil
		default:
			return StagePlanned, ErrInvalidEvidence
		}
	case RolesAndTimelinesVerified:
		return StageRolesAndTimelinesVerified, nil
	case EpochAllocated:
		return StageEpochAllocated, nil
	case ControlRecoveryInstalled:
		return StageControlRecoveryInstalled, nil
	case ShardAuthoritiesInstalled:
		return StageShardAuthoritiesInstalled, nil
	case RecoveryAPIsStarted:
		return StageRecoveryAPIsStarted, nil
	case ReconciliationPassed:
		return StageReconciled, nil
	case PaymentWorkersEnabled:
		return StagePaymentWorkersEnabled, nil
	case SettlementWorkersEnabled:
		return StageSettlementWorkersEnabled, nil
	case IngressSwitched:
		return StageIngressSwitched, nil
	case CustomerWritesConfigured:
		return StageCustomerWritesConfigured, nil
	case TargetActivated:
		return StageTargetActive, nil
	case RTORecorded:
		return StageRTORecorded, nil
	case RPORecorded:
		return StageRPORecorded, nil
	case SourceRetainedFenced:
		return StageSourceRetainedFenced, nil
	default:
		return StagePlanned, ErrInvalidEvidence
	}
}
