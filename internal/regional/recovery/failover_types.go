package recovery

import (
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
)

var (
	ErrInvalidPosition     = errors.New("invalid replication position")
	ErrInvalidVerification = errors.New("invalid promoted database verification")
	ErrInvalidEvidence     = errors.New("invalid failover evidence")
	ErrEvidenceConflict    = errors.New("failover evidence conflicts with durable checkpoint")
	ErrFailoverOutOfOrder  = errors.New("failover evidence is out of order")
	ErrInvalidFailover     = errors.New("invalid failover operation")
)

// ReplicationPosition is a normalized, non-secret WAL position. It never
// carries a DSN, slot name, host, or command.
type ReplicationPosition struct {
	timeline uint32
	wal      uint64
}

func NewReplicationPosition(timeline uint32, wal uint64) (ReplicationPosition, error) {
	if timeline == 0 || wal == 0 {
		return ReplicationPosition{}, ErrInvalidPosition
	}
	return ReplicationPosition{timeline: timeline, wal: wal}, nil
}

func (position ReplicationPosition) Timeline() uint32 { return position.timeline }
func (position ReplicationPosition) WAL() uint64      { return position.wal }
func (position ReplicationPosition) valid() bool      { return position.timeline > 0 && position.wal > 0 }

// DatabaseRole is a normalized PostgreSQL recovery observation.
type DatabaseRole string

const (
	DatabaseRolePrimary DatabaseRole = "primary"
	DatabaseRoleStandby DatabaseRole = "standby"
	DatabaseRoleRestore DatabaseRole = "restore"
)

// DatabaseVerification proves the promoted role and timeline without exposing
// connection topology.
type DatabaseVerification struct {
	Role     DatabaseRole
	Timeline uint32
}

func (verification DatabaseVerification) validPromoted(position ReplicationPosition) bool {
	return verification.Role == DatabaseRolePrimary && verification.Timeline == position.timeline
}

// ShardAuthoritySet is total for the two physical booking databases.
type ShardAuthoritySet struct {
	shard0 authority.Snapshot
	shard1 authority.Snapshot
}

// AuthoritySet is total for the fixed control/shard0/shard1 topology. It is
// used at activation so a terminal checkpoint cannot represent only one active
// database.
type AuthoritySet struct {
	control authority.Snapshot
	shard0  authority.Snapshot
	shard1  authority.Snapshot
}

func NewAuthoritySet(control, shard0, shard1 authority.Snapshot) AuthoritySet {
	return AuthoritySet{control: control, shard0: shard0, shard1: shard1}
}

func (set AuthoritySet) Control() authority.Snapshot { return set.control }
func (set AuthoritySet) Shard0() authority.Snapshot  { return set.shard0 }
func (set AuthoritySet) Shard1() authority.Snapshot  { return set.shard1 }

func NewShardAuthoritySet(shard0, shard1 authority.Snapshot) ShardAuthoritySet {
	return ShardAuthoritySet{shard0: shard0, shard1: shard1}
}

func (set ShardAuthoritySet) Shard0() authority.Snapshot { return set.shard0 }
func (set ShardAuthoritySet) Shard1() authority.Snapshot { return set.shard1 }

// Loss is an observed per-database RPO result. A zero value is an honest
// observed zero for one bounded run, not a zero-RPO guarantee.
type Loss struct {
	MissingRecords uint64
	Window         time.Duration
}

func (loss Loss) valid() bool { return loss.Window >= 0 }

// ActionEvidenceSet binds sanitized observation-artifact digests to fixed
// orchestration phases whose typed outcome would otherwise be only boolean.
type ActionEvidenceSet struct {
	PassiveReadiness  ObservationHash
	RecoveryAPIs      ObservationHash
	Reconciliation    ObservationHash
	PaymentWorkers    ObservationHash
	SettlementWorkers ObservationHash
	Ingress           ObservationHash
	CustomerWrites    ObservationHash
}

func (set ActionEvidenceSet) PassiveReadinessHash() ObservationHash  { return set.PassiveReadiness }
func (set ActionEvidenceSet) RecoveryAPIsHash() ObservationHash      { return set.RecoveryAPIs }
func (set ActionEvidenceSet) ReconciliationHash() ObservationHash    { return set.Reconciliation }
func (set ActionEvidenceSet) PaymentWorkersHash() ObservationHash    { return set.PaymentWorkers }
func (set ActionEvidenceSet) SettlementWorkersHash() ObservationHash { return set.SettlementWorkers }
func (set ActionEvidenceSet) IngressHash() ObservationHash           { return set.Ingress }
func (set ActionEvidenceSet) CustomerWritesHash() ObservationHash    { return set.CustomerWrites }

// Stage is the durable completion marker for exactly one fixed failover step.
type Stage uint8

const (
	StagePlanned Stage = iota
	StageExternalFencingVerified
	StagePositionsRecorded
	StagePassiveReadinessRemoved
	StageControlPromoted
	StageShard0Promoted
	StageShard1Promoted
	StageRolesAndTimelinesVerified
	StageEpochAllocated
	StageControlRecoveryInstalled
	StageShardAuthoritiesInstalled
	StageRecoveryAPIsStarted
	StageReconciled
	StagePaymentWorkersEnabled
	StageSettlementWorkersEnabled
	StageIngressSwitched
	StageCustomerWritesConfigured
	StageRTORecorded
	StageRPORecorded
	StageTargetActive
	StageSourceRetainedFenced
)

func (stage Stage) String() string {
	switch stage {
	case StagePlanned:
		return "planned"
	case StageExternalFencingVerified:
		return "external_fencing_verified"
	case StagePositionsRecorded:
		return "positions_recorded"
	case StagePassiveReadinessRemoved:
		return "passive_readiness_removed"
	case StageControlPromoted:
		return "control_promoted"
	case StageShard0Promoted:
		return "shard_0_promoted"
	case StageShard1Promoted:
		return "shard_1_promoted"
	case StageRolesAndTimelinesVerified:
		return "roles_and_timelines_verified"
	case StageEpochAllocated:
		return "epoch_allocated"
	case StageControlRecoveryInstalled:
		return "control_recovery_installed"
	case StageShardAuthoritiesInstalled:
		return "shard_authorities_installed"
	case StageRecoveryAPIsStarted:
		return "recovery_apis_started"
	case StageReconciled:
		return "reconciled"
	case StagePaymentWorkersEnabled:
		return "payment_workers_enabled"
	case StageSettlementWorkersEnabled:
		return "settlement_workers_enabled"
	case StageIngressSwitched:
		return "ingress_switched"
	case StageCustomerWritesConfigured:
		return "customer_writes_configured"
	case StageRTORecorded:
		return "rto_recorded"
	case StageRPORecorded:
		return "rpo_recorded"
	case StageTargetActive:
		return "target_active"
	case StageSourceRetainedFenced:
		return "source_retained_fenced"
	default:
		return "invalid"
	}
}

func (stage Stage) valid() bool { return stage <= StageSourceRetainedFenced }
