package recovery_test

import (
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
)

func TestFailoverAdvanceUsesTheFixedTwentyStepOrderAndActivatesWritesLast(t *testing.T) {
	t.Parallel()

	declaredAt := time.Date(2026, 8, 11, 2, 0, 0, 0, time.UTC)
	source := mustRecoveryRegion(t, "region-a")
	target := mustRecoveryRegion(t, "region-b")
	sourceEpoch := mustRecoveryEpoch(t, 41)
	operation, err := recovery.NewFailover(
		uuid.New(),
		source,
		target,
		sourceEpoch,
		uuid.New(),
		"operator:alice",
		declaredAt,
	)
	if err != nil {
		t.Fatalf("NewFailover() error = %v", err)
	}
	attestation := mustAttestation(t, operation.Binding(), declaredAt.Add(time.Minute))
	retainedAttestation := mustAttestationForPurpose(t, operation.Binding(), recovery.FencingPurposeRetainedSource, declaredAt.Add(2*time.Minute))
	sourcePositions := recovery.NewDatabaseSet(
		mustPosition(t, 7, 100),
		mustPosition(t, 5, 200),
		mustPosition(t, 9, 300),
	)
	promotedPositions := recovery.NewDatabaseSet(
		mustPosition(t, 8, 100),
		mustPosition(t, 6, 200),
		mustPosition(t, 10, 300),
	)
	verifications := recovery.NewDatabaseSet(
		recovery.DatabaseVerification{Role: recovery.DatabaseRolePrimary, Timeline: 8},
		recovery.DatabaseVerification{Role: recovery.DatabaseRolePrimary, Timeline: 6},
		recovery.DatabaseVerification{Role: recovery.DatabaseRolePrimary, Timeline: 10},
	)
	targetEpoch := mustRecoveryEpoch(t, 42)
	controlRecovery := mustRecoverySnapshot(t, target, targetEpoch, authority.StateRecovery, false)
	shardRecovery := recovery.NewShardAuthoritySet(
		mustRecoverySnapshot(t, target, targetEpoch, authority.StateRecovery, false),
		mustRecoverySnapshot(t, target, targetEpoch, authority.StateRecovery, false),
	)

	steps := []struct {
		want     recovery.Stage
		evidence recovery.Evidence
	}{
		{recovery.StageExternalFencingVerified, recovery.ExternalFencingVerified{Attestation: attestation}},
		{recovery.StagePositionsRecorded, recovery.PositionsRecorded{Positions: sourcePositions}},
		{recovery.StagePassiveReadinessRemoved, recovery.PassiveReadinessRemoved{Observation: actionHash("passive-readiness")}},
		{recovery.StageControlPromoted, recovery.DatabasePromoted{Database: recovery.DatabaseControl, Position: promotedPositions.Control()}},
		{recovery.StageShard0Promoted, recovery.DatabasePromoted{Database: recovery.DatabaseShard0, Position: promotedPositions.Shard0()}},
		{recovery.StageShard1Promoted, recovery.DatabasePromoted{Database: recovery.DatabaseShard1, Position: promotedPositions.Shard1()}},
		{recovery.StageRolesAndTimelinesVerified, recovery.RolesAndTimelinesVerified{Databases: verifications}},
		{recovery.StageEpochAllocated, recovery.EpochAllocated{Epoch: targetEpoch}},
		{recovery.StageControlRecoveryInstalled, recovery.ControlRecoveryInstalled{Authority: controlRecovery}},
		{recovery.StageShardAuthoritiesInstalled, recovery.ShardAuthoritiesInstalled{Authorities: shardRecovery}},
		{recovery.StageRecoveryAPIsStarted, recovery.RecoveryAPIsStarted{Observation: actionHash("recovery-apis")}},
		{recovery.StageReconciled, recovery.ReconciliationPassed{Control: true, Shards: true, Payments: true, Tickets: true, Refunds: true, Ledger: true, Routing: true, Observation: actionHash("reconciliation")}},
		{recovery.StagePaymentWorkersEnabled, recovery.PaymentWorkersEnabled{Observation: actionHash("payment-workers")}},
		{recovery.StageSettlementWorkersEnabled, recovery.SettlementWorkersEnabled{Observation: actionHash("settlement-workers")}},
		{recovery.StageIngressSwitched, recovery.IngressSwitched{Webhook: true, Global: true, Observation: actionHash("ingress")}},
		{recovery.StageCustomerWritesConfigured, recovery.CustomerWritesConfigured{Enabled: true, ReadinessGated: true, Observation: actionHash("customer-writes")}},
		{recovery.StageRTORecorded, recovery.RTORecorded{Duration: 4 * time.Minute}},
		{recovery.StageRPORecorded, recovery.RPORecorded{Loss: recovery.NewDatabaseSet(recovery.Loss{}, recovery.Loss{}, recovery.Loss{})}},
		{recovery.StageTargetActive, recovery.TargetActivated{Authorities: recovery.NewAuthoritySet(
			mustRecoverySnapshot(t, target, targetEpoch, authority.StateActive, true),
			mustRecoverySnapshot(t, target, targetEpoch, authority.StateActive, true),
			mustRecoverySnapshot(t, target, targetEpoch, authority.StateActive, true),
		), ObservedAt: declaredAt.Add(90 * time.Second)}},
		{recovery.StageSourceRetainedFenced, recovery.SourceRetainedFenced{Attestation: retainedAttestation}},
	}

	for index, step := range steps {
		if operation.WriteReady() {
			t.Fatalf("WriteReady() before step %d (%s) = true", index+1, step.want)
		}
		operation, err = recovery.Advance(operation, step.evidence)
		if err != nil {
			t.Fatalf("Advance(step %d, %s) error = %v", index+1, step.want, err)
		}
		if got := operation.Stage(); got != step.want {
			t.Fatalf("stage after step %d = %s, want %s", index+1, got, step.want)
		}
		if index < 18 && operation.WriteReady() {
			t.Fatalf("WriteReady() after early step %d (%s) = true", index+1, step.want)
		}
	}
	if !operation.WriteReady() {
		t.Fatal("WriteReady() after target activation and source retention = false")
	}
}

func TestFailoverCheckpointRestoresAndReplaysTheLastCompletedStep(t *testing.T) {
	t.Parallel()

	declaredAt := time.Date(2026, 8, 11, 3, 0, 0, 0, time.UTC)
	operation, err := recovery.NewFailover(
		uuid.New(),
		mustRecoveryRegion(t, "region-a"),
		mustRecoveryRegion(t, "region-b"),
		mustRecoveryEpoch(t, 51),
		uuid.New(),
		"operator:bob",
		declaredAt,
	)
	if err != nil {
		t.Fatalf("NewFailover() error = %v", err)
	}
	attestation := mustAttestation(t, operation.Binding(), declaredAt.Add(time.Minute))
	operation, err = recovery.Advance(operation, recovery.ExternalFencingVerified{Attestation: attestation})
	if err != nil {
		t.Fatalf("Advance(fencing) error = %v", err)
	}
	positions := recovery.NewDatabaseSet(
		mustPosition(t, 2, 10),
		mustPosition(t, 3, 20),
		mustPosition(t, 4, 30),
	)
	operation, err = recovery.Advance(operation, recovery.PositionsRecorded{Positions: positions})
	if err != nil {
		t.Fatalf("Advance(positions) error = %v", err)
	}

	restored, err := recovery.RestoreFailover(operation.Checkpoint())
	if err != nil {
		t.Fatalf("RestoreFailover() error = %v", err)
	}
	if got := restored.Stage(); got != recovery.StagePositionsRecorded {
		t.Fatalf("restored stage = %s, want positions_recorded", got)
	}
	replayed, err := recovery.Advance(restored, recovery.PositionsRecorded{Positions: positions})
	if err != nil {
		t.Fatalf("Advance(replayed positions) error = %v", err)
	}
	if replayed.Checkpoint() != restored.Checkpoint() {
		t.Fatal("idempotent replay changed the durable checkpoint")
	}
	advanced, err := recovery.Advance(replayed, recovery.PassiveReadinessRemoved{Observation: actionHash("passive-readiness")})
	if err != nil {
		t.Fatalf("Advance(passive readiness) error = %v", err)
	}
	if got := advanced.Stage(); got != recovery.StagePassiveReadinessRemoved {
		t.Fatalf("stage = %s, want passive_readiness_removed", got)
	}
}

func TestFailoverAdvanceRejectsSkippingExternalFencing(t *testing.T) {
	t.Parallel()

	operation, err := recovery.NewFailover(
		uuid.New(),
		mustRecoveryRegion(t, "region-a"),
		mustRecoveryRegion(t, "region-b"),
		mustRecoveryEpoch(t, 61),
		uuid.New(),
		"operator:carol",
		time.Date(2026, 8, 11, 4, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("NewFailover() error = %v", err)
	}
	positions := recovery.NewDatabaseSet(
		mustPosition(t, 2, 10),
		mustPosition(t, 3, 20),
		mustPosition(t, 4, 30),
	)
	advanced, err := recovery.Advance(operation, recovery.PositionsRecorded{Positions: positions})
	if !errors.Is(err, recovery.ErrFailoverOutOfOrder) {
		t.Fatalf("Advance(positions) error = %v, want ErrFailoverOutOfOrder", err)
	}
	if advanced.Stage() != recovery.StagePlanned || advanced.WriteReady() {
		t.Fatalf("rejected operation stage/write-ready = %s/%v", advanced.Stage(), advanced.WriteReady())
	}
}

func mustPosition(t *testing.T, timeline uint32, wal uint64) recovery.ReplicationPosition {
	t.Helper()
	position, err := recovery.NewReplicationPosition(timeline, wal)
	if err != nil {
		t.Fatalf("NewReplicationPosition(%d, %d) error = %v", timeline, wal, err)
	}
	return position
}

func actionHash(name string) recovery.ObservationHash {
	return recovery.HashObservation([]byte(name))
}

func mustAttestation(t *testing.T, binding recovery.FenceBinding, observedAt time.Time) recovery.FencingAttestation {
	return mustAttestationForPurpose(t, binding, recovery.FencingPurposeInitial, observedAt)
}

func mustAttestationForPurpose(t *testing.T, binding recovery.FenceBinding, purpose recovery.FencingPurpose, observedAt time.Time) recovery.FencingAttestation {
	t.Helper()
	hashes := recovery.ObservationHashes{
		Ingress:         recovery.HashObservation([]byte("ingress")),
		Processes:       recovery.HashObservation([]byte("processes")),
		Credentials:     recovery.HashObservation([]byte("credentials")),
		DatabaseNetwork: recovery.HashObservation([]byte("database-network")),
	}
	seed := sha256.Sum256([]byte("regional-recovery-test-fencing-key"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	nonce := "test-nonce-" + strconv.FormatInt(observedAt.UnixNano(), 10)
	attestation, err := recovery.NewFencingAttestation(binding, purpose, observedAt, observedAt.Add(5*time.Minute), "test-fence-authority", "test-key-1", nonce, hashes, make([]byte, ed25519.SignatureSize))
	if err == nil {
		attestation, err = recovery.NewFencingAttestation(binding, purpose, observedAt, observedAt.Add(5*time.Minute), "test-fence-authority", "test-key-1", nonce, hashes, ed25519.Sign(privateKey, attestation.CanonicalPayload()))
	}
	if err != nil {
		t.Fatalf("NewFencingAttestation() error = %v", err)
	}
	return attestation
}

func mustRecoverySnapshot(
	t *testing.T,
	region authority.Region,
	epoch authority.Epoch,
	state authority.State,
	writesEnabled bool,
) authority.Snapshot {
	t.Helper()
	snapshot, err := authority.NewSnapshot(region, epoch, state, writesEnabled)
	if err != nil {
		t.Fatalf("NewSnapshot() error = %v", err)
	}
	return snapshot
}
