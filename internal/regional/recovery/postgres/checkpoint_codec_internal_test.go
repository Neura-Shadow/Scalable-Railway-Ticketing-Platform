package postgres

import (
	"crypto/ed25519"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
)

func TestCheckpointCodecRoundTripsFixedDatabasePromotionEvidence(t *testing.T) {
	t.Parallel()

	declaredAt := time.Date(2026, 8, 11, 14, 0, 0, 0, time.UTC)
	source, _ := authority.ParseRegion("region-a")
	target, _ := authority.ParseRegion("region-b")
	epoch, _ := authority.NewEpoch(21)
	operation, err := recovery.NewFailover(
		uuid.New(), source, target, epoch, uuid.New(), "operator:codec", declaredAt,
	)
	if err != nil {
		t.Fatalf("NewFailover() error = %v", err)
	}
	hashes := recovery.ObservationHashes{
		Ingress:         recovery.HashObservation([]byte("ingress")),
		Processes:       recovery.HashObservation([]byte("processes")),
		Credentials:     recovery.HashObservation([]byte("credentials")),
		DatabaseNetwork: recovery.HashObservation([]byte("database-network")),
	}
	seed := sha256.Sum256([]byte("checkpoint-codec-test-fencing-key"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	observedAt := declaredAt.Add(time.Minute)
	attestation, err := recovery.NewFencingAttestation(operation.Binding(), recovery.FencingPurposeInitial, observedAt, observedAt.Add(5*time.Minute), "test-fence-authority", "test-key-1", "test-nonce-00000003", hashes, make([]byte, ed25519.SignatureSize))
	if err == nil {
		attestation, err = recovery.NewFencingAttestation(operation.Binding(), recovery.FencingPurposeInitial, observedAt, observedAt.Add(5*time.Minute), "test-fence-authority", "test-key-1", "test-nonce-00000003", hashes, ed25519.Sign(privateKey, attestation.CanonicalPayload()))
	}
	if err != nil {
		t.Fatalf("NewFencingAttestation() error = %v", err)
	}
	positions := recovery.NewDatabaseSet(
		codecPosition(t, 2, 100), codecPosition(t, 3, 200), codecPosition(t, 4, 300),
	)
	for _, evidence := range []recovery.Evidence{
		recovery.ExternalFencingVerified{Attestation: attestation},
		recovery.PositionsRecorded{Positions: positions},
		recovery.PassiveReadinessRemoved{Observation: recovery.HashObservation([]byte("passive-readiness"))},
		recovery.DatabasePromoted{Database: recovery.DatabaseControl, Position: codecPosition(t, 3, 100)},
		recovery.DatabasePromoted{Database: recovery.DatabaseShard0, Position: codecPosition(t, 4, 200)},
		recovery.DatabasePromoted{Database: recovery.DatabaseShard1, Position: codecPosition(t, 5, 300)},
	} {
		operation, err = recovery.Advance(operation, evidence)
		if err != nil {
			t.Fatalf("Advance(%T) error = %v", evidence, err)
		}
	}
	payload, err := marshalCheckpoint(operation.Checkpoint())
	if err != nil {
		t.Fatalf("marshalCheckpoint() error = %v", err)
	}
	restored, err := unmarshalCheckpoint(payload)
	if err != nil {
		t.Fatalf("unmarshalCheckpoint() error = %v", err)
	}
	if restored.Checkpoint() != operation.Checkpoint() {
		t.Fatalf("restored checkpoint differs:\n got %+v\nwant %+v", restored.Checkpoint(), operation.Checkpoint())
	}
}

func TestCheckpointCodecRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	_, err := unmarshalCheckpoint([]byte(`{"schema_version":1,"unknown":"host-or-command"}`))
	if err == nil {
		t.Fatal("unmarshalCheckpoint() accepted an unknown field")
	}
}

func codecPosition(t *testing.T, timeline uint32, wal uint64) recovery.ReplicationPosition {
	t.Helper()
	position, err := recovery.NewReplicationPosition(timeline, wal)
	if err != nil {
		t.Fatalf("NewReplicationPosition() error = %v", err)
	}
	return position
}
