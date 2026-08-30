package recovery_test

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
)

func TestDecodeEvidenceDocumentVerifiesSignedFenceForLoadedOperation(t *testing.T) {
	t.Parallel()
	operation, declared := evidenceOperation(t)
	document, verifier := signedFenceDocument(t, operation, declared.Add(time.Minute), false)
	evidence, err := recovery.DecodeEvidenceDocument(operation, document, verifier)
	if err != nil {
		t.Fatalf("DecodeEvidenceDocument() error = %v", err)
	}
	advanced, err := recovery.Advance(operation, evidence)
	if err != nil || advanced.Stage() != recovery.StageExternalFencingVerified {
		t.Fatalf("Advance() stage=%s error=%v", advanced.Stage(), err)
	}
}

func TestDecodeEvidenceDocumentAcceptsAnIdenticalCurrentStageReplay(t *testing.T) {
	t.Parallel()
	operation, declared := evidenceOperation(t)
	document, verifier := signedFenceDocument(t, operation, declared.Add(time.Minute), false)
	evidence, err := recovery.DecodeEvidenceDocument(operation, document, verifier)
	if err != nil {
		t.Fatal(err)
	}
	operation, err = recovery.Advance(operation, evidence)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := recovery.DecodeEvidenceDocument(operation, document, verifier)
	if err != nil {
		t.Fatalf("current-stage DecodeEvidenceDocument() error = %v", err)
	}
	afterReplay, err := recovery.Advance(operation, replayed)
	if err != nil || afterReplay.Stage() != operation.Stage() {
		t.Fatalf("current-stage replay stage=%s error=%v", afterReplay.Stage(), err)
	}
}

func TestDecodeEvidenceDocumentAcceptsLostResponseReplayAtTargetActivation(t *testing.T) {
	t.Parallel()
	operation, declared := evidenceOperation(t)
	initialDocument, verifier := signedFenceDocument(t, operation, declared.Add(time.Minute), false)
	initialEvidence, err := recovery.DecodeEvidenceDocument(operation, initialDocument, verifier)
	if err != nil {
		t.Fatal(err)
	}
	operation, err = recovery.Advance(operation, initialEvidence)
	if err != nil {
		t.Fatal(err)
	}
	activatedAt := declared.Add(2 * time.Minute)
	operation = advanceEvidenceThrough(t, operation, activatedAt, recovery.StageTargetActive)
	document := []byte(fmt.Sprintf(`{
        "stage":"target_active",
        "observed_at":%q,
        "control":{"region":"region-b","epoch":2,"state":"active","writes_enabled":true},
        "shard_0":{"region":"region-b","epoch":2,"state":"active","writes_enabled":true},
        "shard_1":{"region":"region-b","epoch":2,"state":"active","writes_enabled":true}
    }`, activatedAt.UTC().Format(time.RFC3339Nano)))
	replayed, err := recovery.DecodeEvidenceDocument(operation, document, verifier)
	if err != nil {
		t.Fatalf("target-active replay DecodeEvidenceDocument() error = %v", err)
	}
	afterReplay, err := recovery.Advance(operation, replayed)
	if err != nil || afterReplay.Checkpoint() != operation.Checkpoint() {
		t.Fatalf("target-active replay changed checkpoint or failed: %v", err)
	}
}

func TestDecodeEvidenceDocumentRejectsForgedUnknownAndOutOfOrderEvidence(t *testing.T) {
	t.Parallel()
	operation, declared := evidenceOperation(t)
	forged, verifier := signedFenceDocument(t, operation, declared.Add(time.Minute), true)
	if _, err := recovery.DecodeEvidenceDocument(operation, forged, verifier); !errors.Is(err, recovery.ErrInvalidEvidenceDocument) {
		t.Fatalf("forged signature error = %v", err)
	}
	valid, _ := signedFenceDocument(t, operation, declared.Add(time.Minute), false)
	var unknown map[string]any
	if err := json.Unmarshal(valid, &unknown); err != nil {
		t.Fatal(err)
	}
	unknown["raw_dsn"] = "forbidden"
	unknownDocument, _ := json.Marshal(unknown)
	if _, err := recovery.DecodeEvidenceDocument(operation, unknownDocument, verifier); !errors.Is(err, recovery.ErrInvalidEvidenceDocument) {
		t.Fatalf("unknown field error = %v", err)
	}
	if _, err := recovery.DecodeEvidenceDocument(operation, []byte(`{"stage":"positions_recorded"}`), verifier); !errors.Is(err, recovery.ErrFailoverOutOfOrder) {
		t.Fatalf("out-of-order error = %v", err)
	}
}

func TestDecodeEvidenceDocumentRejectsInitialFenceRelabeledAsRetainedFence(t *testing.T) {
	t.Parallel()
	operation, declared := evidenceOperation(t)
	document, verifier := signedFenceDocument(t, operation, declared.Add(time.Minute), false)
	initial, err := recovery.DecodeEvidenceDocument(operation, document, verifier)
	if err != nil {
		t.Fatal(err)
	}
	operation, err = recovery.Advance(operation, initial)
	if err != nil {
		t.Fatal(err)
	}
	operation = advanceEvidenceThrough(t, operation, declared.Add(2*time.Minute), recovery.StageRPORecorded)
	var relabeled map[string]any
	if err := json.Unmarshal(document, &relabeled); err != nil {
		t.Fatal(err)
	}
	relabeled["stage"] = "source_retained_fenced"
	replay, _ := json.Marshal(relabeled)
	if _, err := recovery.DecodeEvidenceDocument(operation, replay, verifier); !errors.Is(err, recovery.ErrInvalidEvidenceDocument) {
		t.Fatalf("relabeled initial fence error=%v", err)
	}
}

func advanceEvidenceThrough(t *testing.T, operation recovery.Failover, activatedAt time.Time, stop recovery.Stage) recovery.Failover {
	t.Helper()
	target := operation.Target()
	epoch := mustRecoveryEpoch(t, operation.Binding().SourceEpoch().Uint64()+1)
	positions := recovery.NewDatabaseSet(mustPosition(t, 1, 100), mustPosition(t, 2, 200), mustPosition(t, 3, 300))
	promotions := recovery.NewDatabaseSet(mustPosition(t, 2, 100), mustPosition(t, 3, 200), mustPosition(t, 4, 300))
	recoverySnapshot := mustRecoverySnapshot(t, target, epoch, authority.StateRecovery, false)
	activeSnapshot := mustRecoverySnapshot(t, target, epoch, authority.StateActive, true)
	steps := []recovery.Evidence{
		recovery.PositionsRecorded{Positions: positions},
		recovery.PassiveReadinessRemoved{Observation: actionHash("passive")},
		recovery.DatabasePromoted{Database: recovery.DatabaseControl, Position: promotions.Control()},
		recovery.DatabasePromoted{Database: recovery.DatabaseShard0, Position: promotions.Shard0()},
		recovery.DatabasePromoted{Database: recovery.DatabaseShard1, Position: promotions.Shard1()},
		recovery.RolesAndTimelinesVerified{Databases: recovery.NewDatabaseSet(
			recovery.DatabaseVerification{Role: recovery.DatabaseRolePrimary, Timeline: 2},
			recovery.DatabaseVerification{Role: recovery.DatabaseRolePrimary, Timeline: 3},
			recovery.DatabaseVerification{Role: recovery.DatabaseRolePrimary, Timeline: 4},
		)},
		recovery.EpochAllocated{Epoch: epoch}, recovery.ControlRecoveryInstalled{Authority: recoverySnapshot},
		recovery.ShardAuthoritiesInstalled{Authorities: recovery.NewShardAuthoritySet(recoverySnapshot, recoverySnapshot)},
		recovery.RecoveryAPIsStarted{Observation: actionHash("apis")},
		recovery.ReconciliationPassed{Control: true, Shards: true, Payments: true, Tickets: true, Refunds: true, Ledger: true, Routing: true, Observation: actionHash("reconcile")},
		recovery.PaymentWorkersEnabled{Observation: actionHash("payments")}, recovery.SettlementWorkersEnabled{Observation: actionHash("settlement")},
		recovery.IngressSwitched{Webhook: true, Global: true, Observation: actionHash("ingress")},
		recovery.CustomerWritesConfigured{Enabled: true, ReadinessGated: true, Observation: actionHash("writes")},
		recovery.TargetActivated{Authorities: recovery.NewAuthoritySet(activeSnapshot, activeSnapshot, activeSnapshot), ObservedAt: activatedAt},
		recovery.RTORecorded{Duration: time.Minute}, recovery.RPORecorded{Loss: recovery.NewDatabaseSet(recovery.Loss{}, recovery.Loss{}, recovery.Loss{})},
	}
	for _, step := range steps {
		var err error
		operation, err = recovery.Advance(operation, step)
		if err != nil {
			t.Fatal(err)
		}
		if operation.Stage() == stop {
			return operation
		}
	}
	t.Fatalf("requested stop stage %s was not reached", stop)
	return recovery.Failover{}
}

func evidenceOperation(t *testing.T) (recovery.Failover, time.Time) {
	t.Helper()
	declared := time.Date(2026, 8, 11, 1, 0, 0, 0, time.UTC)
	source, _ := authority.ParseRegion("region-a")
	target, _ := authority.ParseRegion("region-b")
	epoch, _ := authority.NewEpoch(1)
	operation, err := recovery.NewFailover(uuid.New(), source, target, epoch, uuid.New(), "operator:test", declared)
	if err != nil {
		t.Fatal(err)
	}
	return operation, declared
}

func signedFenceDocument(t *testing.T, operation recovery.Failover, observedAt time.Time, forge bool) ([]byte, recovery.FencingVerifier) {
	t.Helper()
	seed := sha256.Sum256([]byte("evidence-json-test-fencing-key"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	publicKey := privateKey.Public().(ed25519.PublicKey)
	verifier, err := recovery.NewFencingVerifier("test-fence-authority", "test-key-1", publicKey, func() time.Time { return observedAt.Add(time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	hashes := recovery.ObservationHashes{
		Ingress: recovery.HashObservation([]byte("ingress")), Processes: recovery.HashObservation([]byte("processes")),
		Credentials: recovery.HashObservation([]byte("credentials")), DatabaseNetwork: recovery.HashObservation([]byte("database-network")),
	}
	expiresAt := observedAt.Add(5 * time.Minute)
	attestation, err := recovery.NewFencingAttestation(operation.Binding(), recovery.FencingPurposeInitial, observedAt, expiresAt, "test-fence-authority", "test-key-1", "test-nonce-00000004", hashes, make([]byte, ed25519.SignatureSize))
	if err != nil {
		t.Fatal(err)
	}
	signature := ed25519.Sign(privateKey, attestation.CanonicalPayload())
	if forge {
		signature[0] ^= 0xff
	}
	document := map[string]any{
		"stage": "external_fencing_verified", "observed_at": observedAt, "expires_at": expiresAt,
		"issuer": "test-fence-authority", "key_id": "test-key-1", "nonce": "test-nonce-00000004", "purpose": "initial_fence", "signature_b64": base64.StdEncoding.EncodeToString(signature),
		"ingress_sha256": hex.EncodeToString(hashes.Ingress[:]), "processes_sha256": hex.EncodeToString(hashes.Processes[:]),
		"credentials_sha256": hex.EncodeToString(hashes.Credentials[:]), "database_network_sha256": hex.EncodeToString(hashes.DatabaseNetwork[:]),
	}
	payload, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return payload, verifier
}
