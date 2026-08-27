package recovery_test

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
)

func TestDecodeFailbackValidationDocumentRequiresCompleteSignedProvenance(t *testing.T) {
	t.Parallel()
	declaredAt := time.Date(2026, 8, 11, 4, 0, 0, 0, time.UTC)
	source := mustRecoveryRegion(t, "region-b")
	target := mustRecoveryRegion(t, "region-a")
	epoch := mustRecoveryEpoch(t, 71)
	operation, err := recovery.NewFailover(uuid.New(), source, target, epoch, uuid.New(), "operator:test", declaredAt)
	if err != nil {
		t.Fatal(err)
	}
	startedAt, completedAt := declaredAt.Add(time.Minute), declaredAt.Add(3*time.Minute)
	fence := mustAttestationForPurpose(t, operation.Binding(), recovery.FencingPurposeFailbackValidation, declaredAt.Add(4*time.Minute))
	hashes := fence.Hashes()
	provenance := func(timeline uint32, wal uint64) map[string]any {
		return map[string]any{
			"source_region": "region-b", "source_epoch": uint64(71), "started_at": startedAt, "completed_at": completedAt,
			"source_position":   map[string]any{"timeline": timeline, "wal": wal},
			"replayed_position": map[string]any{"timeline": timeline, "wal": wal + 10}, "reconciled": true,
		}
	}
	document := map[string]any{
		"reseed_after": startedAt, "control": provenance(3, 100), "shard_0": provenance(4, 200), "shard_1": provenance(5, 300),
		"current_fence": map[string]any{
			"observed_at": fence.ObservedAt(), "expires_at": fence.ExpiresAt(), "issuer": fence.Issuer(), "key_id": fence.KeyID(), "nonce": fence.Nonce(),
			"purpose":       string(fence.Purpose()),
			"signature_b64": base64.StdEncoding.EncodeToString(fence.Signature()), "ingress_sha256": hex.EncodeToString(hashes.Ingress[:]),
			"processes_sha256": hex.EncodeToString(hashes.Processes[:]), "credentials_sha256": hex.EncodeToString(hashes.Credentials[:]),
			"database_network_sha256": hex.EncodeToString(hashes.DatabaseNetwork[:]),
		},
	}
	payload, _ := json.Marshal(document)
	seed := sha256.Sum256([]byte("regional-recovery-test-fencing-key"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	verifier, err := recovery.NewFencingVerifier("test-fence-authority", "test-key-1", privateKey.Public().(ed25519.PublicKey), func() time.Time { return fence.ObservedAt().Add(time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	plan, err := recovery.DecodeFailbackValidationDocument(operation, mustRecoveryEpoch(t, 72), payload, verifier)
	if err != nil || !plan.Ready() {
		t.Fatalf("DecodeFailbackValidationDocument() ready=%v error=%v", plan.Ready(), err)
	}
	delete(document, "shard_1")
	payload, _ = json.Marshal(document)
	if _, err := recovery.DecodeFailbackValidationDocument(operation, mustRecoveryEpoch(t, 72), payload, verifier); !errors.Is(err, recovery.ErrInvalidFailbackValidationDocument) {
		t.Fatalf("incomplete provenance error=%v", err)
	}
}

func TestPrepareFailbackRequiresFreshReseedFromCurrentActiveAndNewerEpoch(t *testing.T) {
	t.Parallel()

	currentActive := mustRecoveryRegion(t, "region-b")
	target := mustRecoveryRegion(t, "region-a")
	currentEpoch := mustRecoveryEpoch(t, 71)
	candidateEpoch := mustRecoveryEpoch(t, 72)
	declaredAt := time.Date(2026, 8, 11, 5, 0, 0, 0, time.UTC)
	reseedAfter := declaredAt.Add(time.Minute)
	binding, err := recovery.NewFenceBinding(
		uuid.New(),
		currentActive,
		currentEpoch,
		uuid.New(),
		"operator:dana",
		declaredAt,
	)
	if err != nil {
		t.Fatalf("NewFenceBinding() error = %v", err)
	}
	reseeds := recovery.NewDatabaseSet(
		mustReseed(t, currentActive, currentEpoch, reseedAfter, 3, 100),
		mustReseed(t, currentActive, currentEpoch, reseedAfter, 4, 200),
		mustReseed(t, currentActive, currentEpoch, reseedAfter, 5, 300),
	)
	fence := mustAttestationForPurpose(t, binding, recovery.FencingPurposeFailbackValidation, reseedAfter.Add(4*time.Minute))

	plan, err := recovery.PrepareFailback(
		binding,
		target,
		candidateEpoch,
		reseedAfter,
		reseeds,
		fence,
	)
	if err != nil {
		t.Fatalf("PrepareFailback() error = %v", err)
	}
	if !plan.Ready() || plan.CurrentActive() != currentActive || plan.TargetEpoch() != candidateEpoch {
		t.Fatalf("plan = ready %v current %s epoch %d", plan.Ready(), plan.CurrentActive(), plan.TargetEpoch().Uint64())
	}

	_, err = recovery.PrepareFailback(
		binding,
		target,
		currentEpoch,
		reseedAfter,
		reseeds,
		fence,
	)
	if !errors.Is(err, recovery.ErrFailbackEpochNotNewer) {
		t.Fatalf("PrepareFailback(reused epoch) error = %v, want ErrFailbackEpochNotNewer", err)
	}
}

func TestPrepareFailbackRejectsReseedFromFormerDivergentPrimary(t *testing.T) {
	t.Parallel()

	currentActive := mustRecoveryRegion(t, "region-b")
	formerPrimary := mustRecoveryRegion(t, "region-a")
	currentEpoch := mustRecoveryEpoch(t, 81)
	declaredAt := time.Date(2026, 8, 11, 6, 0, 0, 0, time.UTC)
	reseedAfter := declaredAt.Add(time.Minute)
	binding, err := recovery.NewFenceBinding(
		uuid.New(),
		currentActive,
		currentEpoch,
		uuid.New(),
		"operator:erin",
		declaredAt,
	)
	if err != nil {
		t.Fatalf("NewFenceBinding() error = %v", err)
	}
	reseeds := recovery.NewDatabaseSet(
		mustReseed(t, formerPrimary, currentEpoch, reseedAfter, 3, 100),
		mustReseed(t, formerPrimary, currentEpoch, reseedAfter, 4, 200),
		mustReseed(t, formerPrimary, currentEpoch, reseedAfter, 5, 300),
	)
	_, err = recovery.PrepareFailback(
		binding,
		formerPrimary,
		mustRecoveryEpoch(t, 82),
		reseedAfter,
		reseeds,
		mustAttestationForPurpose(t, binding, recovery.FencingPurposeFailbackValidation, reseedAfter.Add(4*time.Minute)),
	)
	if !errors.Is(err, recovery.ErrInvalidReseedProvenance) {
		t.Fatalf("PrepareFailback(former-primary reseed) error = %v, want ErrInvalidReseedProvenance", err)
	}
}

func mustReseed(
	t *testing.T,
	sourceRegion interface{ String() string },
	sourceEpoch interface{ Uint64() uint64 },
	startedAt time.Time,
	timeline uint32,
	wal uint64,
) recovery.ReseedProvenance {
	t.Helper()
	region := mustRecoveryRegion(t, sourceRegion.String())
	epoch := mustRecoveryEpoch(t, sourceEpoch.Uint64())
	provenance, err := recovery.NewReseedProvenance(
		region,
		epoch,
		startedAt,
		startedAt.Add(2*time.Minute),
		mustPosition(t, timeline, wal),
		mustPosition(t, timeline, wal+10),
		true,
	)
	if err != nil {
		t.Fatalf("NewReseedProvenance() error = %v", err)
	}
	return provenance
}
