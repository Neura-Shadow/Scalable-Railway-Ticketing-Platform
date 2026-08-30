package recovery_test

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
)

func TestOrchestratorLoadsVerifiesAdvancesAndCASSavesOnePhase(t *testing.T) {
	t.Parallel()
	operationID := uuid.New()
	declaredAt := time.Date(2026, 8, 11, 1, 0, 0, 0, time.UTC)
	operation, err := recovery.NewFailover(
		operationID, mustRecoveryRegion(t, "region-a"), mustRecoveryRegion(t, "region-b"),
		mustRecoveryEpoch(t, 1), uuid.New(), "operator:test", declaredAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	store := &orchestratorStore{operation: operation, version: 4}
	orchestrator, err := recovery.NewOrchestrator(store, func() time.Time { return declaredAt.Add(time.Minute) }, testFencingVerifier(t, declaredAt.Add(time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	actionCalls := 0
	advanced, version, err := orchestrator.AdvanceNext(context.Background(), operationID, recovery.PhaseActionFunc(func(_ context.Context, loaded recovery.Failover) (recovery.Evidence, error) {
		actionCalls++
		if loaded.Stage() != recovery.StagePlanned {
			t.Fatalf("loaded stage=%s", loaded.Stage())
		}
		return recovery.ExternalFencingVerified{Attestation: mustAttestation(t, loaded.Binding(), declaredAt.Add(30*time.Second))}, nil
	}))
	if err != nil || advanced.Stage() != recovery.StageExternalFencingVerified || version != 5 || actionCalls != 1 {
		t.Fatalf("stage=%s version=%d calls=%d err=%v", advanced.Stage(), version, actionCalls, err)
	}
	if store.savedExpected != 4 || store.saveCalls != 1 {
		t.Fatalf("saved expected=%d calls=%d", store.savedExpected, store.saveCalls)
	}
}

func TestOrchestratorDoesNotSaveFailedVerification(t *testing.T) {
	t.Parallel()
	operationID := uuid.New()
	regionA, _ := authority.ParseRegion("region-a")
	regionB, _ := authority.ParseRegion("region-b")
	epoch, _ := authority.NewEpoch(1)
	operation, _ := recovery.NewFailover(operationID, regionA, regionB, epoch, uuid.New(), "operator:test", time.Now().UTC())
	store := &orchestratorStore{operation: operation, version: 1}
	orchestrator, _ := recovery.NewOrchestrator(store, time.Now, testFencingVerifier(t, time.Now().UTC()))
	want := errors.New("observation failed")
	_, version, err := orchestrator.AdvanceNext(context.Background(), operationID, recovery.PhaseActionFunc(func(context.Context, recovery.Failover) (recovery.Evidence, error) {
		return nil, want
	}))
	if !errors.Is(err, want) || version != 1 || store.saveCalls != 0 {
		t.Fatalf("version=%d saves=%d err=%v", version, store.saveCalls, err)
	}
}

func TestOrchestratorRejectsExpiredDurableFenceBeforePromotion(t *testing.T) {
	t.Parallel()
	declaredAt := time.Date(2026, 8, 11, 7, 0, 0, 0, time.UTC)
	operationID := uuid.New()
	operation, err := recovery.NewFailover(operationID, mustRecoveryRegion(t, "region-a"), mustRecoveryRegion(t, "region-b"), mustRecoveryEpoch(t, 1), uuid.New(), "operator:test", declaredAt)
	if err != nil {
		t.Fatal(err)
	}
	operation, err = recovery.Advance(operation, recovery.ExternalFencingVerified{Attestation: mustAttestation(t, operation.Binding(), declaredAt.Add(time.Minute))})
	if err != nil {
		t.Fatal(err)
	}
	store := &orchestratorStore{operation: operation, version: 2}
	orchestrator, err := recovery.NewOrchestrator(store, func() time.Time { return declaredAt.Add(7 * time.Minute) }, testFencingVerifier(t, declaredAt.Add(7*time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	_, version, err := orchestrator.AdvanceNext(context.Background(), operationID, recovery.PhaseActionFunc(func(context.Context, recovery.Failover) (recovery.Evidence, error) {
		t.Fatal("expired checkpoint reached phase verification")
		return nil, nil
	}))
	if !errors.Is(err, recovery.ErrFencingSignature) || version != 2 || store.saveCalls != 0 {
		t.Fatalf("expired advance version=%d saves=%d error=%v", version, store.saveCalls, err)
	}
}

func testFencingVerifier(t *testing.T, now time.Time) recovery.FencingVerifier {
	t.Helper()
	seed := sha256.Sum256([]byte("regional-recovery-test-fencing-key"))
	key := ed25519.NewKeyFromSeed(seed[:])
	verifier, err := recovery.NewFencingVerifier("test-fence-authority", "test-key-1", key.Public().(ed25519.PublicKey), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	return verifier
}

type orchestratorStore struct {
	operation     recovery.Failover
	version       int64
	savedExpected int64
	saveCalls     int
}

func (store *orchestratorStore) Load(context.Context, uuid.UUID) (recovery.Failover, int64, error) {
	return store.operation, store.version, nil
}

func (store *orchestratorStore) Save(_ context.Context, expected int64, operation recovery.Failover, _ time.Time) (int64, error) {
	store.savedExpected = expected
	store.saveCalls++
	store.operation = operation
	store.version++
	return store.version, nil
}

func (store *orchestratorStore) Refresh(ctx context.Context, expected int64, operation recovery.Failover, at time.Time) (int64, error) {
	return store.Save(ctx, expected, operation, at)
}
