package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

func TestRepairFinalizesFingerprintVerifiedCommittedReceipt(t *testing.T) {
	t.Parallel()
	candidate := testCandidate(t, command.StateNeedsRepair, time.Now().Add(time.Minute))
	inspector := &fakeShardInspector{observation: Observation{
		Kind: ObservationCommitted,
		Receipt: command.Receipt{
			CommandID: candidate.Command.ID, RequestFingerprint: candidate.Command.RequestFingerprint,
			ResultResourceID: candidate.Command.ReservationID, Status: command.ReceiptCommitted,
		},
	}}
	store := &fakeStore{}
	worker := newTestService(t, store, inspector)

	outcome, err := worker.Repair(context.Background(), candidate)
	if err != nil {
		t.Fatalf("Repair() error = %v", err)
	}
	if outcome != OutcomeFinalized || store.finalized != 1 || store.failed != 0 || store.expired != 0 {
		t.Fatalf("Repair() = %q, mutations = finalize:%d fail:%d expire:%d", outcome, store.finalized, store.failed, store.expired)
	}
}

func TestRepairExpiresOnlyAfterAuthoritativeMissingReceipt(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	candidate := testCandidate(t, command.StateReserved, now.Add(-time.Second))
	store := &fakeStore{}
	worker := newTestService(t, store, &fakeShardInspector{observation: Observation{Kind: ObservationMissing}})
	worker.now = func() time.Time { return now }

	outcome, err := worker.Repair(context.Background(), candidate)
	if err != nil || outcome != OutcomeExpired || store.expired != 1 {
		t.Fatalf("Repair() = (%q, %v), expired = %d", outcome, err, store.expired)
	}

	store = &fakeStore{}
	worker = newTestService(t, store, &fakeShardInspector{err: ErrShardUnreachable})
	worker.now = func() time.Time { return now }
	outcome, err = worker.Repair(context.Background(), candidate)
	if !errors.Is(err, ErrShardUnreachable) || outcome != OutcomeDeferred || store.expired != 0 {
		t.Fatalf("unreachable Repair() = (%q, %v), expired = %d", outcome, err, store.expired)
	}
}

func TestRepairRejectsReceiptMismatchConservatively(t *testing.T) {
	t.Parallel()
	candidate := testCandidate(t, command.StateNeedsRepair, time.Now().Add(time.Minute))
	observation := Observation{Kind: ObservationCommitted, Receipt: command.Receipt{
		CommandID: candidate.Command.ID, RequestFingerprint: [32]byte{99},
		ResultResourceID: candidate.Command.ReservationID, Status: command.ReceiptCommitted,
	}}
	store := &fakeStore{}
	worker := newTestService(t, store, &fakeShardInspector{observation: observation})

	outcome, err := worker.Repair(context.Background(), candidate)
	if !errors.Is(err, ErrReceiptMismatch) || outcome != OutcomeDeferred || store.mutations() != 0 {
		t.Fatalf("Repair() = (%q, %v), mutations = %d", outcome, err, store.mutations())
	}
}

func TestRepairReleasesRejectedCommandWithBoundedCategory(t *testing.T) {
	t.Parallel()
	candidate := testCandidate(t, command.StateExecuting, time.Now().Add(time.Minute))
	store := &fakeStore{}
	worker := newTestService(t, store, &fakeShardInspector{observation: rejectedObservation(candidate)})

	outcome, err := worker.Repair(context.Background(), candidate)
	if err != nil || outcome != OutcomeFailed || store.failed != 1 || store.category != FailureShardRejected {
		t.Fatalf("Repair() = (%q, %v), failed = %d category = %q", outcome, err, store.failed, store.category)
	}
}

func TestRepairLeavesStartedAndUnexpiredMissingReceiptsConservative(t *testing.T) {
	t.Parallel()
	for _, kind := range []ObservationKind{ObservationStarted, ObservationMissing} {
		kind := kind
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			candidate := testCandidate(t, command.StateExecuting, time.Now().Add(time.Hour))
			observation := Observation{Kind: kind}
			if kind == ObservationStarted {
				observation.CommandID = candidate.Command.ID
				observation.RequestFingerprint = candidate.Command.RequestFingerprint
			}
			store := &fakeStore{}
			worker := newTestService(t, store, &fakeShardInspector{observation: observation})
			outcome, err := worker.Repair(context.Background(), candidate)
			if err != nil || outcome != OutcomeDeferred || store.mutations() != 0 {
				t.Fatalf("Repair() = (%q, %v), mutations = %d", outcome, err, store.mutations())
			}
		})
	}
}

func TestRunOnceUsesBoundedClaimAndPerItemInspection(t *testing.T) {
	t.Parallel()
	candidates := []Candidate{
		testCandidate(t, command.StateNeedsRepair, time.Now().Add(time.Minute)),
		testCandidate(t, command.StateExecuting, time.Now().Add(time.Minute)),
	}
	store := &fakeStore{claimed: candidates}
	inspector := &fakeShardInspector{observations: map[uuid.UUID]Observation{
		candidates[0].Command.ID: committedObservation(candidates[0]),
		candidates[1].Command.ID: rejectedObservation(candidates[1]),
	}}
	worker := newTestService(t, store, inspector)

	result, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if store.claimOptions.BatchSize != 2 || store.claimOptions.WorkerID != "test-worker" ||
		result.Claimed != 2 || result.Finalized != 1 || result.Failed != 1 || result.Failures != 0 {
		t.Fatalf("RunOnce() result = %+v, claim = %+v", result, store.claimOptions)
	}
}

type fakeStore struct {
	claimed      []Candidate
	claimOptions ClaimOptions
	claimErr     error
	finalized    int
	failed       int
	expired      int
	category     FailureCategory
}

func (store *fakeStore) Claim(_ context.Context, options ClaimOptions) ([]Candidate, error) {
	store.claimOptions = options
	return append([]Candidate(nil), store.claimed...), store.claimErr
}

func (store *fakeStore) Finalize(context.Context, Candidate, command.Receipt) error {
	store.finalized++
	return nil
}

func (store *fakeStore) Fail(_ context.Context, _ Candidate, category FailureCategory) error {
	store.failed++
	store.category = category
	return nil
}

func (store *fakeStore) Expire(context.Context, Candidate) error {
	store.expired++
	return nil
}

func (store *fakeStore) mutations() int { return store.finalized + store.failed + store.expired }

type fakeShardInspector struct {
	observation  Observation
	observations map[uuid.UUID]Observation
	err          error
}

func (inspector *fakeShardInspector) Inspect(_ context.Context, candidate Candidate) (Observation, error) {
	if inspector.err != nil {
		return Observation{}, inspector.err
	}
	if observation, ok := inspector.observations[candidate.Command.ID]; ok {
		return observation, nil
	}
	return inspector.observation, nil
}

func newTestService(t *testing.T, store Store, inspector ShardInspector) *Service {
	t.Helper()
	worker, err := New(store, inspector, Options{
		WorkerID: "test-worker", BatchSize: 2, LeaseTTL: time.Minute, InspectTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return worker
}

func testCandidate(t *testing.T, state command.State, expiresAt time.Time) Candidate {
	t.Helper()
	trainRunID := uuid.New()
	generation, err := sharding.NewAssignmentGeneration(7)
	if err != nil {
		t.Fatal(err)
	}
	route, err := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	if err != nil {
		t.Fatal(err)
	}
	return Candidate{Command: command.Command{
		ID: uuid.New(), Operation: command.OperationCreateReservation, OwnerUserID: uuid.New(),
		TrainRunID: trainRunID, ReservationID: uuid.New(), Route: route,
		RequestFingerprint: [32]byte{1}, State: state,
	}, QuotaExpiresAt: expiresAt}
}

func committedObservation(candidate Candidate) Observation {
	return Observation{Kind: ObservationCommitted, Receipt: command.Receipt{
		CommandID: candidate.Command.ID, RequestFingerprint: candidate.Command.RequestFingerprint,
		ResultResourceID: candidate.Command.ReservationID, Status: command.ReceiptCommitted,
	}}
}

func rejectedObservation(candidate Candidate) Observation {
	return Observation{
		Kind: ObservationRejected, CommandID: candidate.Command.ID,
		RequestFingerprint: candidate.Command.RequestFingerprint, ErrorCode: "inventory_unavailable",
	}
}
