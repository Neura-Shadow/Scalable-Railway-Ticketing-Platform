package operatorcommand_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/operatorcommand"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
)

func TestCoordinatorReservesBeforeShardAndDefersFinalizerFailure(t *testing.T) {
	request, command, receipt := fixture(t, operatorcommand.OperationFareInstall)
	order := []string{}
	store := &fakeStore{command: command, order: &order}
	executor := &fakeExecutor{receipt: receipt, order: &order}
	finalizer := &fakeFinalizer{failures: 1, order: &order}
	coordinator, err := operatorcommand.NewCoordinator(store, executor, finalizer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Execute(context.Background(), request); !errors.Is(err, operatorcommand.ErrFinalizationDeferred) {
		t.Fatalf("first Execute error = %v", err)
	}
	if got := order; len(got) != 3 || got[0] != "reserve" || got[1] != "shard" || got[2] != "finalize" {
		t.Fatalf("call order = %v", got)
	}
	result, err := coordinator.Execute(context.Background(), request)
	if err != nil || result.SourceVersion != request.ExpectedSourceVersion+1 {
		t.Fatalf("retry Execute = (%+v,%v)", result, err)
	}
	if store.reserves != 2 || executor.calls != 2 || finalizer.calls != 2 {
		t.Fatalf("retry calls = reserve:%d shard:%d finalize:%d", store.reserves, executor.calls, finalizer.calls)
	}
}

func TestReceiptRequiresHistoricalRouteAndExactResultVersions(t *testing.T) {
	_, command, receipt := fixture(t, operatorcommand.OperationBookingPolicyBump)
	if !operatorcommand.ValidReceipt(command, receipt) {
		t.Fatal("exact historical receipt rejected")
	}
	for name, mutate := range map[string]func(*operatorcommand.Receipt){
		"generation":     func(value *operatorcommand.Receipt) { value.HistoricalGeneration++ },
		"shard":          func(value *operatorcommand.Receipt) { value.HistoricalShardID = sharding.ShardPhysicalOne },
		"source version": func(value *operatorcommand.Receipt) { value.ResultSourceVersion++ },
		"policy version": func(value *operatorcommand.Receipt) { value.ResultBookingPolicyVersion++ },
		"fingerprint":    func(value *operatorcommand.Receipt) { value.RequestFingerprint[0]++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := receipt
			mutate(&changed)
			if operatorcommand.ValidReceipt(command, changed) {
				t.Fatal("mismatched receipt accepted")
			}
		})
	}
}

func TestRecoveryFinalizerFailureIsRetriedAfterLease(t *testing.T) {
	_, command, receipt := fixture(t, operatorcommand.OperationSeatDisable)
	store := &fakeStore{command: command, candidates: []operatorcommand.Candidate{{Command: command}}}
	inspector := &fakeInspector{receipt: receipt, found: true}
	finalizer := &fakeFinalizer{failures: 1}
	service := recovery(t, store, inspector, finalizer, "worker-a")
	first, err := service.RunOnce(context.Background())
	if err == nil || first.Deferred != 1 || first.Finalized != 0 {
		t.Fatalf("first recovery = (%+v,%v)", first, err)
	}
	store.releaseLease()
	second, err := service.RunOnce(context.Background())
	if err != nil || second.Finalized != 1 || finalizer.calls != 2 {
		t.Fatalf("second recovery = (%+v,%v), finalizer calls=%d", second, err, finalizer.calls)
	}
}

func TestRecoveryReceiptMismatchFailsClosed(t *testing.T) {
	_, command, receipt := fixture(t, operatorcommand.OperationSeatEnable)
	receipt.Operation = operatorcommand.OperationSeatDisable
	store := &fakeStore{command: command, candidates: []operatorcommand.Candidate{{Command: command}}}
	finalizer := &fakeFinalizer{}
	result, err := recovery(t, store, &fakeInspector{receipt: receipt, found: true}, finalizer, "worker-a").RunOnce(context.Background())
	if !errors.Is(err, operatorcommand.ErrReceiptMismatch) || result.Deferred != 1 || finalizer.calls != 0 {
		t.Fatalf("mismatch recovery = (%+v,%v), finalizer calls=%d", result, err, finalizer.calls)
	}
}

func TestRecoveryReexecutesOnlyReservedCommandAfterReserveBeforeShardCrash(t *testing.T) {
	_, command, receipt := fixture(t, operatorcommand.OperationFareInstall)
	store := &fakeStore{command: command, candidates: []operatorcommand.Candidate{{Command: command}}}
	executor := &fakeExecutor{receipt: receipt}
	finalizer := &fakeFinalizer{}
	service := recoveryWithExecutor(t, store, executor, &fakeInspector{found: false}, finalizer, "worker-a")
	result, err := service.RunOnce(context.Background())
	if err != nil || result.Finalized != 1 || executor.calls != 1 || finalizer.calls != 1 {
		t.Fatalf("reserved missing recovery = (%+v,%v), execute=%d finalize=%d", result, err, executor.calls, finalizer.calls)
	}
	if executor.mutation != command.FinalizePayload {
		t.Fatalf("re-execution payload = %+v, want %+v", executor.mutation, command.FinalizePayload)
	}
}

func TestRecoveryDoesNotReexecuteMissingCommittedOrRepairCommand(t *testing.T) {
	for _, state := range []operatorcommand.State{operatorcommand.StateCommittedOnShard, operatorcommand.StateNeedsRepair} {
		t.Run(string(state), func(t *testing.T) {
			_, command, receipt := fixture(t, operatorcommand.OperationSeatEnable)
			command.State = state
			store := &fakeStore{command: command, candidates: []operatorcommand.Candidate{{Command: command}}}
			executor := &fakeExecutor{receipt: receipt}
			result, err := recoveryWithExecutor(t, store, executor, &fakeInspector{found: false}, &fakeFinalizer{}, "worker-a").RunOnce(context.Background())
			if err != nil || result.Deferred != 1 || executor.calls != 0 {
				t.Fatalf("missing %s recovery = (%+v,%v), execute=%d", state, result, err, executor.calls)
			}
		})
	}
}

func TestRecoveryExactReceiptReplayDoesNotExecuteShard(t *testing.T) {
	_, command, receipt := fixture(t, operatorcommand.OperationBookingPolicyBump)
	store := &fakeStore{command: command, candidates: []operatorcommand.Candidate{{Command: command}}}
	executor := &fakeExecutor{}
	finalizer := &fakeFinalizer{}
	result, err := recoveryWithExecutor(t, store, executor, &fakeInspector{receipt: receipt, found: true}, finalizer, "worker-a").RunOnce(context.Background())
	if err != nil || result.Finalized != 1 || executor.calls != 0 || finalizer.calls != 1 {
		t.Fatalf("exact replay = (%+v,%v), execute=%d finalize=%d", result, err, executor.calls, finalizer.calls)
	}
}

func TestRecoveryLeaseAllowsOneConcurrentClaimAndBoundsOptions(t *testing.T) {
	_, command, receipt := fixture(t, operatorcommand.OperationFareInstall)
	store := &fakeStore{command: command, candidates: []operatorcommand.Candidate{{Command: command}}}
	finalizer := &fakeFinalizer{}
	one := recovery(t, store, &fakeInspector{receipt: receipt, found: true}, finalizer, "worker-a")
	two := recovery(t, store, &fakeInspector{receipt: receipt, found: true}, finalizer, "worker-b")
	var wait sync.WaitGroup
	results := make(chan operatorcommand.RecoveryResult, 2)
	for _, service := range []*operatorcommand.RecoveryService{one, two} {
		wait.Add(1)
		go func(service *operatorcommand.RecoveryService) {
			defer wait.Done()
			result, _ := service.RunOnce(context.Background())
			results <- result
		}(service)
	}
	wait.Wait()
	close(results)
	claimed := 0
	for result := range results {
		claimed += result.Claimed
	}
	if claimed != 1 || finalizer.calls != 1 {
		t.Fatalf("concurrent claimed=%d finalizer=%d", claimed, finalizer.calls)
	}
	for _, invalid := range []operatorcommand.ClaimOptions{
		{}, {WorkerID: "bad worker", BatchSize: 1, LeaseTTL: time.Second},
		{WorkerID: "worker", BatchSize: operatorcommand.MaxClaimBatch + 1, LeaseTTL: time.Second},
		{WorkerID: "worker", BatchSize: 1, LeaseTTL: operatorcommand.MaxClaimLeaseTTL + time.Second},
	} {
		if operatorcommand.ValidClaimOptions(invalid) {
			t.Fatalf("invalid claim options accepted: %+v", invalid)
		}
	}
}

func recovery(t *testing.T, store operatorcommand.Store, inspector operatorcommand.ReceiptInspector, finalizer operatorcommand.Finalizer, worker string) *operatorcommand.RecoveryService {
	return recoveryWithExecutor(t, store, &fakeExecutor{}, inspector, finalizer, worker)
}

func recoveryWithExecutor(t *testing.T, store operatorcommand.Store, executor operatorcommand.ShardExecutor, inspector operatorcommand.ReceiptInspector, finalizer operatorcommand.Finalizer, worker string) *operatorcommand.RecoveryService {
	t.Helper()
	service, err := operatorcommand.NewRecoveryService(store, executor, inspector, finalizer, operatorcommand.RecoveryOptions{
		ClaimOptions:   operatorcommand.ClaimOptions{WorkerID: worker, BatchSize: 10, LeaseTTL: time.Minute},
		InspectTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func fixture(t *testing.T, operation operatorcommand.Operation) (operatorcommand.Request, operatorcommand.Command, operatorcommand.Receipt) {
	t.Helper()
	trainRunID := uuid.New()
	resourceID := uuid.New()
	policy := int64(0)
	mutation := operatorcommand.Mutation{}
	switch operation {
	case operatorcommand.OperationFareInstall:
		mutation = operatorcommand.Mutation{FromStopIndex: 0, ToStopIndex: 2, SeatClass: "standard", AmountMinor: 100, Currency: "TWD"}
	case operatorcommand.OperationSeatEnable:
		mutation.SeatActive = true
	case operatorcommand.OperationBookingPolicyBump:
		resourceID = trainRunID
		policy = 7
	}
	generation, _ := sharding.NewAssignmentGeneration(4)
	route, _ := sharding.NewShardRoute(trainRunID, sharding.ShardPhysicalZero, generation)
	request := operatorcommand.Request{ActorID: uuid.New(), TrainRunID: trainRunID, ResourceID: resourceID,
		Operation: operation, IdempotencyKeyHash: [32]byte{1}, RequestFingerprint: [32]byte{2},
		ExpectedSourceVersion: 11, ExpectedBookingPolicyVersion: policy, Mutation: mutation}
	command := operatorcommand.Command{ID: uuid.New(), ActorID: request.ActorID, TrainRunID: trainRunID,
		ResourceID: resourceID, Operation: operation, IdempotencyKeyHash: request.IdempotencyKeyHash,
		RequestFingerprint: request.RequestFingerprint, Route: route, ExpectedSourceVersion: 11,
		ExpectedBookingPolicyVersion: policy, FinalizePayload: mutation, State: operatorcommand.StateReserved}
	receipt := operatorcommand.Receipt{CommandID: command.ID, TrainRunID: trainRunID, ResourceID: resourceID,
		Operation: operation, RequestFingerprint: command.RequestFingerprint,
		HistoricalShardID: sharding.ShardPhysicalZero, HistoricalGeneration: 4,
		ResultSourceVersion: 12, ResultBookingPolicyVersion: policy}
	if policy > 0 {
		receipt.ResultBookingPolicyVersion = policy + 1
	}
	return request, command, receipt
}

type fakeStore struct {
	mu         sync.Mutex
	command    operatorcommand.Command
	candidates []operatorcommand.Candidate
	order      *[]string
	reserves   int
	leased     bool
}

func (store *fakeStore) Reserve(context.Context, operatorcommand.ReserveRequest) (operatorcommand.Command, error) {
	store.reserves++
	if store.order != nil {
		*store.order = append(*store.order, "reserve")
	}
	return store.command, nil
}

func (store *fakeStore) Claim(_ context.Context, options operatorcommand.ClaimOptions) ([]operatorcommand.Candidate, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.leased || len(store.candidates) == 0 {
		return nil, nil
	}
	store.leased = true
	candidate := store.candidates[0]
	candidate.LeaseOwner = options.WorkerID
	candidate.LeaseUntil = time.Now().Add(options.LeaseTTL)
	return []operatorcommand.Candidate{candidate}, nil
}

func (store *fakeStore) releaseLease() { store.mu.Lock(); store.leased = false; store.mu.Unlock() }

type fakeExecutor struct {
	receipt  operatorcommand.Receipt
	order    *[]string
	calls    int
	mutation operatorcommand.Mutation
}

func (executor *fakeExecutor) Execute(_ context.Context, _ operatorcommand.Command, mutation operatorcommand.Mutation) (operatorcommand.Receipt, error) {
	executor.calls++
	executor.mutation = mutation
	if executor.order != nil {
		*executor.order = append(*executor.order, "shard")
	}
	return executor.receipt, nil
}

type fakeFinalizer struct {
	mu       sync.Mutex
	failures int
	calls    int
	order    *[]string
}

func (finalizer *fakeFinalizer) Finalize(context.Context, operatorcommand.Command, operatorcommand.Receipt) error {
	finalizer.mu.Lock()
	defer finalizer.mu.Unlock()
	finalizer.calls++
	if finalizer.order != nil {
		*finalizer.order = append(*finalizer.order, "finalize")
	}
	if finalizer.failures > 0 {
		finalizer.failures--
		return errors.New("injected finalizer failure")
	}
	return nil
}

type fakeInspector struct {
	receipt operatorcommand.Receipt
	found   bool
	err     error
}

func (inspector *fakeInspector) Inspect(context.Context, operatorcommand.Candidate) (operatorcommand.Receipt, bool, error) {
	return inspector.receipt, inspector.found, inspector.err
}
