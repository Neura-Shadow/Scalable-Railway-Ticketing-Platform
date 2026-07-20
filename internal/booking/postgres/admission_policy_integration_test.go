package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/postgres"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/google/uuid"
)

func TestCreateHoldRejectsNewlyEnabledPolicyBeforeDurableMutation(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 2)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	policyID, version := insertEnabledHotTrainPolicy(t, fixture)
	before := quotaMutationSnapshot(t, fixture)

	params := quotaHoldParams(fixture, 0, 0xa1, 0xb1)
	if _, err := store.CreateHold(ctx, params); !errors.Is(err, ErrAdmissionRequired) {
		t.Fatalf("unprotected CreateHold() error = %v, want %v", err, ErrAdmissionRequired)
	}
	if after := quotaMutationSnapshot(t, fixture); after != before {
		t.Fatalf("policy rejection mutated durable booking state: before=%+v after=%+v", before, after)
	}

	params.AdmissionPolicy = &AdmissionPolicyDecision{PolicyID: policyID, Version: version}
	if _, err := store.CreateHold(ctx, params); err != nil {
		t.Fatalf("protected CreateHold() error = %v", err)
	}
}

func TestCreateHoldRejectsStalePolicyVersionAndCompletedReplayStillWins(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 3)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	policyID, version := insertEnabledHotTrainPolicy(t, fixture)

	stale := quotaHoldParams(fixture, 0, 0xa2, 0xb2)
	stale.AdmissionPolicy = &AdmissionPolicyDecision{PolicyID: policyID, Version: version + 1}
	if _, err := store.CreateHold(ctx, stale); !errors.Is(err, ErrAdmissionPolicyChanged) {
		t.Fatalf("future policy CreateHold() error = %v, want %v", err, ErrAdmissionPolicyChanged)
	}

	current := quotaHoldParams(fixture, 1, 0xa3, 0xb3)
	current.AdmissionPolicy = &AdmissionPolicyDecision{PolicyID: policyID, Version: version}
	created, err := store.CreateHold(ctx, current)
	if err != nil {
		t.Fatalf("current policy CreateHold() error = %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, `
UPDATE hot_train_policies
SET version = version + 1, redis_initialized_version = NULL
WHERE id = $1`, policyID); err != nil {
		t.Fatalf("advance policy version: %v", err)
	}

	replayed, err := store.CreateHold(ctx, current)
	if err != nil {
		t.Fatalf("completed replay after policy change error = %v", err)
	}
	if !replayed.Replayed || replayed.ReservationID != created.ReservationID {
		t.Fatalf("completed replay = %+v, want %s", replayed, created.ReservationID)
	}

	fresh := quotaHoldParams(fixture, 2, 0xa4, 0xb4)
	fresh.AdmissionPolicy = &AdmissionPolicyDecision{PolicyID: policyID, Version: version}
	if _, err := store.CreateHold(ctx, fresh); !errors.Is(err, ErrAdmissionPolicyChanged) {
		t.Fatalf("stale policy CreateHold() error = %v, want %v", err, ErrAdmissionPolicyChanged)
	}
}

func TestCreateHoldAllowsDisabledPolicyAsNonHot(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	policyID, _ := insertEnabledHotTrainPolicy(t, fixture)
	if _, err := fixture.pool.Exec(ctx, `
UPDATE hot_train_policies
SET enabled = false, version = version + 1
WHERE id = $1`, policyID); err != nil {
		t.Fatalf("disable policy: %v", err)
	}
	if _, err := store.CreateHold(ctx, quotaHoldParams(fixture, 0, 0xa5, 0xb5)); err != nil {
		t.Fatalf("non-hot CreateHold() with disabled policy error = %v", err)
	}
}

func TestConcurrentProtectedHoldsNeverExceedPostgresSeatCapacity(t *testing.T) {
	const seatCapacity = 2
	_, fixture := newIntegrationFixture(t, seatCapacity)
	store := NewWithReservationQuotaLimits(fixture.pool, ReservationQuotaLimits{
		MaxActiveHoldsPerUser:            20,
		MaxActiveHoldsPerUserPerTrainRun: 20,
		MaxActivePassengersPerUser:       20,
	})
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	policyID, version := insertEnabledHotTrainPolicy(t, fixture)

	const attempts = 8
	start := make(chan struct{})
	results := make(chan error, attempts)
	var wait sync.WaitGroup
	for index := 0; index < attempts; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			params := quotaHoldParams(
				fixture,
				index,
				byte(0x10+index),
				byte(0x30+index),
			)
			params.AdmissionPolicy = &AdmissionPolicyDecision{
				PolicyID: policyID,
				Version:  version,
			}
			_, err := store.CreateHold(ctx, params)
			results <- err
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)

	successes := 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrInsufficientInventory):
		default:
			t.Fatalf("protected capacity attempt error = %v", err)
		}
	}
	if successes != seatCapacity {
		t.Fatalf("protected successful holds = %d, want exact seat capacity %d", successes, seatCapacity)
	}
	var reservationSeats, occupiedSegments int
	err := fixture.pool.QueryRow(ctx, `
SELECT
    (SELECT count(*)
     FROM reservation_seats AS rs
     JOIN reservations AS r ON r.id = rs.reservation_id
     WHERE r.train_run_id = $1 AND r.status = 'held'),
    (SELECT coalesce(sum(bit_count(occupied_segments)), 0)::integer
     FROM seat_inventory
     WHERE train_run_id = $1)`, fixture.trainRunID).Scan(&reservationSeats, &occupiedSegments)
	if err != nil {
		t.Fatalf("read protected capacity state: %v", err)
	}
	if reservationSeats != seatCapacity || occupiedSegments != seatCapacity*2 {
		t.Fatalf("protected capacity state = seats:%d occupied_segments:%d", reservationSeats, occupiedSegments)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("reconcile protected capacity race: %v", err)
	}
}

func TestCanceledProtectedCreateRollsBackIdempotencyAndLeavesInventoryReconcilable(t *testing.T) {
	_, fixture := newIntegrationFixture(t, 1)
	limits := ReservationQuotaLimits{
		MaxActiveHoldsPerUser:            1,
		MaxActiveHoldsPerUserPerTrainRun: 1,
		MaxActivePassengersPerUser:       1,
	}
	store := NewWithReservationQuotaLimits(fixture.pool, limits)
	ctx := context.Background()
	if _, err := store.InitializeInventory(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("initialize inventory: %v", err)
	}
	policyID, version := insertEnabledHotTrainPolicy(t, fixture)
	params := quotaHoldParams(fixture, 0, 0xc1, 0xd1)
	params.AdmissionPolicy = &AdmissionPolicyDecision{PolicyID: policyID, Version: version}
	before := quotaMutationSnapshot(t, fixture)

	blocker, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin train-run blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback(context.Background()) }()
	if _, err := blocker.Exec(ctx, `SELECT id FROM train_runs WHERE id = $1 FOR UPDATE`, fixture.trainRunID); err != nil {
		t.Fatalf("lock train run: %v", err)
	}
	var blockerPID int
	if err := blocker.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
		t.Fatalf("read train-run blocker PID: %v", err)
	}

	createCtx, cancelCreate := context.WithCancel(ctx)
	defer cancelCreate()
	createResult := make(chan error, 1)
	go func() {
		_, err := store.CreateHold(createCtx, params)
		createResult <- err
	}()
	waitForBlockedCreateHold(t, fixture, blockerPID)
	cancelCreate()
	select {
	case err := <-createResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled protected CreateHold() error = %v, want context cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled protected CreateHold() did not terminate")
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatalf("release train-run blocker: %v", err)
	}

	if after := quotaMutationSnapshot(t, fixture); after != before {
		t.Fatalf("cancelled protected create mutated durable state: before=%+v after=%+v", before, after)
	}
	if err := store.ReconcileTrainRun(ctx, fixture.trainRunID); err != nil {
		t.Fatalf("reconcile after cancelled protected create: %v", err)
	}
	quotaState, err := store.ReconcileReservationQuotas(ctx, limits)
	if err != nil {
		t.Fatalf("reconcile quotas after cancelled protected create: %v", err)
	}
	if quotaState.Violations() != 0 {
		t.Fatalf("quota violations after cancelled protected create = %+v", quotaState)
	}

	retried, err := store.CreateHold(ctx, params)
	if err != nil {
		t.Fatalf("retry cancelled protected CreateHold() with released quota: %v", err)
	}
	if retried.Replayed {
		t.Fatalf("retry cancelled protected CreateHold() replayed a rolled-back idempotency record: %+v", retried)
	}
}

func TestPolicyActivationWaitsForBookingAbsentRowRecheck(t *testing.T) {
	store, fixture := newIntegrationFixture(t, 1)
	ctx := context.Background()
	bookingTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin booking transaction: %v", err)
	}
	defer func() { _ = bookingTx.Rollback(context.Background()) }()
	if err := bookingTx.recheckAdmissionPolicy(ctx, fixture.trainRunID, "standard", nil); err != nil {
		t.Fatalf("absent-row policy recheck: %v", err)
	}
	var bookingPID int
	if err := bookingTx.tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&bookingPID); err != nil {
		t.Fatalf("read booking backend PID: %v", err)
	}
	peerTx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin concurrent booking transaction: %v", err)
	}
	peerCtx, cancelPeer := context.WithTimeout(ctx, time.Second)
	if err := peerTx.recheckAdmissionPolicy(peerCtx, fixture.trainRunID, "standard", nil); err != nil {
		cancelPeer()
		_ = peerTx.Rollback(context.Background())
		t.Fatalf("concurrent shared Booking recheck blocked: %v", err)
	}
	cancelPeer()
	if err := peerTx.Commit(ctx); err != nil {
		t.Fatalf("commit concurrent shared Booking recheck: %v", err)
	}

	limits, err := admissiondomain.NewPolicyLimits(admissiondomain.PolicyLimitsInput{
		MaxQueueSize: 100, AdmissionRatePerSecond: 10, MaxInflightAdmissions: 20,
		AdmissionTokenTTL: time.Minute, ProcessingLease: 10 * time.Second, QueueEntryTTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("build policy limits: %v", err)
	}
	policies := admissionpostgres.New(fixture.pool)
	activation := make(chan error, 1)
	go func() {
		_, err := policies.CreatePolicy(ctx, admissionpostgres.CreatePolicyParams{
			TrainRunID: fixture.trainRunID,
			SeatClass:  offeringdomain.SeatClassStandard,
			Limits:     limits,
			Metadata: admissionpostgres.MutationMetadata{
				ActorID: fixture.userID, CorrelationID: "activation-race-test",
			},
		})
		activation <- err
	}()

	waitForAdvisoryWaiter(t, fixture, bookingPID)
	select {
	case err := <-activation:
		t.Fatalf("policy activation completed before booking transaction released scope lock: %v", err)
	default:
	}
	if err := bookingTx.Commit(ctx); err != nil {
		t.Fatalf("commit booking policy-check transaction: %v", err)
	}
	select {
	case err := <-activation:
		if err != nil {
			t.Fatalf("policy activation after booking commit: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("policy activation remained blocked after booking commit")
	}
}

func waitForBlockedCreateHold(t *testing.T, fixture integrationFixture, blockerPID int) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		err := fixture.pool.QueryRow(context.Background(), `
SELECT EXISTS (
    SELECT 1
    FROM pg_stat_activity AS activity
    WHERE activity.datname = current_database()
      AND $1 = ANY(pg_blocking_pids(activity.pid))
      AND activity.query LIKE '%SELECT route_id, segment_count, status, clock_timestamp()%'
)`, blockerPID).Scan(&waiting)
		if err != nil {
			t.Fatalf("inspect blocked protected create: %v", err)
		}
		if waiting {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("protected CreateHold did not reach the controlled train-run lock")
		case <-ticker.C:
		}
	}
}

func waitForAdvisoryWaiter(t *testing.T, fixture integrationFixture, holderPID int) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		err := fixture.pool.QueryRow(context.Background(), `
SELECT EXISTS (
    SELECT 1
    FROM pg_locks AS holder
    JOIN pg_locks AS waiter
      ON waiter.locktype = holder.locktype
     AND waiter.database IS NOT DISTINCT FROM holder.database
     AND waiter.classid = holder.classid
     AND waiter.objid = holder.objid
     AND waiter.objsubid = holder.objsubid
    WHERE holder.pid = $1
      AND holder.locktype = 'advisory'
      AND holder.granted
      AND NOT waiter.granted
      AND waiter.pid <> holder.pid
)`, holderPID).Scan(&waiting)
		if err != nil {
			t.Fatalf("inspect advisory-lock waiter: %v", err)
		}
		if waiting {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("policy activation did not wait on Booking's advisory lock")
		case <-ticker.C:
		}
	}
}

func insertEnabledHotTrainPolicy(t *testing.T, fixture integrationFixture) (policyID uuid.UUID, version int64) {
	t.Helper()
	err := fixture.pool.QueryRow(context.Background(), `
INSERT INTO hot_train_policies (
    train_run_id, seat_class, max_queue_size, admission_rate_per_second,
    max_inflight_admissions, admission_token_ttl_seconds,
    processing_lease_seconds, queue_entry_ttl_seconds
) VALUES ($1, 'standard', 1000, 100, 100, 60, 10, 120)
RETURNING id, version`, fixture.trainRunID).Scan(&policyID, &version)
	if err != nil {
		t.Fatalf("insert hot-train policy: %v", err)
	}
	return policyID, version
}
