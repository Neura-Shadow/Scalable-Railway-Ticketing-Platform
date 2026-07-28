package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

type reservationCallResult struct {
	view httpapi.ReservationView
	err  error
}

type durableReservationCommandsFake struct {
	mu                sync.Mutex
	result            bookingpostgres.CreateHoldResult
	committed         bool
	createCalls       int
	createStarted     chan struct{}
	allowCommit       chan struct{}
	replayReady       chan struct{}
	createStartedOnce sync.Once
	replayReadyOnce   sync.Once
}

func (f *durableReservationCommandsFake) CreateHold(ctx context.Context, _ bookingpostgres.CreateHoldParams) (bookingpostgres.CreateHoldResult, error) {
	f.mu.Lock()
	f.createCalls++
	f.mu.Unlock()
	if f.createStarted != nil {
		f.createStartedOnce.Do(func() { close(f.createStarted) })
	}
	if f.allowCommit != nil {
		select {
		case <-ctx.Done():
			return bookingpostgres.CreateHoldResult{}, ctx.Err()
		case <-f.allowCommit:
		}
	}
	f.mu.Lock()
	f.committed = true
	f.mu.Unlock()
	if f.replayReady != nil {
		f.replayReadyOnce.Do(func() { close(f.replayReady) })
	}
	return f.result, nil
}

func (f *durableReservationCommandsFake) LookupCompletedCreateHold(
	context.Context,
	bookingpostgres.CompletedCreateHoldLookupParams,
) (bookingpostgres.CreateHoldResult, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.committed {
		return bookingpostgres.CreateHoldResult{}, false, nil
	}
	result := f.result
	result.Replayed = true
	return result, true, nil
}

func (f *durableReservationCommandsFake) ConfirmReservation(
	context.Context,
	bookingpostgres.ReservationCommandParams,
) (bookingpostgres.ConfirmReservationResult, error) {
	return bookingpostgres.ConfirmReservationResult{}, nil
}

func (f *durableReservationCommandsFake) CancelReservation(
	context.Context,
	bookingpostgres.ReservationCommandParams,
) (bookingpostgres.CancelReservationResult, error) {
	return bookingpostgres.CancelReservationResult{}, nil
}

func (f *durableReservationCommandsFake) counts() (createCalls int, committed bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createCalls, f.committed
}

type convergingAdmissionFake struct {
	mu                   sync.Mutex
	acquireCalls         int
	inspectCalls         int
	finalizeCalls        int
	secondAcquireStarted chan struct{}
	secondStartedOnce    sync.Once
	replayReady          <-chan struct{}
}

func (f *convergingAdmissionFake) InspectToken(
	context.Context,
	admissionredis.PolicyScope,
	[sha256.Size]byte,
) (admissiondomain.TokenDeliveryFields, error) {
	f.mu.Lock()
	f.inspectCalls++
	f.mu.Unlock()
	return admissiondomain.TokenDeliveryFields{}, nil
}

func (f *convergingAdmissionFake) Acquire(
	ctx context.Context,
	_ admissionredis.AcquireRequest,
) (admissionredis.AcquireResult, error) {
	f.mu.Lock()
	f.acquireCalls++
	call := f.acquireCalls
	f.mu.Unlock()
	if call == 1 {
		return admissionredis.AcquireResult{
			Decision:        admissiondomain.DecisionAcquired,
			LeaseOwner:      "first-lease",
			LeaseGeneration: 1,
		}, nil
	}
	if call == 2 {
		f.secondStartedOnce.Do(func() { close(f.secondAcquireStarted) })
		select {
		case <-ctx.Done():
			return admissionredis.AcquireResult{}, ctx.Err()
		case <-f.replayReady:
		}
	}
	return admissionredis.AcquireResult{Decision: admissiondomain.DecisionReplayAllowed}, nil
}

func (f *convergingAdmissionFake) Release(context.Context, admissionredis.LeaseMutation, bool) error {
	return nil
}

func (f *convergingAdmissionFake) Finalize(context.Context, admissionredis.LeaseMutation) error {
	f.mu.Lock()
	f.finalizeCalls++
	f.mu.Unlock()
	return nil
}

func (f *convergingAdmissionFake) counts() (inspect, acquire, finalize int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inspectCalls, f.acquireCalls, f.finalizeCalls
}

type scriptedAdmissionFake struct {
	mu            sync.Mutex
	inspectErr    error
	acquireErr    error
	finalizeErr   error
	acquireResult admissionredis.AcquireResult
	inspectCalls  int
	acquireCalls  int
	releaseCalls  int
	finalizeCalls int
	repairCalls   int
	lastPermanent bool
}

func (f *scriptedAdmissionFake) InspectToken(
	context.Context,
	admissionredis.PolicyScope,
	[sha256.Size]byte,
) (admissiondomain.TokenDeliveryFields, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inspectCalls++
	return admissiondomain.TokenDeliveryFields{}, f.inspectErr
}

func (f *scriptedAdmissionFake) Acquire(
	context.Context,
	admissionredis.AcquireRequest,
) (admissionredis.AcquireResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acquireCalls++
	return f.acquireResult, f.acquireErr
}

func (f *scriptedAdmissionFake) Release(_ context.Context, _ admissionredis.LeaseMutation, permanent bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releaseCalls++
	f.lastPermanent = permanent
	return nil
}

func (f *scriptedAdmissionFake) Finalize(context.Context, admissionredis.LeaseMutation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.finalizeCalls++
	return f.finalizeErr
}

func (f *scriptedAdmissionFake) FinalizeCommitted(context.Context, admissionredis.CommittedMutation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.repairCalls++
	return nil
}

func (f *scriptedAdmissionFake) counts() (inspect, acquire, release, finalize int, permanent bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inspectCalls, f.acquireCalls, f.releaseCalls, f.finalizeCalls, f.lastPermanent
}

func (f *scriptedAdmissionFake) committedRepairCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.repairCalls
}

type bindingAdmissionFake struct {
	expectedOwner     [sha256.Size]byte
	expectedAdmission [sha256.Size]byte
	expectedBooking   [sha256.Size]byte
	inspectCalls      int
	acquireCalls      int
	mu                sync.Mutex
}

func (f *bindingAdmissionFake) InspectToken(
	context.Context,
	admissionredis.PolicyScope,
	[sha256.Size]byte,
) (admissiondomain.TokenDeliveryFields, error) {
	f.mu.Lock()
	f.inspectCalls++
	f.mu.Unlock()
	return admissiondomain.TokenDeliveryFields{}, nil
}

func (f *bindingAdmissionFake) Acquire(
	_ context.Context,
	request admissionredis.AcquireRequest,
) (admissionredis.AcquireResult, error) {
	f.mu.Lock()
	f.acquireCalls++
	f.mu.Unlock()
	if request.OwnerHash != f.expectedOwner {
		return admissionredis.AcquireResult{}, admissionredis.ErrOwnerMismatch
	}
	if request.AdmissionFingerprint != f.expectedAdmission ||
		request.BookingFingerprint != f.expectedBooking {
		return admissionredis.AcquireResult{}, admissionredis.ErrTokenMismatch
	}
	return admissionredis.AcquireResult{
		Decision:        admissiondomain.DecisionAcquired,
		LeaseOwner:      "bound-lease",
		LeaseGeneration: 1,
	}, nil
}

func (*bindingAdmissionFake) Release(context.Context, admissionredis.LeaseMutation, bool) error {
	return nil
}

func (*bindingAdmissionFake) Finalize(context.Context, admissionredis.LeaseMutation) error {
	return nil
}

type countingReservationCommandsFake struct {
	mu          sync.Mutex
	createCalls int
	create      func(context.Context, bookingpostgres.CreateHoldParams) (bookingpostgres.CreateHoldResult, error)
}

func (f *countingReservationCommandsFake) CreateHold(
	ctx context.Context,
	input bookingpostgres.CreateHoldParams,
) (bookingpostgres.CreateHoldResult, error) {
	f.mu.Lock()
	f.createCalls++
	f.mu.Unlock()
	if f.create != nil {
		return f.create(ctx, input)
	}
	return bookingpostgres.CreateHoldResult{}, nil
}

func (*countingReservationCommandsFake) ConfirmReservation(
	context.Context,
	bookingpostgres.ReservationCommandParams,
) (bookingpostgres.ConfirmReservationResult, error) {
	return bookingpostgres.ConfirmReservationResult{}, nil
}

func (*countingReservationCommandsFake) CancelReservation(
	context.Context,
	bookingpostgres.ReservationCommandParams,
) (bookingpostgres.CancelReservationResult, error) {
	return bookingpostgres.CancelReservationResult{}, nil
}

func (f *countingReservationCommandsFake) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createCalls
}

type rebalancingReservationCommandsFake struct {
	mu               sync.Mutex
	result           bookingpostgres.CreateHoldResult
	rejection        error
	targetServing    bool
	committed        bool
	createAttempts   int
	committedCreates int
	firstInput       bookingpostgres.CreateHoldParams
}

func (f *rebalancingReservationCommandsFake) CreateHold(
	_ context.Context,
	input bookingpostgres.CreateHoldParams,
) (bookingpostgres.CreateHoldResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createAttempts++
	if f.createAttempts == 1 {
		f.firstInput = input
	} else if !sameCreateHoldIdentity(f.firstInput, input) {
		return bookingpostgres.CreateHoldResult{}, bookingpostgres.ErrIdempotencyConflict
	}
	if !f.targetServing {
		return bookingpostgres.CreateHoldResult{}, f.rejection
	}
	if !f.committed {
		f.committed = true
		f.committedCreates++
	}
	return f.result, nil
}

func (f *rebalancingReservationCommandsFake) LookupCompletedCreateHold(
	_ context.Context,
	lookup bookingpostgres.CompletedCreateHoldLookupParams,
) (bookingpostgres.CreateHoldResult, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createAttempts > 0 && !sameCompletedCreateHoldIdentity(f.firstInput, lookup) {
		return bookingpostgres.CreateHoldResult{}, false, bookingpostgres.ErrIdempotencyConflict
	}
	if !f.committed {
		return bookingpostgres.CreateHoldResult{}, false, nil
	}
	result := f.result
	result.Replayed = true
	return result, true, nil
}

func (*rebalancingReservationCommandsFake) ConfirmReservation(
	context.Context,
	bookingpostgres.ReservationCommandParams,
) (bookingpostgres.ConfirmReservationResult, error) {
	return bookingpostgres.ConfirmReservationResult{}, nil
}

func (*rebalancingReservationCommandsFake) CancelReservation(
	context.Context,
	bookingpostgres.ReservationCommandParams,
) (bookingpostgres.CancelReservationResult, error) {
	return bookingpostgres.CancelReservationResult{}, nil
}

func (f *rebalancingReservationCommandsFake) activateTarget() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.targetServing = true
}

func (f *rebalancingReservationCommandsFake) counts() (attempts, committed int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createAttempts, f.committedCreates
}

func sameCreateHoldIdentity(first, next bookingpostgres.CreateHoldParams) bool {
	return first.UserID == next.UserID &&
		first.TrainRunID == next.TrainRunID &&
		bytes.Equal(first.IdempotencyKeyHash, next.IdempotencyKeyHash) &&
		bytes.Equal(first.RequestFingerprint, next.RequestFingerprint)
}

func sameCompletedCreateHoldIdentity(
	created bookingpostgres.CreateHoldParams,
	lookup bookingpostgres.CompletedCreateHoldLookupParams,
) bool {
	return created.UserID == lookup.UserID &&
		created.TrainRunID == lookup.TrainRunID &&
		bytes.Equal(created.IdempotencyKeyHash, lookup.IdempotencyKeyHash) &&
		bytes.Equal(created.RequestFingerprint, lookup.RequestFingerprint)
}

type admissionLeaseState string

const (
	admissionLeaseAvailable admissionLeaseState = "available"
	admissionLeaseAcquired  admissionLeaseState = "acquired"
	admissionLeaseConsumed  admissionLeaseState = "consumed"
)

type rebalancingAdmissionFake struct {
	mu                    sync.Mutex
	state                 admissionLeaseState
	generation            int64
	inflight              int
	inspectCalls          int
	acquireCalls          int
	releaseCalls          int
	finalizeCalls         int
	permanentReleaseCalls int
	tokenHash             [sha256.Size]byte
	currentAcquire        admissionredis.AcquireRequest
}

func (f *rebalancingAdmissionFake) InspectToken(
	_ context.Context,
	_ admissionredis.PolicyScope,
	tokenHash [sha256.Size]byte,
) (admissiondomain.TokenDeliveryFields, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inspectCalls++
	if f.inspectCalls == 1 {
		f.tokenHash = tokenHash
	} else if f.tokenHash != tokenHash {
		return admissiondomain.TokenDeliveryFields{}, admissionredis.ErrTokenMismatch
	}
	return admissiondomain.TokenDeliveryFields{}, nil
}

func (f *rebalancingAdmissionFake) Acquire(
	_ context.Context,
	request admissionredis.AcquireRequest,
) (admissionredis.AcquireResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acquireCalls++
	if uuid.Validate(request.LeaseOwner) != nil || request.TokenHash != f.tokenHash {
		return admissionredis.AcquireResult{}, admissionredis.ErrInvalidInput
	}
	if f.acquireCalls > 1 && !sameAdmissionAcquireBinding(f.currentAcquire, request) {
		return admissionredis.AcquireResult{}, admissionredis.ErrTokenMismatch
	}
	if f.state != admissionLeaseAvailable {
		return admissionredis.AcquireResult{}, errors.New("test admission token is not reusable")
	}
	f.state = admissionLeaseAcquired
	f.generation++
	f.currentAcquire = request
	return admissionredis.AcquireResult{
		Decision:        admissiondomain.DecisionAcquired,
		LeaseOwner:      request.LeaseOwner,
		LeaseGeneration: f.generation,
	}, nil
}

func (f *rebalancingAdmissionFake) Release(
	_ context.Context,
	mutation admissionredis.LeaseMutation,
	permanent bool,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releaseCalls++
	if f.state != admissionLeaseAcquired {
		return errors.New("test admission lease was not acquired")
	}
	if !sameAdmissionLeaseMutation(f.currentAcquire, f.generation, mutation) {
		return admissionredis.ErrTokenMismatch
	}
	if f.inflight != 1 {
		return errors.New("test admission inflight is inconsistent")
	}
	if permanent {
		f.inflight--
		f.permanentReleaseCalls++
		f.state = admissionLeaseConsumed
		return nil
	}
	f.state = admissionLeaseAvailable
	return nil
}

func (f *rebalancingAdmissionFake) Finalize(
	_ context.Context,
	mutation admissionredis.LeaseMutation,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.finalizeCalls++
	if f.state != admissionLeaseAcquired {
		return errors.New("test admission lease was not acquired")
	}
	if !sameAdmissionLeaseMutation(f.currentAcquire, f.generation, mutation) {
		return admissionredis.ErrTokenMismatch
	}
	if f.inflight != 1 {
		return errors.New("test admission inflight is inconsistent")
	}
	f.inflight--
	f.state = admissionLeaseConsumed
	return nil
}

func sameAdmissionAcquireBinding(first, next admissionredis.AcquireRequest) bool {
	return first.Scope == next.Scope &&
		first.TokenHash == next.TokenHash &&
		first.OwnerHash == next.OwnerHash &&
		first.AdmissionFingerprint == next.AdmissionFingerprint &&
		first.BookingFingerprint == next.BookingFingerprint &&
		first.IdempotencyKeyHash == next.IdempotencyKeyHash &&
		first.FromStopIndex == next.FromStopIndex &&
		first.ToStopIndex == next.ToStopIndex &&
		first.PassengerCount == next.PassengerCount &&
		first.ProcessingLease == next.ProcessingLease
}

func sameAdmissionLeaseMutation(
	acquire admissionredis.AcquireRequest,
	generation int64,
	mutation admissionredis.LeaseMutation,
) bool {
	return acquire.Scope == mutation.Scope &&
		acquire.TokenHash == mutation.TokenHash &&
		acquire.OwnerHash == mutation.OwnerHash &&
		acquire.BookingFingerprint == mutation.BookingFingerprint &&
		acquire.IdempotencyKeyHash == mutation.IdempotencyKeyHash &&
		acquire.LeaseOwner == mutation.LeaseOwner &&
		generation == mutation.LeaseGeneration
}

func (f *rebalancingAdmissionFake) snapshot() (
	state admissionLeaseState,
	inflight, inspect, acquire, release, finalize, permanent int,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state, f.inflight, f.inspectCalls, f.acquireCalls, f.releaseCalls, f.finalizeCalls, f.permanentReleaseCalls
}

func TestHotReservationRebalancingRetryReleasesTokenAndInflightThenCreatesOnce(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		rejection error
	}{
		{name: "assignment stale", rejection: sharding.ErrAssignmentStale},
		{name: "write fenced", rejection: sharding.ErrWriteFenced},
		{name: "train run migrating", rejection: sharding.ErrTrainRunMigrating},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := testHotTrainPolicy(t)
			command := protectedReservationCommand(policy)
			command.AdmissionToken = "same-rebalancing-token"
			reservationID := uuid.New()
			commands := &rebalancingReservationCommandsFake{
				result:    bookingpostgres.CreateHoldResult{ReservationID: reservationID},
				rejection: test.rejection,
			}
			admission := &rebalancingAdmissionFake{state: admissionLeaseAvailable, inflight: 1}
			slots, err := NewExecutionSlots(1)
			if err != nil {
				t.Fatal(err)
			}
			service := NewAdmissionProtectedReservationService(
				commands, commands,
				&journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 2}},
				&reservationReaderFake{detail: ReservationDetail{ID: reservationID.String(), Status: "held"}},
				&reservationPolicyFake{policy: policy}, admission, &admissionVerifierFake{},
				slots, fixedClock{now}, time.Minute, 6,
			)

			if _, err := service.CreateHold(context.Background(), command); !errors.Is(err, httpapi.ErrServiceTemporarilyRebalancing) {
				t.Fatalf("pre-cutover rejection = %v, want %v", err, httpapi.ErrServiceTemporarilyRebalancing)
			} else if err == httpapi.ErrServiceTemporarilyRebalancing {
				t.Fatal("pre-cutover rejection omitted its bounded Retry-After wrapper")
			}
			attempts, committed := commands.counts()
			state, admissionInflight, inspect, acquire, release, finalize, permanent := admission.snapshot()
			if attempts != 1 || committed != 0 {
				t.Fatalf("pre-cutover creates = attempts:%d committed:%d, want 1/0", attempts, committed)
			}
			if state != admissionLeaseAvailable || admissionInflight != 1 || inspect != 1 || acquire != 1 || release != 1 || finalize != 0 || permanent != 0 {
				t.Fatalf("pre-cutover token = state:%s inflight:%d inspect:%d acquire:%d release:%d finalize:%d permanent:%d",
					state, admissionInflight, inspect, acquire, release, finalize, permanent)
			}
			if inflight := slots.Inflight(); inflight != 0 {
				t.Fatalf("pre-cutover inflight = %d, want 0", inflight)
			}

			commands.activateTarget()
			created, err := service.CreateHold(context.Background(), command)
			if err != nil || created.ID != reservationID.String() {
				t.Fatalf("post-cutover retry = (%+v, %v)", created, err)
			}
			attempts, committed = commands.counts()
			state, admissionInflight, inspect, acquire, release, finalize, permanent = admission.snapshot()
			if attempts != 2 || committed != 1 {
				t.Fatalf("post-cutover creates = attempts:%d committed:%d, want 2/1", attempts, committed)
			}
			if state != admissionLeaseConsumed || admissionInflight != 0 || inspect != 2 || acquire != 2 || release != 1 || finalize != 1 || permanent != 0 {
				t.Fatalf("post-cutover token = state:%s inflight:%d inspect:%d acquire:%d release:%d finalize:%d permanent:%d",
					state, admissionInflight, inspect, acquire, release, finalize, permanent)
			}
			if inflight := slots.Inflight(); inflight != 0 {
				t.Fatalf("post-cutover inflight = %d, want 0", inflight)
			}

			replayed, err := service.CreateHold(context.Background(), command)
			if err != nil || replayed.ID != reservationID.String() {
				t.Fatalf("durable replay = (%+v, %v)", replayed, err)
			}
			attempts, committed = commands.counts()
			state, admissionInflight, inspect, acquire, release, finalize, permanent = admission.snapshot()
			if attempts != 2 || committed != 1 {
				t.Fatalf("durable replay creates = attempts:%d committed:%d, want 2/1", attempts, committed)
			}
			if state != admissionLeaseConsumed || admissionInflight != 0 || inspect != 2 || acquire != 2 || release != 1 || finalize != 1 || permanent != 0 {
				t.Fatalf("durable replay token = state:%s inflight:%d inspect:%d acquire:%d release:%d finalize:%d permanent:%d",
					state, admissionInflight, inspect, acquire, release, finalize, permanent)
			}
			if inflight := slots.Inflight(); inflight != 0 {
				t.Fatalf("durable replay inflight = %d, want 0", inflight)
			}
		})
	}
}

func TestConcurrentSameAdmissionTokenAndIdempotencyConvergeOnDurableReplay(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	policy := testHotTrainPolicy(t)
	command := protectedReservationCommand(policy)
	command.AdmissionToken = "same-opaque-token"
	reservationID := uuid.New()
	createStarted := make(chan struct{})
	allowCommit := make(chan struct{})
	replayReady := make(chan struct{})
	commands := &durableReservationCommandsFake{
		result:        bookingpostgres.CreateHoldResult{ReservationID: reservationID},
		createStarted: createStarted,
		allowCommit:   allowCommit,
		replayReady:   replayReady,
	}
	admission := &convergingAdmissionFake{
		secondAcquireStarted: make(chan struct{}),
		replayReady:          replayReady,
	}
	service := NewAdmissionProtectedReservationService(
		commands, commands,
		&journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 2}},
		&reservationReaderFake{detail: ReservationDetail{ID: reservationID.String(), Status: "held"}},
		&reservationPolicyFake{policy: policy}, admission, &admissionVerifierFake{},
		nil, fixedClock{now}, time.Minute, 6,
	)

	first := make(chan reservationCallResult, 1)
	go func() {
		view, err := service.CreateHold(context.Background(), command)
		first <- reservationCallResult{view: view, err: err}
	}()
	waitForTestSignal(t, createStarted, "first PostgreSQL create")

	second := make(chan reservationCallResult, 1)
	go func() {
		view, err := service.CreateHold(context.Background(), command)
		second <- reservationCallResult{view: view, err: err}
	}()
	waitForTestSignal(t, admission.secondAcquireStarted, "second token acquisition")
	close(allowCommit)

	for index, resultChannel := range []<-chan reservationCallResult{first, second} {
		result := waitForReservationResult(t, resultChannel)
		if result.err != nil || result.view.ID != reservationID.String() {
			t.Fatalf("concurrent result %d = (%+v, %v)", index, result.view, result.err)
		}
	}

	repeated, err := service.CreateHold(context.Background(), command)
	if err != nil || repeated.ID != reservationID.String() {
		t.Fatalf("repeated durable replay = (%+v, %v)", repeated, err)
	}
	createCalls, committed := commands.counts()
	inspectCalls, acquireCalls, finalizeCalls := admission.counts()
	if !committed || createCalls != 1 {
		t.Fatalf("durable creates = %d committed=%v, want exactly one committed create", createCalls, committed)
	}
	if inspectCalls != 2 || acquireCalls != 2 || finalizeCalls != 1 {
		t.Fatalf("admission calls = inspect:%d acquire:%d finalize:%d", inspectCalls, acquireCalls, finalizeCalls)
	}
}

func TestAdmissionBindingMismatchRejectsBeforeBookingInventory(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	policy := testHotTrainPolicy(t)
	base := protectedReservationCommand(policy)
	base.AdmissionToken = "bound-token"
	prepared, err := prepareCreateHold(base)
	if err != nil {
		t.Fatal(err)
	}
	expectedAdmission, err := admissiondomain.FingerprintAdmissionRequest(admissiondomain.AdmissionFingerprintInput{
		TrainRunID: policy.TrainRunID.String(), FromStopIndex: 0, ToStopIndex: 2,
		SeatClass: base.SeatClass, PassengerCount: len(base.PassengerIDs),
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		command func() httpapi.CreateReservationCommand
	}{
		{
			name: "wrong user",
			command: func() httpapi.CreateReservationCommand {
				command := base
				command.OwnerID = uuid.NewString()
				return command
			},
		},
		{
			name: "request fingerprint mismatch",
			command: func() httpapi.CreateReservationCommand {
				command := base
				command.PassengerIDs = []string{uuid.NewString()}
				return command
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			commands := &countingReservationCommandsFake{}
			admission := &bindingAdmissionFake{
				expectedOwner: hashAdmissionOwner(prepared.owner), expectedAdmission: expectedAdmission,
				expectedBooking: prepared.bookingFingerprint,
			}
			service := NewAdmissionProtectedReservationService(
				commands, &completedReplayFake{},
				&journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 2}},
				&reservationReaderFake{}, &reservationPolicyFake{policy: policy}, admission,
				&admissionVerifierFake{}, nil, fixedClock{now}, time.Minute, 6,
			)
			_, err := service.CreateHold(context.Background(), test.command())
			if !errors.Is(err, httpapi.ErrAdmissionInvalid) {
				t.Fatalf("binding mismatch error = %v, want %v", err, httpapi.ErrAdmissionInvalid)
			}
			if commands.calls() != 0 {
				t.Fatalf("binding mismatch reached authoritative booking inventory %d time(s)", commands.calls())
			}
			admission.mu.Lock()
			inspectCalls, acquireCalls := admission.inspectCalls, admission.acquireCalls
			admission.mu.Unlock()
			if inspectCalls != 1 || acquireCalls != 1 {
				t.Fatalf("binding checks = inspect:%d acquire:%d", inspectCalls, acquireCalls)
			}
		})
	}
}

func TestHotRedisOutageFailsClosedWhileNonHotReservationStillUsesPostgres(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	policy := testHotTrainPolicy(t)
	command := protectedReservationCommand(policy)
	command.AdmissionToken = "opaque"
	outage := &scriptedAdmissionFake{inspectErr: admissionredis.ErrBackendUnavailable}
	hotCommands := &countingReservationCommandsFake{}
	hotService := NewAdmissionProtectedReservationService(
		hotCommands, &completedReplayFake{},
		&journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 2}},
		&reservationReaderFake{}, &reservationPolicyFake{policy: policy}, outage,
		&admissionVerifierFake{}, nil, fixedClock{now}, time.Minute, 6,
	)
	if _, err := hotService.CreateHold(context.Background(), command); !errors.Is(err, httpapi.ErrUnavailable) {
		t.Fatalf("hot Redis outage error = %v, want %v", err, httpapi.ErrUnavailable)
	}
	if hotCommands.calls() != 0 {
		t.Fatal("hot Redis outage reached PostgreSQL booking")
	}

	reservationID := uuid.New()
	nonHotCommands := &countingReservationCommandsFake{create: func(
		context.Context,
		bookingpostgres.CreateHoldParams,
	) (bookingpostgres.CreateHoldResult, error) {
		return bookingpostgres.CreateHoldResult{ReservationID: reservationID}, nil
	}}
	disabled := policy
	disabled.Enabled = false
	nonHotService := NewAdmissionProtectedReservationService(
		nonHotCommands, &completedReplayFake{},
		&journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 2}},
		&reservationReaderFake{detail: ReservationDetail{ID: reservationID.String(), Status: "held"}},
		&reservationPolicyFake{policy: disabled},
		outage, &admissionVerifierFake{}, nil, fixedClock{now}, time.Minute, 6,
	)
	view, err := nonHotService.CreateHold(context.Background(), command)
	if err != nil || view.ID != reservationID.String() {
		t.Fatalf("non-hot reservation during Redis outage = (%+v, %v)", view, err)
	}
	if nonHotCommands.calls() != 1 {
		t.Fatalf("non-hot PostgreSQL creates = %d, want 1", nonHotCommands.calls())
	}
	inspectCalls, _, _, _, _ := outage.counts()
	if inspectCalls != 1 {
		t.Fatalf("non-hot path touched Redis; total inspect calls = %d, want only hot call", inspectCalls)
	}
}

func TestCommittedHotHoldSurvivesFinalizeFailureAndReplaysWithoutDuplicateCreate(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	policy := testHotTrainPolicy(t)
	command := protectedReservationCommand(policy)
	command.AdmissionToken = "opaque"
	reservationID := uuid.New()
	commands := &durableReservationCommandsFake{
		result: bookingpostgres.CreateHoldResult{ReservationID: reservationID},
	}
	admission := &scriptedAdmissionFake{
		acquireResult: admissionredis.AcquireResult{
			Decision: admissiondomain.DecisionAcquired, LeaseOwner: "lease", LeaseGeneration: 1,
		},
		finalizeErr: admissionredis.ErrBackendUnavailable,
	}
	service := NewAdmissionProtectedReservationService(
		commands, commands,
		&journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 2}},
		&reservationReaderFake{detail: ReservationDetail{ID: reservationID.String(), Status: "held"}},
		&reservationPolicyFake{policy: policy}, admission, &admissionVerifierFake{},
		nil, fixedClock{now}, time.Minute, 6,
	)

	first, err := service.CreateHold(context.Background(), command)
	if err != nil || first.ID != reservationID.String() {
		t.Fatalf("committed result after finalize failure = (%+v, %v)", first, err)
	}
	replay, err := service.CreateHold(context.Background(), command)
	if err != nil || replay.ID != reservationID.String() {
		t.Fatalf("durable replay after finalize failure = (%+v, %v)", replay, err)
	}
	createCalls, committed := commands.counts()
	inspectCalls, acquireCalls, releaseCalls, finalizeCalls, _ := admission.counts()
	if !committed || createCalls != 1 {
		t.Fatalf("durable creates = %d committed=%v, want exactly one", createCalls, committed)
	}
	if inspectCalls != 1 || acquireCalls != 1 || releaseCalls != 0 || finalizeCalls != 1 {
		t.Fatalf("Redis calls = inspect:%d acquire:%d release:%d finalize:%d",
			inspectCalls, acquireCalls, releaseCalls, finalizeCalls)
	}
	if repairCalls := admission.committedRepairCalls(); repairCalls != 1 {
		t.Fatalf("durable replay repair calls = %d, want 1", repairCalls)
	}
}

func TestCanceledHotReservationReleasesLeaseWithoutPermanentConsumption(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	policy := testHotTrainPolicy(t)
	command := protectedReservationCommand(policy)
	command.AdmissionToken = "opaque"
	enteredBooking := make(chan struct{})
	var enteredOnce sync.Once
	commands := &countingReservationCommandsFake{create: func(
		ctx context.Context,
		_ bookingpostgres.CreateHoldParams,
	) (bookingpostgres.CreateHoldResult, error) {
		enteredOnce.Do(func() { close(enteredBooking) })
		<-ctx.Done()
		return bookingpostgres.CreateHoldResult{}, ctx.Err()
	}}
	admission := &scriptedAdmissionFake{acquireResult: admissionredis.AcquireResult{
		Decision: admissiondomain.DecisionAcquired, LeaseOwner: "cancelled-lease", LeaseGeneration: 1,
	}}
	service := NewAdmissionProtectedReservationService(
		commands, &completedReplayFake{},
		&journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 2}},
		&reservationReaderFake{}, &reservationPolicyFake{policy: policy}, admission,
		&admissionVerifierFake{}, nil, fixedClock{now}, time.Minute, 6,
	)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := service.CreateHold(ctx, command)
		result <- err
	}()
	waitForTestSignal(t, enteredBooking, "booking transaction entry")
	cancel()
	err := waitForTestError(t, result)
	if !errors.Is(err, httpapi.ErrUnavailable) {
		t.Fatalf("cancelled hot reservation error = %v, want safe unavailable", err)
	}
	_, _, releaseCalls, finalizeCalls, permanent := admission.counts()
	if releaseCalls != 1 || permanent || finalizeCalls != 0 {
		t.Fatalf("cancelled lease = release:%d permanent:%v finalize:%d", releaseCalls, permanent, finalizeCalls)
	}
}

func TestHotReservationDatabaseDeadlineEndsBeforeLeaseAndReleases(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	policy := testHotTrainPolicy(t)
	command := protectedReservationCommand(policy)
	command.AdmissionToken = "opaque"
	commands := &countingReservationCommandsFake{create: func(
		ctx context.Context,
		_ bookingpostgres.CreateHoldParams,
	) (bookingpostgres.CreateHoldResult, error) {
		<-ctx.Done()
		return bookingpostgres.CreateHoldResult{}, ctx.Err()
	}}
	admission := &scriptedAdmissionFake{acquireResult: admissionredis.AcquireResult{
		Decision: admissiondomain.DecisionAcquired, LeaseOwner: uuid.NewString(), LeaseGeneration: 1,
	}}
	service := NewAdmissionProtectedReservationService(
		commands, &completedReplayFake{},
		&journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 2}},
		&reservationReaderFake{}, &reservationPolicyFake{policy: policy}, admission,
		&admissionVerifierFake{}, nil, fixedClock{now}, time.Minute, 6,
	).WithDatabaseCommandTimeout(20 * time.Millisecond)

	started := time.Now()
	_, err := service.CreateHold(context.Background(), command)
	if !errors.Is(err, httpapi.ErrUnavailable) {
		t.Fatalf("deadline error = %v, want safe unavailable", err)
	}
	if elapsed := time.Since(started); elapsed >= admissiondomain.MinProcessingLease {
		t.Fatalf("database attempt lasted %s, must end before %s lease", elapsed, admissiondomain.MinProcessingLease)
	}
	_, _, releaseCalls, finalizeCalls, permanent := admission.counts()
	if releaseCalls != 1 || permanent || finalizeCalls != 0 {
		t.Fatalf("deadline lease = release:%d permanent:%v finalize:%d", releaseCalls, permanent, finalizeCalls)
	}
}

func waitForTestSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", name)
	}
}

func waitForReservationResult(t *testing.T, result <-chan reservationCallResult) reservationCallResult {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case value := <-result:
		return value
	case <-timer.C:
		t.Fatal("timed out waiting for reservation result")
		return reservationCallResult{}
	}
}

func waitForTestError(t *testing.T, result <-chan error) error {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-timer.C:
		t.Fatal("timed out waiting for error result")
		return nil
	}
}
