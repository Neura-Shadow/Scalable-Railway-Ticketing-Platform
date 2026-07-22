package app

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

type replayStep struct {
	result bookingpostgres.CreateHoldResult
	found  bool
	err    error
}

type completedReplayFake struct {
	steps []replayStep
	calls int
}

func (f *completedReplayFake) LookupCompletedCreateHold(context.Context, bookingpostgres.CompletedCreateHoldLookupParams) (bookingpostgres.CreateHoldResult, bool, error) {
	index := f.calls
	f.calls++
	if index >= len(f.steps) {
		return bookingpostgres.CreateHoldResult{}, false, nil
	}
	step := f.steps[index]
	return step.result, step.found, step.err
}

type reservationPolicyFake struct {
	policy admissiondomain.HotTrainPolicy
	err    error
	calls  int
}

func (f *reservationPolicyFake) GetPolicy(context.Context, uuid.UUID, offeringdomain.SeatClass) (admissiondomain.HotTrainPolicy, error) {
	f.calls++
	return f.policy, f.err
}

type reservationAdmissionFake struct {
	fields        admissiondomain.TokenDeliveryFields
	inspectErr    error
	acquire       admissionredis.AcquireResult
	acquireErr    error
	inspectCalls  int
	acquireCalls  int
	finalizeCalls int
	releaseCalls  int
	lastPermanent bool
}

func (f *reservationAdmissionFake) InspectToken(context.Context, admissionredis.PolicyScope, [sha256.Size]byte) (admissiondomain.TokenDeliveryFields, error) {
	f.inspectCalls++
	return f.fields, f.inspectErr
}
func (f *reservationAdmissionFake) Acquire(context.Context, admissionredis.AcquireRequest) (admissionredis.AcquireResult, error) {
	f.acquireCalls++
	return f.acquire, f.acquireErr
}
func (f *reservationAdmissionFake) Release(_ context.Context, _ admissionredis.LeaseMutation, permanent bool) error {
	f.releaseCalls++
	f.lastPermanent = permanent
	return nil
}
func (f *reservationAdmissionFake) Finalize(context.Context, admissionredis.LeaseMutation) error {
	f.finalizeCalls++
	return nil
}

type admissionVerifierFake struct {
	err   error
	calls int
}

func (f *admissionVerifierFake) Verify(string, admissiondomain.TokenDeliveryFields) error {
	f.calls++
	return f.err
}

func TestCompletedReservationReplayPrecedesJourneyAndCurrentPolicy(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	policy := testHotTrainPolicy(t)
	reservationID := uuid.New()
	command := protectedReservationCommand(policy)
	replays := &completedReplayFake{steps: []replayStep{{
		result: bookingpostgres.CreateHoldResult{ReservationID: reservationID, Replayed: true},
		found:  true,
	}}}
	journeys := &journeyResolverFake{}
	policies := &reservationPolicyFake{policy: policy}
	reader := &reservationReaderFake{detail: ReservationDetail{ID: reservationID.String(), Status: "held"}}
	service := NewAdmissionProtectedReservationService(
		&reservationCommandsFake{}, replays, journeys, reader, policies, &reservationAdmissionFake{},
		&admissionVerifierFake{}, nil, fixedClock{now}, time.Minute, 6,
	)
	view, err := service.CreateHold(context.Background(), command)
	if err != nil || view.ID != reservationID.String() {
		t.Fatalf("CreateHold() = (%+v, %v)", view, err)
	}
	if replays.calls != 1 || journeys.input != [3]string{} || policies.calls != 0 {
		t.Fatalf("replay ordering calls: replay=%d journey=%v policy=%d", replays.calls, journeys.input, policies.calls)
	}
}

func TestHotReservationRequiresAdmissionAndProcessingRetryDoesNotCreate(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	policy := testHotTrainPolicy(t)
	command := protectedReservationCommand(policy)
	commands := &reservationCommandsFake{}
	replays := &completedReplayFake{}
	service := NewAdmissionProtectedReservationService(
		commands, replays,
		&journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 2}},
		&reservationReaderFake{}, &reservationPolicyFake{policy: policy}, &reservationAdmissionFake{},
		&admissionVerifierFake{}, nil, fixedClock{now}, time.Minute, 6,
	)
	_, err := service.CreateHold(context.Background(), command)
	if !errors.Is(err, httpapi.ErrAdmissionRequired) {
		t.Fatalf("missing token error = %v, want %v", err, httpapi.ErrAdmissionRequired)
	}
	if commands.createInput.UserID != uuid.Nil {
		t.Fatal("missing admission reached PostgreSQL create")
	}

	command.AdmissionToken = "opaque"
	admission := &reservationAdmissionFake{acquire: admissionredis.AcquireResult{
		Decision: admissiondomain.DecisionRetryAllowed, RetryAfter: 1500 * time.Millisecond,
	}}
	replays = &completedReplayFake{}
	service = NewAdmissionProtectedReservationService(
		commands, replays,
		&journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 2}},
		&reservationReaderFake{}, &reservationPolicyFake{policy: policy}, admission,
		&admissionVerifierFake{}, nil, fixedClock{now}, time.Minute, 6,
	)
	_, err = service.CreateHold(context.Background(), command)
	if !errors.Is(err, httpapi.ErrAdmissionInProgress) {
		t.Fatalf("processing retry error = %v, want %v", err, httpapi.ErrAdmissionInProgress)
	}
	if replays.calls != 2 || admission.acquireCalls != 1 || commands.createInput.UserID != uuid.Nil {
		t.Fatalf("processing retry calls: replay=%d acquire=%d create=%+v", replays.calls, admission.acquireCalls, commands.createInput)
	}
}

func TestHotReservationVerifiesMACBeforeAcquire(t *testing.T) {
	t.Parallel()
	policy := testHotTrainPolicy(t)
	command := protectedReservationCommand(policy)
	command.AdmissionToken = "tampered"
	admission := &reservationAdmissionFake{}
	verifier := &admissionVerifierFake{err: admissiondomain.ErrInvalidAdmissionToken}
	service := NewAdmissionProtectedReservationService(
		&reservationCommandsFake{}, &completedReplayFake{},
		&journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 2}},
		&reservationReaderFake{}, &reservationPolicyFake{policy: policy}, admission, verifier,
		nil, fixedClock{time.Now().UTC()}, time.Minute, 6,
	)
	_, err := service.CreateHold(context.Background(), command)
	if !errors.Is(err, httpapi.ErrAdmissionInvalid) {
		t.Fatalf("tampered token error = %v, want %v", err, httpapi.ErrAdmissionInvalid)
	}
	if verifier.calls != 1 || admission.acquireCalls != 0 {
		t.Fatalf("verification/acquire calls = %d/%d", verifier.calls, admission.acquireCalls)
	}
}

func TestHotReservationFinalizesCommitAndReleasesLeaseOnBackpressure(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	policy := testHotTrainPolicy(t)
	command := protectedReservationCommand(policy)
	command.AdmissionToken = "opaque"
	reservationID := uuid.New()
	commands := &reservationCommandsFake{createResult: bookingpostgres.CreateHoldResult{ReservationID: reservationID}}
	admission := &reservationAdmissionFake{acquire: admissionredis.AcquireResult{
		Decision: admissiondomain.DecisionAcquired, LeaseOwner: uuid.NewString(), LeaseGeneration: 1,
	}}
	slots, err := NewExecutionSlots(1)
	if err != nil {
		t.Fatal(err)
	}
	reader := &reservationReaderFake{detail: ReservationDetail{ID: reservationID.String(), Status: "held"}}
	service := NewAdmissionProtectedReservationService(
		commands, &completedReplayFake{},
		&journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 2}},
		reader, &reservationPolicyFake{policy: policy}, admission, &admissionVerifierFake{},
		slots, fixedClock{now}, time.Minute, 6,
	)
	view, err := service.CreateHold(context.Background(), command)
	if err != nil || view.ID != reservationID.String() || admission.finalizeCalls != 1 || slots.Inflight() != 0 {
		t.Fatalf("committed hot hold = (%+v, %v), finalize=%d inflight=%d", view, err, admission.finalizeCalls, slots.Inflight())
	}
	if commands.createInput.AdmissionPolicy == nil ||
		commands.createInput.AdmissionPolicy.PolicyID != policy.ID ||
		commands.createInput.AdmissionPolicy.Version != policy.Version {
		t.Fatalf("booking policy decision = %+v, want %s version %d", commands.createInput.AdmissionPolicy, policy.ID, policy.Version)
	}

	blockingRelease, ok := slots.TryAcquire()
	if !ok {
		t.Fatal("failed to occupy local slot")
	}
	defer blockingRelease()
	admission.finalizeCalls = 0
	commands.createInput = bookingpostgres.CreateHoldParams{}
	_, err = service.CreateHold(context.Background(), command)
	if !errors.Is(err, httpapi.ErrReservationBackpressure) {
		t.Fatalf("backpressure error = %v, want %v", err, httpapi.ErrReservationBackpressure)
	}
	if admission.releaseCalls != 1 || admission.lastPermanent || commands.createInput.UserID != uuid.Nil {
		t.Fatalf("backpressure release/create = calls:%d permanent:%v input:%+v", admission.releaseCalls, admission.lastPermanent, commands.createInput)
	}
}

func TestHotReservationPermanentlyReleasesStalePolicyLease(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	policy := testHotTrainPolicy(t)
	command := protectedReservationCommand(policy)
	command.AdmissionToken = "opaque"
	commands := &reservationCommandsFake{err: bookingpostgres.ErrAdmissionPolicyChanged}
	admission := &reservationAdmissionFake{acquire: admissionredis.AcquireResult{
		Decision: admissiondomain.DecisionAcquired, LeaseOwner: uuid.NewString(), LeaseGeneration: 1,
	}}
	service := NewAdmissionProtectedReservationService(
		commands, &completedReplayFake{},
		&journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 2}},
		&reservationReaderFake{}, &reservationPolicyFake{policy: policy}, admission,
		&admissionVerifierFake{}, nil, fixedClock{now}, time.Minute, 6,
	)

	_, err := service.CreateHold(context.Background(), command)
	if !errors.Is(err, httpapi.ErrAdmissionExpired) {
		t.Fatalf("stale policy error = %v, want %v", err, httpapi.ErrAdmissionExpired)
	}
	if admission.releaseCalls != 1 || !admission.lastPermanent || admission.finalizeCalls != 0 {
		t.Fatalf("stale policy lease mutations: release=%d permanent=%v finalize=%d",
			admission.releaseCalls, admission.lastPermanent, admission.finalizeCalls)
	}
}

func TestHotReservationQuotaFailureCancelsAdmissionAndAddsRetryHint(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	policy := testHotTrainPolicy(t)
	command := protectedReservationCommand(policy)
	command.AdmissionToken = "opaque"
	commands := &reservationCommandsFake{err: bookingpostgres.ErrReservationQuotaExceeded}
	admission := &reservationAdmissionFake{acquire: admissionredis.AcquireResult{
		Decision: admissiondomain.DecisionAcquired, LeaseOwner: uuid.NewString(), LeaseGeneration: 1,
	}}
	service := NewAdmissionProtectedReservationService(
		commands, &completedReplayFake{},
		&journeyResolverFake{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 2}},
		&reservationReaderFake{}, &reservationPolicyFake{policy: policy}, admission,
		&admissionVerifierFake{}, nil, fixedClock{now}, time.Minute, 6,
	)

	_, err := service.CreateHold(context.Background(), command)
	if !errors.Is(err, httpapi.ErrReservationQuotaExceeded) {
		t.Fatalf("quota error = %v, want %v", err, httpapi.ErrReservationQuotaExceeded)
	}
	if err == httpapi.ErrReservationQuotaExceeded {
		t.Fatal("quota error omitted its bounded Retry-After wrapper")
	}
	if admission.releaseCalls != 1 || !admission.lastPermanent || admission.finalizeCalls != 0 {
		t.Fatalf("quota lease mutations: release=%d permanent=%v finalize=%d",
			admission.releaseCalls, admission.lastPermanent, admission.finalizeCalls)
	}
}

func TestAdmissionMetricReasonsAreBoundedAndSpecific(t *testing.T) {
	t.Parallel()

	tokenTests := []struct {
		err        error
		wantResult string
		wantReason string
	}{
		{admissionredis.ErrTerminal, "expired", "token_expired"},
		{admissionredis.ErrTokenMismatch, "conflict", "binding_conflict"},
		{admissionredis.ErrPolicyMismatch, "failure", "policy_version"},
		{admissionredis.ErrContinuityLost, "failure", "continuity_lost"},
		{admissionredis.ErrBackendUnavailable, "failure", "redis"},
	}
	for _, test := range tokenTests {
		result, reason := admissionTokenFailure(test.err)
		if result != test.wantResult || reason != test.wantReason {
			t.Fatalf("admissionTokenFailure(%v) = %q/%q, want %q/%q",
				test.err, result, reason, test.wantResult, test.wantReason)
		}
	}

	hotTests := []struct {
		err        error
		wantReason string
	}{
		{httpapi.ErrAdmissionRequired, "admission_required"},
		{httpapi.ErrAdmissionInvalid, "token_invalid"},
		{httpapi.ErrAdmissionExpired, "token_expired"},
		{httpapi.ErrAdmissionInProgress, "processing"},
		{httpapi.ErrReservationQuotaExceeded, "quota"},
		{httpapi.ErrReservationBackpressure, "backpressure"},
		{httpapi.ErrUnavailable, "unavailable"},
	}
	for _, test := range hotTests {
		if reason := hotReservationFailureReason(test.err); reason != test.wantReason {
			t.Fatalf("hotReservationFailureReason(%v) = %q, want %q", test.err, reason, test.wantReason)
		}
	}
}

func protectedReservationCommand(policy admissiondomain.HotTrainPolicy) httpapi.CreateReservationCommand {
	return httpapi.CreateReservationCommand{
		OwnerID: uuid.NewString(), IdempotencyKey: "protected-idempotency",
		TrainRunID: policy.TrainRunID.String(), OriginStationCode: "AAA", DestinationStationCode: "BBB",
		SeatClass: policy.SeatClass.String(), PassengerIDs: []string{uuid.NewString()},
	}
}
