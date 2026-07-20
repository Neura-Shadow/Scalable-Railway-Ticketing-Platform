package app

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"
	"time"

	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	querypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

type fakeWaitingPolicyReader struct {
	policy admissiondomain.HotTrainPolicy
	err    error
}

func (f fakeWaitingPolicyReader) GetPolicy(context.Context, uuid.UUID, offeringdomain.SeatClass) (admissiondomain.HotTrainPolicy, error) {
	return f.policy, f.err
}

type fakeJourneyResolver struct {
	journey querypostgres.Journey
	err     error
}

func (f fakeJourneyResolver) ResolveJourney(context.Context, string, string, string) (querypostgres.Journey, error) {
	return f.journey, f.err
}

type fakeWaitingControl struct {
	entry        admissiondomain.WaitingRoomEntry
	scope        admissionredis.PolicyScope
	duplicate    bool
	err          error
	cancelErr    error
	cancelResult admissionredis.CancelResult
	locatorPut   bool
	cancelled    bool
	getCalls     int
	delivery     admissiondomain.TokenDeliveryFields
	deliveryErr  error
	claimCalls   int
}

func (f *fakeWaitingControl) Join(context.Context, admissionredis.JoinRequest) (admissiondomain.WaitingRoomEntry, bool, error) {
	return f.entry, f.duplicate, f.err
}
func (f *fakeWaitingControl) PutEntryLocator(context.Context, string, admissionredis.PolicyScope, time.Duration) error {
	f.locatorPut = true
	return f.err
}
func (f *fakeWaitingControl) ResolveEntryLocator(context.Context, string) (admissionredis.PolicyScope, error) {
	return f.scope, f.err
}
func (f *fakeWaitingControl) Get(context.Context, admissionredis.PolicyScope, string, [sha256.Size]byte) (admissiondomain.WaitingRoomEntry, error) {
	f.getCalls++
	return f.entry, f.err
}
func (f *fakeWaitingControl) InspectDelivery(context.Context, admissionredis.PolicyScope, string, [sha256.Size]byte) (admissiondomain.TokenDeliveryFields, error) {
	return f.delivery, f.deliveryErr
}
func (f *fakeWaitingControl) ClaimDelivery(context.Context, admissionredis.PolicyScope, string, [sha256.Size]byte) (admissiondomain.TokenDeliveryFields, error) {
	f.claimCalls++
	return f.delivery, f.deliveryErr
}
func (f *fakeWaitingControl) Cancel(context.Context, admissionredis.PolicyScope, string, [sha256.Size]byte) (admissionredis.CancelResult, error) {
	f.cancelled = true
	return f.cancelResult, f.cancelErr
}

type fakeTokenReconstructor struct {
	raw   string
	err   error
	calls *int
}

func (f fakeTokenReconstructor) Reconstruct(admissiondomain.TokenDeliveryFields) (string, error) {
	if f.calls != nil {
		*f.calls++
	}
	return f.raw, f.err
}

func TestWaitingRoomJoinWritesLocatorAndReturnsBoundedRetry(t *testing.T) {
	t.Parallel()
	policy := testHotTrainPolicy(t)
	entry := waitingEntry(policy, admissiondomain.EntryQueued)
	entry.Position.Approximate = 250
	control := &fakeWaitingControl{entry: entry, scope: scopeForWaitingRoomPolicy(policy), duplicate: true}
	service := NewWaitingRoomService(
		fakeWaitingPolicyReader{policy: policy},
		fakeJourneyResolver{journey: querypostgres.Journey{FromStopIndex: 1, ToStopIndex: 3}},
		control, fakeTokenReconstructor{raw: "unused"},
	)
	result, err := service.JoinWaitingRoom(context.Background(), httpapi.JoinWaitingRoomCommand{
		OwnerID: uuid.NewString(), TrainRunID: policy.TrainRunID.String(),
		OriginStationCode: "AAA", DestinationStationCode: "BBB",
		SeatClass: "STANDARD", PassengerCount: 2,
	})
	if err != nil {
		t.Fatalf("JoinWaitingRoom() error = %v", err)
	}
	if result.EntryID != entry.ID || result.RetryAfterSeconds != 25 || !control.locatorPut {
		t.Fatalf("JoinWaitingRoom() result/state = (%+v, locator=%v)", result, control.locatorPut)
	}
}

func TestWaitingRoomAdmittedTokenIsHeaderOnlyViewData(t *testing.T) {
	t.Parallel()
	policy := testHotTrainPolicy(t)
	entry := waitingEntry(policy, admissiondomain.EntryAdmitted)
	control := &fakeWaitingControl{entry: entry, scope: scopeForWaitingRoomPolicy(policy)}
	service := NewWaitingRoomService(
		fakeWaitingPolicyReader{policy: policy}, fakeJourneyResolver{}, control,
		fakeTokenReconstructor{raw: "raw-admission-token"},
	)
	result, err := service.GetWaitingRoomEntry(context.Background(), uuid.NewString(), entry.ID)
	if err != nil {
		t.Fatalf("GetWaitingRoomEntry() error = %v", err)
	}
	if result.AdmissionToken != "raw-admission-token" {
		t.Fatalf("AdmissionToken = %q", result.AdmissionToken)
	}
	if control.claimCalls != 1 {
		t.Fatalf("ClaimDelivery calls = %d, want 1 after successful preflight", control.claimCalls)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) == "" || containsBytes(encoded, []byte("raw-admission-token")) {
		t.Fatalf("serialized view leaked admission token: %s", encoded)
	}
}

func TestWaitingRoomUnknownTokenKeyDoesNotBurnAtMostOnceDelivery(t *testing.T) {
	t.Parallel()
	policy := testHotTrainPolicy(t)
	entry := waitingEntry(policy, admissiondomain.EntryAdmitted)
	control := &fakeWaitingControl{entry: entry, scope: scopeForWaitingRoomPolicy(policy)}
	reconstructCalls := 0
	service := NewWaitingRoomService(
		fakeWaitingPolicyReader{policy: policy}, fakeJourneyResolver{}, control,
		fakeTokenReconstructor{err: admissiondomain.ErrUnknownAdmissionTokenKey, calls: &reconstructCalls},
	)

	_, err := service.GetWaitingRoomEntry(context.Background(), uuid.NewString(), entry.ID)
	if !errors.Is(err, httpapi.ErrUnavailable) {
		t.Fatalf("GetWaitingRoomEntry() error = %v, want unavailable", err)
	}
	if reconstructCalls != 1 {
		t.Fatalf("preflight reconstruct calls = %d, want 1", reconstructCalls)
	}
	if control.claimCalls != 0 {
		t.Fatalf("ClaimDelivery calls = %d; unknown key burned at-most-once delivery", control.claimCalls)
	}
}

func TestWaitingRoomVerifiesClaimedFieldsAfterClaim(t *testing.T) {
	t.Parallel()
	policy := testHotTrainPolicy(t)
	entry := waitingEntry(policy, admissiondomain.EntryAdmitted)
	control := &fakeWaitingControl{entry: entry, scope: scopeForWaitingRoomPolicy(policy)}
	reconstructCalls := 0
	service := NewWaitingRoomService(
		fakeWaitingPolicyReader{policy: policy}, fakeJourneyResolver{}, control,
		fakeTokenReconstructor{raw: "raw-admission-token", calls: &reconstructCalls},
	)

	if _, err := service.GetWaitingRoomEntry(context.Background(), uuid.NewString(), entry.ID); err != nil {
		t.Fatalf("GetWaitingRoomEntry() error = %v", err)
	}
	if reconstructCalls != 2 {
		t.Fatalf("Reconstruct calls = %d, want preflight and post-claim verification", reconstructCalls)
	}
}

func TestWaitingRoomOwnershipAndQueueFailuresMapSafely(t *testing.T) {
	t.Parallel()
	policy := testHotTrainPolicy(t)
	service := NewWaitingRoomService(
		fakeWaitingPolicyReader{policy: policy},
		fakeJourneyResolver{journey: querypostgres.Journey{FromStopIndex: 0, ToStopIndex: 1}},
		&fakeWaitingControl{entry: waitingEntry(policy, admissiondomain.EntryQueued), err: admissionredis.ErrQueueFull},
		fakeTokenReconstructor{},
	)
	_, err := service.JoinWaitingRoom(context.Background(), httpapi.JoinWaitingRoomCommand{
		OwnerID: uuid.NewString(), TrainRunID: policy.TrainRunID.String(),
		OriginStationCode: "AAA", DestinationStationCode: "BBB", SeatClass: "standard", PassengerCount: 1,
	})
	if !errors.Is(err, httpapi.ErrQueueFull) {
		t.Fatalf("queue-full error = %v, want %v", err, httpapi.ErrQueueFull)
	}

	control := &fakeWaitingControl{entry: waitingEntry(policy, admissiondomain.EntryQueued), scope: scopeForWaitingRoomPolicy(policy), err: admissionredis.ErrOwnerMismatch}
	service = NewWaitingRoomService(fakeWaitingPolicyReader{}, fakeJourneyResolver{}, control, fakeTokenReconstructor{})
	_, err = service.GetWaitingRoomEntry(context.Background(), uuid.NewString(), control.entry.ID)
	if !errors.Is(err, httpapi.ErrNotFound) {
		t.Fatalf("owner-mismatch error = %v, want %v", err, httpapi.ErrNotFound)
	}
}

func TestWaitingRoomCancelReturnsPreCancelViewAfterPhysicalRedisCleanup(t *testing.T) {
	t.Parallel()
	policy := testHotTrainPolicy(t)
	entry := waitingEntry(policy, admissiondomain.EntryQueued)
	entry.Position.Approximate = 7
	control := &fakeWaitingControl{entry: entry, scope: scopeForWaitingRoomPolicy(policy)}
	service := NewWaitingRoomService(
		fakeWaitingPolicyReader{}, fakeJourneyResolver{}, control, fakeTokenReconstructor{},
	)

	result, err := service.CancelWaitingRoomEntry(context.Background(), uuid.NewString(), entry.ID)
	if err != nil {
		t.Fatalf("CancelWaitingRoomEntry() error = %v", err)
	}
	if !control.cancelled || control.getCalls != 1 {
		t.Fatalf("cancel state = %v, Get calls = %d; want one pre-cancel read only", control.cancelled, control.getCalls)
	}
	if result.EntryID != entry.ID || result.Status != string(admissiondomain.EntryCancelled) ||
		result.ApproximatePosition != 0 {
		t.Fatalf("cancel result = %+v", result)
	}
}

func TestWaitingRoomProcessingCancelMapsToAdmissionInProgress(t *testing.T) {
	t.Parallel()
	policy := testHotTrainPolicy(t)
	entry := waitingEntry(policy, admissiondomain.EntryAdmitted)
	control := &fakeWaitingControl{
		entry: entry, scope: scopeForWaitingRoomPolicy(policy), cancelErr: admissionredis.ErrInProgress,
	}
	service := NewWaitingRoomService(
		fakeWaitingPolicyReader{}, fakeJourneyResolver{}, control, fakeTokenReconstructor{},
	)

	_, err := service.CancelWaitingRoomEntry(context.Background(), uuid.NewString(), entry.ID)
	if !errors.Is(err, httpapi.ErrAdmissionInProgress) {
		t.Fatalf("CancelWaitingRoomEntry() error = %v, want admission in progress", err)
	}
}

func waitingEntry(policy admissiondomain.HotTrainPolicy, status admissiondomain.EntryStatus) admissiondomain.WaitingRoomEntry {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	entry := admissiondomain.WaitingRoomEntry{
		ID: uuid.NewString(), PolicyID: policy.ID.String(), PolicyVersion: policy.Version,
		TrainRunID: policy.TrainRunID.String(), SeatClass: policy.SeatClass.String(), Status: status,
		OwnerHash: sha256.Sum256([]byte("owner")), AdmissionFingerprint: sha256.Sum256([]byte("request")),
		JoinedAt: now, ExpiresAt: now.Add(policy.Limits.QueueEntryTTL),
	}
	if status == admissiondomain.EntryAdmitted {
		admitted := now.Add(time.Minute)
		entry.AdmittedAt = &admitted
	}
	return entry
}

func containsBytes(value, pattern []byte) bool {
	if len(pattern) == 0 || len(pattern) > len(value) {
		return false
	}
	for index := 0; index <= len(value)-len(pattern); index++ {
		match := true
		for offset := range pattern {
			if value[index+offset] != pattern[offset] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
