package app

import (
	"context"
	"errors"
	"testing"
	"time"

	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/postgres"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

type fakeHotTrainPolicyStore struct {
	policies []admissiondomain.HotTrainPolicy
	created  admissionpostgres.CreatePolicyParams
	updated  admissionpostgres.UpdatePolicyParams
	disabled admissionpostgres.MutationMetadata
	listed   admissionpostgres.ListPoliciesParams
	err      error
}

func (s *fakeHotTrainPolicyStore) ListPoliciesPage(_ context.Context, params admissionpostgres.ListPoliciesParams) (admissionpostgres.PolicyPage, error) {
	s.listed = params
	if s.err != nil {
		return admissionpostgres.PolicyPage{}, s.err
	}
	start := int(params.Offset)
	if start > len(s.policies) {
		start = len(s.policies)
	}
	end := start + params.Limit
	if end > len(s.policies) {
		end = len(s.policies)
	}
	return admissionpostgres.PolicyPage{
		Policies: append([]admissiondomain.HotTrainPolicy(nil), s.policies[start:end]...),
		Total:    int64(len(s.policies)),
	}, nil
}
func (s *fakeHotTrainPolicyStore) GetPolicyByID(context.Context, uuid.UUID) (admissiondomain.HotTrainPolicy, error) {
	if s.err != nil {
		return admissiondomain.HotTrainPolicy{}, s.err
	}
	return s.policies[0], nil
}
func (s *fakeHotTrainPolicyStore) CreatePolicy(_ context.Context, params admissionpostgres.CreatePolicyParams) (admissiondomain.HotTrainPolicy, error) {
	s.created = params
	if s.err != nil {
		return admissiondomain.HotTrainPolicy{}, s.err
	}
	return s.policies[0], nil
}
func (s *fakeHotTrainPolicyStore) UpdatePolicy(_ context.Context, _ uuid.UUID, params admissionpostgres.UpdatePolicyParams) (admissiondomain.HotTrainPolicy, error) {
	s.updated = params
	if s.err != nil {
		return admissiondomain.HotTrainPolicy{}, s.err
	}
	return s.policies[0], nil
}
func (s *fakeHotTrainPolicyStore) DisablePolicy(_ context.Context, _ uuid.UUID, _ int64, metadata admissionpostgres.MutationMetadata) (admissiondomain.HotTrainPolicy, error) {
	s.disabled = metadata
	if s.err != nil {
		return admissiondomain.HotTrainPolicy{}, s.err
	}
	return s.policies[0], nil
}

func TestHotTrainPolicyServiceValidatesAndMapsCreate(t *testing.T) {
	t.Parallel()
	policy := testHotTrainPolicy(t)
	store := &fakeHotTrainPolicyStore{policies: []admissiondomain.HotTrainPolicy{policy}}
	service := NewHotTrainPolicyService(store)
	result, err := service.CreateHotTrainPolicy(context.Background(), httpapi.CreateHotTrainPolicyCommand{
		ActorID:       uuid.NewString(),
		CorrelationID: "request-42",
		TrainRunID:    policy.TrainRunID.String(),
		SeatClass:     "STANDARD",
		HotTrainPolicyLimits: httpapi.HotTrainPolicyLimits{
			MaxQueueSize: 100, AdmissionRatePerSecond: 10, MaxInflightAdmissions: 20,
			AdmissionTokenTTLSeconds: 60, ProcessingLeaseSeconds: 10, QueueEntryTTLSeconds: 600,
		},
	})
	if err != nil {
		t.Fatalf("CreateHotTrainPolicy() error = %v", err)
	}
	if result.ID != policy.ID.String() || store.created.SeatClass != offeringdomain.SeatClassStandard ||
		store.created.Metadata.CorrelationID != "request-42" {
		t.Fatalf("create result/store params = (%+v, %+v)", result, store.created)
	}
}

func TestHotTrainPolicyServiceRejectsUnsafeLimitsAndFalseUpdate(t *testing.T) {
	t.Parallel()
	policy := testHotTrainPolicy(t)
	service := NewHotTrainPolicyService(&fakeHotTrainPolicyStore{policies: []admissiondomain.HotTrainPolicy{policy}})
	_, err := service.CreateHotTrainPolicy(context.Background(), httpapi.CreateHotTrainPolicyCommand{
		ActorID: uuid.NewString(), CorrelationID: "request", TrainRunID: policy.TrainRunID.String(),
		SeatClass: "standard",
	})
	if !errors.Is(err, httpapi.ErrInvalidInput) {
		t.Fatalf("unsafe create error = %v, want %v", err, httpapi.ErrInvalidInput)
	}
	disabled := false
	_, err = service.UpdateHotTrainPolicy(context.Background(), httpapi.UpdateHotTrainPolicyCommand{
		ActorID: uuid.NewString(), CorrelationID: "request", PolicyID: policy.ID.String(),
		ExpectedVersion: 1, Enabled: &disabled,
	})
	if !errors.Is(err, httpapi.ErrInvalidInput) {
		t.Fatalf("false update error = %v, want %v", err, httpapi.ErrInvalidInput)
	}
}

func TestHotTrainPolicyServicePaginatesAndMapsVersionConflict(t *testing.T) {
	t.Parallel()
	policy := testHotTrainPolicy(t)
	store := &fakeHotTrainPolicyStore{policies: []admissiondomain.HotTrainPolicy{policy, policy, policy}}
	service := NewHotTrainPolicyService(store)
	page, err := service.ListHotTrainPolicies(context.Background(), httpapi.PageRequest{Page: 2, Limit: 2})
	if err != nil {
		t.Fatalf("ListHotTrainPolicies() error = %v", err)
	}
	if len(page.Items) != 1 || page.Total != 3 {
		t.Fatalf("page = %+v, want one of three", page)
	}
	if store.listed != (admissionpostgres.ListPoliciesParams{
		Offset: 2, Limit: 2, Sort: admissionpostgres.PolicySortTrainRunID,
	}) {
		t.Fatalf("database page params = %+v", store.listed)
	}
	store.err = admissionpostgres.ErrVersionConflict
	_, err = service.DisableHotTrainPolicy(context.Background(), httpapi.DisableHotTrainPolicyCommand{
		ActorID: uuid.NewString(), CorrelationID: "request", PolicyID: policy.ID.String(), ExpectedVersion: 1,
	})
	if !errors.Is(err, httpapi.ErrConflict) {
		t.Fatalf("DisableHotTrainPolicy() error = %v, want %v", err, httpapi.ErrConflict)
	}
}

func TestHotTrainPolicyServiceMapsWhitelistedDescendingSortAndRejectsUnknownSort(t *testing.T) {
	t.Parallel()
	policy := testHotTrainPolicy(t)
	store := &fakeHotTrainPolicyStore{policies: []admissiondomain.HotTrainPolicy{policy}}
	service := NewHotTrainPolicyService(store)

	if _, err := service.ListHotTrainPolicies(context.Background(), httpapi.PageRequest{
		Page: 1, Limit: 25, Sort: "-updated_at",
	}); err != nil {
		t.Fatalf("ListHotTrainPolicies() error = %v", err)
	}
	if store.listed != (admissionpostgres.ListPoliciesParams{
		Limit: 25, Sort: admissionpostgres.PolicySortUpdatedAt, Descending: true,
	}) {
		t.Fatalf("database page params = %+v", store.listed)
	}
	if _, err := service.ListHotTrainPolicies(context.Background(), httpapi.PageRequest{
		Page: 1, Limit: 25, Sort: "version",
	}); !errors.Is(err, httpapi.ErrInvalidInput) {
		t.Fatalf("unknown sort error = %v, want %v", err, httpapi.ErrInvalidInput)
	}
}

func testHotTrainPolicy(t *testing.T) admissiondomain.HotTrainPolicy {
	t.Helper()
	limits, err := admissiondomain.NewPolicyLimits(admissiondomain.PolicyLimitsInput{
		MaxQueueSize: 100, AdmissionRatePerSecond: 10, MaxInflightAdmissions: 20,
		AdmissionTokenTTL: time.Minute, ProcessingLease: 10 * time.Second, QueueEntryTTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	policy, err := admissiondomain.NewHotTrainPolicy(
		uuid.New(), uuid.New(), offeringdomain.SeatClassStandard, true, 1, nil, limits, now, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
