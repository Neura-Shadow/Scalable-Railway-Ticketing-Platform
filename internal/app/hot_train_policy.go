package app

import (
	"context"
	"errors"
	"strings"
	"time"

	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/postgres"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

type hotTrainPolicyStore interface {
	ListPoliciesPage(context.Context, admissionpostgres.ListPoliciesParams) (admissionpostgres.PolicyPage, error)
	GetPolicyByID(context.Context, uuid.UUID) (admissiondomain.HotTrainPolicy, error)
	CreatePolicy(context.Context, admissionpostgres.CreatePolicyParams) (admissiondomain.HotTrainPolicy, error)
	UpdatePolicy(context.Context, uuid.UUID, admissionpostgres.UpdatePolicyParams) (admissiondomain.HotTrainPolicy, error)
	DisablePolicy(context.Context, uuid.UUID, int64, admissionpostgres.MutationMetadata) (admissiondomain.HotTrainPolicy, error)
}

type HotTrainPolicyService struct {
	store hotTrainPolicyStore
}

func NewHotTrainPolicyService(store hotTrainPolicyStore) *HotTrainPolicyService {
	return &HotTrainPolicyService{store: store}
}

func (s *HotTrainPolicyService) ListHotTrainPolicies(ctx context.Context, page httpapi.PageRequest) (httpapi.HotTrainPolicyPage, error) {
	if s == nil || s.store == nil || page.Page < 1 || page.Limit < 1 || page.Limit > 100 {
		return httpapi.HotTrainPolicyPage{}, httpapi.ErrInvalidInput
	}
	params, ok := hotTrainPolicyPageParams(page)
	if !ok {
		return httpapi.HotTrainPolicyPage{}, httpapi.ErrInvalidInput
	}
	result, err := s.store.ListPoliciesPage(ctx, params)
	if err != nil {
		return httpapi.HotTrainPolicyPage{}, mapAdmissionPolicyError(err)
	}
	items := make([]httpapi.HotTrainPolicyView, 0, len(result.Policies))
	for _, policy := range result.Policies {
		items = append(items, hotTrainPolicyView(policy))
	}
	return httpapi.HotTrainPolicyPage{
		Items: items,
		Page:  page.Page,
		Limit: page.Limit,
		Total: result.Total,
	}, nil
}

func hotTrainPolicyPageParams(page httpapi.PageRequest) (admissionpostgres.ListPoliciesParams, bool) {
	rawSort := strings.TrimSpace(page.Sort)
	if rawSort == "" {
		rawSort = string(admissionpostgres.PolicySortTrainRunID)
	}
	descending := strings.HasPrefix(rawSort, "-")
	baseSort := strings.TrimPrefix(rawSort, "-")
	var sort admissionpostgres.PolicySortField
	switch baseSort {
	case string(admissionpostgres.PolicySortTrainRunID):
		sort = admissionpostgres.PolicySortTrainRunID
	case string(admissionpostgres.PolicySortSeatClass):
		sort = admissionpostgres.PolicySortSeatClass
	case string(admissionpostgres.PolicySortUpdatedAt):
		sort = admissionpostgres.PolicySortUpdatedAt
	default:
		return admissionpostgres.ListPoliciesParams{}, false
	}
	pageIndex := int64(page.Page - 1)
	limit := int64(page.Limit)
	const maxInt64 = int64(^uint64(0) >> 1)
	if pageIndex > maxInt64/limit {
		return admissionpostgres.ListPoliciesParams{}, false
	}
	return admissionpostgres.ListPoliciesParams{
		Offset:     pageIndex * limit,
		Limit:      page.Limit,
		Sort:       sort,
		Descending: descending,
	}, true
}

func (s *HotTrainPolicyService) GetHotTrainPolicy(ctx context.Context, actorID, policyID string) (httpapi.HotTrainPolicyView, error) {
	if s == nil || s.store == nil {
		return httpapi.HotTrainPolicyView{}, httpapi.ErrUnavailable
	}
	if _, err := uuid.Parse(actorID); err != nil {
		return httpapi.HotTrainPolicyView{}, httpapi.ErrInvalidInput
	}
	id, err := uuid.Parse(policyID)
	if err != nil {
		return httpapi.HotTrainPolicyView{}, httpapi.ErrInvalidInput
	}
	policy, err := s.store.GetPolicyByID(ctx, id)
	if err != nil {
		return httpapi.HotTrainPolicyView{}, mapAdmissionPolicyError(err)
	}
	return hotTrainPolicyView(policy), nil
}

func (s *HotTrainPolicyService) CreateHotTrainPolicy(ctx context.Context, command httpapi.CreateHotTrainPolicyCommand) (httpapi.HotTrainPolicyView, error) {
	if s == nil || s.store == nil {
		return httpapi.HotTrainPolicyView{}, httpapi.ErrUnavailable
	}
	actorID, metadata, err := policyMutationMetadata(command.ActorID, command.CorrelationID)
	if err != nil {
		return httpapi.HotTrainPolicyView{}, err
	}
	trainRunID, err := uuid.Parse(command.TrainRunID)
	if err != nil {
		return httpapi.HotTrainPolicyView{}, httpapi.ErrInvalidInput
	}
	seatClass, err := offeringdomain.ParseSeatClass(command.SeatClass)
	if err != nil {
		return httpapi.HotTrainPolicyView{}, httpapi.ErrInvalidInput
	}
	limits, err := policyLimits(command.HotTrainPolicyLimits)
	if err != nil {
		return httpapi.HotTrainPolicyView{}, err
	}
	policy, err := s.store.CreatePolicy(ctx, admissionpostgres.CreatePolicyParams{
		TrainRunID: trainRunID,
		SeatClass:  seatClass,
		Limits:     limits,
		Metadata:   metadata,
	})
	_ = actorID
	if err != nil {
		return httpapi.HotTrainPolicyView{}, mapAdmissionPolicyError(err)
	}
	return hotTrainPolicyView(policy), nil
}

func (s *HotTrainPolicyService) UpdateHotTrainPolicy(ctx context.Context, command httpapi.UpdateHotTrainPolicyCommand) (httpapi.HotTrainPolicyView, error) {
	if s == nil || s.store == nil {
		return httpapi.HotTrainPolicyView{}, httpapi.ErrUnavailable
	}
	_, metadata, err := policyMutationMetadata(command.ActorID, command.CorrelationID)
	if err != nil {
		return httpapi.HotTrainPolicyView{}, err
	}
	policyID, err := uuid.Parse(command.PolicyID)
	if err != nil || command.ExpectedVersion < 1 || command.Enabled != nil && !*command.Enabled {
		return httpapi.HotTrainPolicyView{}, httpapi.ErrInvalidInput
	}
	limits, err := policyLimits(command.HotTrainPolicyLimits)
	if err != nil {
		return httpapi.HotTrainPolicyView{}, err
	}
	policy, err := s.store.UpdatePolicy(ctx, policyID, admissionpostgres.UpdatePolicyParams{
		ExpectedVersion: command.ExpectedVersion,
		Limits:          limits,
		Enabled:         command.Enabled,
		Metadata:        metadata,
	})
	if err != nil {
		return httpapi.HotTrainPolicyView{}, mapAdmissionPolicyError(err)
	}
	return hotTrainPolicyView(policy), nil
}

func (s *HotTrainPolicyService) DisableHotTrainPolicy(ctx context.Context, command httpapi.DisableHotTrainPolicyCommand) (httpapi.HotTrainPolicyView, error) {
	if s == nil || s.store == nil {
		return httpapi.HotTrainPolicyView{}, httpapi.ErrUnavailable
	}
	_, metadata, err := policyMutationMetadata(command.ActorID, command.CorrelationID)
	if err != nil {
		return httpapi.HotTrainPolicyView{}, err
	}
	policyID, err := uuid.Parse(command.PolicyID)
	if err != nil || command.ExpectedVersion < 1 {
		return httpapi.HotTrainPolicyView{}, httpapi.ErrInvalidInput
	}
	policy, err := s.store.DisablePolicy(ctx, policyID, command.ExpectedVersion, metadata)
	if err != nil {
		return httpapi.HotTrainPolicyView{}, mapAdmissionPolicyError(err)
	}
	return hotTrainPolicyView(policy), nil
}

func policyMutationMetadata(rawActorID, correlationID string) (uuid.UUID, admissionpostgres.MutationMetadata, error) {
	actorID, err := uuid.Parse(rawActorID)
	if err != nil {
		return uuid.Nil, admissionpostgres.MutationMetadata{}, httpapi.ErrInvalidInput
	}
	metadata := admissionpostgres.MutationMetadata{ActorID: actorID, CorrelationID: correlationID}
	return actorID, metadata, nil
}

func policyLimits(input httpapi.HotTrainPolicyLimits) (admissiondomain.PolicyLimits, error) {
	limits, err := admissiondomain.NewPolicyLimits(admissiondomain.PolicyLimitsInput{
		MaxQueueSize:           input.MaxQueueSize,
		AdmissionRatePerSecond: input.AdmissionRatePerSecond,
		MaxInflightAdmissions:  input.MaxInflightAdmissions,
		AdmissionTokenTTL:      time.Duration(input.AdmissionTokenTTLSeconds) * time.Second,
		ProcessingLease:        time.Duration(input.ProcessingLeaseSeconds) * time.Second,
		QueueEntryTTL:          time.Duration(input.QueueEntryTTLSeconds) * time.Second,
	})
	if err != nil {
		return admissiondomain.PolicyLimits{}, httpapi.ErrInvalidInput
	}
	return limits, nil
}

func hotTrainPolicyView(policy admissiondomain.HotTrainPolicy) httpapi.HotTrainPolicyView {
	return httpapi.HotTrainPolicyView{
		ID:                      policy.ID.String(),
		TrainRunID:              policy.TrainRunID.String(),
		SeatClass:               policy.SeatClass.String(),
		Enabled:                 policy.Enabled,
		Version:                 policy.Version,
		RedisInitializedVersion: cloneInt64(policy.RedisInitializedVersion),
		HotTrainPolicyLimits: httpapi.HotTrainPolicyLimits{
			MaxQueueSize:             policy.Limits.MaxQueueSize,
			AdmissionRatePerSecond:   policy.Limits.AdmissionRatePerSecond,
			MaxInflightAdmissions:    policy.Limits.MaxInflightAdmissions,
			AdmissionTokenTTLSeconds: int(policy.Limits.AdmissionTokenTTL / time.Second),
			ProcessingLeaseSeconds:   int(policy.Limits.ProcessingLease / time.Second),
			QueueEntryTTLSeconds:     int(policy.Limits.QueueEntryTTL / time.Second),
		},
		CreatedAt: policy.CreatedAt,
		UpdatedAt: policy.UpdatedAt,
	}
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func mapAdmissionPolicyError(err error) error {
	switch {
	case errors.Is(err, admissionpostgres.ErrInvalidInput):
		return httpapi.ErrInvalidInput
	case errors.Is(err, admissionpostgres.ErrNotFound):
		return httpapi.ErrNotFound
	case errors.Is(err, admissionpostgres.ErrConflict), errors.Is(err, admissionpostgres.ErrVersionConflict):
		return httpapi.ErrConflict
	default:
		return httpapi.ErrUnavailable
	}
}

var _ httpapi.HotTrainPolicyService = (*HotTrainPolicyService)(nil)
