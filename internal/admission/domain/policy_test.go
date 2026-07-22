package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/google/uuid"
)

func TestNewPolicyLimitsAcceptsBoundedConfiguration(t *testing.T) {
	t.Parallel()

	limits, err := domain.NewPolicyLimits(domain.PolicyLimitsInput{
		MaxQueueSize:           10_000,
		AdmissionRatePerSecond: 500,
		MaxInflightAdmissions:  2_000,
		AdmissionTokenTTL:      2 * time.Minute,
		ProcessingLease:        15 * time.Second,
		QueueEntryTTL:          30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewPolicyLimits() error = %v", err)
	}
	if limits.MaxInflightAdmissions != 2_000 {
		t.Fatalf("MaxInflightAdmissions = %d, want 2000", limits.MaxInflightAdmissions)
	}
}

func TestNewHotTrainPolicyAcceptsOnlyCoherentDurableState(t *testing.T) {
	t.Parallel()

	limits, err := domain.NewPolicyLimits(domain.PolicyLimitsInput{
		MaxQueueSize: 100, AdmissionRatePerSecond: 10, MaxInflightAdmissions: 20,
		AdmissionTokenTTL: time.Minute, ProcessingLease: 10 * time.Second, QueueEntryTTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewPolicyLimits() error = %v", err)
	}
	now := time.Now().UTC()
	initialized := int64(2)
	policy, err := domain.NewHotTrainPolicy(
		uuid.New(), uuid.New(), offeringdomain.SeatClassStandard, true, 2, &initialized, limits, now, now,
	)
	if err != nil {
		t.Fatalf("NewHotTrainPolicy() error = %v", err)
	}
	initialized = 9
	if policy.RedisInitializedVersion == nil || *policy.RedisInitializedVersion != 2 {
		t.Fatalf("RedisInitializedVersion = %v, want independent value 2", policy.RedisInitializedVersion)
	}
}

func TestNewHotTrainPolicyRejectsInvalidIdentityAndProjectionVersion(t *testing.T) {
	t.Parallel()
	limits, err := domain.NewPolicyLimits(domain.PolicyLimitsInput{
		MaxQueueSize: 100, AdmissionRatePerSecond: 10, MaxInflightAdmissions: 20,
		AdmissionTokenTTL: time.Minute, ProcessingLease: 10 * time.Second, QueueEntryTTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewPolicyLimits() error = %v", err)
	}
	now := time.Now().UTC()
	tooNew := int64(3)
	if _, err := domain.NewHotTrainPolicy(uuid.New(), uuid.New(), offeringdomain.SeatClassStandard, true, 2, &tooNew, limits, now, now); !errors.Is(err, domain.ErrInvalidHotTrainPolicy) {
		t.Fatalf("NewHotTrainPolicy() error = %v, want %v", err, domain.ErrInvalidHotTrainPolicy)
	}
	if _, err := domain.NewHotTrainPolicy(uuid.Nil, uuid.New(), offeringdomain.SeatClassStandard, true, 1, nil, limits, now, now); !errors.Is(err, domain.ErrInvalidHotTrainPolicy) {
		t.Fatalf("NewHotTrainPolicy() error = %v, want %v", err, domain.ErrInvalidHotTrainPolicy)
	}
}

func TestNewPolicyLimitsRejectsUnsafeConfiguration(t *testing.T) {
	t.Parallel()

	valid := domain.PolicyLimitsInput{
		MaxQueueSize:           10_000,
		AdmissionRatePerSecond: 500,
		MaxInflightAdmissions:  2_000,
		AdmissionTokenTTL:      2 * time.Minute,
		ProcessingLease:        15 * time.Second,
		QueueEntryTTL:          30 * time.Minute,
	}
	tests := map[string]func(*domain.PolicyLimitsInput){
		"zero queue":            func(input *domain.PolicyLimitsInput) { input.MaxQueueSize = 0 },
		"queue above maximum":   func(input *domain.PolicyLimitsInput) { input.MaxQueueSize = 100_001 },
		"zero rate":             func(input *domain.PolicyLimitsInput) { input.AdmissionRatePerSecond = 0 },
		"rate above maximum":    func(input *domain.PolicyLimitsInput) { input.AdmissionRatePerSecond = 10_001 },
		"zero inflight":         func(input *domain.PolicyLimitsInput) { input.MaxInflightAdmissions = 0 },
		"inflight above safe":   func(input *domain.PolicyLimitsInput) { input.MaxInflightAdmissions = 10_001 },
		"short token TTL":       func(input *domain.PolicyLimitsInput) { input.AdmissionTokenTTL = 4 * time.Second },
		"long token TTL":        func(input *domain.PolicyLimitsInput) { input.AdmissionTokenTTL = 901 * time.Second },
		"zero processing lease": func(input *domain.PolicyLimitsInput) { input.ProcessingLease = 0 },
		"lease below minimum":   func(input *domain.PolicyLimitsInput) { input.ProcessingLease = 4 * time.Second },
		"lease reaches TTL":     func(input *domain.PolicyLimitsInput) { input.ProcessingLease = input.AdmissionTokenTTL },
		"short queue TTL":       func(input *domain.PolicyLimitsInput) { input.QueueEntryTTL = 59 * time.Second },
		"long queue TTL":        func(input *domain.PolicyLimitsInput) { input.QueueEntryTTL = 86_401 * time.Second },
		"queue TTL shorter than token lifecycle": func(input *domain.PolicyLimitsInput) {
			input.AdmissionTokenTTL = 55 * time.Second
			input.ProcessingLease = 10 * time.Second
			input.QueueEntryTTL = time.Minute
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			input := valid
			mutate(&input)
			if _, err := domain.NewPolicyLimits(input); !errors.Is(err, domain.ErrInvalidPolicyLimits) {
				t.Fatalf("NewPolicyLimits() error = %v, want %v", err, domain.ErrInvalidPolicyLimits)
			}
		})
	}
}
