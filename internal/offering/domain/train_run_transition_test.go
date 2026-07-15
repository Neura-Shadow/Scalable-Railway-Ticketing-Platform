package domain_test

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
)

func TestTerminalTrainRunStatusesCannotReopen(t *testing.T) {
	for _, current := range []domain.TrainRunStatus{domain.TrainRunStatusCancelled, domain.TrainRunStatusCompleted} {
		if err := domain.ValidateTrainRunTransition(current, domain.TrainRunStatusScheduled); !errors.Is(err, domain.ErrInvalidTrainRunTransition) {
			t.Fatalf("%s -> scheduled error = %v", current, err)
		}
		if err := domain.ValidateTrainRunTransition(current, current); err != nil {
			t.Fatalf("idempotent %s transition: %v", current, err)
		}
	}
}

func TestTrainRunLifecycleAllowsOnlyForwardOperationalTransitions(t *testing.T) {
	for _, transition := range [][2]domain.TrainRunStatus{
		{domain.TrainRunStatusScheduled, domain.TrainRunStatusBoarding},
		{domain.TrainRunStatusScheduled, domain.TrainRunStatusCancelled},
		{domain.TrainRunStatusBoarding, domain.TrainRunStatusDeparted},
		{domain.TrainRunStatusBoarding, domain.TrainRunStatusCancelled},
		{domain.TrainRunStatusDeparted, domain.TrainRunStatusCompleted},
	} {
		if err := domain.ValidateTrainRunTransition(transition[0], transition[1]); err != nil {
			t.Fatalf("%s -> %s: %v", transition[0], transition[1], err)
		}
	}
}
