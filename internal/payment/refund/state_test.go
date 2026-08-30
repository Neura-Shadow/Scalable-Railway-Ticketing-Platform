package refund_test

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/refund"
)

func TestProcessorStateModelsUncertaintyWithoutBlindRetry(t *testing.T) {
	t.Parallel()

	state := refund.NewProcessorState()
	var err error
	for _, next := range []refund.SagaState{refund.SagaValidating, refund.SagaRefundPending, refund.SagaProviderUncertain} {
		state, err = state.AdvanceSaga(next)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, next := range []refund.OperationState{refund.OperationClaimed, refund.OperationInFlight, refund.OperationUncertain} {
		state, err = state.AdvanceOperation(next)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := state.AdvanceOperation(refund.OperationPending); !errors.Is(err, refund.ErrInvalidProcessorTransition) {
		t.Fatalf("uncertain -> pending error = %v, want ErrInvalidProcessorTransition", err)
	}
	state, err = state.AdvanceOperation(refund.OperationSucceeded)
	if err != nil {
		t.Fatalf("uncertain -> succeeded error = %v", err)
	}
	for _, next := range []refund.SagaState{refund.SagaRefundSucceeded, refund.SagaShardCompensating, refund.SagaCompleted} {
		state, err = state.AdvanceSaga(next)
		if err != nil {
			t.Fatal(err)
		}
	}
	if state.Saga != refund.SagaCompleted || state.Operation != refund.OperationSucceeded {
		t.Fatalf("terminal processor state = %+v", state)
	}
}
