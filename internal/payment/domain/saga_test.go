package domain_test

import (
	"errors"
	"testing"

	paymentdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/domain"
)

func TestPaymentSagaSupportsCompletionAndCompensationJourneys(t *testing.T) {
	t.Parallel()
	completed := paymentdomain.NewSaga()
	for _, state := range []paymentdomain.SagaState{
		paymentdomain.SagaReservationSecured, paymentdomain.SagaCheckoutCreated,
		paymentdomain.SagaAwaitingProvider, paymentdomain.SagaAuthorized,
		paymentdomain.SagaCapturing, paymentdomain.SagaCaptured,
		paymentdomain.SagaIssuingTickets, paymentdomain.SagaCompleted,
	} {
		if changed, err := completed.Transition(state); err != nil || !changed {
			t.Fatalf("completion Transition(%q) changed=%t error=%v", state, changed, err)
		}
	}
	if _, err := completed.Transition(paymentdomain.SagaCapturing); !errors.Is(err, paymentdomain.ErrInvalidSagaTransition) {
		t.Fatalf("terminal saga regression error = %v", err)
	}

	compensated := paymentdomain.NewSaga()
	for _, state := range []paymentdomain.SagaState{
		paymentdomain.SagaReservationSecured, paymentdomain.SagaCheckoutCreated,
		paymentdomain.SagaAwaitingProvider, paymentdomain.SagaAuthorized,
		paymentdomain.SagaCapturing, paymentdomain.SagaCaptured,
		paymentdomain.SagaIssuingTickets, paymentdomain.SagaCompensating,
		paymentdomain.SagaRefunding, paymentdomain.SagaCompensated,
	} {
		if _, err := compensated.Transition(state); err != nil {
			t.Fatalf("compensation Transition(%q) error=%v", state, err)
		}
	}
}
