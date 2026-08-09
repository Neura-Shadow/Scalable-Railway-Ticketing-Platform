package domain_test

import (
	"errors"
	"testing"

	paymentdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/domain"
)

func TestUncertainProviderOperationCannotReturnToBlindRetry(t *testing.T) {
	t.Parallel()
	operation, err := paymentdomain.NewOperation(paymentdomain.OperationCapture)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []paymentdomain.OperationState{
		paymentdomain.OperationClaimed,
		paymentdomain.OperationInFlight,
		paymentdomain.OperationUncertain,
	} {
		if _, err := operation.Transition(state); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := operation.Transition(paymentdomain.OperationPending); !errors.Is(err, paymentdomain.ErrInvalidOperationTransition) {
		t.Fatalf("uncertain -> pending error = %v", err)
	}
	if _, err := operation.Transition(paymentdomain.OperationSucceeded); err != nil {
		t.Fatalf("reconciled uncertain -> succeeded error = %v", err)
	}
}
