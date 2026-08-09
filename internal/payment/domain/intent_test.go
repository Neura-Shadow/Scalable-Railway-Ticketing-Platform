package domain_test

import (
	"errors"
	"testing"

	bookingdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/domain"
	paymentdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/domain"
)

func TestPaymentIntentAdvancesThroughExplicitPaymentJourney(t *testing.T) {
	t.Parallel()
	money, err := bookingdomain.NewMoney(12500, "TWD")
	if err != nil {
		t.Fatal(err)
	}
	intent, err := paymentdomain.NewIntent(money)
	if err != nil {
		t.Fatal(err)
	}
	states := []paymentdomain.IntentState{
		paymentdomain.IntentReservationSecuring,
		paymentdomain.IntentCheckoutPending,
		paymentdomain.IntentAwaitingCustomer,
		paymentdomain.IntentAuthorizationPending,
		paymentdomain.IntentAuthorized,
		paymentdomain.IntentCapturePending,
		paymentdomain.IntentCaptured,
		paymentdomain.IntentTicketIssuePending,
		paymentdomain.IntentCompleted,
	}
	for _, state := range states {
		changed, transitionErr := intent.Transition(state)
		if transitionErr != nil || !changed {
			t.Fatalf("Transition(%q) changed=%t error=%v", state, changed, transitionErr)
		}
	}
	if intent.State() != paymentdomain.IntentCompleted || intent.AmountMinor() != 12500 || intent.Currency() != "TWD" {
		t.Fatalf("intent = state %q amount %d currency %q", intent.State(), intent.AmountMinor(), intent.Currency())
	}
}

func TestPaymentIntentRejectsRegressionAndStabilizesReplay(t *testing.T) {
	t.Parallel()
	money, _ := bookingdomain.NewMoney(12500, "TWD")
	intent, _ := paymentdomain.NewIntent(money)
	if _, err := intent.Transition(paymentdomain.IntentCaptured); !errors.Is(err, paymentdomain.ErrInvalidIntentTransition) {
		t.Fatalf("created -> captured error = %v", err)
	}
	if changed, err := intent.Transition(paymentdomain.IntentCreated); err != nil || changed {
		t.Fatalf("same-state replay changed=%t error=%v", changed, err)
	}
	for _, state := range []paymentdomain.IntentState{
		paymentdomain.IntentReservationSecuring, paymentdomain.IntentCheckoutPending,
		paymentdomain.IntentAwaitingCustomer, paymentdomain.IntentAuthorizationPending,
		paymentdomain.IntentAuthorized, paymentdomain.IntentCapturePending,
		paymentdomain.IntentCaptured, paymentdomain.IntentTicketIssuePending,
		paymentdomain.IntentCompleted,
	} {
		if _, err := intent.Transition(state); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := intent.Transition(paymentdomain.IntentAwaitingCustomer); !errors.Is(err, paymentdomain.ErrInvalidIntentTransition) {
		t.Fatalf("completed regression error = %v", err)
	}
}

func TestPaymentIntentEnforcesExactCaptureAndFullRefund(t *testing.T) {
	t.Parallel()
	money, _ := bookingdomain.NewMoney(12500, "TWD")
	intent, _ := paymentdomain.NewIntent(money)
	for _, state := range []paymentdomain.IntentState{
		paymentdomain.IntentReservationSecuring, paymentdomain.IntentCheckoutPending,
		paymentdomain.IntentAwaitingCustomer, paymentdomain.IntentAuthorizationPending,
		paymentdomain.IntentAuthorized, paymentdomain.IntentCapturePending,
	} {
		if _, err := intent.Transition(state); err != nil {
			t.Fatal(err)
		}
	}
	changed, err := intent.RecordCapture(money)
	if err != nil || !changed {
		t.Fatalf("RecordCapture() changed=%t error=%v", changed, err)
	}
	if changed, err := intent.RecordCapture(money); err != nil || changed {
		t.Fatalf("capture replay changed=%t error=%v", changed, err)
	}
	if _, err := intent.Transition(paymentdomain.IntentCaptured); err != nil {
		t.Fatal(err)
	}
	if _, err := intent.Transition(paymentdomain.IntentRefundPending); err != nil {
		t.Fatal(err)
	}
	wrongAmount, _ := bookingdomain.NewMoney(12499, "TWD")
	if _, err := intent.RecordRefund(wrongAmount); !errors.Is(err, paymentdomain.ErrFinancialMismatch) {
		t.Fatalf("partial refund error = %v", err)
	}
	changed, err = intent.RecordRefund(money)
	if err != nil || !changed {
		t.Fatalf("RecordRefund() changed=%t error=%v", changed, err)
	}
	if intent.CapturedAmountMinor() != 12500 || intent.RefundedAmountMinor() != 12500 || intent.NetPaidAmountMinor() != 0 {
		t.Fatalf("financial totals = captured %d refunded %d net %d", intent.CapturedAmountMinor(), intent.RefundedAmountMinor(), intent.NetPaidAmountMinor())
	}
}
