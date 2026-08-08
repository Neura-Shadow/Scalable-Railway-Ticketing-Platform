// Package domain owns the provider-neutral payment state machines and money
// invariants. It contains no HTTP, PostgreSQL, or provider-specific types.
package domain

import (
	"errors"

	bookingdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/domain"
)

type IntentState string

const (
	IntentCreated              IntentState = "created"
	IntentReservationSecuring  IntentState = "reservation_securing"
	IntentCheckoutPending      IntentState = "checkout_pending"
	IntentAwaitingCustomer     IntentState = "awaiting_customer"
	IntentAuthorizationPending IntentState = "authorization_pending"
	IntentAuthorized           IntentState = "authorized"
	IntentCapturePending       IntentState = "capture_pending"
	IntentCaptured             IntentState = "captured"
	IntentTicketIssuePending   IntentState = "ticket_issue_pending"
	IntentCompleted            IntentState = "completed"
	IntentVoidPending          IntentState = "void_pending"
	IntentVoided               IntentState = "voided"
	IntentRefundPending        IntentState = "refund_pending"
	IntentRefunded             IntentState = "refunded"
	IntentCancelled            IntentState = "cancelled"
	IntentFailed               IntentState = "failed"
	IntentManualReview         IntentState = "manual_review"
	IntentExpired              IntentState = "expired"
)

var (
	ErrInvalidIntent           = errors.New("invalid payment intent")
	ErrInvalidIntentTransition = errors.New("invalid payment intent transition")
	ErrFinancialMismatch       = errors.New("payment financial amount or currency mismatch")
	ErrFinancialState          = errors.New("payment financial operation invalid for state")
)

type Intent struct {
	money           bookingdomain.Money
	state           IntentState
	capturedAmount  int64
	refundedAmount  int64
	captureRecorded bool
	refundRecorded  bool
}

func NewIntent(money bookingdomain.Money) (*Intent, error) {
	if money.Currency() == "" {
		return nil, ErrInvalidIntent
	}
	return &Intent{money: money, state: IntentCreated}, nil
}

func (intent *Intent) Transition(next IntentState) (bool, error) {
	if intent == nil {
		return false, ErrInvalidIntent
	}
	if next == intent.state {
		return false, nil
	}
	if !intentTransitions[intent.state][next] {
		return false, ErrInvalidIntentTransition
	}
	intent.state = next
	return true, nil
}

func (intent *Intent) State() IntentState { return intent.state }

func (intent *Intent) AmountMinor() int64 { return intent.money.AmountMinor() }

func (intent *Intent) Currency() string { return intent.money.Currency() }

func (intent *Intent) RecordCapture(money bookingdomain.Money) (bool, error) {
	if intent == nil {
		return false, ErrInvalidIntent
	}
	if intent.state != IntentCapturePending && intent.state != IntentCaptured {
		return false, ErrFinancialState
	}
	if money.AmountMinor() != intent.money.AmountMinor() || money.Currency() != intent.money.Currency() {
		return false, ErrFinancialMismatch
	}
	if intent.captureRecorded {
		return false, nil
	}
	intent.capturedAmount = money.AmountMinor()
	intent.captureRecorded = true
	return true, nil
}

func (intent *Intent) RecordRefund(money bookingdomain.Money) (bool, error) {
	if intent == nil {
		return false, ErrInvalidIntent
	}
	if intent.state != IntentRefundPending && intent.state != IntentRefunded {
		return false, ErrFinancialState
	}
	if !intent.captureRecorded || money.AmountMinor() != intent.capturedAmount || money.Currency() != intent.money.Currency() {
		return false, ErrFinancialMismatch
	}
	if intent.refundRecorded {
		return false, nil
	}
	intent.refundedAmount = money.AmountMinor()
	intent.refundRecorded = true
	return true, nil
}

func (intent *Intent) CapturedAmountMinor() int64 { return intent.capturedAmount }

func (intent *Intent) RefundedAmountMinor() int64 { return intent.refundedAmount }

func (intent *Intent) NetPaidAmountMinor() int64 {
	return intent.capturedAmount - intent.refundedAmount
}

var intentTransitions = map[IntentState]map[IntentState]bool{
	IntentCreated:              transitions(IntentReservationSecuring, IntentCancelled, IntentFailed),
	IntentReservationSecuring:  transitions(IntentCheckoutPending, IntentCancelled, IntentFailed, IntentManualReview),
	IntentCheckoutPending:      transitions(IntentAwaitingCustomer, IntentCancelled, IntentFailed, IntentManualReview),
	IntentAwaitingCustomer:     transitions(IntentAuthorizationPending, IntentVoidPending, IntentCancelled, IntentExpired, IntentManualReview),
	IntentAuthorizationPending: transitions(IntentAuthorized, IntentCancelled, IntentFailed, IntentManualReview),
	IntentAuthorized:           transitions(IntentCapturePending, IntentVoidPending, IntentManualReview),
	IntentCapturePending:       transitions(IntentCaptured, IntentFailed, IntentManualReview),
	IntentCaptured:             transitions(IntentTicketIssuePending, IntentRefundPending, IntentManualReview),
	IntentTicketIssuePending:   transitions(IntentCompleted, IntentRefundPending, IntentFailed, IntentManualReview),
	IntentCompleted:            transitions(IntentRefundPending, IntentManualReview),
	IntentVoidPending:          transitions(IntentVoided, IntentManualReview),
	IntentVoided:               transitions(IntentCancelled),
	IntentRefundPending:        transitions(IntentRefunded, IntentManualReview),
	IntentRefunded:             transitions(IntentCancelled),
	IntentManualReview:         transitions(IntentAuthorizationPending, IntentAuthorized, IntentCapturePending, IntentCaptured, IntentTicketIssuePending, IntentVoidPending, IntentRefundPending, IntentCancelled, IntentFailed),
}

func transitions(states ...IntentState) map[IntentState]bool {
	result := make(map[IntentState]bool, len(states))
	for _, state := range states {
		result[state] = true
	}
	return result
}
