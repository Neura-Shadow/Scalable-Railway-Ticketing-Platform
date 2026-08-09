package domain

import "errors"

type SagaState string

const (
	SagaCreated            SagaState = "created"
	SagaReservationSecured SagaState = "reservation_secured"
	SagaCheckoutCreated    SagaState = "checkout_created"
	SagaAwaitingProvider   SagaState = "awaiting_provider"
	SagaAuthorized         SagaState = "authorized"
	SagaCapturing          SagaState = "capturing"
	SagaCaptured           SagaState = "captured"
	SagaIssuingTickets     SagaState = "issuing_tickets"
	SagaCompleted          SagaState = "completed"
	SagaCompensating       SagaState = "compensating"
	SagaRefunding          SagaState = "refunding"
	SagaCompensated        SagaState = "compensated"
	SagaFailed             SagaState = "failed"
	SagaManualReview       SagaState = "manual_review"
)

var ErrInvalidSagaTransition = errors.New("invalid payment saga transition")

type Saga struct{ state SagaState }

func NewSaga() *Saga { return &Saga{state: SagaCreated} }

func (saga *Saga) State() SagaState { return saga.state }

func (saga *Saga) Transition(next SagaState) (bool, error) {
	if saga == nil {
		return false, ErrInvalidSagaTransition
	}
	if next == saga.state {
		return false, nil
	}
	if !sagaTransitions[saga.state][next] {
		return false, ErrInvalidSagaTransition
	}
	saga.state = next
	return true, nil
}

var sagaTransitions = map[SagaState]map[SagaState]bool{
	SagaCreated:            sagaEdges(SagaReservationSecured, SagaFailed, SagaManualReview),
	SagaReservationSecured: sagaEdges(SagaCheckoutCreated, SagaFailed, SagaManualReview),
	SagaCheckoutCreated:    sagaEdges(SagaAwaitingProvider, SagaFailed, SagaManualReview),
	SagaAwaitingProvider:   sagaEdges(SagaAuthorized, SagaCompensating, SagaFailed, SagaManualReview),
	SagaAuthorized:         sagaEdges(SagaCapturing, SagaCompensating, SagaManualReview),
	SagaCapturing:          sagaEdges(SagaCaptured, SagaFailed, SagaManualReview),
	SagaCaptured:           sagaEdges(SagaIssuingTickets, SagaCompensating, SagaManualReview),
	SagaIssuingTickets:     sagaEdges(SagaCompleted, SagaCompensating, SagaFailed, SagaManualReview),
	SagaCompensating:       sagaEdges(SagaRefunding, SagaCompensated, SagaFailed, SagaManualReview),
	SagaRefunding:          sagaEdges(SagaCompensated, SagaFailed, SagaManualReview),
	SagaManualReview:       sagaEdges(SagaAwaitingProvider, SagaAuthorized, SagaCapturing, SagaCaptured, SagaIssuingTickets, SagaCompensating, SagaRefunding, SagaCompleted, SagaCompensated, SagaFailed),
}

func sagaEdges(states ...SagaState) map[SagaState]bool {
	result := make(map[SagaState]bool, len(states))
	for _, state := range states {
		result[state] = true
	}
	return result
}
