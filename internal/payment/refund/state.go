package refund

import "errors"

var (
	ErrInvalidProcessorState      = errors.New("invalid ticket refund processor state")
	ErrInvalidProcessorTransition = errors.New("invalid ticket refund processor transition")
)

type SagaState string

const (
	SagaCreated           SagaState = "created"
	SagaValidating        SagaState = "validating"
	SagaRefundPending     SagaState = "refund_pending"
	SagaProviderUncertain SagaState = "provider_uncertain"
	SagaRefundSucceeded   SagaState = "refund_succeeded"
	SagaShardCompensating SagaState = "shard_compensating"
	SagaCompleted         SagaState = "completed"
	SagaManualReview      SagaState = "manual_review"
	SagaFailed            SagaState = "failed"
)

type OperationState string

const (
	OperationPending         OperationState = "pending"
	OperationClaimed         OperationState = "claimed"
	OperationInFlight        OperationState = "in_flight"
	OperationSucceeded       OperationState = "succeeded"
	OperationFailedRetryable OperationState = "failed_retryable"
	OperationFailedPermanent OperationState = "failed_permanent"
	OperationUncertain       OperationState = "uncertain"
	OperationCancelled       OperationState = "cancelled"
)

type ProcessorState struct {
	Saga      SagaState
	Operation OperationState
}

func NewProcessorState() ProcessorState {
	return ProcessorState{Saga: SagaCreated, Operation: OperationPending}
}

func RestoreProcessorState(saga SagaState, operation OperationState) (ProcessorState, error) {
	if !validSagaState(saga) || !validOperationState(operation) {
		return ProcessorState{}, ErrInvalidProcessorState
	}
	return ProcessorState{Saga: saga, Operation: operation}, nil
}

func (state ProcessorState) AdvanceSaga(next SagaState) (ProcessorState, error) {
	if !validSagaState(state.Saga) || !validOperationState(state.Operation) || !validSagaState(next) {
		return ProcessorState{}, ErrInvalidProcessorState
	}
	if next == state.Saga {
		return state, nil
	}
	if !sagaTransitions[state.Saga][next] {
		return ProcessorState{}, ErrInvalidProcessorTransition
	}
	state.Saga = next
	return state, nil
}

func (state ProcessorState) AdvanceOperation(next OperationState) (ProcessorState, error) {
	if !validSagaState(state.Saga) || !validOperationState(state.Operation) || !validOperationState(next) {
		return ProcessorState{}, ErrInvalidProcessorState
	}
	if next == state.Operation {
		return state, nil
	}
	if !operationTransitions[state.Operation][next] {
		return ProcessorState{}, ErrInvalidProcessorTransition
	}
	state.Operation = next
	return state, nil
}

var sagaTransitions = map[SagaState]map[SagaState]bool{
	SagaCreated:           sagaEdges(SagaValidating, SagaManualReview, SagaFailed),
	SagaValidating:        sagaEdges(SagaRefundPending, SagaManualReview, SagaFailed),
	SagaRefundPending:     sagaEdges(SagaProviderUncertain, SagaRefundSucceeded, SagaManualReview, SagaFailed),
	SagaProviderUncertain: sagaEdges(SagaRefundPending, SagaRefundSucceeded, SagaManualReview),
	SagaRefundSucceeded:   sagaEdges(SagaShardCompensating, SagaManualReview),
	SagaShardCompensating: sagaEdges(SagaCompleted, SagaManualReview),
}

var operationTransitions = map[OperationState]map[OperationState]bool{
	OperationPending:         operationEdges(OperationClaimed, OperationCancelled),
	OperationClaimed:         operationEdges(OperationInFlight, OperationPending, OperationCancelled),
	OperationInFlight:        operationEdges(OperationSucceeded, OperationFailedRetryable, OperationFailedPermanent, OperationUncertain),
	OperationFailedRetryable: operationEdges(OperationPending, OperationCancelled),
	OperationUncertain:       operationEdges(OperationSucceeded, OperationFailedRetryable, OperationFailedPermanent),
}

func validSagaState(state SagaState) bool {
	switch state {
	case SagaCreated, SagaValidating, SagaRefundPending, SagaProviderUncertain, SagaRefundSucceeded, SagaShardCompensating, SagaCompleted, SagaManualReview, SagaFailed:
		return true
	default:
		return false
	}
}

func validOperationState(state OperationState) bool {
	switch state {
	case OperationPending, OperationClaimed, OperationInFlight, OperationSucceeded, OperationFailedRetryable, OperationFailedPermanent, OperationUncertain, OperationCancelled:
		return true
	default:
		return false
	}
}

func sagaEdges(states ...SagaState) map[SagaState]bool {
	result := make(map[SagaState]bool, len(states))
	for _, state := range states {
		result[state] = true
	}
	return result
}

func operationEdges(states ...OperationState) map[OperationState]bool {
	result := make(map[OperationState]bool, len(states))
	for _, state := range states {
		result[state] = true
	}
	return result
}
