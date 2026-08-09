package domain

import "errors"

type OperationType string

const (
	OperationCreateCheckout OperationType = "create_checkout"
	OperationQueryStatus    OperationType = "query_status"
	OperationAuthorize      OperationType = "authorize"
	OperationCapture        OperationType = "capture"
	OperationVoid           OperationType = "void"
	OperationRefund         OperationType = "refund"
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

var (
	ErrInvalidOperation           = errors.New("invalid payment operation")
	ErrInvalidOperationTransition = errors.New("invalid payment operation transition")
)

type Operation struct {
	kind  OperationType
	state OperationState
}

func NewOperation(kind OperationType) (*Operation, error) {
	if !validOperationTypes[kind] {
		return nil, ErrInvalidOperation
	}
	return &Operation{kind: kind, state: OperationPending}, nil
}

func (operation *Operation) Type() OperationType { return operation.kind }

func (operation *Operation) State() OperationState { return operation.state }

func (operation *Operation) Transition(next OperationState) (bool, error) {
	if operation == nil {
		return false, ErrInvalidOperation
	}
	if next == operation.state {
		return false, nil
	}
	if !operationTransitions[operation.state][next] {
		return false, ErrInvalidOperationTransition
	}
	operation.state = next
	return true, nil
}

var validOperationTypes = map[OperationType]bool{
	OperationCreateCheckout: true,
	OperationQueryStatus:    true,
	OperationAuthorize:      true,
	OperationCapture:        true,
	OperationVoid:           true,
	OperationRefund:         true,
}

var operationTransitions = map[OperationState]map[OperationState]bool{
	OperationPending:         operationEdges(OperationClaimed, OperationCancelled),
	OperationClaimed:         operationEdges(OperationInFlight, OperationPending, OperationCancelled),
	OperationInFlight:        operationEdges(OperationSucceeded, OperationFailedRetryable, OperationFailedPermanent, OperationUncertain),
	OperationFailedRetryable: operationEdges(OperationPending, OperationCancelled),
	OperationUncertain:       operationEdges(OperationSucceeded, OperationFailedRetryable, OperationFailedPermanent),
}

func operationEdges(states ...OperationState) map[OperationState]bool {
	result := make(map[OperationState]bool, len(states))
	for _, state := range states {
		result[state] = true
	}
	return result
}
