package sandbox

import (
	"errors"
	"sync"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
)

const maxFaultsPerOperation = 1024

type Operation string

const (
	OperationCreateCheckout Operation = "create_checkout"
	OperationGetStatus      Operation = "get_payment_status"
	OperationAuthorize      Operation = "authorize"
	OperationCapture        Operation = "capture"
	OperationVoid           Operation = "void"
	OperationRefund         Operation = "refund"
)

type FaultKind string

const (
	FaultNone                FaultKind = "none"
	FaultTimeoutBeforeCommit FaultKind = "timeout_before_commit"
	FaultTimeoutAfterCommit  FaultKind = "timeout_after_commit"
	FaultResponseLoss        FaultKind = "response_loss"
	FaultRateLimited         FaultKind = "rate_limited"
	FaultProviderError       FaultKind = "provider_error"
	FaultOutage              FaultKind = "outage"
	FaultInvalidResponse     FaultKind = "invalid_response"
	FaultOversizedResponse   FaultKind = "oversized_response"
	FaultRefundTransient     FaultKind = "refund_transient"
	FaultRefundPermanent     FaultKind = "refund_permanent"
	FaultDuplicateWebhook    FaultKind = "duplicate_webhook"
	FaultOutOfOrderWebhook   FaultKind = "out_of_order_webhook"
	FaultDelayedWebhook      FaultKind = "delayed_webhook"
)

type Fault struct {
	Kind       FaultKind
	DelaySteps uint64
}

// FaultError preserves a deterministic sandbox fault for the disposable HTTP
// emulator while unwrapping to the provider-neutral bounded error.
type FaultError struct {
	Kind FaultKind
	Err  *provider.Error
}

func (e *FaultError) Error() string {
	if e == nil || e.Err == nil {
		return "payment sandbox fault"
	}
	return e.Err.Error()
}

func (e *FaultError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func FaultKindOf(err error) (FaultKind, bool) {
	var faultError *FaultError
	if !errors.As(err, &faultError) {
		return "", false
	}
	return faultError.Kind, true
}

type Call struct {
	Operation Operation
	PaymentID string
}

type FaultPlan interface {
	Next(Call) Fault
}

// Script is a deterministic per-operation fault queue. Calls consume one
// entry, so tests advance faults explicitly without wall-clock sleeps.
type Script struct {
	mu     sync.Mutex
	faults map[Operation][]Fault
}

func NewScript() *Script { return &Script{faults: make(map[Operation][]Fault)} }

func (s *Script) Push(operation Operation, faults ...Fault) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(faults) == 0 || len(s.faults[operation])+len(faults) > maxFaultsPerOperation {
		return false
	}
	s.faults[operation] = append(s.faults[operation], faults...)
	return true
}

func (s *Script) Next(call Call) Fault {
	if s == nil {
		return Fault{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	queued := s.faults[call.Operation]
	if len(queued) == 0 {
		return Fault{}
	}
	fault := queued[0]
	s.faults[call.Operation] = queued[1:]
	return fault
}
