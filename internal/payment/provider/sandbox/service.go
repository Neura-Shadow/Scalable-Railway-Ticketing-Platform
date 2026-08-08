// Package sandbox provides a deterministic, in-memory payment provider for
// tests and disposable development environments. It accepts opaque synthetic
// tokens only and is rejected in production by default.
package sandbox

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
)

const (
	defaultWebhookMaxBody  = 64 << 10
	defaultReplayTolerance = 5 * time.Minute
	defaultMaxWebhooks     = 10000
)

type Config struct {
	Environment                                   string
	AllowSandboxInProductionForDisposableTestOnly bool
	Now                                           func() time.Time
	WebhookKeys                                   map[string][]byte
	IssueKeyID                                    string
	WebhookMaxBodyBytes                           int
	WebhookReplayTolerance                        time.Duration
	MaxQueuedWebhooks                             int
	Faults                                        FaultPlan
}

type paymentRecord struct {
	id        string
	token     string
	status    provider.Status
	amount    int64
	currency  string
	captured  int64
	refunded  int64
	updatedAt time.Time
}

type idempotentResult struct {
	fingerprint string
	checkout    provider.Checkout
	operation   provider.OperationResult
}

type QueuedWebhook struct {
	Headers      provider.WebhookHeaders
	Body         []byte
	Sequence     uint64
	DeliverAfter uint64
}

type Service struct {
	mu              sync.Mutex
	now             func() time.Time
	faults          FaultPlan
	payments        map[string]*paymentRecord
	idempotency     map[string]idempotentResult
	keys            map[string][]byte
	issueKeyID      string
	maxWebhookBody  int
	maxWebhooks     int
	replayTolerance time.Duration
	nextPayment     uint64
	nextOperation   uint64
	nextEvent       uint64
	step            uint64
	webhooks        []QueuedWebhook
}

func New(config Config) (*Service, error) {
	environment := strings.ToLower(strings.TrimSpace(config.Environment))
	if environment == "production" && !config.AllowSandboxInProductionForDisposableTestOnly {
		return nil, errors.New("payment sandbox is disabled in production")
	}
	if environment != "test" && environment != "development" && environment != "production" {
		return nil, errors.New("payment sandbox environment is invalid")
	}
	if config.Now == nil {
		config.Now = func() time.Time { return time.Now().UTC() }
	}
	if config.WebhookMaxBodyBytes == 0 {
		config.WebhookMaxBodyBytes = defaultWebhookMaxBody
	}
	if config.WebhookMaxBodyBytes < 1024 || config.WebhookMaxBodyBytes > 1<<20 {
		return nil, errors.New("payment sandbox webhook body limit is invalid")
	}
	if config.WebhookReplayTolerance == 0 {
		config.WebhookReplayTolerance = defaultReplayTolerance
	}
	if config.WebhookReplayTolerance < time.Second || config.WebhookReplayTolerance > time.Hour {
		return nil, errors.New("payment sandbox replay tolerance is invalid")
	}
	if config.MaxQueuedWebhooks == 0 {
		config.MaxQueuedWebhooks = defaultMaxWebhooks
	}
	if config.MaxQueuedWebhooks < 1 || config.MaxQueuedWebhooks > 100000 {
		return nil, errors.New("payment sandbox webhook queue limit is invalid")
	}
	if len(config.WebhookKeys) > 8 {
		return nil, errors.New("payment sandbox webhook keyring is invalid")
	}
	keys := make(map[string][]byte, len(config.WebhookKeys))
	for id, key := range config.WebhookKeys {
		if !validKeyID(id) || len(key) < 16 || len(key) > 128 {
			return nil, errors.New("payment sandbox webhook keyring is invalid")
		}
		keys[id] = append([]byte(nil), key...)
	}
	if _, ok := keys[config.IssueKeyID]; !ok || config.IssueKeyID == "" {
		return nil, errors.New("payment sandbox issue key is invalid")
	}
	return &Service{
		now:             config.Now,
		faults:          config.Faults,
		payments:        make(map[string]*paymentRecord),
		idempotency:     make(map[string]idempotentResult),
		keys:            keys,
		issueKeyID:      config.IssueKeyID,
		maxWebhookBody:  config.WebhookMaxBodyBytes,
		maxWebhooks:     config.MaxQueuedWebhooks,
		replayTolerance: config.WebhookReplayTolerance,
	}, nil
}

func (s *Service) CreateCheckout(ctx context.Context, request provider.CreateCheckoutRequest) (provider.Checkout, error) {
	if err := ctx.Err(); err != nil {
		return provider.Checkout{}, transportError(OperationCreateCheckout, false)
	}
	if err := validateCommon(request.PaymentIntentID, request.MerchantReference, request.AmountMinor, request.Currency, request.IdempotencyKey, request.Metadata); err != nil {
		return provider.Checkout{}, err
	}
	fingerprint := fingerprint(request)
	s.mu.Lock()
	defer s.mu.Unlock()
	if replay, ok := s.idempotency[request.IdempotencyKey]; ok {
		if replay.fingerprint != fingerprint {
			return provider.Checkout{}, conflictError(OperationCreateCheckout)
		}
		return replay.checkout, nil
	}
	fault := s.nextFault(OperationCreateCheckout, "")
	if err := beforeFault(OperationCreateCheckout, fault); err != nil {
		return provider.Checkout{}, err
	}
	s.nextPayment++
	id := fmt.Sprintf("pay_sandbox_%012d", s.nextPayment)
	token := fmt.Sprintf("tok_sandbox_%012d", s.nextPayment)
	checkout := provider.Checkout{ProviderPaymentID: id, HostedReference: "sandbox-checkout:" + id, SyntheticToken: token, Status: provider.StatusCreated, AmountMinor: request.AmountMinor, Currency: normalizeCurrency(request.Currency)}
	s.payments[id] = &paymentRecord{id: id, token: token, status: provider.StatusCreated, amount: request.AmountMinor, currency: checkout.Currency, updatedAt: s.now().UTC()}
	s.idempotency[request.IdempotencyKey] = idempotentResult{fingerprint: fingerprint, checkout: checkout}
	s.enqueueWebhook(s.payments[id], provider.EventCheckoutCreated, fault)
	if err := afterFault(OperationCreateCheckout, fault); err != nil {
		return provider.Checkout{}, err
	}
	return checkout, nil
}

func (s *Service) GetPaymentStatus(ctx context.Context, paymentID string) (provider.Payment, error) {
	if err := ctx.Err(); err != nil {
		return provider.Payment{}, transportError(OperationGetStatus, false)
	}
	if !validIdentifier(paymentID) {
		return provider.Payment{}, validationError(OperationGetStatus, "provider payment ID is invalid")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	fault := s.nextFault(OperationGetStatus, paymentID)
	if err := beforeFault(OperationGetStatus, fault); err != nil {
		return provider.Payment{}, err
	}
	record, ok := s.payments[paymentID]
	if !ok {
		return provider.Payment{}, validationError(OperationGetStatus, "provider payment was not found")
	}
	return paymentFrom(record), nil
}

func (s *Service) Authorize(ctx context.Context, request provider.AuthorizeRequest) (provider.OperationResult, error) {
	if request.SyntheticToken == "" || len(request.SyntheticToken) > 128 || !strings.HasPrefix(request.SyntheticToken, "tok_sandbox_") {
		return provider.OperationResult{}, validationError(OperationAuthorize, "synthetic payment token is invalid")
	}
	return s.transition(ctx, OperationAuthorize, request.PaymentIntentID, request.ProviderPaymentID, request.AmountMinor, request.Currency, request.IdempotencyKey, request.Metadata, request.SyntheticToken)
}

func (s *Service) Capture(ctx context.Context, request provider.CaptureRequest) (provider.OperationResult, error) {
	return s.transition(ctx, OperationCapture, request.PaymentIntentID, request.ProviderPaymentID, request.AmountMinor, request.Currency, request.IdempotencyKey, request.Metadata, "")
}

func (s *Service) Void(ctx context.Context, request provider.VoidRequest) (provider.OperationResult, error) {
	return s.transition(ctx, OperationVoid, request.PaymentIntentID, request.ProviderPaymentID, 0, "", request.IdempotencyKey, request.Metadata, "")
}

func (s *Service) Refund(ctx context.Context, request provider.RefundRequest) (provider.OperationResult, error) {
	return s.transition(ctx, OperationRefund, request.PaymentIntentID, request.ProviderPaymentID, request.AmountMinor, request.Currency, request.IdempotencyKey, request.Metadata, "")
}

func (s *Service) transition(ctx context.Context, operation Operation, intentID, paymentID string, amount int64, currency, idempotencyKey string, metadata provider.Metadata, token string) (provider.OperationResult, error) {
	if err := ctx.Err(); err != nil {
		return provider.OperationResult{}, transportError(operation, false)
	}
	if !validIdentifier(intentID) || !validIdentifier(paymentID) || !validIdentifier(idempotencyKey) {
		return provider.OperationResult{}, validationError(operation, "provider operation identity is invalid")
	}
	if err := provider.ValidateMetadata(metadata); err != nil {
		return provider.OperationResult{}, withOperation(err, operation)
	}
	if operation != OperationVoid && (amount <= 0 || normalizeCurrency(currency) == "") {
		return provider.OperationResult{}, validationError(operation, "amount or currency is invalid")
	}
	fingerprint := fingerprint(struct {
		Operation Operation
		IntentID  string
		Payment   string
		Amount    int64
		Currency  string
		Token     string
		Metadata  provider.Metadata
	}{operation, intentID, paymentID, amount, normalizeCurrency(currency), token, metadata})

	s.mu.Lock()
	defer s.mu.Unlock()
	if replay, ok := s.idempotency[idempotencyKey]; ok {
		if replay.fingerprint != fingerprint {
			return provider.OperationResult{}, conflictError(operation)
		}
		return replay.operation, nil
	}
	fault := s.nextFault(operation, paymentID)
	if err := beforeFault(operation, fault); err != nil {
		return provider.OperationResult{}, err
	}
	record, ok := s.payments[paymentID]
	if !ok {
		return provider.OperationResult{}, validationError(operation, "provider payment was not found")
	}
	if operation == OperationAuthorize && record.token != token {
		return provider.OperationResult{}, validationError(operation, "synthetic payment token is invalid")
	}
	if operation != OperationVoid && (record.amount != amount || record.currency != normalizeCurrency(currency)) {
		return provider.OperationResult{}, conflictError(operation)
	}
	nextStatus, eventType, err := validateTransition(record, operation)
	if err != nil {
		return provider.OperationResult{}, err
	}
	s.nextOperation++
	resultAmount := amount
	if operation == OperationVoid {
		resultAmount = record.amount
	}
	result := provider.OperationResult{ProviderPaymentID: record.id, ProviderOperationID: fmt.Sprintf("op_sandbox_%012d", s.nextOperation), Status: nextStatus, AmountMinor: resultAmount, Currency: record.currency}
	record.status = nextStatus
	record.updatedAt = s.now().UTC()
	if operation == OperationCapture {
		record.captured = amount
	}
	if operation == OperationRefund {
		record.refunded = amount
	}
	s.idempotency[idempotencyKey] = idempotentResult{fingerprint: fingerprint, operation: result}
	s.enqueueWebhook(record, eventType, fault)
	if err := afterFault(operation, fault); err != nil {
		return provider.OperationResult{}, err
	}
	return result, nil
}

func validateTransition(record *paymentRecord, operation Operation) (provider.Status, provider.EventType, error) {
	switch operation {
	case OperationAuthorize:
		if record.status != provider.StatusCreated && record.status != provider.StatusRequiresCustomerAction {
			return "", "", conflictError(operation)
		}
		return provider.StatusAuthorized, provider.EventAuthorized, nil
	case OperationCapture:
		if record.status != provider.StatusAuthorized {
			return "", "", conflictError(operation)
		}
		return provider.StatusCaptured, provider.EventCaptured, nil
	case OperationVoid:
		if record.status != provider.StatusCreated && record.status != provider.StatusAuthorized {
			return "", "", conflictError(operation)
		}
		return provider.StatusVoided, provider.EventVoided, nil
	case OperationRefund:
		if record.status != provider.StatusCaptured || record.captured <= 0 || record.refunded != 0 {
			return "", "", conflictError(operation)
		}
		return provider.StatusRefunded, provider.EventRefunded, nil
	default:
		return "", "", validationError(operation, "provider operation is invalid")
	}
}

func (s *Service) nextFault(operation Operation, paymentID string) Fault {
	if s.faults == nil {
		return Fault{}
	}
	return s.faults.Next(Call{Operation: operation, PaymentID: paymentID})
}

func beforeFault(operation Operation, fault Fault) error {
	switch fault.Kind {
	case "", FaultNone, FaultTimeoutAfterCommit, FaultResponseLoss, FaultDuplicateWebhook, FaultOutOfOrderWebhook, FaultDelayedWebhook:
		return nil
	case FaultTimeoutBeforeCommit:
		return faultProviderError(fault.Kind, provider.ErrorTransport, operation, true, false, "payment provider timed out before commit")
	case FaultRateLimited:
		return faultProviderError(fault.Kind, provider.ErrorRateLimited, operation, true, false, "payment provider rate limited request")
	case FaultProviderError, FaultRefundTransient:
		return faultProviderError(fault.Kind, provider.ErrorTransport, operation, true, false, "payment provider transient failure")
	case FaultOutage:
		return faultProviderError(fault.Kind, provider.ErrorUnavailable, operation, true, false, "payment provider unavailable")
	case FaultInvalidResponse, FaultOversizedResponse:
		return faultProviderError(fault.Kind, provider.ErrorInconsistentResponse, operation, false, false, "payment provider response was invalid")
	case FaultRefundPermanent:
		return faultProviderError(fault.Kind, provider.ErrorPermanentValidation, operation, false, false, "payment provider rejected refund")
	default:
		return &provider.Error{Category: provider.ErrorInconsistentResponse, Operation: string(operation), Message: "payment provider fault was invalid"}
	}
}

func afterFault(operation Operation, fault Fault) error {
	switch fault.Kind {
	case FaultTimeoutAfterCommit:
		return faultProviderError(fault.Kind, provider.ErrorTimeoutUnknown, operation, false, true, "payment provider timed out with unknown outcome")
	case FaultResponseLoss:
		return faultProviderError(fault.Kind, provider.ErrorTransport, operation, false, true, "payment provider response was lost after commit")
	default:
		return nil
	}
}

func faultProviderError(kind FaultKind, category provider.ErrorCategory, operation Operation, retryable, uncertain bool, message string) *FaultError {
	return &FaultError{Kind: kind, Err: &provider.Error{Category: category, Operation: string(operation), Retryable: retryable, Uncertain: uncertain, Message: message}}
}

func validateCommon(intentID, reference string, amount int64, currency, idempotencyKey string, metadata provider.Metadata) error {
	if !validIdentifier(intentID) || !validIdentifier(reference) {
		return validationError(OperationCreateCheckout, "payment intent identity is invalid")
	}
	if !validIdentifier(idempotencyKey) || amount <= 0 || normalizeCurrency(currency) == "" {
		return validationError(OperationCreateCheckout, "checkout request is invalid")
	}
	if err := provider.ValidateMetadata(metadata); err != nil {
		return withOperation(err, OperationCreateCheckout)
	}
	return nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 160 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == ':' || char == '.' {
			continue
		}
		return false
	}
	return true
}

func validKeyID(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			continue
		}
		return false
	}
	return true
}

func normalizeCurrency(currency string) string {
	currency = strings.ToUpper(strings.TrimSpace(currency))
	if len(currency) != 3 {
		return ""
	}
	for _, char := range currency {
		if char < 'A' || char > 'Z' {
			return ""
		}
	}
	return currency
}

func fingerprint(value any) string {
	encoded, _ := json.Marshal(value)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func validationError(operation Operation, message string) *provider.Error {
	return &provider.Error{Category: provider.ErrorPermanentValidation, Operation: string(operation), Message: message}
}

func conflictError(operation Operation) *provider.Error {
	return &provider.Error{Category: provider.ErrorConflict, Operation: string(operation), Message: "payment provider idempotency or state conflict"}
}

func transportError(operation Operation, uncertain bool) *provider.Error {
	return &provider.Error{Category: provider.ErrorTransport, Operation: string(operation), Retryable: !uncertain, Uncertain: uncertain, Message: "payment provider transport failure"}
}

func withOperation(err error, operation Operation) error {
	var providerErr *provider.Error
	if errors.As(err, &providerErr) {
		clone := *providerErr
		clone.Operation = string(operation)
		return &clone
	}
	return err
}

func paymentFrom(record *paymentRecord) provider.Payment {
	return provider.Payment{ProviderPaymentID: record.id, Status: record.status, AmountMinor: record.amount, Currency: record.currency, CapturedMinor: record.captured, RefundedMinor: record.refunded, ProviderUpdatedAt: record.updatedAt}
}

func (s *Service) enqueueWebhook(record *paymentRecord, eventType provider.EventType, fault Fault) {
	requiredSlots := 1
	if fault.Kind == FaultDuplicateWebhook {
		requiredSlots = 2
	}
	if len(s.webhooks)+requiredSlots > s.maxWebhooks {
		return
	}
	s.nextEvent++
	s.step++
	event := provider.WebhookEvent{ProviderEventID: fmt.Sprintf("evt_sandbox_%012d", s.nextEvent), Type: eventType, ProviderPaymentID: record.id, Status: record.status, AmountMinor: record.amount, Currency: record.currency, OccurredAt: record.updatedAt}
	body, _ := json.Marshal(event)
	queued, _ := s.sign(body, s.now().UTC())
	queued.Sequence = s.step
	if fault.Kind == FaultDelayedWebhook {
		queued.DeliverAfter = s.step + max(fault.DelaySteps, 1)
	}
	if fault.Kind == FaultOutOfOrderWebhook {
		s.webhooks = append([]QueuedWebhook{queued}, s.webhooks...)
		return
	}
	s.webhooks = append(s.webhooks, queued)
	if fault.Kind == FaultDuplicateWebhook {
		duplicate := queued
		duplicate.Body = append([]byte(nil), queued.Body...)
		s.webhooks = append(s.webhooks, duplicate)
	}
}

// Advance moves the sandbox logical delivery clock without sleeping.
func (s *Service) Advance(steps uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.step += steps
}

// DrainWebhooks returns currently deliverable signed webhooks in deterministic
// queue order. Delayed events remain queued until Advance reaches their step.
func (s *Service) DrainWebhooks() []QueuedWebhook {
	return s.DrainWebhooksLimit(0)
}

// DrainWebhooksLimit returns at most limit currently deliverable events. A
// non-positive limit is reserved for in-process tests and drains all events.
func (s *Service) DrainWebhooksLimit(limit int) []QueuedWebhook {
	s.mu.Lock()
	defer s.mu.Unlock()
	capacity := len(s.webhooks)
	if limit > 0 && capacity > limit {
		capacity = limit
	}
	ready := make([]QueuedWebhook, 0, capacity)
	pending := s.webhooks[:0]
	for _, webhook := range s.webhooks {
		if webhook.DeliverAfter > s.step || (limit > 0 && len(ready) >= limit) {
			pending = append(pending, webhook)
			continue
		}
		webhook.Body = append([]byte(nil), webhook.Body...)
		ready = append(ready, webhook)
	}
	s.webhooks = pending
	return ready
}

func (s *Service) sign(body []byte, timestamp time.Time) (QueuedWebhook, error) {
	if len(body) > s.maxWebhookBody {
		return QueuedWebhook{}, errors.New("payment sandbox webhook body exceeds limit")
	}
	unix := strconv.FormatInt(timestamp.Unix(), 10)
	signature := signatureFor(s.keys[s.issueKeyID], unix, body)
	return QueuedWebhook{Headers: provider.WebhookHeaders{KeyID: s.issueKeyID, Timestamp: unix, Signature: signature}, Body: append([]byte(nil), body...)}, nil
}

// SignWebhook signs a bounded test event with the active issue key.
func (s *Service) SignWebhook(event provider.WebhookEvent) (provider.WebhookHeaders, []byte, error) {
	body, err := json.Marshal(event)
	if err != nil {
		return provider.WebhookHeaders{}, nil, errors.New("payment sandbox webhook event is invalid")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	queued, err := s.sign(body, s.now().UTC())
	return queued.Headers, queued.Body, err
}

func (s *Service) VerifyWebhook(ctx context.Context, headers provider.WebhookHeaders, body []byte) (provider.WebhookEvent, error) {
	if err := ctx.Err(); err != nil {
		return provider.WebhookEvent{}, &provider.Error{Category: provider.ErrorTransport, Operation: "verify_webhook", Retryable: true, Message: "payment provider transport failure"}
	}
	if len(body) == 0 || len(body) > s.maxWebhookBody {
		return provider.WebhookEvent{}, &provider.Error{Category: provider.ErrorPermanentValidation, Operation: "verify_webhook", Message: "payment webhook body is invalid"}
	}
	timestampUnix, err := strconv.ParseInt(headers.Timestamp, 10, 64)
	if err != nil {
		return provider.WebhookEvent{}, authenticationError()
	}
	timestamp := time.Unix(timestampUnix, 0)
	delta := s.now().UTC().Sub(timestamp)
	if delta < 0 {
		delta = -delta
	}
	if delta > s.replayTolerance {
		return provider.WebhookEvent{}, authenticationError()
	}
	s.mu.Lock()
	key, ok := s.keys[headers.KeyID]
	s.mu.Unlock()
	if !ok {
		return provider.WebhookEvent{}, authenticationError()
	}
	expected := signatureBytes(key, headers.Timestamp, body)
	provided, decodeErr := hex.DecodeString(headers.Signature)
	if decodeErr != nil || !hmac.Equal(expected, provided) {
		return provider.WebhookEvent{}, authenticationError()
	}
	var event provider.WebhookEvent
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil || !validIdentifier(event.ProviderEventID) || !validIdentifier(event.ProviderPaymentID) || event.AmountMinor <= 0 || normalizeCurrency(event.Currency) == "" || event.OccurredAt.IsZero() || !validStatus(event.Status) {
		return provider.WebhookEvent{}, &provider.Error{Category: provider.ErrorInconsistentResponse, Operation: "verify_webhook", Message: "payment webhook payload is invalid"}
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return provider.WebhookEvent{}, &provider.Error{Category: provider.ErrorInconsistentResponse, Operation: "verify_webhook", Message: "payment webhook payload is invalid"}
	}
	if !knownEventType(event.Type) {
		event.OriginalType = string(event.Type)
		event.Type = provider.EventUnknown
	}
	return event, nil
}

func signatureFor(key []byte, timestamp string, body []byte) string {
	return hex.EncodeToString(signatureBytes(key, timestamp, body))
}

func signatureBytes(key []byte, timestamp string, body []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	return mac.Sum(nil)
}

func authenticationError() *provider.Error {
	return &provider.Error{Category: provider.ErrorAuthentication, Operation: "verify_webhook", Message: "payment webhook authentication failed"}
}

func knownEventType(eventType provider.EventType) bool {
	switch eventType {
	case provider.EventCheckoutCreated, provider.EventAuthorized, provider.EventCaptured, provider.EventVoided, provider.EventRefunded:
		return true
	default:
		return false
	}
}

func validStatus(status provider.Status) bool {
	switch status {
	case provider.StatusCreated, provider.StatusRequiresCustomerAction, provider.StatusAuthorized, provider.StatusCaptured, provider.StatusVoided, provider.StatusRefunded, provider.StatusFailed, provider.StatusCancelled, provider.StatusUnknown:
		return true
	default:
		return false
	}
}

var _ provider.Client = (*Service)(nil)
