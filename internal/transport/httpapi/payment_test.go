package httpapi_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

func TestCreatePaymentIntentUsesJWTIdentityAndRejectsFinancialInput(t *testing.T) {
	t.Parallel()
	parser := &tokenParserStub{identity: httpapi.Identity{Subject: "customer-from-jwt", Role: httpapi.RoleCustomer}}
	payments := &paymentServiceStub{result: httpapi.PaymentIntentView{ID: "intent-1", ReservationID: "reservation-1", State: "reservation_securing", AmountMinor: 12500, Currency: "TWD"}}
	router := httpapi.New(httpapi.Dependencies{TokenParser: parser, Payments: payments})

	request := httptest.NewRequest(http.MethodPost, "/api/v1/reservations/reservation-1/payment-intents", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer signed-token")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "payment-request-1")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("create payment status = %d body=%s", response.Code, response.Body)
	}
	if payments.create.OwnerID != "customer-from-jwt" || payments.create.ReservationID != "reservation-1" || payments.create.IdempotencyKey != "payment-request-1" {
		t.Fatalf("create command = %#v", payments.create)
	}

	for _, body := range []string{`{"amount_minor":1}`, `{"currency":"USD"}`, `{"card_number":"4111111111111111"}`, `{"cvv":"123"}`} {
		payments.create = httpapi.CreatePaymentIntentCommand{}
		request = httptest.NewRequest(http.MethodPost, "/api/v1/reservations/reservation-1/payment-intents", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer signed-token")
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "payment-request-2")
		response = httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || payments.create.OwnerID != "" {
			t.Fatalf("forbidden payload %s status=%d command=%#v", body, response.Code, payments.create)
		}
	}
}

func TestPaymentIntentReadAndCancelRemainOwnerScoped(t *testing.T) {
	t.Parallel()
	parser := &tokenParserStub{identity: httpapi.Identity{Subject: "customer-1", Role: httpapi.RoleCustomer}}
	payments := &paymentServiceStub{result: httpapi.PaymentIntentView{ID: "intent-1", State: "awaiting_customer"}}
	router := httpapi.New(httpapi.Dependencies{TokenParser: parser, Payments: payments})

	get := httptest.NewRequest(http.MethodGet, "/api/v1/payment-intents/intent-1", nil)
	get.Header.Set("Authorization", "Bearer signed-token")
	getResponse := httptest.NewRecorder()
	router.ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusOK || payments.getOwner != "customer-1" || payments.getID != "intent-1" {
		t.Fatalf("get status=%d owner=%q id=%q", getResponse.Code, payments.getOwner, payments.getID)
	}

	cancel := httptest.NewRequest(http.MethodPost, "/api/v1/payment-intents/intent-1/cancel", strings.NewReader(`{}`))
	cancel.Header.Set("Authorization", "Bearer signed-token")
	cancel.Header.Set("Content-Type", "application/json")
	cancel.Header.Set("Idempotency-Key", "payment-cancel-1")
	cancelResponse := httptest.NewRecorder()
	router.ServeHTTP(cancelResponse, cancel)
	if cancelResponse.Code != http.StatusAccepted || payments.cancel.OwnerID != "customer-1" || payments.cancel.PaymentIntentID != "intent-1" {
		t.Fatalf("cancel status=%d command=%#v", cancelResponse.Code, payments.cancel)
	}
}

func TestPaymentWebhookIsUnauthenticatedButBoundedAndContentTyped(t *testing.T) {
	t.Parallel()
	webhooks := &webhookServiceStub{result: httpapi.PaymentWebhookAccepted}
	router := httpapi.New(httpapi.Dependencies{PaymentWebhooks: webhooks, PaymentWebhookMaxBodyBytes: 32})

	request := httptest.NewRequest(http.MethodPost, "/webhooks/payments/sandbox", strings.NewReader(`{"id":"evt-1"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Payment-Key-ID", "test-key")
	request.Header.Set("X-Payment-Timestamp", "1786161600")
	request.Header.Set("X-Payment-Signature", "synthetic-signature")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || webhooks.request.Provider != "sandbox" || string(webhooks.request.Body) != `{"id":"evt-1"}` {
		t.Fatalf("webhook status=%d request=%#v", response.Code, webhooks.request)
	}

	oversized := httptest.NewRequest(http.MethodPost, "/webhooks/payments/sandbox", strings.NewReader(strings.Repeat("x", 33)))
	oversized.Header.Set("Content-Type", "application/json")
	oversizedResponse := httptest.NewRecorder()
	router.ServeHTTP(oversizedResponse, oversized)
	if oversizedResponse.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized webhook status=%d", oversizedResponse.Code)
	}

	wrongType := httptest.NewRequest(http.MethodPost, "/webhooks/payments/sandbox", strings.NewReader(`{}`))
	wrongType.Header.Set("Content-Type", "text/plain")
	wrongTypeResponse := httptest.NewRecorder()
	router.ServeHTTP(wrongTypeResponse, wrongType)
	if wrongTypeResponse.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("wrong content type status=%d", wrongTypeResponse.Code)
	}
}

func TestStripeWebhookUsesRawStripeSignatureHeader(t *testing.T) {
	t.Parallel()
	webhooks := &webhookServiceStub{result: httpapi.PaymentWebhookAccepted}
	router := httpapi.New(httpapi.Dependencies{PaymentWebhooks: webhooks, PaymentWebhookMaxBodyBytes: 64})
	request := httptest.NewRequest(http.MethodPost, "/webhooks/payments/stripe", strings.NewReader(`{"id":"evt-1"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Stripe-Signature", "t=1786449600,v1=abcdef")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || webhooks.request.Provider != "stripe" ||
		webhooks.request.Signature != "t=1786449600,v1=abcdef" || webhooks.request.KeyID != "" || webhooks.request.Timestamp != "" {
		t.Fatalf("status=%d request=%#v", response.Code, webhooks.request)
	}
}

func TestTicketRefundEndpointsAreOwnerScopedAndAcceptOnlyTicketSelection(t *testing.T) {
	t.Parallel()
	parser := &tokenParserStub{identity: httpapi.Identity{Subject: "customer-1", Role: httpapi.RoleCustomer}}
	refunds := &ticketRefundServiceStub{result: httpapi.TicketRefundView{
		ID: "refund-1", TicketOrderID: "order-1", TicketIDs: []string{"ticket-1"},
		State: "created", AmountMinor: 500, Currency: "TWD",
	}}
	router := httpapi.New(httpapi.Dependencies{TokenParser: parser, TicketRefunds: refunds})

	request := httptest.NewRequest(http.MethodPost, "/api/v1/ticket-orders/order-1/refunds", strings.NewReader(`{"ticket_ids":["ticket-1"]}`))
	request.Header.Set("Authorization", "Bearer signed-token")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "refund-request-1")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || refunds.create.OwnerID != "customer-1" ||
		refunds.create.TicketOrderID != "order-1" || refunds.create.IdempotencyKey != "refund-request-1" ||
		len(refunds.create.TicketIDs) != 1 || refunds.create.TicketIDs[0] != "ticket-1" {
		t.Fatalf("status=%d command=%#v body=%s", response.Code, refunds.create, response.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/api/v1/ticket-refunds/refund-1", nil)
	get.Header.Set("Authorization", "Bearer signed-token")
	getResponse := httptest.NewRecorder()
	router.ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusOK || refunds.getOwner != "customer-1" || refunds.getID != "refund-1" {
		t.Fatalf("status=%d owner=%q id=%q", getResponse.Code, refunds.getOwner, refunds.getID)
	}

	for _, body := range []string{`{"ticket_ids":["ticket-1"],"amount_minor":1}`, `{"ticket_ids":["ticket-1"],"provider":"stripe"}`, `{"ticket_ids":["ticket-1"],"currency":"USD"}`} {
		refunds.create = httpapi.CreateTicketRefundCommand{}
		request = httptest.NewRequest(http.MethodPost, "/api/v1/ticket-orders/order-1/refunds", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer signed-token")
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "refund-request-2")
		response = httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || refunds.create.OwnerID != "" {
			t.Fatalf("forbidden payload=%s status=%d command=%#v", body, response.Code, refunds.create)
		}
	}
}

func TestPaymentWebhookEnforcesConfiguredRequestTimeout(t *testing.T) {
	webhooks := &webhookServiceStub{waitForCancellation: true}
	router := httpapi.New(httpapi.Dependencies{
		PaymentWebhooks: webhooks, PaymentWebhookMaxBodyBytes: 64,
		PaymentWebhookTimeout: time.Millisecond,
	})
	request := httptest.NewRequest(http.MethodPost, "/webhooks/payments/sandbox", strings.NewReader(`{"id":"evt-1"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Payment-Key-ID", "test-key")
	request.Header.Set("X-Payment-Timestamp", "1786161600")
	request.Header.Set("X-Payment-Signature", "synthetic-signature")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || !webhooks.contextCancelled {
		t.Fatalf("status=%d cancelled=%t body=%s", response.Code, webhooks.contextCancelled, response.Body.String())
	}
}

func TestPaymentWebhookRateLimitRejectsKnownExcessAndFailsOpenOnBackendOutage(t *testing.T) {
	t.Parallel()
	webhooks := &webhookServiceStub{result: httpapi.PaymentWebhookAccepted}
	limiter := &rateLimiterStub{allowed: false}
	router := httpapi.New(httpapi.Dependencies{PaymentWebhooks: webhooks, RateLimiter: limiter})
	request := httptest.NewRequest(http.MethodPost, "/webhooks/payments/sandbox", strings.NewReader(strings.Repeat("x", 1024)))
	request.RemoteAddr = "192.0.2.45:49152"
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusTooManyRequests || webhooks.request.Provider != "" {
		t.Fatalf("status=%d request=%+v", response.Code, webhooks.request)
	}
	if limiter.input != (httpapi.RateLimitRequest{Scope: httpapi.RateLimitPaymentWebhook, Key: "192.0.2.45"}) {
		t.Fatalf("limiter input = %+v", limiter.input)
	}

	outageWebhooks := &webhookServiceStub{result: httpapi.PaymentWebhookAccepted}
	outage := &rateLimiterStub{err: errors.New("redis unavailable")}
	router = httpapi.New(httpapi.Dependencies{PaymentWebhooks: outageWebhooks, RateLimiter: outage})
	request = httptest.NewRequest(http.MethodPost, "/webhooks/payments/sandbox", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || outageWebhooks.request.Provider != "sandbox" {
		t.Fatalf("outage status=%d request=%+v", response.Code, outageWebhooks.request)
	}
}

func TestCommittedWebhookConflictReturnsSuccess(t *testing.T) {
	t.Parallel()
	webhooks := &webhookServiceStub{result: httpapi.PaymentWebhookConflict}
	router := httpapi.New(httpapi.Dependencies{PaymentWebhooks: webhooks})
	request := httptest.NewRequest(http.MethodPost, "/webhooks/payments/sandbox", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"conflict"`) {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestUncommittedWebhookEvidenceReturnsServerError(t *testing.T) {
	t.Parallel()
	webhooks := &webhookServiceStub{err: errors.New("persistence unavailable")}
	router := httpapi.New(httpapi.Dependencies{PaymentWebhooks: webhooks})
	request := httptest.NewRequest(http.MethodPost, "/webhooks/payments/sandbox", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
}

type paymentServiceStub struct {
	create            httpapi.CreatePaymentIntentCommand
	cancel            httpapi.CancelPaymentIntentCommand
	reservationCancel httpapi.CancelReservationPaymentCommand
	getOwner          string
	getID             string
	result            httpapi.PaymentIntentView
	err               error
}

func (stub *paymentServiceStub) CreatePaymentIntent(_ context.Context, command httpapi.CreatePaymentIntentCommand) (httpapi.PaymentIntentView, error) {
	stub.create = command
	return stub.result, stub.err
}

func (stub *paymentServiceStub) GetPaymentIntent(_ context.Context, ownerID, paymentIntentID string) (httpapi.PaymentIntentView, error) {
	stub.getOwner, stub.getID = ownerID, paymentIntentID
	return stub.result, stub.err
}

func (stub *paymentServiceStub) CancelPaymentIntent(_ context.Context, command httpapi.CancelPaymentIntentCommand) (httpapi.PaymentIntentView, error) {
	stub.cancel = command
	return stub.result, stub.err
}

func (stub *paymentServiceStub) CancelReservationPayment(_ context.Context, command httpapi.CancelReservationPaymentCommand) (httpapi.PaymentIntentView, error) {
	stub.reservationCancel = command
	return stub.result, stub.err
}

type webhookServiceStub struct {
	request             httpapi.PaymentWebhookRequest
	result              httpapi.PaymentWebhookDisposition
	err                 error
	waitForCancellation bool
	contextCancelled    bool
}

type ticketRefundServiceStub struct {
	create   httpapi.CreateTicketRefundCommand
	getOwner string
	getID    string
	result   httpapi.TicketRefundView
	err      error
}

func (stub *ticketRefundServiceStub) CreateTicketRefund(_ context.Context, command httpapi.CreateTicketRefundCommand) (httpapi.TicketRefundView, error) {
	stub.create = command
	return stub.result, stub.err
}

func (stub *ticketRefundServiceStub) GetTicketRefund(_ context.Context, ownerID, refundID string) (httpapi.TicketRefundView, error) {
	stub.getOwner, stub.getID = ownerID, refundID
	return stub.result, stub.err
}

func (stub *webhookServiceStub) IngestPaymentWebhook(ctx context.Context, request httpapi.PaymentWebhookRequest) (httpapi.PaymentWebhookDisposition, error) {
	stub.request = request
	if stub.waitForCancellation {
		<-ctx.Done()
		stub.contextCancelled = true
		return "", httpapi.ErrWebhookInvalid
	}
	return stub.result, stub.err
}
