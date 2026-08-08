package webhook_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/webhook"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

func TestVerifiedWebhookIsStoredAsBoundedNormalizedEvidence(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 8, 10, 0, 0, 0, time.UTC)
	event := provider.WebhookEvent{
		ProviderEventID: "evt-1", Type: provider.EventCaptured,
		ProviderPaymentID: "pay-1", Status: provider.StatusCaptured,
		AmountMinor: 12500, Currency: "TWD", OccurredAt: now.Add(-time.Minute),
	}
	store := &repositoryFake{result: webhook.StoreAccepted}
	service, err := webhook.NewService(webhook.Config{
		Providers:  map[string]webhook.Verifier{"sandbox": verifierFake{event: event}},
		Repository: store, Now: func() time.Time { return now },
		NewID: func() uuid.UUID { return uuid.MustParse("c03e0caa-35f8-47c0-b4fb-754f5e407aa4") },
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	body := []byte(`{"provider_event_id":"evt-1"}`)
	disposition, err := service.IngestPaymentWebhook(context.Background(), httpapi.PaymentWebhookRequest{
		Provider: "sandbox", KeyID: "test-key", Timestamp: "1786161600",
		Signature: "signed-value", Body: body,
	})
	if err != nil || disposition != httpapi.PaymentWebhookAccepted {
		t.Fatalf("ingest disposition=%q error=%v", disposition, err)
	}
	wantHash := sha256.Sum256(body)
	if store.record.Provider != "sandbox" || store.record.ProviderEventID != "evt-1" ||
		store.record.EventType != string(provider.EventCaptured) || store.record.ProviderPaymentID != "pay-1" ||
		store.record.PayloadHash != wantHash || store.record.VerifiedKeyID != "test-key" ||
		store.record.EventCreatedAt != event.OccurredAt || store.record.SignatureVerifiedAt != now ||
		store.record.ReceivedAt != now || store.record.BodySizeBytes != len(body) || store.record.Ignored {
		t.Fatalf("stored record = %#v", store.record)
	}
}

func TestInvalidSignatureMapsToBoundedWebhookErrorWithoutPersistence(t *testing.T) {
	t.Parallel()
	store := &repositoryFake{}
	metrics := &webhookMetricsFake{}
	service := newTestService(t, verifierFake{err: &provider.Error{
		Category: provider.ErrorAuthentication, Operation: "verify_webhook",
		Message: "provider detail that must not escape",
	}}, store, metrics)

	disposition, err := service.IngestPaymentWebhook(context.Background(), validRequest([]byte(`{}`)))
	if disposition != "" || !errors.Is(err, httpapi.ErrWebhookInvalid) {
		t.Fatalf("ingest disposition=%q error=%v, want bounded invalid error", disposition, err)
	}
	if store.calls != 0 {
		t.Fatalf("repository calls = %d, want 0", store.calls)
	}
	if metrics.invalidCalls != 1 || metrics.provider != "sandbox" {
		t.Fatalf("invalid metrics = calls %d provider %q", metrics.invalidCalls, metrics.provider)
	}
}

func TestSameProviderEventAndPayloadIsReportedAsDuplicate(t *testing.T) {
	t.Parallel()
	event := provider.WebhookEvent{
		ProviderEventID: "evt-replay", Type: provider.EventAuthorized,
		ProviderPaymentID: "pay-1", Status: provider.StatusAuthorized,
		AmountMinor: 12500, Currency: "TWD", OccurredAt: time.Now().UTC(),
	}
	store := newMemoryRepository()
	service := newTestService(t, verifierFake{event: event}, store)
	request := validRequest([]byte(`{"provider_event_id":"evt-replay"}`))

	first, err := service.IngestPaymentWebhook(context.Background(), request)
	if err != nil || first != httpapi.PaymentWebhookAccepted {
		t.Fatalf("first ingest disposition=%q error=%v", first, err)
	}
	second, err := service.IngestPaymentWebhook(context.Background(), request)
	if err != nil || second != httpapi.PaymentWebhookDuplicate {
		t.Fatalf("duplicate ingest disposition=%q error=%v", second, err)
	}
	if len(store.records) != 1 {
		t.Fatalf("durable records = %d, want 1", len(store.records))
	}
}

func TestSameProviderEventWithChangedPayloadReturnsBoundedConflict(t *testing.T) {
	t.Parallel()
	event := provider.WebhookEvent{
		ProviderEventID: "evt-conflict", Type: provider.EventAuthorized,
		ProviderPaymentID: "pay-1", Status: provider.StatusAuthorized,
		AmountMinor: 12500, Currency: "TWD", OccurredAt: time.Now().UTC(),
	}
	store := newMemoryRepository()
	service := newTestService(t, verifierFake{event: event}, store)
	if _, err := service.IngestPaymentWebhook(context.Background(), validRequest([]byte(`{"revision":1}`))); err != nil {
		t.Fatalf("first ingest: %v", err)
	}

	disposition, err := service.IngestPaymentWebhook(context.Background(), validRequest([]byte(`{"revision":2}`)))
	if disposition != "" || !errors.Is(err, httpapi.ErrWebhookConflict) {
		t.Fatalf("changed payload disposition=%q error=%v, want bounded conflict", disposition, err)
	}
	if len(store.records) != 1 {
		t.Fatalf("canonical records = %d, want 1", len(store.records))
	}
}

func TestUnknownSignedEventIsDurablyIgnoredAndAcknowledged(t *testing.T) {
	t.Parallel()
	event := provider.WebhookEvent{
		ProviderEventID: "evt-future", Type: provider.EventUnknown,
		OriginalType: "payment.future_state", ProviderPaymentID: "pay-1",
		Status: provider.StatusAuthorized, AmountMinor: 12500, Currency: "TWD",
		OccurredAt: time.Now().UTC(),
	}
	store := &repositoryFake{result: webhook.StoreAccepted}
	service := newTestService(t, verifierFake{event: event}, store)

	disposition, err := service.IngestPaymentWebhook(context.Background(), validRequest([]byte(`{"type":"payment.future_state"}`)))
	if err != nil || disposition != httpapi.PaymentWebhookIgnored {
		t.Fatalf("unknown event disposition=%q error=%v", disposition, err)
	}
	if !store.record.Ignored || store.record.EventType != "payment.future_state" {
		t.Fatalf("unknown event record = %#v", store.record)
	}
}

func TestRawPayloadAndSignatureNeverCrossRepositoryBoundary(t *testing.T) {
	t.Parallel()
	event := provider.WebhookEvent{
		ProviderEventID: "evt-sensitive-boundary", Type: provider.EventCaptured,
		ProviderPaymentID: "pay-1", Status: provider.StatusCaptured,
		AmountMinor: 12500, Currency: "TWD", OccurredAt: time.Now().UTC(),
	}
	store := &repositoryFake{result: webhook.StoreAccepted}
	service := newTestService(t, verifierFake{event: event}, store)
	request := validRequest([]byte(`{"private_marker":"raw-body-must-not-persist"}`))
	request.Signature = "signature-must-not-persist"

	if _, err := service.IngestPaymentWebhook(context.Background(), request); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	stored := fmt.Sprintf("%#v", store.record)
	for _, forbidden := range []string{"raw-body-must-not-persist", "signature-must-not-persist"} {
		if strings.Contains(stored, forbidden) {
			t.Fatalf("repository record contains forbidden raw value %q: %s", forbidden, stored)
		}
	}
}

func TestProviderHeadersAndBodyAreBoundedBeforeVerification(t *testing.T) {
	t.Parallel()
	calls := 0
	verifier := verifierFake{calls: &calls, event: provider.WebhookEvent{
		ProviderEventID: "evt-1", Type: provider.EventCaptured,
		ProviderPaymentID: "pay-1", OccurredAt: time.Now().UTC(),
	}}
	store := &repositoryFake{result: webhook.StoreAccepted}
	service, err := webhook.NewService(webhook.Config{
		Providers: map[string]webhook.Verifier{"sandbox": verifier}, Repository: store,
		MaxBodyBytes: 8, Now: time.Now, NewID: uuid.New,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	tests := []struct {
		name    string
		request httpapi.PaymentWebhookRequest
	}{
		{name: "provider outside allowlist", request: validRequest([]byte(`{}`))},
		{name: "missing key id", request: validRequest([]byte(`{}`))},
		{name: "oversized timestamp", request: validRequest([]byte(`{}`))},
		{name: "oversized signature", request: validRequest([]byte(`{}`))},
		{name: "oversized body", request: validRequest([]byte(`123456789`))},
	}
	tests[0].request.Provider = "another"
	tests[1].request.KeyID = ""
	tests[2].request.Timestamp = strings.Repeat("1", 33)
	tests[3].request.Signature = strings.Repeat("a", 257)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			disposition, ingestErr := service.IngestPaymentWebhook(context.Background(), test.request)
			if disposition != "" || !errors.Is(ingestErr, httpapi.ErrWebhookInvalid) {
				t.Fatalf("disposition=%q error=%v", disposition, ingestErr)
			}
		})
	}
	if calls != 0 || store.calls != 0 {
		t.Fatalf("verification calls=%d persistence calls=%d, want 0,0", calls, store.calls)
	}
}

type verifierFake struct {
	event provider.WebhookEvent
	err   error
	calls *int
}

func (fake verifierFake) VerifyWebhook(context.Context, provider.WebhookHeaders, []byte) (provider.WebhookEvent, error) {
	if fake.calls != nil {
		*fake.calls++
	}
	return fake.event, fake.err
}

type repositoryFake struct {
	record webhook.Record
	result webhook.StoreResult
	err    error
	calls  int
}

func (fake *repositoryFake) StoreVerified(_ context.Context, record webhook.Record) (webhook.StoreResult, error) {
	fake.calls++
	fake.record = record
	return fake.result, fake.err
}

func newTestService(t *testing.T, verifier webhook.Verifier, store webhook.Repository, metrics ...webhook.Metrics) *webhook.Service {
	t.Helper()
	service, err := webhook.NewService(webhook.Config{
		Providers: map[string]webhook.Verifier{"sandbox": verifier}, Repository: store,
		Now:   func() time.Time { return time.Date(2026, time.August, 8, 10, 0, 0, 0, time.UTC) },
		NewID: uuid.New, Metrics: firstMetrics(metrics),
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return service
}

func firstMetrics(values []webhook.Metrics) webhook.Metrics {
	if len(values) == 0 {
		return nil
	}
	return values[0]
}

type webhookMetricsFake struct {
	provider     string
	invalidCalls int
}

func (fake *webhookMetricsFake) RecordPaymentWebhookInvalid(providerName string) {
	fake.provider = providerName
	fake.invalidCalls++
}

func validRequest(body []byte) httpapi.PaymentWebhookRequest {
	return httpapi.PaymentWebhookRequest{
		Provider: "sandbox", KeyID: "test-key", Timestamp: "1786161600",
		Signature: "signed-value", Body: body,
	}
}

type memoryRepository struct {
	records map[string]webhook.Record
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{records: make(map[string]webhook.Record)}
}

func (repository *memoryRepository) StoreVerified(_ context.Context, record webhook.Record) (webhook.StoreResult, error) {
	key := record.Provider + "\x00" + record.ProviderEventID
	existing, found := repository.records[key]
	if !found {
		repository.records[key] = record
		return webhook.StoreAccepted, nil
	}
	if existing.PayloadHash == record.PayloadHash {
		return webhook.StoreDuplicate, nil
	}
	return webhook.StoreConflict, nil
}
