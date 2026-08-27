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
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
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

func TestStripeAccountAndEnvironmentAreStoredAsBoundedEvidence(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 8, 10, 0, 0, 0, time.UTC)
	store := &repositoryFake{result: webhook.StoreAccepted}
	verifier := verifierFake{event: provider.WebhookEvent{
		ProviderEventID: "evt_stripe_bound", VerifiedKeyID: "stripe-current",
		ProviderAccountID: "acct_contract", Environment: provider.WebhookEnvironmentTest,
		Type: provider.EventCaptured, ProviderPaymentID: "pi_contract", OccurredAt: now,
	}}
	service, err := webhook.NewService(webhook.Config{
		Providers: map[string]webhook.Verifier{"stripe": verifier}, Repository: store,
		Now: func() time.Time { return now }, NewID: uuid.New, Keyring: keyringValidatorFake{},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := validRequest([]byte(`{"id":"evt_stripe_bound"}`))
	request.Provider = "stripe"
	request.KeyID = ""
	disposition, err := service.IngestPaymentWebhook(context.Background(), request)
	if err != nil || disposition != httpapi.PaymentWebhookAccepted {
		t.Fatalf("disposition=%q error=%v", disposition, err)
	}
	if store.record.ProviderAccountID != "acct_contract" || store.record.ProviderEnvironment != "test" {
		t.Fatalf("stored binding = %#v", store.record)
	}
}

func TestStripeDurableKeyringFenceRejectsStaleReplicaBeforePersistence(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 8, 10, 0, 0, 0, time.UTC)
	store := &repositoryFake{result: webhook.StoreAccepted}
	service, err := webhook.NewService(webhook.Config{
		Providers: map[string]webhook.Verifier{"stripe": verifierFake{event: provider.WebhookEvent{
			ProviderEventID: "evt_stale_key", VerifiedKeyID: "retired",
			ProviderAccountID: "acct_contract", Environment: provider.WebhookEnvironmentTest,
			Type: provider.EventCaptured, ProviderPaymentID: "pi_contract", OccurredAt: now,
		}}},
		Repository: store, Now: func() time.Time { return now }, NewID: uuid.New,
		Keyring: keyringValidatorFake{err: webhook.ErrKeyringConflict},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := validRequest([]byte(`{"id":"evt_stale_key"}`))
	request.Provider, request.KeyID = "stripe", ""
	if _, err := service.IngestPaymentWebhook(context.Background(), request); !errors.Is(err, httpapi.ErrWebhookInvalid) {
		t.Fatalf("stale key error = %v", err)
	}
	if store.calls != 0 {
		t.Fatalf("stale key reached persistence %d times", store.calls)
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

func TestWebhookAcknowledgementMetricsFollowDurableStoreResult(t *testing.T) {
	t.Parallel()
	event := provider.WebhookEvent{
		ProviderEventID: "evt-metric-replay", Type: provider.EventAuthorized,
		ProviderPaymentID: "pay-1", Status: provider.StatusAuthorized,
		AmountMinor: 12500, Currency: "TWD", OccurredAt: time.Now().UTC(),
	}
	metrics := &webhookMetricsFake{}
	service := newTestService(t, verifierFake{event: event}, newMemoryRepository(), metrics)
	request := validRequest([]byte(`{"provider_event_id":"evt-metric-replay"}`))

	if _, err := service.IngestPaymentWebhook(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := service.IngestPaymentWebhook(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := metrics.ackResults; len(got) != 2 || got[0] != "sandbox:accepted:none" || got[1] != "sandbox:duplicate:duplicate" {
		t.Fatalf("ack observations = %v", got)
	}
}

func TestWebhookAuthorityRejectionRecordsRegionalFenceReason(t *testing.T) {
	t.Parallel()
	metrics := &webhookMetricsFake{}
	service := newTestService(t, verifierFake{event: provider.WebhookEvent{
		ProviderEventID: "evt-fenced", Type: provider.EventAuthorized,
		ProviderPaymentID: "pay-fenced", OccurredAt: time.Now().UTC(),
	}}, &repositoryFake{err: authority.ErrWritesDisabled}, metrics)
	if _, err := service.IngestPaymentWebhook(context.Background(), validRequest([]byte(`{"id":"evt-fenced"}`))); !errors.Is(err, webhook.ErrPersistence) {
		t.Fatalf("error = %v", err)
	}
	want := []string{"sandbox:failure:writes_disabled", "region-a:control:none:writes_disabled"}
	if len(metrics.ackResults) != len(want) || metrics.ackResults[0] != want[0] || metrics.ackResults[1] != want[1] {
		t.Fatalf("observations = %v, want %v", metrics.ackResults, want)
	}
}

func TestWebhookInactiveAuthorityRecordsFencedReason(t *testing.T) {
	t.Parallel()
	metrics := &webhookMetricsFake{}
	service := newTestService(t, verifierFake{event: provider.WebhookEvent{
		ProviderEventID: "evt-authority-inactive", Type: provider.EventAuthorized,
		ProviderPaymentID: "pay-fenced", OccurredAt: time.Now().UTC(),
	}}, &repositoryFake{err: authority.ErrAuthorityNotActive}, metrics)
	if _, err := service.IngestPaymentWebhook(context.Background(), validRequest([]byte(`{"id":"evt-authority-inactive"}`))); !errors.Is(err, webhook.ErrPersistence) {
		t.Fatalf("error = %v", err)
	}
	want := []string{"sandbox:failure:fenced", "region-a:control:none:fenced"}
	if len(metrics.ackResults) != len(want) || metrics.ackResults[0] != want[0] || metrics.ackResults[1] != want[1] {
		t.Fatalf("observations = %v, want %v", metrics.ackResults, want)
	}
}

func TestSameProviderEventWithChangedPayloadIsAcknowledgedAfterDurableConflict(t *testing.T) {
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
	if err != nil || disposition != httpapi.PaymentWebhookConflict {
		t.Fatalf("changed payload disposition=%q error=%v, want acknowledged conflict", disposition, err)
	}
	if len(store.records) != 1 {
		t.Fatalf("canonical records = %d, want 1", len(store.records))
	}
}

func TestChangedPayloadConflictPersistenceFailureIsRetryable(t *testing.T) {
	t.Parallel()
	service := newTestService(t, verifierFake{event: provider.WebhookEvent{
		ProviderEventID: "evt-conflict-rollback", Type: provider.EventAuthorized,
		ProviderPaymentID: "pay-1", OccurredAt: time.Now().UTC(),
	}}, &repositoryFake{err: webhook.ErrPersistence})

	disposition, err := service.IngestPaymentWebhook(context.Background(), validRequest([]byte(`{"revision":2}`)))
	if disposition != "" || !errors.Is(err, webhook.ErrPersistence) {
		t.Fatalf("rollback disposition=%q error=%v, want retryable persistence failure", disposition, err)
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
	metrics := &webhookMetricsFake{}
	verifier := verifierFake{calls: &calls, event: provider.WebhookEvent{
		ProviderEventID: "evt-1", Type: provider.EventCaptured,
		ProviderPaymentID: "pay-1", OccurredAt: time.Now().UTC(),
	}}
	store := &repositoryFake{result: webhook.StoreAccepted}
	service, err := webhook.NewService(webhook.Config{
		Providers: map[string]webhook.Verifier{"sandbox": verifier}, Repository: store,
		MaxBodyBytes: 8, Now: time.Now, NewID: uuid.New, Metrics: metrics,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	tests := []struct {
		name    string
		request httpapi.PaymentWebhookRequest
	}{
		{name: "provider outside allowlist", request: validRequest([]byte(`{}`))},
		{name: "oversized timestamp", request: validRequest([]byte(`{}`))},
		{name: "oversized signature", request: validRequest([]byte(`{}`))},
		{name: "oversized body", request: validRequest([]byte(`123456789`))},
	}
	tests[0].request.Provider = "another"
	tests[1].request.Timestamp = strings.Repeat("1", 33)
	tests[2].request.Signature = strings.Repeat("a", 257)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			disposition, ingestErr := service.IngestPaymentWebhook(context.Background(), test.request)
			if disposition != "" || !errors.Is(ingestErr, httpapi.ErrWebhookInvalid) {
				t.Fatalf("disposition=%q error=%v", disposition, ingestErr)
			}
		})
	}
	if calls != 0 || store.calls != 0 || metrics.invalidCalls != 0 {
		t.Fatalf("verification calls=%d persistence calls=%d invalid metrics=%d, want 0,0,0", calls, store.calls, metrics.invalidCalls)
	}
}

func TestIngressAcceptsProviderVerifiedKeyWithoutLegacyHeaders(t *testing.T) {
	t.Parallel()

	store := &repositoryFake{result: webhook.StoreAccepted}
	service := newTestService(t, verifierFake{event: provider.WebhookEvent{
		ProviderEventID: "evt-stripe", Type: provider.EventAuthorized,
		ProviderPaymentID: "pi-stripe", VerifiedKeyID: "stripe-current",
		OccurredAt: time.Now().UTC(),
	}}, store, &webhookMetricsFake{})
	request := validRequest([]byte(`{"id":"evt-stripe"}`))
	request.KeyID = ""
	request.Timestamp = ""

	disposition, err := service.IngestPaymentWebhook(context.Background(), request)
	if err != nil || disposition != httpapi.PaymentWebhookAccepted {
		t.Fatalf("disposition=%q error=%v", disposition, err)
	}
	if store.record.VerifiedKeyID != "stripe-current" {
		t.Fatalf("verified key = %q", store.record.VerifiedKeyID)
	}
}

func TestInvalidSignatureMetricExcludesNonAuthenticationFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		verifier verifierFake
	}{
		{
			name: "provider verifier unavailable",
			verifier: verifierFake{err: &provider.Error{
				Category: provider.ErrorUnavailable, Operation: "verify_webhook", Message: "provider unavailable",
			}},
		},
		{
			name: "verified event has invalid shape",
			verifier: verifierFake{event: provider.WebhookEvent{
				ProviderEventID: "evt-invalid", Type: provider.EventCaptured, ProviderPaymentID: "pay-1",
			}},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			metrics := &webhookMetricsFake{}
			store := &repositoryFake{}
			service := newTestService(t, test.verifier, store, metrics)
			disposition, err := service.IngestPaymentWebhook(context.Background(), validRequest([]byte(`{}`)))
			if disposition != "" || !errors.Is(err, httpapi.ErrWebhookInvalid) {
				t.Fatalf("ingest disposition=%q error=%v", disposition, err)
			}
			if metrics.invalidCalls != 0 || store.calls != 0 {
				t.Fatalf("invalid metrics=%d persistence calls=%d, want 0,0", metrics.invalidCalls, store.calls)
			}
		})
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
	return newTestServiceForProvider(t, "sandbox", verifier, store, metrics...)
}

func newTestServiceForProvider(t *testing.T, providerName string, verifier webhook.Verifier, store webhook.Repository, metrics ...webhook.Metrics) *webhook.Service {
	t.Helper()
	var keyring webhook.KeyringValidator
	if providerName == "stripe" {
		keyring = keyringValidatorFake{}
	}
	service, err := webhook.NewService(webhook.Config{
		Providers: map[string]webhook.Verifier{providerName: verifier}, Repository: store,
		Now:   func() time.Time { return time.Date(2026, time.August, 8, 10, 0, 0, 0, time.UTC) },
		NewID: uuid.New, Metrics: firstMetrics(metrics), Region: "region-a", Keyring: keyring,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return service
}

type keyringValidatorFake struct{ err error }

func (fake keyringValidatorFake) ValidateVerifiedKey(context.Context, string, string, string, time.Time) error {
	return fake.err
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
	ackResults   []string
}

func (fake *webhookMetricsFake) RecordWebhookAck(provider, result, reason string, _ time.Duration) {
	fake.ackResults = append(fake.ackResults, provider+":"+result+":"+reason)
}

func (fake *webhookMetricsFake) RecordRegionalWriteRejected(region, databaseRole, shardID, reason string) {
	fake.ackResults = append(fake.ackResults, region+":"+databaseRole+":"+shardID+":"+reason)
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
