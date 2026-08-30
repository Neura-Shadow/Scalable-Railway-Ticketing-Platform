package conformance

import (
	"context"
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
)

type WebhookVerifier interface {
	VerifyWebhook(context.Context, provider.WebhookHeaders, []byte) (provider.WebhookEvent, error)
}

type WebhookCase struct {
	Verifier WebhookVerifier
	Headers  provider.WebhookHeaders
	Body     []byte
	Expected provider.WebhookEvent
}

type WebhookHarness struct {
	Descriptor provider.Descriptor
	Current    func(*testing.T) WebhookCase
	Rotated    func(*testing.T) WebhookCase
	Retired    func(*testing.T) WebhookCase
}

// RunWebhook verifies the provider-neutral webhook contract: exact raw-body
// authentication, normalized event identity, bounded two-key overlap, and
// fail-closed retirement. Provider-specific signature formats stay in adapters.
func RunWebhook(t *testing.T, harness WebhookHarness) {
	t.Helper()
	if err := harness.Descriptor.Validate(); err != nil {
		t.Fatalf("provider descriptor: %v", err)
	}
	if !harness.Descriptor.Capabilities.WebhookSignatures {
		t.Skip("provider does not advertise webhook signatures")
	}
	if harness.Current == nil {
		t.Fatal("webhook conformance harness is incomplete")
	}

	t.Run("authenticates exact raw body and normalizes event", func(t *testing.T) {
		test := harness.Current(t)
		verifyWebhookCase(t, test)
		tampered := append([]byte(nil), test.Body...)
		if len(tampered) == 0 {
			t.Fatal("webhook body is empty")
		}
		tampered[len(tampered)-1] ^= 1
		_, err := test.Verifier.VerifyWebhook(context.Background(), test.Headers, tampered)
		var providerErr *provider.Error
		if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorAuthentication {
			t.Fatalf("tampered webhook error = %#v", err)
		}
	})

	if !harness.Descriptor.Capabilities.WebhookKeyRotation {
		return
	}
	if harness.Rotated == nil || harness.Retired == nil {
		t.Fatal("webhook key-rotation harness is incomplete")
	}
	t.Run("accepts overlap and rejects retired key", func(t *testing.T) {
		verifyWebhookCase(t, harness.Rotated(t))
		retired := harness.Retired(t)
		_, err := retired.Verifier.VerifyWebhook(context.Background(), retired.Headers, retired.Body)
		var providerErr *provider.Error
		if !errors.As(err, &providerErr) || providerErr.Category != provider.ErrorAuthentication {
			t.Fatalf("retired webhook key error = %#v", err)
		}
	})
}

func verifyWebhookCase(t *testing.T, test WebhookCase) {
	t.Helper()
	if test.Verifier == nil || len(test.Body) == 0 || test.Expected.ProviderEventID == "" {
		t.Fatal("webhook conformance case is incomplete")
	}
	actual, err := test.Verifier.VerifyWebhook(context.Background(), test.Headers, test.Body)
	if err != nil {
		t.Fatalf("VerifyWebhook: %v", err)
	}
	if actual.ProviderEventID != test.Expected.ProviderEventID || actual.Type != test.Expected.Type || actual.ProviderPaymentID != test.Expected.ProviderPaymentID || actual.Status != test.Expected.Status || actual.AmountMinor != test.Expected.AmountMinor || actual.Currency != test.Expected.Currency {
		t.Fatalf("webhook event = %#v, want %#v", actual, test.Expected)
	}
}
