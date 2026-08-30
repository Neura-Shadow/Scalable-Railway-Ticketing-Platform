package main

import (
	"testing"
	"time"

	paymentwebhook "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/webhook"
)

func TestStripeWebhookSecretDeadlinesBindAcceptedKeyGrace(t *testing.T) {
	t.Parallel()
	deadline := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	plan := paymentwebhook.KeyringPlan{ByID: map[string]paymentwebhook.KeyVersion{
		"current":  {KeyID: "current", State: paymentwebhook.KeyPrimary, ActivatedAt: deadline.Add(-time.Hour)},
		"previous": {KeyID: "previous", State: paymentwebhook.KeyAccepted, ActivatedAt: deadline.Add(-time.Hour), RetirementNotBefore: &deadline},
	}}
	deadlines, err := stripeWebhookSecretDeadlines([]string{"current", "previous"}, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !deadlines[0].IsZero() || !deadlines[1].Equal(deadline) {
		t.Fatalf("deadlines = %#v", deadlines)
	}
}

func TestStripeWebhookSecretDeadlinesRejectRetiredConfiguredKey(t *testing.T) {
	t.Parallel()
	retiredAt := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	plan := paymentwebhook.KeyringPlan{ByID: map[string]paymentwebhook.KeyVersion{
		"current":  {KeyID: "current", State: paymentwebhook.KeyPrimary, ActivatedAt: retiredAt.Add(-time.Hour)},
		"previous": {KeyID: "previous", State: paymentwebhook.KeyRetired, ActivatedAt: retiredAt.Add(-time.Hour), RetiredAt: &retiredAt},
	}}
	if _, err := stripeWebhookSecretDeadlines([]string{"current", "previous"}, plan); err == nil {
		t.Fatal("retired configured key was accepted")
	}
}
