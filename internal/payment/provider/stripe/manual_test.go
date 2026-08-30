//go:build stripe_test

package stripe_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	stripeprovider "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/stripe"
)

// TestManualStripeTestModeStatusReadOnly is intentionally read-only. The
// protected workflow supplies an existing test-mode PaymentIntent fixture.
func TestManualStripeTestModeStatusReadOnly(t *testing.T) {
	secretKey := os.Getenv("STRIPE_TEST_SECRET_KEY")
	accountID := os.Getenv("STRIPE_TEST_ACCOUNT_ID")
	paymentIntentID := os.Getenv("STRIPE_TEST_PAYMENT_INTENT_ID")
	if secretKey == "" || accountID == "" || paymentIntentID == "" {
		t.Skip("Stripe test-mode fixture secrets are not configured")
	}
	if !strings.HasPrefix(secretKey, "sk_test_") {
		t.Fatal("manual Stripe test refuses any non-test secret key")
	}
	if !strings.HasPrefix(accountID, "acct_") || !strings.HasPrefix(paymentIntentID, "pi_") {
		t.Fatal("manual Stripe test fixture identifiers are invalid")
	}
	client, err := stripeprovider.New(stripeprovider.Config{
		SecretKey: secretKey, AccountID: accountID, APIOrigin: "https://api.stripe.com",
		SuccessURL: "https://manual-test.invalid/success", CancelURL: "https://manual-test.invalid/cancel",
		ConnectTimeout: 5 * time.Second, RequestTimeout: 15 * time.Second, MaxResponseBodyBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	payment, err := client.GetPaymentStatus(context.Background(), paymentIntentID)
	if err != nil {
		t.Fatalf("read-only test-mode status: %v", err)
	}
	if payment.ProviderPaymentID != paymentIntentID || payment.AmountMinor <= 0 || payment.Currency == "" {
		t.Fatalf("invalid normalized test-mode payment: %#v", payment)
	}
}
