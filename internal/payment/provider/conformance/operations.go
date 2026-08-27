package conformance

import (
	"context"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
)

// OperationHarness supplies only provider-specific request construction. The
// shared suite owns the lifecycle, replay, normalized-result, and capability
// assertions. NewClient must return a fresh isolated provider for each call.
type OperationHarness struct {
	NewClient            func(*testing.T) provider.Client
	ValidCreateCheckout  provider.CreateCheckoutRequest
	AuthorizeRequest     func(provider.Checkout) provider.AuthorizeRequest
	CaptureRequest       func(provider.Checkout) provider.CaptureRequest
	VoidRequest          func(provider.Checkout) provider.VoidRequest
	FullRefundRequest    func(provider.Checkout) provider.RefundRequest
	PartialRefundRequest func(provider.Checkout) provider.RefundRequest
	StatusAfterCheckout  []provider.Status
}

// RunOperations executes the provider-neutral lifecycle contract against a
// direct provider implementation or an HTTP adapter backed by a deterministic
// contract server.
func RunOperations(t *testing.T, harness OperationHarness) {
	t.Helper()
	validateOperationHarness(t, harness)

	t.Run("checkout replay preserves one provider identity", func(t *testing.T) {
		client, _ := describedClient(t, harness)
		first := createCheckout(t, client, harness.ValidCreateCheckout)
		replayed := createCheckout(t, client, harness.ValidCreateCheckout)
		if replayed != first {
			t.Fatalf("checkout replay = %#v, want %#v", replayed, first)
		}
	})

	t.Run("status query returns a bounded financial observation", func(t *testing.T) {
		client, descriptor := describedClient(t, harness)
		if !descriptor.Capabilities.PaymentStatusQuery {
			t.Skip("provider does not advertise payment status queries")
		}
		if len(harness.StatusAfterCheckout) == 0 {
			t.Fatal("status-query conformance expectation is missing")
		}
		checkout := createCheckout(t, client, harness.ValidCreateCheckout)
		payment, err := client.GetPaymentStatus(context.Background(), checkout.ProviderPaymentID)
		if err != nil {
			t.Fatalf("GetPaymentStatus: %v", err)
		}
		if payment.ProviderPaymentID != checkout.ProviderPaymentID || payment.AmountMinor != checkout.AmountMinor ||
			payment.Currency != checkout.Currency || payment.ProviderUpdatedAt.IsZero() ||
			!containsStatus(harness.StatusAfterCheckout, payment.Status) ||
			provider.EvaluateFinancialObservation(
				provider.FinancialExpectation{AmountMinor: checkout.AmountMinor, Currency: checkout.Currency},
				provider.FinancialObservation{
					Status: payment.Status, AmountMinor: payment.AmountMinor, Currency: payment.Currency,
					CapturedMinor: payment.CapturedMinor, RefundedMinor: payment.RefundedMinor,
				},
			) != nil {
			t.Fatalf("payment observation = %#v", payment)
		}
	})

	t.Run("authorization replay preserves one operation", func(t *testing.T) {
		client, descriptor := describedClient(t, harness)
		if !descriptor.Capabilities.Authorize {
			t.Skip("provider does not advertise authorization")
		}
		requireOperationBuilder(t, "authorization", harness.AuthorizeRequest != nil)
		checkout := createCheckout(t, client, harness.ValidCreateCheckout)
		request := harness.AuthorizeRequest(checkout)
		first := authorize(t, client, request)
		replayed := authorize(t, client, request)
		assertOperationReplay(t, first, replayed, provider.StatusAuthorized, request.AmountMinor, request.Currency)
	})

	t.Run("capture replay does not create another operation", func(t *testing.T) {
		client, descriptor := describedClient(t, harness)
		if !descriptor.Capabilities.Capture {
			t.Skip("provider does not advertise capture")
		}
		requireOperationBuilder(t, "capture", harness.AuthorizeRequest != nil && harness.CaptureRequest != nil)
		checkout := createAuthorized(t, client, harness)
		request := harness.CaptureRequest(checkout)
		first, err := client.Capture(context.Background(), request)
		if err != nil {
			t.Fatalf("Capture: %v", err)
		}
		replayed, err := client.Capture(context.Background(), request)
		if err != nil {
			t.Fatalf("Capture replay: %v", err)
		}
		assertOperationReplay(t, first, replayed, provider.StatusCaptured, request.AmountMinor, request.Currency)
	})

	t.Run("void replay does not create another operation", func(t *testing.T) {
		client, descriptor := describedClient(t, harness)
		if !descriptor.Capabilities.Void {
			t.Skip("provider does not advertise void")
		}
		requireOperationBuilder(t, "void", harness.AuthorizeRequest != nil && harness.VoidRequest != nil)
		checkout := createAuthorized(t, client, harness)
		request := harness.VoidRequest(checkout)
		first, err := client.Void(context.Background(), request)
		if err != nil {
			t.Fatalf("Void: %v", err)
		}
		replayed, err := client.Void(context.Background(), request)
		if err != nil {
			t.Fatalf("Void replay: %v", err)
		}
		assertOperationReplay(t, first, replayed, provider.StatusVoided, checkout.AmountMinor, checkout.Currency)
	})

	t.Run("full refund replay preserves one operation", func(t *testing.T) {
		client, descriptor := describedClient(t, harness)
		if !descriptor.Capabilities.FullRefund {
			t.Skip("provider does not advertise full refunds")
		}
		requireOperationBuilder(t, "full refund", harness.AuthorizeRequest != nil && harness.CaptureRequest != nil && harness.FullRefundRequest != nil)
		checkout := createCaptured(t, client, harness)
		request := harness.FullRefundRequest(checkout)
		assertRefundReplay(t, client, request)
	})

	t.Run("partial refund follows the advertised capability", func(t *testing.T) {
		client, descriptor := describedClient(t, harness)
		if !descriptor.Capabilities.PartialRefund {
			t.Skip("provider does not advertise partial refunds")
		}
		requireOperationBuilder(t, "partial refund", harness.AuthorizeRequest != nil && harness.CaptureRequest != nil && harness.PartialRefundRequest != nil)
		checkout := createCaptured(t, client, harness)
		request := harness.PartialRefundRequest(checkout)
		if request.AmountMinor <= 0 || request.AmountMinor >= checkout.AmountMinor {
			t.Fatal("partial refund request must be a positive amount below the capture")
		}
		refunded := assertRefundReplay(t, client, request)
		reader, ok := client.(provider.RefundLookupReader)
		if !ok {
			t.Fatal("advertised partial refund capability has no exact refund lookup")
		}
		lookup, err := reader.LookupRefund(context.Background(), provider.RefundLookupRequest{
			PaymentIntentID: request.PaymentIntentID, ProviderPaymentID: request.ProviderPaymentID,
			AmountMinor: request.AmountMinor, Currency: request.Currency, IdempotencyKey: request.IdempotencyKey,
			Metadata: request.Metadata, Limit: 100,
		})
		if err != nil || !lookup.Found || !lookup.Definitive || lookup.Refund != refunded {
			t.Fatalf("exact refund lookup = %#v, %v; want %#v", lookup, err, refunded)
		}
	})
}

func validateOperationHarness(t *testing.T, harness OperationHarness) {
	t.Helper()
	if harness.NewClient == nil || harness.ValidCreateCheckout.AmountMinor <= 0 || harness.ValidCreateCheckout.Currency == "" {
		t.Fatal("operation conformance harness is incomplete")
	}
}

func requireOperationBuilder(t *testing.T, operation string, available bool) {
	t.Helper()
	if !available {
		t.Fatalf("%s conformance builder is missing for an advertised capability", operation)
	}
}

func describedClient(t *testing.T, harness OperationHarness) (provider.Client, provider.Descriptor) {
	t.Helper()
	client := harness.NewClient(t)
	if client == nil {
		t.Fatal("operation conformance client is nil")
	}
	described, ok := client.(provider.Described)
	if !ok {
		t.Fatal("operation conformance client has no descriptor")
	}
	descriptor := described.Descriptor()
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("provider descriptor: %v", err)
	}
	return client, descriptor
}

func createCheckout(t *testing.T, client provider.Client, request provider.CreateCheckoutRequest) provider.Checkout {
	t.Helper()
	checkout, err := client.CreateCheckout(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateCheckout: %v", err)
	}
	if checkout.ProviderPaymentID == "" || checkout.HostedReference == "" || checkout.AmountMinor != request.AmountMinor ||
		checkout.Currency != request.Currency {
		t.Fatalf("checkout = %#v", checkout)
	}
	return checkout
}

func createAuthorized(t *testing.T, client provider.Client, harness OperationHarness) provider.Checkout {
	t.Helper()
	checkout := createCheckout(t, client, harness.ValidCreateCheckout)
	authorize(t, client, harness.AuthorizeRequest(checkout))
	return checkout
}

func createCaptured(t *testing.T, client provider.Client, harness OperationHarness) provider.Checkout {
	t.Helper()
	checkout := createAuthorized(t, client, harness)
	request := harness.CaptureRequest(checkout)
	result, err := client.Capture(context.Background(), request)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	assertOperation(t, result, provider.StatusCaptured, request.AmountMinor, request.Currency)
	return checkout
}

func authorize(t *testing.T, client provider.Client, request provider.AuthorizeRequest) provider.OperationResult {
	t.Helper()
	result, err := client.Authorize(context.Background(), request)
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	assertOperation(t, result, provider.StatusAuthorized, request.AmountMinor, request.Currency)
	return result
}

func assertRefundReplay(t *testing.T, client provider.Client, request provider.RefundRequest) provider.OperationResult {
	t.Helper()
	first, err := client.Refund(context.Background(), request)
	if err != nil {
		t.Fatalf("Refund: %v", err)
	}
	replayed, err := client.Refund(context.Background(), request)
	if err != nil {
		t.Fatalf("Refund replay: %v", err)
	}
	assertOperationReplay(t, first, replayed, provider.StatusRefunded, request.AmountMinor, request.Currency)
	return first
}

func assertOperationReplay(t *testing.T, first, replayed provider.OperationResult, status provider.Status, amount int64, currency string) {
	t.Helper()
	assertOperation(t, first, status, amount, currency)
	if replayed != first {
		t.Fatalf("operation replay = %#v, want %#v", replayed, first)
	}
}

func assertOperation(t *testing.T, result provider.OperationResult, status provider.Status, amount int64, currency string) {
	t.Helper()
	if result.ProviderPaymentID == "" || result.ProviderOperationID == "" || result.Status != status ||
		result.AmountMinor != amount || result.Currency != currency {
		t.Fatalf("operation = %#v", result)
	}
}

func containsStatus(statuses []provider.Status, value provider.Status) bool {
	for _, status := range statuses {
		if status == value {
			return true
		}
	}
	return false
}
