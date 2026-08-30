package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
)

func TestStripeSourceUsesPinnedReadOnlyRequestAndNormalizesBalanceEvidence(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/v1/balance_transactions" ||
			request.URL.Query().Get("limit") != "2" || request.URL.Query().Get("expand[]") != "data.source" ||
			request.Header.Get("Stripe-Version") != stripeAPIVersion ||
			request.Header.Get("Stripe-Account") != "acct_ops" || request.Header.Get("Authorization") != "Bearer rk_test_contract" {
			t.Error("unexpected settlement provider request metadata")
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		fmt.Fprint(writer, `{"data":[{"id":"txn_1","amount":1000,"fee":30,"net":970,"currency":"twd","available_on":1786413600,"created":1786410000,"source":{"id":"ch_1","object":"charge","payment_intent":"pi_1"},"type":"charge","reporting_category":"charge","status":"available"}],"has_more":false}`)
	}))
	defer server.Close()

	source, err := newStripeSource(stripeSourceConfig{
		BaseURL: server.URL, APIKey: "rk_test_contract", APIVersion: stripeAPIVersion, AccountID: "acct_ops",
		ConnectTimeout: time.Second, RequestTimeout: 2 * time.Second, MaxResponseBytes: 4096,
		AllowInsecureForTest: true, Now: func() time.Time { return time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := source.ListPage(context.Background(), settlement.AccountScope{Provider: "stripe", AccountID: "acct_ops"}, "", 2)
	if err != nil || len(page.Records) != 1 || page.NextCursor != "p:" || page.Done {
		t.Fatalf("ListPage() = (%+v, %v)", page, err)
	}
	record := page.Records[0]
	if record.Operation != settlement.OperationCapture || record.GrossMinor != 1000 || record.FeeMinor != 30 || record.NetMinor != 970 || record.Currency != "TWD" || record.PaymentCorrelation != "pi_1" {
		t.Fatalf("normalized record = %+v", record)
	}
}

func TestStripeSourcePreservesExactRefundIdentityForPartialRefunds(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/v1/balance_transactions" {
			t.Errorf("unexpected refund evidence request %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		fmt.Fprint(writer, `{"data":[
			{"id":"txn_refund_1","amount":-300,"fee":0,"net":-300,"currency":"twd","available_on":1786413600,"created":1786410000,"source":{"id":"re_first","object":"refund","payment_intent":"pi_shared"},"type":"refund","reporting_category":"refund","status":"available"},
			{"id":"txn_refund_2","amount":-200,"fee":0,"net":-200,"currency":"twd","available_on":1786413601,"created":1786410001,"source":{"id":"re_second","object":"refund","payment_intent":"pi_shared"},"type":"refund","reporting_category":"refund","status":"available"}
		],"has_more":false}`)
	}))
	defer server.Close()

	source, err := newStripeSource(stripeSourceConfig{
		BaseURL: server.URL, APIKey: "rk_test_contract", APIVersion: stripeAPIVersion, AccountID: "acct_ops",
		ConnectTimeout: time.Second, RequestTimeout: 2 * time.Second, MaxResponseBytes: 8192,
		AllowInsecureForTest: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := source.ListPage(context.Background(), settlement.AccountScope{Provider: "stripe", AccountID: "acct_ops"}, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 2 || page.Records[0].PaymentCorrelation != "re_first" || page.Records[1].PaymentCorrelation != "re_second" {
		t.Fatalf("refund correlations = %+v; want exact provider refund identities", page.Records)
	}
}

func TestStripeSourceRejectsMismatchedAccountScopeBeforeNetwork(t *testing.T) {
	t.Parallel()

	source, err := newStripeSource(stripeSourceConfig{
		BaseURL: "https://api.stripe.com", APIKey: "rk_test_contract", APIVersion: stripeAPIVersion, AccountID: "acct_ops",
		ConnectTimeout: time.Second, RequestTimeout: 2 * time.Second, MaxResponseBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.ListPage(context.Background(), settlement.AccountScope{Provider: "stripe", AccountID: "acct_other"}, "", 1)
	if !errors.Is(err, errSettlementSource) {
		t.Fatalf("ListPage() error = %v, want provider source rejection", err)
	}
}

func TestStripeSourceRequiresSettlementAndPayoutCapabilities(t *testing.T) {
	t.Parallel()

	source, err := newStripeSource(stripeSourceConfig{
		BaseURL: "https://api.stripe.com", APIKey: "rk_test_contract", APIVersion: stripeAPIVersion, AccountID: "acct_ops",
		ConnectTimeout: time.Second, RequestTimeout: 2 * time.Second, MaxResponseBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := source.reader.Descriptor()
	if descriptor.Name != "stripe" || !descriptor.Capabilities.SettlementTransactions || !descriptor.Capabilities.PayoutReports {
		t.Fatalf("descriptor = %#v", descriptor)
	}
}
