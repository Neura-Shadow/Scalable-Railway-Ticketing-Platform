package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	stripeprovider "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/stripe"
)

func TestContractReadinessIsPublicBoundedAndIdentifiesTestMode(t *testing.T) {
	t.Parallel()

	handler, err := newHandler(contractConfig{AccountID: "acct_contract", APIKey: "rk_test_contract", APIVersion: stripeprovider.APIVersion})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusOK || response.Body.Len() > 65536 || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("ready response code=%d bytes=%d cache=%q", response.Code, response.Body.Len(), response.Header().Get("Cache-Control"))
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["provider"] != "stripe" || body["mode"] != "deterministic_test_contract" || body["api_version"] != stripeprovider.APIVersion {
		t.Fatalf("ready body = %#v", body)
	}
}

func TestContractSupportsSelectedAdapterReadinessAndDeterministicSettlementPages(t *testing.T) {
	t.Parallel()

	handler, err := newHandler(contractConfig{AccountID: "acct_contract", APIKey: "rk_test_contract", APIVersion: stripeprovider.APIVersion})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := stripeprovider.NewStatusClient(stripeprovider.Config{
		SecretKey: "rk_test_contract", AccountID: "acct_contract", APIOrigin: server.URL,
		ConnectTimeout: time.Second, RequestTimeout: time.Second, MaxResponseBodyBytes: 64 << 10,
		AllowInsecureForTest: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)
	if err := client.Ready(context.Background()); err != nil {
		t.Fatalf("Ready() = %v", err)
	}
	transactions, err := client.ListBalanceTransactions(context.Background(), stripeprovider.ListOptions{Limit: 1})
	if err != nil || len(transactions.Items) != 1 || !transactions.HasMore || transactions.Items[0].ID != "txn_m7_capture" {
		t.Fatalf("balance page=%+v err=%v", transactions, err)
	}
	second, err := client.ListBalanceTransactions(context.Background(), stripeprovider.ListOptions{Limit: 1, StartingAfter: transactions.NextStartingAfter})
	if err != nil || len(second.Items) != 1 || !second.HasMore || second.Items[0].ID != "txn_m7_refund" {
		t.Fatalf("second balance page=%+v err=%v", second, err)
	}
	third, err := client.ListBalanceTransactions(context.Background(), stripeprovider.ListOptions{Limit: 1, StartingAfter: second.NextStartingAfter})
	if err != nil || len(third.Items) != 1 || third.HasMore || third.Items[0].ID != "txn_m7_payout" {
		t.Fatalf("third balance page=%+v err=%v", third, err)
	}
	payouts, err := client.ListPayouts(context.Background(), stripeprovider.ListOptions{Limit: 1})
	if err != nil || len(payouts.Items) != 1 || payouts.HasMore || payouts.Items[0].ID != "po_m7_settlement" {
		t.Fatalf("payout page=%+v err=%v", payouts, err)
	}
}

func TestContractRejectsMissingOrWrongCredentialWithoutDisclosure(t *testing.T) {
	t.Parallel()

	handler, err := newHandler(contractConfig{AccountID: "acct_contract", APIKey: "rk_test_contract", APIVersion: stripeprovider.APIVersion})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"", "wrong"} {
		request := httptest.NewRequest(http.MethodGet, "/v1/account", nil)
		if key != "" {
			request.Header.Set("Authorization", "Bearer "+key)
		}
		request.Header.Set("Stripe-Version", stripeprovider.APIVersion)
		request.Header.Set("Stripe-Account", "acct_contract")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized || response.Body.String() != "{\"error\":{\"type\":\"authentication_error\"}}\n" {
			t.Fatalf("key=%q code=%d body=%q", key, response.Code, response.Body.String())
		}
	}
}

func TestContractPageBarrierIsExplicitAndBounded(t *testing.T) {
	values := map[string]string{
		"PAYMENT_STRIPE_CONTRACT_TEST_ONLY":          "true",
		"PAYMENT_PROVIDER_ACCOUNT_ID":                "acct_contract",
		"PAYMENT_PROVIDER_API_KEY":                   "rk_test_contract",
		"PAYMENT_PROVIDER_API_VERSION":               stripeprovider.APIVersion,
		"PAYMENT_STRIPE_CONTRACT_ADAPTER_ORIGIN":     "http://stripe-contract.test:8100",
		"PAYMENT_STRIPE_CONTRACT_PAGE_BARRIER_DELAY": "8s",
	}
	config, err := loadContractConfig(func(name string) string { return values[name] })
	if err != nil || config.PageBarrierDelay != 8*time.Second {
		t.Fatalf("loadContractConfig() delay=%s err=%v", config.PageBarrierDelay, err)
	}
	values["PAYMENT_STRIPE_CONTRACT_PAGE_BARRIER_DELAY"] = "11s"
	if _, err := loadContractConfig(func(name string) string { return values[name] }); err == nil {
		t.Fatal("loadContractConfig() accepted an unbounded page barrier")
	}
}

func TestAdapterProbeTraversesStripeNormalizationAndErrorClassification(t *testing.T) {
	t.Parallel()

	fixture, err := newHandler(contractConfig{
		AccountID: "acct_contract", APIKey: "rk_test_contract", APIVersion: stripeprovider.APIVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixtureServer := httptest.NewServer(fixture)
	t.Cleanup(fixtureServer.Close)
	probe, err := newHandler(contractConfig{
		AccountID: "acct_contract", APIKey: "rk_test_contract", APIVersion: stripeprovider.APIVersion,
		AdapterOrigin: fixtureServer.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		path string
		want func(map[string]any) bool
	}{
		{path: "/adapter/balance-transactions", want: func(body map[string]any) bool {
			return body["adapter"] == "stripe" && body["provider_record_id"] == "txn_m7_capture" && body["currency"] == "TWD"
		}},
		{path: "/adapter/payouts", want: func(body map[string]any) bool {
			return body["provider_record_id"] == "po_m7_settlement" && body["status"] == "paid"
		}},
		{path: "/adapter/error-classification", want: func(body map[string]any) bool {
			return body["category"] == "provider_unavailable" && body["retryable"] == true && body["uncertain"] == false
		}},
	} {
		request := httptest.NewRequest(http.MethodGet, test.path, nil)
		request.Header.Set("Authorization", "Bearer rk_test_contract")
		request.Header.Set("Stripe-Version", stripeprovider.APIVersion)
		request.Header.Set("Stripe-Account", "acct_contract")
		response := httptest.NewRecorder()
		probe.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s code=%d body=%q", test.path, response.Code, response.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || !test.want(body) {
			t.Fatalf("%s body=%#v err=%v", test.path, body, err)
		}
	}
}
