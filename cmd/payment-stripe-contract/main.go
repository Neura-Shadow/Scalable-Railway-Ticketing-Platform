// payment-stripe-contract is a deterministic, read-only Stripe-shaped service
// for disposable provider and settlement evidence. It cannot create a live or
// test-mode financial transaction and refuses to start without an explicit
// test-only acknowledgement.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	stripeprovider "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider/stripe"
)

type contractConfig struct {
	Address          string
	AccountID        string
	APIKey           string
	APIVersion       string
	AdapterOrigin    string
	PageBarrierDelay time.Duration
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	config, err := loadContractConfig(os.Getenv)
	if err != nil {
		logger.Error("stripe contract configuration rejected", "reason", "invalid_test_contract_configuration")
		os.Exit(2)
	}
	handler, err := newHandler(config)
	if err != nil {
		logger.Error("stripe contract configuration rejected", "reason", "invalid_test_contract_configuration")
		os.Exit(2)
	}
	server := &http.Server{
		Addr: config.Address, Handler: handler, ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 15 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("stripe contract stopped", "reason", "http_server_failed")
			os.Exit(1)
		}
	}
}

func loadContractConfig(getenv func(string) string) (contractConfig, error) {
	if getenv == nil || !strings.EqualFold(strings.TrimSpace(getenv("PAYMENT_STRIPE_CONTRACT_TEST_ONLY")), "true") {
		return contractConfig{}, errors.New("explicit test-only acknowledgement required")
	}
	config := contractConfig{
		Address:    strings.TrimSpace(getenv("PAYMENT_STRIPE_CONTRACT_ADDRESS")),
		AccountID:  strings.TrimSpace(getenv("PAYMENT_PROVIDER_ACCOUNT_ID")),
		APIKey:     strings.TrimSpace(getenv("PAYMENT_PROVIDER_API_KEY")),
		APIVersion: strings.TrimSpace(getenv("PAYMENT_PROVIDER_API_VERSION")),
		AdapterOrigin: strings.TrimSpace(
			getenv("PAYMENT_STRIPE_CONTRACT_ADAPTER_ORIGIN"),
		),
	}
	if raw := strings.TrimSpace(getenv("PAYMENT_STRIPE_CONTRACT_PAGE_BARRIER_DELAY")); raw != "" {
		delay, err := time.ParseDuration(raw)
		if err != nil || delay < 0 || delay > 10*time.Second {
			return contractConfig{}, errors.New("invalid deterministic Stripe contract page barrier")
		}
		config.PageBarrierDelay = delay
	}
	if config.Address == "" {
		config.Address = ":8100"
	}
	if config.AdapterOrigin == "" {
		return contractConfig{}, errors.New("explicit adapter origin is required")
	}
	if _, err := newHandler(config); err != nil {
		return contractConfig{}, err
	}
	return config, nil
}

func newHandler(config contractConfig) (http.Handler, error) {
	if config.Address == "" {
		config.Address = ":8100"
	}
	if !strings.HasPrefix(config.AccountID, "acct_") || len(config.AccountID) > 200 ||
		!strings.HasPrefix(config.APIKey, "rk_test_") || len(config.APIKey) < 16 || len(config.APIKey) > 256 ||
		config.APIVersion != stripeprovider.APIVersion {
		return nil, errors.New("invalid deterministic Stripe contract configuration")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writeJSON(writer, http.StatusOK, map[string]any{
			"status": "ready", "provider": "stripe", "mode": "deterministic_test_contract",
			"api_version": config.APIVersion, "mutations_enabled": false,
		})
	})
	mux.HandleFunc("GET /v1/account", func(writer http.ResponseWriter, request *http.Request) {
		if !authenticateContract(request, config) {
			writeAuthenticationError(writer)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"id": config.AccountID, "object": "account"})
	})
	mux.HandleFunc("GET /v1/balance_transactions", func(writer http.ResponseWriter, request *http.Request) {
		if !authenticateContract(request, config) {
			writeAuthenticationError(writer)
			return
		}
		if !validListRequest(request, "data.source") {
			writeRequestError(writer)
			return
		}
		if request.URL.Query().Get("starting_after") == "contract_503" {
			writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": map[string]string{"type": "api_error"}})
			return
		}
		if request.URL.Query().Get("starting_after") == "txn_m7_capture" && config.PageBarrierDelay > 0 {
			timer := time.NewTimer(config.PageBarrierDelay)
			defer timer.Stop()
			select {
			case <-request.Context().Done():
				return
			case <-timer.C:
			}
		}
		writeJSON(writer, http.StatusOK, balanceTransactionFixture(request.URL.Query().Get("starting_after")))
	})
	mux.HandleFunc("GET /v1/payouts", func(writer http.ResponseWriter, request *http.Request) {
		if !authenticateContract(request, config) {
			writeAuthenticationError(writer)
			return
		}
		if !validListRequest(request, "data.balance_transaction") {
			writeRequestError(writer)
			return
		}
		writeJSON(writer, http.StatusOK, payoutFixture(request.URL.Query().Get("starting_after")))
	})
	if config.AdapterOrigin != "" {
		adapter, err := stripeprovider.NewStatusClient(stripeprovider.Config{
			SecretKey: config.APIKey, AccountID: config.AccountID, APIOrigin: config.AdapterOrigin,
			ConnectTimeout: time.Second, RequestTimeout: 5 * time.Second, MaxResponseBodyBytes: 64 << 10,
			AllowInsecureForTest: true,
		})
		if err != nil {
			return nil, errors.New("invalid deterministic Stripe adapter configuration")
		}
		mux.HandleFunc("GET /adapter/balance-transactions", func(writer http.ResponseWriter, request *http.Request) {
			if !authenticateContract(request, config) {
				writeAuthenticationError(writer)
				return
			}
			page, err := adapter.ListBalanceTransactions(request.Context(), stripeprovider.ListOptions{Limit: 1})
			if err != nil || len(page.Items) != 1 {
				writeAdapterFailure(writer, err)
				return
			}
			item := page.Items[0]
			writeJSON(writer, http.StatusOK, map[string]any{
				"adapter": "stripe", "operation": "list_balance_transactions", "result": "succeeded",
				"provider_record_id": item.ID, "gross_minor": item.GrossMinor,
				"fee_minor": item.FeeMinor, "net_minor": item.NetMinor, "currency": item.Currency,
			})
		})
		mux.HandleFunc("GET /adapter/payouts", func(writer http.ResponseWriter, request *http.Request) {
			if !authenticateContract(request, config) {
				writeAuthenticationError(writer)
				return
			}
			page, err := adapter.ListPayouts(request.Context(), stripeprovider.ListOptions{Limit: 1})
			if err != nil || len(page.Items) != 1 {
				writeAdapterFailure(writer, err)
				return
			}
			item := page.Items[0]
			writeJSON(writer, http.StatusOK, map[string]any{
				"adapter": "stripe", "operation": "list_payouts", "result": "succeeded",
				"provider_record_id": item.ID, "amount_minor": item.AmountMinor,
				"currency": item.Currency, "status": item.Status,
			})
		})
		mux.HandleFunc("GET /adapter/error-classification", func(writer http.ResponseWriter, request *http.Request) {
			if !authenticateContract(request, config) {
				writeAuthenticationError(writer)
				return
			}
			_, err := adapter.ListBalanceTransactions(request.Context(), stripeprovider.ListOptions{Limit: 1, StartingAfter: "contract_503"})
			var providerErr *provider.Error
			if !errors.As(err, &providerErr) {
				writeJSON(writer, http.StatusBadGateway, map[string]any{"result": "classification_failed"})
				return
			}
			writeJSON(writer, http.StatusOK, map[string]any{
				"adapter": "stripe", "operation": providerErr.Operation, "result": "classified",
				"category": providerErr.Category, "retryable": providerErr.Retryable,
				"uncertain": providerErr.Uncertain,
			})
		})
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(writer, request)
	}), nil
}

func writeAdapterFailure(writer http.ResponseWriter, err error) {
	var providerErr *provider.Error
	if errors.As(err, &providerErr) {
		writeJSON(writer, http.StatusBadGateway, map[string]any{
			"result": "adapter_failed", "category": providerErr.Category,
			"retryable": providerErr.Retryable, "uncertain": providerErr.Uncertain,
		})
		return
	}
	writeJSON(writer, http.StatusBadGateway, map[string]any{"result": "adapter_failed"})
}

func balanceTransactionFixture(cursor string) map[string]any {
	items := []map[string]any{
		{"id": "txn_m7_capture", "amount": 1000, "fee": 30, "net": 970, "currency": "twd",
			"created": 1786406400, "available_on": 1786492800, "status": "available", "type": "charge",
			"reporting_category": "charge", "source": map[string]any{"id": "ch_m7_capture", "object": "charge", "payment_intent": "pi_m7_capture"}},
		{"id": "txn_m7_refund", "amount": -300, "fee": 0, "net": -300, "currency": "twd",
			"created": 1786406500, "available_on": 1786492900, "status": "available", "type": "refund",
			"reporting_category": "refund", "source": "re_m7_partial"},
		{"id": "txn_m7_payout", "amount": -670, "fee": 0, "net": -670, "currency": "twd",
			"created": 1786406600, "available_on": 1786493000, "status": "available", "type": "payout",
			"reporting_category": "payout", "source": "po_m7_settlement"},
	}
	index := 0
	if cursor == "txn_m7_capture" {
		index = 1
	} else if cursor == "txn_m7_refund" {
		index = 2
	} else if cursor != "" {
		return map[string]any{"object": "list", "url": "/v1/balance_transactions", "has_more": false, "data": []any{}}
	}
	return map[string]any{"object": "list", "url": "/v1/balance_transactions", "has_more": index < len(items)-1, "data": []any{items[index]}}
}

func payoutFixture(cursor string) map[string]any {
	if cursor != "" {
		return map[string]any{"object": "list", "url": "/v1/payouts", "has_more": false, "data": []any{}}
	}
	return map[string]any{"object": "list", "url": "/v1/payouts", "has_more": false, "data": []any{map[string]any{
		"id": "po_m7_settlement", "amount": 670, "currency": "twd", "status": "paid", "automatic": true,
		"created": 1786406600, "arrival_date": 1786493000, "balance_transaction": "txn_m7_payout",
	}}}
}

func authenticateContract(request *http.Request, config contractConfig) bool {
	if request == nil || request.Header.Get("Stripe-Version") != config.APIVersion ||
		request.Header.Get("Stripe-Account") != config.AccountID {
		return false
	}
	want := "Bearer " + config.APIKey
	got := request.Header.Get("Authorization")
	return len(got) == len(want) && subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func validListRequest(request *http.Request, expansion string) bool {
	if request == nil {
		return false
	}
	limit, err := strconv.Atoi(request.URL.Query().Get("limit"))
	if err != nil || limit < 1 || limit > 100 || request.URL.Query().Get("expand[]") != expansion {
		return false
	}
	startingAfter := request.URL.Query().Get("starting_after")
	return len(startingAfter) <= 200 && !strings.ContainsAny(startingAfter, "\r\n")
}

func writeAuthenticationError(writer http.ResponseWriter) {
	writeJSON(writer, http.StatusUnauthorized, map[string]any{"error": map[string]string{"type": "authentication_error"}})
}

func writeRequestError(writer http.ResponseWriter) {
	writeJSON(writer, http.StatusBadRequest, map[string]any{"error": map[string]string{"type": "invalid_request_error"}})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
