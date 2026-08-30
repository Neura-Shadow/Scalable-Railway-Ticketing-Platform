// Package stripe implements the payment provider boundary against a fixed
// Stripe account and API origin.
package stripe

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	sdkstripe "github.com/stripe/stripe-go/v86"
)

const (
	APIVersion                 = sdkstripe.APIVersion
	defaultRequestTimeout      = 15 * time.Second
	defaultConnectTimeout      = 5 * time.Second
	defaultMaxResponseBodySize = int64(1 << 20)
	defaultWebhookTolerance    = 5 * time.Minute
	maximumWebhookSecrets      = 2
)

// Config contains only explicit, immutable Stripe connectivity values. The
// adapter never reads credentials, proxy settings, or endpoints from the
// process environment.
type Config struct {
	SecretKey             string
	AccountID             string
	APIOrigin             string
	SuccessURL            string
	CancelURL             string
	RequestTimeout        time.Duration
	ConnectTimeout        time.Duration
	MaxResponseBodyBytes  int64
	MaxWebhookBodyBytes   int64
	WebhookTolerance      time.Duration
	WebhookSecrets        []string
	WebhookSecretIDs      []string
	WebhookSecretNotAfter []time.Time
	Now                   func() time.Time
	AllowInsecureForTest  bool
}

// Client is a Stripe-backed provider client.
type Client struct {
	secretKey        string
	accountID        string
	origin           *url.URL
	successURL       string
	cancelURL        string
	http             *http.Client
	maxBody          int64
	maxWebhook       int64
	tolerance        time.Duration
	secrets          [][]byte
	secretIDs        []string
	secretNotAfter   []time.Time
	webhookAccountID string
	webhookLiveMode  bool
	webhookBinding   bool
	now              func() time.Time
}

func (client *Client) Descriptor() provider.Descriptor {
	return Descriptor()
}

func Descriptor() provider.Descriptor {
	return provider.Descriptor{
		Name: "stripe", APIVersion: APIVersion,
		Capabilities: provider.CapabilitySet{
			HostedCheckout: true, Authorize: true, Capture: true, Void: true,
			FullRefund: true, PartialRefund: true, PaymentStatusQuery: true,
			SettlementTransactions: true, PayoutReports: true,
			WebhookSignatures: true, WebhookKeyRotation: true,
		},
	}
}

// New constructs an isolated Stripe client with redirects and ambient proxy
// discovery disabled.
func New(config Config) (*Client, error) {
	return newClient(config, true, false)
}

// NewStatusClient constructs the read-side adapter used by reconciliation.
// Checkout creation remains disabled even though the returned concrete client
// implements the wider provider contract.
func NewStatusClient(config Config) (*Client, error) {
	return newClient(config, false, true)
}

func newClient(config Config, checkoutEnabled, restrictedReadOnly bool) (*Client, error) {
	if sdkstripe.APIVersion != "2026-07-29.dahlia" {
		return nil, errors.New("stripe SDK API version is not supported")
	}
	if (restrictedReadOnly && !validCredential(config.SecretKey, "rk_")) ||
		(!restrictedReadOnly && !validCredential(config.SecretKey, "sk_", "rk_")) {
		return nil, errors.New("stripe secret key is required")
	}
	if config.AccountID != "" && !validIdentifier(config.AccountID, "acct_") {
		return nil, errors.New("stripe account identifier is invalid")
	}
	origin, err := parseOrigin(config.APIOrigin, config.AllowInsecureForTest)
	if err != nil {
		return nil, err
	}
	if checkoutEnabled {
		if err := validateHTTPSURL(config.SuccessURL); err != nil {
			return nil, fmt.Errorf("stripe success URL: %w", err)
		}
		if err := validateHTTPSURL(config.CancelURL); err != nil {
			return nil, fmt.Errorf("stripe cancel URL: %w", err)
		}
	} else {
		config.SuccessURL = ""
		config.CancelURL = ""
	}
	requestTimeout := config.RequestTimeout
	if requestTimeout == 0 {
		requestTimeout = defaultRequestTimeout
	}
	connectTimeout := config.ConnectTimeout
	if connectTimeout == 0 {
		connectTimeout = defaultConnectTimeout
	}
	maxBody := config.MaxResponseBodyBytes
	if maxBody == 0 {
		maxBody = defaultMaxResponseBodySize
	}
	maxWebhook := config.MaxWebhookBodyBytes
	if maxWebhook == 0 {
		maxWebhook = defaultMaxResponseBodySize
	}
	tolerance := config.WebhookTolerance
	if tolerance == 0 {
		tolerance = defaultWebhookTolerance
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	if requestTimeout < time.Millisecond || requestTimeout > time.Minute || connectTimeout < time.Millisecond || connectTimeout > 30*time.Second || maxBody < 1024 || maxBody > 8<<20 || maxWebhook < 1024 || maxWebhook > 8<<20 || tolerance < time.Second || tolerance > 15*time.Minute {
		return nil, errors.New("stripe transport limits are invalid")
	}
	if len(config.WebhookSecrets) > maximumWebhookSecrets {
		return nil, errors.New("stripe webhook secret rotation exceeds limit")
	}
	if len(config.WebhookSecretIDs) != 0 && len(config.WebhookSecretIDs) != len(config.WebhookSecrets) {
		return nil, errors.New("stripe webhook secret identities are invalid")
	}
	if len(config.WebhookSecretNotAfter) != 0 && len(config.WebhookSecretNotAfter) != len(config.WebhookSecrets) {
		return nil, errors.New("stripe webhook secret retirement metadata is invalid")
	}
	secrets := make([][]byte, 0, len(config.WebhookSecrets))
	secretIDs := make([]string, 0, len(config.WebhookSecrets))
	secretNotAfter := make([]time.Time, len(config.WebhookSecrets))
	seenSecrets := make(map[string]struct{}, len(config.WebhookSecrets))
	seenSecretIDs := make(map[string]struct{}, len(config.WebhookSecrets))
	for index, secret := range config.WebhookSecrets {
		if !validCredential(secret, "whsec_") {
			return nil, errors.New("stripe webhook secret is invalid")
		}
		if _, exists := seenSecrets[secret]; exists {
			return nil, errors.New("stripe webhook secrets must be unique")
		}
		seenSecrets[secret] = struct{}{}
		secrets = append(secrets, []byte(secret))
		keyID := "stripe-primary"
		if index == 1 {
			keyID = "stripe-previous"
		}
		if len(config.WebhookSecretIDs) != 0 {
			keyID = config.WebhookSecretIDs[index]
		}
		if !validIdentifier(keyID, "") {
			return nil, errors.New("stripe webhook secret identity is invalid")
		}
		if _, exists := seenSecretIDs[keyID]; exists {
			return nil, errors.New("stripe webhook secret identities must be unique")
		}
		seenSecretIDs[keyID] = struct{}{}
		secretIDs = append(secretIDs, keyID)
		if len(config.WebhookSecretNotAfter) != 0 {
			secretNotAfter[index] = config.WebhookSecretNotAfter[index].UTC()
		}
	}
	dialer := &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   connectTimeout,
		ResponseHeaderTimeout: requestTimeout,
		ExpectContinueTimeout: time.Second,
	}
	return &Client{
		secretKey:  config.SecretKey,
		accountID:  config.AccountID,
		origin:     origin,
		successURL: config.SuccessURL,
		cancelURL:  config.CancelURL,
		http: &http.Client{
			Transport: transport,
			Timeout:   requestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		maxBody:          maxBody,
		maxWebhook:       maxWebhook,
		tolerance:        tolerance,
		secrets:          secrets,
		secretIDs:        secretIDs,
		secretNotAfter:   secretNotAfter,
		webhookAccountID: config.AccountID,
		webhookLiveMode:  strings.Contains(config.SecretKey, "_live_"),
		webhookBinding:   config.AccountID != "",
		now:              now,
	}, nil
}

// Ready verifies that the configured credential can read the exact Stripe
// account bound to this client. It performs no financial mutation.
func (c *Client) Ready(ctx context.Context) error {
	var account struct {
		ID string `json:"id"`
	}
	if _, err := c.doForm(ctx, http.MethodGet, "/v1/account", "ready", "", nil, &account); err != nil {
		return err
	}
	if !validIdentifier(account.ID, "acct_") || (c.accountID != "" && account.ID != c.accountID) {
		return inconsistentError("ready")
	}
	return nil
}

// CloseIdleConnections releases reusable provider connections during graceful
// process shutdown.
func (c *Client) CloseIdleConnections() {
	if c != nil && c.http != nil {
		c.http.CloseIdleConnections()
	}
}

// CreateCheckout creates a hosted Checkout Session whose PaymentIntent uses
// manual capture. The returned session ID remains an opaque hosted reference;
// the PaymentIntent ID is the provider payment identity.
func (c *Client) CreateCheckout(ctx context.Context, request provider.CreateCheckoutRequest) (provider.Checkout, error) {
	if c == nil || c.successURL == "" || c.cancelURL == "" || request.AmountMinor <= 0 || !validCurrency(request.Currency) || !validIdentifier(request.PaymentIntentID, "") || !validIdentifier(request.MerchantReference, "") || !validIdempotencyKey(request.IdempotencyKey) {
		return provider.Checkout{}, validationError("create_checkout")
	}
	if !validStripeMetadata(request.Metadata, "payment_intent_id") {
		return provider.Checkout{}, validationError("create_checkout")
	}
	form := url.Values{
		"mode":                                             {"payment"},
		"payment_method_types[0]":                          {"card"},
		"payment_intent_data[capture_method]":              {"manual"},
		"line_items[0][quantity]":                          {"1"},
		"line_items[0][price_data][currency]":              {strings.ToLower(request.Currency)},
		"line_items[0][price_data][unit_amount]":           {strconv.FormatInt(request.AmountMinor, 10)},
		"line_items[0][price_data][product_data][name]":    {"Railway ticket"},
		"client_reference_id":                              {request.MerchantReference},
		"success_url":                                      {c.successURL},
		"cancel_url":                                       {c.cancelURL},
		"payment_intent_data[metadata][payment_intent_id]": {request.PaymentIntentID},
	}
	for key, value := range request.Metadata {
		form.Set("payment_intent_data[metadata]["+key+"]", value)
	}

	var session sdkstripe.CheckoutSession
	if _, err := c.doForm(ctx, http.MethodPost, "/v1/checkout/sessions", "create_checkout", request.IdempotencyKey, form, &session); err != nil {
		return provider.Checkout{}, err
	}
	if session.PaymentIntent == nil || !validIdentifier(session.PaymentIntent.ID, "pi_") || !validIdentifier(session.ID, "cs_") || session.URL == "" || session.AmountTotal != request.AmountMinor || strings.ToUpper(string(session.Currency)) != request.Currency {
		return provider.Checkout{}, inconsistentError("create_checkout")
	}
	return provider.Checkout{
		ProviderPaymentID: session.PaymentIntent.ID,
		HostedReference:   session.ID,
		SyntheticToken:    session.ID,
		Status:            provider.StatusCreated,
		AmountMinor:       session.AmountTotal,
		Currency:          strings.ToUpper(string(session.Currency)),
	}, nil
}

// GetPaymentStatus returns a normalized financial snapshot. A partial capture
// or partial refund is intentionally StatusUnknown: its totals remain useful
// evidence, but it cannot be mistaken for a full terminal transition.
func (c *Client) GetPaymentStatus(ctx context.Context, providerPaymentID string) (provider.Payment, error) {
	if !validIdentifier(providerPaymentID, "pi_") {
		return provider.Payment{}, validationError("get_payment_status")
	}
	var intent sdkstripe.PaymentIntent
	if _, err := c.doForm(ctx, http.MethodGet, "/v1/payment_intents/"+providerPaymentID, "get_payment_status", "", url.Values{"expand[]": {"latest_charge"}}, &intent); err != nil {
		return provider.Payment{}, err
	}
	payment, err := normalizePaymentIntent(&intent)
	if err != nil {
		return provider.Payment{}, inconsistentError("get_payment_status")
	}
	return payment, nil
}

// Capture captures the full server-owned amount for a manual-capture
// PaymentIntent. Stripe's Request-Id is persisted as the provider operation
// identity while the caller's stable key controls Stripe idempotency.
func (c *Client) Capture(ctx context.Context, request provider.CaptureRequest) (provider.OperationResult, error) {
	if request.AmountMinor <= 0 || !validCurrency(request.Currency) || !validIdentifier(request.PaymentIntentID, "") || !validIdentifier(request.ProviderPaymentID, "pi_") || !validIdempotencyKey(request.IdempotencyKey) || !validStripeMetadata(request.Metadata) {
		return provider.OperationResult{}, validationError("capture")
	}
	form := url.Values{
		"amount_to_capture": {strconv.FormatInt(request.AmountMinor, 10)},
		"expand[]":          {"latest_charge"},
	}
	for key, value := range request.Metadata {
		form.Set("metadata["+key+"]", value)
	}
	var intent sdkstripe.PaymentIntent
	requestID, err := c.doForm(ctx, http.MethodPost, "/v1/payment_intents/"+request.ProviderPaymentID+"/capture", "capture", request.IdempotencyKey, form, &intent)
	if err != nil {
		return provider.OperationResult{}, err
	}
	payment, err := normalizePaymentIntent(&intent)
	if err != nil || payment.ProviderPaymentID != request.ProviderPaymentID || payment.Status != provider.StatusCaptured || payment.AmountMinor != request.AmountMinor || payment.Currency != request.Currency || !validIdentifier(requestID, "req_") {
		return provider.OperationResult{}, inconsistentError("capture")
	}
	return provider.OperationResult{
		ProviderPaymentID:   payment.ProviderPaymentID,
		ProviderOperationID: requestID,
		Status:              payment.Status,
		AmountMinor:         payment.AmountMinor,
		Currency:            payment.Currency,
	}, nil
}

// Authorize observes the PaymentIntent created and confirmed by hosted
// Checkout. It never creates or confirms a second PaymentIntent.
func (c *Client) Authorize(ctx context.Context, request provider.AuthorizeRequest) (provider.OperationResult, error) {
	if request.AmountMinor <= 0 || !validCurrency(request.Currency) || !validIdentifier(request.PaymentIntentID, "") || !validIdentifier(request.ProviderPaymentID, "pi_") || !validIdentifier(request.SyntheticToken, "cs_") || !validIdempotencyKey(request.IdempotencyKey) || !validStripeMetadata(request.Metadata) {
		return provider.OperationResult{}, validationError("authorize")
	}
	payment, err := c.GetPaymentStatus(ctx, request.ProviderPaymentID)
	if err != nil {
		return provider.OperationResult{}, err
	}
	if payment.Status != provider.StatusAuthorized || payment.AmountMinor != request.AmountMinor || payment.Currency != request.Currency {
		return provider.OperationResult{}, inconsistentError("authorize")
	}
	return provider.OperationResult{
		ProviderPaymentID:   payment.ProviderPaymentID,
		ProviderOperationID: payment.ProviderPaymentID + ".authorization",
		Status:              payment.Status,
		AmountMinor:         payment.AmountMinor,
		Currency:            payment.Currency,
	}, nil
}

// Void cancels an uncaptured PaymentIntent.
func (c *Client) Void(ctx context.Context, request provider.VoidRequest) (provider.OperationResult, error) {
	if !validIdentifier(request.PaymentIntentID, "") || !validIdentifier(request.ProviderPaymentID, "pi_") || !validIdempotencyKey(request.IdempotencyKey) || !validStripeMetadata(request.Metadata) {
		return provider.OperationResult{}, validationError("void")
	}
	form := url.Values{
		"cancellation_reason": {"requested_by_customer"},
		"expand[]":            {"latest_charge"},
	}
	for key, value := range request.Metadata {
		form.Set("metadata["+key+"]", value)
	}
	var intent sdkstripe.PaymentIntent
	requestID, err := c.doForm(ctx, http.MethodPost, "/v1/payment_intents/"+request.ProviderPaymentID+"/cancel", "void", request.IdempotencyKey, form, &intent)
	if err != nil {
		return provider.OperationResult{}, err
	}
	payment, err := normalizePaymentIntent(&intent)
	if err != nil || payment.ProviderPaymentID != request.ProviderPaymentID || payment.Status != provider.StatusVoided || !validIdentifier(requestID, "req_") {
		return provider.OperationResult{}, inconsistentError("void")
	}
	return provider.OperationResult{
		ProviderPaymentID:   payment.ProviderPaymentID,
		ProviderOperationID: requestID,
		Status:              payment.Status,
		AmountMinor:         payment.AmountMinor,
		Currency:            payment.Currency,
	}, nil
}

// Refund creates either a full or partial refund. AmountMinor is the refund
// delta; the refund object ID is the durable provider operation identity.
func (c *Client) Refund(ctx context.Context, request provider.RefundRequest) (provider.OperationResult, error) {
	if request.AmountMinor <= 0 || !validCurrency(request.Currency) || !validIdentifier(request.PaymentIntentID, "") || !validIdentifier(request.ProviderPaymentID, "pi_") || !validIdempotencyKey(request.IdempotencyKey) || !validStripeMetadata(request.Metadata) {
		return provider.OperationResult{}, validationError("refund")
	}
	form := url.Values{
		"payment_intent": {request.ProviderPaymentID},
		"amount":         {strconv.FormatInt(request.AmountMinor, 10)},
		"reason":         {"requested_by_customer"},
	}
	for key, value := range request.Metadata {
		form.Set("metadata["+key+"]", value)
	}
	var refund sdkstripe.Refund
	if _, err := c.doForm(ctx, http.MethodPost, "/v1/refunds", "refund", request.IdempotencyKey, form, &refund); err != nil {
		return provider.OperationResult{}, err
	}
	currency := strings.ToUpper(string(refund.Currency))
	if !validIdentifier(refund.ID, "re_") || refund.PaymentIntent == nil || refund.PaymentIntent.ID != request.ProviderPaymentID || refund.Status != sdkstripe.RefundStatusSucceeded || refund.Amount != request.AmountMinor || currency != request.Currency {
		return provider.OperationResult{}, inconsistentError("refund")
	}
	return provider.OperationResult{
		ProviderPaymentID:   refund.PaymentIntent.ID,
		ProviderOperationID: refund.ID,
		Status:              provider.StatusRefunded,
		AmountMinor:         refund.Amount,
		Currency:            currency,
	}, nil
}

// VerifyWebhook authenticates Stripe's raw Stripe-Signature value against at
// most the current and immediately previous secret, then normalizes the event
// without re-encoding the signed body.
func (c *Client) VerifyWebhook(ctx context.Context, headers provider.WebhookHeaders, body []byte) (provider.WebhookEvent, error) {
	if c == nil || ctx == nil || ctx.Err() != nil || len(body) == 0 || int64(len(body)) > c.maxWebhook {
		return provider.WebhookEvent{}, validationError("verify_webhook")
	}
	timestamp, signatures, ok := parseStripeSignature(headers.Signature)
	if !ok || len(c.secrets) == 0 {
		return provider.WebhookEvent{}, authenticationError("verify_webhook")
	}
	verificationNow := c.now().UTC()
	delta := verificationNow.Sub(time.Unix(timestamp, 0).UTC())
	if delta < 0 {
		delta = -delta
	}
	if delta > c.tolerance {
		return provider.WebhookEvent{}, authenticationError("verify_webhook")
	}
	matchedSecrets := 0
	verifiedKeyID := ""
	for index, secret := range c.secrets {
		if index < len(c.secretNotAfter) && !c.secretNotAfter[index].IsZero() &&
			!verificationNow.Before(c.secretNotAfter[index]) {
			continue
		}
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
		_, _ = mac.Write([]byte("."))
		_, _ = mac.Write(body)
		expected := mac.Sum(nil)
		matched := false
		for _, signature := range signatures {
			matched = hmac.Equal(expected, signature) || matched
		}
		if matched {
			matchedSecrets++
			if verifiedKeyID == "" && index < len(c.secretIDs) {
				verifiedKeyID = c.secretIDs[index]
			}
		}
	}
	if matchedSecrets != 1 {
		return provider.WebhookEvent{}, authenticationError("verify_webhook")
	}

	var envelope struct {
		ID         string `json:"id"`
		Type       string `json:"type"`
		APIVersion string `json:"api_version"`
		Account    string `json:"account"`
		LiveMode   *bool  `json:"livemode"`
		Created    int64  `json:"created"`
		Data       struct {
			Object json.RawMessage `json:"object"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || !validIdentifier(envelope.ID, "evt_") ||
		!validEventType(envelope.Type) || envelope.APIVersion != sdkstripe.APIVersion ||
		envelope.Created <= 0 || len(envelope.Data.Object) == 0 ||
		(envelope.Account != "" && !validIdentifier(envelope.Account, "acct_")) {
		return provider.WebhookEvent{}, inconsistentError("verify_webhook")
	}
	if c.webhookBinding && (envelope.LiveMode == nil || *envelope.LiveMode != c.webhookLiveMode ||
		(envelope.Account != "" && envelope.Account != c.webhookAccountID)) {
		return provider.WebhookEvent{}, inconsistentError("verify_webhook")
	}
	accountID := envelope.Account
	if accountID == "" {
		accountID = c.webhookAccountID
	}
	environment := provider.WebhookEnvironmentTest
	if envelope.LiveMode != nil && *envelope.LiveMode {
		environment = provider.WebhookEnvironmentLive
	}
	event := provider.WebhookEvent{
		ProviderEventID:   envelope.ID,
		VerifiedKeyID:     verifiedKeyID,
		ProviderAccountID: accountID,
		Environment:       environment,
		Type:              provider.EventUnknown,
		OriginalType:      envelope.Type,
		Status:            provider.StatusUnknown,
		OccurredAt:        time.Unix(envelope.Created, 0).UTC(),
	}
	switch envelope.Type {
	case "payment_intent.amount_capturable_updated", "payment_intent.succeeded", "payment_intent.canceled":
		var intent sdkstripe.PaymentIntent
		if err := json.Unmarshal(envelope.Data.Object, &intent); err != nil {
			return provider.WebhookEvent{}, inconsistentError("verify_webhook")
		}
		payment, err := normalizePaymentIntent(&intent)
		if err != nil {
			return provider.WebhookEvent{}, inconsistentError("verify_webhook")
		}
		event.ProviderPaymentID = payment.ProviderPaymentID
		event.Status = payment.Status
		event.AmountMinor = payment.AmountMinor
		event.Currency = payment.Currency
		switch {
		case envelope.Type == "payment_intent.amount_capturable_updated" && payment.Status == provider.StatusAuthorized:
			event.Type = provider.EventAuthorized
		case envelope.Type == "payment_intent.succeeded" && payment.Status == provider.StatusCaptured:
			event.Type = provider.EventCaptured
		case envelope.Type == "payment_intent.canceled" && payment.Status == provider.StatusVoided:
			event.Type = provider.EventVoided
		}
	case "charge.refunded":
		var charge sdkstripe.Charge
		if err := json.Unmarshal(envelope.Data.Object, &charge); err != nil || charge.PaymentIntent == nil {
			return provider.WebhookEvent{}, inconsistentError("verify_webhook")
		}
		currency := strings.ToUpper(string(charge.Currency))
		status := provider.StatusUnknown
		if charge.AmountRefunded == charge.Amount && charge.Refunded {
			status = provider.StatusRefunded
		} else if charge.Refunded || charge.AmountRefunded <= 0 || charge.AmountRefunded >= charge.Amount {
			return provider.WebhookEvent{}, inconsistentError("verify_webhook")
		}
		observation := provider.FinancialObservation{Status: status, AmountMinor: charge.Amount, Currency: currency, CapturedMinor: charge.Amount, RefundedMinor: charge.AmountRefunded}
		if !charge.Captured || provider.EvaluateFinancialObservation(provider.FinancialExpectation{AmountMinor: charge.Amount, Currency: currency}, observation) != nil {
			return provider.WebhookEvent{}, inconsistentError("verify_webhook")
		}
		event.ProviderPaymentID = charge.PaymentIntent.ID
		event.Status = status
		event.AmountMinor = charge.Amount
		event.Currency = currency
		if status == provider.StatusRefunded {
			event.Type = provider.EventRefunded
		}
	case "checkout.session.completed":
		var session sdkstripe.CheckoutSession
		if err := json.Unmarshal(envelope.Data.Object, &session); err != nil || session.PaymentIntent == nil || !validIdentifier(session.PaymentIntent.ID, "pi_") || session.AmountTotal <= 0 {
			return provider.WebhookEvent{}, inconsistentError("verify_webhook")
		}
		event.ProviderPaymentID = session.PaymentIntent.ID
		event.AmountMinor = session.AmountTotal
		event.Currency = strings.ToUpper(string(session.Currency))
		if provider.EvaluateFinancialObservation(
			provider.FinancialExpectation{AmountMinor: event.AmountMinor, Currency: event.Currency},
			provider.FinancialObservation{Status: provider.StatusUnknown, AmountMinor: event.AmountMinor, Currency: event.Currency},
		) != nil {
			return provider.WebhookEvent{}, inconsistentError("verify_webhook")
		}
	default:
		var object struct {
			ID       string             `json:"id"`
			Amount   int64              `json:"amount"`
			Currency sdkstripe.Currency `json:"currency"`
		}
		if json.Unmarshal(envelope.Data.Object, &object) == nil && validIdentifier(object.ID, "pi_") {
			event.ProviderPaymentID = object.ID
			event.AmountMinor = object.Amount
			event.Currency = strings.ToUpper(string(object.Currency))
		}
	}
	return event, nil
}

func parseStripeSignature(header string) (int64, [][]byte, bool) {
	if len(header) == 0 || len(header) > 1024 {
		return 0, nil, false
	}
	parts := strings.Split(header, ",")
	if len(parts) > 16 {
		return 0, nil, false
	}
	var timestamp int64
	hasTimestamp := false
	signatures := make([][]byte, 0, 2)
	for _, part := range parts {
		key, value, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found {
			return 0, nil, false
		}
		switch key {
		case "t":
			if hasTimestamp {
				return 0, nil, false
			}
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil || parsed <= 0 {
				return 0, nil, false
			}
			timestamp, hasTimestamp = parsed, true
		case "v1":
			if len(signatures) >= 8 {
				return 0, nil, false
			}
			decoded, err := hex.DecodeString(value)
			if err != nil || len(decoded) != sha256.Size {
				return 0, nil, false
			}
			signatures = append(signatures, decoded)
		}
	}
	return timestamp, signatures, hasTimestamp && len(signatures) > 0
}

func normalizePaymentIntent(intent *sdkstripe.PaymentIntent) (provider.Payment, error) {
	if intent == nil || !validIdentifier(intent.ID, "pi_") || intent.Amount <= 0 {
		return provider.Payment{}, provider.ErrInconsistentFinancialObservation
	}
	currency := strings.ToUpper(string(intent.Currency))
	if !validCurrency(currency) || intent.AmountReceived < 0 || intent.AmountReceived > intent.Amount {
		return provider.Payment{}, provider.ErrInconsistentFinancialObservation
	}
	refunded := int64(0)
	updated := intent.Created
	if intent.LatestCharge != nil {
		if intent.LatestCharge.Amount != 0 {
			fullRefund := intent.LatestCharge.AmountRefunded == intent.Amount
			if intent.LatestCharge.Amount != intent.Amount || (intent.AmountReceived > 0 && !intent.LatestCharge.Captured) || fullRefund != intent.LatestCharge.Refunded {
				return provider.Payment{}, provider.ErrInconsistentFinancialObservation
			}
		}
		refunded = intent.LatestCharge.AmountRefunded
		if intent.LatestCharge.Created > updated {
			updated = intent.LatestCharge.Created
		}
	}
	if intent.CanceledAt > updated {
		updated = intent.CanceledAt
	}
	status := provider.StatusUnknown
	switch intent.Status {
	case sdkstripe.PaymentIntentStatusRequiresCapture:
		status = provider.StatusAuthorized
	case sdkstripe.PaymentIntentStatusSucceeded:
		switch {
		case intent.AmountReceived == intent.Amount && refunded == 0:
			status = provider.StatusCaptured
		case intent.AmountReceived == intent.Amount && refunded == intent.Amount:
			status = provider.StatusRefunded
		}
	case sdkstripe.PaymentIntentStatusCanceled:
		if intent.AmountReceived == 0 && refunded == 0 {
			status = provider.StatusVoided
		}
	case sdkstripe.PaymentIntentStatusRequiresAction, sdkstripe.PaymentIntentStatusRequiresConfirmation, sdkstripe.PaymentIntentStatusRequiresPaymentMethod:
		status = provider.StatusRequiresCustomerAction
	}
	payment := provider.Payment{
		ProviderPaymentID: intent.ID,
		Status:            status,
		AmountMinor:       intent.Amount,
		Currency:          currency,
		CapturedMinor:     intent.AmountReceived,
		RefundedMinor:     refunded,
		ProviderUpdatedAt: time.Unix(updated, 0).UTC(),
	}
	if updated <= 0 || provider.EvaluateFinancialObservation(
		provider.FinancialExpectation{AmountMinor: payment.AmountMinor, Currency: payment.Currency},
		provider.FinancialObservation{Status: payment.Status, AmountMinor: payment.AmountMinor, Currency: payment.Currency, CapturedMinor: payment.CapturedMinor, RefundedMinor: payment.RefundedMinor},
	) != nil {
		return provider.Payment{}, provider.ErrInconsistentFinancialObservation
	}
	return payment, nil
}

func (c *Client) doForm(ctx context.Context, method, path, operation, idempotencyKey string, form url.Values, target any) (string, error) {
	if ctx == nil || ctx.Err() != nil {
		return "", transportError(operation, false)
	}
	var dispatched atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteHeaders: func() { dispatched.Store(true) },
	})
	endpoint := *c.origin
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/") + path
	var body io.Reader
	if method == http.MethodGet {
		endpoint.RawQuery = form.Encode()
	} else {
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return "", transportError(operation, false)
	}
	req.Header.Set("Authorization", "Bearer "+c.secretKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		// Prevent net/http from transparently replaying a mutating request on a
		// stale connection. Durable saga retries must remain observable.
		req.GetBody = nil
	}
	req.Header.Set("Stripe-Version", sdkstripe.APIVersion)
	if c.accountID != "" {
		req.Header.Set("Stripe-Account", c.accountID)
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := c.http.Do(req)
	if err != nil {
		return "", transportError(operation, method != http.MethodGet && dispatched.Load())
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, c.maxBody+1))
	if err != nil {
		return "", transportError(operation, method != http.MethodGet)
	}
	if int64(len(payload)) > c.maxBody {
		return "", unreadableResponseError(operation, method != http.MethodGet)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return "", statusError(operation, response.StatusCode, method != http.MethodGet, response.Header.Get("Retry-After"))
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return "", unreadableResponseError(operation, method != http.MethodGet)
	}
	return response.Header.Get("Request-Id"), nil
}

func parseOrigin(raw string, allowInsecureForTest bool) (*url.URL, error) {
	origin, err := url.Parse(raw)
	if err != nil || origin.Host == "" || origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" || (origin.Path != "" && origin.Path != "/") {
		return nil, errors.New("stripe API origin is invalid")
	}
	if origin.Scheme == "https" {
		origin.Path = ""
		return origin, nil
	}
	host := origin.Hostname()
	parsedIP := net.ParseIP(host)
	reservedTestName := strings.HasSuffix(strings.ToLower(host), ".test") && len(host) > len(".test")
	if origin.Scheme != "http" || !allowInsecureForTest ||
		(host != "localhost" && parsedIP == nil && !reservedTestName) ||
		(parsedIP != nil && !parsedIP.IsLoopback()) {
		return nil, errors.New("stripe API origin must use HTTPS")
	}
	origin.Path = ""
	return origin, nil
}

func validateHTTPSURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return errors.New("must be an absolute HTTPS URL")
	}
	return nil
}

func validCredential(value string, prefixes ...string) bool {
	if len(value) > 256 || strings.ContainsAny(value, "\r\n \t") {
		return false
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(value, prefix) && len(value) >= len(prefix)+4 {
			return true
		}
	}
	return false
}

func validIdentifier(value, prefix string) bool {
	if len(value) < 3 || len(value) > 128 || (prefix != "" && !strings.HasPrefix(value, prefix)) {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') && (character < '0' || character > '9') && character != '_' && character != '-' {
			return false
		}
	}
	return true
}

func validEventType(value string) bool {
	if len(value) < 3 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' && character != '.' {
			return false
		}
	}
	return true
}

func validCurrency(currency string) bool {
	if len(currency) != 3 {
		return false
	}
	for _, character := range currency {
		if character < 'A' || character > 'Z' {
			return false
		}
	}
	return true
}

func validIdempotencyKey(key string) bool {
	return len(key) >= 8 && len(key) <= 128 && !strings.ContainsAny(key, "\r\n \t")
}

func validStripeMetadata(metadata provider.Metadata, reserved ...string) bool {
	if provider.ValidateMetadata(metadata) != nil {
		return false
	}
	reservedKeys := make(map[string]struct{}, len(reserved))
	for _, key := range reserved {
		reservedKeys[key] = struct{}{}
	}
	for key := range metadata {
		if _, exists := reservedKeys[key]; exists {
			return false
		}
		for _, character := range key {
			if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') && (character < '0' || character > '9') && character != '_' && character != '-' && character != '.' {
				return false
			}
		}
	}
	return true
}

func validationError(operation string) *provider.Error {
	return &provider.Error{Category: provider.ErrorPermanentValidation, Operation: operation, Message: "stripe request validation failed"}
}

func inconsistentError(operation string) *provider.Error {
	return &provider.Error{Category: provider.ErrorInconsistentResponse, Operation: operation, Message: "stripe returned an inconsistent response"}
}

func unreadableResponseError(operation string, mutating bool) *provider.Error {
	if mutating {
		return transportError(operation, true)
	}
	return inconsistentError(operation)
}

func authenticationError(operation string) *provider.Error {
	return &provider.Error{Category: provider.ErrorAuthentication, Operation: operation, Message: "stripe webhook authentication failed"}
}

func transportError(operation string, uncertain bool) *provider.Error {
	category := provider.ErrorTransport
	if uncertain {
		category = provider.ErrorTimeoutUnknown
	}
	return &provider.Error{Category: category, Operation: operation, Retryable: true, Uncertain: uncertain, Message: "stripe transport failed"}
}

func statusError(operation string, status int, mutating bool, retryAfter string) *provider.Error {
	result := &provider.Error{Operation: operation, Message: "stripe request failed"}
	switch {
	case status == http.StatusTooManyRequests:
		result.Category, result.Retryable = provider.ErrorRateLimited, true
		if seconds, err := strconv.Atoi(strings.TrimSpace(retryAfter)); err == nil && seconds >= 1 && seconds <= 300 {
			result.RetryAfter = time.Duration(seconds) * time.Second
		}
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		result.Category = provider.ErrorAuthentication
	case status == http.StatusConflict:
		result.Category = provider.ErrorConflict
	case status >= 500:
		result.Category, result.Retryable = provider.ErrorUnavailable, true
		result.Uncertain = mutating && (status == http.StatusInternalServerError || status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout)
	default:
		result.Category = provider.ErrorPermanentValidation
	}
	return result
}
