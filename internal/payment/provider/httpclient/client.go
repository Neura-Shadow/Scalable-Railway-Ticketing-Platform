// Package httpclient implements the bounded HTTP adapter for the deterministic
// payment sandbox. Endpoints are fixed at construction time; request data can
// never select a host or redirect target.
package httpclient

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
)

const maximumBoundaryBytes int64 = 1 << 20

type Config struct {
	BaseURL             string
	APIKey              string
	ConnectTimeout      time.Duration
	RequestTimeout      time.Duration
	MaxResponseBytes    int64
	MaxWebhookBodyBytes int64
	WebhookKeys         map[string][]byte
	WebhookClockSkew    time.Duration
	Now                 func() time.Time
	Transport           http.RoundTripper
}

type Client struct {
	baseURL          string
	apiKey           string
	http             *http.Client
	requestTimeout   time.Duration
	maxResponseBytes int64
	maxWebhookBytes  int64
	webhookKeys      map[string][]byte
	webhookClockSkew time.Duration
	now              func() time.Time
}

// Ready probes only the fixed provider readiness endpoint and validates a
// bounded response. It never reveals the endpoint or credential in errors.
func (client *Client) Ready(ctx context.Context) error {
	var response struct {
		Status string `json:"status"`
	}
	if err := client.doJSON(ctx, http.MethodGet, "/readyz", nil, &response, "readiness", false); err != nil {
		return err
	}
	if response.Status != "ready" {
		return inconsistent("readiness", false)
	}
	return nil
}

func (client *Client) CloseIdleConnections() {
	if client != nil && client.http != nil {
		client.http.CloseIdleConnections()
	}
}

func New(config Config) (*Client, error) {
	parsed, err := url.Parse(strings.TrimSpace(config.BaseURL))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("payment provider HTTP configuration invalid")
	}
	if config.ConnectTimeout <= 0 || config.ConnectTimeout > 30*time.Second ||
		config.RequestTimeout < config.ConnectTimeout || config.RequestTimeout > time.Minute ||
		config.MaxResponseBytes <= 0 || config.MaxResponseBytes > maximumBoundaryBytes ||
		(len(config.WebhookKeys) > 0 && (config.WebhookClockSkew <= 0 || config.WebhookClockSkew > time.Hour)) {
		return nil, errors.New("payment provider HTTP configuration invalid")
	}
	if config.MaxWebhookBodyBytes == 0 {
		config.MaxWebhookBodyBytes = config.MaxResponseBytes
	}
	if config.MaxWebhookBodyBytes <= 0 || config.MaxWebhookBodyBytes > maximumBoundaryBytes {
		return nil, errors.New("payment provider HTTP configuration invalid")
	}
	keys := make(map[string][]byte, len(config.WebhookKeys))
	for keyID, key := range config.WebhookKeys {
		if !validKeyID(keyID) || len(key) < 32 || len(key) > 128 {
			return nil, errors.New("payment provider HTTP configuration invalid")
		}
		keys[keyID] = append([]byte(nil), key...)
	}
	transport := config.Transport
	if transport == nil {
		transport = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   config.ConnectTimeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          16,
			MaxIdleConnsPerHost:   8,
			IdleConnTimeout:       30 * time.Second,
			TLSHandshakeTimeout:   config.ConnectTimeout,
			ResponseHeaderTimeout: config.RequestTimeout,
			ExpectContinueTimeout: time.Second,
		}
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &Client{
		baseURL: strings.TrimRight(parsed.String(), "/"), apiKey: config.APIKey,
		http: &http.Client{
			Transport:     transport,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
		},
		requestTimeout: config.RequestTimeout, maxResponseBytes: config.MaxResponseBytes,
		maxWebhookBytes: config.MaxWebhookBodyBytes, webhookKeys: keys,
		webhookClockSkew: config.WebhookClockSkew, now: now,
	}, nil
}

func (client *Client) CreateCheckout(ctx context.Context, request provider.CreateCheckoutRequest) (provider.Checkout, error) {
	var result provider.Checkout
	if err := validateCheckout(request); err != nil {
		return result, err
	}
	err := client.doJSON(ctx, http.MethodPost, "/v1/checkouts", request, &result, "create_checkout", true)
	if err == nil && (!validPersistedIdentifier(result.ProviderPaymentID) || !validHostedReference(result.HostedReference) ||
		!validIdentifier(result.SyntheticToken) || result.Status != provider.StatusCreated ||
		result.AmountMinor != request.AmountMinor || result.Currency != request.Currency) {
		err = inconsistent("create_checkout", true)
	}
	return result, err
}

func (client *Client) GetPaymentStatus(ctx context.Context, paymentID string) (provider.Payment, error) {
	var result provider.Payment
	if !validIdentifier(paymentID) {
		return result, validation("query_status")
	}
	err := client.doJSON(ctx, http.MethodGet, "/v1/payments/"+url.PathEscape(paymentID), nil, &result, "query_status", false)
	if err == nil && (!validPersistedIdentifier(result.ProviderPaymentID) || result.ProviderPaymentID != paymentID ||
		!validStatus(result.Status) || result.AmountMinor <= 0 || !validCurrency(result.Currency) ||
		result.CapturedMinor < 0 || result.RefundedMinor < 0 || result.RefundedMinor > result.CapturedMinor ||
		result.CapturedMinor > result.AmountMinor || result.ProviderUpdatedAt.IsZero()) {
		err = inconsistent("query_status", false)
	}
	return result, err
}

func (client *Client) Authorize(ctx context.Context, request provider.AuthorizeRequest) (provider.OperationResult, error) {
	if !validCommon(request.PaymentIntentID, request.ProviderPaymentID, request.AmountMinor, request.Currency, request.IdempotencyKey, request.Metadata) || !validIdentifier(request.SyntheticToken) {
		return provider.OperationResult{}, validation("authorize")
	}
	return client.operation(ctx, "authorize", request.ProviderPaymentID, request, request.AmountMinor, request.Currency, true)
}

func (client *Client) Capture(ctx context.Context, request provider.CaptureRequest) (provider.OperationResult, error) {
	if !validCommon(request.PaymentIntentID, request.ProviderPaymentID, request.AmountMinor, request.Currency, request.IdempotencyKey, request.Metadata) {
		return provider.OperationResult{}, validation("capture")
	}
	return client.operation(ctx, "capture", request.ProviderPaymentID, request, request.AmountMinor, request.Currency, true)
}

func (client *Client) Void(ctx context.Context, request provider.VoidRequest) (provider.OperationResult, error) {
	if !validIdentifier(request.PaymentIntentID) || !validIdentifier(request.ProviderPaymentID) || !validIdentifier(request.IdempotencyKey) || provider.ValidateMetadata(request.Metadata) != nil {
		return provider.OperationResult{}, validation("void")
	}
	return client.operation(ctx, "void", request.ProviderPaymentID, request, 0, "", false)
}

func (client *Client) Refund(ctx context.Context, request provider.RefundRequest) (provider.OperationResult, error) {
	if !validCommon(request.PaymentIntentID, request.ProviderPaymentID, request.AmountMinor, request.Currency, request.IdempotencyKey, request.Metadata) {
		return provider.OperationResult{}, validation("refund")
	}
	return client.operation(ctx, "refund", request.ProviderPaymentID, request, request.AmountMinor, request.Currency, true)
}

func (client *Client) operation(ctx context.Context, operation, paymentID string, request any, amount int64, currency string, exactMoney bool) (provider.OperationResult, error) {
	var result provider.OperationResult
	err := client.doJSON(ctx, http.MethodPost, "/v1/payments/"+url.PathEscape(paymentID)+"/"+operation, request, &result, operation, true)
	if err == nil && (!validPersistedIdentifier(result.ProviderPaymentID) || result.ProviderPaymentID != paymentID ||
		!validPersistedIdentifier(result.ProviderOperationID) || !validStatus(result.Status) ||
		(exactMoney && (result.AmountMinor != amount || result.Currency != currency)) ||
		(!exactMoney && (result.AmountMinor <= 0 || !validCurrency(result.Currency)))) {
		err = inconsistent(operation, true)
	}
	return result, err
}

func (client *Client) doJSON(ctx context.Context, method, path string, input, output any, operation string, mutating bool) error {
	if client == nil || client.http == nil || ctx == nil {
		return transportFailure(operation, mutating)
	}
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return validation(operation)
		}
		body = bytes.NewReader(encoded)
	}
	requestContext, cancel := context.WithTimeout(ctx, client.requestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, method, client.baseURL+path, body)
	if err != nil {
		return validation(operation)
	}
	request.Header.Set("Accept", "application/json")
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if client.apiKey != "" {
		request.Header.Set("Authorization", "Bearer "+client.apiKey)
	}
	response, err := client.http.Do(request)
	if err != nil {
		return transportFailure(operation, mutating)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, client.maxResponseBytes+1))
		return statusError(operation, response.StatusCode, mutating)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return inconsistent(operation, mutating)
	}
	encoded, err := io.ReadAll(io.LimitReader(response.Body, client.maxResponseBytes+1))
	if err != nil || int64(len(encoded)) > client.maxResponseBytes {
		return inconsistent(operation, mutating)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return inconsistent(operation, mutating)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return inconsistent(operation, mutating)
	}
	return nil
}

func (client *Client) VerifyWebhook(ctx context.Context, headers provider.WebhookHeaders, body []byte) (provider.WebhookEvent, error) {
	if client == nil || ctx == nil || ctx.Err() != nil || len(body) == 0 || int64(len(body)) > client.maxWebhookBytes {
		return provider.WebhookEvent{}, validation("verify_webhook")
	}
	timestampUnix, err := strconv.ParseInt(headers.Timestamp, 10, 64)
	if err != nil || !validKeyID(headers.KeyID) {
		return provider.WebhookEvent{}, authentication()
	}
	delta := client.now().UTC().Sub(time.Unix(timestampUnix, 0))
	if delta < 0 {
		delta = -delta
	}
	if delta > client.webhookClockSkew {
		return provider.WebhookEvent{}, authentication()
	}
	key, exists := client.webhookKeys[headers.KeyID]
	if !exists {
		return provider.WebhookEvent{}, authentication()
	}
	provided, err := hex.DecodeString(headers.Signature)
	if err != nil {
		return provider.WebhookEvent{}, authentication()
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(headers.Timestamp))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	if !hmac.Equal(mac.Sum(nil), provided) {
		return provider.WebhookEvent{}, authentication()
	}
	var event provider.WebhookEvent
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil || !validWebhookEvent(event) {
		return provider.WebhookEvent{}, inconsistent("verify_webhook", false)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return provider.WebhookEvent{}, inconsistent("verify_webhook", false)
	}
	if !knownEventType(event.Type) {
		event.OriginalType = string(event.Type)
		event.Type = provider.EventUnknown
	}
	return event, nil
}

func validateCheckout(request provider.CreateCheckoutRequest) error {
	if !validIdentifier(request.PaymentIntentID) || !validIdentifier(request.MerchantReference) ||
		request.AmountMinor <= 0 || !validCurrency(request.Currency) || !validIdentifier(request.IdempotencyKey) ||
		provider.ValidateMetadata(request.Metadata) != nil {
		return validation("create_checkout")
	}
	return nil
}

func validCommon(intentID, paymentID string, amount int64, currency, key string, metadata provider.Metadata) bool {
	return validIdentifier(intentID) && validIdentifier(paymentID) && amount > 0 && validCurrency(currency) &&
		validIdentifier(key) && provider.ValidateMetadata(metadata) == nil
}

func validIdentifier(value string) bool {
	return validBoundedIdentifier(value, 160)
}

func validPersistedIdentifier(value string) bool {
	return validBoundedIdentifier(value, 128)
}

func validBoundedIdentifier(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !asciiAlphanumeric(value[0]) {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("-_:.", character) {
			continue
		}
		return false
	}
	return true
}

func validHostedReference(value string) bool {
	return validBoundedIdentifier(value, 256)
}

func asciiAlphanumeric(character byte) bool {
	return (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
		(character >= '0' && character <= '9')
}

func validKeyID(value string) bool {
	return len(value) <= 64 && validIdentifier(value)
}

func validCurrency(value string) bool {
	if len(value) != 3 {
		return false
	}
	for _, character := range value {
		if character < 'A' || character > 'Z' {
			return false
		}
	}
	return true
}

func validStatus(status provider.Status) bool {
	switch status {
	case provider.StatusCreated, provider.StatusRequiresCustomerAction, provider.StatusAuthorized,
		provider.StatusCaptured, provider.StatusVoided, provider.StatusRefunded, provider.StatusFailed,
		provider.StatusCancelled, provider.StatusUnknown:
		return true
	default:
		return false
	}
}

func knownEventType(eventType provider.EventType) bool {
	switch eventType {
	case provider.EventCheckoutCreated, provider.EventAuthorized, provider.EventCaptured, provider.EventVoided, provider.EventRefunded:
		return true
	default:
		return false
	}
}

func validWebhookEvent(event provider.WebhookEvent) bool {
	return validPersistedIdentifier(event.ProviderEventID) && validPersistedIdentifier(event.ProviderPaymentID) &&
		event.AmountMinor > 0 && validCurrency(event.Currency) && !event.OccurredAt.IsZero() && validStatus(event.Status)
}

func statusError(operation string, status int, mutating bool) *provider.Error {
	switch status {
	case http.StatusBadRequest:
		return &provider.Error{Category: provider.ErrorPermanentValidation, Operation: operation, Message: "payment provider rejected request"}
	case http.StatusUnauthorized, http.StatusForbidden:
		return &provider.Error{Category: provider.ErrorAuthentication, Operation: operation, Message: "payment provider authentication failed"}
	case http.StatusConflict:
		return &provider.Error{Category: provider.ErrorConflict, Operation: operation, Message: "payment provider operation conflicted"}
	case http.StatusTooManyRequests:
		return &provider.Error{Category: provider.ErrorRateLimited, Operation: operation, Retryable: true, Message: "payment provider rate limited request"}
	case http.StatusGatewayTimeout:
		return &provider.Error{Category: provider.ErrorTimeoutUnknown, Operation: operation, Uncertain: mutating, Retryable: !mutating, Message: "payment provider outcome is unknown"}
	case http.StatusServiceUnavailable:
		return &provider.Error{Category: provider.ErrorUnavailable, Operation: operation, Retryable: true, Message: "payment provider is unavailable"}
	default:
		if status >= 500 {
			return &provider.Error{Category: provider.ErrorTransport, Operation: operation, Retryable: true, Message: "payment provider transport failure"}
		}
		return inconsistent(operation, mutating)
	}
}

func validation(operation string) *provider.Error {
	return &provider.Error{Category: provider.ErrorPermanentValidation, Operation: operation, Message: "payment provider request is invalid"}
}

func authentication() *provider.Error {
	return &provider.Error{Category: provider.ErrorAuthentication, Operation: "verify_webhook", Message: "payment webhook authentication failed"}
}

func transportFailure(operation string, mutating bool) *provider.Error {
	if mutating {
		return &provider.Error{Category: provider.ErrorTimeoutUnknown, Operation: operation, Uncertain: true, Message: "payment provider outcome is unknown"}
	}
	return &provider.Error{Category: provider.ErrorTransport, Operation: operation, Retryable: true, Message: "payment provider transport failure"}
}

func inconsistent(operation string, uncertain bool) *provider.Error {
	return &provider.Error{Category: provider.ErrorInconsistentResponse, Operation: operation, Uncertain: uncertain, Message: "payment provider response is inconsistent"}
}

var _ provider.Client = (*Client)(nil)
