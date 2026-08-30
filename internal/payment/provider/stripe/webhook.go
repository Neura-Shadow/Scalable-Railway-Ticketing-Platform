package stripe

import (
	"context"
	"errors"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
)

// WebhookConfig is the ingress-only Stripe trust configuration. It contains
// no outbound API credential or checkout URL.
type WebhookConfig struct {
	MaxBodyBytes   int64
	Tolerance      time.Duration
	Secrets        []string
	SecretIDs      []string
	SecretNotAfter []time.Time
	AccountID      string
	LiveMode       bool
	Now            func() time.Time
}

// WebhookVerifier exposes only signature verification to the public ingress
// composition root.
type WebhookVerifier struct {
	client Client
}

// NewWebhookVerifier constructs an ingress-only verifier for the current and
// immediately previous Stripe endpoint secrets.
func NewWebhookVerifier(config WebhookConfig) (*WebhookVerifier, error) {
	maxBody := config.MaxBodyBytes
	if maxBody == 0 {
		maxBody = defaultMaxResponseBodySize
	}
	tolerance := config.Tolerance
	if tolerance == 0 {
		tolerance = defaultWebhookTolerance
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	if maxBody < 1024 || maxBody > 8<<20 || tolerance < time.Second || tolerance > 15*time.Minute ||
		!validIdentifier(config.AccountID, "acct_") || len(config.Secrets) == 0 ||
		len(config.Secrets) > maximumWebhookSecrets || len(config.SecretIDs) != len(config.Secrets) ||
		(len(config.SecretNotAfter) != 0 && len(config.SecretNotAfter) != len(config.Secrets)) {
		return nil, errors.New("stripe webhook verifier configuration is invalid")
	}
	secrets := make([][]byte, 0, len(config.Secrets))
	secretIDs := make([]string, 0, len(config.SecretIDs))
	secretNotAfter := make([]time.Time, len(config.Secrets))
	seenSecrets := make(map[string]struct{}, len(config.Secrets))
	seenIDs := make(map[string]struct{}, len(config.SecretIDs))
	for index, secret := range config.Secrets {
		keyID := config.SecretIDs[index]
		if !validCredential(secret, "whsec_") || !validIdentifier(keyID, "") {
			return nil, errors.New("stripe webhook verifier configuration is invalid")
		}
		if _, exists := seenSecrets[secret]; exists {
			return nil, errors.New("stripe webhook verifier configuration is invalid")
		}
		if _, exists := seenIDs[keyID]; exists {
			return nil, errors.New("stripe webhook verifier configuration is invalid")
		}
		seenSecrets[secret] = struct{}{}
		seenIDs[keyID] = struct{}{}
		secrets = append(secrets, []byte(secret))
		secretIDs = append(secretIDs, keyID)
		if len(config.SecretNotAfter) != 0 {
			secretNotAfter[index] = config.SecretNotAfter[index].UTC()
		}
	}
	return &WebhookVerifier{client: Client{
		maxWebhook:       maxBody,
		tolerance:        tolerance,
		secrets:          secrets,
		secretIDs:        secretIDs,
		secretNotAfter:   secretNotAfter,
		webhookAccountID: config.AccountID,
		webhookLiveMode:  config.LiveMode,
		webhookBinding:   true,
		now:              now,
	}}, nil
}

// VerifyWebhook authenticates and normalizes the untouched request body.
func (v *WebhookVerifier) VerifyWebhook(ctx context.Context, headers provider.WebhookHeaders, body []byte) (provider.WebhookEvent, error) {
	if v == nil {
		return provider.WebhookEvent{}, validationError("verify_webhook")
	}
	return v.client.VerifyWebhook(ctx, headers, body)
}

var _ interface {
	VerifyWebhook(context.Context, provider.WebhookHeaders, []byte) (provider.WebhookEvent, error)
} = (*WebhookVerifier)(nil)
