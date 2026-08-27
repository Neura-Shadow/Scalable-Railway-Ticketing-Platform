// Package webhook verifies and durably records bounded payment-provider events.
// It deliberately does not advance payment sagas or issue tickets inline.
package webhook

import (
	"context"
	"crypto/sha256"
	"errors"
	"regexp"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

const (
	defaultMaxBodyBytes = 64 << 10
	maximumBodyBytes    = 1 << 20
	maximumTimestampLen = 32
	maximumSignatureLen = 256
)

var (
	ErrInvalidConfiguration = errors.New("payment webhook configuration is invalid")
	ErrPersistence          = errors.New("payment webhook persistence unavailable")

	providerNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	identifierPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
)

// Verifier is the narrow portion of the provider boundary needed at ingress.
type Verifier interface {
	VerifyWebhook(context.Context, provider.WebhookHeaders, []byte) (provider.WebhookEvent, error)
}

// Record contains only normalized, bounded evidence. Raw request bodies,
// signatures, and provider secrets cannot cross this persistence boundary.
type Record struct {
	ID                  uuid.UUID
	Provider            string
	ProviderEventID     string
	ProviderAccountID   string
	ProviderEnvironment string
	EventType           string
	ProviderPaymentID   string
	PayloadHash         [sha256.Size]byte
	VerifiedKeyID       string
	EventCreatedAt      time.Time
	SignatureVerifiedAt time.Time
	ReceivedAt          time.Time
	BodySizeBytes       int
	Ignored             bool
}

type StoreResult string

const (
	StoreAccepted  StoreResult = "accepted"
	StoreDuplicate StoreResult = "duplicate"
	StoreConflict  StoreResult = "conflict"
)

type Repository interface {
	StoreVerified(context.Context, Record) (StoreResult, error)
}

type KeyringValidator interface {
	ValidateVerifiedKey(context.Context, string, string, string, time.Time) error
}

type Metrics interface {
	RecordPaymentWebhookInvalid(provider string)
	RecordWebhookAck(provider, result, reason string, commitDuration time.Duration)
	RecordRegionalWriteRejected(region, databaseRole, shardID, reason string)
}

type Config struct {
	Providers    map[string]Verifier
	Repository   Repository
	MaxBodyBytes int
	Now          func() time.Time
	NewID        func() uuid.UUID
	Metrics      Metrics
	Region       string
	Keyring      KeyringValidator
}

type Service struct {
	providers    map[string]Verifier
	repository   Repository
	maxBodyBytes int
	now          func() time.Time
	newID        func() uuid.UUID
	metrics      Metrics
	region       string
	keyring      KeyringValidator
}

func NewService(config Config) (*Service, error) {
	if len(config.Providers) == 0 || config.Repository == nil || config.Now == nil || config.NewID == nil {
		return nil, ErrInvalidConfiguration
	}
	providers := make(map[string]Verifier, len(config.Providers))
	for name, verifier := range config.Providers {
		if !providerNamePattern.MatchString(name) || verifier == nil {
			return nil, ErrInvalidConfiguration
		}
		providers[name] = verifier
	}
	if _, stripe := providers["stripe"]; stripe && config.Keyring == nil {
		return nil, ErrInvalidConfiguration
	}
	maxBodyBytes := config.MaxBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = defaultMaxBodyBytes
	}
	if maxBodyBytes > maximumBodyBytes {
		return nil, ErrInvalidConfiguration
	}
	return &Service{
		providers: providers, repository: config.Repository,
		maxBodyBytes: maxBodyBytes, now: config.Now, newID: config.NewID, metrics: config.Metrics,
		region: config.Region, keyring: config.Keyring,
	}, nil
}

func (service *Service) IngestPaymentWebhook(ctx context.Context, request httpapi.PaymentWebhookRequest) (httpapi.PaymentWebhookDisposition, error) {
	if service == nil || ctx == nil || service.repository == nil || service.now == nil || service.newID == nil {
		return "", ErrPersistence
	}
	verifier, ok := service.providers[request.Provider]
	if !ok || verifier == nil || !validIdentifier(request.KeyID, 64, true) ||
		!validOptionalVisibleHeader(request.Timestamp, maximumTimestampLen) ||
		!validVisibleHeader(request.Signature, maximumSignatureLen) ||
		len(request.Body) == 0 || len(request.Body) > service.maxBodyBytes {
		return "", httpapi.ErrWebhookInvalid
	}
	body := append([]byte(nil), request.Body...)
	verificationBody := append([]byte(nil), body...)
	event, err := verifier.VerifyWebhook(ctx, provider.WebhookHeaders{
		KeyID: request.KeyID, Timestamp: request.Timestamp, Signature: request.Signature,
	}, verificationBody)
	if err != nil {
		if webhookAuthenticationFailure(err) {
			service.recordInvalid(request.Provider)
		}
		return "", httpapi.ErrWebhookInvalid
	}
	now := service.now().UTC()
	eventType := string(event.Type)
	ignored := event.Type == provider.EventUnknown
	if ignored && event.OriginalType != "" {
		eventType = event.OriginalType
	}
	if now.IsZero() || !validIdentifier(event.ProviderEventID, 128, false) ||
		!validIdentifier(eventType, 128, false) ||
		!validIdentifier(event.ProviderPaymentID, 128, true) || event.OccurredAt.IsZero() {
		return "", httpapi.ErrWebhookInvalid
	}
	if request.Provider == "stripe" && (!validIdentifier(event.ProviderAccountID, 128, false) ||
		(event.Environment != provider.WebhookEnvironmentTest && event.Environment != provider.WebhookEnvironmentLive)) {
		return "", httpapi.ErrWebhookInvalid
	}
	if request.Provider != "stripe" && (event.ProviderAccountID != "" || event.Environment != "") {
		return "", httpapi.ErrWebhookInvalid
	}
	verifiedKeyID := event.VerifiedKeyID
	if verifiedKeyID == "" {
		verifiedKeyID = request.KeyID
	}
	if !validIdentifier(verifiedKeyID, 64, false) {
		return "", httpapi.ErrWebhookInvalid
	}
	if request.Provider == "stripe" {
		if err := service.keyring.ValidateVerifiedKey(ctx, request.Provider, event.ProviderAccountID, verifiedKeyID, now); err != nil {
			if errors.Is(err, ErrPersistence) {
				service.recordAck(request.Provider, "failure", "database", 0)
				return "", ErrPersistence
			}
			service.recordInvalid(request.Provider)
			return "", httpapi.ErrWebhookInvalid
		}
	}
	id := service.newID()
	if id == uuid.Nil {
		return "", ErrPersistence
	}
	record := Record{
		ID: id, Provider: request.Provider,
		ProviderEventID: event.ProviderEventID, EventType: eventType,
		ProviderAccountID: event.ProviderAccountID, ProviderEnvironment: string(event.Environment),
		ProviderPaymentID: event.ProviderPaymentID, PayloadHash: sha256.Sum256(body),
		VerifiedKeyID: verifiedKeyID, EventCreatedAt: event.OccurredAt.UTC(),
		SignatureVerifiedAt: now, ReceivedAt: now, BodySizeBytes: len(request.Body),
		Ignored: ignored,
	}
	commitStarted := time.Now()
	result, err := service.repository.StoreVerified(ctx, record)
	if err != nil {
		reason := webhookCommitReason(err)
		service.recordAck(request.Provider, "failure", reason, time.Since(commitStarted))
		if isRegionalWriteRejection(err) && service.metrics != nil {
			service.metrics.RecordRegionalWriteRejected(service.region, "control", "none", reason)
		}
		return "", ErrPersistence
	}
	switch result {
	case StoreAccepted:
		service.recordAck(request.Provider, "accepted", "none", time.Since(commitStarted))
		if ignored {
			return httpapi.PaymentWebhookIgnored, nil
		}
		return httpapi.PaymentWebhookAccepted, nil
	case StoreDuplicate:
		service.recordAck(request.Provider, "duplicate", "duplicate", time.Since(commitStarted))
		return httpapi.PaymentWebhookDuplicate, nil
	case StoreConflict:
		service.recordAck(request.Provider, "conflict", "event_conflict", time.Since(commitStarted))
		return httpapi.PaymentWebhookConflict, nil
	default:
		service.recordAck(request.Provider, "failure", "unexpected", time.Since(commitStarted))
		return "", ErrPersistence
	}
}

func isRegionalWriteRejection(err error) bool {
	return errors.Is(err, authority.ErrRoleNotActive) || errors.Is(err, authority.ErrWritesDisabled) ||
		errors.Is(err, authority.ErrRegionMismatch) || errors.Is(err, authority.ErrEpochMismatch) ||
		errors.Is(err, authority.ErrAuthorityNotActive)
}

func webhookCommitReason(err error) string {
	switch {
	case errors.Is(err, authority.ErrRoleNotActive):
		return "passive"
	case errors.Is(err, authority.ErrWritesDisabled):
		return "writes_disabled"
	case errors.Is(err, authority.ErrRegionMismatch):
		return "region_mismatch"
	case errors.Is(err, authority.ErrEpochMismatch):
		return "stale_epoch"
	case errors.Is(err, authority.ErrAuthorityNotActive):
		return "fenced"
	default:
		return "database"
	}
}

func webhookAuthenticationFailure(err error) bool {
	var providerError *provider.Error
	return errors.As(err, &providerError) && providerError.Category == provider.ErrorAuthentication
}

func (service *Service) recordInvalid(providerName string) {
	if service != nil && service.metrics != nil {
		service.metrics.RecordPaymentWebhookInvalid(providerName)
	}
}

func (service *Service) recordAck(providerName, result, reason string, duration time.Duration) {
	if service != nil && service.metrics != nil {
		service.metrics.RecordWebhookAck(providerName, result, reason, duration)
	}
}

func validIdentifier(value string, maximum int, optional bool) bool {
	if value == "" {
		return optional
	}
	return len(value) <= maximum && identifierPattern.MatchString(value)
}

func validVisibleHeader(value string, maximum int) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func validOptionalVisibleHeader(value string, maximum int) bool {
	return value == "" || validVisibleHeader(value, maximum)
}

var _ httpapi.PaymentWebhookService = (*Service)(nil)
