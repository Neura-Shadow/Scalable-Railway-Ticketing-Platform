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

type Metrics interface {
	RecordPaymentWebhookInvalid(provider string)
}

type Config struct {
	Providers    map[string]Verifier
	Repository   Repository
	MaxBodyBytes int
	Now          func() time.Time
	NewID        func() uuid.UUID
	Metrics      Metrics
}

type Service struct {
	providers    map[string]Verifier
	repository   Repository
	maxBodyBytes int
	now          func() time.Time
	newID        func() uuid.UUID
	metrics      Metrics
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
	}, nil
}

func (service *Service) IngestPaymentWebhook(ctx context.Context, request httpapi.PaymentWebhookRequest) (httpapi.PaymentWebhookDisposition, error) {
	if service == nil || ctx == nil || service.repository == nil || service.now == nil || service.newID == nil {
		return "", ErrPersistence
	}
	verifier, ok := service.providers[request.Provider]
	if !ok || verifier == nil || !validIdentifier(request.KeyID, 64, false) ||
		!validVisibleHeader(request.Timestamp, maximumTimestampLen) ||
		!validVisibleHeader(request.Signature, maximumSignatureLen) ||
		len(request.Body) == 0 || len(request.Body) > service.maxBodyBytes {
		service.recordInvalid(request.Provider)
		return "", httpapi.ErrWebhookInvalid
	}
	body := append([]byte(nil), request.Body...)
	verificationBody := append([]byte(nil), body...)
	event, err := verifier.VerifyWebhook(ctx, provider.WebhookHeaders{
		KeyID: request.KeyID, Timestamp: request.Timestamp, Signature: request.Signature,
	}, verificationBody)
	if err != nil {
		service.recordInvalid(request.Provider)
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
		service.recordInvalid(request.Provider)
		return "", httpapi.ErrWebhookInvalid
	}
	id := service.newID()
	if id == uuid.Nil {
		return "", ErrPersistence
	}
	record := Record{
		ID: id, Provider: request.Provider,
		ProviderEventID: event.ProviderEventID, EventType: eventType,
		ProviderPaymentID: event.ProviderPaymentID, PayloadHash: sha256.Sum256(body),
		VerifiedKeyID: request.KeyID, EventCreatedAt: event.OccurredAt.UTC(),
		SignatureVerifiedAt: now, ReceivedAt: now, BodySizeBytes: len(request.Body),
		Ignored: ignored,
	}
	result, err := service.repository.StoreVerified(ctx, record)
	if err != nil {
		return "", ErrPersistence
	}
	switch result {
	case StoreAccepted:
		if ignored {
			return httpapi.PaymentWebhookIgnored, nil
		}
		return httpapi.PaymentWebhookAccepted, nil
	case StoreDuplicate:
		return httpapi.PaymentWebhookDuplicate, nil
	case StoreConflict:
		return "", httpapi.ErrWebhookConflict
	default:
		return "", ErrPersistence
	}
}

func (service *Service) recordInvalid(providerName string) {
	if service != nil && service.metrics != nil {
		service.metrics.RecordPaymentWebhookInvalid(providerName)
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

var _ httpapi.PaymentWebhookService = (*Service)(nil)
