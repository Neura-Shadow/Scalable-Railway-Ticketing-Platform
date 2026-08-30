// Package application coordinates durable control-plane payment intent state
// with one independently committed authoritative reservation-shard command.
package application

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	minIdempotencyKeyBytes = 8
	maxIdempotencyKeyBytes = 128
	defaultProvider        = "sandbox"
)

var (
	ErrInvalidPaymentRequest       = errors.New("invalid payment request")
	ErrPaymentNotFound             = errors.New("payment resource not found")
	ErrPaymentConflict             = errors.New("payment intent conflict")
	ErrReservationNotPayable       = errors.New("reservation not payable")
	ErrPaymentUnavailable          = errors.New("payment unavailable")
	ErrControlFinalizationDeferred = errors.New("payment control finalization deferred")
	ErrShardPaymentCommandDeferred = errors.New("payment shard command deferred")
)

// ReservationSnapshot contains only immutable data needed to create a
// server-derived payment intent. It never contains passenger or provider data.
type ReservationSnapshot struct {
	ID          uuid.UUID
	OwnerID     uuid.UUID
	TrainRunID  uuid.UUID
	Status      string
	AmountMinor int64
	Currency    string
	ExpiresAt   time.Time
}

type CreateIntentCommand struct {
	OwnerID        uuid.UUID
	ReservationID  uuid.UUID
	IdempotencyKey string
}

type CancelIntentCommand struct {
	OwnerID         uuid.UUID
	PaymentIntentID uuid.UUID
	IdempotencyKey  string
}

type CancelReservationCommand struct {
	OwnerID        uuid.UUID
	ReservationID  uuid.UUID
	IdempotencyKey string
}

type IntentRecord struct {
	ID                uuid.UUID
	SagaID            uuid.UUID
	BeginCommandID    uuid.UUID
	ReservationID     uuid.UUID
	TrainRunID        uuid.UUID
	OwnerID           uuid.UUID
	Provider          string
	ProviderPaymentID string
	HostedSessionRef  string
	AmountMinor       int64
	Currency          string
	State             string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	CompletedAt       *time.Time
}

type ReserveIntentRequest struct {
	PaymentIntentID    uuid.UUID
	SagaID             uuid.UUID
	BeginCommandID     uuid.UUID
	ReservationID      uuid.UUID
	TrainRunID         uuid.UUID
	OwnerID            uuid.UUID
	Provider           string
	AmountMinor        int64
	Currency           string
	IdempotencyKeyHash [sha256.Size]byte
	RequestFingerprint [sha256.Size]byte
}

type CancelIntentRequest struct {
	OwnerID            uuid.UUID
	PaymentIntentID    uuid.UUID
	OperationID        uuid.UUID
	IdempotencyKeyHash [sha256.Size]byte
	RequestFingerprint [sha256.Size]byte
}

type BeginPaymentCommand struct {
	CommandID          uuid.UUID
	PaymentIntentID    uuid.UUID
	ReservationID      uuid.UUID
	TrainRunID         uuid.UUID
	OwnerID            uuid.UUID
	AmountMinor        int64
	Currency           string
	GraceExpiresAt     time.Time
	RequestFingerprint [sha256.Size]byte
}

type BeginPaymentReceipt struct {
	CommandID          uuid.UUID
	PaymentIntentID    uuid.UUID
	RequestFingerprint [sha256.Size]byte
}

type IntentStore interface {
	LookupIntentByIdempotency(context.Context, uuid.UUID, [sha256.Size]byte, [sha256.Size]byte) (IntentRecord, bool, error)
	ReserveIntent(context.Context, ReserveIntentRequest) (IntentRecord, bool, error)
	MarkReservationSecured(context.Context, uuid.UUID, uuid.UUID, [sha256.Size]byte) (IntentRecord, error)
	FailReservationSecuring(context.Context, uuid.UUID, [sha256.Size]byte) error
	GetOwnedIntent(context.Context, uuid.UUID, uuid.UUID) (IntentRecord, error)
	GetOwnedIntentByReservation(context.Context, uuid.UUID, uuid.UUID) (IntentRecord, error)
	RequestCancellation(context.Context, CancelIntentRequest) (IntentRecord, error)
}

func (service *Service) GetIntent(ctx context.Context, ownerID, intentID uuid.UUID) (IntentRecord, error) {
	if service == nil || service.store == nil || ctx == nil || ownerID == uuid.Nil || intentID == uuid.Nil {
		return IntentRecord{}, ErrInvalidPaymentRequest
	}
	intent, err := service.store.GetOwnedIntent(ctx, ownerID, intentID)
	if err != nil {
		if errors.Is(err, ErrPaymentNotFound) {
			return IntentRecord{}, ErrPaymentNotFound
		}
		return IntentRecord{}, ErrPaymentUnavailable
	}
	return intent, nil
}

func (service *Service) CancelIntent(ctx context.Context, command CancelIntentCommand) (IntentRecord, error) {
	if service == nil || service.store == nil || service.newID == nil || ctx == nil {
		return IntentRecord{}, ErrPaymentUnavailable
	}
	keyHash, fingerprint, err := cancellationIdentity(command)
	if err != nil {
		return IntentRecord{}, err
	}
	operationID := service.newID()
	if operationID == uuid.Nil {
		return IntentRecord{}, ErrPaymentUnavailable
	}
	intent, err := service.store.RequestCancellation(ctx, CancelIntentRequest{
		OwnerID: command.OwnerID, PaymentIntentID: command.PaymentIntentID, OperationID: operationID,
		IdempotencyKeyHash: keyHash, RequestFingerprint: fingerprint,
	})
	if err != nil {
		switch {
		case errors.Is(err, ErrPaymentNotFound):
			return IntentRecord{}, ErrPaymentNotFound
		case errors.Is(err, ErrPaymentConflict):
			return IntentRecord{}, ErrPaymentConflict
		default:
			return IntentRecord{}, ErrPaymentUnavailable
		}
	}
	return intent, nil
}

func (service *Service) CancelReservation(ctx context.Context, command CancelReservationCommand) (IntentRecord, error) {
	if service == nil || service.store == nil || ctx == nil || command.OwnerID == uuid.Nil || command.ReservationID == uuid.Nil {
		return IntentRecord{}, ErrInvalidPaymentRequest
	}
	intent, err := service.store.GetOwnedIntentByReservation(ctx, command.OwnerID, command.ReservationID)
	if err != nil {
		if errors.Is(err, ErrPaymentNotFound) {
			return IntentRecord{}, ErrPaymentNotFound
		}
		return IntentRecord{}, ErrPaymentUnavailable
	}
	switch intent.State {
	case "failed", "expired":
		return IntentRecord{}, ErrPaymentNotFound
	case "cancelled", "voided", "refunded":
		return intent, nil
	case "created", "reservation_securing", "checkout_pending", "authorization_pending", "capture_pending",
		"captured", "ticket_issue_pending", "manual_review":
		return IntentRecord{}, ErrPaymentConflict
	}
	return service.CancelIntent(ctx, CancelIntentCommand{
		OwnerID: command.OwnerID, PaymentIntentID: intent.ID, IdempotencyKey: command.IdempotencyKey,
	})
}

type ReservationGateway interface {
	GetPayableReservation(context.Context, uuid.UUID) (ReservationSnapshot, error)
	BeginPayment(context.Context, BeginPaymentCommand) (BeginPaymentReceipt, error)
}

type Service struct {
	store        IntentStore
	reservations ReservationGateway
	now          func() time.Time
	newID        func() uuid.UUID
	grace        time.Duration
	provider     string
}

func NewService(store IntentStore, reservations ReservationGateway, now func() time.Time, newID func() uuid.UUID) *Service {
	return &Service{
		store: store, reservations: reservations, now: now, newID: newID,
		grace: 15 * time.Minute, provider: defaultProvider,
	}
}

// WithProcessingGrace applies the bounded reservation protection window used
// while provider outcome is being established. Invalid values retain the safe
// default and can never create an unbounded inventory hold.
func (service *Service) WithProcessingGrace(grace time.Duration) *Service {
	if service != nil && grace > 0 && grace <= 24*time.Hour {
		service.grace = grace
	}
	return service
}

// WithProvider selects the startup-configured provider identity written into
// every new intent. Customer input never controls this value.
func (service *Service) WithProvider(provider string) *Service {
	if service == nil {
		return service
	}
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "sandbox", "stripe":
		service.provider = strings.ToLower(strings.TrimSpace(provider))
	}
	return service
}

// CreateIntent is deliberately split into control reserve, shard mutation,
// and control finalization. No transaction spans either database.
func (service *Service) CreateIntent(ctx context.Context, command CreateIntentCommand) (IntentRecord, error) {
	if service == nil || service.store == nil || service.reservations == nil || service.now == nil || service.newID == nil || ctx == nil {
		return IntentRecord{}, ErrPaymentUnavailable
	}
	keyHash, fingerprint, err := intentIdentity(command)
	if err != nil {
		return IntentRecord{}, err
	}
	if existing, found, lookupErr := service.store.LookupIntentByIdempotency(ctx, command.OwnerID, keyHash, fingerprint); lookupErr != nil {
		return IntentRecord{}, normalizeStoreError(lookupErr)
	} else if found {
		if existing.State != "reservation_securing" {
			return existing, nil
		}
		return service.secureReservation(ctx, existing, fingerprint, service.now().UTC())
	}

	snapshot, err := service.reservations.GetPayableReservation(ctx, command.ReservationID)
	if err != nil {
		return IntentRecord{}, normalizeReservationError(err)
	}
	now := service.now().UTC()
	if snapshot.ID != command.ReservationID || snapshot.OwnerID != command.OwnerID {
		return IntentRecord{}, ErrPaymentNotFound
	}
	if snapshot.TrainRunID == uuid.Nil || snapshot.Status != "held" || !now.Before(snapshot.ExpiresAt) ||
		snapshot.AmountMinor <= 0 || !validCurrency(snapshot.Currency) {
		return IntentRecord{}, ErrReservationNotPayable
	}

	request := ReserveIntentRequest{
		PaymentIntentID: service.newID(), SagaID: service.newID(), BeginCommandID: service.newID(),
		ReservationID: snapshot.ID, TrainRunID: snapshot.TrainRunID, OwnerID: snapshot.OwnerID,
		Provider: service.provider, AmountMinor: snapshot.AmountMinor, Currency: snapshot.Currency,
		IdempotencyKeyHash: keyHash, RequestFingerprint: fingerprint,
	}
	if request.PaymentIntentID == uuid.Nil || request.SagaID == uuid.Nil || request.BeginCommandID == uuid.Nil {
		return IntentRecord{}, ErrPaymentUnavailable
	}
	intent, replayed, err := service.store.ReserveIntent(ctx, request)
	if err != nil {
		return IntentRecord{}, normalizeStoreError(err)
	}
	if replayed && intent.State != "reservation_securing" {
		return intent, nil
	}

	return service.secureReservation(ctx, intent, fingerprint, now)
}

func (service *Service) secureReservation(ctx context.Context, intent IntentRecord, fingerprint [sha256.Size]byte, now time.Time) (IntentRecord, error) {
	receipt, err := service.reservations.BeginPayment(ctx, BeginPaymentCommand{
		CommandID: intent.BeginCommandID, PaymentIntentID: intent.ID, ReservationID: intent.ReservationID,
		TrainRunID: intent.TrainRunID, OwnerID: intent.OwnerID, AmountMinor: intent.AmountMinor,
		Currency: intent.Currency, GraceExpiresAt: now.Add(service.grace), RequestFingerprint: fingerprint,
	})
	if err != nil {
		if errors.Is(err, ErrReservationNotPayable) {
			if failErr := service.store.FailReservationSecuring(ctx, intent.ID, fingerprint); failErr != nil {
				return intent, normalizeStoreError(failErr)
			}
		}
		return intent, normalizeReservationError(err)
	}
	if receipt.CommandID != intent.BeginCommandID || receipt.PaymentIntentID != intent.ID || receipt.RequestFingerprint != fingerprint {
		return intent, ErrPaymentConflict
	}
	secured, err := service.store.MarkReservationSecured(ctx, intent.ID, receipt.CommandID, receipt.RequestFingerprint)
	if err != nil {
		return intent, normalizeStoreError(err)
	}
	return secured, nil
}

func intentIdentity(command CreateIntentCommand) ([sha256.Size]byte, [sha256.Size]byte, error) {
	var empty [sha256.Size]byte
	key := strings.TrimSpace(command.IdempotencyKey)
	if command.OwnerID == uuid.Nil || command.ReservationID == uuid.Nil || len(key) < minIdempotencyKeyBytes || len(key) > maxIdempotencyKeyBytes {
		return empty, empty, ErrInvalidPaymentRequest
	}
	for _, character := range key {
		if character < 0x21 || character > 0x7e {
			return empty, empty, ErrInvalidPaymentRequest
		}
	}
	keyHash := sha256.Sum256([]byte(key))
	digest := sha256.New()
	writeIdentityField(digest, "v1")
	writeIdentityField(digest, "payment_intent.create")
	writeIdentityField(digest, command.OwnerID.String())
	writeIdentityField(digest, command.ReservationID.String())
	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], digest.Sum(nil))
	return keyHash, fingerprint, nil
}

func cancellationIdentity(command CancelIntentCommand) ([sha256.Size]byte, [sha256.Size]byte, error) {
	var empty [sha256.Size]byte
	key := strings.TrimSpace(command.IdempotencyKey)
	if command.OwnerID == uuid.Nil || command.PaymentIntentID == uuid.Nil || len(key) < minIdempotencyKeyBytes || len(key) > maxIdempotencyKeyBytes {
		return empty, empty, ErrInvalidPaymentRequest
	}
	for _, character := range key {
		if character < 0x21 || character > 0x7e {
			return empty, empty, ErrInvalidPaymentRequest
		}
	}
	keyHash := sha256.Sum256([]byte(key))
	digest := sha256.New()
	writeIdentityField(digest, "v1")
	writeIdentityField(digest, "payment_intent.cancel")
	writeIdentityField(digest, command.OwnerID.String())
	writeIdentityField(digest, command.PaymentIntentID.String())
	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], digest.Sum(nil))
	return keyHash, fingerprint, nil
}

type identityWriter interface{ Write([]byte) (int, error) }

func writeIdentityField(writer identityWriter, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = writer.Write(size[:])
	_, _ = writer.Write([]byte(value))
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

func normalizeStoreError(err error) error {
	switch {
	case errors.Is(err, ErrPaymentConflict):
		return ErrPaymentConflict
	case errors.Is(err, ErrControlFinalizationDeferred):
		return ErrControlFinalizationDeferred
	default:
		return ErrPaymentUnavailable
	}
}

func normalizeReservationError(err error) error {
	switch {
	case errors.Is(err, ErrPaymentNotFound):
		return ErrPaymentNotFound
	case errors.Is(err, ErrReservationNotPayable):
		return ErrReservationNotPayable
	case errors.Is(err, ErrPaymentConflict):
		return ErrPaymentConflict
	default:
		return ErrShardPaymentCommandDeferred
	}
}
