// Package refund owns whole-ticket subset refund eligibility and durable
// request identity. Its client request has no money, currency, provider, fee,
// exchange-rate, or shard field; those facts are derived from locked server
// snapshots and rechecked by the Store adapter.
package refund

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

const maxRequestFieldBytes = 200

var (
	ErrInvalidService        = errors.New("invalid ticket refund service")
	ErrInvalidRequest        = errors.New("invalid ticket refund request")
	ErrNotFound              = errors.New("ticket order not found")
	ErrCutoffPassed          = errors.New("ticket refund cutoff passed")
	ErrTicketUnavailable     = errors.New("ticket is not refundable")
	ErrCurrencyMismatch      = errors.New("ticket refund currency mismatch")
	ErrAmountOverflow        = errors.New("ticket refund amount overflow")
	ErrRefundLimit           = errors.New("ticket refund exceeds captured amount")
	ErrCapabilityUnavailable = errors.New("partial refund capability unavailable")
	ErrIdempotencyConflict   = errors.New("ticket refund idempotency conflict")
	ErrSnapshotConflict      = errors.New("ticket refund snapshot conflict")
)

type TicketState string

const (
	TicketActive        TicketState = "active"
	TicketRefundPending TicketState = "refund_pending"
	TicketRefunded      TicketState = "refunded"
	TicketCancelled     TicketState = "cancelled"
)

type Request struct {
	TicketIDs      []uuid.UUID
	IdempotencyKey string
}

type TicketSnapshot struct {
	ID        uuid.UUID
	State     TicketState
	FareMinor int64
	Currency  string
}

// OrderSnapshot is storage-owned server evidence. Provider and shard are not
// accepted from the caller and cannot be overridden by Request.
type OrderSnapshot struct {
	ID                     uuid.UUID
	OwnerID                uuid.UUID
	Version                uint64
	PaymentIntentID        uuid.UUID
	ReservationID          uuid.UUID
	TrainRunID             uuid.UUID
	ProviderPaymentID      string
	DepartureAt            time.Time
	CapturedMinor          int64
	RefundedMinor          int64
	Currency               string
	Provider               string
	ShardID                string
	PartialRefundSupported bool
	Tickets                []TicketSnapshot
}

type Hash [sha256.Size]byte

type RefundItem struct {
	TicketID  uuid.UUID
	FareMinor int64
}

type RefundRequest struct {
	ID                   uuid.UUID
	OrderID              uuid.UUID
	OwnerID              uuid.UUID
	PaymentIntentID      uuid.UUID
	ReservationID        uuid.UUID
	TrainRunID           uuid.UUID
	ProviderPaymentID    string
	AssignmentGeneration uint64
	TicketIDs            []uuid.UUID
	Items                []RefundItem
	AmountMinor          int64
	CapturedMinor        int64
	RefundedBeforeMinor  int64
	Currency             string
	Provider             string
	ShardID              string
	Fingerprint          Hash
	State                SagaState
	CreatedAt            time.Time
	UpdatedAt            time.Time
	CompletedAt          *time.Time
	EligibilityCutoffAt  time.Time
	// Replayed is invocation-local evidence that Request returned an existing
	// durable idempotency record. Stores do not persist this field.
	Replayed bool
}

type Lookup struct {
	OwnerID         uuid.UUID
	OrderID         uuid.UUID
	IdempotencyHash Hash
}

type CreateCommand struct {
	Lookup
	ExpectedVersion      uint64
	SelectionFingerprint Hash
	Request              RefundRequest
}

// Store must recheck ExpectedVersion and selected-ticket state atomically with
// request creation. Exact idempotency replays return the original request;
// changed selection under one key returns ErrIdempotencyConflict.
type Store interface {
	FindRequest(context.Context, Lookup) (RefundRequest, Hash, bool, error)
	GetRequest(context.Context, uuid.UUID, uuid.UUID) (RefundRequest, bool, error)
	LoadOrder(context.Context, uuid.UUID, uuid.UUID) (OrderSnapshot, bool, error)
	CreateRequest(context.Context, CreateCommand) (RefundRequest, bool, error)
}

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type Policy struct {
	CutoffBeforeDeparture time.Duration
	MaxTickets            int
	Clock                 Clock
}

type Service struct {
	store  Store
	cutoff time.Duration
	max    int
	clock  Clock
}

func NewService(store Store, policy Policy) (*Service, error) {
	if store == nil || policy.CutoffBeforeDeparture < 0 || policy.MaxTickets <= 0 || policy.MaxTickets > 1000 {
		return nil, ErrInvalidService
	}
	clock := policy.Clock
	if clock == nil {
		clock = realClock{}
	}
	return &Service{store: store, cutoff: policy.CutoffBeforeDeparture, max: policy.MaxTickets, clock: clock}, nil
}

// Request creates or replays one immutable whole-ticket refund request.
func (service *Service) Request(ctx context.Context, ownerID, orderID uuid.UUID, input Request) (RefundRequest, error) {
	if service == nil || service.store == nil || service.clock == nil {
		return RefundRequest{}, ErrInvalidService
	}
	if ownerID == uuid.Nil || orderID == uuid.Nil {
		return RefundRequest{}, ErrInvalidRequest
	}
	ticketIDs, idempotencyKey, err := normalizeRequest(input, service.max)
	if err != nil {
		return RefundRequest{}, err
	}
	idempotencyHash := sha256.Sum256([]byte(idempotencyKey))
	selectionFingerprint, err := selectionHash(ownerID, orderID, ticketIDs)
	if err != nil {
		return RefundRequest{}, errors.Join(ErrInvalidRequest, err)
	}
	lookup := Lookup{OwnerID: ownerID, OrderID: orderID, IdempotencyHash: idempotencyHash}
	existing, found, err := service.findExactReplay(ctx, lookup, selectionFingerprint)
	if err != nil {
		return RefundRequest{}, err
	}
	if found {
		existing.Replayed = true
		return existing, nil
	}

	snapshot, found, err := service.store.LoadOrder(ctx, ownerID, orderID)
	if err != nil {
		return RefundRequest{}, err
	}
	if !found || snapshot.OwnerID != ownerID {
		return RefundRequest{}, ErrNotFound
	}
	now := service.clock.Now().UTC()
	if now.IsZero() {
		return RefundRequest{}, ErrInvalidService
	}
	amount, currency, err := deriveMoney(snapshot, ticketIDs, now, service.cutoff)
	if err != nil {
		// Another exact request may have won after the first lookup and moved the
		// selected tickets to refund_pending. Recheck the immutable idempotency
		// record before reporting that raced snapshot as non-refundable.
		existing, replayed, replayErr := service.findExactReplay(ctx, lookup, selectionFingerprint)
		if replayErr != nil {
			return RefundRequest{}, replayErr
		}
		if replayed {
			existing.Replayed = true
			return existing, nil
		}
		return RefundRequest{}, err
	}
	eligibilityCutoffAt := snapshot.DepartureAt.UTC().Add(-service.cutoff)
	fingerprint, err := requestHash(ownerID, snapshot, ticketIDs, amount, currency, eligibilityCutoffAt)
	if err != nil {
		return RefundRequest{}, errors.Join(ErrInvalidRequest, err)
	}
	items := selectedItems(snapshot, ticketIDs)
	record := RefundRequest{
		ID: uuid.New(), OrderID: orderID, OwnerID: ownerID,
		PaymentIntentID: snapshot.PaymentIntentID, ReservationID: snapshot.ReservationID,
		TrainRunID: snapshot.TrainRunID, ProviderPaymentID: snapshot.ProviderPaymentID,
		AssignmentGeneration: snapshot.Version, TicketIDs: append([]uuid.UUID(nil), ticketIDs...), Items: items,
		AmountMinor: amount, CapturedMinor: snapshot.CapturedMinor, RefundedBeforeMinor: snapshot.RefundedMinor,
		Currency: currency, Provider: snapshot.Provider, ShardID: snapshot.ShardID,
		Fingerprint: fingerprint, State: SagaCreated, CreatedAt: now, UpdatedAt: now,
		EligibilityCutoffAt: eligibilityCutoffAt,
	}
	stored, created, err := service.store.CreateRequest(ctx, CreateCommand{
		Lookup: lookup, ExpectedVersion: snapshot.Version, SelectionFingerprint: selectionFingerprint, Request: record,
	})
	if err != nil {
		return RefundRequest{}, err
	}
	stored.Replayed = !created
	return cloneRefundRequest(stored), nil
}

func (service *Service) findExactReplay(ctx context.Context, lookup Lookup, selectionFingerprint Hash) (RefundRequest, bool, error) {
	existing, existingSelection, found, err := service.store.FindRequest(ctx, lookup)
	if err != nil {
		return RefundRequest{}, false, err
	}
	if !found {
		return RefundRequest{}, false, nil
	}
	if existingSelection != selectionFingerprint {
		return RefundRequest{}, false, ErrIdempotencyConflict
	}
	return cloneRefundRequest(existing), true, nil
}

// Get returns one refund only to its authenticated owner.
func (service *Service) Get(ctx context.Context, ownerID, requestID uuid.UUID) (RefundRequest, error) {
	if service == nil || service.store == nil || ownerID == uuid.Nil || requestID == uuid.Nil {
		return RefundRequest{}, ErrInvalidRequest
	}
	request, found, err := service.store.GetRequest(ctx, ownerID, requestID)
	if err != nil {
		return RefundRequest{}, err
	}
	if !found {
		return RefundRequest{}, ErrNotFound
	}
	return cloneRefundRequest(request), nil
}

func selectedItems(snapshot OrderSnapshot, selected []uuid.UUID) []RefundItem {
	fares := make(map[uuid.UUID]int64, len(snapshot.Tickets))
	for _, ticket := range snapshot.Tickets {
		fares[ticket.ID] = ticket.FareMinor
	}
	items := make([]RefundItem, 0, len(selected))
	for _, ticketID := range selected {
		items = append(items, RefundItem{TicketID: ticketID, FareMinor: fares[ticketID]})
	}
	return items
}

func normalizeRequest(input Request, maxTickets int) ([]uuid.UUID, string, error) {
	key := strings.TrimSpace(input.IdempotencyKey)
	if key == "" || len(key) > maxRequestFieldBytes || len(input.TicketIDs) == 0 || len(input.TicketIDs) > maxTickets {
		return nil, "", ErrInvalidRequest
	}
	ticketIDs := append([]uuid.UUID(nil), input.TicketIDs...)
	for _, ticketID := range ticketIDs {
		if ticketID == uuid.Nil {
			return nil, "", ErrInvalidRequest
		}
	}
	sort.Slice(ticketIDs, func(left, right int) bool { return bytes.Compare(ticketIDs[left][:], ticketIDs[right][:]) < 0 })
	unique := ticketIDs[:0]
	for _, ticketID := range ticketIDs {
		if len(unique) == 0 || unique[len(unique)-1] != ticketID {
			unique = append(unique, ticketID)
		}
	}
	return append([]uuid.UUID(nil), unique...), key, nil
}

func deriveMoney(snapshot OrderSnapshot, selected []uuid.UUID, now time.Time, cutoff time.Duration) (int64, string, error) {
	if snapshot.ID == uuid.Nil || snapshot.OwnerID == uuid.Nil || snapshot.Version == 0 ||
		snapshot.PaymentIntentID == uuid.Nil || snapshot.ReservationID == uuid.Nil || snapshot.TrainRunID == uuid.Nil ||
		snapshot.DepartureAt.IsZero() ||
		snapshot.CapturedMinor <= 0 || snapshot.RefundedMinor < 0 || snapshot.RefundedMinor > snapshot.CapturedMinor ||
		!validCurrency(snapshot.Currency) || !validBounded(snapshot.Provider) || !validBounded(snapshot.ProviderPaymentID) || !validBounded(snapshot.ShardID) {
		return 0, "", ErrSnapshotConflict
	}
	if !snapshot.PartialRefundSupported {
		return 0, "", ErrCapabilityUnavailable
	}
	if !now.Before(snapshot.DepartureAt.Add(-cutoff)) {
		return 0, "", ErrCutoffPassed
	}
	byID := make(map[uuid.UUID]TicketSnapshot, len(snapshot.Tickets))
	for _, ticket := range snapshot.Tickets {
		if ticket.ID == uuid.Nil {
			return 0, "", ErrSnapshotConflict
		}
		if _, duplicate := byID[ticket.ID]; duplicate {
			return 0, "", ErrSnapshotConflict
		}
		byID[ticket.ID] = ticket
	}
	amount := int64(0)
	for _, ticketID := range selected {
		ticket, found := byID[ticketID]
		if !found || ticket.State != TicketActive || ticket.FareMinor <= 0 {
			return 0, "", ErrTicketUnavailable
		}
		if ticket.Currency != snapshot.Currency {
			return 0, "", ErrCurrencyMismatch
		}
		if amount > math.MaxInt64-ticket.FareMinor {
			return 0, "", ErrAmountOverflow
		}
		amount += ticket.FareMinor
	}
	if amount <= 0 || amount > snapshot.CapturedMinor-snapshot.RefundedMinor {
		return 0, "", ErrRefundLimit
	}
	return amount, snapshot.Currency, nil
}

func selectionHash(ownerID, orderID uuid.UUID, ticketIDs []uuid.UUID) (Hash, error) {
	payload := struct {
		OwnerID   uuid.UUID   `json:"owner_id"`
		OrderID   uuid.UUID   `json:"order_id"`
		TicketIDs []uuid.UUID `json:"ticket_ids"`
	}{ownerID, orderID, ticketIDs}
	return marshalHash(payload)
}

// SelectionFingerprint exposes the canonical dedupe identity to durable store
// adapters. Callers must provide the already normalized, sorted ticket set.
func SelectionFingerprint(ownerID, orderID uuid.UUID, ticketIDs []uuid.UUID) (Hash, error) {
	return selectionHash(ownerID, orderID, ticketIDs)
}

func requestHash(ownerID uuid.UUID, snapshot OrderSnapshot, ticketIDs []uuid.UUID, amount int64, currency string, eligibilityCutoffAt time.Time) (Hash, error) {
	payload := struct {
		OwnerID              uuid.UUID   `json:"owner_id"`
		OrderID              uuid.UUID   `json:"order_id"`
		PaymentIntentID      uuid.UUID   `json:"payment_intent_id"`
		ReservationID        uuid.UUID   `json:"reservation_id"`
		TrainRunID           uuid.UUID   `json:"train_run_id"`
		AssignmentGeneration uint64      `json:"assignment_generation"`
		TicketIDs            []uuid.UUID `json:"ticket_ids"`
		AmountMinor          int64       `json:"amount_minor"`
		Currency             string      `json:"currency"`
		Provider             string      `json:"provider"`
		ProviderPaymentID    string      `json:"provider_payment_id"`
		ShardID              string      `json:"shard_id"`
		EligibilityCutoffAt  time.Time   `json:"eligibility_cutoff_at"`
	}{ownerID, snapshot.ID, snapshot.PaymentIntentID, snapshot.ReservationID, snapshot.TrainRunID,
		snapshot.Version, ticketIDs, amount, currency, snapshot.Provider, snapshot.ProviderPaymentID, snapshot.ShardID, eligibilityCutoffAt.UTC()}
	return marshalHash(payload)
}

func marshalHash(value any) (Hash, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return Hash{}, err
	}
	return sha256.Sum256(encoded), nil
}

func validCurrency(currency string) bool {
	if len(currency) != 3 {
		return false
	}
	for index := range currency {
		if currency[index] < 'A' || currency[index] > 'Z' {
			return false
		}
	}
	return true
}

func validBounded(value string) bool {
	trimmed := strings.TrimSpace(value)
	return trimmed != "" && trimmed == value && len(value) <= maxRequestFieldBytes
}

func cloneOrderSnapshot(snapshot OrderSnapshot) OrderSnapshot {
	snapshot.Tickets = append([]TicketSnapshot(nil), snapshot.Tickets...)
	return snapshot
}

func cloneRefundRequest(request RefundRequest) RefundRequest {
	request.TicketIDs = append([]uuid.UUID(nil), request.TicketIDs...)
	request.Items = append([]RefundItem(nil), request.Items...)
	if request.CompletedAt != nil {
		completedAt := *request.CompletedAt
		request.CompletedAt = &completedAt
	}
	return request
}
