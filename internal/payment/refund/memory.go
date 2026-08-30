package refund

import (
	"context"
	"math"
	"sync"

	"github.com/google/uuid"
)

type requestKey struct {
	ownerID uuid.UUID
	keyHash Hash
}

type storedRequest struct {
	request              RefundRequest
	selectionFingerprint Hash
}

// MemoryStore is a concurrency-safe storage adapter for tests and deterministic
// local composition. It implements idempotency, snapshot compare-and-set, and
// one active refund request per ticket atomically.
type MemoryStore struct {
	mu              sync.RWMutex
	orders          map[uuid.UUID]OrderSnapshot
	requests        map[requestKey]storedRequest
	requestsByID    map[uuid.UUID]storedRequest
	requestByTicket map[uuid.UUID]uuid.UUID
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		orders:          make(map[uuid.UUID]OrderSnapshot),
		requests:        make(map[requestKey]storedRequest),
		requestsByID:    make(map[uuid.UUID]storedRequest),
		requestByTicket: make(map[uuid.UUID]uuid.UUID),
	}
}

func (store *MemoryStore) GetRequest(ctx context.Context, ownerID, requestID uuid.UUID) (RefundRequest, bool, error) {
	if err := ctx.Err(); err != nil {
		return RefundRequest{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	record, found := store.requestsByID[requestID]
	if !found || record.request.OwnerID != ownerID {
		return RefundRequest{}, false, nil
	}
	return cloneRefundRequest(record.request), true, nil
}

func (store *MemoryStore) PutOrder(snapshot OrderSnapshot) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.orders[snapshot.ID] = cloneOrderSnapshot(snapshot)
}

func (store *MemoryStore) FindRequest(ctx context.Context, lookup Lookup) (RefundRequest, Hash, bool, error) {
	if err := ctx.Err(); err != nil {
		return RefundRequest{}, Hash{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	record, found := store.requests[keyFromLookup(lookup)]
	return cloneRefundRequest(record.request), record.selectionFingerprint, found, nil
}

func (store *MemoryStore) LoadOrder(ctx context.Context, ownerID, orderID uuid.UUID) (OrderSnapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return OrderSnapshot{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	snapshot, found := store.orders[orderID]
	if !found || snapshot.OwnerID != ownerID {
		return OrderSnapshot{}, false, nil
	}
	return cloneOrderSnapshot(snapshot), found, nil
}

func (store *MemoryStore) CreateRequest(ctx context.Context, command CreateCommand) (RefundRequest, bool, error) {
	if err := ctx.Err(); err != nil {
		return RefundRequest{}, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	key := keyFromLookup(command.Lookup)
	if existing, found := store.requests[key]; found {
		if existing.selectionFingerprint != command.SelectionFingerprint {
			return RefundRequest{}, false, ErrIdempotencyConflict
		}
		return cloneRefundRequest(existing.request), false, nil
	}
	snapshot, found := store.orders[command.OrderID]
	if !found || snapshot.OwnerID != command.OwnerID {
		return RefundRequest{}, false, ErrNotFound
	}
	for _, ticketID := range command.Request.TicketIDs {
		if _, exists := store.requestByTicket[ticketID]; exists {
			return RefundRequest{}, false, ErrTicketUnavailable
		}
	}
	if snapshot.Version != command.ExpectedVersion {
		return RefundRequest{}, false, ErrSnapshotConflict
	}
	if err := verifyCreate(snapshot, command.Request); err != nil {
		return RefundRequest{}, false, err
	}
	stored := storedRequest{request: cloneRefundRequest(command.Request), selectionFingerprint: command.SelectionFingerprint}
	store.requests[key] = stored
	store.requestsByID[stored.request.ID] = stored
	selected := make(map[uuid.UUID]struct{}, len(command.Request.TicketIDs))
	for _, ticketID := range command.Request.TicketIDs {
		selected[ticketID] = struct{}{}
		store.requestByTicket[ticketID] = command.Request.ID
	}
	for index := range snapshot.Tickets {
		if _, ok := selected[snapshot.Tickets[index].ID]; ok {
			snapshot.Tickets[index].State = TicketRefundPending
		}
	}
	snapshot.Version++
	store.orders[snapshot.ID] = cloneOrderSnapshot(snapshot)
	return cloneRefundRequest(stored.request), true, nil
}

func verifyCreate(snapshot OrderSnapshot, request RefundRequest) error {
	if request.ID == uuid.Nil || request.OrderID != snapshot.ID || request.OwnerID != snapshot.OwnerID ||
		request.PaymentIntentID != snapshot.PaymentIntentID || request.ReservationID != snapshot.ReservationID ||
		request.TrainRunID != snapshot.TrainRunID || request.AssignmentGeneration != snapshot.Version ||
		request.ProviderPaymentID != snapshot.ProviderPaymentID || request.CapturedMinor != snapshot.CapturedMinor ||
		request.RefundedBeforeMinor != snapshot.RefundedMinor || request.Provider != snapshot.Provider ||
		request.ShardID != snapshot.ShardID || request.Currency != snapshot.Currency ||
		request.AmountMinor <= 0 || request.State != SagaCreated || request.EligibilityCutoffAt.IsZero() ||
		!request.CreatedAt.Before(request.EligibilityCutoffAt) {
		return ErrSnapshotConflict
	}
	byID := make(map[uuid.UUID]TicketSnapshot, len(snapshot.Tickets))
	for _, ticket := range snapshot.Tickets {
		byID[ticket.ID] = ticket
	}
	total := int64(0)
	for _, ticketID := range request.TicketIDs {
		ticket, found := byID[ticketID]
		if !found || ticket.State != TicketActive || ticket.Currency != request.Currency || ticket.FareMinor <= 0 {
			return ErrTicketUnavailable
		}
		if total > math.MaxInt64-ticket.FareMinor {
			return ErrAmountOverflow
		}
		total += ticket.FareMinor
	}
	if total != request.AmountMinor || total > snapshot.CapturedMinor-snapshot.RefundedMinor {
		return ErrSnapshotConflict
	}
	return nil
}

func keyFromLookup(lookup Lookup) requestKey {
	return requestKey{ownerID: lookup.OwnerID, keyHash: lookup.IdempotencyHash}
}

var _ Store = (*MemoryStore)(nil)
