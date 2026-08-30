package ledger

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

// MemoryStore is a concurrency-safe Store adapter intended for domain tests and
// deterministic local composition. It mirrors the uniqueness guarantees a
// durable adapter must enforce in one transaction.
type MemoryStore struct {
	mu         sync.RWMutex
	byID       map[uuid.UUID]Transaction
	byEvent    map[string]uuid.UUID
	reversedBy map[uuid.UUID]uuid.UUID
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		byID:       make(map[uuid.UUID]Transaction),
		byEvent:    make(map[string]uuid.UUID),
		reversedBy: make(map[uuid.UUID]uuid.UUID),
	}
}

func (store *MemoryStore) Append(ctx context.Context, candidate Transaction) (Transaction, bool, error) {
	if err := ctx.Err(); err != nil {
		return Transaction{}, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.appendLocked(candidate)
}

func (store *MemoryStore) AppendReversal(ctx context.Context, originalID uuid.UUID, candidate Transaction) (Transaction, bool, error) {
	if err := ctx.Err(); err != nil {
		return Transaction{}, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	if existingID, exists := store.byEvent[candidate.EventID]; exists {
		existing := store.byID[existingID]
		if sameMemoryCanonicalIdentity(existing, candidate) {
			return cloneTransaction(existing), false, nil
		}
		return Transaction{}, false, ErrEventConflict
	}
	if _, exists := store.byID[originalID]; !exists {
		return Transaction{}, false, ErrNotFound
	}
	if _, exists := store.reversedBy[originalID]; exists {
		return Transaction{}, false, ErrAlreadyReversed
	}
	if candidate.ReversalOf == nil || *candidate.ReversalOf != originalID {
		return Transaction{}, false, ErrStoreConflict
	}
	stored, created, err := store.appendLocked(candidate)
	if err != nil || !created {
		return stored, created, err
	}
	store.reversedBy[originalID] = stored.ID
	return stored, true, nil
}

func (store *MemoryStore) Get(ctx context.Context, id uuid.UUID) (Transaction, bool, error) {
	if err := ctx.Err(); err != nil {
		return Transaction{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	transaction, found := store.byID[id]
	return cloneTransaction(transaction), found, nil
}

func (store *MemoryStore) appendLocked(candidate Transaction) (Transaction, bool, error) {
	if existingID, exists := store.byEvent[candidate.EventID]; exists {
		existing := store.byID[existingID]
		if sameMemoryCanonicalIdentity(existing, candidate) {
			return cloneTransaction(existing), false, nil
		}
		return Transaction{}, false, ErrEventConflict
	}
	if candidate.ID == uuid.Nil || candidate.EventID == "" {
		return Transaction{}, false, ErrStoreConflict
	}
	if _, exists := store.byID[candidate.ID]; exists {
		return Transaction{}, false, ErrStoreConflict
	}
	stored := cloneTransaction(candidate)
	store.byID[stored.ID] = stored
	store.byEvent[stored.EventID] = stored.ID
	return cloneTransaction(stored), true, nil
}

func sameMemoryCanonicalIdentity(existing, candidate Transaction) bool {
	if existing.ID != candidate.ID || existing.Fingerprint != candidate.Fingerprint {
		return false
	}
	if existing.ReversalOf == nil || candidate.ReversalOf == nil {
		return existing.ReversalOf == nil && candidate.ReversalOf == nil
	}
	return *existing.ReversalOf == *candidate.ReversalOf
}

var _ Store = (*MemoryStore)(nil)
