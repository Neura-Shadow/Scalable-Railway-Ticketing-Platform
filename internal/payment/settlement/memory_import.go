package settlement

import (
	"context"
	"sync"
)

type importRecordKey struct {
	scope    string
	kind     RecordKind
	identity string
}

type PayloadConflict struct {
	Scope        AccountScope
	Kind         RecordKind
	ProviderID   string
	StoredHash   PayloadHash
	IncomingHash PayloadHash
}

type MemoryImportStore struct {
	mu          sync.RWMutex
	checkpoints map[string]string
	records     map[importRecordKey]ImportedRecord
	conflicts   map[string][]PayloadConflict
}

func NewMemoryImportStore() *MemoryImportStore {
	return &MemoryImportStore{
		checkpoints: make(map[string]string),
		records:     make(map[importRecordKey]ImportedRecord),
		conflicts:   make(map[string][]PayloadConflict),
	}
}

func (store *MemoryImportStore) Checkpoint(ctx context.Context, scope AccountScope) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.checkpoints[scopeKey(scope)], nil
}

func (store *MemoryImportStore) CommitPage(ctx context.Context, commit PageCommit) (CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return CommitResult{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	key := scopeKey(commit.Scope)
	if store.checkpoints[key] != commit.ExpectedCursor {
		return CommitResult{}, ErrCheckpointConflict
	}
	result := CommitResult{}
	for _, record := range commit.Records {
		recordKey := importRecordKey{scope: key, kind: record.Kind, identity: record.ProviderID}
		existing, found := store.records[recordKey]
		if !found {
			store.records[recordKey] = record
			result.Inserted++
			continue
		}
		if existing.PayloadHash == record.PayloadHash {
			result.Replayed++
			continue
		}
		result.Conflicts++
		conflict := PayloadConflict{
			Scope: commit.Scope, Kind: record.Kind, ProviderID: record.ProviderID,
			StoredHash: existing.PayloadHash, IncomingHash: record.PayloadHash,
		}
		if !containsConflict(store.conflicts[key], conflict) {
			store.conflicts[key] = append(store.conflicts[key], conflict)
		}
	}
	store.checkpoints[key] = commit.NextCursor
	return result, nil
}

func (store *MemoryImportStore) Record(ctx context.Context, scope AccountScope, kind RecordKind, providerID string) (ImportedRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return ImportedRecord{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	record, found := store.records[importRecordKey{scope: scopeKey(scope), kind: kind, identity: providerID}]
	return record, found, nil
}

func (store *MemoryImportStore) PayloadConflicts(ctx context.Context, scope AccountScope) ([]PayloadConflict, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return append([]PayloadConflict(nil), store.conflicts[scopeKey(scope)]...), nil
}

func containsConflict(conflicts []PayloadConflict, candidate PayloadConflict) bool {
	for _, conflict := range conflicts {
		if conflict.Kind == candidate.Kind && conflict.ProviderID == candidate.ProviderID &&
			conflict.StoredHash == candidate.StoredHash && conflict.IncomingHash == candidate.IncomingHash {
			return true
		}
	}
	return false
}

func scopeKey(scope AccountScope) string { return scope.Provider + "\x00" + scope.AccountID }

var _ ImportStore = (*MemoryImportStore)(nil)

type MemorySource struct {
	mu    sync.RWMutex
	pages map[string]Page
	calls []string
	err   error
}

func NewMemorySource(pages map[string]Page) *MemorySource {
	copyPages := make(map[string]Page, len(pages))
	for cursor, page := range pages {
		copyPages[cursor] = clonePage(page)
	}
	return &MemorySource{pages: copyPages}
}

func (source *MemorySource) ListPage(ctx context.Context, _ AccountScope, cursor string, _ int) (Page, error) {
	if err := ctx.Err(); err != nil {
		return Page{}, err
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	source.calls = append(source.calls, cursor)
	if source.err != nil {
		return Page{}, source.err
	}
	page, found := source.pages[cursor]
	if !found {
		return Page{NextCursor: cursor, Done: true}, nil
	}
	return clonePage(page), nil
}

func (source *MemorySource) SetPage(cursor string, page Page) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.pages[cursor] = clonePage(page)
}

func (source *MemorySource) SetError(err error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.err = err
}

func (source *MemorySource) Calls() []string {
	source.mu.RLock()
	defer source.mu.RUnlock()
	return append([]string(nil), source.calls...)
}

func clonePage(page Page) Page {
	page.Records = append([]Record(nil), page.Records...)
	return page
}

var _ Source = (*MemorySource)(nil)
