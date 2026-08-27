package settlement

import (
	"context"
	"strconv"
	"sync"
)

type MemoryDetectionStore struct {
	mu          sync.RWMutex
	comparisons map[string][]Comparison
	runs        []DetectionRun
}

func NewMemoryDetectionStore() *MemoryDetectionStore {
	return &MemoryDetectionStore{comparisons: make(map[string][]Comparison)}
}

func (store *MemoryDetectionStore) SetComparisons(scope DetectionScope, comparisons []Comparison) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.comparisons[detectionScopeKey(scope)] = append([]Comparison(nil), comparisons...)
}

func (store *MemoryDetectionStore) ReadDetectionPage(ctx context.Context, scope DetectionScope, cursor string, limit int) (DetectionPage, error) {
	if err := ctx.Err(); err != nil {
		return DetectionPage{}, err
	}
	start := 0
	if cursor != "" {
		parsed, err := strconv.Atoi(cursor)
		if err != nil || parsed < 0 {
			return DetectionPage{}, ErrDetectionCursor
		}
		start = parsed
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	comparisons := store.comparisons[detectionScopeKey(scope)]
	if start >= len(comparisons) {
		return DetectionPage{NextCursor: cursor, Done: true}, nil
	}
	end := start + limit
	if end > len(comparisons) {
		end = len(comparisons)
	}
	return DetectionPage{
		Comparisons: append([]Comparison(nil), comparisons[start:end]...),
		NextCursor:  strconv.Itoa(end),
		Done:        end == len(comparisons),
	}, nil
}

func (store *MemoryDetectionStore) AppendDetectionRun(ctx context.Context, run DetectionRun) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.runs = append(store.runs, cloneDetectionRun(run))
	return nil
}

func (store *MemoryDetectionStore) Runs() []DetectionRun {
	store.mu.RLock()
	defer store.mu.RUnlock()
	runs := make([]DetectionRun, len(store.runs))
	for index, run := range store.runs {
		runs[index] = cloneDetectionRun(run)
	}
	return runs
}

func cloneDetectionRun(run DetectionRun) DetectionRun {
	run.Findings = append([]Finding(nil), run.Findings...)
	return run
}

func detectionScopeKey(scope DetectionScope) string { return string(scope.Kind) + "\x00" + scope.Value }

var _ DetectionStore = (*MemoryDetectionStore)(nil)
