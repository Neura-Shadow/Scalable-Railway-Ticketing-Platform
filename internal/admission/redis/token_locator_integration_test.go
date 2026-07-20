package admissionredis_test

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	"github.com/google/uuid"
)

func TestTokenLocatorIsExactBoundedAndDeletable(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2tokenlocator_")
	tokenHash := sha256.Sum256([]byte("test-only opaque bearer"))

	if err := h.store.PutTokenLocator(h.ctx, tokenHash, h.scope, time.Hour); err != nil {
		t.Fatalf("PutTokenLocator() error = %v", err)
	}
	resolved, err := h.store.ResolveTokenLocator(h.ctx, tokenHash)
	if err != nil {
		t.Fatalf("ResolveTokenLocator() error = %v", err)
	}
	if resolved != h.scope {
		t.Fatalf("ResolveTokenLocator() = %#v, want %#v", resolved, h.scope)
	}
	if err := h.store.DeleteTokenLocator(h.ctx, tokenHash); err != nil {
		t.Fatalf("DeleteTokenLocator() error = %v", err)
	}
	if _, err := h.store.ResolveTokenLocator(h.ctx, tokenHash); !errors.Is(err, admissionredis.ErrNotFound) {
		t.Fatalf("ResolveTokenLocator() after delete error = %v, want %v", err, admissionredis.ErrNotFound)
	}
}

func TestRepeatedBlockedIssueBatchesKeepLocatorCardinalityBounded(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2blockedlocators_")
	entryIDs := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}

	const passes = 100
	for pass := 0; pass < passes; pass++ {
		locators := make([]admissionredis.IssueLocator, 0, len(entryIDs))
		tokenHashes := make([][sha256.Size]byte, 0, len(entryIDs))
		for index, entryID := range entryIDs {
			tokenHash := sha256.Sum256([]byte(fmt.Sprintf("blocked-%d-%d", pass, index)))
			locators = append(locators, admissionredis.IssueLocator{
				EntryID:   entryID,
				TokenHash: tokenHash,
			})
			tokenHashes = append(tokenHashes, tokenHash)
		}
		if err := h.store.PutIssueLocators(h.ctx, locators, h.scope, time.Hour, time.Hour); err != nil {
			t.Fatalf("PutIssueLocators pass %d error = %v", pass, err)
		}
		if err := h.store.DeleteTokenLocators(h.ctx, tokenHashes); err != nil {
			t.Fatalf("DeleteTokenLocators pass %d error = %v", pass, err)
		}
	}

	assertLocatorKeyCount(t, h, h.namespace+":wr:entry-locator:*", int64(len(entryIDs)))
	assertLocatorKeyCount(t, h, h.namespace+":wr:token-locator:*", 0)
}

func assertLocatorKeyCount(t *testing.T, h *liveAdmissionRedis, pattern string, want int64) {
	t.Helper()
	iterator := h.client.Scan(h.ctx, 0, pattern, 100).Iterator()
	var count int64
	for iterator.Next(h.ctx) {
		count++
	}
	if err := iterator.Err(); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("locator key count for %q = %d, want %d", pattern, count, want)
	}
}
