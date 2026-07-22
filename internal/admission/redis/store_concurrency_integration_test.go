package admissionredis_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

type liveAdmissionRedis struct {
	address   string
	namespace string
	ctx       context.Context
	client    *goredis.Client
	store     *admissionredis.Store
	scope     admissionredis.PolicyScope
	keyring   *domain.TokenKeyring
}

func newLiveAdmissionRedis(t *testing.T, prefix string) *liveAdmissionRedis {
	t.Helper()
	address := strings.TrimSpace(os.Getenv("TEST_REDIS_ADDR"))
	if address == "" {
		t.Skip("TEST_REDIS_ADDR is not set; skipping admission Redis integration test")
	}
	ctx := context.Background()
	client := goredis.NewClient(&goredis.Options{Addr: address})
	var pingErr error
	for attempt := 0; attempt < 3; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		pingErr = client.Ping(pingCtx).Err()
		cancel()
		if pingErr == nil {
			break
		}
	}
	if pingErr != nil {
		_ = client.Close()
		t.Fatalf("ping Redis after three bounded attempts: %v", pingErr)
	}
	namespace := prefix + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	store, err := admissionredis.NewStore(client, namespace)
	if err != nil {
		_ = client.Close()
		t.Fatalf("create admission Redis store: %v", err)
	}
	scope := admissionredis.PolicyScope{
		PolicyID: uuid.NewString(), TrainRunID: uuid.NewString(), SeatClass: "standard", Version: 1,
	}
	if err := store.InstallPolicy(ctx, scope, true, time.Hour); err != nil {
		_ = client.Close()
		t.Fatalf("install policy: %v", err)
	}
	keyring, err := domain.NewTokenKeyring("integration", map[string][]byte{
		"integration": bytes.Repeat([]byte{0x63}, sha256.Size),
	})
	if err != nil {
		_ = client.Close()
		t.Fatalf("create token keyring: %v", err)
	}
	harness := &liveAdmissionRedis{
		address: address, namespace: namespace, ctx: ctx, client: client,
		store: store, scope: scope, keyring: keyring,
	}
	t.Cleanup(func() {
		var cursor uint64
		for {
			keys, next, scanErr := client.Scan(ctx, cursor, namespace+":*", 1_000).Result()
			if scanErr != nil {
				break
			}
			if len(keys) > 0 {
				_ = client.Del(ctx, keys...).Err()
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		_ = client.Close()
	})
	return harness
}

func (h *liveAdmissionRedis) join(
	t *testing.T,
	owner [sha256.Size]byte,
	fingerprint [sha256.Size]byte,
	fromStopIndex int,
	toStopIndex int,
	maxQueueSize int,
) (domain.WaitingRoomEntry, error) {
	t.Helper()
	entry, _, err := h.store.Join(h.ctx, admissionredis.JoinRequest{
		Scope: h.scope, EntryID: uuid.NewString(), OwnerHash: owner,
		AdmissionFingerprint: fingerprint, FromStopIndex: fromStopIndex,
		ToStopIndex: toStopIndex, PassengerCount: 1,
		MaxQueueSize: maxQueueSize, EntryTTL: time.Hour,
	})
	return entry, err
}

func (h *liveAdmissionRedis) issueCandidate(
	t *testing.T,
	entry domain.WaitingRoomEntry,
	issuedAt time.Time,
) admissionredis.IssueCandidate {
	t.Helper()
	token, err := h.keyring.Issue(domain.TokenClaims{
		PolicyID: h.scope.PolicyID, PolicyVersion: h.scope.Version, EntryID: entry.ID,
		OwnerHash: entry.OwnerHash, AdmissionFingerprint: entry.AdmissionFingerprint,
		IssuedAt: issuedAt.UTC(), ExpiresAt: issuedAt.Add(30 * time.Second).UTC(),
	})
	if err != nil {
		t.Fatalf("issue token for entry %s: %v", entry.ID, err)
	}
	return admissionredis.IssueCandidate{EntryID: entry.ID, Token: token.Fields}
}

func TestConcurrentMismatchedJoinsLeaveOneActiveEntry(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2joinrace_")
	const attempts = 64
	start := make(chan struct{})
	results := make(chan domain.WaitingRoomEntry, attempts)
	errs := make(chan error, attempts)
	var group sync.WaitGroup

	for index := 0; index < attempts; index++ {
		index := index
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			fingerprint, fingerprintErr := domain.FingerprintAdmissionRequest(domain.AdmissionFingerprintInput{
				TrainRunID: h.scope.TrainRunID, FromStopIndex: index,
				ToStopIndex: index + 1, SeatClass: h.scope.SeatClass, PassengerCount: 1,
			})
			if fingerprintErr != nil {
				errs <- fingerprintErr
				return
			}
			entry, joinErr := h.join(
				t, sha256.Sum256([]byte("one-customer")), fingerprint, index, index+1, attempts,
			)
			if joinErr != nil {
				errs <- joinErr
				return
			}
			results <- entry
		}()
	}
	close(start)
	group.Wait()
	close(results)
	close(errs)

	successes := make([]domain.WaitingRoomEntry, 0, 1)
	for entry := range results {
		successes = append(successes, entry)
	}
	conflicts := 0
	for err := range errs {
		if !errors.Is(err, admissionredis.ErrJoinConflict) {
			t.Fatalf("concurrent mismatched join error = %v, want ErrJoinConflict", err)
		}
		conflicts++
	}
	if len(successes) != 1 || conflicts != attempts-1 {
		t.Fatalf("successes = %d, conflicts = %d; want 1 and %d", len(successes), conflicts, attempts-1)
	}
	queued, err := h.store.PeekQueued(h.ctx, h.scope, attempts)
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 1 || queued[0].ID != successes[0].ID {
		t.Fatalf("active queue = %#v, want only winning entry %q", queued, successes[0].ID)
	}
}

func TestThousandConcurrentJoinsHaveUniqueMonotonicSequence(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2sequence_")
	const (
		total   = 1_000
		workers = 40
	)
	type joinResult struct {
		entry domain.WaitingRoomEntry
		err   error
	}
	jobs := make(chan int)
	results := make(chan joinResult, total)
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range jobs {
				owner := sha256.Sum256([]byte(fmt.Sprintf("sequence-owner-%04d", index)))
				fingerprint, fingerprintErr := domain.FingerprintAdmissionRequest(domain.AdmissionFingerprintInput{
					TrainRunID: h.scope.TrainRunID, FromStopIndex: index,
					ToStopIndex: index + 1, SeatClass: h.scope.SeatClass, PassengerCount: 1,
				})
				if fingerprintErr != nil {
					results <- joinResult{err: fingerprintErr}
					continue
				}
				entry, joinErr := h.join(t, owner, fingerprint, index, index+1, total)
				results <- joinResult{entry: entry, err: joinErr}
			}
		}()
	}
	for index := 0; index < total; index++ {
		jobs <- index
	}
	close(jobs)
	group.Wait()
	close(results)

	ids := make(map[string]struct{}, total)
	sequences := make([]int64, 0, total)
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if _, exists := ids[result.entry.ID]; exists {
			t.Fatalf("duplicate entry ID %q", result.entry.ID)
		}
		ids[result.entry.ID] = struct{}{}
		sequences = append(sequences, result.entry.Sequence)
	}
	if len(sequences) != total {
		t.Fatalf("joined entries = %d, want %d", len(sequences), total)
	}
	sort.Slice(sequences, func(left, right int) bool { return sequences[left] < sequences[right] })
	for index, sequence := range sequences {
		if sequence != int64(index+1) {
			t.Fatalf("sorted sequence[%d] = %d, want %d", index, sequence, index+1)
		}
	}
	queued, err := h.store.PeekQueued(h.ctx, h.scope, total)
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != total {
		t.Fatalf("queue length = %d, want %d", len(queued), total)
	}
	for index, entry := range queued {
		if entry.Sequence != int64(index+1) {
			t.Fatalf("queue[%d].Sequence = %d, want %d", index, entry.Sequence, index+1)
		}
	}
}

func TestConcurrentJoinsNeverExceedQueueCapacity(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2capacity_")
	const (
		attempts = 80
		capacity = 17
	)
	start := make(chan struct{})
	errs := make(chan error, attempts)
	var group sync.WaitGroup
	for index := 0; index < attempts; index++ {
		index := index
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			owner := sha256.Sum256([]byte(fmt.Sprintf("capacity-owner-%03d", index)))
			fingerprint := sha256.Sum256([]byte(fmt.Sprintf("capacity-request-%03d", index)))
			_, joinErr := h.join(t, owner, fingerprint, index, index+1, capacity)
			errs <- joinErr
		}()
	}
	close(start)
	group.Wait()
	close(errs)

	joined := 0
	full := 0
	for err := range errs {
		switch {
		case err == nil:
			joined++
		case errors.Is(err, admissionredis.ErrQueueFull):
			full++
		default:
			t.Fatalf("join error = %v, want nil or ErrQueueFull", err)
		}
	}
	if joined != capacity || full != attempts-capacity {
		t.Fatalf("joined = %d, full = %d; want %d and %d", joined, full, capacity, attempts-capacity)
	}
	queued, err := h.store.PeekQueued(h.ctx, h.scope, attempts)
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != capacity {
		t.Fatalf("queue length = %d, want capacity %d", len(queued), capacity)
	}
}

func TestConcurrentIssuersShareGlobalRateAndInflightBounds(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		rate          int
		inflight      int
		expectedIssue int
	}{
		{name: "rate", rate: 7, inflight: 50, expectedIssue: 7},
		{name: "inflight", rate: 50, inflight: 11, expectedIssue: 11},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			h := newLiveAdmissionRedis(t, "m2issuers_")
			const (
				queueSize = 50
				callers   = 12
			)
			entries := make([]domain.WaitingRoomEntry, 0, queueSize)
			for index := 0; index < queueSize; index++ {
				owner := sha256.Sum256([]byte(fmt.Sprintf("issuer-owner-%02d", index)))
				fingerprint := sha256.Sum256([]byte(fmt.Sprintf("issuer-request-%02d", index)))
				entry, err := h.join(t, owner, fingerprint, index, index+1, queueSize)
				if err != nil {
					t.Fatal(err)
				}
				entries = append(entries, entry)
			}

			stores := make([]*admissionredis.Store, 0, callers)
			clients := make([]*goredis.Client, 0, callers)
			for index := 0; index < callers; index++ {
				client := goredis.NewClient(&goredis.Options{Addr: h.address})
				store, err := admissionredis.NewStore(client, h.namespace)
				if err != nil {
					_ = client.Close()
					t.Fatal(err)
				}
				stores = append(stores, store)
				clients = append(clients, client)
			}
			t.Cleanup(func() {
				for _, client := range clients {
					_ = client.Close()
				}
			})

			issuedAt, err := h.store.Time(h.ctx)
			if err != nil {
				t.Fatal(err)
			}
			candidateSets := make([][]admissionredis.IssueCandidate, callers)
			for caller := 0; caller < callers; caller++ {
				candidates := make([]admissionredis.IssueCandidate, 0, queueSize)
				for _, entry := range entries {
					candidates = append(candidates, h.issueCandidate(t, entry, issuedAt))
				}
				candidateSets[caller] = candidates
			}

			start := make(chan struct{})
			results := make(chan admissionredis.IssueResult, callers)
			errs := make(chan error, callers)
			var group sync.WaitGroup
			for caller := 0; caller < callers; caller++ {
				caller := caller
				group.Add(1)
				go func() {
					defer group.Done()
					<-start
					result, issueErr := stores[caller].Issue(h.ctx, admissionredis.IssueRequest{
						Scope: h.scope, AdmissionRatePerSecond: testCase.rate,
						MaxInflightAdmissions: testCase.inflight, TokenTTL: 30 * time.Second,
						GenerationTTL: time.Hour, Candidates: candidateSets[caller],
					})
					if issueErr != nil {
						errs <- issueErr
						return
					}
					results <- result
				}()
			}
			close(start)
			group.Wait()
			close(results)
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}

			issuedIDs := make(map[string]struct{}, testCase.expectedIssue)
			for result := range results {
				for _, entryID := range result.IssuedEntryIDs {
					if _, exists := issuedIDs[entryID]; exists {
						t.Fatalf("entry %q was admitted by more than one caller", entryID)
					}
					issuedIDs[entryID] = struct{}{}
				}
			}
			if len(issuedIDs) != testCase.expectedIssue {
				t.Fatalf("issued entries = %d, want global bound %d", len(issuedIDs), testCase.expectedIssue)
			}
			admitted := 0
			for _, entry := range entries {
				current, getErr := h.store.Get(h.ctx, h.scope, entry.ID, entry.OwnerHash)
				if getErr != nil {
					t.Fatal(getErr)
				}
				if current.Status == domain.EntryAdmitted {
					admitted++
				}
			}
			if admitted != testCase.expectedIssue {
				t.Fatalf("admitted entries = %d, want %d", admitted, testCase.expectedIssue)
			}
		})
	}
}

func TestAdmissionTokenCannotBeStolenOrRebound(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2binding_")
	owner := sha256.Sum256([]byte("rightful-owner"))
	fingerprint := sha256.Sum256([]byte("admission-request"))
	entry, err := h.join(t, owner, fingerprint, 1, 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	issuedAt, err := h.store.Time(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	candidate := h.issueCandidate(t, entry, issuedAt)
	result, err := h.store.Issue(h.ctx, admissionredis.IssueRequest{
		Scope: h.scope, AdmissionRatePerSecond: 10, MaxInflightAdmissions: 10,
		TokenTTL: 30 * time.Second, GenerationTTL: time.Hour,
		Candidates: []admissionredis.IssueCandidate{candidate},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.IssuedEntryIDs) != 1 {
		t.Fatalf("issued entries = %#v, want the queued entry", result.IssuedEntryIDs)
	}

	bookingFingerprint := sha256.Sum256([]byte("booking-shape-a"))
	idempotencyHash := sha256.Sum256([]byte("idempotency-a"))
	base := admissionredis.AcquireRequest{
		Scope: h.scope, TokenHash: candidate.Token.TokenHash, OwnerHash: owner,
		AdmissionFingerprint: fingerprint, BookingFingerprint: bookingFingerprint,
		IdempotencyKeyHash: idempotencyHash, FromStopIndex: 1, ToStopIndex: 3,
		PassengerCount: 1, ProcessingLease: 10 * time.Second,
	}
	stolen := base
	stolen.OwnerHash = sha256.Sum256([]byte("token-thief"))
	stolen.LeaseOwner = uuid.NewString()
	if _, err := h.store.Acquire(h.ctx, stolen); !errors.Is(err, admissionredis.ErrTokenMismatch) {
		t.Fatalf("stolen token acquire error = %v, want ErrTokenMismatch", err)
	}
	alteredAdmission := base
	alteredAdmission.AdmissionFingerprint = sha256.Sum256([]byte("different-admission-request"))
	alteredAdmission.LeaseOwner = uuid.NewString()
	if _, err := h.store.Acquire(h.ctx, alteredAdmission); !errors.Is(err, admissionredis.ErrTokenMismatch) {
		t.Fatalf("altered admission acquire error = %v, want ErrTokenMismatch", err)
	}

	base.LeaseOwner = uuid.NewString()
	acquired, err := h.store.Acquire(h.ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	if acquired.Decision != domain.DecisionAcquired || acquired.LeaseGeneration != 1 {
		t.Fatalf("first acquire = %#v", acquired)
	}
	mutation := admissionredis.LeaseMutation{
		Scope: h.scope, TokenHash: candidate.Token.TokenHash, OwnerHash: owner,
		BookingFingerprint: bookingFingerprint, IdempotencyKeyHash: idempotencyHash,
		LeaseOwner: acquired.LeaseOwner, LeaseGeneration: acquired.LeaseGeneration,
	}
	if err := h.store.Release(h.ctx, mutation, false); err != nil {
		t.Fatal(err)
	}

	rebound := base
	rebound.BookingFingerprint = sha256.Sum256([]byte("booking-shape-b"))
	rebound.IdempotencyKeyHash = sha256.Sum256([]byte("idempotency-b"))
	rebound.LeaseOwner = uuid.NewString()
	if _, err := h.store.Acquire(h.ctx, rebound); !errors.Is(err, admissionredis.ErrTokenMismatch) {
		t.Fatalf("rebound token acquire error = %v, want ErrTokenMismatch", err)
	}

	base.LeaseOwner = uuid.NewString()
	reacquired, err := h.store.Acquire(h.ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	if reacquired.Decision != domain.DecisionAcquired || reacquired.LeaseGeneration != 2 {
		t.Fatalf("reacquire = %#v, want generation 2", reacquired)
	}
	mutation.LeaseOwner = reacquired.LeaseOwner
	mutation.LeaseGeneration = reacquired.LeaseGeneration
	if err := h.store.Finalize(h.ctx, mutation); err != nil {
		t.Fatal(err)
	}
	base.LeaseOwner = uuid.NewString()
	replayed, err := h.store.Acquire(h.ctx, base)
	if err != nil || replayed.Decision != domain.DecisionReplayAllowed {
		t.Fatalf("finalized token replay = (%+v, %v)", replayed, err)
	}
}

func TestCancelledQueueEntryCannotBeAdmitted(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2cancel_")
	cancelledOwner := sha256.Sum256([]byte("cancelled-owner"))
	cancelledFingerprint := sha256.Sum256([]byte("cancelled-request"))
	cancelled, err := h.join(t, cancelledOwner, cancelledFingerprint, 0, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	activeOwner := sha256.Sum256([]byte("active-owner"))
	activeFingerprint := sha256.Sum256([]byte("active-request"))
	active, err := h.join(t, activeOwner, activeFingerprint, 1, 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.Cancel(h.ctx, h.scope, cancelled.ID, cancelledOwner); err != nil {
		t.Fatal(err)
	}
	issuedAt, err := h.store.Time(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancelledCandidate := h.issueCandidate(t, cancelled, issuedAt)
	staleResult, err := h.store.Issue(h.ctx, admissionredis.IssueRequest{
		Scope: h.scope, AdmissionRatePerSecond: 10, MaxInflightAdmissions: 10,
		TokenTTL: 30 * time.Second, GenerationTTL: time.Hour,
		Candidates: []admissionredis.IssueCandidate{cancelledCandidate},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(staleResult.IssuedEntryIDs) != 0 {
		t.Fatalf("cancelled candidate was admitted: %#v", staleResult.IssuedEntryIDs)
	}
	if _, err := h.store.Get(h.ctx, h.scope, cancelled.ID, cancelledOwner); !errors.Is(err, admissionredis.ErrNotFound) {
		t.Fatalf("cancelled entry lookup error = %v, want ErrNotFound after physical deletion", err)
	}
	if _, err := h.store.InspectToken(h.ctx, h.scope, cancelledCandidate.Token.TokenHash); !errors.Is(err, admissionredis.ErrNotFound) {
		t.Fatalf("cancelled entry token lookup error = %v, want ErrNotFound", err)
	}

	activeCandidate := h.issueCandidate(t, active, issuedAt)
	result, err := h.store.Issue(h.ctx, admissionredis.IssueRequest{
		Scope: h.scope, AdmissionRatePerSecond: 10, MaxInflightAdmissions: 10,
		TokenTTL: 30 * time.Second, GenerationTTL: time.Hour,
		Candidates: []admissionredis.IssueCandidate{activeCandidate},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.IssuedEntryIDs) != 1 || result.IssuedEntryIDs[0] != active.ID {
		t.Fatalf("active issue result = %#v, want only %q", result.IssuedEntryIDs, active.ID)
	}
}
