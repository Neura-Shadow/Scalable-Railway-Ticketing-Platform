package admissionredis_test

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

const queuedEntryFieldCount = 12

func TestJoinCancelRejoinChurnImmediatelyCollectsQueuedStateAndLocator(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2terminalchurn_")
	keys := lifecyclePolicyKeys(t, h)
	owner := sha256.Sum256([]byte("repeat-customer"))

	const cycles = 256
	for index := 0; index < cycles; index++ {
		fingerprint := sha256.Sum256([]byte(fmt.Sprintf("request-%d", index)))
		entry, err := h.join(t, owner, fingerprint, 0, 1, 10)
		if err != nil {
			t.Fatalf("join cycle %d: %v", index, err)
		}
		if err := h.store.PutEntryLocator(h.ctx, entry.ID, h.scope, time.Hour); err != nil {
			t.Fatalf("put locator cycle %d: %v", index, err)
		}
		cancelled, err := h.store.Cancel(h.ctx, h.scope, entry.ID, owner)
		if err != nil {
			t.Fatalf("cancel cycle %d: %v", index, err)
		}
		if !cancelled.EntryDeleted || cancelled.LocatorCleanupPending {
			t.Fatalf("cancel cycle %d result = %+v", index, cancelled)
		}
		if _, err := h.store.ResolveEntryLocator(h.ctx, entry.ID); !errors.Is(err, admissionredis.ErrNotFound) {
			t.Fatalf("resolve cancelled locator cycle %d error = %v, want %v", index, err, admissionredis.ErrNotFound)
		}
	}

	assertLifecycleState(t, h, keys, 0, 0, 0, 0, 0)
	locators := h.client.Scan(h.ctx, 0, h.namespace+":wr:entry-locator:*", 1_000).Iterator()
	locatorCount := 0
	for locators.Next(h.ctx) {
		locatorCount++
	}
	if err := locators.Err(); err != nil {
		t.Fatal(err)
	}
	if locatorCount != 0 {
		t.Fatalf("entry locator count after %d join/cancel cycles = %d, want 0", cycles, locatorCount)
	}
}

func TestInspectStateCountsCompletedEntryScanOnlyOnceWhileInflightContinues(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2unequalscans_")
	keys := lifecyclePolicyKeys(t, h)
	owner := sha256.Sum256([]byte("unequal-scan-owner"))
	fingerprint := sha256.Sum256([]byte("unequal-scan-request"))
	if _, err := h.join(t, owner, fingerprint, 0, 1, 10); err != nil {
		t.Fatal(err)
	}
	if err := h.client.HDel(h.ctx, keys.Users, fmt.Sprintf("%x", owner[:])).Err(); err != nil {
		t.Fatal(err)
	}

	future := float64(time.Now().Add(time.Hour).UnixMilli())
	inflight := make([]goredis.Z, 0, 512)
	for index := 0; index < cap(inflight); index++ {
		inflight = append(inflight, goredis.Z{
			Score:  future,
			Member: fmt.Sprintf("%064x", index+1),
		})
	}
	if err := h.client.ZAdd(h.ctx, keys.Inflight, inflight...).Err(); err != nil {
		t.Fatal(err)
	}

	cursor := admissionredis.StateInspectionCursor{}
	var duplicateUsers int64
	for page := 0; page < 2_000; page++ {
		inspection, err := h.store.InspectState(h.ctx, h.scope, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		duplicateUsers += inspection.DuplicateActiveUsers
		if !inspection.Truncated {
			if duplicateUsers != 1 {
				t.Fatalf("duplicate active users across all pages = %d, want 1", duplicateUsers)
			}
			return
		}
		cursor = inspection.NextCursor
	}
	t.Fatal("InspectState did not terminate within the bounded page count")
}

func TestMaintenanceCountsQueuedExpiryAndRetainsBoundedExpiredEntry(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2queueexpiry_")
	keys := lifecyclePolicyKeys(t, h)
	owner := sha256.Sum256([]byte("expired-queue-owner"))
	fingerprint := sha256.Sum256([]byte("expired-queue-request"))
	entry, err := h.join(t, owner, fingerprint, 0, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.client.HSet(h.ctx, keys.Entries, entry.ID+"|x", 1).Err(); err != nil {
		t.Fatal(err)
	}
	maintenance, err := h.store.Maintain(h.ctx, h.scope, 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if maintenance.ExpiredEntries != 1 || maintenance.ExpiredTokens != 0 {
		t.Fatalf("Maintain() = %+v, want one expired entry", maintenance)
	}
	expired, err := h.store.Get(h.ctx, h.scope, entry.ID, owner)
	if err != nil || expired.Status != domain.EntryExpired {
		t.Fatalf("Get() after queue expiry = (%+v, %v)", expired, err)
	}
	assertLifecycleState(t, h, keys, queuedEntryFieldCount, 0, 0, 0, 1)
	forceLifecycleGC(t, h, keys)
	assertLifecycleState(t, h, keys, 0, 0, 0, 0, 0)
}

func TestIssuedCustomerCancelPersistsBoundedCancelledState(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2issuedcancel_")
	keys := lifecyclePolicyKeys(t, h)
	owner := sha256.Sum256([]byte("issued-cancel-owner"))
	fingerprint := sha256.Sum256([]byte("issued-cancel-request"))
	entry, token := joinAndIssueLifecycleToken(t, h, owner, fingerprint, 30*time.Second)
	if err := h.store.PutEntryLocator(h.ctx, entry.ID, h.scope, time.Hour); err != nil {
		t.Fatal(err)
	}

	cancelledResult, err := h.store.Cancel(h.ctx, h.scope, entry.ID, owner)
	if err != nil {
		t.Fatal(err)
	}
	if cancelledResult.EntryDeleted || cancelledResult.LocatorCleanupPending {
		t.Fatalf("issued cancellation result = %+v, want retained bounded state", cancelledResult)
	}
	if resolved, err := h.store.ResolveEntryLocator(h.ctx, entry.ID); err != nil || resolved != h.scope {
		t.Fatalf("issued cancellation locator = (%+v, %v), want retained scope", resolved, err)
	}
	cancelled, err := h.store.Get(h.ctx, h.scope, entry.ID, owner)
	if err != nil || cancelled.Status != domain.EntryCancelled {
		t.Fatalf("Get() after cancel = (%+v, %v)", cancelled, err)
	}
	assertTokenStatus(t, h, keys, token.Hash, domain.TokenCancelled)
	assertLifecycleState(t, h, keys, 14, 17, 0, 0, 1)
	forceLifecycleGC(t, h, keys)
	assertLifecycleState(t, h, keys, 0, 0, 0, 0, 0)
}

func TestInspectStateAllowsPreviousLiveGenerationByContinuity(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2prevg_")
	next := h.scope
	next.Version++
	if err := h.store.InstallPolicy(h.ctx, next, true, time.Hour); err != nil {
		t.Fatal(err)
	}

	inspection, err := h.store.InspectState(
		h.ctx, h.scope, admissionredis.StateInspectionCursor{}, 10,
	)
	if err != nil {
		t.Fatalf("InspectState(previous live generation) error = %v", err)
	}
	if inspection.Truncated {
		t.Fatalf("empty previous generation inspection = %+v", inspection)
	}
}

func TestValidateCurrentGenerationDetectsMissingPolicyMarkerWithContinuityPresent(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2missingmarker_")
	keys := lifecyclePolicyKeys(t, h)
	if err := h.store.ValidateCurrentGeneration(h.ctx, h.scope); err != nil {
		t.Fatalf("ValidateCurrentGeneration() live pair error = %v", err)
	}
	if err := h.client.Del(h.ctx, keys.PolicyVersion).Err(); err != nil {
		t.Fatal(err)
	}

	if err := h.store.ValidateCurrentGeneration(h.ctx, h.scope); !errors.Is(err, admissionredis.ErrPolicyMismatch) {
		t.Fatalf("ValidateCurrentGeneration() error = %v, want %v", err, admissionredis.ErrPolicyMismatch)
	}
	inspection, err := h.store.InspectState(h.ctx, h.scope, admissionredis.StateInspectionCursor{}, 10)
	if err != nil || inspection.Truncated {
		t.Fatalf("historical InspectState() = (%+v, %v), want continuity-only inspection", inspection, err)
	}
}

func TestAcquireRejectsNearExpiryTokenWithBoundedExpiredTombstone(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2nearexpiry_")
	keys := lifecyclePolicyKeys(t, h)
	owner := sha256.Sum256([]byte("near-expiry-owner"))
	fingerprint := sha256.Sum256([]byte("near-expiry-request"))
	entry, token := joinAndIssueLifecycleToken(t, h, owner, fingerprint, 4*time.Second)

	_, err := h.store.Acquire(h.ctx, admissionredis.AcquireRequest{
		Scope: h.scope, TokenHash: token.Hash, OwnerHash: owner,
		AdmissionFingerprint: fingerprint,
		BookingFingerprint:   sha256.Sum256([]byte("near-expiry-booking")),
		IdempotencyKeyHash:   sha256.Sum256([]byte("near-expiry-idempotency")),
		FromStopIndex:        0, ToStopIndex: 1, PassengerCount: 1,
		LeaseOwner: uuid.NewString(), ProcessingLease: domain.MinProcessingLease,
	})
	if !errors.Is(err, admissionredis.ErrTerminal) {
		t.Fatalf("near-expiry acquire error = %v, want ErrTerminal", err)
	}
	expired, err := h.store.Get(h.ctx, h.scope, entry.ID, owner)
	if err != nil || expired.Status != domain.EntryExpired {
		t.Fatalf("near-expiry entry = (%+v, %v), want expired tombstone", expired, err)
	}
	assertTokenStatus(t, h, keys, token.Hash, domain.TokenExpired)
	assertLifecycleState(t, h, keys, 14, 17, 0, 0, 1)
	forceLifecycleGC(t, h, keys)
	assertLifecycleState(t, h, keys, 0, 0, 0, 0, 0)
}

func TestMaintenanceExpiryRetainsTerminalStateUntilBoundedGC(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2maintenanceexpiry_")
	keys := lifecyclePolicyKeys(t, h)
	owner := sha256.Sum256([]byte("maintenance-expiry-owner"))
	fingerprint := sha256.Sum256([]byte("maintenance-expiry-request"))
	entry, token := joinAndIssueLifecycleToken(t, h, owner, fingerprint, 30*time.Second)

	if err := h.client.ZAdd(h.ctx, keys.Inflight, goredis.Z{Score: 1, Member: fmt.Sprintf("%x", token.Hash[:])}).Err(); err != nil {
		t.Fatal(err)
	}
	result, err := h.store.Maintain(h.ctx, h.scope, 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if result.ExpiredTokens != 1 || result.ExpiredEntries != 0 {
		t.Fatalf("Maintain() = %+v, want one expired token", result)
	}
	if _, err := h.store.InspectToken(h.ctx, h.scope, token.Hash); err != nil {
		t.Fatalf("InspectToken() after maintenance expiry error = %v", err)
	}
	expired, err := h.store.Get(h.ctx, h.scope, entry.ID, owner)
	if err != nil || expired.Status != domain.EntryExpired {
		t.Fatalf("Get() after maintenance expiry = (%+v, %v)", expired, err)
	}
	assertTokenStatus(t, h, keys, token.Hash, domain.TokenExpired)
	assertLifecycleState(t, h, keys, 14, 17, 0, 0, 1)

	forceLifecycleGC(t, h, keys)
	if _, err := h.store.InspectToken(h.ctx, h.scope, token.Hash); !errors.Is(err, admissionredis.ErrNotFound) {
		t.Fatalf("InspectToken() after bounded GC error = %v, want %v", err, admissionredis.ErrNotFound)
	}
	assertLifecycleState(t, h, keys, 0, 0, 0, 0, 0)
}

func TestProcessingCancelRequestTerminalizesOnTransientRelease(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2processingcancel_")
	keys := lifecyclePolicyKeys(t, h)
	owner := sha256.Sum256([]byte("processing-owner"))
	fingerprint := sha256.Sum256([]byte("processing-request"))
	entry, token := joinAndIssueLifecycleToken(t, h, owner, fingerprint, 30*time.Second)
	request := lifecycleAcquireRequest(h, token.Hash, owner, fingerprint)

	acquired, err := h.store.Acquire(h.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.Cancel(h.ctx, h.scope, entry.ID, owner); !errors.Is(err, admissionredis.ErrInProgress) {
		t.Fatalf("processing cancel error = %v, want ErrInProgress", err)
	}
	assertLifecycleState(t, h, keys, 14, 22, 1, 1, 1)

	mutation := lifecycleMutation(h, request, acquired)
	if err := h.store.Release(h.ctx, mutation, false); err != nil {
		t.Fatal(err)
	}
	assertTokenStatus(t, h, keys, token.Hash, domain.TokenCancelled)
	assertLifecycleState(t, h, keys, 14, 19, 0, 0, 1)
	forceLifecycleGC(t, h, keys)
	assertLifecycleState(t, h, keys, 0, 0, 0, 0, 0)
}

func TestHundredConcurrentSameTokenAcquiresGrantOneProcessingLease(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2token100_")
	keys := lifecyclePolicyKeys(t, h)
	owner := sha256.Sum256([]byte("token-100-owner"))
	fingerprint := sha256.Sum256([]byte("token-100-request"))
	_, token := joinAndIssueLifecycleToken(t, h, owner, fingerprint, 30*time.Second)
	base := lifecycleAcquireRequest(h, token.Hash, owner, fingerprint)

	const attempts = 100
	type outcome struct {
		result admissionredis.AcquireResult
		err    error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, attempts)
	var group sync.WaitGroup
	for index := 0; index < attempts; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			request := base
			request.LeaseOwner = uuid.NewString()
			result, err := h.store.Acquire(h.ctx, request)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(start)
	group.Wait()
	close(outcomes)

	acquiredCount := 0
	retryCount := 0
	var winner admissionredis.AcquireResult
	for current := range outcomes {
		if current.err != nil {
			t.Fatalf("concurrent acquire error = %v", current.err)
		}
		switch current.result.Decision {
		case domain.DecisionAcquired:
			acquiredCount++
			winner = current.result
		case domain.DecisionRetryAllowed:
			retryCount++
			if current.result.LeaseOwner != "" || current.result.LeaseGeneration != 0 {
				t.Fatalf("retry leaked processing authority: %+v", current.result)
			}
		default:
			t.Fatalf("unexpected concurrent decision: %+v", current.result)
		}
	}
	if acquiredCount != 1 || retryCount != attempts-1 {
		t.Fatalf("concurrent decisions: acquired=%d retry=%d", acquiredCount, retryCount)
	}
	assertLifecycleState(t, h, keys, 14, 21, 1, 1, 1)

	if err := h.store.Finalize(h.ctx, lifecycleMutation(h, base, winner)); err != nil {
		t.Fatal(err)
	}
	assertTokenStatus(t, h, keys, token.Hash, domain.TokenConsumed)
	replayRequest := base
	replayRequest.LeaseOwner = uuid.NewString()
	replayed, err := h.store.Acquire(h.ctx, replayRequest)
	if err != nil || replayed.Decision != domain.DecisionReplayAllowed {
		t.Fatalf("same-binding replay = (%+v, %v)", replayed, err)
	}
	conflictRequest := replayRequest
	conflictRequest.BookingFingerprint = sha256.Sum256([]byte("different-booking"))
	if _, err := h.store.Acquire(h.ctx, conflictRequest); !errors.Is(err, admissionredis.ErrTokenMismatch) {
		t.Fatalf("different-binding consumed replay error = %v, want %v", err, admissionredis.ErrTokenMismatch)
	}
	assertLifecycleState(t, h, keys, 14, 19, 0, 0, 1)
	forceLifecycleGC(t, h, keys)
	assertLifecycleState(t, h, keys, 0, 0, 0, 0, 0)
}

func TestPermanentReleasePersistsCancelledAndAllowsRejoin(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2permanentrelease_")
	keys := lifecyclePolicyKeys(t, h)
	owner := sha256.Sum256([]byte("permanent-owner"))
	fingerprint := sha256.Sum256([]byte("permanent-request"))
	_, token := joinAndIssueLifecycleToken(t, h, owner, fingerprint, 30*time.Second)
	request := lifecycleAcquireRequest(h, token.Hash, owner, fingerprint)
	acquired, err := h.store.Acquire(h.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.Release(h.ctx, lifecycleMutation(h, request, acquired), true); err != nil {
		t.Fatal(err)
	}
	assertTokenStatus(t, h, keys, token.Hash, domain.TokenCancelled)
	assertLifecycleState(t, h, keys, 14, 19, 0, 0, 1)

	rejoined, duplicate, err := h.store.Join(h.ctx, admissionredis.JoinRequest{
		Scope: h.scope, EntryID: uuid.NewString(), OwnerHash: owner,
		AdmissionFingerprint: sha256.Sum256([]byte("replacement-request")),
		FromStopIndex:        1, ToStopIndex: 2, PassengerCount: 1,
		MaxQueueSize: 10, EntryTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate || rejoined.Status != domain.EntryQueued {
		t.Fatalf("rejoin = %+v, duplicate=%v", rejoined, duplicate)
	}
	forceLifecycleGC(t, h, keys)
}

func TestTransientReleaseRetainsIssuedRetryAndFinalizePersistsConsumed(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2releasefinalize_")
	keys := lifecyclePolicyKeys(t, h)
	owner := sha256.Sum256([]byte("finalize-owner"))
	fingerprint := sha256.Sum256([]byte("finalize-request"))
	_, token := joinAndIssueLifecycleToken(t, h, owner, fingerprint, 30*time.Second)
	request := lifecycleAcquireRequest(h, token.Hash, owner, fingerprint)

	first, err := h.store.Acquire(h.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.Release(h.ctx, lifecycleMutation(h, request, first), false); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.InspectToken(h.ctx, h.scope, token.Hash); err != nil {
		t.Fatalf("transient release removed issued token: %v", err)
	}
	request.LeaseOwner = uuid.NewString()
	second, err := h.store.Acquire(h.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if second.LeaseGeneration != 2 {
		t.Fatalf("second lease generation = %d, want 2", second.LeaseGeneration)
	}
	if err := h.store.Finalize(h.ctx, lifecycleMutation(h, request, second)); err != nil {
		t.Fatal(err)
	}
	assertTokenStatus(t, h, keys, token.Hash, domain.TokenConsumed)
	assertLifecycleState(t, h, keys, 14, 19, 0, 0, 1)
	forceLifecycleGC(t, h, keys)
	assertLifecycleState(t, h, keys, 0, 0, 0, 0, 0)
}

func joinAndIssueLifecycleToken(
	t *testing.T,
	h *liveAdmissionRedis,
	owner [sha256.Size]byte,
	fingerprint [sha256.Size]byte,
	lifetime time.Duration,
) (domain.WaitingRoomEntry, domain.IssuedToken) {
	t.Helper()
	entry, err := h.join(t, owner, fingerprint, 0, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	issuedAt, err := h.store.Time(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	token, err := h.keyring.Issue(domain.TokenClaims{
		PolicyID: h.scope.PolicyID, PolicyVersion: h.scope.Version, EntryID: entry.ID,
		OwnerHash: owner, AdmissionFingerprint: fingerprint,
		IssuedAt: issuedAt, ExpiresAt: issuedAt.Add(lifetime),
	})
	if err != nil {
		t.Fatal(err)
	}
	tokenTTL := lifetime
	if tokenTTL < domain.MinAdmissionTokenTTL {
		tokenTTL = domain.MinAdmissionTokenTTL
	}
	result, err := h.store.Issue(h.ctx, admissionredis.IssueRequest{
		Scope: h.scope, AdmissionRatePerSecond: 100, MaxInflightAdmissions: 100,
		TokenTTL: tokenTTL, GenerationTTL: time.Hour,
		Candidates: []admissionredis.IssueCandidate{{EntryID: entry.ID, Token: token.Fields}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.IssuedEntryIDs) != 1 || result.IssuedEntryIDs[0] != entry.ID {
		t.Fatalf("issue result = %+v", result)
	}
	return entry, token
}

func lifecycleAcquireRequest(
	h *liveAdmissionRedis,
	tokenHash [sha256.Size]byte,
	owner [sha256.Size]byte,
	fingerprint [sha256.Size]byte,
) admissionredis.AcquireRequest {
	return admissionredis.AcquireRequest{
		Scope: h.scope, TokenHash: tokenHash, OwnerHash: owner,
		AdmissionFingerprint: fingerprint,
		BookingFingerprint:   sha256.Sum256([]byte("lifecycle-booking")),
		IdempotencyKeyHash:   sha256.Sum256([]byte("lifecycle-idempotency")),
		FromStopIndex:        0, ToStopIndex: 1, PassengerCount: 1,
		LeaseOwner: uuid.NewString(), ProcessingLease: 10 * time.Second,
	}
}

func lifecycleMutation(
	h *liveAdmissionRedis,
	request admissionredis.AcquireRequest,
	acquired admissionredis.AcquireResult,
) admissionredis.LeaseMutation {
	return admissionredis.LeaseMutation{
		Scope: h.scope, TokenHash: request.TokenHash, OwnerHash: request.OwnerHash,
		BookingFingerprint: request.BookingFingerprint, IdempotencyKeyHash: request.IdempotencyKeyHash,
		LeaseOwner: acquired.LeaseOwner, LeaseGeneration: acquired.LeaseGeneration,
	}
}

func lifecyclePolicyKeys(t *testing.T, h *liveAdmissionRedis) admissionredis.PolicyKeys {
	t.Helper()
	builder, err := admissionredis.NewKeyBuilder(h.namespace)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := builder.ForPolicy(h.scope.TrainRunID, h.scope.SeatClass, h.scope.Version)
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func assertTokenStatus(
	t *testing.T,
	h *liveAdmissionRedis,
	keys admissionredis.PolicyKeys,
	tokenHash [sha256.Size]byte,
	want domain.TokenStatus,
) {
	t.Helper()
	token := fmt.Sprintf("%x", tokenHash[:])
	status, err := h.client.HGet(h.ctx, keys.Tokens, token+"|s").Result()
	if err != nil {
		t.Fatal(err)
	}
	if domain.TokenStatus(status) != want {
		t.Fatalf("token status = %q, want %q", status, want)
	}
}

func forceLifecycleGC(t *testing.T, h *liveAdmissionRedis, keys admissionredis.PolicyKeys) {
	t.Helper()
	members, err := h.client.ZRange(h.ctx, keys.Leases, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) == 0 {
		t.Fatal("no terminal GC members")
	}
	due := make([]goredis.Z, 0, len(members))
	for _, member := range members {
		due = append(due, goredis.Z{Score: 1, Member: member})
	}
	if err := h.client.ZAdd(h.ctx, keys.Leases, due...).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.Maintain(h.ctx, h.scope, admissionredis.MaxAdmissionBatch, time.Hour); err != nil {
		t.Fatal(err)
	}
}

func assertLifecycleState(
	t *testing.T,
	h *liveAdmissionRedis,
	keys admissionredis.PolicyKeys,
	entryFields int64,
	tokenFields int64,
	users int64,
	inflight int64,
	leases int64,
) {
	t.Helper()
	values, err := h.client.Pipelined(h.ctx, func(pipe goredis.Pipeliner) error {
		pipe.HLen(h.ctx, keys.Entries)
		pipe.HLen(h.ctx, keys.Tokens)
		pipe.HLen(h.ctx, keys.Users)
		pipe.ZCard(h.ctx, keys.Queue)
		pipe.ZCard(h.ctx, keys.Inflight)
		pipe.ZCard(h.ctx, keys.Leases)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]int64, len(values))
	for index, command := range values {
		value, ok := command.(*goredis.IntCmd)
		if !ok {
			t.Fatalf("lifecycle command %d type = %T", index, command)
		}
		got[index], err = value.Result()
		if err != nil {
			t.Fatal(err)
		}
	}
	want := []int64{entryFields, tokenFields, users, 0, inflight, leases}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("lifecycle counts = %v, want %v", got, want)
		}
	}
}
