package admissionredis_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

func TestWaitingRoomIntegrationConvergesIdenticalJoinsAndNeverStoresRawToken(t *testing.T) {
	address := strings.TrimSpace(os.Getenv("TEST_REDIS_ADDR"))
	if address == "" {
		t.Skip("TEST_REDIS_ADDR is not set; skipping admission Redis integration test")
	}
	ctx := context.Background()
	client := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	namespace := "m2test_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	store, err := admissionredis.NewStore(client, namespace)
	if err != nil {
		t.Fatal(err)
	}
	scope := admissionredis.PolicyScope{
		PolicyID: uuid.NewString(), TrainRunID: uuid.NewString(), SeatClass: "standard", Version: 1,
	}
	if err := store.InstallPolicy(ctx, scope, true, time.Hour); err != nil {
		t.Fatal(err)
	}
	locatorEntry := uuid.NewString()
	if err := store.PutEntryLocator(ctx, locatorEntry, scope, time.Hour); err != nil {
		t.Fatal(err)
	}
	resolved, err := store.ResolveEntryLocator(ctx, locatorEntry)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != scope {
		t.Fatalf("resolved locator = %#v, want %#v", resolved, scope)
	}
	owner := sha256.Sum256([]byte("customer-1"))
	fingerprint, err := domain.FingerprintAdmissionRequest(domain.AdmissionFingerprintInput{
		TrainRunID: scope.TrainRunID, FromStopIndex: 0, ToStopIndex: 2, SeatClass: scope.SeatClass, PassengerCount: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	const attempts = 100
	ids := make(chan string, attempts)
	errs := make(chan error, attempts)
	var group sync.WaitGroup
	for index := 0; index < attempts; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			entry, _, joinErr := store.Join(ctx, admissionredis.JoinRequest{
				Scope: scope, EntryID: uuid.NewString(), OwnerHash: owner, AdmissionFingerprint: fingerprint,
				FromStopIndex: 0, ToStopIndex: 2, PassengerCount: 2, MaxQueueSize: 10, EntryTTL: time.Hour,
			})
			if joinErr != nil {
				errs <- joinErr
				return
			}
			ids <- entry.ID
		}()
	}
	group.Wait()
	close(ids)
	close(errs)
	for joinErr := range errs {
		t.Fatal(joinErr)
	}
	var stableID string
	for id := range ids {
		if stableID == "" {
			stableID = id
		}
		if id != stableID {
			t.Fatalf("identical joins returned %q and %q", stableID, id)
		}
	}

	queued, err := store.PeekQueued(ctx, scope, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 1 || queued[0].ID != stableID {
		t.Fatalf("queued entries = %#v, want one stable entry", queued)
	}
	serverTime, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	keyring, err := domain.NewTokenKeyring("integration", map[string][]byte{
		"integration": bytes.Repeat([]byte{0x7a}, 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	issued, err := keyring.Issue(domain.TokenClaims{
		PolicyID: scope.PolicyID, PolicyVersion: scope.Version, EntryID: stableID,
		OwnerHash: owner, AdmissionFingerprint: fingerprint,
		IssuedAt: serverTime.UTC(), ExpiresAt: serverTime.Add(time.Minute).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Issue(ctx, admissionredis.IssueRequest{
		Scope: scope, AdmissionRatePerSecond: 10, MaxInflightAdmissions: 10,
		TokenTTL: time.Minute, GenerationTTL: time.Hour,
		Candidates: []admissionredis.IssueCandidate{{EntryID: stableID, Token: issued.Fields}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.IssuedEntryIDs) != 1 || result.IssuedEntryIDs[0] != stableID {
		t.Fatalf("issue result = %#v", result)
	}
	admitted, err := store.Get(ctx, scope, stableID, owner)
	if err != nil {
		t.Fatal(err)
	}
	if admitted.Status != domain.EntryAdmitted || admitted.AdmittedAt == nil {
		t.Fatalf("admitted entry omitted Redis admission time: %#v", admitted)
	}
	if admitted.ExpiresAt.Before(issued.Fields.Claims.ExpiresAt) {
		t.Fatalf("admitted entry expires at %s before token %s", admitted.ExpiresAt, issued.Fields.Claims.ExpiresAt)
	}
	inspected, err := store.InspectToken(ctx, scope, issued.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if err := keyring.Verify(issued.Raw, inspected); err != nil {
		t.Fatalf("raw token did not verify against inspected signed claims: %v", err)
	}

	keys, _, err := client.Scan(ctx, 0, namespace+":wr:*", 100).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		keyType, typeErr := client.Type(ctx, key).Result()
		if typeErr != nil {
			t.Fatal(typeErr)
		}
		var stored string
		switch keyType {
		case "string":
			stored, err = client.Get(ctx, key).Result()
		case "hash":
			values, hashErr := client.HGetAll(ctx, key).Result()
			err = hashErr
			for field, value := range values {
				stored += field + value
			}
		case "zset":
			values, zsetErr := client.ZRange(ctx, key, 0, -1).Result()
			err = zsetErr
			stored = strings.Join(values, "")
		}
		if err != nil {
			t.Fatal(err)
		}
		rawEnvelope, decodeErr := base64.RawURLEncoding.Strict().DecodeString(issued.Raw)
		if decodeErr != nil || len(rawEnvelope) < sha256.Size {
			t.Fatal("issued raw token envelope is invalid")
		}
		rawMAC := rawEnvelope[len(rawEnvelope)-sha256.Size:]
		if strings.Contains(stored, issued.Raw) ||
			strings.Contains(stored, base64.RawURLEncoding.EncodeToString(rawMAC)) ||
			strings.Contains(stored, hex.EncodeToString(rawMAC)) {
			t.Fatalf("raw admission token or reconstructable MAC was stored in Redis key %q", key)
		}
	}

	fields, err := store.InspectDelivery(ctx, scope, stableID, owner)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := keyring.Reconstruct(fields)
	if err != nil {
		t.Fatal(err)
	}
	if raw != issued.Raw {
		t.Fatal("preflight metadata did not reconstruct the issued token with the process key")
	}
	withoutIssueKey, err := domain.NewTokenKeyring("", map[string][]byte{
		"other": bytes.Repeat([]byte{0x6b}, 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := withoutIssueKey.Reconstruct(fields); !errors.Is(err, domain.ErrUnknownAdmissionTokenKey) {
		t.Fatalf("Redis metadata reconstructed without the issue key: %v", err)
	}
	builder, err := admissionredis.NewKeyBuilder(namespace)
	if err != nil {
		t.Fatal(err)
	}
	policyKeys, err := builder.ForPolicy(scope.TrainRunID, scope.SeatClass, scope.Version)
	if err != nil {
		t.Fatal(err)
	}
	deliveryField := hex.EncodeToString(issued.Hash[:]) + "|d"
	if delivered, err := client.HGet(ctx, policyKeys.Tokens, deliveryField).Result(); err != nil || delivered != "0" {
		t.Fatalf("unknown key changed delivery marker = %q, %v", delivered, err)
	}

	claimedFields, err := store.ClaimDelivery(ctx, scope, stableID, owner)
	if err != nil {
		t.Fatal(err)
	}
	claimedRaw, err := keyring.Reconstruct(claimedFields)
	if err != nil {
		t.Fatal(err)
	}
	if claimedRaw != issued.Raw {
		t.Fatal("claimed metadata did not reconstruct the issued token")
	}
	if _, err := store.ClaimDelivery(ctx, scope, stableID, owner); !errors.Is(err, admissionredis.ErrNotFound) {
		t.Fatalf("second delivery claim error = %v, want ErrNotFound", err)
	}

	bookingFingerprint := sha256.Sum256([]byte("booking-shape"))
	idempotencyHash := sha256.Sum256([]byte("idempotency-key"))
	firstLeaseOwner := uuid.NewString()
	acquired, err := store.Acquire(ctx, admissionredis.AcquireRequest{
		Scope: scope, TokenHash: issued.Hash, OwnerHash: owner, AdmissionFingerprint: fingerprint,
		BookingFingerprint: bookingFingerprint, IdempotencyKeyHash: idempotencyHash,
		FromStopIndex: 0, ToStopIndex: 2, PassengerCount: 2,
		LeaseOwner: firstLeaseOwner, ProcessingLease: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if acquired.Decision != domain.DecisionAcquired || acquired.LeaseOwner != firstLeaseOwner ||
		acquired.LeaseGeneration != 1 {
		t.Fatalf("first acquire = %#v", acquired)
	}
	retry, err := store.Acquire(ctx, admissionredis.AcquireRequest{
		Scope: scope, TokenHash: issued.Hash, OwnerHash: owner, AdmissionFingerprint: fingerprint,
		BookingFingerprint: bookingFingerprint, IdempotencyKeyHash: idempotencyHash,
		FromStopIndex: 0, ToStopIndex: 2, PassengerCount: 2,
		LeaseOwner: uuid.NewString(), ProcessingLease: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if retry.Decision != domain.DecisionRetryAllowed || retry.LeaseOwner != "" ||
		retry.LeaseGeneration != 0 || retry.RetryAfter <= 0 {
		t.Fatalf("processing retry received execution authority: %#v", retry)
	}
}

func TestMaintenanceIntegrationReclaimsExpiredTokenWithEmptyQueue(t *testing.T) {
	address := strings.TrimSpace(os.Getenv("TEST_REDIS_ADDR"))
	if address == "" {
		t.Skip("TEST_REDIS_ADDR is not set; skipping admission Redis integration test")
	}
	ctx := context.Background()
	client := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	namespace := "m2maint_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	store, err := admissionredis.NewStore(client, namespace)
	if err != nil {
		t.Fatal(err)
	}
	scope := admissionredis.PolicyScope{
		PolicyID: uuid.NewString(), TrainRunID: uuid.NewString(), SeatClass: "first", Version: 1,
	}
	if err := store.InstallPolicy(ctx, scope, true, time.Hour); err != nil {
		t.Fatal(err)
	}
	builder, err := admissionredis.NewKeyBuilder(namespace)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := builder.ForPolicy(scope.TrainRunID, scope.SeatClass, scope.Version)
	if err != nil {
		t.Fatal(err)
	}
	tokenHash := strings.Repeat("a", 64)
	if err := client.HSet(ctx, keys.Tokens, tokenHash+"|s", "issued").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, keys.Inflight, goredis.Z{Score: 1, Member: tokenHash}).Err(); err != nil {
		t.Fatal(err)
	}
	serverTime, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	processingHash := strings.Repeat("b", 64)
	if err := client.HSet(ctx, keys.Tokens,
		processingHash+"|s", "processing",
		processingHash+"|x", serverTime.Add(time.Hour).UnixMilli(),
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, keys.Inflight,
		goredis.Z{Score: float64(serverTime.Add(time.Hour).UnixMilli()), Member: processingHash},
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, keys.Leases, goredis.Z{Score: 1, Member: processingHash}).Err(); err != nil {
		t.Fatal(err)
	}
	duplicateEntry := uuid.NewString()
	admittedEntry := uuid.NewString()
	admittedToken := strings.Repeat("c", 64)
	if err := client.HSet(ctx, keys.Entries,
		duplicateEntry+"|s", "queued", duplicateEntry+"|o", "owner-a",
		admittedEntry+"|s", "admitted", admittedEntry+"|o", "owner-b", admittedEntry+"|t", admittedToken,
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, keys.Users, "owner-a", uuid.NewString(), "owner-b", admittedEntry).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, keys.Tokens,
		admittedToken+"|s", "issued", admittedToken+"|e", uuid.NewString(), admittedToken+"|o", "different-owner",
	).Err(); err != nil {
		t.Fatal(err)
	}
	inspection, err := store.InspectState(ctx, scope, admissionredis.StateInspectionCursor{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.DuplicateActiveUsers != 1 || inspection.InflightTokenMismatch != 1 ||
		inspection.ExpiredInflightTokens != 1 || inspection.ExpiredProcessingLeases != 1 ||
		inspection.TokenEntryOwnerMismatch != 1 || inspection.Truncated {
		t.Fatalf("state inspection = %#v", inspection)
	}
	result, err := store.Maintain(ctx, scope, 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if result.ExpiredTokens != 1 || result.RecoveredLeases != 1 {
		t.Fatalf("maintenance result = %#v", result)
	}
	if status, err := client.HGet(ctx, keys.Tokens, tokenHash+"|s").Result(); err != nil || status != "expired" {
		t.Fatalf("expired token status = %q, %v; want bounded tombstone", status, err)
	}
	inflight, err := client.ZRange(ctx, keys.Inflight, 0, -1).Result()
	if err != nil || len(inflight) != 1 || inflight[0] != processingHash {
		t.Fatalf("inflight tokens = %#v, %v; want recovered token %q", inflight, err, processingHash)
	}
	if status, err := client.HGet(ctx, keys.Tokens, processingHash+"|s").Result(); err != nil || status != "issued" {
		t.Fatalf("recovered token status = %q, %v; want issued", status, err)
	}
}

func TestIssueIntegrationAcceptsUnexpiredCandidateAfterWriteAheadDelay(t *testing.T) {
	address := strings.TrimSpace(os.Getenv("TEST_REDIS_ADDR"))
	if address == "" {
		t.Skip("TEST_REDIS_ADDR is not set; skipping admission Redis integration test")
	}
	ctx := context.Background()
	client := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	namespace := "m2delay_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	store, err := admissionredis.NewStore(client, namespace)
	if err != nil {
		t.Fatal(err)
	}
	scope := admissionredis.PolicyScope{
		PolicyID: uuid.NewString(), TrainRunID: uuid.NewString(), SeatClass: "standard", Version: 1,
	}
	if err := store.InstallPolicy(ctx, scope, true, time.Hour); err != nil {
		t.Fatal(err)
	}
	owner := sha256.Sum256([]byte("delayed-owner"))
	fingerprint := sha256.Sum256([]byte("delayed-admission"))
	entry, _, err := store.Join(ctx, admissionredis.JoinRequest{
		Scope: scope, EntryID: uuid.NewString(), OwnerHash: owner, AdmissionFingerprint: fingerprint,
		FromStopIndex: 0, ToStopIndex: 2, PassengerCount: 1, MaxQueueSize: 10, EntryTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	serverTime, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	keyring, err := domain.NewTokenKeyring("delay", map[string][]byte{
		"delay": bytes.Repeat([]byte{0x5d}, 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	token, err := keyring.Issue(domain.TokenClaims{
		PolicyID: scope.PolicyID, PolicyVersion: scope.Version, EntryID: entry.ID,
		OwnerHash: owner, AdmissionFingerprint: fingerprint,
		IssuedAt:  serverTime.Add(-10 * time.Second).UTC(),
		ExpiresAt: serverTime.Add(50 * time.Second).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutIssueLocators(
		ctx,
		[]admissionredis.IssueLocator{{EntryID: entry.ID, TokenHash: token.Hash}},
		scope,
		time.Hour,
		6*time.Minute,
	); err != nil {
		t.Fatal(err)
	}
	result, err := store.Issue(ctx, admissionredis.IssueRequest{
		Scope: scope, AdmissionRatePerSecond: 10, MaxInflightAdmissions: 10,
		TokenTTL: time.Minute, GenerationTTL: time.Hour,
		Candidates: []admissionredis.IssueCandidate{{EntryID: entry.ID, Token: token.Fields}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.IssuedEntryIDs) != 1 || result.IssuedEntryIDs[0] != entry.ID {
		t.Fatalf("delayed unexpired candidate was starved: %#v", result)
	}
}

func TestInspectStateIntegrationAcceptsConsumedAdmissionWithoutActiveUserMapping(t *testing.T) {
	address := strings.TrimSpace(os.Getenv("TEST_REDIS_ADDR"))
	if address == "" {
		t.Skip("TEST_REDIS_ADDR is not set; skipping admission Redis integration test")
	}
	ctx := context.Background()
	client := goredis.NewClient(&goredis.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	namespace := "m2inspect_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	store, err := admissionredis.NewStore(client, namespace)
	if err != nil {
		t.Fatal(err)
	}
	scope := admissionredis.PolicyScope{
		PolicyID: uuid.NewString(), TrainRunID: uuid.NewString(), SeatClass: "first", Version: 1,
	}
	if err := store.InstallPolicy(ctx, scope, true, time.Hour); err != nil {
		t.Fatal(err)
	}
	builder, err := admissionredis.NewKeyBuilder(namespace)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := builder.ForPolicy(scope.TrainRunID, scope.SeatClass, scope.Version)
	if err != nil {
		t.Fatal(err)
	}
	entryID := uuid.NewString()
	tokenHash := strings.Repeat("d", 64)
	ownerHash := strings.Repeat("e", 64)
	if err := client.HSet(ctx, keys.Entries,
		entryID+"|s", "admitted",
		entryID+"|o", ownerHash,
		entryID+"|t", tokenHash,
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, keys.Tokens,
		tokenHash+"|s", "consumed",
		tokenHash+"|e", entryID,
		tokenHash+"|o", ownerHash,
	).Err(); err != nil {
		t.Fatal(err)
	}

	inspection, err := store.InspectState(ctx, scope, admissionredis.StateInspectionCursor{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.DuplicateActiveUsers != 0 ||
		inspection.TokenEntryOwnerMismatch != 0 ||
		inspection.InflightTokenMismatch != 0 {
		t.Fatalf("valid consumed admission reported reconciliation violations: %#v", inspection)
	}
}
