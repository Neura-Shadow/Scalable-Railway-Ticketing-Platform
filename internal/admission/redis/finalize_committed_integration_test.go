package admissionredis_test

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	"github.com/google/uuid"
)

func TestDurableCommittedReplayRepairsLostFinalizeByExactTokenLocator(t *testing.T) {
	h := newLiveAdmissionRedis(t, "m2committedrepair_")
	owner := sha256.Sum256([]byte("committed-owner"))
	admissionFingerprint := sha256.Sum256([]byte("committed-admission"))
	entry, err := h.join(t, owner, admissionFingerprint, 0, 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	now, err := h.store.Time(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	candidate := h.issueCandidate(t, entry, now)
	issued, err := h.store.Issue(h.ctx, admissionredis.IssueRequest{
		Scope: h.scope, AdmissionRatePerSecond: 10, MaxInflightAdmissions: 10,
		TokenTTL: 30 * time.Second, GenerationTTL: time.Hour,
		Candidates: []admissionredis.IssueCandidate{candidate},
	})
	if err != nil || len(issued.IssuedEntryIDs) != 1 {
		t.Fatalf("Issue() = (%+v, %v)", issued, err)
	}
	if err := h.store.PutTokenLocator(h.ctx, candidate.Token.TokenHash, h.scope, time.Hour); err != nil {
		t.Fatal(err)
	}

	bookingFingerprint := sha256.Sum256([]byte("committed-booking"))
	idempotencyHash := sha256.Sum256([]byte("committed-idempotency"))
	acquired, err := h.store.Acquire(h.ctx, admissionredis.AcquireRequest{
		Scope: h.scope, TokenHash: candidate.Token.TokenHash, OwnerHash: owner,
		AdmissionFingerprint: admissionFingerprint, BookingFingerprint: bookingFingerprint,
		IdempotencyKeyHash: idempotencyHash, FromStopIndex: 0, ToStopIndex: 2,
		PassengerCount: 1, LeaseOwner: uuid.NewString(), ProcessingLease: 10 * time.Second,
	})
	if err != nil || acquired.Decision != domain.DecisionAcquired {
		t.Fatalf("Acquire() = (%+v, %v)", acquired, err)
	}

	wrong := admissionredis.CommittedMutation{
		TokenHash: candidate.Token.TokenHash, OwnerHash: owner,
		BookingFingerprint: sha256.Sum256([]byte("wrong-booking")),
		IdempotencyKeyHash: idempotencyHash,
	}
	if err := h.store.FinalizeCommitted(h.ctx, wrong); !errors.Is(err, admissionredis.ErrTokenMismatch) {
		t.Fatalf("mismatched FinalizeCommitted() error = %v, want %v", err, admissionredis.ErrTokenMismatch)
	}
	if _, err := h.store.InspectToken(h.ctx, h.scope, candidate.Token.TokenHash); err != nil {
		t.Fatalf("mismatched repair changed token: %v", err)
	}

	if err := h.store.FinalizeCommitted(h.ctx, admissionredis.CommittedMutation{
		TokenHash: candidate.Token.TokenHash, OwnerHash: owner,
		BookingFingerprint: bookingFingerprint, IdempotencyKeyHash: idempotencyHash,
	}); err != nil {
		t.Fatalf("FinalizeCommitted() error = %v", err)
	}
	if _, err := h.store.InspectToken(h.ctx, h.scope, candidate.Token.TokenHash); err != nil {
		t.Fatalf("InspectToken() after repair error = %v", err)
	}
	resolved, err := h.store.ResolveTokenLocator(h.ctx, candidate.Token.TokenHash)
	if err != nil || resolved != h.scope {
		t.Fatalf("ResolveTokenLocator() after repair = (%+v, %v), want %+v", resolved, err, h.scope)
	}
	replayed, err := h.store.Acquire(h.ctx, admissionredis.AcquireRequest{
		Scope: h.scope, TokenHash: candidate.Token.TokenHash, OwnerHash: owner,
		AdmissionFingerprint: admissionFingerprint, BookingFingerprint: bookingFingerprint,
		IdempotencyKeyHash: idempotencyHash, FromStopIndex: 0, ToStopIndex: 2,
		PassengerCount: 1, LeaseOwner: uuid.NewString(), ProcessingLease: 10 * time.Second,
	})
	if err != nil || replayed.Decision != domain.DecisionReplayAllowed {
		t.Fatalf("Acquire() after committed repair = (%+v, %v)", replayed, err)
	}
	counts, err := h.store.StateCounts(h.ctx, h.scope)
	if err != nil || counts.QueueDepth != 0 || counts.InflightAdmissions != 0 {
		t.Fatalf("StateCounts() after repair = (%+v, %v)", counts, err)
	}
}
