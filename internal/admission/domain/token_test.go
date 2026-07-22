package domain_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
)

func TestAdmissionTokenCanBeReconstructedOnlyWithKeyFromHashBoundFields(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	keyring, err := domain.NewTokenKeyring("kid-2026-07", map[string][]byte{"kid-2026-07": key})
	if err != nil {
		t.Fatal(err)
	}
	claims := domain.TokenClaims{
		PolicyID:             "policy-1",
		PolicyVersion:        7,
		EntryID:              "entry-1",
		OwnerHash:            sha256.Sum256([]byte("owner-1")),
		AdmissionFingerprint: sha256.Sum256([]byte("shape-1")),
		IssuedAt:             time.Unix(1_800_000_000, 0).UTC(),
		ExpiresAt:            time.Unix(1_800_000_120, 0).UTC(),
	}
	issued, err := keyring.Issue(claims)
	if err != nil {
		t.Fatal(err)
	}
	if issued.Raw == "" || issued.Hash != sha256.Sum256([]byte(issued.Raw)) {
		t.Fatal("issued token did not contain a SHA-256-bound opaque raw value")
	}
	if decoded, err := base64.RawURLEncoding.DecodeString(issued.Raw); err != nil || len(decoded) < 36 {
		t.Fatalf("raw token is not a versioned base64url envelope: len=%d err=%v", len(decoded), err)
	}
	reconstructed, err := keyring.Reconstruct(issued.Fields)
	if err != nil {
		t.Fatal(err)
	}
	if reconstructed != issued.Raw {
		t.Fatal("signed delivery fields did not deterministically reconstruct the raw token")
	}
	if err := keyring.Verify(reconstructed, issued.Fields); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if err := keyring.VerifyFields(issued.Fields); err != nil {
		t.Fatalf("VerifyFields() error = %v", err)
	}

	withoutKey, err := domain.NewTokenKeyring("", map[string][]byte{"other": bytes.Repeat([]byte{2}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := withoutKey.Reconstruct(issued.Fields); !errors.Is(err, domain.ErrUnknownAdmissionTokenKey) {
		t.Fatalf("Redis metadata reconstructed bearer without its signing key: %v", err)
	}
	if encoded := base64.RawURLEncoding.EncodeToString(issued.Fields.Nonce[:]); strings.Contains(issued.Raw, encoded) {
		t.Fatal("raw bearer exposed the Redis-stored nonce")
	}
}

func TestAdmissionTokenVerificationRejectsTamperingAndUnknownKeys(t *testing.T) {
	keyring, err := domain.NewTokenKeyring("active", map[string][]byte{"active": bytes.Repeat([]byte{1}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	claims := domain.TokenClaims{
		PolicyID: "p", PolicyVersion: 1, EntryID: "e",
		OwnerHash: sha256.Sum256([]byte("u")), AdmissionFingerprint: sha256.Sum256([]byte("f")),
		IssuedAt: time.Unix(100, 0), ExpiresAt: time.Unix(200, 0),
	}
	issued, err := keyring.Issue(claims)
	if err != nil {
		t.Fatal(err)
	}

	changed := claims
	changed.PolicyVersion++
	changedFields := issued.Fields
	changedFields.Claims = changed
	if err := keyring.Verify(issued.Raw, changedFields); !errors.Is(err, domain.ErrInvalidAdmissionToken) {
		t.Fatalf("changed claims error = %v", err)
	}
	tampered := issued.Raw[:len(issued.Raw)-1] + flipBase64Character(issued.Raw[len(issued.Raw)-1:])
	if err := keyring.Verify(tampered, issued.Fields); !errors.Is(err, domain.ErrInvalidAdmissionToken) {
		t.Fatalf("tampered raw error = %v", err)
	}
	acceptOther, err := domain.NewTokenKeyring("", map[string][]byte{"other": bytes.Repeat([]byte{2}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	if err := acceptOther.Verify(issued.Raw, issued.Fields); !errors.Is(err, domain.ErrUnknownAdmissionTokenKey) {
		t.Fatalf("unknown key error = %v", err)
	}

	changedHash := issued.Fields
	changedHash.TokenHash = sha256.Sum256([]byte("different-token"))
	if err := keyring.Verify(issued.Raw, changedHash); !errors.Is(err, domain.ErrInvalidAdmissionToken) {
		t.Fatalf("changed stored hash error = %v", err)
	}
}

func TestAdmissionTokenKeyringRejectsWeakOrAmbiguousConfiguration(t *testing.T) {
	cases := []struct {
		issue string
		keys  map[string][]byte
	}{
		{"active", map[string][]byte{"active": bytes.Repeat([]byte{1}, 31)}},
		{"missing", map[string][]byte{"other": bytes.Repeat([]byte{1}, 32)}},
		{"bad key", map[string][]byte{"bad key": bytes.Repeat([]byte{1}, 32)}},
		{"", nil},
	}
	for index, tc := range cases {
		if _, err := domain.NewTokenKeyring(tc.issue, tc.keys); !errors.Is(err, domain.ErrInvalidAdmissionTokenKeyring) {
			t.Fatalf("case %d error = %v", index, err)
		}
	}
}

func flipBase64Character(value string) string {
	if strings.EqualFold(value, "A") {
		return "B"
	}
	return "A"
}
