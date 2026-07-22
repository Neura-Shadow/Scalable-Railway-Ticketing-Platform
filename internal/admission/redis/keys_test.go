package admissionredis_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	"github.com/google/uuid"
)

func TestKeyBuilderProducesValidatedSingleSlotKeys(t *testing.T) {
	runID := uuid.New()
	builder, err := admissionredis.NewKeyBuilder("railway")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := builder.ForPolicy(runID.String(), "BUSINESS", 17)
	if err != nil {
		t.Fatal(err)
	}
	wantTag := "{" + runID.String() + "|business}"
	for name, key := range map[string]string{
		"version": keys.PolicyVersion, "continuity": keys.Continuity, "queue": keys.Queue,
		"sequence": keys.Sequence, "entries": keys.Entries, "users": keys.Users,
		"tokens": keys.Tokens, "inflight": keys.Inflight, "rate": keys.Rate, "leases": keys.Leases,
	} {
		if strings.Count(key, "{") != 1 || !strings.Contains(key, wantTag) {
			t.Fatalf("%s key %q does not contain exactly one canonical hash tag %q", name, key, wantTag)
		}
	}
}

func TestKeyBuilderRejectsRawOrUnboundedKeyComponents(t *testing.T) {
	builder, err := admissionredis.NewKeyBuilder("railway")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		run     string
		class   string
		version int64
	}{
		{"not-a-uuid", "standard", 1},
		{uuid.NewString(), "premium", 1},
		{uuid.NewString(), "standard}|evil", 1},
		{uuid.NewString(), "standard", 0},
		{uuid.NewString(), "standard", admissionredis.MaxPolicyVersion + 1},
	}
	for index, tc := range cases {
		if _, err := builder.ForPolicy(tc.run, tc.class, tc.version); !errors.Is(err, admissionredis.ErrInvalidKeyScope) {
			t.Fatalf("case %d error = %v", index, err)
		}
	}
	if _, err := admissionredis.NewKeyBuilder("raw:{unsafe}"); !errors.Is(err, admissionredis.ErrInvalidKeyScope) {
		t.Fatalf("unsafe namespace error = %v", err)
	}
}

func TestKeyBuilderUsesOnlyCanonicalServerEntryIDForExactLocator(t *testing.T) {
	builder, err := admissionredis.NewKeyBuilder("railway")
	if err != nil {
		t.Fatal(err)
	}
	entryID := uuid.New()
	key, err := builder.EntryLocator(strings.ToUpper(entryID.String()))
	if err != nil {
		t.Fatal(err)
	}
	if want := "railway:wr:entry-locator:" + entryID.String(); key != want {
		t.Fatalf("EntryLocator() = %q, want %q", key, want)
	}
	if _, err := builder.EntryLocator("../" + entryID.String()); !errors.Is(err, admissionredis.ErrInvalidKeyScope) {
		t.Fatalf("unsafe locator error = %v", err)
	}
}

func TestKeyBuilderUsesOnlyFixedWidthTokenHashForExactLocator(t *testing.T) {
	builder, err := admissionredis.NewKeyBuilder("railway")
	if err != nil {
		t.Fatal(err)
	}
	tokenHash := sha256.Sum256([]byte("opaque bearer used only by the test"))
	key, err := builder.TokenLocator(tokenHash)
	if err != nil {
		t.Fatal(err)
	}
	locatorInput := append([]byte("railway-admission-token-locator/v1\x00"), tokenHash[:]...)
	locatorDigest := sha256.Sum256(locatorInput)
	if want := "railway:wr:token-locator:" + hex.EncodeToString(locatorDigest[:]); key != want {
		t.Fatalf("TokenLocator() = %q, want fixed-width %q", key, want)
	}
	if strings.Contains(key, hex.EncodeToString(tokenHash[:])) {
		t.Fatal("TokenLocator() exposed the token hash")
	}
	if strings.Contains(key, "opaque bearer") {
		t.Fatal("TokenLocator() exposed raw bearer material")
	}
}
