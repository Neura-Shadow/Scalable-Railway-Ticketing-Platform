package recovery_test

import (
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/recovery"
	"github.com/google/uuid"
)

func TestFencingAttestationCannotBeReusedForAnotherOperation(t *testing.T) {
	t.Parallel()

	source := mustRecoveryRegion(t, "region-a")
	epoch := mustRecoveryEpoch(t, 31)
	incidentID := uuid.New()
	declaredAt := time.Date(2026, 8, 11, 1, 2, 3, 0, time.UTC)
	binding, err := recovery.NewFenceBinding(
		uuid.New(),
		source,
		epoch,
		incidentID,
		"operator:alice",
		declaredAt,
	)
	if err != nil {
		t.Fatalf("NewFenceBinding() error = %v", err)
	}
	attestation := mustAttestation(t, binding, declaredAt.Add(time.Minute))
	if err := attestation.ValidateFor(binding); err != nil {
		t.Fatalf("ValidateFor(original) error = %v", err)
	}

	other, err := recovery.NewFenceBinding(
		uuid.New(),
		source,
		epoch,
		incidentID,
		"operator:alice",
		declaredAt,
	)
	if err != nil {
		t.Fatalf("NewFenceBinding(other) error = %v", err)
	}
	if err := attestation.ValidateFor(other); !errors.Is(err, recovery.ErrFencingBinding) {
		t.Fatalf("ValidateFor(other) error = %v, want ErrFencingBinding", err)
	}
}

func mustRecoveryRegion(t *testing.T, raw string) authority.Region {
	t.Helper()
	region, err := authority.ParseRegion(raw)
	if err != nil {
		t.Fatalf("ParseRegion(%q) error = %v", raw, err)
	}
	return region
}

func mustRecoveryEpoch(t *testing.T, raw uint64) authority.Epoch {
	t.Helper()
	epoch, err := authority.NewEpoch(raw)
	if err != nil {
		t.Fatalf("NewEpoch(%d) error = %v", raw, err)
	}
	return epoch
}
