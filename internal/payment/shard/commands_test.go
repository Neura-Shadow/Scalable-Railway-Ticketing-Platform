package shard_test

import (
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/google/uuid"
)

func TestDeterministicIssuanceIDMatchesMigrationGoldenVector(t *testing.T) {
	t.Parallel()

	sagaID := uuid.MustParse("79000000-0000-4000-8000-000000000001")
	want := uuid.MustParse("ddb62b09-9c50-526a-adb4-e32a16aa7c66")
	if got := shard.DeterministicIssuanceID(sagaID); got != want {
		t.Fatalf("DeterministicIssuanceID(%s) = %s, want %s", sagaID, got, want)
	}
}
