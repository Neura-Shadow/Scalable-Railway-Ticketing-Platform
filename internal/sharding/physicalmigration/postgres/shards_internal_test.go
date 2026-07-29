package postgres

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
)

func TestBoundedCaptureBytesEnforcesWholeSnapshotCap(t *testing.T) {
	t.Parallel()

	remaining := int64(captureOutboxTotalBytes - 1)
	if total, err := boundedCaptureBytes(remaining, 1); err != nil || total != captureOutboxTotalBytes {
		t.Fatalf("boundedCaptureBytes at cap = %d, %v", total, err)
	}
	if _, err := boundedCaptureBytes(captureOutboxTotalBytes, 1); !errors.Is(err, physicalmigration.ErrCleanupLimitExceeded) {
		t.Fatalf("boundedCaptureBytes overflow = %v", err)
	}
}
