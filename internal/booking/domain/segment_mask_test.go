package domain_test

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/domain"
)

func TestSegmentMaskUsesRouteOrder(t *testing.T) {
	t.Parallel()

	mask, err := domain.NewSegmentMask(3, 0, 2)
	if err != nil {
		t.Fatalf("NewSegmentMask() error = %v", err)
	}
	if got, want := mask.String(), "110"; got != want {
		t.Fatalf("SegmentMask.String() = %q, want %q", got, want)
	}
}

func TestSegmentMaskRejectsInvalidRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		segmentCount int
		from         int
		to           int
	}{
		{name: "no segments", segmentCount: 0, from: 0, to: 1},
		{name: "negative origin", segmentCount: 3, from: -1, to: 1},
		{name: "same station", segmentCount: 3, from: 1, to: 1},
		{name: "reverse direction", segmentCount: 3, from: 2, to: 1},
		{name: "destination outside route", segmentCount: 3, from: 0, to: 4},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := domain.NewSegmentMask(test.segmentCount, test.from, test.to)
			if !errors.Is(err, domain.ErrInvalidSegmentRange) {
				t.Fatalf("NewSegmentMask() error = %v, want ErrInvalidSegmentRange", err)
			}
		})
	}
}

func TestSegmentMaskRejectsInvalidStorageBytes(t *testing.T) {
	t.Parallel()

	if _, err := domain.SegmentMaskFromBytes(9, []byte{0x80}); !errors.Is(err, domain.ErrInvalidSegmentMask) {
		t.Fatalf("short bytes error = %v, want ErrInvalidSegmentMask", err)
	}
	if _, err := domain.SegmentMaskFromBytes(9, []byte{0x80, 0x01}); !errors.Is(err, domain.ErrInvalidSegmentMask) {
		t.Fatalf("non-zero padding error = %v, want ErrInvalidSegmentMask", err)
	}

	mask, err := domain.SegmentMaskFromBytes(9, []byte{0x80, 0x80})
	if err != nil {
		t.Fatalf("SegmentMaskFromBytes() error = %v", err)
	}
	got := mask.Bytes()
	got[0] = 0
	if mask.String() != "100000001" {
		t.Fatal("Bytes() must return a copy")
	}
}

func TestSegmentMaskOverlapFollowsSharedSegments(t *testing.T) {
	t.Parallel()

	aToC, err := domain.NewSegmentMask(3, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	bToD, err := domain.NewSegmentMask(3, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	cToD, err := domain.NewSegmentMask(3, 2, 3)
	if err != nil {
		t.Fatal(err)
	}

	overlaps, err := aToC.Overlaps(bToD)
	if err != nil {
		t.Fatalf("Overlaps() error = %v", err)
	}
	if !overlaps {
		t.Fatal("A-C should overlap B-D")
	}

	overlaps, err = aToC.Overlaps(cToD)
	if err != nil {
		t.Fatalf("Overlaps() error = %v", err)
	}
	if overlaps {
		t.Fatal("A-C should not overlap C-D")
	}
}

func TestSegmentMaskAlgebraSupportsMoreThan64Segments(t *testing.T) {
	t.Parallel()

	first, err := domain.NewSegmentMask(129, 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	second, err := domain.NewSegmentMask(129, 64, 129)
	if err != nil {
		t.Fatal(err)
	}

	union, err := first.Union(second)
	if err != nil {
		t.Fatalf("Union() error = %v", err)
	}
	if got, want := union.BitLength(), 129; got != want {
		t.Fatalf("BitLength() = %d, want %d", got, want)
	}
	if union.IsZero() {
		t.Fatal("union should not be zero")
	}

	remainder, err := union.Subtract(first)
	if err != nil {
		t.Fatalf("Subtract() error = %v", err)
	}
	if !remainder.Equal(second) {
		t.Fatal("subtract should preserve only the unrelated second mask")
	}
}

func TestSegmentMaskOverlapMatchesIntervalIntersection(t *testing.T) {
	t.Parallel()

	for segmentCount := 1; segmentCount <= 8; segmentCount++ {
		for fromA := 0; fromA < segmentCount; fromA++ {
			for toA := fromA + 1; toA <= segmentCount; toA++ {
				maskA, err := domain.NewSegmentMask(segmentCount, fromA, toA)
				if err != nil {
					t.Fatal(err)
				}
				for fromB := 0; fromB < segmentCount; fromB++ {
					for toB := fromB + 1; toB <= segmentCount; toB++ {
						maskB, err := domain.NewSegmentMask(segmentCount, fromB, toB)
						if err != nil {
							t.Fatal(err)
						}
						got, err := maskA.Overlaps(maskB)
						if err != nil {
							t.Fatal(err)
						}
						want := fromA < toB && fromB < toA
						if got != want {
							t.Fatalf("segments=%d A=[%d,%d) B=[%d,%d): Overlaps()=%t, want %t", segmentCount, fromA, toA, fromB, toB, got, want)
						}
					}
				}
			}
		}
	}
}

func TestSegmentMaskRejectsDifferentLengths(t *testing.T) {
	t.Parallel()

	short, _ := domain.NewSegmentMask(3, 0, 1)
	long, _ := domain.NewSegmentMask(4, 0, 1)
	if _, err := short.Overlaps(long); !errors.Is(err, domain.ErrSegmentMaskLengthMismatch) {
		t.Fatalf("Overlaps() error = %v, want ErrSegmentMaskLengthMismatch", err)
	}
	if _, err := short.Union(long); !errors.Is(err, domain.ErrSegmentMaskLengthMismatch) {
		t.Fatalf("Union() error = %v, want ErrSegmentMaskLengthMismatch", err)
	}
	if _, err := short.Subtract(long); !errors.Is(err, domain.ErrSegmentMaskLengthMismatch) {
		t.Fatalf("Subtract() error = %v, want ErrSegmentMaskLengthMismatch", err)
	}
}
