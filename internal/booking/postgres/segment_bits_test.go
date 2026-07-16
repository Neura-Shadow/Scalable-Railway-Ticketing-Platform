package postgres

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/domain"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestSegmentBitsRoundTripPreservesLengthAndRouteOrder(t *testing.T) {
	t.Parallel()

	mask, err := domain.NewSegmentMask(129, 1, 128)
	if err != nil {
		t.Fatalf("new segment mask: %v", err)
	}

	encoded := EncodeSegmentMask(mask)
	if encoded.Len != 129 || !encoded.Valid {
		t.Fatalf("encoded bits = {Len:%d Valid:%t}, want {Len:129 Valid:true}", encoded.Len, encoded.Valid)
	}

	decoded, err := DecodeSegmentMask(encoded)
	if err != nil {
		t.Fatalf("decode segment mask: %v", err)
	}
	if !decoded.Equal(mask) {
		t.Fatalf("round trip = %s, want %s", decoded.String(), mask.String())
	}
}

func TestDecodeSegmentMaskRejectsInvalidDatabaseBits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		bits pgtype.Bits
	}{
		{name: "null", bits: pgtype.Bits{}},
		{name: "zero length", bits: pgtype.Bits{Bytes: []byte{}, Len: 0, Valid: true}},
		{name: "length exceeds bytes", bits: pgtype.Bits{Bytes: []byte{0x80}, Len: 9, Valid: true}},
		{name: "unused bits set", bits: pgtype.Bits{Bytes: []byte{0x81}, Len: 7, Valid: true}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeSegmentMask(test.bits)
			if !errors.Is(err, ErrInvalidSegmentBits) {
				t.Fatalf("DecodeSegmentMask() error = %v, want %v", err, ErrInvalidSegmentBits)
			}
		})
	}
}
