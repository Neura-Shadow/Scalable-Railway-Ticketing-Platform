package postgres

import (
	"errors"
	"fmt"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/domain"
	"github.com/jackc/pgx/v5/pgtype"
)

var ErrInvalidSegmentBits = errors.New("invalid PostgreSQL segment bits")

// EncodeSegmentMask is the only boundary that maps the domain's route-ordered
// segment mask to PostgreSQL BIT VARYING. pgtype and SegmentMask both number
// the most-significant bit of the first byte as bit zero.
func EncodeSegmentMask(mask domain.SegmentMask) pgtype.Bits {
	return pgtype.Bits{
		Bytes: mask.Bytes(),
		Len:   int32(mask.BitLength()),
		Valid: true,
	}
}

// DecodeSegmentMask validates database values before returning a domain mask.
// NULL, non-positive lengths, malformed byte counts, and set padding bits are
// treated as corrupted persistence state.
func DecodeSegmentMask(bits pgtype.Bits) (domain.SegmentMask, error) {
	if !bits.Valid || bits.Len <= 0 || int64(bits.Len) > int64(^uint(0)>>1) {
		return domain.SegmentMask{}, ErrInvalidSegmentBits
	}
	if len(bits.Bytes) != (int(bits.Len)+7)/8 {
		return domain.SegmentMask{}, ErrInvalidSegmentBits
	}

	mask, err := domain.SegmentMaskFromBytes(int(bits.Len), bits.Bytes)
	if err != nil {
		return domain.SegmentMask{}, fmt.Errorf("%w: %v", ErrInvalidSegmentBits, err)
	}
	return mask, nil
}
