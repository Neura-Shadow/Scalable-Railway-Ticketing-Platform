package domain

import (
	"errors"
	"strings"
)

var (
	ErrInvalidSegmentRange       = errors.New("invalid segment range")
	ErrInvalidSegmentMask        = errors.New("invalid segment mask")
	ErrSegmentMaskLengthMismatch = errors.New("segment mask length mismatch")
)

// SegmentMask represents occupied route segments in route order. Bit index zero
// is the most-significant bit of the first byte and the leftmost display bit.
type SegmentMask struct {
	bits      []byte
	bitLength int
}

func NewSegmentMask(segmentCount, fromIndex, toIndex int) (SegmentMask, error) {
	if segmentCount <= 0 || fromIndex < 0 || fromIndex >= toIndex || toIndex > segmentCount {
		return SegmentMask{}, ErrInvalidSegmentRange
	}

	bits := make([]byte, (segmentCount+7)/8)
	for index := fromIndex; index < toIndex; index++ {
		bits[index/8] |= byte(0x80 >> (index % 8))
	}

	return SegmentMask{bits: bits, bitLength: segmentCount}, nil
}

func SegmentMaskFromBytes(bitLength int, source []byte) (SegmentMask, error) {
	if bitLength <= 0 || len(source) != (bitLength+7)/8 {
		return SegmentMask{}, ErrInvalidSegmentMask
	}
	unusedBits := len(source)*8 - bitLength
	if unusedBits > 0 {
		unusedMask := byte((1 << unusedBits) - 1)
		if source[len(source)-1]&unusedMask != 0 {
			return SegmentMask{}, ErrInvalidSegmentMask
		}
	}
	bits := append([]byte(nil), source...)
	return SegmentMask{bits: bits, bitLength: bitLength}, nil
}

func (m SegmentMask) String() string {
	var builder strings.Builder
	builder.Grow(m.bitLength)
	for index := 0; index < m.bitLength; index++ {
		if m.bits[index/8]&(byte(0x80>>(index%8))) != 0 {
			builder.WriteByte('1')
		} else {
			builder.WriteByte('0')
		}
	}
	return builder.String()
}

func (m SegmentMask) Overlaps(other SegmentMask) (bool, error) {
	if !m.valid() || !other.valid() {
		return false, ErrInvalidSegmentMask
	}
	if m.bitLength != other.bitLength {
		return false, ErrSegmentMaskLengthMismatch
	}
	for index := range m.bits {
		if m.bits[index]&other.bits[index] != 0 {
			return true, nil
		}
	}
	return false, nil
}

func (m SegmentMask) Union(other SegmentMask) (SegmentMask, error) {
	if !m.valid() || !other.valid() {
		return SegmentMask{}, ErrInvalidSegmentMask
	}
	if m.bitLength != other.bitLength {
		return SegmentMask{}, ErrSegmentMaskLengthMismatch
	}
	bits := make([]byte, len(m.bits))
	for index := range bits {
		bits[index] = m.bits[index] | other.bits[index]
	}
	return SegmentMask{bits: bits, bitLength: m.bitLength}, nil
}

func (m SegmentMask) Subtract(other SegmentMask) (SegmentMask, error) {
	if !m.valid() || !other.valid() {
		return SegmentMask{}, ErrInvalidSegmentMask
	}
	if m.bitLength != other.bitLength {
		return SegmentMask{}, ErrSegmentMaskLengthMismatch
	}
	bits := make([]byte, len(m.bits))
	for index := range bits {
		bits[index] = m.bits[index] &^ other.bits[index]
	}
	return SegmentMask{bits: bits, bitLength: m.bitLength}, nil
}

func (m SegmentMask) IsZero() bool {
	for _, value := range m.bits {
		if value != 0 {
			return false
		}
	}
	return true
}

func (m SegmentMask) BitLength() int {
	return m.bitLength
}

func (m SegmentMask) Bytes() []byte {
	return append([]byte(nil), m.bits...)
}

func (m SegmentMask) Equal(other SegmentMask) bool {
	if m.bitLength != other.bitLength || len(m.bits) != len(other.bits) {
		return false
	}
	for index := range m.bits {
		if m.bits[index] != other.bits[index] {
			return false
		}
	}
	return true
}

func (m SegmentMask) valid() bool {
	if m.bitLength <= 0 || len(m.bits) != (m.bitLength+7)/8 {
		return false
	}
	unusedBits := len(m.bits)*8 - m.bitLength
	return unusedBits == 0 || m.bits[len(m.bits)-1]&byte((1<<unusedBits)-1) == 0
}
