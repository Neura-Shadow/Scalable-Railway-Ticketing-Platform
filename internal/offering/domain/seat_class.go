package domain

import (
	"errors"
	"strings"
)

var ErrInvalidSeatClass = errors.New("invalid seat class")

type SeatClass string

const (
	SeatClassStandard SeatClass = "standard"
	SeatClassBusiness SeatClass = "business"
	SeatClassFirst    SeatClass = "first"
)

func ParseSeatClass(input string) (SeatClass, error) {
	seatClass := SeatClass(strings.ToLower(strings.TrimSpace(input)))
	if !seatClass.IsValid() {
		return "", ErrInvalidSeatClass
	}
	return seatClass, nil
}

func (c SeatClass) IsValid() bool {
	switch c {
	case SeatClassStandard, SeatClassBusiness, SeatClassFirst:
		return true
	default:
		return false
	}
}

func (c SeatClass) String() string {
	return string(c)
}
