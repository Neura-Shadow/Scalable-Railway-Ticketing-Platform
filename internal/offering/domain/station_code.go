package domain

import (
	"errors"
	"strings"
)

const (
	minStationCodeLength = 2
	maxStationCodeLength = 12
)

var ErrInvalidStationCode = errors.New("invalid station code")

// StationCode is a normalized, bounded identifier suitable for exact lookup.
type StationCode string

func NewStationCode(input string) (StationCode, error) {
	normalized := strings.ToUpper(strings.TrimSpace(input))
	if len(normalized) < minStationCodeLength || len(normalized) > maxStationCodeLength {
		return "", ErrInvalidStationCode
	}
	for _, character := range normalized {
		if (character < 'A' || character > 'Z') && (character < '0' || character > '9') {
			return "", ErrInvalidStationCode
		}
	}
	return StationCode(normalized), nil
}

func (c StationCode) String() string {
	return string(c)
}

func (c StationCode) IsValid() bool {
	_, err := NewStationCode(string(c))
	return err == nil && c == StationCode(strings.ToUpper(strings.TrimSpace(string(c))))
}
