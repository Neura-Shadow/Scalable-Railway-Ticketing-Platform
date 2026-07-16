package domain_test

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
)

func TestNewStationCodeNormalizesUserInput(t *testing.T) {
	t.Parallel()

	code, err := domain.NewStationCode("  tpe01  ")
	if err != nil {
		t.Fatalf("NewStationCode() error = %v", err)
	}
	if got, want := code.String(), "TPE01"; got != want {
		t.Fatalf("StationCode.String() = %q, want %q", got, want)
	}
}

func TestNewStationCodeRejectsInvalidCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code string
	}{
		{name: "empty", code: ""},
		{name: "too short", code: "A"},
		{name: "too long", code: "ABCDEFGHIJKLM"},
		{name: "punctuation", code: "TPE-01"},
		{name: "unicode", code: "\u53f0\u5317"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := domain.NewStationCode(tt.code)
			if !errors.Is(err, domain.ErrInvalidStationCode) {
				t.Fatalf("NewStationCode(%q) error = %v, want ErrInvalidStationCode", tt.code, err)
			}
		})
	}
}
