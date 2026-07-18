package domain_test

import (
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
)

func TestValidPassengerDisplayNameUsesRuneBoundary(t *testing.T) {
	t.Parallel()

	for name, testCase := range map[string]struct {
		value string
		valid bool
	}{
		"empty":               {value: " \t ", valid: false},
		"one rune":            {value: "界", valid: true},
		"exact rune limit":    {value: strings.Repeat("界", domain.MaxPassengerDisplayNameRunes), valid: true},
		"above rune limit":    {value: strings.Repeat("界", domain.MaxPassengerDisplayNameRunes+1), valid: false},
		"trimmed exact limit": {value: " " + strings.Repeat("界", domain.MaxPassengerDisplayNameRunes) + " ", valid: true},
		"embedded control":    {value: "Rider\x00", valid: false},
		"malformed UTF-8":     {value: string([]byte{0xff}), valid: false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := domain.ValidPassengerDisplayName(testCase.value); got != testCase.valid {
				t.Fatalf("ValidPassengerDisplayName() = %t, want %t", got, testCase.valid)
			}
		})
	}
}
