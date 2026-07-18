package domain_test

import (
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
)

func TestValidRegistrationEmail(t *testing.T) {
	t.Parallel()

	for name, testCase := range map[string]struct {
		value string
		valid bool
	}{
		"ordinary":           {value: "rider@example.test", valid: true},
		"normalized edge":    {value: " rider@example.test ", valid: true},
		"unicode local":      {value: "旅客@example.test", valid: true},
		"double separator":   {value: "rider@@example.test", valid: false},
		"missing local":      {value: "@example.test", valid: false},
		"missing host":       {value: "rider@", valid: false},
		"embedded space":     {value: "rider @example.test", valid: false},
		"embedded control":   {value: "rider\x00@example.test", valid: false},
		"malformed UTF-8":    {value: string([]byte{0xff}) + "@example.test", valid: false},
		"above length limit": {value: strings.Repeat("a", 309) + "@example.test", valid: false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := domain.ValidRegistrationEmail(testCase.value); got != testCase.valid {
				t.Fatalf("ValidRegistrationEmail() = %t, want %t", got, testCase.valid)
			}
		})
	}
}

func TestValidRegistrationPassword(t *testing.T) {
	t.Parallel()

	for name, testCase := range map[string]struct {
		value string
		valid bool
	}{
		"eleven ASCII runes":     {value: strings.Repeat("a", 11), valid: false},
		"twelve ASCII runes":     {value: strings.Repeat("a", 12), valid: true},
		"four multibyte runes":   {value: strings.Repeat("界", 4), valid: false},
		"twelve multibyte runes": {value: strings.Repeat("界", 12), valid: true},
		"exact bcrypt bytes":     {value: strings.Repeat("界", 24), valid: true},
		"above bcrypt bytes":     {value: strings.Repeat("界", 25), valid: false},
		"malformed UTF-8":        {value: strings.Repeat("a", 12) + string([]byte{0xff}), valid: false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := domain.ValidRegistrationPassword(testCase.value); got != testCase.valid {
				t.Fatalf("ValidRegistrationPassword() = %t, want %t", got, testCase.valid)
			}
		})
	}
}
