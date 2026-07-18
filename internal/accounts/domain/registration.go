package domain

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// MinRegistrationPasswordRunes is the public registration character
	// minimum. Existing-account login remains backward compatible.
	MinRegistrationPasswordRunes = 12
	// MaxBcryptPasswordBytes is bcrypt's maximum accepted password payload.
	MaxBcryptPasswordBytes    = 72
	maxRegistrationEmailRunes = 320
)

// ValidRegistrationEmail applies the bounded registration syntax before any
// uniqueness-sensitive database operation.
func ValidRegistrationEmail(value string) bool {
	normalized := strings.TrimSpace(value)
	if !utf8.ValidString(normalized) {
		return false
	}
	length := utf8.RuneCountInString(normalized)
	if length < 3 || length > maxRegistrationEmailRunes || strings.Count(normalized, "@") != 1 {
		return false
	}
	separator := strings.IndexByte(normalized, '@')
	if separator <= 0 || separator >= len(normalized)-1 {
		return false
	}
	for _, character := range normalized {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return false
		}
	}
	return true
}

// ValidRegistrationPassword aligns the public character minimum with
// bcrypt's byte ceiling before any uniqueness-sensitive database operation.
func ValidRegistrationPassword(value string) bool {
	return utf8.ValidString(value) &&
		utf8.RuneCountInString(value) >= MinRegistrationPasswordRunes &&
		len(value) <= MaxBcryptPasswordBytes
}
