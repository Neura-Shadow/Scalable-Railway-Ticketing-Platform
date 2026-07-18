package domain

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// MaxPassengerDisplayNameRunes is the shared public and persistence boundary
// for passenger display names.
const MaxPassengerDisplayNameRunes = 100

// ValidPassengerDisplayName reports whether value contains a non-empty display
// name that fits the persisted character limit.
func ValidPassengerDisplayName(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	normalized := strings.TrimSpace(value)
	for _, character := range normalized {
		if unicode.IsControl(character) {
			return false
		}
	}
	length := utf8.RuneCountInString(normalized)
	return length >= 1 && length <= MaxPassengerDisplayNameRunes
}
