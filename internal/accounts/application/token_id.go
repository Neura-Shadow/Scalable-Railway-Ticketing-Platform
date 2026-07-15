package application

import (
	"crypto/sha256"
	"errors"

	"github.com/google/uuid"
)

var ErrInvalidTokenIdentifier = errors.New("invalid token identifier")

func HashTokenID(tokenID string) ([sha256.Size]byte, error) {
	parsed, err := uuid.Parse(tokenID)
	if err != nil || parsed == uuid.Nil {
		return [sha256.Size]byte{}, ErrInvalidTokenIdentifier
	}
	return sha256.Sum256([]byte(parsed.String())), nil
}
