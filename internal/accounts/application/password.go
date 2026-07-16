package application

import (
	"errors"
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

var (
	ErrInvalidBcryptCost = errors.New("invalid bcrypt cost")
	ErrPasswordMismatch  = errors.New("password mismatch")
)

type PasswordHasher interface {
	Hash(password string) (string, error)
	Verify(password, encodedHash string) error
}

type BcryptPasswordHasher struct {
	cost int
}

var _ PasswordHasher = (*BcryptPasswordHasher)(nil)

func NewBcryptPasswordHasher(cost int) (*BcryptPasswordHasher, error) {
	if cost < bcrypt.MinCost || cost > bcrypt.MaxCost {
		return nil, fmt.Errorf("%w: %d", ErrInvalidBcryptCost, cost)
	}
	return &BcryptPasswordHasher{cost: cost}, nil
}

func (h *BcryptPasswordHasher) Hash(password string) (string, error) {
	encoded, err := bcrypt.GenerateFromPassword([]byte(password), h.cost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(encoded), nil
}

func (h *BcryptPasswordHasher) Verify(password, encodedHash string) error {
	if err := bcrypt.CompareHashAndPassword([]byte(encodedHash), []byte(password)); err != nil {
		return ErrPasswordMismatch
	}
	return nil
}
