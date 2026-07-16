package application_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/application"
	"golang.org/x/crypto/bcrypt"
)

func TestBcryptPasswordHasherHashesAndVerifiesPassword(t *testing.T) {
	t.Parallel()

	hasher, err := application.NewBcryptPasswordHasher(bcrypt.MinCost)
	if err != nil {
		t.Fatalf("NewBcryptPasswordHasher() error = %v", err)
	}

	first, err := hasher.Hash("correct horse battery staple")
	if err != nil {
		t.Fatalf("Hash() error = %v", err)
	}
	second, err := hasher.Hash("correct horse battery staple")
	if err != nil {
		t.Fatalf("Hash() second error = %v", err)
	}
	if first == second {
		t.Fatal("Hash() returned identical salted hashes")
	}
	if err := hasher.Verify("correct horse battery staple", first); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
}

func TestBcryptPasswordHasherRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	t.Run("cost", func(t *testing.T) {
		t.Parallel()

		tests := []int{bcrypt.MinCost - 1, bcrypt.MaxCost + 1}
		for _, cost := range tests {
			if _, err := application.NewBcryptPasswordHasher(cost); !errors.Is(err, application.ErrInvalidBcryptCost) {
				t.Fatalf("NewBcryptPasswordHasher(%d) error = %v, want %v", cost, err, application.ErrInvalidBcryptCost)
			}
		}
	})

	t.Run("password", func(t *testing.T) {
		t.Parallel()

		hasher, err := application.NewBcryptPasswordHasher(bcrypt.MinCost)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := hasher.Hash("right password")
		if err != nil {
			t.Fatal(err)
		}

		tests := []struct {
			name     string
			password string
			hash     string
		}{
			{name: "wrong password", password: "wrong password", hash: encoded},
			{name: "malformed hash", password: "right password", hash: "not-bcrypt"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				if err := hasher.Verify(tt.password, tt.hash); !errors.Is(err, application.ErrPasswordMismatch) {
					t.Fatalf("Verify() error = %v, want %v", err, application.ErrPasswordMismatch)
				}
			})
		}

		if _, err := hasher.Hash(strings.Repeat("x", 73)); err == nil {
			t.Fatal("Hash() error = nil for password exceeding bcrypt's 72-byte limit")
		}
	})
}
