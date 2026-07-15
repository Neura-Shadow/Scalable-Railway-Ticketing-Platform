package application_test

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/application"
)

func TestHashTokenIDCanonicalizesUUIDAndRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	first, err := application.HashTokenID("313AEF88-E233-4BB9-9575-9B4C3BA358EE")
	if err != nil {
		t.Fatal(err)
	}
	second, err := application.HashTokenID("313aef88-e233-4bb9-9575-9b4c3ba358ee")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("equivalent token IDs produced different hashes")
	}
	if _, err := application.HashTokenID("not-a-jti"); !errors.Is(err, application.ErrInvalidTokenIdentifier) {
		t.Fatalf("HashTokenID() error = %v", err)
	}
}
