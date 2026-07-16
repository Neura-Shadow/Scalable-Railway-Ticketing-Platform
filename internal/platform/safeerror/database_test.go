package safeerror

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestDatabaseRedactsConnectionDetailsFromEverySupportedFailureShape(t *testing.T) {
	t.Parallel()

	const secret = "sentinel-m11-database-password"
	inputs := []string{
		"postgres://railway:" + secret + "@db.example:5432/railway?sslmode=require",
		"postgres://railway:" + secret + "@@db.example:5432/railway?token=query-secret",
		"host=db.example user=railway password=" + secret + " dbname=railway sslkey=/private/client.key",
		"completely-invalid-" + secret,
	}
	for _, input := range inputs {
		got := Database(DatabaseConnectionFailed, fmt.Errorf("open %s: failed", input))
		if !errors.Is(got, ErrDatabaseConnectionFailed) {
			t.Fatalf("Database() error = %v, want %v", got, ErrDatabaseConnectionFailed)
		}
		wrapped := fmt.Errorf("startup: %w", got)
		for _, forbidden := range []string{secret, "query-secret", "client.key", input} {
			if strings.Contains(wrapped.Error(), forbidden) {
				t.Fatalf("Database() error exposed %q: %v", forbidden, wrapped)
			}
		}
	}
}

func TestDatabaseUsesOnlyBoundedFailureCategories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		category DatabaseCategory
		want     error
	}{
		{DatabaseConfigurationInvalid, ErrDatabaseConfigurationInvalid},
		{DatabaseConnectionFailed, ErrDatabaseConnectionFailed},
		{MigrationConnectionFailed, ErrMigrationConnectionFailed},
		{MigrationOperationFailed, ErrMigrationOperationFailed},
		{MigrationCloseFailed, ErrMigrationCloseFailed},
		{DatabaseCategory(255), ErrDatabaseOperationFailed},
	}
	for _, test := range tests {
		got := Database(test.category, errors.New("sentinel-m11-database-password"))
		if !errors.Is(got, test.want) || got.Error() != test.want.Error() {
			t.Fatalf("Database(%d) = %v, want %v", test.category, got, test.want)
		}
	}
}
