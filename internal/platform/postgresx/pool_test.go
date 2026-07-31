package postgresx_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
)

func TestNewBoundedPoolAppliesMaximumWithoutConnecting(t *testing.T) {
	t.Parallel()
	pool, err := postgresx.NewBoundedPool(context.Background(),
		"postgres://synthetic@127.0.0.1:1/railway?connect_timeout=1", 7)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if got := pool.Config().MaxConns; got != 7 {
		t.Fatalf("MaxConns = %d, want 7", got)
	}
}

func TestNewBoundedPoolRejectsInvalidOrSecretBearingConfigSafely(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		dsn string
		max int
	}{
		{"postgres://user:sentinel-secret@%zz", 4},
		{"postgres://user:sentinel-secret@localhost/db", 0},
	} {
		_, err := postgresx.NewBoundedPool(context.Background(), test.dsn, test.max)
		if !errors.Is(err, postgresx.ErrInvalidPoolConfig) || strings.Contains(err.Error(), "sentinel-secret") {
			t.Fatalf("error = %v, want redacted sentinel", err)
		}
	}
}
