package postgresx_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/jackc/pgx/v5"
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

func TestNewRegionalBoundedPoolInstallsBoundedAuthorityParameters(t *testing.T) {
	t.Parallel()
	pool, err := postgresx.NewRegionalBoundedPool(context.Background(),
		"postgres://synthetic@127.0.0.1:1/railway?connect_timeout=1", 7,
		postgresx.RegionalSession{Region: "region-b", Role: "active", Epoch: 9, WritesEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	want := map[string]string{
		"railway.deployment_region":       "region-b",
		"railway.deployment_role":         "active",
		"railway.region_epoch":            "9",
		"railway.regional_writes_enabled": "true",
	}
	for name, value := range want {
		if got := pool.Config().ConnConfig.RuntimeParams[name]; got != value {
			t.Fatalf("runtime parameter %s = %q, want %q", name, got, value)
		}
	}
}

func TestNewRegionalBoundedPoolRejectsUnboundedAuthorityParameters(t *testing.T) {
	t.Parallel()
	for _, session := range []postgresx.RegionalSession{
		{},
		{Region: "attacker-region", Role: "active", Epoch: 1, WritesEnabled: true},
		{Region: "region-a", Role: "operator", Epoch: 1, WritesEnabled: true},
		{Region: "region-a", Role: "active", Epoch: 0, WritesEnabled: true},
		{Region: "region-a", Role: "passive", Epoch: 1, WritesEnabled: true},
	} {
		if _, err := postgresx.NewRegionalBoundedPool(context.Background(),
			"postgres://synthetic@127.0.0.1:1/railway?connect_timeout=1", 7, session); !errors.Is(err, postgresx.ErrInvalidPoolConfig) {
			t.Fatalf("session %+v error = %v", session, err)
		}
	}
}

func TestParseRegionalSessionRequiresCanonicalBoundedValues(t *testing.T) {
	t.Parallel()
	session, err := postgresx.ParseRegionalSession(" region-b ", " ACTIVE ", "9", "true")
	if err != nil {
		t.Fatal(err)
	}
	if session != (postgresx.RegionalSession{Region: "region-b", Role: "active", Epoch: 9, WritesEnabled: true}) {
		t.Fatalf("session = %+v", session)
	}
}

func TestParseRegionalSessionFailsClosedWithoutExposingInput(t *testing.T) {
	t.Parallel()
	const sentinel = "do-not-expose-regional-input"
	tests := []struct {
		region string
		role   string
		epoch  string
		writes string
	}{
		{},
		{region: "region-a", role: "active", epoch: sentinel, writes: "true"},
		{region: "region-a", role: "active", epoch: "1", writes: sentinel},
		{region: "region-a", role: "passive", epoch: "1", writes: "true"},
	}
	for _, test := range tests {
		_, err := postgresx.ParseRegionalSession(test.region, test.role, test.epoch, test.writes)
		if !errors.Is(err, postgresx.ErrInvalidPoolConfig) {
			t.Fatalf("ParseRegionalSession(%q, %q, %q, %q) error = %v", test.region, test.role, test.epoch, test.writes, err)
		}
		if strings.Contains(err.Error(), sentinel) {
			t.Fatalf("error exposed configuration input: %v", err)
		}
	}
}

func TestApplyRegionalSessionInitializesRuntimeParameters(t *testing.T) {
	t.Parallel()
	config := &pgx.ConnConfig{}
	err := postgresx.ApplyRegionalSession(config, postgresx.RegionalSession{
		Region: "region-a", Role: "passive", Epoch: 11, WritesEnabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.RuntimeParams["railway.region_epoch"] != "11" ||
		config.RuntimeParams["railway.regional_writes_enabled"] != "false" {
		t.Fatalf("runtime parameters = %+v", config.RuntimeParams)
	}
}
