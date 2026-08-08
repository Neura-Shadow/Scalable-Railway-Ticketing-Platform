package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
)

func TestRetryMaximumIsBounded(t *testing.T) {
	t.Parallel()
	if got := retryMaximum(time.Second); got != 32*time.Second {
		t.Fatalf("retryMaximum(1s) = %s", got)
	}
	if got := retryMaximum(5 * time.Minute); got != time.Hour {
		t.Fatalf("retryMaximum(5m) = %s", got)
	}
}

func TestReadinessTimeoutCoversProviderAndShard(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.DatabaseTimeout = time.Second
	cfg.PaymentProviderRequestTimeout = 3 * time.Second
	cfg.PhysicalShardQueryTimeout = 2 * time.Second
	if got := readinessTimeout(cfg); got != 3*time.Second {
		t.Fatalf("readinessTimeout() = %s", got)
	}
}

func TestPublicReasonDoesNotExposeUnexpectedErrorText(t *testing.T) {
	t.Parallel()
	secret := "postgres://sentinel-secret"
	got := publicReason(errors.New(secret))
	if got != "payment worker failure" || strings.Contains(got, secret) {
		t.Fatalf("publicReason() = %q", got)
	}
}

func TestPaymentWorkerControlSchemaVersionMatchesLatestMigration(t *testing.T) {
	t.Parallel()
	if paymentControlSchemaVersion != 10 {
		t.Fatalf("paymentControlSchemaVersion = %d, want 10", paymentControlSchemaVersion)
	}
}
