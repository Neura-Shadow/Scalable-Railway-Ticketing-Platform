package config_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
)

func TestDefaultsProvideSafeOperationalValues(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()

	if cfg.Environment != config.EnvironmentDevelopment {
		t.Fatalf("Environment = %q, want %q", cfg.Environment, config.EnvironmentDevelopment)
	}
	if cfg.HTTPAddress != ":8080" {
		t.Fatalf("HTTPAddress = %q, want :8080", cfg.HTTPAddress)
	}
	if cfg.AccessTokenTTL != 15*time.Minute {
		t.Fatalf("AccessTokenTTL = %s, want 15m", cfg.AccessTokenTTL)
	}
	if cfg.RefreshTokenTTL != 7*24*time.Hour || cfg.BcryptCost != 12 {
		t.Fatalf("authentication defaults = refresh %s cost %d", cfg.RefreshTokenTTL, cfg.BcryptCost)
	}
	if cfg.HoldTTL != 10*time.Minute {
		t.Fatalf("HoldTTL = %s, want 10m", cfg.HoldTTL)
	}
	if cfg.MaxPassengersPerReservation != 6 {
		t.Fatalf("MaxPassengersPerReservation = %d, want 6", cfg.MaxPassengersPerReservation)
	}
	if cfg.WorkerBatchSize != 100 {
		t.Fatalf("WorkerBatchSize = %d, want 100", cfg.WorkerBatchSize)
	}
	if cfg.DatabaseURL != "" || cfg.RedisAddress != "" || cfg.JWTSecret != "" {
		t.Fatal("connection settings and secrets must not have built-in defaults")
	}
}

func TestProductionRequiresSafeDependenciesAndSecret(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	cfg.Environment = config.EnvironmentProduction
	cfg.JWTSecret = "sentinel-short-secret"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want production requirements error")
	}
	message := err.Error()
	for _, field := range []string{"JWT_SECRET", "DATABASE_URL", "REDIS_ADDRESS"} {
		if !strings.Contains(message, field) {
			t.Errorf("Validate() error %q does not name %s", message, field)
		}
	}
	if strings.Contains(message, cfg.JWTSecret) {
		t.Fatal("validation error exposed JWT secret")
	}
}

func TestHoldExpirerValidationDoesNotRequireJWTOrRedis(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	cfg.Environment = config.EnvironmentProduction
	cfg.DatabaseURL = "postgres://worker@db.example/railway"
	if err := cfg.ValidateFor(config.ProcessHoldExpirer); err != nil {
		t.Fatalf("ValidateFor(hold-expirer) error = %v", err)
	}
}

func TestAPIRequiresOwnedDependenciesInEveryEnvironment(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	cfg.DatabaseURL = "postgres://api@db.example/railway"

	err := cfg.ValidateFor(config.ProcessAPI)
	if err == nil {
		t.Fatal("ValidateFor(api) error = nil, want dependency requirements")
	}
	for _, field := range []string{"JWT_SECRET", "REDIS_ADDRESS"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("ValidateFor(api) error %q does not name %s", err, field)
		}
	}
}

func TestWorkersIgnoreInvalidUnusedOperationalSettings(t *testing.T) {
	t.Parallel()

	hold := config.Defaults()
	hold.DatabaseURL = "postgres://worker@db.example/railway"
	hold.AccessTokenTTL = 0
	hold.RedisTimeout = 0
	hold.OutboxBatchSize = 0
	if err := hold.ValidateFor(config.ProcessHoldExpirer); err != nil {
		t.Fatalf("ValidateFor(hold-expirer) error = %v", err)
	}

	outbox := config.Defaults()
	outbox.DatabaseURL = "postgres://worker@db.example/railway"
	outbox.JWTSecret = ""
	outbox.HoldExpirerBatchSize = 0
	outbox.HTTPReadTimeout = 0
	if err := outbox.ValidateFor(config.ProcessOutboxWorker); err != nil {
		t.Fatalf("ValidateFor(outbox-worker) error = %v", err)
	}
}

func TestOutboxPublisherProductionPolicy(t *testing.T) {
	t.Parallel()

	base := config.Defaults()
	base.Environment = config.EnvironmentProduction
	base.DatabaseURL = "postgres://worker@db.example/railway"

	t.Run("log rejected by default", func(t *testing.T) {
		cfg := base
		cfg.OutboxPublisher = "log"
		err := cfg.ValidateFor(config.ProcessOutboxWorker)
		if err == nil || !strings.Contains(err.Error(), "OUTBOX_PUBLISHER") {
			t.Fatalf("ValidateFor(outbox-worker) error = %v, want production log rejection", err)
		}
	})

	t.Run("explicit log override", func(t *testing.T) {
		cfg := base
		cfg.OutboxPublisher = "log"
		cfg.AllowLogPublisherInProduction = true
		if err := cfg.ValidateFor(config.ProcessOutboxWorker); err != nil {
			t.Fatalf("ValidateFor(outbox-worker) error = %v", err)
		}
	})

	t.Run("disabled publisher", func(t *testing.T) {
		cfg := base
		cfg.OutboxPublisherEnabled = false
		cfg.OutboxPublisher = "log"
		if err := cfg.ValidateFor(config.ProcessOutboxWorker); err != nil {
			t.Fatalf("ValidateFor(outbox-worker) error = %v", err)
		}
	})

	t.Run("redis stream", func(t *testing.T) {
		cfg := base
		cfg.OutboxPublisher = "redis_stream"
		cfg.RedisAddress = "redis.example:6379"
		if err := cfg.ValidateFor(config.ProcessOutboxWorker); err != nil {
			t.Fatalf("ValidateFor(outbox-worker) error = %v", err)
		}
	})

	t.Run("development log", func(t *testing.T) {
		cfg := base
		cfg.Environment = config.EnvironmentDevelopment
		cfg.OutboxPublisher = "log"
		if err := cfg.ValidateFor(config.ProcessOutboxWorker); err != nil {
			t.Fatalf("ValidateFor(outbox-worker) error = %v", err)
		}
	})
}

func TestLoadFromForHoldExpirerIgnoresUnusedSecretsAndSettings(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"APP_ENV":                       "production",
		"DATABASE_URL":                  "postgres://worker@db.example/railway",
		"HOLD_EXPIRER_ENABLED":          "true",
		"HOLD_EXPIRER_BATCH_SIZE":       "25",
		"HOLD_EXPIRER_INTERVAL_SECONDS": "15",
		"JWT_ACCESS_TTL":                "not-a-duration",
		"REDIS_ADDR":                    "not-an-address",
		"OUTBOX_PUBLISHER":              "unknown",
	}
	lookup := func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	}
	cfg, err := config.LoadFromFor(lookup, config.ProcessHoldExpirer)
	if err != nil {
		t.Fatalf("LoadFromFor(hold-expirer) error = %v", err)
	}
	if !cfg.HoldExpirerEnabled || cfg.HoldExpirerBatchSize != 25 || cfg.HoldExpirerInterval != 15*time.Second {
		t.Fatalf("hold-expirer settings = %+v", cfg)
	}
}

func TestLoadFromForLogOutboxIgnoresJWTAndRedisSettings(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"APP_ENV":                           "production",
		"DATABASE_URL":                      "postgres://worker@db.example/railway",
		"OUTBOX_PUBLISHER_ENABLED":          "true",
		"OUTBOX_PUBLISHER":                  "log",
		"ALLOW_LOG_PUBLISHER_IN_PRODUCTION": "true",
		"JWT_ACCESS_TTL":                    "not-a-duration",
		"REDIS_ADDR":                        "sentinel-redis-credential-not-an-address",
	}
	lookup := func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	}

	cfg, err := config.LoadFromFor(lookup, config.ProcessOutboxWorker)
	if err != nil {
		t.Fatalf("LoadFromFor(outbox-worker) error = %v", err)
	}
	if cfg.RedisAddress != "" || cfg.JWTSecret != "" {
		t.Fatalf("unused secrets were loaded: RedisAddress=%q JWTSecret=%q", cfg.RedisAddress, cfg.JWTSecret)
	}
}

func TestLoadFromForRedisOutboxRequiresRedisWithoutJWT(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"APP_ENV":                  "production",
		"DATABASE_URL":             "postgres://worker@db.example/railway",
		"OUTBOX_PUBLISHER_ENABLED": "true",
		"OUTBOX_PUBLISHER":         "redis_stream",
		"JWT_ACCESS_TTL":           "not-a-duration",
	}
	lookup := func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	}

	_, err := config.LoadFromFor(lookup, config.ProcessOutboxWorker)
	if err == nil || !strings.Contains(err.Error(), "REDIS_ADDRESS") {
		t.Fatalf("LoadFromFor(outbox-worker) error = %v, want Redis requirement", err)
	}
	if strings.Contains(err.Error(), env["JWT_ACCESS_TTL"]) {
		t.Fatal("unused JWT setting leaked into the outbox error")
	}
}

func TestEveryProcessRequiresDatabaseConfiguration(t *testing.T) {
	t.Parallel()

	for _, process := range []config.Process{config.ProcessAPI, config.ProcessHoldExpirer, config.ProcessOutboxWorker} {
		cfg := config.Defaults()
		if process == config.ProcessOutboxWorker {
			cfg.OutboxPublisher = "log"
		}
		err := cfg.ValidateFor(process)
		if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
			t.Fatalf("ValidateFor(%s) error = %v, want DATABASE_URL error", process, err)
		}
	}
}

func TestProductionRejectsDocumentedPlaceholderSecret(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	cfg.Environment = config.EnvironmentProduction
	cfg.DatabaseURL = "postgres://db.example/railway"
	cfg.RedisAddress = "redis.example:6379"
	cfg.JWTSecret = "replace-with-at-least-32-random-bytes"

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "JWT_SECRET") {
		t.Fatalf("Validate() error = %v, want JWT_SECRET error", err)
	}
	if strings.Contains(err.Error(), cfg.JWTSecret) {
		t.Fatal("validation error exposed placeholder secret")
	}
}

func TestValidateRejectsNonPositiveOperationalLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		field  string
		mutate func(*config.Config)
	}{
		{"access token TTL", "JWT_ACCESS_TTL", func(c *config.Config) { c.AccessTokenTTL = 0 }},
		{"hold TTL", "RESERVATION_HOLD_TTL", func(c *config.Config) { c.HoldTTL = -time.Second }},
		{"passengers", "MAX_PASSENGERS_PER_RESERVATION", func(c *config.Config) { c.MaxPassengersPerReservation = 0 }},
		{"HTTP read", "HTTP_READ_TIMEOUT", func(c *config.Config) { c.HTTPReadTimeout = 0 }},
		{"HTTP write", "HTTP_WRITE_TIMEOUT", func(c *config.Config) { c.HTTPWriteTimeout = 0 }},
		{"shutdown", "SHUTDOWN_TIMEOUT", func(c *config.Config) { c.ShutdownTimeout = 0 }},
		{"database", "DATABASE_TIMEOUT", func(c *config.Config) { c.DatabaseTimeout = 0 }},
		{"Redis", "REDIS_TIMEOUT", func(c *config.Config) { c.RedisTimeout = 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := config.Defaults()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.field) {
				t.Fatalf("Validate() error = %v, want error naming %s", err, tt.field)
			}
		})
	}
}

func TestValidateRejectsInvalidProxyAndCORSLists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		field  string
		mutate func(*config.Config)
	}{
		{"proxy", "TRUSTED_PROXIES", func(c *config.Config) { c.TrustedProxies = []string{"not-a-proxy"} }},
		{"CORS path", "CORS_ALLOWED_ORIGINS", func(c *config.Config) { c.CORSAllowedOrigins = []string{"https://app.example/private"} }},
		{"CORS wildcard in production", "CORS_ALLOWED_ORIGINS", func(c *config.Config) {
			c.Environment = config.EnvironmentProduction
			c.DatabaseURL = "postgres://db.example/railway"
			c.RedisAddress = "redis.example:6379"
			c.JWTSecret = strings.Repeat("s", 32)
			c.CORSAllowedOrigins = []string{"*"}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := config.Defaults()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.field) {
				t.Fatalf("Validate() error = %v, want error naming %s", err, tt.field)
			}
		})
	}
}

func TestLoadFromEnvironmentOverridesDefaults(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"APP_ENV":                        "test",
		"HTTP_ADDRESS":                   "127.0.0.1:9090",
		"DATABASE_URL":                   "postgres://db.example/railway",
		"REDIS_ADDRESS":                  "redis.example:6379",
		"JWT_SECRET":                     "test-secret",
		"JWT_ACCESS_TTL":                 "30m",
		"RESERVATION_HOLD_TTL":           "7m",
		"MAX_PASSENGERS_PER_RESERVATION": "4",
		"HTTP_READ_TIMEOUT":              "2s",
		"HTTP_WRITE_TIMEOUT":             "4s",
		"SHUTDOWN_TIMEOUT":               "6s",
		"DATABASE_TIMEOUT":               "800ms",
		"REDIS_TIMEOUT":                  "300ms",
		"TRUSTED_PROXIES":                "10.0.0.0/8, 192.0.2.1",
		"CORS_ALLOWED_ORIGINS":           "https://app.example, https://admin.example",
	}
	lookup := func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	}

	cfg, err := config.LoadFrom(lookup)
	if err != nil {
		t.Fatalf("LoadFrom() error = %v", err)
	}

	if cfg.Environment != config.EnvironmentTest || cfg.HTTPAddress != "127.0.0.1:9090" {
		t.Fatalf("environment or HTTP address not loaded: %+v", cfg)
	}
	if cfg.DatabaseURL != env["DATABASE_URL"] || cfg.RedisAddress != env["REDIS_ADDRESS"] || cfg.JWTSecret != env["JWT_SECRET"] {
		t.Fatal("dependency settings or secret not loaded")
	}
	if cfg.AccessTokenTTL != 30*time.Minute || cfg.HoldTTL != 7*time.Minute {
		t.Fatal("TTL values not parsed")
	}
	if cfg.MaxPassengersPerReservation != 4 {
		t.Fatal("API integer values not parsed")
	}
	if cfg.HTTPReadTimeout != 2*time.Second || cfg.HTTPWriteTimeout != 4*time.Second || cfg.ShutdownTimeout != 6*time.Second || cfg.DatabaseTimeout != 800*time.Millisecond || cfg.RedisTimeout != 300*time.Millisecond {
		t.Fatal("timeout values not parsed")
	}
	if !reflect.DeepEqual(cfg.TrustedProxies, []string{"10.0.0.0/8", "192.0.2.1"}) {
		t.Fatalf("TrustedProxies = %v", cfg.TrustedProxies)
	}
	if !reflect.DeepEqual(cfg.CORSAllowedOrigins, []string{"https://app.example", "https://admin.example"}) {
		t.Fatalf("CORSAllowedOrigins = %v", cfg.CORSAllowedOrigins)
	}
}

func TestLoadFromSupportsCommittedEnvironmentContract(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"DATABASE_URL":                      "postgres://db.example/railway",
		"HTTP_ADDR":                         ":8181",
		"REDIS_ADDR":                        "redis:6379",
		"REDIS_PASSWORD":                    "sentinel-redis-secret",
		"JWT_SECRET":                        "sentinel-test-secret",
		"JWT_ISSUER":                        "railway-issuer",
		"JWT_AUDIENCE":                      "railway-users",
		"JWT_ACCESS_TTL":                    "20m",
		"JWT_REFRESH_TTL":                   "240h",
		"BCRYPT_COST":                       "11",
		"RESERVATION_HOLD_TTL_SECONDS":      "420",
		"RESERVATION_MAX_PASSENGERS":        "8",
		"HOLD_EXPIRER_ENABLED":              "true",
		"HOLD_EXPIRER_BATCH_SIZE":           "50",
		"HOLD_EXPIRER_INTERVAL_SECONDS":     "30",
		"OUTBOX_PUBLISHER":                  "log",
		"OUTBOX_BATCH_SIZE":                 "75",
		"OUTBOX_MAX_ATTEMPTS":               "7",
		"OUTBOX_POLL_INTERVAL_SECONDS":      "3",
		"OUTBOX_PROCESSING_TIMEOUT_SECONDS": "60",
		"OUTBOX_RETRY_BASE_SECONDS":         "2",
		"OUTBOX_RETRY_MAX_SECONDS":          "45",
	}
	lookup := func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	}

	cfg, err := config.LoadFrom(lookup)
	if err != nil {
		t.Fatalf("LoadFrom() error = %v", err)
	}
	if cfg.HTTPAddress != ":8181" || cfg.RedisAddress != "redis:6379" || cfg.RedisPassword != env["REDIS_PASSWORD"] {
		t.Fatal("HTTP or Redis environment contract not loaded")
	}
	if cfg.JWTIssuer != "railway-issuer" || cfg.JWTAudience != "railway-users" {
		t.Fatal("JWT metadata not loaded")
	}
	if cfg.AccessTokenTTL != 20*time.Minute || cfg.RefreshTokenTTL != 240*time.Hour || cfg.BcryptCost != 11 {
		t.Fatal("authentication runtime settings not loaded")
	}
	if cfg.HoldTTL != 420*time.Second || cfg.MaxPassengersPerReservation != 8 {
		t.Fatal("reservation settings not loaded")
	}
	holdCfg, err := config.LoadFromFor(lookup, config.ProcessHoldExpirer)
	if err != nil {
		t.Fatalf("LoadFromFor(hold-expirer) error = %v", err)
	}
	if !holdCfg.HoldExpirerEnabled || holdCfg.HoldExpirerBatchSize != 50 || holdCfg.HoldExpirerInterval != 30*time.Second {
		t.Fatal("hold-expirer settings not loaded")
	}
	outboxCfg, err := config.LoadFromFor(lookup, config.ProcessOutboxWorker)
	if err != nil {
		t.Fatalf("LoadFromFor(outbox-worker) error = %v", err)
	}
	if outboxCfg.OutboxPublisher != "log" || outboxCfg.OutboxBatchSize != 75 || outboxCfg.OutboxMaxAttempts != 7 ||
		outboxCfg.OutboxPollInterval != 3*time.Second || outboxCfg.OutboxProcessingTimeout != 60*time.Second ||
		outboxCfg.OutboxRetryBase != 2*time.Second || outboxCfg.OutboxRetryMax != 45*time.Second {
		t.Fatal("outbox settings not loaded")
	}
}

func TestValidateRejectsUnknownOutboxPublisher(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	cfg.OutboxPublisher = "shell"
	if err := cfg.ValidateFor(config.ProcessOutboxWorker); err == nil || !strings.Contains(err.Error(), "OUTBOX_PUBLISHER") {
		t.Fatalf("ValidateFor(outbox-worker) error = %v", err)
	}
}

func TestLoadErrorsNameVariablesWithoutEchoingValues(t *testing.T) {
	t.Parallel()

	invalidValue := "sentinel-secret-shaped-invalid-duration"
	lookup := func(key string) (string, bool) {
		if key == "RESERVATION_HOLD_TTL_SECONDS" {
			return invalidValue, true
		}
		return "", false
	}

	_, err := config.LoadFrom(lookup)
	if err == nil {
		t.Fatal("LoadFrom() error = nil")
	}
	if !strings.Contains(err.Error(), "RESERVATION_HOLD_TTL_SECONDS") {
		t.Fatalf("LoadFrom() error = %q, want variable name", err)
	}
	if strings.Contains(err.Error(), invalidValue) {
		t.Fatal("LoadFrom() error exposed the invalid environment value")
	}
}
