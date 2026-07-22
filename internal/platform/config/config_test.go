package config_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
)

const testAdmissionKeyring = "current=a2tra2tra2tra2tra2tra2tra2tra2tra2tra2tra2s"

func setValidAdmissionTokenConfig(cfg *config.Config) {
	cfg.AdmissionTokenKeyring = testAdmissionKeyring
	cfg.AdmissionTokenIssueKeyID = "current"
	cfg.AdmissionTokenAcceptKeyIDs = "current"
}

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
	if cfg.ReservationMaxActiveHoldsPerUser != 10 ||
		cfg.ReservationMaxActiveHoldsPerUserPerTrainRun != 3 ||
		cfg.ReservationMaxActivePassengersPerUser != 24 ||
		cfg.ReservationMaxInflightPerInstance != 32 {
		t.Fatalf("reservation protection defaults = %+v", cfg)
	}
	if cfg.WorkerBatchSize != 100 {
		t.Fatalf("WorkerBatchSize = %d, want 100", cfg.WorkerBatchSize)
	}
	if cfg.DatabaseURL != "" || cfg.RedisAddress != "" || cfg.JWTSecret != "" {
		t.Fatal("connection settings and secrets must not have built-in defaults")
	}
}

func TestLoadFromForAPILoadsReservationProtectionAndSharedTokenKeyring(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"DATABASE_URL":                          "postgres://api@db.example/railway",
		"REDIS_ADDRESS":                         "redis.example:6379",
		"JWT_SECRET":                            "test-secret",
		"RESERVATION_MAX_ACTIVE_HOLDS_PER_USER": "7",
		"RESERVATION_MAX_ACTIVE_HOLDS_PER_USER_PER_TRAIN_RUN": "2",
		"RESERVATION_MAX_ACTIVE_PASSENGERS_PER_USER":          "15",
		"RESERVATION_MAX_INFLIGHT_PER_INSTANCE":               "11",
		"ADMISSION_WORKER_BATCH_SIZE":                         "not-an-integer",
		"ADMISSION_TOKEN_KEYRING":                             testAdmissionKeyring,
		"ADMISSION_TOKEN_ISSUE_KEY_ID":                        "current",
		"ADMISSION_TOKEN_ACCEPT_KEY_IDS":                      "current",
	}
	cfg, err := config.LoadFromFor(func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	}, config.ProcessAPI)
	if err != nil {
		t.Fatalf("LoadFromFor(api) error = %v", err)
	}
	if cfg.ReservationMaxActiveHoldsPerUser != 7 ||
		cfg.ReservationMaxActiveHoldsPerUserPerTrainRun != 2 ||
		cfg.ReservationMaxActivePassengersPerUser != 15 ||
		cfg.ReservationMaxInflightPerInstance != 11 {
		t.Fatalf("reservation protection settings = %+v", cfg)
	}
	if cfg.AdmissionTokenKeyring != testAdmissionKeyring ||
		cfg.AdmissionTokenIssueKeyID != "current" ||
		cfg.AdmissionTokenAcceptKeyIDs != "current" {
		t.Fatal("API did not load the shared admission-token keyring")
	}
}

func TestAdmissionWorkerLoadsOnlyItsOwnedDependencyAndKeySettings(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"APP_ENV":                                "production",
		"DATABASE_URL":                           "postgres://worker@db.example/railway",
		"REDIS_ADDRESS":                          "redis.example:6379",
		"ADMISSION_WORKER_ENABLED":               "true",
		"ADMISSION_WORKER_BATCH_SIZE":            "25",
		"ADMISSION_WORKER_INTERVAL_MILLISECONDS": "250",
		"ADMISSION_TOKEN_KEYRING":                testAdmissionKeyring,
		"ADMISSION_TOKEN_ISSUE_KEY_ID":           "current",
		"ADMISSION_TOKEN_ACCEPT_KEY_IDS":         "current",
		"JWT_ACCESS_TTL":                         "not-a-duration",
		"JWT_SECRET":                             "must-not-load",
	}
	cfg, err := config.LoadFromFor(func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	}, config.ProcessAdmissionWorker)
	if err != nil {
		t.Fatalf("LoadFromFor(admission-worker) error = %v", err)
	}
	if !cfg.AdmissionWorkerEnabled || cfg.AdmissionWorkerBatchSize != 25 ||
		cfg.AdmissionWorkerPollInterval != 250*time.Millisecond {
		t.Fatalf("admission-worker settings = %+v", cfg)
	}
	if cfg.AdmissionTokenKeyring != env["ADMISSION_TOKEN_KEYRING"] ||
		cfg.AdmissionTokenIssueKeyID != "current" ||
		cfg.AdmissionTokenAcceptKeyIDs != "current" {
		t.Fatal("admission-worker keyring settings not loaded")
	}
	if cfg.JWTSecret != "" {
		t.Fatal("admission worker loaded API JWT secret")
	}
}

func TestParseAdmissionTokenKeysExcludesConfiguredButUnacceptedKeys(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	cfg.AdmissionTokenKeyring = testAdmissionKeyring + ",previous=cHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHA"
	cfg.AdmissionTokenIssueKeyID = "current"
	cfg.AdmissionTokenAcceptKeyIDs = "current"
	selection, err := cfg.ParseAdmissionTokenKeys()
	if err != nil {
		t.Fatalf("ParseAdmissionTokenKeys() error = %v", err)
	}
	if selection.IssueKeyID != "current" || len(selection.AcceptKeys) != 1 {
		t.Fatalf("selection = %+v", selection)
	}
	if _, ok := selection.AcceptKeys["current"]; !ok {
		t.Fatal("current issue/accept key missing")
	}
	if _, ok := selection.AcceptKeys["previous"]; ok {
		t.Fatal("unaccepted configured key was exposed as accepted")
	}
}

func TestAdmissionTokenKeyringSupportsAPIFirstRotation(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	cfg.AdmissionTokenKeyring = testAdmissionKeyring + ",next=bm5ubm5ubm5ubm5ubm5ubm5ubm5ubm5ubm5ubm5ubm4"
	cfg.AdmissionTokenIssueKeyID = "current"
	cfg.AdmissionTokenAcceptKeyIDs = "current,next"
	if _, err := cfg.ParseAdmissionTokenKeys(); err != nil {
		t.Fatalf("API accept-first keyring error = %v", err)
	}

	cfg.AdmissionTokenIssueKeyID = "next"
	selection, err := cfg.ParseAdmissionTokenKeys()
	if err != nil {
		t.Fatalf("worker issue-key switch error = %v", err)
	}
	if selection.IssueKeyID != "next" || len(selection.AcceptKeys) != 2 {
		t.Fatalf("rotated selection = %+v", selection)
	}
}

func TestAdmissionTokenKeyringRejectsUnacceptedIssueKeyWithoutExposingMaterial(t *testing.T) {
	t.Parallel()

	sentinel := "bm5ubm5ubm5ubm5ubm5ubm5ubm5ubm5ubm5ubm5ubm4"
	cfg := config.Defaults()
	cfg.AdmissionTokenKeyring = testAdmissionKeyring + ",next=" + sentinel
	cfg.AdmissionTokenIssueKeyID = "next"
	cfg.AdmissionTokenAcceptKeyIDs = "current"
	_, err := cfg.ParseAdmissionTokenKeys()
	if err == nil || !strings.Contains(err.Error(), "ADMISSION_TOKEN_ISSUE_KEY_ID") {
		t.Fatalf("ParseAdmissionTokenKeys() error = %v", err)
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatal("keyring validation error exposed key material")
	}
}

func TestReservationProtectionLimitsMustBePositiveAndBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		field  string
		mutate func(*config.Config)
	}{
		{"holds per user", "RESERVATION_MAX_ACTIVE_HOLDS_PER_USER", func(c *config.Config) { c.ReservationMaxActiveHoldsPerUser = 0 }},
		{"holds per run", "RESERVATION_MAX_ACTIVE_HOLDS_PER_USER_PER_TRAIN_RUN", func(c *config.Config) { c.ReservationMaxActiveHoldsPerUserPerTrainRun = 0 }},
		{"passengers per user", "RESERVATION_MAX_ACTIVE_PASSENGERS_PER_USER", func(c *config.Config) { c.ReservationMaxActivePassengersPerUser = 0 }},
		{"inflight per instance", "RESERVATION_MAX_INFLIGHT_PER_INSTANCE", func(c *config.Config) { c.ReservationMaxInflightPerInstance = 0 }},
		{"holds upper bound", "RESERVATION_MAX_ACTIVE_HOLDS_PER_USER", func(c *config.Config) { c.ReservationMaxActiveHoldsPerUser = 100001 }},
		{"inflight upper bound", "RESERVATION_MAX_INFLIGHT_PER_INSTANCE", func(c *config.Config) { c.ReservationMaxInflightPerInstance = 100001 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.Defaults()
			cfg.DatabaseURL = "postgres://api@db.example/railway"
			cfg.RedisAddress = "redis.example:6379"
			cfg.JWTSecret = "test-secret"
			setValidAdmissionTokenConfig(&cfg)
			tt.mutate(&cfg)
			err := cfg.ValidateFor(config.ProcessAPI)
			if err == nil || !strings.Contains(err.Error(), tt.field) {
				t.Fatalf("ValidateFor(api) error = %v, want %s", err, tt.field)
			}
		})
	}
}

func TestAdmissionWorkerRequiresOwnedDependenciesAndKeyring(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	cfg.Environment = config.EnvironmentProduction
	err := cfg.ValidateFor(config.ProcessAdmissionWorker)
	if err == nil {
		t.Fatal("ValidateFor(admission-worker) error = nil")
	}
	for _, field := range []string{"DATABASE_URL", "REDIS_ADDRESS", "ADMISSION_TOKEN_KEYRING", "ADMISSION_TOKEN_ISSUE_KEY_ID", "ADMISSION_TOKEN_ACCEPT_KEY_IDS"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("ValidateFor(admission-worker) error %q does not name %s", err, field)
		}
	}
}

func TestAdmissionWorkerBatchMatchesRedisScriptMaximum(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.DatabaseURL = "postgres://worker@db.example/railway"
	cfg.RedisAddress = "redis.example:6379"
	setValidAdmissionTokenConfig(&cfg)

	cfg.AdmissionWorkerBatchSize = 1_000
	if err := cfg.ValidateFor(config.ProcessAdmissionWorker); err != nil {
		t.Fatalf("ValidateFor(admission-worker, batch=1000) error = %v", err)
	}
	cfg.AdmissionWorkerBatchSize = 1_001
	err := cfg.ValidateFor(config.ProcessAdmissionWorker)
	if err == nil || !strings.Contains(err.Error(), "ADMISSION_WORKER_BATCH_SIZE") {
		t.Fatalf("ValidateFor(admission-worker, batch=1001) error = %v", err)
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
	for _, field := range []string{"JWT_SECRET", "REDIS_ADDRESS", "ADMISSION_TOKEN_KEYRING", "ADMISSION_TOKEN_ISSUE_KEY_ID", "ADMISSION_TOKEN_ACCEPT_KEY_IDS"} {
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

func TestRedisOutboxValidationNeverEchoesRedisCredentials(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	cfg.Environment = config.EnvironmentProduction
	cfg.DatabaseURL = "postgres://worker@db.example/railway"
	cfg.OutboxPublisher = "redis_stream"
	cfg.RedisAddress = "sentinel-m11-redis-address-secret"
	cfg.RedisPassword = "sentinel-m11-redis-password-secret"

	err := cfg.ValidateFor(config.ProcessOutboxWorker)
	if err == nil || !strings.Contains(err.Error(), "REDIS_ADDRESS") {
		t.Fatalf("ValidateFor(outbox-worker) error = %v, want bounded Redis address error", err)
	}
	for _, forbidden := range []string{cfg.RedisAddress, cfg.RedisPassword} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("ValidateFor(outbox-worker) exposed Redis credential %q", forbidden)
		}
	}
}

func TestEveryProcessRequiresDatabaseConfiguration(t *testing.T) {
	t.Parallel()

	for _, process := range []config.Process{config.ProcessAPI, config.ProcessHoldExpirer, config.ProcessOutboxWorker, config.ProcessAdmissionWorker} {
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

func TestProductionRejectsCommittedDevelopmentCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		database string
		secret   string
		field    string
	}{
		{
			name:     "compose database password",
			database: "postgres://railway:railway-local@db.example/railway",
			secret:   strings.Repeat("s", 32),
			field:    "DATABASE_URL",
		},
		{
			name:     "compose JWT secret",
			database: "postgres://db.example/railway",
			secret:   "local-development-secret-change-me-123456789",
			field:    "JWT_SECRET",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := config.Defaults()
			cfg.Environment = config.EnvironmentProduction
			cfg.DatabaseURL = tt.database
			cfg.RedisAddress = "redis.example:6379"
			cfg.JWTSecret = tt.secret

			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.field) {
				t.Fatalf("Validate() error = %v, want error naming %s", err, tt.field)
			}
			if strings.Contains(err.Error(), "railway-local") || strings.Contains(err.Error(), tt.secret) {
				t.Fatal("validation error exposed a committed development credential")
			}
		})
	}
}

func TestProductionRejectsCommittedDevelopmentDatabaseCredentialInQuery(t *testing.T) {
	t.Parallel()

	for name, databaseURL := range map[string]string{
		"query without user info":   "postgres://db.example/railway?password=railway-local",
		"query overrides user info": "postgres://railway:strong-secret@db.example/railway?password=railway-local",
		"encoded query key":         "postgres://db.example/railway?pass%77ord=railway-local",
		"duplicate query values":    "postgres://db.example/railway?password=strong-secret&password=railway-local",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := config.Defaults()
			cfg.Environment = config.EnvironmentProduction
			cfg.DatabaseURL = databaseURL
			cfg.RedisAddress = "redis.example:6379"
			cfg.JWTSecret = strings.Repeat("s", 32)

			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
				t.Fatalf("Validate() error = %v, want DATABASE_URL error", err)
			}
			if strings.Contains(err.Error(), "railway-local") || strings.Contains(err.Error(), "strong-secret") {
				t.Fatal("validation error exposed a database credential")
			}
		})
	}
}

func TestProductionAllowsNonDevelopmentDatabaseCredentialInQuery(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	cfg.Environment = config.EnvironmentProduction
	cfg.DatabaseURL = "postgres://db.example/railway?password=strong-secret"
	cfg.RedisAddress = "redis.example:6379"
	cfg.JWTSecret = strings.Repeat("s", 32)
	setValidAdmissionTokenConfig(&cfg)

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestProductionRejectsUniversalTrustedProxyRanges(t *testing.T) {
	t.Parallel()

	for _, proxy := range []string{"0.0.0.0/0", "::/0", "::ffff:0:0/96"} {
		t.Run(proxy, func(t *testing.T) {
			t.Parallel()
			cfg := config.Defaults()
			cfg.Environment = config.EnvironmentProduction
			cfg.DatabaseURL = "postgres://db.example/railway"
			cfg.RedisAddress = "redis.example:6379"
			cfg.JWTSecret = strings.Repeat("s", 32)
			cfg.TrustedProxies = []string{proxy}

			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), "TRUSTED_PROXIES") {
				t.Fatalf("Validate() error = %v, want TRUSTED_PROXIES error", err)
			}
		})
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

func TestValidateAPIRejectsDatabaseTimeoutThatCanOutliveAdmissionLease(t *testing.T) {
	t.Parallel()

	cfg := config.Defaults()
	cfg.DatabaseTimeout = 5 * time.Second

	err := cfg.ValidateFor(config.ProcessAPI)
	if err == nil || !strings.Contains(err.Error(), "DATABASE_TIMEOUT") ||
		!strings.Contains(err.Error(), "less than") {
		t.Fatalf("ValidateFor(ProcessAPI) error = %v, want bounded DATABASE_TIMEOUT error", err)
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
		"ADMISSION_TOKEN_KEYRING":        testAdmissionKeyring,
		"ADMISSION_TOKEN_ISSUE_KEY_ID":   "current",
		"ADMISSION_TOKEN_ACCEPT_KEY_IDS": "current",
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
		"ADMISSION_TOKEN_KEYRING":           testAdmissionKeyring,
		"ADMISSION_TOKEN_ISSUE_KEY_ID":      "current",
		"ADMISSION_TOKEN_ACCEPT_KEY_IDS":    "current",
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

func TestLoadReadModelWorkerUsesOnlyProcessOwnedSettings(t *testing.T) {
	t.Parallel()
	env := map[string]string{
		"APP_ENV":                         "test",
		"DATABASE_URL":                    "postgres://worker@db.example/railway",
		"REDIS_ADDR":                      "redis.example:6379",
		"READ_MODEL_WORKER_ENABLED":       "true",
		"READ_MODEL_WORKER_BATCH_SIZE":    "40",
		"READ_MODEL_WORKER_MAX_ATTEMPTS":  "4",
		"READ_MODEL_WORKER_POLL_INTERVAL": "750ms",
		"READ_MODEL_WORKER_PENDING_IDLE":  "20s",
		"JWT_SECRET":                      "unused-invalid-secret",
		"ADMISSION_TOKEN_KEYRING":         "unused-invalid-keyring",
	}
	lookup := func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	}
	cfg, err := config.LoadFromFor(lookup, config.ProcessReadModelWorker)
	if err != nil {
		t.Fatalf("LoadFromFor(read-model-worker) error = %v", err)
	}
	if !cfg.ReadModelWorkerEnabled || cfg.ReadModelWorkerBatchSize != 40 || cfg.ReadModelWorkerMaxAttempts != 4 ||
		cfg.ReadModelWorkerPollInterval != 750*time.Millisecond || cfg.ReadModelWorkerPendingIdle != 20*time.Second {
		t.Fatalf("read-model worker settings = %+v", cfg)
	}
}

func TestValidateRejectsUnboundedReadCacheAndWorkerSettings(t *testing.T) {
	t.Parallel()
	api := config.Defaults()
	api.DatabaseURL = "postgres://api@db.example/railway"
	api.RedisAddress = "redis.example:6379"
	api.JWTSecret = "test"
	api.AdmissionTokenKeyring = "test=" + strings.Repeat("A", 43)
	api.AdmissionTokenIssueKeyID = "test"
	api.AdmissionTokenAcceptKeyIDs = "test"
	api.SearchCacheJitter = api.SearchCacheTTL + time.Second
	if err := api.ValidateFor(config.ProcessAPI); err == nil || !strings.Contains(err.Error(), "cache jitter") {
		t.Fatalf("ValidateFor(api cache bounds) error = %v", err)
	}
	worker := config.Defaults()
	worker.DatabaseURL = "postgres://worker@db.example/railway"
	worker.RedisAddress = "redis.example:6379"
	worker.ReadModelWorkerBatchSize = 101
	if err := worker.ValidateFor(config.ProcessReadModelWorker); err == nil || !strings.Contains(err.Error(), "READ_MODEL_WORKER_BATCH_SIZE") {
		t.Fatalf("ValidateFor(read-model worker bounds) error = %v", err)
	}
}
