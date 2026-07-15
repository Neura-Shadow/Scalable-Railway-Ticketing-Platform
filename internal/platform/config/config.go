// Package config loads and validates the application's environment-first
// configuration.
package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Environment identifies an application runtime environment.
type Environment string

const (
	EnvironmentDevelopment Environment = "development"
	EnvironmentTest        Environment = "test"
	EnvironmentProduction  Environment = "production"
)

// Config contains the typed runtime settings used by the application and its
// background workers. Secret values are intentionally never assigned defaults.
type Config struct {
	Environment Environment

	HTTPAddress   string
	DatabaseURL   string
	RedisAddress  string
	RedisPassword string
	JWTSecret     string
	JWTIssuer     string
	JWTAudience   string

	AccessTokenTTL              time.Duration
	RefreshTokenTTL             time.Duration
	BcryptCost                  int
	HoldTTL                     time.Duration
	MaxPassengersPerReservation int
	WorkerBatchSize             int
	HoldExpirerEnabled          bool
	HoldExpirerBatchSize        int
	HoldExpirerInterval         time.Duration
	OutboxPublisher             string
	OutboxBatchSize             int
	OutboxMaxAttempts           int
	OutboxPollInterval          time.Duration
	OutboxProcessingTimeout     time.Duration
	OutboxRetryBase             time.Duration
	OutboxRetryMax              time.Duration

	HTTPReadTimeout  time.Duration
	HTTPWriteTimeout time.Duration
	ShutdownTimeout  time.Duration
	DatabaseTimeout  time.Duration
	RedisTimeout     time.Duration

	TrustedProxies     []string
	CORSAllowedOrigins []string
}

// LookupFunc matches os.LookupEnv and makes configuration loading deterministic
// in tests.
type LookupFunc func(key string) (string, bool)

// Defaults returns development-friendly operational defaults. It does not
// invent credentials, secrets, or dependency addresses.
func Defaults() Config {
	return Config{
		Environment:                 EnvironmentDevelopment,
		HTTPAddress:                 ":8080",
		JWTIssuer:                   "scalable-railway-ticketing-platform",
		JWTAudience:                 "railway-api",
		AccessTokenTTL:              15 * time.Minute,
		RefreshTokenTTL:             7 * 24 * time.Hour,
		BcryptCost:                  12,
		HoldTTL:                     10 * time.Minute,
		MaxPassengersPerReservation: 6,
		WorkerBatchSize:             100,
		HoldExpirerBatchSize:        50,
		HoldExpirerInterval:         30 * time.Second,
		OutboxPublisher:             "log",
		OutboxBatchSize:             100,
		OutboxMaxAttempts:           5,
		OutboxPollInterval:          2 * time.Second,
		OutboxProcessingTimeout:     60 * time.Second,
		OutboxRetryBase:             time.Second,
		OutboxRetryMax:              time.Minute,
		HTTPReadTimeout:             5 * time.Second,
		HTTPWriteTimeout:            10 * time.Second,
		ShutdownTimeout:             15 * time.Second,
		DatabaseTimeout:             3 * time.Second,
		RedisTimeout:                time.Second,
		TrustedProxies:              []string{"127.0.0.1", "::1"},
		CORSAllowedOrigins:          nil,
	}
}

// Validate reports invalid configuration values.
func (c Config) Validate() error {
	var problems []error
	if c.Environment != EnvironmentDevelopment && c.Environment != EnvironmentTest && c.Environment != EnvironmentProduction {
		problems = append(problems, errors.New("APP_ENV must be development, test, or production"))
	}
	if c.Environment == EnvironmentProduction {
		if len(c.JWTSecret) < 32 || strings.HasPrefix(strings.ToLower(c.JWTSecret), "replace-with-") {
			problems = append(problems, errors.New("JWT_SECRET must contain at least 32 bytes in production"))
		}
		if err := validateDatabaseURL(c.DatabaseURL); err != nil {
			problems = append(problems, err)
		}
		if err := validateRedisAddress(c.RedisAddress); err != nil {
			problems = append(problems, err)
		}
	}
	if c.OutboxPublisher != "log" && c.OutboxPublisher != "redis_stream" {
		problems = append(problems, errors.New("OUTBOX_PUBLISHER must be log or redis_stream"))
	}
	for _, value := range []struct {
		name     string
		positive bool
	}{
		{"JWT_ACCESS_TTL", c.AccessTokenTTL > 0},
		{"JWT_REFRESH_TTL", c.RefreshTokenTTL > 0},
		{"BCRYPT_COST", c.BcryptCost >= 4 && c.BcryptCost <= 31},
		{"RESERVATION_HOLD_TTL", c.HoldTTL > 0},
		{"MAX_PASSENGERS_PER_RESERVATION", c.MaxPassengersPerReservation > 0},
		{"WORKER_BATCH_SIZE", c.WorkerBatchSize > 0},
		{"HOLD_EXPIRER_BATCH_SIZE", c.HoldExpirerBatchSize > 0},
		{"HOLD_EXPIRER_INTERVAL_SECONDS", c.HoldExpirerInterval > 0},
		{"OUTBOX_BATCH_SIZE", c.OutboxBatchSize > 0},
		{"OUTBOX_MAX_ATTEMPTS", c.OutboxMaxAttempts > 0},
		{"OUTBOX_POLL_INTERVAL_SECONDS", c.OutboxPollInterval > 0},
		{"OUTBOX_PROCESSING_TIMEOUT_SECONDS", c.OutboxProcessingTimeout > 0},
		{"OUTBOX_RETRY_BASE_SECONDS", c.OutboxRetryBase > 0},
		{"OUTBOX_RETRY_MAX_SECONDS", c.OutboxRetryMax >= c.OutboxRetryBase},
		{"HTTP_READ_TIMEOUT", c.HTTPReadTimeout > 0},
		{"HTTP_WRITE_TIMEOUT", c.HTTPWriteTimeout > 0},
		{"SHUTDOWN_TIMEOUT", c.ShutdownTimeout > 0},
		{"DATABASE_TIMEOUT", c.DatabaseTimeout > 0},
		{"REDIS_TIMEOUT", c.RedisTimeout > 0},
	} {
		if !value.positive {
			problems = append(problems, fmt.Errorf("%s must be positive", value.name))
		}
	}
	for range invalidTrustedProxies(c.TrustedProxies) {
		problems = append(problems, errors.New("TRUSTED_PROXIES entries must be IP addresses or CIDR ranges"))
		break
	}
	for _, origin := range c.CORSAllowedOrigins {
		if !validCORSOrigin(origin, c.Environment) {
			problems = append(problems, errors.New("CORS_ALLOWED_ORIGINS entries must be explicit HTTP or HTTPS origins"))
			break
		}
	}
	return errors.Join(problems...)
}

func invalidTrustedProxies(values []string) []string {
	var invalid []string
	for _, value := range values {
		if net.ParseIP(value) != nil {
			continue
		}
		if _, _, err := net.ParseCIDR(value); err != nil {
			invalid = append(invalid, value)
		}
	}
	return invalid
}

func validCORSOrigin(value string, environment Environment) bool {
	if value == "*" {
		return environment != EnvironmentProduction
	}
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return false
	}
	return parsed.User == nil && (parsed.Path == "" || parsed.Path == "/") && parsed.RawQuery == "" && parsed.Fragment == ""
}

func validateDatabaseURL(value string) error {
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" {
		return errors.New("DATABASE_URL must be an absolute postgres or postgresql URL in production")
	}
	return nil
}

func validateRedisAddress(value string) error {
	host, portText, err := net.SplitHostPort(value)
	if err != nil || strings.TrimSpace(host) == "" {
		return errors.New("REDIS_ADDRESS must be a host:port address in production")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("REDIS_ADDRESS must use a valid TCP port in production")
	}
	return nil
}

// Load reads configuration from the process environment.
func Load() (Config, error) {
	return LoadFrom(os.LookupEnv)
}

// LoadFrom overlays environment values on Defaults and validates the result.
// Parse errors name the variable but never echo its value.
func LoadFrom(lookup LookupFunc) (Config, error) {
	if lookup == nil {
		return Config{}, errors.New("config: nil environment lookup")
	}

	cfg := Defaults()
	setString(lookup, "HTTP_ADDRESS", &cfg.HTTPAddress)
	setString(lookup, "HTTP_ADDR", &cfg.HTTPAddress)
	setString(lookup, "DATABASE_URL", &cfg.DatabaseURL)
	setString(lookup, "REDIS_ADDRESS", &cfg.RedisAddress)
	setString(lookup, "REDIS_ADDR", &cfg.RedisAddress)
	setString(lookup, "REDIS_PASSWORD", &cfg.RedisPassword)
	setString(lookup, "JWT_SECRET", &cfg.JWTSecret)
	setString(lookup, "JWT_ISSUER", &cfg.JWTIssuer)
	setString(lookup, "JWT_AUDIENCE", &cfg.JWTAudience)
	setString(lookup, "OUTBOX_PUBLISHER", &cfg.OutboxPublisher)

	if value, ok := lookup("APP_ENV"); ok {
		cfg.Environment = Environment(strings.ToLower(strings.TrimSpace(value)))
	}

	for _, item := range []struct {
		name   string
		target *time.Duration
	}{
		{"JWT_ACCESS_TTL", &cfg.AccessTokenTTL},
		{"JWT_REFRESH_TTL", &cfg.RefreshTokenTTL},
		{"RESERVATION_HOLD_TTL", &cfg.HoldTTL},
		{"HTTP_READ_TIMEOUT", &cfg.HTTPReadTimeout},
		{"HTTP_WRITE_TIMEOUT", &cfg.HTTPWriteTimeout},
		{"SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout},
		{"DATABASE_TIMEOUT", &cfg.DatabaseTimeout},
		{"REDIS_TIMEOUT", &cfg.RedisTimeout},
	} {
		if err := setDuration(lookup, item.name, item.target); err != nil {
			return Config{}, err
		}
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
	}{
		{"RESERVATION_HOLD_TTL_SECONDS", &cfg.HoldTTL},
		{"HOLD_EXPIRER_INTERVAL_SECONDS", &cfg.HoldExpirerInterval},
		{"OUTBOX_PROCESSING_TIMEOUT_SECONDS", &cfg.OutboxProcessingTimeout},
		{"OUTBOX_POLL_INTERVAL_SECONDS", &cfg.OutboxPollInterval},
		{"OUTBOX_RETRY_BASE_SECONDS", &cfg.OutboxRetryBase},
		{"OUTBOX_RETRY_MAX_SECONDS", &cfg.OutboxRetryMax},
	} {
		if err := setSeconds(lookup, item.name, item.target); err != nil {
			return Config{}, err
		}
	}

	for _, item := range []struct {
		name   string
		target *int
	}{
		{"MAX_PASSENGERS_PER_RESERVATION", &cfg.MaxPassengersPerReservation},
		{"WORKER_BATCH_SIZE", &cfg.WorkerBatchSize},
		{"RESERVATION_MAX_PASSENGERS", &cfg.MaxPassengersPerReservation},
		{"HOLD_EXPIRER_BATCH_SIZE", &cfg.HoldExpirerBatchSize},
		{"OUTBOX_BATCH_SIZE", &cfg.OutboxBatchSize},
		{"OUTBOX_MAX_ATTEMPTS", &cfg.OutboxMaxAttempts},
		{"BCRYPT_COST", &cfg.BcryptCost},
	} {
		if err := setInt(lookup, item.name, item.target); err != nil {
			return Config{}, err
		}
	}
	if err := setBool(lookup, "HOLD_EXPIRER_ENABLED", &cfg.HoldExpirerEnabled); err != nil {
		return Config{}, err
	}

	if value, ok := lookup("TRUSTED_PROXIES"); ok {
		cfg.TrustedProxies = splitList(value)
	}
	if value, ok := lookup("CORS_ALLOWED_ORIGINS"); ok {
		cfg.CORSAllowedOrigins = splitList(value)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func setString(lookup LookupFunc, name string, target *string) {
	if value, ok := lookup(name); ok {
		*target = strings.TrimSpace(value)
	}
}

func setDuration(lookup LookupFunc, name string, target *time.Duration) error {
	value, ok := lookup(name)
	if !ok {
		return nil
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("config: %s must be a duration", name)
	}
	*target = parsed
	return nil
}

func setSeconds(lookup LookupFunc, name string, target *time.Duration) error {
	value, ok := lookup(name)
	if !ok {
		return nil
	}
	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || seconds > math.MaxInt64/int64(time.Second) || seconds < math.MinInt64/int64(time.Second) {
		return fmt.Errorf("config: %s must be an integer number of seconds", name)
	}
	*target = time.Duration(seconds) * time.Second
	return nil
}

func setBool(lookup LookupFunc, name string, target *bool) error {
	value, ok := lookup(name)
	if !ok {
		return nil
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("config: %s must be a boolean", name)
	}
	*target = parsed
	return nil
}

func setInt(lookup LookupFunc, name string, target *int) error {
	value, ok := lookup(name)
	if !ok {
		return nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("config: %s must be an integer", name)
	}
	*target = parsed
	return nil
}

func splitList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if item := strings.TrimSpace(part); item != "" {
			result = append(result, item)
		}
	}
	return result
}
