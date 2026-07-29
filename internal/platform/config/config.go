// Package config loads and validates the application's environment-first
// configuration.
package config

import (
	"encoding/base64"
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

// BookingShardMode selects the booking storage-routing policy.
type BookingShardMode string

const (
	// BookingShardModeLegacy preserves the pre-Milestone 4 single-schema path.
	BookingShardModeLegacy BookingShardMode = "legacy"
	// BookingShardModeSchemaPOC enables the reversible same-cluster schema POC.
	BookingShardModeSchemaPOC BookingShardMode = "schema_poc"
	// BookingShardModePhysical enables the bounded independent-PostgreSQL pilot.
	BookingShardModePhysical BookingShardMode = "physical"
)

// Process identifies the executable whose configuration contract is being
// loaded and validated.
type Process string

const (
	ProcessAPI             Process = "api"
	ProcessHoldExpirer     Process = "hold-expirer"
	ProcessOutboxWorker    Process = "outbox-worker"
	ProcessAdmissionWorker Process = "admission-worker"
	ProcessReadModelWorker Process = "read-model-worker"
)

const (
	maxReservationProtectionLimit = 10_000
	// Keep this process-boundary validation aligned with
	// admissionredis.MaxAdmissionBatch. Config deliberately does not import an
	// infrastructure adapter solely to share a constant.
	maxAdmissionWorkerBatchSize = 1_000
	maxBookingRouteCacheEntries = 100_000
	maxBookingShardQueryTimeout = 30 * time.Second
	maxPhysicalShardCount       = 2
	maxPhysicalShardPoolSize    = 100
)

// Config contains the typed runtime settings used by the application and its
// background workers. Secret values are intentionally never assigned defaults.
type Config struct {
	Environment Environment

	BookingShardMode BookingShardMode
	BookingShardIDs  []string
	// BookingShardSchemaPOCProductionEnabled is an explicit acknowledgement
	// that the same-cluster schema sharding proof of concept is being enabled
	// outside development and test environments.
	BookingShardSchemaPOCProductionEnabled bool
	PhysicalShardingProductionEnabled      bool
	// PhysicalShardConnections contains secret DSNs keyed only by fixed
	// connection references. Catalog rows never populate this map.
	PhysicalShardConnections        map[string]string
	PhysicalShardMaxCount           int
	PhysicalShardMaxOpenConns       int
	PhysicalShardMaxIdleConns       int
	PhysicalShardConnMaxLifetime    time.Duration
	PhysicalShardConnMaxIdleTime    time.Duration
	PhysicalShardConnectTimeout     time.Duration
	PhysicalShardQueryTimeout       time.Duration
	PhysicalShardTotalPoolBudget    int
	ControlDatabaseMaxOpenConns     int
	ControlDatabasePoolCount        int
	PhysicalShardAPIReplicaCount    int
	PhysicalShardWorkerReplicas     int
	WorkerShardConcurrency          int
	PhysicalShardMigrationReserve   int
	PhysicalShardOperationalReserve int
	PostgresMaxConnectionsLimit     int
	BookingRouteCacheEnabled        bool
	BookingRouteCacheTTL            time.Duration
	BookingRouteCacheMaxEntries     int
	BookingShardQueryTimeout        time.Duration

	HTTPAddress   string
	DatabaseURL   string
	RedisAddress  string
	RedisPassword string
	JWTSecret     string
	JWTIssuer     string
	JWTAudience   string
	// Admission token key material stays raw until the owning process builds
	// its keyring. It is never defaulted, formatted into errors, or loaded by
	// processes that do not issue or accept admission tokens.
	AdmissionTokenKeyring      string
	AdmissionTokenIssueKeyID   string
	AdmissionTokenAcceptKeyIDs string

	AccessTokenTTL                              time.Duration
	RefreshTokenTTL                             time.Duration
	BcryptCost                                  int
	HoldTTL                                     time.Duration
	MaxPassengersPerReservation                 int
	WorkerBatchSize                             int
	HoldExpirerEnabled                          bool
	HoldExpirerBatchSize                        int
	HoldExpirerInterval                         time.Duration
	OutboxPublisherEnabled                      bool
	OutboxPublisher                             string
	AllowLogPublisherInProduction               bool
	OutboxBatchSize                             int
	OutboxMaxAttempts                           int
	OutboxPollInterval                          time.Duration
	OutboxProcessingTimeout                     time.Duration
	OutboxRetryBase                             time.Duration
	OutboxRetryMax                              time.Duration
	WorkerHTTPAddress                           string
	WorkerPassTimeout                           time.Duration
	AdmissionWorkerEnabled                      bool
	AdmissionWorkerBatchSize                    int
	AdmissionWorkerPollInterval                 time.Duration
	ReadModelWorkerEnabled                      bool
	ReadModelWorkerBatchSize                    int
	ReadModelWorkerMaxAttempts                  int
	ReadModelWorkerPollInterval                 time.Duration
	ReadModelWorkerPendingIdle                  time.Duration
	ReadModelConsumerGroup                      string
	ReadModelConsumerName                       string
	StationCacheEnabled                         bool
	StationCacheTTL                             time.Duration
	StationCacheJitter                          time.Duration
	TrainSearchCacheEnabled                     bool
	TrainSearchFallbackEnabled                  bool
	SearchCacheTTL                              time.Duration
	SearchCacheJitter                           time.Duration
	AvailabilityCacheEnabled                    bool
	AvailabilityCacheTTL                        time.Duration
	AvailabilityCacheJitter                     time.Duration
	AvailabilityCacheMaxStale                   time.Duration
	ReservationMaxActiveHoldsPerUser            int
	ReservationMaxActiveHoldsPerUserPerTrainRun int
	ReservationMaxActivePassengersPerUser       int
	ReservationMaxInflightPerInstance           int

	HTTPReadTimeout  time.Duration
	HTTPWriteTimeout time.Duration
	ShutdownTimeout  time.Duration
	DatabaseTimeout  time.Duration
	RedisTimeout     time.Duration

	TrustedProxies     []string
	CORSAllowedOrigins []string
}

// String returns a deliberately small operational summary. Config contains
// database, Redis, JWT, and admission secrets, so its default struct formatting
// must never be used in logs or errors.
func (c Config) String() string {
	return fmt.Sprintf(
		"Config{Environment:%q BookingShardMode:%q BookingShardIDs:%q PhysicalShardConnections:[redacted]}",
		c.Environment,
		c.BookingShardMode,
		c.BookingShardIDs,
	)
}

// LookupFunc matches os.LookupEnv and makes configuration loading deterministic
// in tests.
type LookupFunc func(key string) (string, bool)

// Defaults returns development-friendly operational defaults. It does not
// invent credentials, secrets, or dependency addresses.
func Defaults() Config {
	return Config{
		Environment:                                 EnvironmentDevelopment,
		BookingShardMode:                            BookingShardModeLegacy,
		BookingShardIDs:                             []string{"legacy"},
		PhysicalShardMaxCount:                       2,
		PhysicalShardMaxOpenConns:                   8,
		PhysicalShardMaxIdleConns:                   4,
		PhysicalShardConnMaxLifetime:                30 * time.Minute,
		PhysicalShardConnMaxIdleTime:                5 * time.Minute,
		PhysicalShardConnectTimeout:                 3 * time.Second,
		PhysicalShardQueryTimeout:                   2 * time.Second,
		PhysicalShardTotalPoolBudget:                32,
		ControlDatabaseMaxOpenConns:                 16,
		ControlDatabasePoolCount:                    1,
		PhysicalShardAPIReplicaCount:                1,
		PhysicalShardWorkerReplicas:                 0,
		WorkerShardConcurrency:                      1,
		PhysicalShardMigrationReserve:               4,
		PhysicalShardOperationalReserve:             4,
		BookingRouteCacheEnabled:                    true,
		BookingRouteCacheTTL:                        30 * time.Second,
		BookingRouteCacheMaxEntries:                 1_000,
		BookingShardQueryTimeout:                    2 * time.Second,
		HTTPAddress:                                 ":8080",
		JWTIssuer:                                   "scalable-railway-ticketing-platform",
		JWTAudience:                                 "railway-api",
		AccessTokenTTL:                              15 * time.Minute,
		RefreshTokenTTL:                             7 * 24 * time.Hour,
		BcryptCost:                                  12,
		HoldTTL:                                     10 * time.Minute,
		MaxPassengersPerReservation:                 6,
		ReservationMaxActiveHoldsPerUser:            10,
		ReservationMaxActiveHoldsPerUserPerTrainRun: 3,
		ReservationMaxActivePassengersPerUser:       24,
		ReservationMaxInflightPerInstance:           32,
		WorkerBatchSize:                             100,
		HoldExpirerBatchSize:                        50,
		HoldExpirerInterval:                         30 * time.Second,
		OutboxPublisherEnabled:                      true,
		OutboxPublisher:                             "log",
		OutboxBatchSize:                             100,
		OutboxMaxAttempts:                           5,
		OutboxPollInterval:                          2 * time.Second,
		OutboxProcessingTimeout:                     60 * time.Second,
		OutboxRetryBase:                             time.Second,
		OutboxRetryMax:                              time.Minute,
		WorkerHTTPAddress:                           ":9090",
		WorkerPassTimeout:                           60 * time.Second,
		AdmissionWorkerBatchSize:                    100,
		AdmissionWorkerPollInterval:                 250 * time.Millisecond,
		ReadModelWorkerBatchSize:                    100,
		ReadModelWorkerMaxAttempts:                  5,
		ReadModelWorkerPollInterval:                 time.Second,
		ReadModelWorkerPendingIdle:                  90 * time.Second,
		ReadModelConsumerGroup:                      "railway-read-model",
		StationCacheEnabled:                         true,
		StationCacheTTL:                             5 * time.Minute,
		StationCacheJitter:                          30 * time.Second,
		TrainSearchCacheEnabled:                     true,
		TrainSearchFallbackEnabled:                  true,
		SearchCacheTTL:                              60 * time.Second,
		SearchCacheJitter:                           10 * time.Second,
		AvailabilityCacheEnabled:                    true,
		AvailabilityCacheTTL:                        10 * time.Second,
		AvailabilityCacheJitter:                     2 * time.Second,
		AvailabilityCacheMaxStale:                   10 * time.Second,
		HTTPReadTimeout:                             5 * time.Second,
		HTTPWriteTimeout:                            10 * time.Second,
		ShutdownTimeout:                             15 * time.Second,
		DatabaseTimeout:                             3 * time.Second,
		RedisTimeout:                                time.Second,
		TrustedProxies:                              []string{"127.0.0.1", "::1"},
		CORSAllowedOrigins:                          nil,
	}
}

// Validate reports invalid configuration values.
func (c Config) Validate() error {
	return c.ValidateFor(ProcessAPI)
}

// ValidateFor reports invalid values only for dependencies and settings used
// by process.
func (c Config) ValidateFor(process Process) error {
	type validationCheck struct {
		name     string
		positive bool
	}

	var problems []error
	if process != ProcessAPI && process != ProcessHoldExpirer && process != ProcessOutboxWorker && process != ProcessAdmissionWorker && process != ProcessReadModelWorker {
		problems = append(problems, errors.New("runtime process must be api, hold-expirer, outbox-worker, admission-worker, or read-model-worker"))
	}
	if c.Environment != EnvironmentDevelopment && c.Environment != EnvironmentTest && c.Environment != EnvironmentProduction {
		problems = append(problems, errors.New("APP_ENV must be development, test, or production"))
	}
	if err := validateDatabaseURL(c.DatabaseURL); err != nil {
		problems = append(problems, err)
	} else if c.Environment == EnvironmentProduction && usesCommittedDevelopmentDatabaseCredential(c.DatabaseURL) {
		problems = append(problems, errors.New("DATABASE_URL must not use the committed local development credential in production"))
	}
	validatePositive := func(values ...validationCheck) {
		for _, value := range values {
			if !value.positive {
				problems = append(problems, fmt.Errorf("%s must be positive", value.name))
			}
		}
	}
	validateWorkerHTTP := func() {
		if _, _, err := net.SplitHostPort(c.WorkerHTTPAddress); err != nil {
			problems = append(problems, errors.New("WORKER_HTTP_ADDRESS must be a host:port listen address"))
		}
	}

	validatePositive(
		validationCheck{"SHUTDOWN_TIMEOUT", c.ShutdownTimeout > 0},
		validationCheck{"DATABASE_TIMEOUT", c.DatabaseTimeout > 0},
		validationCheck{"BOOKING_ROUTE_CACHE_TTL_SECONDS", c.BookingRouteCacheTTL > 0 && c.BookingRouteCacheTTL <= 24*time.Hour},
		validationCheck{"BOOKING_ROUTE_CACHE_MAX_ENTRIES", positiveBounded(c.BookingRouteCacheMaxEntries, maxBookingRouteCacheEntries)},
		validationCheck{"BOOKING_SHARD_QUERY_TIMEOUT", c.BookingShardQueryTimeout > 0 && c.BookingShardQueryTimeout <= maxBookingShardQueryTimeout},
	)
	if err := validateBookingShardConfig(c); err != nil {
		problems = append(problems, err)
	}

	switch process {
	case ProcessAPI:
		// Admission policies enforce a minimum five-second processing lease.
		// Every reservation database command must end before Redis can make the
		// same admission available for retry.
		if c.DatabaseTimeout >= 5*time.Second {
			problems = append(problems, errors.New("DATABASE_TIMEOUT must be less than the minimum admission processing lease of 5s"))
		}
		if strings.TrimSpace(c.JWTSecret) == "" {
			problems = append(problems, errors.New("JWT_SECRET is required"))
		} else if c.Environment == EnvironmentProduction && (len(c.JWTSecret) < 32 || isCommittedDevelopmentJWTSecret(c.JWTSecret)) {
			problems = append(problems, errors.New("JWT_SECRET must be a non-development value containing at least 32 bytes in production"))
		}
		if err := validateRedisAddress(c.RedisAddress); err != nil {
			problems = append(problems, err)
		}
		if err := validateAdmissionTokenKeyring(c); err != nil {
			problems = append(problems, err)
		}
		validatePositive(
			validationCheck{"JWT_ACCESS_TTL", c.AccessTokenTTL > 0},
			validationCheck{"JWT_REFRESH_TTL", c.RefreshTokenTTL > 0},
			validationCheck{"BCRYPT_COST", c.BcryptCost >= 4 && c.BcryptCost <= 31},
			validationCheck{"RESERVATION_HOLD_TTL", c.HoldTTL > 0},
			validationCheck{"MAX_PASSENGERS_PER_RESERVATION", c.MaxPassengersPerReservation > 0},
			validationCheck{"HTTP_READ_TIMEOUT", c.HTTPReadTimeout > 0},
			validationCheck{"HTTP_WRITE_TIMEOUT", c.HTTPWriteTimeout > 0},
			validationCheck{"REDIS_TIMEOUT", c.RedisTimeout > 0},
			validationCheck{"RESERVATION_MAX_ACTIVE_HOLDS_PER_USER", positiveBounded(c.ReservationMaxActiveHoldsPerUser, maxReservationProtectionLimit)},
			validationCheck{"RESERVATION_MAX_ACTIVE_HOLDS_PER_USER_PER_TRAIN_RUN", positiveBounded(c.ReservationMaxActiveHoldsPerUserPerTrainRun, maxReservationProtectionLimit)},
			validationCheck{"RESERVATION_MAX_ACTIVE_PASSENGERS_PER_USER", positiveBounded(c.ReservationMaxActivePassengersPerUser, maxReservationProtectionLimit)},
			validationCheck{"RESERVATION_MAX_INFLIGHT_PER_INSTANCE", positiveBounded(c.ReservationMaxInflightPerInstance, maxReservationProtectionLimit)},
			validationCheck{"CACHE_STATION_TTL", c.StationCacheTTL > 0 && c.StationCacheTTL <= 24*time.Hour},
			validationCheck{"CACHE_SEARCH_TTL", c.SearchCacheTTL > 0 && c.SearchCacheTTL <= 24*time.Hour},
			validationCheck{"CACHE_AVAILABILITY_TTL", c.AvailabilityCacheTTL > 0 && c.AvailabilityCacheTTL <= 24*time.Hour},
			validationCheck{"AVAILABILITY_CACHE_MAX_STALE_SECONDS", c.AvailabilityCacheMaxStale > 0 && c.AvailabilityCacheMaxStale <= 24*time.Hour},
		)
		if c.StationCacheJitter < 0 || c.StationCacheJitter > c.StationCacheTTL ||
			c.SearchCacheJitter < 0 || c.SearchCacheJitter > c.SearchCacheTTL ||
			c.AvailabilityCacheJitter < 0 || c.AvailabilityCacheJitter > c.AvailabilityCacheTTL {
			problems = append(problems, errors.New("cache jitter must be non-negative and no greater than its TTL"))
		}
		if c.ReservationMaxActiveHoldsPerUserPerTrainRun > c.ReservationMaxActiveHoldsPerUser {
			problems = append(problems, errors.New("RESERVATION_MAX_ACTIVE_HOLDS_PER_USER_PER_TRAIN_RUN must not exceed RESERVATION_MAX_ACTIVE_HOLDS_PER_USER"))
		}
		for range invalidTrustedProxies(c.TrustedProxies) {
			problems = append(problems, errors.New("TRUSTED_PROXIES entries must be IP addresses or CIDR ranges"))
			break
		}
		if c.Environment == EnvironmentProduction {
			for _, proxy := range c.TrustedProxies {
				if isUniversalProxyRange(proxy) {
					problems = append(problems, errors.New("TRUSTED_PROXIES must not trust a universal CIDR range in production"))
					break
				}
			}
		}
		for _, origin := range c.CORSAllowedOrigins {
			if !validCORSOrigin(origin, c.Environment) {
				problems = append(problems, errors.New("CORS_ALLOWED_ORIGINS entries must be explicit HTTP or HTTPS origins"))
				break
			}
		}
	case ProcessHoldExpirer:
		validateWorkerHTTP()
		validatePositive(
			validationCheck{"HOLD_EXPIRER_BATCH_SIZE", c.HoldExpirerBatchSize > 0},
			validationCheck{"HOLD_EXPIRER_INTERVAL_SECONDS", c.HoldExpirerInterval > 0},
			validationCheck{"WORKER_PASS_TIMEOUT", c.WorkerPassTimeout > 0},
		)
	case ProcessOutboxWorker:
		validateWorkerHTTP()
		if c.OutboxPublisher != "log" && c.OutboxPublisher != "redis_stream" {
			problems = append(problems, errors.New("OUTBOX_PUBLISHER must be log or redis_stream"))
		}
		if c.OutboxPublisherEnabled && c.Environment == EnvironmentProduction && c.OutboxPublisher == "log" && !c.AllowLogPublisherInProduction {
			problems = append(problems, errors.New("OUTBOX_PUBLISHER=log is disabled in production unless ALLOW_LOG_PUBLISHER_IN_PRODUCTION is true"))
		}
		if c.OutboxPublisherEnabled && c.OutboxPublisher == "redis_stream" {
			if err := validateRedisAddress(c.RedisAddress); err != nil {
				problems = append(problems, err)
			}
			validatePositive(validationCheck{"REDIS_TIMEOUT", c.RedisTimeout > 0})
		}
		validatePositive(
			validationCheck{"OUTBOX_BATCH_SIZE", c.OutboxBatchSize > 0},
			validationCheck{"OUTBOX_MAX_ATTEMPTS", c.OutboxMaxAttempts > 0},
			validationCheck{"OUTBOX_POLL_INTERVAL_SECONDS", c.OutboxPollInterval > 0},
			validationCheck{"OUTBOX_PROCESSING_TIMEOUT_SECONDS", c.OutboxProcessingTimeout > 0},
			validationCheck{"OUTBOX_RETRY_BASE_SECONDS", c.OutboxRetryBase > 0},
			validationCheck{"OUTBOX_RETRY_MAX_SECONDS", c.OutboxRetryMax >= c.OutboxRetryBase},
			validationCheck{"WORKER_PASS_TIMEOUT", c.WorkerPassTimeout > 0},
		)
	case ProcessAdmissionWorker:
		validateWorkerHTTP()
		if err := validateRedisAddress(c.RedisAddress); err != nil {
			problems = append(problems, err)
		}
		if err := validateAdmissionTokenKeyring(c); err != nil {
			problems = append(problems, err)
		}
		validatePositive(
			validationCheck{"REDIS_TIMEOUT", c.RedisTimeout > 0},
			validationCheck{"ADMISSION_WORKER_BATCH_SIZE", positiveBounded(c.AdmissionWorkerBatchSize, maxAdmissionWorkerBatchSize)},
			validationCheck{"ADMISSION_WORKER_INTERVAL_MILLISECONDS", c.AdmissionWorkerPollInterval > 0},
			validationCheck{"WORKER_PASS_TIMEOUT", c.WorkerPassTimeout > 0},
		)
	case ProcessReadModelWorker:
		validateWorkerHTTP()
		if err := validateRedisAddress(c.RedisAddress); err != nil {
			problems = append(problems, err)
		}
		validatePositive(
			validationCheck{"REDIS_TIMEOUT", c.RedisTimeout > 0},
			validationCheck{"READ_MODEL_WORKER_BATCH_SIZE", positiveBounded(c.ReadModelWorkerBatchSize, 100)},
			validationCheck{"READ_MODEL_WORKER_MAX_ATTEMPTS", positiveBounded(c.ReadModelWorkerMaxAttempts, 10)},
			validationCheck{"READ_MODEL_WORKER_POLL_INTERVAL", c.ReadModelWorkerPollInterval > 0},
			validationCheck{"READ_MODEL_WORKER_PENDING_IDLE", c.ReadModelWorkerPendingIdle > 0},
			validationCheck{"WORKER_PASS_TIMEOUT", c.WorkerPassTimeout > 0},
		)
		if c.ReadModelWorkerPendingIdle <= c.WorkerPassTimeout {
			problems = append(problems, errors.New("READ_MODEL_CLAIM_MIN_IDLE_SECONDS must be greater than WORKER_PASS_TIMEOUT"))
		}
		if !validRuntimeName(c.ReadModelConsumerGroup, 256) {
			problems = append(problems, errors.New("READ_MODEL_CONSUMER_GROUP must contain between 1 and 256 trimmed characters"))
		}
		if c.ReadModelConsumerName != "" && !validRuntimeName(c.ReadModelConsumerName, 128) {
			problems = append(problems, errors.New("READ_MODEL_CONSUMER_NAME must be empty or contain between 1 and 128 trimmed characters"))
		}
	}
	return errors.Join(problems...)
}

func positiveBounded(value, maximum int) bool {
	return value > 0 && value <= maximum
}

func validRuntimeName(value string, maximum int) bool {
	return value == strings.TrimSpace(value) && len(value) >= 1 && len(value) <= maximum &&
		!strings.ContainsAny(value, "\x00\r\n")
}

func validateAdmissionTokenKeyring(c Config) error {
	var missing []error
	if strings.TrimSpace(c.AdmissionTokenKeyring) == "" {
		missing = append(missing, errors.New("ADMISSION_TOKEN_KEYRING is required"))
	}
	if strings.TrimSpace(c.AdmissionTokenIssueKeyID) == "" {
		missing = append(missing, errors.New("ADMISSION_TOKEN_ISSUE_KEY_ID is required"))
	}
	if strings.TrimSpace(c.AdmissionTokenAcceptKeyIDs) == "" {
		missing = append(missing, errors.New("ADMISSION_TOKEN_ACCEPT_KEY_IDS is required"))
	}
	if len(missing) > 0 {
		return errors.Join(missing...)
	}

	keys := make(map[string]struct{})
	entries := splitList(c.AdmissionTokenKeyring)
	if len(entries) == 0 || len(entries) > 8 {
		return errors.New("ADMISSION_TOKEN_KEYRING must contain between one and eight keys")
	}
	for _, entry := range entries {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || !validAdmissionKeyID(parts[0]) {
			return errors.New("ADMISSION_TOKEN_KEYRING must use key-id=base64url entries")
		}
		material, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil || len(material) != 32 {
			return errors.New("ADMISSION_TOKEN_KEYRING entries must decode to exactly 32 bytes")
		}
		if _, duplicate := keys[parts[0]]; duplicate {
			return errors.New("ADMISSION_TOKEN_KEYRING key IDs must be unique")
		}
		keys[parts[0]] = struct{}{}
	}

	issueID := strings.TrimSpace(c.AdmissionTokenIssueKeyID)
	if _, ok := keys[issueID]; !ok {
		return errors.New("ADMISSION_TOKEN_ISSUE_KEY_ID must name a configured key")
	}
	acceptIDs := splitList(c.AdmissionTokenAcceptKeyIDs)
	if len(acceptIDs) == 0 || len(acceptIDs) > len(keys) {
		return errors.New("ADMISSION_TOKEN_ACCEPT_KEY_IDS must name configured keys")
	}
	accepted := make(map[string]struct{}, len(acceptIDs))
	for _, keyID := range acceptIDs {
		if !validAdmissionKeyID(keyID) {
			return errors.New("ADMISSION_TOKEN_ACCEPT_KEY_IDS contains an invalid key ID")
		}
		if _, ok := keys[keyID]; !ok {
			return errors.New("ADMISSION_TOKEN_ACCEPT_KEY_IDS must name configured keys")
		}
		if _, duplicate := accepted[keyID]; duplicate {
			return errors.New("ADMISSION_TOKEN_ACCEPT_KEY_IDS must not contain duplicates")
		}
		accepted[keyID] = struct{}{}
	}
	if _, ok := accepted[issueID]; !ok {
		return errors.New("ADMISSION_TOKEN_ISSUE_KEY_ID must also be accepted")
	}
	return nil
}

type AdmissionTokenKeySelection struct {
	IssueKeyID string
	// AcceptKeys intentionally excludes configured key material that is not in
	// ADMISSION_TOKEN_ACCEPT_KEY_IDS. Callers must not infer acceptance from
	// mere keyring presence.
	AcceptKeys map[string][32]byte
}

// ParseAdmissionTokenKeys returns only explicitly accepted key material after
// applying the same fail-closed validation used by API and worker readiness.
func (c Config) ParseAdmissionTokenKeys() (AdmissionTokenKeySelection, error) {
	if err := validateAdmissionTokenKeyring(c); err != nil {
		return AdmissionTokenKeySelection{}, err
	}
	all := make(map[string][32]byte)
	for _, entry := range splitList(c.AdmissionTokenKeyring) {
		parts := strings.SplitN(entry, "=", 2)
		material, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil || len(material) != 32 {
			return AdmissionTokenKeySelection{}, errors.New("ADMISSION_TOKEN_KEYRING entries must decode to exactly 32 bytes")
		}
		var key [32]byte
		copy(key[:], material)
		all[parts[0]] = key
	}
	selection := AdmissionTokenKeySelection{
		IssueKeyID: strings.TrimSpace(c.AdmissionTokenIssueKeyID),
		AcceptKeys: make(map[string][32]byte),
	}
	for _, keyID := range splitList(c.AdmissionTokenAcceptKeyIDs) {
		selection.AcceptKeys[keyID] = all[keyID]
	}
	return selection, nil
}

func validAdmissionKeyID(value string) bool {
	if len(value) < 1 || len(value) > 32 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
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

func isUniversalProxyRange(value string) bool {
	_, network, err := net.ParseCIDR(value)
	if err != nil {
		return false
	}
	ones, _ := network.Mask.Size()
	if ones == 0 {
		return true
	}
	return network.Contains(net.IPv4(0, 0, 0, 0)) &&
		network.Contains(net.IPv4(255, 255, 255, 255))
}

func isCommittedDevelopmentJWTSecret(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	return strings.HasPrefix(normalized, "replace-with-") ||
		normalized == "local-development-secret-change-me-123456789"
}

func usesCommittedDevelopmentDatabaseCredential(value string) bool {
	parsed, err := url.Parse(value)
	if err != nil {
		return false
	}
	if parsed.User != nil {
		if password, ok := parsed.User.Password(); ok && password == "railway-local" {
			return true
		}
	}
	for _, password := range parsed.Query()["password"] {
		if password == "railway-local" {
			return true
		}
	}
	return false
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
		return errors.New("DATABASE_URL must be an absolute postgres or postgresql URL")
	}
	return nil
}

func validateRedisAddress(value string) error {
	host, portText, err := net.SplitHostPort(value)
	if err != nil || strings.TrimSpace(host) == "" {
		return errors.New("REDIS_ADDRESS must be a host:port address")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("REDIS_ADDRESS must use a valid TCP port")
	}
	return nil
}

// Load reads configuration from the process environment.
func Load() (Config, error) {
	return LoadFor(ProcessAPI)
}

// LoadFor reads and validates only the environment contract used by process.
func LoadFor(process Process) (Config, error) {
	return LoadFromFor(os.LookupEnv, process)
}

// LoadFrom is the deterministic API-process loader retained for callers and
// tests that do not need to select another executable.
func LoadFrom(lookup LookupFunc) (Config, error) {
	return LoadFromFor(lookup, ProcessAPI)
}

// LoadFromFor overlays process-owned environment values on Defaults. Unused
// settings are not parsed, so a malformed variable for another executable
// cannot block this process. Parse errors name variables but never their values.
func LoadFromFor(lookup LookupFunc, process Process) (Config, error) {
	if lookup == nil {
		return Config{}, errors.New("config: nil environment lookup")
	}

	cfg := Defaults()
	setString(lookup, "DATABASE_URL", &cfg.DatabaseURL)
	if value, ok := lookup("APP_ENV"); ok {
		cfg.Environment = Environment(strings.ToLower(strings.TrimSpace(value)))
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
	}{
		{"SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout},
		{"DATABASE_TIMEOUT", &cfg.DatabaseTimeout},
	} {
		if err := setDuration(lookup, item.name, item.target); err != nil {
			return Config{}, err
		}
	}
	if err := loadBookingShardSettings(lookup, &cfg); err != nil {
		return Config{}, err
	}
	var err error
	switch process {
	case ProcessAPI:
		err = loadAPISettings(lookup, &cfg)
	case ProcessHoldExpirer:
		err = loadHoldExpirerSettings(lookup, &cfg)
	case ProcessOutboxWorker:
		err = loadOutboxSettings(lookup, &cfg)
	case ProcessAdmissionWorker:
		err = loadAdmissionWorkerSettings(lookup, &cfg)
	case ProcessReadModelWorker:
		err = loadReadModelWorkerSettings(lookup, &cfg)
	default:
		err = errors.New("runtime process must be api, hold-expirer, outbox-worker, admission-worker, or read-model-worker")
	}
	if err != nil {
		return Config{}, err
	}
	if err := cfg.ValidateFor(process); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func loadBookingShardSettings(lookup LookupFunc, cfg *Config) error {
	if value, ok := lookup("BOOKING_SHARD_MODE"); ok {
		cfg.BookingShardMode = BookingShardMode(strings.ToLower(strings.TrimSpace(value)))
	}
	if value, ok := lookup("BOOKING_SHARD_IDS"); ok {
		cfg.BookingShardIDs = splitList(value)
	} else if cfg.BookingShardMode == BookingShardModeSchemaPOC {
		cfg.BookingShardIDs = []string{"legacy", "shard-0", "shard-1"}
	} else if cfg.BookingShardMode == BookingShardModePhysical {
		cfg.BookingShardIDs = []string{"physical-shard-0", "physical-shard-1"}
	}
	for _, item := range []struct {
		name   string
		target *bool
	}{
		{"BOOKING_SHARD_SCHEMA_POC_PRODUCTION_ENABLED", &cfg.BookingShardSchemaPOCProductionEnabled},
		{"PHYSICAL_SHARDING_PRODUCTION_ENABLED", &cfg.PhysicalShardingProductionEnabled},
		{"BOOKING_ROUTE_CACHE_ENABLED", &cfg.BookingRouteCacheEnabled},
	} {
		if err := setBool(lookup, item.name, item.target); err != nil {
			return err
		}
	}
	for _, item := range []struct {
		name   string
		target *int
	}{
		{"BOOKING_ROUTE_CACHE_MAX_ENTRIES", &cfg.BookingRouteCacheMaxEntries},
		{"PHYSICAL_SHARD_MAX_COUNT", &cfg.PhysicalShardMaxCount},
		{"PHYSICAL_SHARD_MAX_OPEN_CONNS", &cfg.PhysicalShardMaxOpenConns},
		{"PHYSICAL_SHARD_MAX_IDLE_CONNS", &cfg.PhysicalShardMaxIdleConns},
		{"PHYSICAL_SHARD_TOTAL_POOL_BUDGET", &cfg.PhysicalShardTotalPoolBudget},
		{"CONTROL_DATABASE_MAX_OPEN_CONNS", &cfg.ControlDatabaseMaxOpenConns},
		{"CONTROL_DATABASE_POOL_COUNT", &cfg.ControlDatabasePoolCount},
		{"PHYSICAL_SHARD_API_REPLICA_COUNT", &cfg.PhysicalShardAPIReplicaCount},
		{"PHYSICAL_SHARD_WORKER_REPLICA_COUNT", &cfg.PhysicalShardWorkerReplicas},
		{"WORKER_SHARD_CONCURRENCY", &cfg.WorkerShardConcurrency},
		{"PHYSICAL_SHARD_MIGRATION_ADMIN_RESERVE", &cfg.PhysicalShardMigrationReserve},
		{"PHYSICAL_SHARD_OPERATIONAL_RESERVE", &cfg.PhysicalShardOperationalReserve},
		{"POSTGRES_MAX_CONNECTIONS_LIMIT", &cfg.PostgresMaxConnectionsLimit},
	} {
		if err := setInt(lookup, item.name, item.target); err != nil {
			return err
		}
	}
	if err := setSeconds(lookup, "BOOKING_ROUTE_CACHE_TTL_SECONDS", &cfg.BookingRouteCacheTTL); err != nil {
		return err
	}
	if err := setDuration(lookup, "BOOKING_SHARD_QUERY_TIMEOUT", &cfg.BookingShardQueryTimeout); err != nil {
		return err
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
	}{
		{"PHYSICAL_SHARD_CONN_MAX_LIFETIME_SECONDS", &cfg.PhysicalShardConnMaxLifetime},
		{"PHYSICAL_SHARD_CONN_MAX_IDLE_TIME_SECONDS", &cfg.PhysicalShardConnMaxIdleTime},
	} {
		if err := setSeconds(lookup, item.name, item.target); err != nil {
			return err
		}
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
	}{
		{"PHYSICAL_SHARD_CONNECT_TIMEOUT", &cfg.PhysicalShardConnectTimeout},
		{"PHYSICAL_SHARD_QUERY_TIMEOUT", &cfg.PhysicalShardQueryTimeout},
	} {
		if err := setDuration(lookup, item.name, item.target); err != nil {
			return err
		}
	}
	if cfg.BookingShardMode == BookingShardModePhysical {
		cfg.PhysicalShardConnections = make(map[string]string, 2)
		for _, item := range []struct {
			ref string
			env string
		}{
			{"physical-shard-0", "BOOKING_SHARD_0_DATABASE_URL"},
			{"physical-shard-1", "BOOKING_SHARD_1_DATABASE_URL"},
		} {
			if value, ok := lookup(item.env); ok {
				cfg.PhysicalShardConnections[item.ref] = strings.TrimSpace(value)
			}
		}
	}
	return nil
}

func validateBookingShardConfig(c Config) error {
	if c.BookingShardMode != BookingShardModeLegacy && c.BookingShardMode != BookingShardModeSchemaPOC && c.BookingShardMode != BookingShardModePhysical {
		return errors.New("BOOKING_SHARD_MODE must be legacy, schema_poc, or physical")
	}
	if c.BookingShardMode == BookingShardModeSchemaPOC && c.Environment == EnvironmentProduction && !c.BookingShardSchemaPOCProductionEnabled {
		return errors.New("BOOKING_SHARD_SCHEMA_POC_PRODUCTION_ENABLED must be true when BOOKING_SHARD_MODE=schema_poc in production")
	}
	if c.BookingShardMode == BookingShardModePhysical && c.Environment == EnvironmentProduction && !c.PhysicalShardingProductionEnabled {
		return errors.New("PHYSICAL_SHARDING_PRODUCTION_ENABLED must be true when BOOKING_SHARD_MODE=physical in production")
	}

	known := map[string]struct{}{"legacy": {}, "shard-0": {}, "shard-1": {}}
	if c.BookingShardMode == BookingShardModePhysical {
		known = map[string]struct{}{"physical-shard-0": {}, "physical-shard-1": {}}
	}
	seen := make(map[string]struct{}, len(c.BookingShardIDs))
	for _, shardID := range c.BookingShardIDs {
		if _, ok := known[shardID]; !ok {
			return errors.New("BOOKING_SHARD_IDS must contain only known bounded shard IDs, not database identifiers")
		}
		if _, duplicate := seen[shardID]; duplicate {
			return errors.New("BOOKING_SHARD_IDS must not contain duplicates")
		}
		seen[shardID] = struct{}{}
	}
	if c.BookingShardMode == BookingShardModeLegacy {
		if len(c.BookingShardIDs) != 1 || c.BookingShardIDs[0] != "legacy" {
			return errors.New("BOOKING_SHARD_IDS must be legacy when BOOKING_SHARD_MODE=legacy")
		}
		return nil
	}
	if c.BookingShardMode == BookingShardModePhysical {
		if c.PhysicalShardMaxCount < 1 || c.PhysicalShardMaxCount > maxPhysicalShardCount ||
			len(c.BookingShardIDs) < 1 || len(c.BookingShardIDs) > c.PhysicalShardMaxCount {
			return errors.New("PHYSICAL_SHARD_MAX_COUNT must bound the fixed configured physical shards")
		}
		if c.PhysicalShardMaxOpenConns < 1 || c.PhysicalShardMaxOpenConns > maxPhysicalShardPoolSize ||
			c.PhysicalShardMaxIdleConns < 0 || c.PhysicalShardMaxIdleConns > c.PhysicalShardMaxOpenConns {
			return errors.New("physical shard pool limits are invalid")
		}
		if c.PhysicalShardConnMaxLifetime <= 0 || c.PhysicalShardConnMaxIdleTime <= 0 ||
			c.PhysicalShardConnectTimeout <= 0 || c.PhysicalShardConnectTimeout > maxBookingShardQueryTimeout ||
			c.PhysicalShardQueryTimeout <= 0 || c.PhysicalShardQueryTimeout > maxBookingShardQueryTimeout {
			return errors.New("physical shard connection and query timeouts must be positive and bounded")
		}
		if len(c.PhysicalShardConnections) != len(c.BookingShardIDs) {
			return errors.New("every configured physical shard requires one allowlisted connection secret")
		}
		seenDSNs := make(map[string]struct{}, len(c.BookingShardIDs))
		for _, shardID := range c.BookingShardIDs {
			dsn, ok := c.PhysicalShardConnections[shardID]
			if !ok || validateDatabaseURL(dsn) != nil {
				return errors.New("every configured physical shard requires a valid postgres connection secret")
			}
			if _, duplicate := seenDSNs[dsn]; duplicate {
				return errors.New("physical shard connection secrets must resolve to distinct databases")
			}
			seenDSNs[dsn] = struct{}{}
		}
		if c.PhysicalShardTotalPoolBudget < len(c.BookingShardIDs)*c.PhysicalShardMaxOpenConns {
			return errors.New("PHYSICAL_SHARD_TOTAL_POOL_BUDGET is smaller than the configured per-shard maximum")
		}
		budget, err := c.PhysicalShardConnectionBudget()
		if err != nil {
			return err
		}
		if c.PostgresMaxConnectionsLimit > 0 && budget > c.PostgresMaxConnectionsLimit {
			return errors.New("POSTGRES_MAX_CONNECTIONS_LIMIT is smaller than the configured deployment connection budget")
		}
		return nil
	}
	if len(c.BookingShardIDs) < 2 {
		return errors.New("BOOKING_SHARD_IDS must include legacy and at least one schema_poc target")
	}
	if _, ok := seen["legacy"]; !ok {
		return errors.New("BOOKING_SHARD_IDS must include legacy")
	}
	return nil
}

// PhysicalShardConnectionBudget returns the configured deployment-wide upper
// bound. It is a capacity guard, not an observed connection count. Control
// pools, API shard pools, bounded worker concurrency, and both reserves are
// included explicitly so a catalog row can never create unbudgeted pools.
func (c Config) PhysicalShardConnectionBudget() (int, error) {
	values := []int{
		c.ControlDatabaseMaxOpenConns, c.ControlDatabasePoolCount,
		c.PhysicalShardAPIReplicaCount, c.PhysicalShardWorkerReplicas,
		c.WorkerShardConcurrency, c.PhysicalShardMigrationReserve,
		c.PhysicalShardOperationalReserve, c.PostgresMaxConnectionsLimit,
	}
	for _, value := range values {
		if value < 0 || value > 10_000 {
			return 0, errors.New("physical shard deployment connection budget values must be bounded")
		}
	}
	if c.ControlDatabaseMaxOpenConns < 1 || c.ControlDatabasePoolCount < 1 ||
		c.PhysicalShardAPIReplicaCount < 1 || c.WorkerShardConcurrency < 1 {
		return 0, errors.New("physical shard deployment connection budget requires positive pool and concurrency values")
	}
	control := int64(c.ControlDatabaseMaxOpenConns) * int64(c.ControlDatabasePoolCount)
	api := int64(c.PhysicalShardAPIReplicaCount) * int64(len(c.BookingShardIDs)) * int64(c.PhysicalShardMaxOpenConns)
	workers := int64(c.PhysicalShardWorkerReplicas) * int64(c.WorkerShardConcurrency)
	total := control + api + workers + int64(c.PhysicalShardMigrationReserve) + int64(c.PhysicalShardOperationalReserve)
	if total <= 0 || total > int64(math.MaxInt) {
		return 0, errors.New("physical shard deployment connection budget overflows")
	}
	return int(total), nil
}

func loadAPISettings(lookup LookupFunc, cfg *Config) error {
	setString(lookup, "HTTP_ADDRESS", &cfg.HTTPAddress)
	setString(lookup, "HTTP_ADDR", &cfg.HTTPAddress)
	setString(lookup, "REDIS_ADDRESS", &cfg.RedisAddress)
	setString(lookup, "REDIS_ADDR", &cfg.RedisAddress)
	setString(lookup, "REDIS_PASSWORD", &cfg.RedisPassword)
	setString(lookup, "JWT_SECRET", &cfg.JWTSecret)
	setString(lookup, "JWT_ISSUER", &cfg.JWTIssuer)
	setString(lookup, "JWT_AUDIENCE", &cfg.JWTAudience)
	loadAdmissionTokenSettings(lookup, cfg)
	for _, item := range []struct {
		name   string
		target *bool
	}{
		{"STATION_CACHE_ENABLED", &cfg.StationCacheEnabled},
		{"TRAIN_SEARCH_CACHE_ENABLED", &cfg.TrainSearchCacheEnabled},
		{"TRAIN_SEARCH_FALLBACK_ENABLED", &cfg.TrainSearchFallbackEnabled},
		{"AVAILABILITY_CACHE_ENABLED", &cfg.AvailabilityCacheEnabled},
	} {
		if err := setBool(lookup, item.name, item.target); err != nil {
			return err
		}
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
		{"REDIS_TIMEOUT", &cfg.RedisTimeout},
		{"CACHE_STATION_TTL", &cfg.StationCacheTTL},
		{"CACHE_STATION_JITTER", &cfg.StationCacheJitter},
		{"CACHE_SEARCH_TTL", &cfg.SearchCacheTTL},
		{"CACHE_SEARCH_JITTER", &cfg.SearchCacheJitter},
		{"CACHE_AVAILABILITY_TTL", &cfg.AvailabilityCacheTTL},
		{"CACHE_AVAILABILITY_JITTER", &cfg.AvailabilityCacheJitter},
	} {
		if err := setDuration(lookup, item.name, item.target); err != nil {
			return err
		}
	}
	if err := setSeconds(lookup, "RESERVATION_HOLD_TTL_SECONDS", &cfg.HoldTTL); err != nil {
		return err
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
	}{
		{"STATION_CACHE_TTL_SECONDS", &cfg.StationCacheTTL},
		{"STATION_CACHE_JITTER_SECONDS", &cfg.StationCacheJitter},
		{"TRAIN_SEARCH_CACHE_TTL_SECONDS", &cfg.SearchCacheTTL},
		{"TRAIN_SEARCH_CACHE_JITTER_SECONDS", &cfg.SearchCacheJitter},
		{"AVAILABILITY_CACHE_TTL_SECONDS", &cfg.AvailabilityCacheTTL},
		{"AVAILABILITY_CACHE_JITTER_SECONDS", &cfg.AvailabilityCacheJitter},
		{"AVAILABILITY_CACHE_MAX_STALE_SECONDS", &cfg.AvailabilityCacheMaxStale},
	} {
		if err := setSeconds(lookup, item.name, item.target); err != nil {
			return err
		}
	}
	for _, item := range []struct {
		name   string
		target *int
	}{
		{"MAX_PASSENGERS_PER_RESERVATION", &cfg.MaxPassengersPerReservation},
		{"RESERVATION_MAX_PASSENGERS", &cfg.MaxPassengersPerReservation},
		{"BCRYPT_COST", &cfg.BcryptCost},
		{"RESERVATION_MAX_ACTIVE_HOLDS_PER_USER", &cfg.ReservationMaxActiveHoldsPerUser},
		{"RESERVATION_MAX_ACTIVE_HOLDS_PER_USER_PER_TRAIN_RUN", &cfg.ReservationMaxActiveHoldsPerUserPerTrainRun},
		{"RESERVATION_MAX_ACTIVE_PASSENGERS_PER_USER", &cfg.ReservationMaxActivePassengersPerUser},
		{"RESERVATION_MAX_INFLIGHT_PER_INSTANCE", &cfg.ReservationMaxInflightPerInstance},
	} {
		if err := setInt(lookup, item.name, item.target); err != nil {
			return err
		}
	}
	if value, ok := lookup("TRUSTED_PROXIES"); ok {
		cfg.TrustedProxies = splitList(value)
	}
	if value, ok := lookup("CORS_ALLOWED_ORIGINS"); ok {
		cfg.CORSAllowedOrigins = splitList(value)
	}
	return nil
}

func loadAdmissionWorkerSettings(lookup LookupFunc, cfg *Config) error {
	setString(lookup, "WORKER_HTTP_ADDRESS", &cfg.WorkerHTTPAddress)
	setString(lookup, "REDIS_ADDRESS", &cfg.RedisAddress)
	setString(lookup, "REDIS_ADDR", &cfg.RedisAddress)
	setString(lookup, "REDIS_PASSWORD", &cfg.RedisPassword)
	loadAdmissionTokenSettings(lookup, cfg)
	if err := setBool(lookup, "ADMISSION_WORKER_ENABLED", &cfg.AdmissionWorkerEnabled); err != nil {
		return err
	}
	if err := setInt(lookup, "ADMISSION_WORKER_BATCH_SIZE", &cfg.AdmissionWorkerBatchSize); err != nil {
		return err
	}
	if err := setDuration(lookup, "ADMISSION_WORKER_POLL_INTERVAL", &cfg.AdmissionWorkerPollInterval); err != nil {
		return err
	}
	if err := setMilliseconds(lookup, "ADMISSION_WORKER_INTERVAL_MILLISECONDS", &cfg.AdmissionWorkerPollInterval); err != nil {
		return err
	}
	if err := setDuration(lookup, "REDIS_TIMEOUT", &cfg.RedisTimeout); err != nil {
		return err
	}
	return setDuration(lookup, "WORKER_PASS_TIMEOUT", &cfg.WorkerPassTimeout)
}

func loadReadModelWorkerSettings(lookup LookupFunc, cfg *Config) error {
	setString(lookup, "WORKER_HTTP_ADDRESS", &cfg.WorkerHTTPAddress)
	setString(lookup, "REDIS_ADDRESS", &cfg.RedisAddress)
	setString(lookup, "REDIS_ADDR", &cfg.RedisAddress)
	setString(lookup, "REDIS_PASSWORD", &cfg.RedisPassword)
	setString(lookup, "READ_MODEL_CONSUMER_GROUP", &cfg.ReadModelConsumerGroup)
	setString(lookup, "READ_MODEL_CONSUMER_NAME", &cfg.ReadModelConsumerName)
	if err := setBool(lookup, "READ_MODEL_WORKER_ENABLED", &cfg.ReadModelWorkerEnabled); err != nil {
		return err
	}
	if err := setInt(lookup, "READ_MODEL_WORKER_BATCH_SIZE", &cfg.ReadModelWorkerBatchSize); err != nil {
		return err
	}
	if err := setInt(lookup, "READ_MODEL_WORKER_MAX_ATTEMPTS", &cfg.ReadModelWorkerMaxAttempts); err != nil {
		return err
	}
	if err := setInt(lookup, "READ_MODEL_MAX_ATTEMPTS", &cfg.ReadModelWorkerMaxAttempts); err != nil {
		return err
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
	}{
		{"READ_MODEL_WORKER_POLL_INTERVAL", &cfg.ReadModelWorkerPollInterval},
		{"READ_MODEL_WORKER_PENDING_IDLE", &cfg.ReadModelWorkerPendingIdle},
		{"REDIS_TIMEOUT", &cfg.RedisTimeout},
		{"WORKER_PASS_TIMEOUT", &cfg.WorkerPassTimeout},
	} {
		if err := setDuration(lookup, item.name, item.target); err != nil {
			return err
		}
	}
	// The documented Milestone 3 names are authoritative when both a legacy
	// duration alias and the exact unit-bearing setting are present.
	if err := setMilliseconds(lookup, "READ_MODEL_WORKER_INTERVAL_MILLISECONDS", &cfg.ReadModelWorkerPollInterval); err != nil {
		return err
	}
	if err := setSeconds(lookup, "READ_MODEL_CLAIM_MIN_IDLE_SECONDS", &cfg.ReadModelWorkerPendingIdle); err != nil {
		return err
	}
	return nil
}

func loadAdmissionTokenSettings(lookup LookupFunc, cfg *Config) {
	setString(lookup, "ADMISSION_TOKEN_KEYRING", &cfg.AdmissionTokenKeyring)
	setString(lookup, "ADMISSION_TOKEN_ISSUE_KEY_ID", &cfg.AdmissionTokenIssueKeyID)
	setString(lookup, "ADMISSION_TOKEN_ACCEPT_KEY_IDS", &cfg.AdmissionTokenAcceptKeyIDs)
}

func loadHoldExpirerSettings(lookup LookupFunc, cfg *Config) error {
	setString(lookup, "WORKER_HTTP_ADDRESS", &cfg.WorkerHTTPAddress)
	if err := setBool(lookup, "HOLD_EXPIRER_ENABLED", &cfg.HoldExpirerEnabled); err != nil {
		return err
	}
	if err := setInt(lookup, "HOLD_EXPIRER_BATCH_SIZE", &cfg.HoldExpirerBatchSize); err != nil {
		return err
	}
	if err := setSeconds(lookup, "HOLD_EXPIRER_INTERVAL_SECONDS", &cfg.HoldExpirerInterval); err != nil {
		return err
	}
	return setDuration(lookup, "WORKER_PASS_TIMEOUT", &cfg.WorkerPassTimeout)
}

func loadOutboxSettings(lookup LookupFunc, cfg *Config) error {
	setString(lookup, "OUTBOX_PUBLISHER", &cfg.OutboxPublisher)
	setString(lookup, "WORKER_HTTP_ADDRESS", &cfg.WorkerHTTPAddress)
	if err := setBool(lookup, "OUTBOX_PUBLISHER_ENABLED", &cfg.OutboxPublisherEnabled); err != nil {
		return err
	}
	if err := setBool(lookup, "ALLOW_LOG_PUBLISHER_IN_PRODUCTION", &cfg.AllowLogPublisherInProduction); err != nil {
		return err
	}
	for _, item := range []struct {
		name   string
		target *int
	}{
		{"OUTBOX_BATCH_SIZE", &cfg.OutboxBatchSize},
		{"OUTBOX_MAX_ATTEMPTS", &cfg.OutboxMaxAttempts},
	} {
		if err := setInt(lookup, item.name, item.target); err != nil {
			return err
		}
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
	}{
		{"OUTBOX_PROCESSING_TIMEOUT_SECONDS", &cfg.OutboxProcessingTimeout},
		{"OUTBOX_POLL_INTERVAL_SECONDS", &cfg.OutboxPollInterval},
		{"OUTBOX_RETRY_BASE_SECONDS", &cfg.OutboxRetryBase},
		{"OUTBOX_RETRY_MAX_SECONDS", &cfg.OutboxRetryMax},
	} {
		if err := setSeconds(lookup, item.name, item.target); err != nil {
			return err
		}
	}
	if err := setDuration(lookup, "WORKER_PASS_TIMEOUT", &cfg.WorkerPassTimeout); err != nil {
		return err
	}
	if cfg.OutboxPublisherEnabled && cfg.OutboxPublisher == "redis_stream" {
		setString(lookup, "REDIS_ADDRESS", &cfg.RedisAddress)
		setString(lookup, "REDIS_ADDR", &cfg.RedisAddress)
		setString(lookup, "REDIS_PASSWORD", &cfg.RedisPassword)
		if err := setDuration(lookup, "REDIS_TIMEOUT", &cfg.RedisTimeout); err != nil {
			return err
		}
	}
	return nil
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

func setMilliseconds(lookup LookupFunc, name string, target *time.Duration) error {
	value, ok := lookup(name)
	if !ok {
		return nil
	}
	milliseconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || milliseconds > math.MaxInt64/int64(time.Millisecond) ||
		milliseconds < math.MinInt64/int64(time.Millisecond) {
		return fmt.Errorf("config: %s must be an integer number of milliseconds", name)
	}
	*target = time.Duration(milliseconds) * time.Millisecond
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
