package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	admissionpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/postgres"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	bookingpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/clock"
	querycache "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/cache"
	queryreadmodel "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/query/readmodel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	defaultTimeout      = 30 * time.Second
	defaultPageSize     = 100
	defaultMaximumPages = 10_000
)

var errReconciliationViolations = errors.New("reconciliation violations detected")

type environmentLookup func(string) (string, bool)

type resultEnvelope struct {
	Command  string `json:"command"`
	Status   string `json:"status"`
	ReadOnly bool   `json:"read_only"`
	Result   any    `json:"result,omitempty"`
}

type admissionStateResult struct {
	Policies                        int64 `json:"policies"`
	RedisPages                      int64 `json:"redis_pages"`
	DuplicateActiveUsers            int64 `json:"duplicate_active_users"`
	InflightTokenMismatches         int64 `json:"inflight_token_mismatches"`
	ExpiredInflightTokens           int64 `json:"expired_inflight_tokens"`
	ExpiredProcessingLeases         int64 `json:"expired_processing_leases"`
	TokenEntryOwnerMismatches       int64 `json:"token_entry_owner_mismatches"`
	UninitializedPolicyGenerations  int64 `json:"uninitialized_policy_generations"`
	MissingCurrentPolicyGenerations int64 `json:"missing_current_policy_generations"`
	InvalidCurrentPolicyGenerations int64 `json:"invalid_current_policy_generations"`
	DisabledPolicies                int64 `json:"disabled_policies"`
	LiveRedisGenerations            int64 `json:"live_redis_generations"`
	PreviousOrDisabledGenerations   int64 `json:"previous_or_disabled_generations"`
}

func (result admissionStateResult) violations() int64 {
	return result.DuplicateActiveUsers +
		result.InflightTokenMismatches +
		result.ExpiredInflightTokens +
		result.ExpiredProcessingLeases +
		result.TokenEntryOwnerMismatches +
		result.UninitializedPolicyGenerations +
		result.MissingCurrentPolicyGenerations +
		result.InvalidCurrentPolicyGenerations
}

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr))
}

func run(
	parent context.Context,
	args []string,
	lookup environmentLookup,
	stdout io.Writer,
	stderr io.Writer,
) int {
	if parent == nil || lookup == nil || stdout == nil || stderr == nil {
		return 2
	}
	if len(args) == 0 {
		writeUsage(stderr)
		return 2
	}

	databaseURL := strings.TrimSpace(environmentValue(lookup, "DATABASE_URL"))
	if databaseURL == "" && args[0] != "cache-versions" {
		fmt.Fprintln(stderr, "configuration invalid: DATABASE_URL is required")
		return 2
	}

	var (
		envelope resultEnvelope
		err      error
	)
	switch args[0] {
	case "seat-inventory":
		envelope, err = runSeatInventory(parent, databaseURL, args[1:])
	case "reservation-quotas":
		envelope, err = runReservationQuotas(parent, databaseURL, lookup, args[1:])
	case "admission-state":
		envelope, err = runAdmissionState(parent, databaseURL, lookup, args[1:])
	case "read-model":
		envelope, err = runReadModel(parent, databaseURL, args[1:])
	case "cache-versions":
		envelope, err = runCacheVersions(parent, lookup, args[1:])
	default:
		writeUsage(stderr)
		return 2
	}

	if err != nil {
		if envelope.Command != "" {
			envelope.Status = "failed"
			if errors.Is(err, bookingpostgres.ErrPersistenceInvariant) ||
				errors.Is(err, errReconciliationViolations) {
				envelope.Status = "violations"
			}
			if encodeErr := json.NewEncoder(stdout).Encode(envelope); encodeErr != nil {
				fmt.Fprintln(stderr, "failed to encode reconciliation result")
			}
		}
		fmt.Fprintln(stderr, publicError(err))
		return 1
	}
	if err := json.NewEncoder(stdout).Encode(envelope); err != nil {
		fmt.Fprintln(stderr, "failed to encode reconciliation result")
		return 1
	}
	return 0
}

func runReadModel(parent context.Context, databaseURL string, args []string) (resultEnvelope, error) {
	flags := flag.NewFlagSet("read-model", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	trainRunText := flags.String("train-run-id", "", "canonical train-run UUID")
	timeout := flags.Duration("timeout", defaultTimeout, "maximum reconciliation duration")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *timeout <= 0 {
		return resultEnvelope{}, errors.New("usage: reconcile read-model --train-run-id UUID [--timeout 30s]")
	}
	trainRunID, err := uuid.Parse(strings.TrimSpace(*trainRunText))
	if err != nil || trainRunID == uuid.Nil {
		return resultEnvelope{}, errors.New("train-run-id must be a canonical UUID")
	}
	ctx, cancel := context.WithTimeout(parent, *timeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return resultEnvelope{}, errors.New("postgres configuration invalid")
	}
	defer pool.Close()
	store, err := queryreadmodel.NewStore(pool, clock.RealClock{})
	if err != nil {
		return resultEnvelope{}, errors.New("read-model store initialization failed")
	}
	result, reconcileErr := store.ReconcileTrainRun(ctx, trainRunID.String())
	envelope := resultEnvelope{Command: "read-model", Status: "healthy", ReadOnly: true, Result: result}
	if reconcileErr != nil {
		return envelope, reconcileErr
	}
	if !result.Consistent {
		return envelope, fmt.Errorf("%w: read-model mismatches detected", errReconciliationViolations)
	}
	return envelope, nil
}

type cacheVersionResult struct {
	Checked int `json:"checked"`
	Missing int `json:"missing"`
	Invalid int `json:"invalid"`
}

func runCacheVersions(
	parent context.Context,
	lookup environmentLookup,
	args []string,
) (resultEnvelope, error) {
	flags := flag.NewFlagSet("cache-versions", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	trainRunText := flags.String("train-run-id", "", "optional canonical train-run UUID")
	timeout := flags.Duration("timeout", defaultTimeout, "maximum reconciliation duration")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *timeout <= 0 {
		return resultEnvelope{}, errors.New("usage: reconcile cache-versions [--train-run-id UUID] [--timeout 30s]")
	}
	redisAddress := strings.TrimSpace(firstEnvironmentValue(lookup, "REDIS_ADDRESS", "REDIS_ADDR"))
	if redisAddress == "" {
		return resultEnvelope{}, errors.New("configuration invalid: REDIS_ADDRESS is required")
	}
	keys := []string{querycache.StationVersionKey(), querycache.SearchVersionKey()}
	if strings.TrimSpace(*trainRunText) != "" {
		trainRunID, err := uuid.Parse(strings.TrimSpace(*trainRunText))
		if err != nil || trainRunID == uuid.Nil {
			return resultEnvelope{}, errors.New("train-run-id must be a canonical UUID")
		}
		key, _ := querycache.AvailabilityVersionKey(trainRunID.String())
		keys = append(keys, key)
	}
	ctx, cancel := context.WithTimeout(parent, *timeout)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: redisAddress, Password: environmentValue(lookup, "REDIS_PASSWORD")})
	defer func() { _ = client.Close() }()
	result := cacheVersionResult{Checked: len(keys)}
	for _, key := range keys {
		value, err := client.Get(ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			result.Missing++
			continue
		}
		if err != nil {
			envelope := resultEnvelope{Command: "cache-versions", Status: "failed", ReadOnly: true, Result: result}
			return envelope, errors.New("cache version lookup failed")
		}
		if !querycache.ValidVersionToken(value) {
			result.Invalid++
		}
	}
	envelope := resultEnvelope{Command: "cache-versions", Status: "healthy", ReadOnly: true, Result: result}
	if result.Missing+result.Invalid > 0 {
		return envelope, fmt.Errorf("%w: cache version mismatches detected", errReconciliationViolations)
	}
	return envelope, nil
}

func runSeatInventory(parent context.Context, databaseURL string, args []string) (resultEnvelope, error) {
	flags := flag.NewFlagSet("seat-inventory", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	trainRunText := flags.String("train-run-id", "", "canonical train-run UUID")
	timeout := flags.Duration("timeout", defaultTimeout, "maximum reconciliation duration")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *timeout <= 0 {
		return resultEnvelope{}, errors.New("usage: reconcile seat-inventory --train-run-id UUID [--timeout 30s]")
	}
	trainRunID, err := uuid.Parse(strings.TrimSpace(*trainRunText))
	if err != nil || trainRunID == uuid.Nil {
		return resultEnvelope{}, errors.New("train-run-id must be a canonical UUID")
	}
	ctx, cancel := context.WithTimeout(parent, *timeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return resultEnvelope{}, errors.New("postgres configuration invalid")
	}
	defer pool.Close()

	envelope := resultEnvelope{
		Command: "seat-inventory", Status: "healthy", ReadOnly: true,
		Result: map[string]string{"train_run_id": trainRunID.String()},
	}
	if err := bookingpostgres.New(pool).ReconcileTrainRun(ctx, trainRunID); err != nil {
		return envelope, err
	}
	return envelope, nil
}

func runReservationQuotas(
	parent context.Context,
	databaseURL string,
	lookup environmentLookup,
	args []string,
) (resultEnvelope, error) {
	defaultHolds, err := environmentInt(lookup, "RESERVATION_MAX_ACTIVE_HOLDS_PER_USER", 10)
	if err != nil {
		return resultEnvelope{}, err
	}
	defaultHoldsPerRun, err := environmentInt(
		lookup,
		"RESERVATION_MAX_ACTIVE_HOLDS_PER_USER_PER_TRAIN_RUN",
		3,
	)
	if err != nil {
		return resultEnvelope{}, err
	}
	defaultPassengers, err := environmentInt(
		lookup,
		"RESERVATION_MAX_ACTIVE_PASSENGERS_PER_USER",
		24,
	)
	if err != nil {
		return resultEnvelope{}, err
	}
	defaults := bookingpostgres.ReservationQuotaLimits{
		MaxActiveHoldsPerUser:            defaultHolds,
		MaxActiveHoldsPerUserPerTrainRun: defaultHoldsPerRun,
		MaxActivePassengersPerUser:       defaultPassengers,
	}
	flags := flag.NewFlagSet("reservation-quotas", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	holds := flags.Int("max-active-holds-per-user", defaults.MaxActiveHoldsPerUser, "active hold limit per user")
	holdsPerRun := flags.Int("max-active-holds-per-user-per-train-run", defaults.MaxActiveHoldsPerUserPerTrainRun, "active hold limit per user and train run")
	passengers := flags.Int("max-active-passengers-per-user", defaults.MaxActivePassengersPerUser, "active passenger limit per user")
	timeout := flags.Duration("timeout", defaultTimeout, "maximum reconciliation duration")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *timeout <= 0 {
		return resultEnvelope{}, errors.New("usage: reconcile reservation-quotas [bounded quota flags] [--timeout 30s]")
	}
	limits := bookingpostgres.ReservationQuotaLimits{
		MaxActiveHoldsPerUser:            *holds,
		MaxActiveHoldsPerUserPerTrainRun: *holdsPerRun,
		MaxActivePassengersPerUser:       *passengers,
	}
	ctx, cancel := context.WithTimeout(parent, *timeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return resultEnvelope{}, errors.New("postgres configuration invalid")
	}
	defer pool.Close()

	store := bookingpostgres.NewWithReservationQuotaLimits(pool, limits)
	result, reconcileErr := store.ReconcileReservationQuotas(ctx, limits)
	envelope := resultEnvelope{
		Command: "reservation-quotas", Status: "healthy", ReadOnly: true, Result: result,
	}
	return envelope, reconcileErr
}

func runAdmissionState(
	parent context.Context,
	databaseURL string,
	lookup environmentLookup,
	args []string,
) (resultEnvelope, error) {
	flags := flag.NewFlagSet("admission-state", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	pageSize := flags.Int("page-size", defaultPageSize, "bounded PostgreSQL and Redis page size")
	maximumPages := flags.Int("max-pages", defaultMaximumPages, "maximum pages before failing closed")
	timeout := flags.Duration("timeout", defaultTimeout, "maximum reconciliation duration")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 ||
		*pageSize < 1 || *pageSize > admissionredis.MaxAdmissionBatch ||
		*maximumPages < 1 || *maximumPages > defaultMaximumPages || *timeout <= 0 {
		return resultEnvelope{}, errors.New("usage: reconcile admission-state [--page-size 100] [--max-pages 10000] [--timeout 30s]")
	}
	redisAddress := strings.TrimSpace(firstEnvironmentValue(lookup, "REDIS_ADDRESS", "REDIS_ADDR"))
	if redisAddress == "" {
		return resultEnvelope{}, errors.New("configuration invalid: REDIS_ADDRESS is required")
	}

	ctx, cancel := context.WithTimeout(parent, *timeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return resultEnvelope{}, errors.New("postgres configuration invalid")
	}
	defer pool.Close()
	policyStore, err := admissionpostgres.NewStore(pool)
	if err != nil {
		return resultEnvelope{}, errors.New("admission policy store initialization failed")
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddress, Password: environmentValue(lookup, "REDIS_PASSWORD"),
	})
	defer func() { _ = redisClient.Close() }()
	control, err := admissionredis.NewStore(redisClient, "railway-admission")
	if err != nil {
		return resultEnvelope{}, errors.New("admission state store initialization failed")
	}

	result, reconcileErr := inspectAdmissionState(ctx, policyStore, control, *pageSize, *maximumPages)
	envelope := resultEnvelope{
		Command: "admission-state", Status: "healthy", ReadOnly: true, Result: result,
	}
	return envelope, reconcileErr
}

type policyPager interface {
	ListPoliciesPage(context.Context, admissionpostgres.ListPoliciesParams) (admissionpostgres.PolicyPage, error)
}

type admissionInspector interface {
	ListLiveGenerations(context.Context, uint64, int) ([]admissionredis.PolicyScope, uint64, error)
	ValidateCurrentGeneration(context.Context, admissionredis.PolicyScope) error
	InspectState(
		context.Context,
		admissionredis.PolicyScope,
		admissionredis.StateInspectionCursor,
		int,
	) (admissionredis.StateInspection, error)
}

func inspectAdmissionState(
	ctx context.Context,
	policies policyPager,
	control admissionInspector,
	pageSize int,
	maximumPages int,
) (admissionStateResult, error) {
	if ctx == nil || policies == nil || control == nil ||
		pageSize < 1 || pageSize > admissionredis.MaxAdmissionBatch ||
		maximumPages < 1 || maximumPages > defaultMaximumPages {
		return admissionStateResult{}, errors.New("invalid admission reconciliation input")
	}
	var (
		result           admissionStateResult
		policyOffset     int64
		pages            int
		current          = make(map[string]struct{})
		missingCurrent   = make(map[string]struct{})
		currentScopes    = make(map[string]admissionredis.PolicyScope)
		validatedCurrent = make(map[string]struct{})
	)
	for {
		if pages >= maximumPages {
			return result, errors.New("admission reconciliation exceeded the bounded page limit")
		}
		page, err := policies.ListPoliciesPage(ctx, admissionpostgres.ListPoliciesParams{
			Offset: policyOffset, Limit: min(pageSize, 100), Sort: admissionpostgres.PolicySortTrainRunID,
		})
		if err != nil {
			return result, err
		}
		pages++
		if len(page.Policies) == 0 {
			break
		}
		for _, policy := range page.Policies {
			result.Policies++
			if !policy.Enabled {
				result.DisabledPolicies++
				continue
			}
			identity := generationIdentity(policy.TrainRunID.String(), policy.SeatClass.String(), policy.Version)
			current[identity] = struct{}{}
			if policy.RedisInitializedVersion == nil || *policy.RedisInitializedVersion != policy.Version {
				result.UninitializedPolicyGenerations++
			} else {
				missingCurrent[identity] = struct{}{}
				currentScopes[identity] = admissionredis.PolicyScope{
					PolicyID: policy.ID.String(), TrainRunID: policy.TrainRunID.String(),
					SeatClass: policy.SeatClass.String(), Version: policy.Version,
				}
			}
		}
		policyOffset += int64(len(page.Policies))
		if policyOffset >= page.Total {
			break
		}
	}

	var generationCursor uint64
	for {
		if pages >= maximumPages {
			return result, errors.New("admission reconciliation exceeded the bounded page limit")
		}
		scopes, next, err := control.ListLiveGenerations(ctx, generationCursor, pageSize)
		if err != nil {
			return result, err
		}
		pages++
		for _, scope := range scopes {
			result.LiveRedisGenerations++
			identity := generationIdentity(scope.TrainRunID, scope.SeatClass, scope.Version)
			if _, exists := current[identity]; !exists {
				result.PreviousOrDisabledGenerations++
			} else {
				delete(missingCurrent, identity)
			}
			if currentScope, expected := currentScopes[identity]; expected {
				if _, alreadyValidated := validatedCurrent[identity]; !alreadyValidated {
					if pages >= maximumPages {
						return result, errors.New("admission reconciliation exceeded the bounded page limit")
					}
					validateErr := control.ValidateCurrentGeneration(ctx, currentScope)
					pages++
					if errors.Is(validateErr, admissionredis.ErrPolicyMismatch) ||
						errors.Is(validateErr, admissionredis.ErrContinuityLost) {
						result.InvalidCurrentPolicyGenerations++
					} else if validateErr != nil {
						return result, validateErr
					}
					validatedCurrent[identity] = struct{}{}
				}
			}
			cursor := admissionredis.StateInspectionCursor{}
			for {
				if pages >= maximumPages {
					return result, errors.New("admission reconciliation exceeded the bounded page limit")
				}
				inspection, err := control.InspectState(ctx, scope, cursor, pageSize)
				if err != nil {
					return result, err
				}
				pages++
				result.RedisPages++
				result.DuplicateActiveUsers += inspection.DuplicateActiveUsers
				result.InflightTokenMismatches += inspection.InflightTokenMismatch
				result.ExpiredInflightTokens += inspection.ExpiredInflightTokens
				result.ExpiredProcessingLeases += inspection.ExpiredProcessingLeases
				result.TokenEntryOwnerMismatches += inspection.TokenEntryOwnerMismatch
				if !inspection.Truncated {
					break
				}
				cursor = inspection.NextCursor
			}
		}
		generationCursor = next
		if generationCursor == 0 {
			break
		}
	}
	result.MissingCurrentPolicyGenerations = int64(len(missingCurrent))
	if result.violations() != 0 {
		return result, fmt.Errorf(
			"%w: admission-state reconciliation found %d violations",
			errReconciliationViolations,
			result.violations(),
		)
	}
	return result, nil
}

func generationIdentity(trainRunID, seatClass string, version int64) string {
	return trainRunID + "|" + strings.ToLower(seatClass) + "|" + strconv.FormatInt(version, 10)
}

func environmentValue(lookup environmentLookup, key string) string {
	value, _ := lookup(key)
	return strings.TrimSpace(value)
}

func firstEnvironmentValue(lookup environmentLookup, keys ...string) string {
	for _, key := range keys {
		if value := environmentValue(lookup, key); value != "" {
			return value
		}
	}
	return ""
}

func environmentInt(lookup environmentLookup, key string, fallback int) (int, error) {
	value, present := lookup(key)
	value = strings.TrimSpace(value)
	if !present || value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 || parsed > 10_000 {
		return 0, fmt.Errorf("%s must be an integer between 1 and 10000", key)
	}
	return parsed, nil
}

func publicError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "reconciliation deadline exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "reconciliation canceled"
	}
	return "reconciliation failed; inspect structured result and service logs"
}

func writeUsage(output io.Writer) {
	fmt.Fprintln(output, "usage: reconcile {seat-inventory|reservation-quotas|admission-state|read-model|cache-versions} [options]")
}
