package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/postgres"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/postgresx"
	"github.com/google/uuid"
)

type fakePolicyPager struct {
	policies []admissiondomain.HotTrainPolicy
}

var testRegionalSession = postgresx.RegionalSession{Region: "region-a", Role: "active", Epoch: 1, WritesEnabled: true}

func (fake *fakePolicyPager) ListPoliciesPage(
	_ context.Context,
	params admissionpostgres.ListPoliciesParams,
) (admissionpostgres.PolicyPage, error) {
	if params.Offset >= int64(len(fake.policies)) {
		return admissionpostgres.PolicyPage{Total: int64(len(fake.policies))}, nil
	}
	end := min(int(params.Offset)+params.Limit, len(fake.policies))
	return admissionpostgres.PolicyPage{
		Policies: fake.policies[int(params.Offset):end],
		Total:    int64(len(fake.policies)),
	}, nil
}

type fakeAdmissionInspector struct {
	pages         []admissionredis.StateInspection
	generations   []admissionredis.PolicyScope
	calls         int
	scanCalls     int
	validateCalls int
	err           error
	validateErr   error
}

func (fake *fakeAdmissionInspector) ListLiveGenerations(
	context.Context,
	uint64,
	int,
) ([]admissionredis.PolicyScope, uint64, error) {
	fake.scanCalls++
	if fake.err != nil {
		return nil, 0, fake.err
	}
	if fake.scanCalls > 1 {
		return nil, 0, nil
	}
	return append([]admissionredis.PolicyScope(nil), fake.generations...), 0, nil
}

func (fake *fakeAdmissionInspector) InspectState(
	context.Context,
	admissionredis.PolicyScope,
	admissionredis.StateInspectionCursor,
	int,
) (admissionredis.StateInspection, error) {
	if fake.err != nil {
		return admissionredis.StateInspection{}, fake.err
	}
	index := fake.calls
	fake.calls++
	if index >= len(fake.pages) {
		return admissionredis.StateInspection{}, nil
	}
	return fake.pages[index], nil
}

func (fake *fakeAdmissionInspector) ValidateCurrentGeneration(
	context.Context,
	admissionredis.PolicyScope,
) error {
	fake.validateCalls++
	return fake.validateErr
}

func TestRunRejectsMissingCommandAndSecretFreeConfigurationError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exitCode := run(context.Background(), nil, mapLookup(map[string]string{
		"DATABASE_URL": "postgres://user:do-not-leak@localhost/railway",
	}), &stdout, &stderr)
	if exitCode != 2 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "usage:") {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout.String(), stderr.String())
	}
	if strings.Contains(stderr.String(), "do-not-leak") {
		t.Fatal("usage output leaked database credential")
	}
}

func TestRunDatabaseCommandRequiresRegionalSessionWithoutLeakingInputs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exitCode := run(context.Background(), []string{"seat-inventory"}, mapLookup(map[string]string{
		"DATABASE_URL":      "postgres://user:do-not-leak@localhost/railway",
		"DEPLOYMENT_REGION": "do-not-leak-regional-input",
	}), &stdout, &stderr)
	if exitCode != 2 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "regional database session is required") {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout.String(), stderr.String())
	}
	if strings.Contains(stderr.String(), "do-not-leak") {
		t.Fatal("configuration error leaked database or regional input")
	}
}

func TestInspectAdmissionStateAggregatesBoundedPages(t *testing.T) {
	version := int64(3)
	policies := &fakePolicyPager{policies: []admissiondomain.HotTrainPolicy{
		validPolicy(t, true, &version),
		validPolicy(t, false, nil),
	}}
	inspector := &fakeAdmissionInspector{pages: []admissionredis.StateInspection{
		{
			DuplicateActiveUsers: 1,
			NextCursor:           admissionredis.StateInspectionCursor{Entries: 9},
			Truncated:            true,
		},
		{
			InflightTokenMismatch:   2,
			ExpiredInflightTokens:   3,
			TokenEntryOwnerMismatch: 4,
		},
	}, generations: []admissionredis.PolicyScope{{
		PolicyID:   policies.policies[0].ID.String(),
		TrainRunID: policies.policies[0].TrainRunID.String(),
		SeatClass:  policies.policies[0].SeatClass.String(),
		Version:    policies.policies[0].Version,
	}}}
	result, err := inspectAdmissionState(context.Background(), policies, inspector, 10, 20)
	if err == nil || !strings.Contains(err.Error(), "10 violations") {
		t.Fatalf("error=%v, want aggregated violations", err)
	}
	if result.Policies != 2 || result.RedisPages != 2 || result.DisabledPolicies != 1 ||
		result.LiveRedisGenerations != 1 ||
		result.DuplicateActiveUsers != 1 || result.InflightTokenMismatches != 2 ||
		result.ExpiredInflightTokens != 3 || result.TokenEntryOwnerMismatches != 4 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestInspectAdmissionStateTreatsUninitializedGenerationAsViolation(t *testing.T) {
	policies := &fakePolicyPager{policies: []admissiondomain.HotTrainPolicy{
		validPolicy(t, true, nil),
	}}
	result, err := inspectAdmissionState(context.Background(), policies, &fakeAdmissionInspector{}, 10, 20)
	if err == nil || result.UninitializedPolicyGenerations != 1 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestInspectAdmissionStateTreatsMissingInitializedCurrentGenerationAsViolation(t *testing.T) {
	version := int64(3)
	policies := &fakePolicyPager{policies: []admissiondomain.HotTrainPolicy{
		validPolicy(t, true, &version),
	}}

	result, err := inspectAdmissionState(
		context.Background(),
		policies,
		&fakeAdmissionInspector{},
		10,
		20,
	)
	if err == nil || result.violations() != 1 || result.MissingCurrentPolicyGenerations != 1 {
		t.Fatalf("result=%+v error=%v, want one missing-current violation", result, err)
	}
}

func TestInspectAdmissionStateTreatsInvalidCurrentMarkerPairAsViolation(t *testing.T) {
	version := int64(3)
	policy := validPolicy(t, true, &version)
	inspector := &fakeAdmissionInspector{
		generations: []admissionredis.PolicyScope{{
			PolicyID: policy.ID.String(), TrainRunID: policy.TrainRunID.String(),
			SeatClass: policy.SeatClass.String(), Version: policy.Version,
		}},
		validateErr: admissionredis.ErrPolicyMismatch,
	}

	result, err := inspectAdmissionState(
		context.Background(),
		&fakePolicyPager{policies: []admissiondomain.HotTrainPolicy{policy}},
		inspector,
		10,
		20,
	)
	if err == nil || result.InvalidCurrentPolicyGenerations != 1 ||
		result.MissingCurrentPolicyGenerations != 0 || inspector.validateCalls != 1 {
		t.Fatalf("result=%+v validate_calls=%d error=%v, want one invalid-current violation", result, inspector.validateCalls, err)
	}
}

func TestInspectAdmissionStatePropagatesBackendFailure(t *testing.T) {
	version := int64(3)
	policies := &fakePolicyPager{policies: []admissiondomain.HotTrainPolicy{
		validPolicy(t, true, &version),
	}}
	_, err := inspectAdmissionState(context.Background(), policies, &fakeAdmissionInspector{
		err: errors.New("redis unavailable"),
	}, 10, 20)
	if err == nil {
		t.Fatal("expected backend failure")
	}
}

func TestInspectAdmissionStateIncludesDisabledAndPreviousLiveGenerations(t *testing.T) {
	currentVersion := int64(3)
	disabled := validPolicy(t, false, nil)
	enabled := validPolicy(t, true, &currentVersion)
	inspector := &fakeAdmissionInspector{generations: []admissionredis.PolicyScope{
		{
			PolicyID: disabled.ID.String(), TrainRunID: disabled.TrainRunID.String(),
			SeatClass: disabled.SeatClass.String(), Version: 2,
		},
		{
			PolicyID: enabled.ID.String(), TrainRunID: enabled.TrainRunID.String(),
			SeatClass: enabled.SeatClass.String(), Version: 2,
		},
		{
			PolicyID: enabled.ID.String(), TrainRunID: enabled.TrainRunID.String(),
			SeatClass: enabled.SeatClass.String(), Version: 3,
		},
	}}
	result, err := inspectAdmissionState(
		context.Background(),
		&fakePolicyPager{policies: []admissiondomain.HotTrainPolicy{disabled, enabled}},
		inspector,
		10,
		20,
	)
	if err != nil {
		t.Fatalf("inspectAdmissionState() error = %v", err)
	}
	if result.DisabledPolicies != 1 || result.LiveRedisGenerations != 3 ||
		result.PreviousOrDisabledGenerations != 2 ||
		result.MissingCurrentPolicyGenerations != 0 || inspector.calls != 3 {
		t.Fatalf("historical generation result = %+v calls=%d", result, inspector.calls)
	}
}

func TestEnvironmentIntFailsClosedForMalformedOrUnboundedValues(t *testing.T) {
	lookup := mapLookup(map[string]string{
		"BAD":  "not-a-number",
		"HUGE": "10001",
		"OK":   "42",
	})
	if _, err := environmentInt(lookup, "BAD", 7); err == nil {
		t.Fatal("malformed value did not fail closed")
	}
	if _, err := environmentInt(lookup, "HUGE", 7); err == nil {
		t.Fatal("unbounded value did not fail closed")
	}
	if got, err := environmentInt(lookup, "OK", 7); err != nil || got != 42 {
		t.Fatalf("OK=%d", got)
	}
	if got, err := environmentInt(lookup, "MISSING", 7); err != nil || got != 7 {
		t.Fatalf("MISSING=%d", got)
	}
}

func TestShardScopesRejectInvalidBoundsBeforeDatabaseAccess(t *testing.T) {
	tests := []struct {
		name string
		run  func() (resultEnvelope, error)
	}{
		{
			name: "assignments row limit",
			run: func() (resultEnvelope, error) {
				return runShardAssignments(context.Background(), "postgres://do-not-connect", testRegionalSession, []string{"--max-rows", "0"})
			},
		},
		{
			name: "assignments timeout upper bound",
			run: func() (resultEnvelope, error) {
				return runShardAssignments(context.Background(), "postgres://do-not-connect", testRegionalSession, []string{"--timeout", "5m1ns"})
			},
		},
		{
			name: "locators page size",
			run: func() (resultEnvelope, error) {
				return runShardLocators(context.Background(), "postgres://do-not-connect", testRegionalSession, []string{"--page-size", "1001"})
			},
		},
		{
			name: "migration page limit",
			run: func() (resultEnvelope, error) {
				return runShardMigration(context.Background(), "postgres://do-not-connect", testRegionalSession, []string{
					"--migration-id", uuid.NewString(), "--max-pages", "10001",
				})
			},
		},
		{
			name: "repair mode is not exposed",
			run: func() (resultEnvelope, error) {
				return runShardAssignments(context.Background(), "postgres://do-not-connect", testRegionalSession, []string{"--repair"})
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			envelope, err := testCase.run()
			if err == nil || envelope.Command != "" {
				t.Fatalf("envelope=%+v error=%v, want pre-connection rejection", envelope, err)
			}
		})
	}
}

func TestShardReconciliationTimeoutBounds(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		valid   bool
	}{
		{name: "default", timeout: defaultTimeout, valid: true},
		{name: "maximum", timeout: maximumShardReconciliationTimeout, valid: true},
		{name: "zero", timeout: 0, valid: false},
		{name: "negative", timeout: -time.Nanosecond, valid: false},
		{name: "above maximum", timeout: maximumShardReconciliationTimeout + time.Nanosecond, valid: false},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := validShardReconciliationTimeout(testCase.timeout); got != testCase.valid {
				t.Fatalf("validShardReconciliationTimeout(%s) = %t, want %t", testCase.timeout, got, testCase.valid)
			}
		})
	}
}

func TestShardScopesRequireCanonicalUUIDs(t *testing.T) {
	if _, err := runShardLocators(
		context.Background(),
		"postgres://do-not-connect",
		testRegionalSession,
		[]string{"--train-run-id", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"},
	); err == nil || !strings.Contains(err.Error(), "canonical UUID") {
		t.Fatalf("uppercase locator UUID error = %v", err)
	}
	if _, err := runShardMigration(
		context.Background(),
		"postgres://do-not-connect",
		testRegionalSession,
		[]string{"--migration-id", ""},
	); err == nil || !strings.Contains(err.Error(), "canonical UUID") {
		t.Fatalf("empty migration UUID error = %v", err)
	}
}

func TestUsagePreservesExistingAndAddsExactShardScopes(t *testing.T) {
	var output bytes.Buffer
	writeUsage(&output)
	usage := output.String()
	for _, scope := range []string{
		"seat-inventory", "reservation-quotas", "admission-state", "read-model", "cache-versions",
		"shard-assignments", "shard-locators", "shard-migration",
	} {
		if !strings.Contains(usage, scope) {
			t.Fatalf("usage %q is missing scope %q", usage, scope)
		}
	}
}

func validPolicy(t *testing.T, enabled bool, initialized *int64) admissiondomain.HotTrainPolicy {
	t.Helper()
	limits, err := admissiondomain.NewPolicyLimits(admissiondomain.PolicyLimitsInput{
		MaxQueueSize: 100, AdmissionRatePerSecond: 10, MaxInflightAdmissions: 20,
		AdmissionTokenTTL: 30 * time.Second,
		ProcessingLease:   10 * time.Second,
		QueueEntryTTL:     time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := testTime()
	policy, err := admissiondomain.NewHotTrainPolicy(
		uuid.New(), uuid.New(), offeringdomain.SeatClassStandard, enabled, 3, initialized, limits, now, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func testTime() (nowTime time.Time) {
	return time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
}

func mapLookup(values map[string]string) environmentLookup {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
