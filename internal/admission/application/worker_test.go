package application

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/google/uuid"
)

type fakePolicySource struct {
	policies []domain.HotTrainPolicy
	marked   []uuid.UUID
	calls    []policyPageCall
	mu       sync.Mutex
}

type policyPageCall struct {
	after uuid.UUID
	limit int
}

func (f *fakePolicySource) ListEnabledPoliciesAfter(_ context.Context, after uuid.UUID, limit int) ([]domain.HotTrainPolicy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, policyPageCall{after: after, limit: limit})
	policies := make([]domain.HotTrainPolicy, 0, len(f.policies))
	for _, policy := range f.policies {
		if policy.Enabled && (after == uuid.Nil || bytes.Compare(policy.ID[:], after[:]) > 0) {
			policies = append(policies, policy)
		}
	}
	sort.Slice(policies, func(i, j int) bool {
		return bytes.Compare(policies[i].ID[:], policies[j].ID[:]) < 0
	})
	if len(policies) > limit {
		policies = policies[:limit]
	}
	return append([]domain.HotTrainPolicy(nil), policies...), nil
}
func (f *fakePolicySource) MarkRedisInitialized(_ context.Context, id uuid.UUID, version int64) (domain.HotTrainPolicy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marked = append(f.marked, id)
	for _, policy := range f.policies {
		if policy.ID == id && policy.Version == version {
			value := version
			policy.RedisInitializedVersion = &value
			return policy, nil
		}
	}
	return domain.HotTrainPolicy{}, errors.New("missing policy")
}

type fakeControlPlane struct {
	entries                  map[string][]domain.WaitingRoomEntry
	failInstall              map[string]error
	installed                []string
	maintained               []string
	issued                   []string
	issueCalls               int
	locators                 []string
	locatorCalls             int
	entryLocatorTTLs         []time.Duration
	tokenLocators            int
	tokenLocatorTTLs         []time.Duration
	deletedTokenLocators     int
	failTokenLocatorAttempts int
	now                      time.Time
	maintenance              admissionredis.MaintenanceResult
	state                    admissionredis.StateCounts
	issueAll                 bool
}

func (f *fakeControlPlane) InstallPolicy(_ context.Context, scope admissionredis.PolicyScope, _ bool, _ time.Duration) error {
	f.installed = append(f.installed, scope.PolicyID)
	return f.failInstall[scope.PolicyID]
}
func (f *fakeControlPlane) Maintain(_ context.Context, scope admissionredis.PolicyScope, _ int, _ time.Duration) (admissionredis.MaintenanceResult, error) {
	f.maintained = append(f.maintained, scope.PolicyID)
	return f.maintenance, nil
}
func (f *fakeControlPlane) PeekQueued(_ context.Context, scope admissionredis.PolicyScope, _ int) ([]domain.WaitingRoomEntry, error) {
	return append([]domain.WaitingRoomEntry(nil), f.entries[scope.PolicyID]...), nil
}
func (f *fakeControlPlane) Time(context.Context) (time.Time, error) { return f.now, nil }
func (f *fakeControlPlane) Issue(_ context.Context, request admissionredis.IssueRequest) (admissionredis.IssueResult, error) {
	f.issueCalls++
	result := admissionredis.IssueResult{}
	if f.issueAll {
		for _, candidate := range request.Candidates {
			result.IssuedEntryIDs = append(result.IssuedEntryIDs, candidate.EntryID)
			f.issued = append(f.issued, candidate.EntryID)
		}
	}
	return result, nil
}
func (f *fakeControlPlane) PutIssueLocators(
	_ context.Context,
	locators []admissionredis.IssueLocator,
	_ admissionredis.PolicyScope,
	entryTTL time.Duration,
	tokenTTL time.Duration,
) error {
	f.locatorCalls++
	for _, locator := range locators {
		f.locators = append(f.locators, locator.EntryID)
		f.entryLocatorTTLs = append(f.entryLocatorTTLs, entryTTL)
		f.tokenLocators++
		f.tokenLocatorTTLs = append(f.tokenLocatorTTLs, tokenTTL)
	}
	if f.failTokenLocatorAttempts > 0 {
		f.failTokenLocatorAttempts--
		return errors.New("token locator unavailable")
	}
	return nil
}
func (f *fakeControlPlane) DeleteTokenLocators(_ context.Context, tokenHashes [][32]byte) error {
	f.deletedTokenLocators += len(tokenHashes)
	return nil
}
func (f *fakeControlPlane) StateCounts(context.Context, admissionredis.PolicyScope) (admissionredis.StateCounts, error) {
	return f.state, nil
}

type fakeTokenIssuer struct {
	claims []domain.TokenClaims
}

func (f *fakeTokenIssuer) Issue(claims domain.TokenClaims) (domain.IssuedToken, error) {
	f.claims = append(f.claims, claims)
	raw := "opaque-" + claims.EntryID
	return domain.IssuedToken{
		Raw:    raw,
		Hash:   sha256.Sum256([]byte(raw)),
		Fields: domain.TokenDeliveryFields{Claims: claims},
	}, nil
}

func TestRunOnceMaintainsAnEmptyQueueAndClosesContinuityLatch(t *testing.T) {
	t.Parallel()
	policy := workerPolicy(t, nil)
	policies := &fakePolicySource{policies: []domain.HotTrainPolicy{policy}}
	control := &fakeControlPlane{
		entries: map[string][]domain.WaitingRoomEntry{}, failInstall: map[string]error{},
		now: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC),
		maintenance: admissionredis.MaintenanceResult{
			RecoveredLeases: 2,
			ExpiredTokens:   3,
			ExpiredEntries:  4,
		},
		state: admissionredis.StateCounts{QueueDepth: 7, InflightAdmissions: 4},
	}
	worker, err := NewWorker(policies, control, &fakeTokenIssuer{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if len(policies.marked) != 1 || len(control.maintained) != 1 || result.RecoveredLeases != 2 ||
		result.ExpiredTokens != 3 || result.ExpiredEntries != 4 || result.Issued != 0 ||
		result.QueueDepth != 7 || result.InflightAdmissions != 4 {
		t.Fatalf("RunOnce() result/state = (%+v, marked=%v, maintained=%v)", result, policies.marked, control.maintained)
	}
}

func TestRunOnceContinuesAfterOnePolicyFailsAndIssuesAnother(t *testing.T) {
	t.Parallel()
	version := int64(1)
	first := workerPolicy(t, &version)
	second := workerPolicy(t, &version)
	entry := domain.WaitingRoomEntry{
		ID: uuid.NewString(), PolicyID: second.ID.String(), PolicyVersion: second.Version,
		TrainRunID: second.TrainRunID.String(), SeatClass: second.SeatClass.String(),
		OwnerHash: sha256.Sum256([]byte("owner")), AdmissionFingerprint: sha256.Sum256([]byte("request")),
		Status: domain.EntryQueued, JoinedAt: time.Date(2026, 7, 18, 11, 59, 0, 0, time.UTC),
	}
	control := &fakeControlPlane{
		entries:     map[string][]domain.WaitingRoomEntry{second.ID.String(): {entry}},
		failInstall: map[string]error{first.ID.String(): errors.New("redis unavailable")},
		now:         time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), issueAll: true,
	}
	issuer := &fakeTokenIssuer{}
	worker, err := NewWorker(&fakePolicySource{policies: []domain.HotTrainPolicy{first, second}}, control, issuer, 10)
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.RunOnce(context.Background())
	if err == nil {
		t.Fatal("RunOnce() error = nil, want isolated first-policy failure")
	}
	if result.Failures != 1 || result.PoliciesProcessed != 2 || result.Issued != 1 ||
		len(control.issued) != 1 || len(control.locators) != 1 ||
		control.tokenLocators != 1 || len(issuer.claims) != 1 {
		t.Fatalf("RunOnce() result/state = (%+v, issued=%v, locators=%v, claims=%v)", result, control.issued, control.locators, issuer.claims)
	}
	if !issuer.claims[0].ExpiresAt.Equal(control.now.Add(second.Limits.AdmissionTokenTTL)) {
		t.Fatalf("token expiry = %v", issuer.claims[0].ExpiresAt)
	}
}

func TestRunOncePersistsBoundedLocatorsBeforeIssueAndRetriesWithoutDoubleAdmission(t *testing.T) {
	version := int64(1)
	policy := workerPolicy(t, &version)
	entry := domain.WaitingRoomEntry{
		ID: uuid.NewString(), PolicyID: policy.ID.String(), PolicyVersion: policy.Version,
		TrainRunID: policy.TrainRunID.String(), SeatClass: policy.SeatClass.String(),
		OwnerHash: sha256.Sum256([]byte("locator-owner")), AdmissionFingerprint: sha256.Sum256([]byte("locator-request")),
		Status: domain.EntryQueued, JoinedAt: time.Date(2026, 7, 18, 11, 59, 0, 0, time.UTC),
	}
	control := &fakeControlPlane{
		entries:                  map[string][]domain.WaitingRoomEntry{policy.ID.String(): {entry}},
		failInstall:              map[string]error{},
		failTokenLocatorAttempts: 1,
		now:                      time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC),
		issueAll:                 true,
	}
	worker, err := NewWorker(
		&fakePolicySource{policies: []domain.HotTrainPolicy{policy}},
		control,
		&fakeTokenIssuer{},
		10,
	)
	if err != nil {
		t.Fatal(err)
	}

	first, err := worker.RunOnce(context.Background())
	if err == nil {
		t.Fatal("first RunOnce() error = nil, want locator persistence failure")
	}
	if first.Issued != 0 || control.issueCalls != 0 || len(control.issued) != 0 {
		t.Fatalf("first RunOnce() admitted before locators were durable: result=%+v issue_calls=%d issued=%v", first, control.issueCalls, control.issued)
	}
	if len(control.entryLocatorTTLs) != 1 || control.entryLocatorTTLs[0] != policy.Limits.QueueEntryTTL+generationCleanupMargin {
		t.Fatalf("unissued entry locator TTLs = %v, want queue lifetime plus cleanup margin", control.entryLocatorTTLs)
	}
	if len(control.tokenLocatorTTLs) != 1 || control.tokenLocatorTTLs[0] != tokenLocatorTTL(policy) {
		t.Fatalf("unissued token locator TTLs = %v, want bounded token locator lifetime", control.tokenLocatorTTLs)
	}

	second, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	if second.Issued != 1 || control.issueCalls != 1 || len(control.issued) != 1 || control.issued[0] != entry.ID {
		t.Fatalf("retry admission = (%+v, issue_calls=%d, issued=%v), want one admission", second, control.issueCalls, control.issued)
	}
}

func TestRunOncePersistsOneBatchedLocatorWriteForAllCandidates(t *testing.T) {
	version := int64(1)
	policy := workerPolicy(t, &version)
	entries := make([]domain.WaitingRoomEntry, 0, 3)
	for index := 0; index < 3; index++ {
		entries = append(entries, domain.WaitingRoomEntry{
			ID: uuid.NewString(), PolicyID: policy.ID.String(), PolicyVersion: policy.Version,
			TrainRunID: policy.TrainRunID.String(), SeatClass: policy.SeatClass.String(),
			OwnerHash:            sha256.Sum256([]byte(fmt.Sprintf("batch-owner-%d", index))),
			AdmissionFingerprint: sha256.Sum256([]byte(fmt.Sprintf("batch-request-%d", index))),
			Status:               domain.EntryQueued, JoinedAt: time.Date(2026, 7, 18, 11, 59, 0, 0, time.UTC),
		})
	}
	control := &fakeControlPlane{
		entries:     map[string][]domain.WaitingRoomEntry{policy.ID.String(): entries},
		failInstall: map[string]error{},
		now:         time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC), issueAll: true,
	}
	worker, err := NewWorker(
		&fakePolicySource{policies: []domain.HotTrainPolicy{policy}},
		control,
		&fakeTokenIssuer{},
		admissionredis.MaxAdmissionBatch,
	)
	if err != nil {
		t.Fatal(err)
	}

	result, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Issued != len(entries) || control.locatorCalls != 1 {
		t.Fatalf(
			"RunOnce() issued=%d locator_calls=%d, want %d candidates in one locator batch",
			result.Issued,
			control.locatorCalls,
			len(entries),
		)
	}
}

func TestRunOnceDeletesDefinitivelyUnissuedTokenLocatorsOnRepeatedZeroCapacityPasses(t *testing.T) {
	version := int64(1)
	policy := workerPolicy(t, &version)
	entry := domain.WaitingRoomEntry{
		ID: uuid.NewString(), PolicyID: policy.ID.String(), PolicyVersion: policy.Version,
		TrainRunID: policy.TrainRunID.String(), SeatClass: policy.SeatClass.String(),
		OwnerHash:            sha256.Sum256([]byte("blocked-owner")),
		AdmissionFingerprint: sha256.Sum256([]byte("blocked-request")),
		Status:               domain.EntryQueued, JoinedAt: time.Date(2026, 7, 18, 11, 59, 0, 0, time.UTC),
	}
	control := &fakeControlPlane{
		entries:     map[string][]domain.WaitingRoomEntry{policy.ID.String(): {entry}},
		failInstall: map[string]error{},
		now:         time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC),
	}
	worker, err := NewWorker(
		&fakePolicySource{policies: []domain.HotTrainPolicy{policy}},
		control,
		&fakeTokenIssuer{},
		10,
	)
	if err != nil {
		t.Fatal(err)
	}

	const passes = 100
	for pass := 0; pass < passes; pass++ {
		result, runErr := worker.RunOnce(context.Background())
		if runErr != nil || result.Issued != 0 {
			t.Fatalf("blocked pass %d = (%+v, %v)", pass, result, runErr)
		}
	}
	if control.tokenLocators != passes || control.deletedTokenLocators != passes {
		t.Fatalf(
			"blocked locator lifecycle = created:%d deleted:%d, want %d/%d",
			control.tokenLocators,
			control.deletedTokenLocators,
			passes,
			passes,
		)
	}
}

func TestWorkerRejectsUnboundedBatch(t *testing.T) {
	t.Parallel()
	_, err := NewWorker(&fakePolicySource{}, &fakeControlPlane{}, &fakeTokenIssuer{}, admissionredis.MaxAdmissionBatch+1)
	if !errors.Is(err, ErrInvalidWorkerConfiguration) {
		t.Fatalf("NewWorker() error = %v, want %v", err, ErrInvalidWorkerConfiguration)
	}
}

func TestRunOnceUsesBoundedKeysetPagesAndRotatesAfterTail(t *testing.T) {
	t.Parallel()
	version := int64(1)
	policies := make([]domain.HotTrainPolicy, 0, MaxPoliciesPerPass+5)
	for index := 0; index < MaxPoliciesPerPass+5; index++ {
		policy := workerPolicy(t, &version)
		policy.ID = uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012x", index+1))
		policies = append(policies, policy)
	}
	source := &fakePolicySource{policies: policies}
	control := &fakeControlPlane{
		entries: map[string][]domain.WaitingRoomEntry{}, failInstall: map[string]error{},
		now: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC),
	}
	worker, err := NewWorker(source, control, &fakeTokenIssuer{}, 10)
	if err != nil {
		t.Fatal(err)
	}

	first, err := worker.RunOnce(context.Background())
	if err != nil || first.PoliciesSeen != MaxPoliciesPerPass || first.PoliciesProcessed != MaxPoliciesPerPass {
		t.Fatalf("first RunOnce() = (%+v, %v)", first, err)
	}
	second, err := worker.RunOnce(context.Background())
	if err != nil || second.PoliciesSeen != 5 || second.PoliciesProcessed != 5 {
		t.Fatalf("second RunOnce() = (%+v, %v)", second, err)
	}
	third, err := worker.RunOnce(context.Background())
	if err != nil || third.PoliciesSeen != MaxPoliciesPerPass || third.PoliciesProcessed != MaxPoliciesPerPass {
		t.Fatalf("wrapped RunOnce() = (%+v, %v)", third, err)
	}

	source.mu.Lock()
	calls := append([]policyPageCall(nil), source.calls...)
	source.mu.Unlock()
	if len(calls) != 4 || calls[0].after != uuid.Nil ||
		calls[1].after != policies[MaxPoliciesPerPass-1].ID ||
		calls[2].after != policies[len(policies)-1].ID || calls[3].after != uuid.Nil {
		t.Fatalf("keyset calls = %+v", calls)
	}
	for _, call := range calls {
		if call.limit != MaxPoliciesPerPass {
			t.Fatalf("policy page limit = %d, want %d", call.limit, MaxPoliciesPerPass)
		}
	}
}

func TestRunOnceSerializesConcurrentPasses(t *testing.T) {
	t.Parallel()
	version := int64(1)
	policy := workerPolicy(t, &version)
	source := &fakePolicySource{policies: []domain.HotTrainPolicy{policy}}
	control := &fakeControlPlane{
		entries: map[string][]domain.WaitingRoomEntry{}, failInstall: map[string]error{},
		now: time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC),
	}
	worker, err := NewWorker(source, control, &fakeTokenIssuer{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	const passes = 8
	start := make(chan struct{})
	errs := make(chan error, passes)
	var group sync.WaitGroup
	for index := 0; index < passes; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			_, runErr := worker.RunOnce(context.Background())
			errs <- runErr
		}()
	}
	close(start)
	group.Wait()
	close(errs)
	for runErr := range errs {
		if runErr != nil {
			t.Fatalf("concurrent RunOnce() error = %v", runErr)
		}
	}
	if len(control.installed) != passes {
		t.Fatalf("serialized install calls = %d, want %d", len(control.installed), passes)
	}
}

func workerPolicy(t *testing.T, initialized *int64) domain.HotTrainPolicy {
	t.Helper()
	limits, err := domain.NewPolicyLimits(domain.PolicyLimitsInput{
		MaxQueueSize: 100, AdmissionRatePerSecond: 10, MaxInflightAdmissions: 20,
		AdmissionTokenTTL: time.Minute, ProcessingLease: 10 * time.Second, QueueEntryTTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	policy, err := domain.NewHotTrainPolicy(
		uuid.New(), uuid.New(), offeringdomain.SeatClassStandard, true, 1, initialized, limits, now, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
