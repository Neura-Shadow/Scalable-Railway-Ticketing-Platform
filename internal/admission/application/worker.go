// Package application orchestrates bounded Admission use cases while leaving
// queue ordering and global limits inside the atomic Redis adapter.
package application

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	admissionredis "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/redis"
	"github.com/google/uuid"
)

var ErrInvalidWorkerConfiguration = errors.New("invalid admission worker configuration")

const (
	generationCleanupMargin = 5 * time.Minute
	// MaxPoliciesPerPass is deliberately independent of the per-policy Redis
	// admission batch. It bounds PostgreSQL reads and the amount of work one
	// RunOnce call can perform before another policy page gets a turn.
	MaxPoliciesPerPass = 100
)

type PolicySource interface {
	ListEnabledPoliciesAfter(context.Context, uuid.UUID, int) ([]domain.HotTrainPolicy, error)
	MarkRedisInitialized(context.Context, uuid.UUID, int64) (domain.HotTrainPolicy, error)
}

type ControlPlane interface {
	InstallPolicy(context.Context, admissionredis.PolicyScope, bool, time.Duration) error
	Maintain(context.Context, admissionredis.PolicyScope, int, time.Duration) (admissionredis.MaintenanceResult, error)
	PeekQueued(context.Context, admissionredis.PolicyScope, int) ([]domain.WaitingRoomEntry, error)
	Time(context.Context) (time.Time, error)
	Issue(context.Context, admissionredis.IssueRequest) (admissionredis.IssueResult, error)
	PutIssueLocators(context.Context, []admissionredis.IssueLocator, admissionredis.PolicyScope, time.Duration, time.Duration) error
	DeleteTokenLocators(context.Context, [][32]byte) error
	StateCounts(context.Context, admissionredis.PolicyScope) (admissionredis.StateCounts, error)
}

type TokenIssuer interface {
	Issue(domain.TokenClaims) (domain.IssuedToken, error)
}

type Metrics interface {
	RecordAdmissionAttempt(result, reason, seatClass string, wait time.Duration)
	RecordAdmissionExpirations(expiredTokens, expiredEntries int64, seatClass string)
}

type Worker struct {
	policies PolicySource
	control  ControlPlane
	tokens   TokenIssuer
	batch    int
	metrics  Metrics
	runMu    sync.Mutex
	cursor   uuid.UUID
}

type RunResult struct {
	PoliciesSeen       int
	PoliciesProcessed  int
	Issued             int
	RecoveredLeases    int64
	ExpiredTokens      int64
	ExpiredEntries     int64
	QueueDepth         int64
	InflightAdmissions int64
	Failures           int
}

func NewWorker(policies PolicySource, control ControlPlane, tokens TokenIssuer, batch int, metrics ...Metrics) (*Worker, error) {
	if policies == nil || control == nil || tokens == nil || batch < 1 || batch > admissionredis.MaxAdmissionBatch {
		return nil, ErrInvalidWorkerConfiguration
	}
	worker := &Worker{policies: policies, control: control, tokens: tokens, batch: batch}
	if len(metrics) > 0 {
		worker.metrics = metrics[0]
	}
	return worker, nil
}

// RunOnce performs one deterministic, bounded pass. A policy failure is
// isolated so another policy can still make progress; the joined error is for
// process-level observability and must be redacted before logging.
func (w *Worker) RunOnce(ctx context.Context) (RunResult, error) {
	if w == nil || w.policies == nil || w.control == nil || w.tokens == nil ||
		w.batch < 1 || w.batch > admissionredis.MaxAdmissionBatch {
		return RunResult{}, ErrInvalidWorkerConfiguration
	}
	// RunOnce mutates the rotating keyset cursor. Serializing passes keeps both
	// that cursor and per-policy side effects deterministic when a scheduler or
	// test invokes the same worker concurrently.
	w.runMu.Lock()
	defer w.runMu.Unlock()
	if err := ctx.Err(); err != nil {
		return RunResult{}, err
	}
	policies, err := w.nextPolicies(ctx)
	if err != nil {
		return RunResult{}, fmt.Errorf("list enabled admission policies: %w", err)
	}
	result := RunResult{PoliciesSeen: len(policies)}
	failures := make([]error, 0)
	for _, policy := range policies {
		if err := ctx.Err(); err != nil {
			failures = append(failures, err)
			break
		}
		policyResult, policyErr := w.runPolicy(ctx, policy)
		w.cursor = policy.ID
		result.PoliciesProcessed++
		result.Issued += policyResult.Issued
		result.RecoveredLeases += policyResult.RecoveredLeases
		result.ExpiredTokens += policyResult.ExpiredTokens
		result.ExpiredEntries += policyResult.ExpiredEntries
		result.QueueDepth += policyResult.QueueDepth
		result.InflightAdmissions += policyResult.InflightAdmissions
		if policyErr != nil {
			result.Failures++
			failures = append(failures, fmt.Errorf("process admission policy: %w", policyErr))
		}
	}
	return result, errors.Join(failures...)
}

func (w *Worker) nextPolicies(ctx context.Context) ([]domain.HotTrainPolicy, error) {
	policies, err := w.policies.ListEnabledPoliciesAfter(ctx, w.cursor, MaxPoliciesPerPass)
	if err != nil {
		return nil, err
	}
	if len(policies) > 0 || w.cursor == uuid.Nil {
		return policies, nil
	}

	// Reaching the tail rotates back to the beginning. The wrap is itself
	// keyset-based and bounded, so continuously enabled tail policies cannot
	// starve while OFFSET cost grows with table size.
	policies, err = w.policies.ListEnabledPoliciesAfter(ctx, uuid.Nil, MaxPoliciesPerPass)
	if err != nil {
		return nil, err
	}
	if len(policies) == 0 {
		w.cursor = uuid.Nil
	}
	return policies, nil
}

func (w *Worker) runPolicy(ctx context.Context, policy domain.HotTrainPolicy) (RunResult, error) {
	scope := scopeForPolicy(policy)
	generationTTL := policyGenerationTTL(policy)
	allowCreate := policy.RedisInitializedVersion == nil
	if err := w.control.InstallPolicy(ctx, scope, allowCreate, generationTTL); err != nil {
		w.recordFailure(policy, "continuity")
		return RunResult{}, err
	}
	if allowCreate {
		initialized, err := w.policies.MarkRedisInitialized(ctx, policy.ID, policy.Version)
		if err != nil || initialized.RedisInitializedVersion == nil ||
			*initialized.RedisInitializedVersion != policy.Version {
			w.recordFailure(policy, "continuity")
			if err == nil {
				err = errors.New("continuity marker not persisted")
			}
			return RunResult{}, err
		}
	}

	maintenance, err := w.control.Maintain(ctx, scope, w.batch, generationTTL)
	if err != nil {
		w.recordFailure(policy, "maintenance")
		return RunResult{}, err
	}
	policyResult := RunResult{
		RecoveredLeases: maintenance.RecoveredLeases,
		ExpiredTokens:   maintenance.ExpiredTokens,
		ExpiredEntries:  maintenance.ExpiredEntries,
	}
	if w.metrics != nil && (maintenance.ExpiredTokens > 0 || maintenance.ExpiredEntries > 0) {
		w.metrics.RecordAdmissionExpirations(
			maintenance.ExpiredTokens,
			maintenance.ExpiredEntries,
			policy.SeatClass.String(),
		)
	}
	entries, err := w.control.PeekQueued(ctx, scope, w.batch)
	if err != nil {
		w.recordFailure(policy, "queue_read")
		return policyResult, err
	}
	if len(entries) == 0 {
		return w.observeState(ctx, policy, scope, policyResult)
	}
	now, err := w.control.Time(ctx)
	if err != nil || now.IsZero() {
		w.recordFailure(policy, "redis_time")
		if err == nil {
			err = errors.New("redis time unavailable")
		}
		return policyResult, err
	}
	candidates := make([]admissionredis.IssueCandidate, 0, len(entries))
	entryByID := make(map[string]domain.WaitingRoomEntry, len(entries))
	tokenByEntryID := make(map[string][32]byte, len(entries))
	for _, entry := range entries {
		token, issueErr := w.tokens.Issue(domain.TokenClaims{
			PolicyID:             policy.ID.String(),
			PolicyVersion:        policy.Version,
			EntryID:              entry.ID,
			OwnerHash:            entry.OwnerHash,
			AdmissionFingerprint: entry.AdmissionFingerprint,
			IssuedAt:             now.UTC(),
			ExpiresAt:            now.UTC().Add(policy.Limits.AdmissionTokenTTL),
		})
		if issueErr != nil {
			w.recordFailure(policy, "token_generation")
			return policyResult, issueErr
		}
		candidates = append(candidates, admissionredis.IssueCandidate{EntryID: entry.ID, Token: token.Fields})
		entryByID[entry.ID] = entry
		tokenByEntryID[entry.ID] = token.Hash
	}
	// Locators are write-ahead records: every bounded candidate must be
	// discoverable before the atomic issue script can admit it. A candidate
	// that definitively loses a rate, inflight, or concurrency race has its
	// token locator removed after the successful Issue response. An ambiguous
	// Issue failure retains write-ahead locators for repair. No raw bearer is
	// stored or delivered, locator retries cannot double-admit an entry, and a
	// crash after Issue cannot strand repair.
	entryTTL := entryLocatorTTL(policy)
	tokenTTL := tokenLocatorTTL(policy)
	locators := make([]admissionredis.IssueLocator, 0, len(candidates))
	for _, candidate := range candidates {
		tokenHash, exists := tokenByEntryID[candidate.EntryID]
		if !exists {
			w.recordFailure(policy, "locator")
			return policyResult, errors.New("candidate token hash unavailable")
		}
		locators = append(locators, admissionredis.IssueLocator{
			EntryID: candidate.EntryID, TokenHash: tokenHash,
		})
	}
	if err := w.control.PutIssueLocators(ctx, locators, scope, entryTTL, tokenTTL); err != nil {
		w.recordFailure(policy, "locator")
		return policyResult, err
	}
	issued, err := w.control.Issue(ctx, admissionredis.IssueRequest{
		Scope:                  scope,
		AdmissionRatePerSecond: policy.Limits.AdmissionRatePerSecond,
		MaxInflightAdmissions:  policy.Limits.MaxInflightAdmissions,
		TokenTTL:               policy.Limits.AdmissionTokenTTL,
		GenerationTTL:          generationTTL,
		CleanupLimit:           w.batch,
		Candidates:             candidates,
	})
	if err != nil {
		w.recordFailure(policy, "issue")
		return policyResult, err
	}
	policyResult.RecoveredLeases += issued.RecoveredLeases
	policyResult.ExpiredTokens += issued.ExpiredTokens
	policyResult.ExpiredEntries += issued.ExpiredEntries
	if w.metrics != nil && (issued.ExpiredTokens > 0 || issued.ExpiredEntries > 0) {
		w.metrics.RecordAdmissionExpirations(
			issued.ExpiredTokens,
			issued.ExpiredEntries,
			policy.SeatClass.String(),
		)
	}
	issuedEntries := make(map[string]struct{}, len(issued.IssuedEntryIDs))
	for _, entryID := range issued.IssuedEntryIDs {
		entry, exists := entryByID[entryID]
		if !exists {
			w.recordFailure(policy, "locator")
			return policyResult, errors.New("issued unknown entry")
		}
		if _, duplicate := issuedEntries[entryID]; duplicate {
			w.recordFailure(policy, "issue")
			return policyResult, errors.New("duplicate issued entry")
		}
		issuedEntries[entryID] = struct{}{}
		policyResult.Issued++
		if w.metrics != nil {
			w.metrics.RecordAdmissionAttempt("success", "none", policy.SeatClass.String(), now.Sub(entry.JoinedAt))
		}
	}
	unissuedTokenHashes := make([][32]byte, 0, len(candidates)-len(issuedEntries))
	for _, candidate := range candidates {
		if _, wasIssued := issuedEntries[candidate.EntryID]; !wasIssued {
			unissuedTokenHashes = append(unissuedTokenHashes, tokenByEntryID[candidate.EntryID])
		}
	}
	if err := w.control.DeleteTokenLocators(ctx, unissuedTokenHashes); err != nil {
		w.recordFailure(policy, "locator_cleanup")
		return policyResult, err
	}
	return w.observeState(ctx, policy, scope, policyResult)
}

func (w *Worker) observeState(
	ctx context.Context,
	policy domain.HotTrainPolicy,
	scope admissionredis.PolicyScope,
	result RunResult,
) (RunResult, error) {
	counts, err := w.control.StateCounts(ctx, scope)
	if err != nil {
		w.recordFailure(policy, "state_counts")
		return result, err
	}
	result.QueueDepth = counts.QueueDepth
	result.InflightAdmissions = counts.InflightAdmissions
	return result, nil
}

func scopeForPolicy(policy domain.HotTrainPolicy) admissionredis.PolicyScope {
	return admissionredis.PolicyScope{
		PolicyID:   policy.ID.String(),
		TrainRunID: policy.TrainRunID.String(),
		SeatClass:  policy.SeatClass.String(),
		Version:    policy.Version,
	}
}

func policyGenerationTTL(policy domain.HotTrainPolicy) time.Duration {
	lifecycle := policy.Limits.AdmissionTokenTTL + policy.Limits.ProcessingLease
	if policy.Limits.QueueEntryTTL > lifecycle {
		lifecycle = policy.Limits.QueueEntryTTL
	}
	return lifecycle + generationCleanupMargin
}

func tokenLocatorTTL(policy domain.HotTrainPolicy) time.Duration {
	return policy.Limits.AdmissionTokenTTL + policy.Limits.ProcessingLease + generationCleanupMargin
}

func entryLocatorTTL(policy domain.HotTrainPolicy) time.Duration {
	return policy.Limits.QueueEntryTTL + generationCleanupMargin
}

func (w *Worker) recordFailure(policy domain.HotTrainPolicy, reason string) {
	if w.metrics != nil {
		w.metrics.RecordAdmissionAttempt("failure", reason, policy.SeatClass.String(), 0)
	}
}
