package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	admissiondomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/policylock"
	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const maxCorrelationIDRunes = 128

const maxPolicyPageSize = 100

// MutationMetadata enters only a bounded outbox payload. Authorization belongs
// to the HTTP/application boundary; the store validates that the audit fields
// are well-formed so direct callers cannot create anonymous policy changes.
type MutationMetadata struct {
	ActorID       uuid.UUID
	CorrelationID string
}

type CreatePolicyParams struct {
	TrainRunID uuid.UUID
	SeatClass  offeringdomain.SeatClass
	Limits     admissiondomain.PolicyLimits
	Metadata   MutationMetadata
}

type UpdatePolicyParams struct {
	ExpectedVersion int64
	Limits          admissiondomain.PolicyLimits
	// Enabled may be supplied to deliberately start a fresh enabled generation
	// after a soft disable. Nil preserves the current enabled state.
	Enabled  *bool
	Metadata MutationMetadata
}

type PolicySortField string

const (
	PolicySortTrainRunID PolicySortField = "train_run_id"
	PolicySortSeatClass  PolicySortField = "seat_class"
	PolicySortUpdatedAt  PolicySortField = "updated_at"
)

type ListPoliciesParams struct {
	Offset     int64
	Limit      int
	Sort       PolicySortField
	Descending bool
}

type PolicyPage struct {
	Policies []admissiondomain.HotTrainPolicy
	Total    int64
}

func (s *Store) CreatePolicy(ctx context.Context, params CreatePolicyParams) (admissiondomain.HotTrainPolicy, error) {
	if s == nil || s.pool == nil || params.TrainRunID == uuid.Nil || !params.SeatClass.IsValid() || !validMetadata(params.Metadata) {
		return admissiondomain.HotTrainPolicy{}, ErrInvalidInput
	}
	if _, err := admissiondomain.NewPolicyLimits(admissiondomain.PolicyLimitsInput(params.Limits)); err != nil {
		return admissiondomain.HotTrainPolicy{}, ErrInvalidInput
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return admissiondomain.HotTrainPolicy{}, safeError(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if err := policylock.AcquirePolicyMutation(ctx, tx, params.TrainRunID, params.SeatClass.String()); err != nil {
		return admissiondomain.HotTrainPolicy{}, safeError(err)
	}
	policy, err := insertPolicy(ctx, tx, params)
	if err != nil {
		return admissiondomain.HotTrainPolicy{}, safeError(err)
	}
	if err := appendPolicyEvent(ctx, tx, policy, "hot_train_policy.created", params.Metadata); err != nil {
		return admissiondomain.HotTrainPolicy{}, safeError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return admissiondomain.HotTrainPolicy{}, safeError(err)
	}
	return policy, nil
}

func (s *Store) GetPolicy(ctx context.Context, trainRunID uuid.UUID, seatClass offeringdomain.SeatClass) (admissiondomain.HotTrainPolicy, error) {
	if s == nil || s.pool == nil || trainRunID == uuid.Nil || !seatClass.IsValid() {
		return admissiondomain.HotTrainPolicy{}, ErrInvalidInput
	}
	return scanPolicy(s.pool.QueryRow(ctx, policySelect+`
WHERE train_run_id = $1 AND seat_class = $2`, trainRunID, seatClass.String()))
}

func (s *Store) GetPolicyByID(ctx context.Context, policyID uuid.UUID) (admissiondomain.HotTrainPolicy, error) {
	if s == nil || s.pool == nil || policyID == uuid.Nil {
		return admissiondomain.HotTrainPolicy{}, ErrInvalidInput
	}
	return scanPolicy(s.pool.QueryRow(ctx, policySelect+` WHERE id = $1`, policyID))
}

// ListEnabledPoliciesAfter returns one bounded keyset page for admission
// workers. Ordering by the immutable primary key avoids OFFSET rescans and
// gives callers a stable cursor they can rotate between passes.
func (s *Store) ListEnabledPoliciesAfter(
	ctx context.Context,
	afterPolicyID uuid.UUID,
	limit int,
) ([]admissiondomain.HotTrainPolicy, error) {
	if s == nil || s.pool == nil || limit < 1 || limit > maxPolicyPageSize {
		return nil, ErrInvalidInput
	}
	query := policySelect + ` WHERE enabled`
	args := []any{limit}
	if afterPolicyID != uuid.Nil {
		query += ` AND id > $1 ORDER BY id LIMIT $2`
		args = []any{afterPolicyID, limit}
	} else {
		query += ` ORDER BY id LIMIT $1`
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, safeError(err)
	}
	defer rows.Close()
	policies := make([]admissiondomain.HotTrainPolicy, 0, limit)
	for rows.Next() {
		policy, err := scanPolicy(rows)
		if err != nil {
			return nil, err
		}
		policies = append(policies, policy)
	}
	if err := rows.Err(); err != nil {
		return nil, safeError(err)
	}
	return policies, nil
}

// ListPoliciesPage performs both the count and bounded row read in one
// read-only repeatable-read transaction. The ORDER BY clause is selected from
// a closed enum; request text is never interpolated into SQL.
func (s *Store) ListPoliciesPage(ctx context.Context, params ListPoliciesParams) (PolicyPage, error) {
	orderBy, ok := policyOrderBy(params.Sort, params.Descending)
	if s == nil || s.pool == nil || params.Offset < 0 || params.Limit < 1 || params.Limit > maxPolicyPageSize || !ok {
		return PolicyPage{}, ErrInvalidInput
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return PolicyPage{}, safeError(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var total int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM hot_train_policies`).Scan(&total); err != nil {
		return PolicyPage{}, safeError(err)
	}
	rows, err := tx.Query(ctx, policySelect+orderBy+` OFFSET $1 LIMIT $2`, params.Offset, params.Limit)
	if err != nil {
		return PolicyPage{}, safeError(err)
	}
	defer rows.Close()
	policies := make([]admissiondomain.HotTrainPolicy, 0, params.Limit)
	for rows.Next() {
		policy, err := scanPolicy(rows)
		if err != nil {
			return PolicyPage{}, err
		}
		policies = append(policies, policy)
	}
	if err := rows.Err(); err != nil {
		return PolicyPage{}, safeError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return PolicyPage{}, safeError(err)
	}
	return PolicyPage{Policies: policies, Total: total}, nil
}

func policyOrderBy(field PolicySortField, descending bool) (string, bool) {
	direction := " ASC"
	if descending {
		direction = " DESC"
	}
	switch field {
	case PolicySortTrainRunID:
		return " ORDER BY train_run_id" + direction + ", seat_class" + direction + ", id" + direction, true
	case PolicySortSeatClass:
		return " ORDER BY seat_class" + direction + ", train_run_id" + direction + ", id" + direction, true
	case PolicySortUpdatedAt:
		return " ORDER BY updated_at" + direction + ", id" + direction, true
	default:
		return "", false
	}
}

// MarkRedisInitialized closes the durable continuity latch only after the
// matching Redis generation has been installed. It is idempotent for the
// current version so concurrent workers may safely race the projection, while
// stale workers cannot acknowledge a newer policy generation.
func (s *Store) MarkRedisInitialized(ctx context.Context, policyID uuid.UUID, expectedVersion int64) (admissiondomain.HotTrainPolicy, error) {
	if s == nil || s.pool == nil || policyID == uuid.Nil || expectedVersion < 1 {
		return admissiondomain.HotTrainPolicy{}, ErrInvalidInput
	}
	policy, err := scanPolicy(s.pool.QueryRow(ctx, `
UPDATE hot_train_policies
SET redis_initialized_version = version
WHERE id = $1
  AND version = $2
  AND enabled
  AND (redis_initialized_version IS NULL OR redis_initialized_version = version)
RETURNING id, train_run_id, seat_class, enabled, version, redis_initialized_version,
          max_queue_size, admission_rate_per_second, max_inflight_admissions,
          admission_token_ttl_seconds, processing_lease_seconds, queue_entry_ttl_seconds,
          created_at, updated_at`, policyID, expectedVersion))
	if err == nil {
		return policy, nil
	}
	return resolveUpdateError(ctx, s.pool, policyID, err)
}

func (s *Store) UpdatePolicy(ctx context.Context, policyID uuid.UUID, params UpdatePolicyParams) (admissiondomain.HotTrainPolicy, error) {
	if s == nil || s.pool == nil || policyID == uuid.Nil || params.ExpectedVersion < 1 || !validMetadata(params.Metadata) {
		return admissiondomain.HotTrainPolicy{}, ErrInvalidInput
	}
	if _, err := admissiondomain.NewPolicyLimits(admissiondomain.PolicyLimitsInput(params.Limits)); err != nil {
		return admissiondomain.HotTrainPolicy{}, ErrInvalidInput
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return admissiondomain.HotTrainPolicy{}, safeError(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockPolicyScopeByID(ctx, tx, policyID); err != nil {
		return admissiondomain.HotTrainPolicy{}, err
	}
	policy, err := updatePolicy(ctx, tx, policyID, params)
	if err != nil {
		return admissiondomain.HotTrainPolicy{}, err
	}
	if err := appendPolicyEvent(ctx, tx, policy, "hot_train_policy.updated", params.Metadata); err != nil {
		return admissiondomain.HotTrainPolicy{}, safeError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return admissiondomain.HotTrainPolicy{}, safeError(err)
	}
	return policy, nil
}

func (s *Store) DisablePolicy(ctx context.Context, policyID uuid.UUID, expectedVersion int64, metadata MutationMetadata) (admissiondomain.HotTrainPolicy, error) {
	if s == nil || s.pool == nil || policyID == uuid.Nil || expectedVersion < 1 || !validMetadata(metadata) {
		return admissiondomain.HotTrainPolicy{}, ErrInvalidInput
	}
	tx, err := begin(ctx, s.pool)
	if err != nil {
		return admissiondomain.HotTrainPolicy{}, safeError(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockPolicyScopeByID(ctx, tx, policyID); err != nil {
		return admissiondomain.HotTrainPolicy{}, err
	}
	policy, err := updateEnabled(ctx, tx, policyID, expectedVersion, false)
	if err != nil {
		return admissiondomain.HotTrainPolicy{}, err
	}
	if err := appendPolicyEvent(ctx, tx, policy, "hot_train_policy.disabled", metadata); err != nil {
		return admissiondomain.HotTrainPolicy{}, safeError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return admissiondomain.HotTrainPolicy{}, safeError(err)
	}
	return policy, nil
}

const policySelect = `
SELECT id, train_run_id, seat_class, enabled, version, redis_initialized_version,
       max_queue_size, admission_rate_per_second, max_inflight_admissions,
       admission_token_ttl_seconds, processing_lease_seconds, queue_entry_ttl_seconds,
       created_at, updated_at
FROM hot_train_policies`

func insertPolicy(ctx context.Context, tx pgx.Tx, params CreatePolicyParams) (admissiondomain.HotTrainPolicy, error) {
	return scanPolicy(tx.QueryRow(ctx, `
INSERT INTO hot_train_policies (
    train_run_id, seat_class, max_queue_size, admission_rate_per_second,
    max_inflight_admissions, admission_token_ttl_seconds,
    processing_lease_seconds, queue_entry_ttl_seconds
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING id, train_run_id, seat_class, enabled, version, redis_initialized_version,
          max_queue_size, admission_rate_per_second, max_inflight_admissions,
          admission_token_ttl_seconds, processing_lease_seconds, queue_entry_ttl_seconds,
          created_at, updated_at`,
		params.TrainRunID, params.SeatClass.String(), params.Limits.MaxQueueSize,
		params.Limits.AdmissionRatePerSecond, params.Limits.MaxInflightAdmissions,
		int64(params.Limits.AdmissionTokenTTL/time.Second), int64(params.Limits.ProcessingLease/time.Second),
		int64(params.Limits.QueueEntryTTL/time.Second)))
}

// lockPolicyScopeByID resolves the immutable policy tuple before taking the
// exclusive transaction-scoped advisory lock. Optimistic version checks still
// decide concurrent operator conflicts after the lock is acquired.
func lockPolicyScopeByID(ctx context.Context, tx pgx.Tx, policyID uuid.UUID) error {
	var (
		trainRunID uuid.UUID
		seatClass  string
	)
	if err := tx.QueryRow(ctx, `
SELECT train_run_id, seat_class
FROM hot_train_policies
WHERE id = $1`, policyID).Scan(&trainRunID, &seatClass); err != nil {
		return safeError(err)
	}
	if err := policylock.AcquirePolicyMutation(ctx, tx, trainRunID, seatClass); err != nil {
		return safeError(err)
	}
	return nil
}

func updatePolicy(ctx context.Context, tx pgx.Tx, policyID uuid.UUID, params UpdatePolicyParams) (admissiondomain.HotTrainPolicy, error) {
	enabled := pgtype.Bool{}
	if params.Enabled != nil {
		enabled = pgtype.Bool{Bool: *params.Enabled, Valid: true}
	}
	policy, err := scanPolicy(tx.QueryRow(ctx, `
UPDATE hot_train_policies
SET max_queue_size = $3,
    admission_rate_per_second = $4,
    max_inflight_admissions = $5,
    admission_token_ttl_seconds = $6,
    processing_lease_seconds = $7,
    queue_entry_ttl_seconds = $8,
    enabled = COALESCE($9::boolean, enabled),
    version = version + 1,
    redis_initialized_version = NULL
WHERE id = $1 AND version = $2
RETURNING id, train_run_id, seat_class, enabled, version, redis_initialized_version,
          max_queue_size, admission_rate_per_second, max_inflight_admissions,
          admission_token_ttl_seconds, processing_lease_seconds, queue_entry_ttl_seconds,
          created_at, updated_at`,
		policyID, params.ExpectedVersion, params.Limits.MaxQueueSize,
		params.Limits.AdmissionRatePerSecond, params.Limits.MaxInflightAdmissions,
		int64(params.Limits.AdmissionTokenTTL/time.Second), int64(params.Limits.ProcessingLease/time.Second),
		int64(params.Limits.QueueEntryTTL/time.Second), enabled))
	if err == nil {
		return policy, nil
	}
	return resolveUpdateError(ctx, tx, policyID, err)
}

func updateEnabled(ctx context.Context, tx pgx.Tx, policyID uuid.UUID, expectedVersion int64, enabled bool) (admissiondomain.HotTrainPolicy, error) {
	policy, err := scanPolicy(tx.QueryRow(ctx, `
UPDATE hot_train_policies
SET enabled = $3, version = version + 1, redis_initialized_version = NULL
WHERE id = $1 AND version = $2
RETURNING id, train_run_id, seat_class, enabled, version, redis_initialized_version,
          max_queue_size, admission_rate_per_second, max_inflight_admissions,
          admission_token_ttl_seconds, processing_lease_seconds, queue_entry_ttl_seconds,
          created_at, updated_at`, policyID, expectedVersion, enabled))
	if err == nil {
		return policy, nil
	}
	return resolveUpdateError(ctx, tx, policyID, err)
}

type policyQueryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func resolveUpdateError(ctx context.Context, tx policyQueryRower, policyID uuid.UUID, original error) (admissiondomain.HotTrainPolicy, error) {
	// scanPolicy deliberately normalizes pgx.ErrNoRows to ErrNotFound for
	// ordinary reads. Mutation callers still need one bounded existence check
	// to distinguish a stale version from a genuinely missing policy.
	if !errors.Is(original, pgx.ErrNoRows) && !errors.Is(original, ErrNotFound) {
		return admissiondomain.HotTrainPolicy{}, safeError(original)
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM hot_train_policies WHERE id = $1)`, policyID).Scan(&exists); err != nil {
		return admissiondomain.HotTrainPolicy{}, safeError(err)
	}
	if exists {
		return admissiondomain.HotTrainPolicy{}, ErrVersionConflict
	}
	return admissiondomain.HotTrainPolicy{}, ErrNotFound
}

func scanPolicy(row pgx.Row) (admissiondomain.HotTrainPolicy, error) {
	var (
		id, trainRunID              uuid.UUID
		seatClassRaw                string
		enabled                     bool
		version                     int64
		redisInitialized            pgtype.Int8
		maxQueue, rate, maxInflight int
		tokenTTL, lease, queueTTL   int64
		createdAt, updatedAt        time.Time
	)
	if err := row.Scan(&id, &trainRunID, &seatClassRaw, &enabled, &version, &redisInitialized,
		&maxQueue, &rate, &maxInflight, &tokenTTL, &lease, &queueTTL, &createdAt, &updatedAt); err != nil {
		return admissiondomain.HotTrainPolicy{}, safeError(err)
	}
	seatClass, err := offeringdomain.ParseSeatClass(seatClassRaw)
	if err != nil {
		return admissiondomain.HotTrainPolicy{}, ErrPersistence
	}
	limits, err := admissiondomain.NewPolicyLimits(admissiondomain.PolicyLimitsInput{
		MaxQueueSize: maxQueue, AdmissionRatePerSecond: rate, MaxInflightAdmissions: maxInflight,
		AdmissionTokenTTL: time.Duration(tokenTTL) * time.Second,
		ProcessingLease:   time.Duration(lease) * time.Second,
		QueueEntryTTL:     time.Duration(queueTTL) * time.Second,
	})
	if err != nil {
		return admissiondomain.HotTrainPolicy{}, ErrPersistence
	}
	var initialized *int64
	if redisInitialized.Valid {
		initialized = &redisInitialized.Int64
	}
	policy, err := admissiondomain.NewHotTrainPolicy(id, trainRunID, seatClass, enabled, version, initialized, limits, createdAt, updatedAt)
	if err != nil {
		return admissiondomain.HotTrainPolicy{}, ErrPersistence
	}
	return policy, nil
}

func validMetadata(metadata MutationMetadata) bool {
	return metadata.ActorID != uuid.Nil && runeLength(metadata.CorrelationID) >= 1 && runeLength(metadata.CorrelationID) <= maxCorrelationIDRunes
}

func runeLength(value string) int { return utf8.RuneCountInString(strings.TrimSpace(value)) }

func appendPolicyEvent(ctx context.Context, tx pgx.Tx, policy admissiondomain.HotTrainPolicy, eventType string, metadata MutationMetadata) error {
	payload, err := json.Marshal(struct {
		PolicyID      string `json:"policy_id"`
		TrainRunID    string `json:"train_run_id"`
		SeatClass     string `json:"seat_class"`
		Enabled       bool   `json:"enabled"`
		Version       int64  `json:"version"`
		ActorID       string `json:"actor_id"`
		CorrelationID string `json:"correlation_id"`
	}{
		PolicyID: policy.ID.String(), TrainRunID: policy.TrainRunID.String(), SeatClass: policy.SeatClass.String(),
		Enabled: policy.Enabled, Version: policy.Version, ActorID: metadata.ActorID.String(),
		CorrelationID: strings.TrimSpace(metadata.CorrelationID),
	})
	if err != nil || len(payload) > 64*1024 {
		return ErrInvalidInput
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, event_version, payload)
VALUES ('hot_train_policy', $1, $2, 1, $3::jsonb)`, policy.ID, eventType, payload); err != nil {
		return fmt.Errorf("append hot-train-policy outbox event: %w", err)
	}
	return nil
}
