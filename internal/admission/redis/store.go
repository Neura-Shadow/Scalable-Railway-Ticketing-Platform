package admissionredis

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/admission/domain"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

var (
	ErrInvalidInput       = errors.New("invalid admission redis input")
	ErrBackendUnavailable = errors.New("admission redis backend unavailable")
	ErrPolicyMismatch     = errors.New("admission policy version mismatch")
	ErrContinuityLost     = errors.New("admission redis continuity lost")
	ErrQueueFull          = errors.New("admission waiting room full")
	ErrJoinConflict       = errors.New("admission waiting room join conflict")
	ErrNotFound           = errors.New("admission redis record not found")
	ErrOwnerMismatch      = errors.New("admission record owner mismatch")
	ErrTerminal           = errors.New("admission record is terminal")
	ErrInProgress         = errors.New("admission token is processing")
	ErrTokenMismatch      = errors.New("admission token binding mismatch")
)

const (
	MaxAdmissionBatch     = 1_000
	cleanupMargin         = 5 * time.Minute
	locatorCleanupTimeout = 2 * time.Second
)

type PolicyScope struct {
	PolicyID   string
	TrainRunID string
	SeatClass  string
	Version    int64
}

type JoinRequest struct {
	Scope                PolicyScope
	EntryID              string
	OwnerHash            [sha256.Size]byte
	AdmissionFingerprint [sha256.Size]byte
	FromStopIndex        int
	ToStopIndex          int
	PassengerCount       int
	MaxQueueSize         int
	EntryTTL             time.Duration
}

type IssueCandidate struct {
	EntryID string
	Token   domain.TokenDeliveryFields
}

type IssueLocator struct {
	EntryID   string
	TokenHash [sha256.Size]byte
}

type IssueRequest struct {
	Scope                  PolicyScope
	AdmissionRatePerSecond int
	MaxInflightAdmissions  int
	TokenTTL               time.Duration
	GenerationTTL          time.Duration
	CleanupLimit           int
	Candidates             []IssueCandidate
}

type IssueResult struct {
	IssuedEntryIDs  []string
	RecoveredLeases int64
	ExpiredTokens   int64
	ExpiredEntries  int64
}

type CancelResult struct {
	EntryDeleted          bool
	LocatorCleanupPending bool
}

type MaintenanceResult struct {
	RecoveredLeases int64
	ExpiredTokens   int64
	ExpiredEntries  int64
}

type StateInspectionCursor struct {
	Entries      uint64
	Inflight     uint64
	Leases       uint64
	EntriesDone  bool
	InflightDone bool
	LeasesDone   bool
}

type StateInspection struct {
	DuplicateActiveUsers    int64
	InflightTokenMismatch   int64
	ExpiredInflightTokens   int64
	ExpiredProcessingLeases int64
	TokenEntryOwnerMismatch int64
	NextCursor              StateInspectionCursor
	Truncated               bool
}

type AcquireRequest struct {
	Scope                PolicyScope
	TokenHash            [sha256.Size]byte
	OwnerHash            [sha256.Size]byte
	AdmissionFingerprint [sha256.Size]byte
	BookingFingerprint   [sha256.Size]byte
	IdempotencyKeyHash   [sha256.Size]byte
	FromStopIndex        int
	ToStopIndex          int
	PassengerCount       int
	LeaseOwner           string
	ProcessingLease      time.Duration
}

type AcquireResult struct {
	Decision        domain.AdmissionDecision
	LeaseOwner      string
	LeaseGeneration int64
	RetryAfter      time.Duration
}

type LeaseMutation struct {
	Scope              PolicyScope
	TokenHash          [sha256.Size]byte
	OwnerHash          [sha256.Size]byte
	BookingFingerprint [sha256.Size]byte
	IdempotencyKeyHash [sha256.Size]byte
	LeaseOwner         string
	LeaseGeneration    int64
}

type Store struct {
	client goredis.UniversalClient
	keys   KeyBuilder

	install   *goredis.Script
	join      *goredis.Script
	get       *goredis.Script
	preflight *goredis.Script
	cancel    *goredis.Script
	peek      *goredis.Script
	issue     *goredis.Script
	acquire   *goredis.Script
	release   *goredis.Script
	finalize  *goredis.Script
	locate    *goredis.Script
	inspect   *goredis.Script
	reconcile *goredis.Script
}

func NewStore(client goredis.UniversalClient, namespace string) (*Store, error) {
	builder, err := NewKeyBuilder(namespace)
	if err != nil || client == nil {
		return nil, ErrInvalidInput
	}
	return &Store{
		client: client, keys: builder,
		install:   goredis.NewScript(installPolicyScript),
		join:      goredis.NewScript(joinScript),
		get:       goredis.NewScript(getScript),
		preflight: goredis.NewScript(inspectDeliveryScript),
		cancel:    goredis.NewScript(cancelScript),
		peek:      goredis.NewScript(peekScript),
		issue:     goredis.NewScript(issueScript),
		acquire:   goredis.NewScript(acquireScript),
		release:   goredis.NewScript(releaseScript),
		finalize:  goredis.NewScript(finalizeScript),
		locate:    goredis.NewScript(putLocatorScript),
		inspect:   goredis.NewScript(inspectTokenScript),
		reconcile: goredis.NewScript(inspectStateScript),
	}, nil
}

// InstallPolicy creates a generation only when explicitly allowed. A missing
// marker for an already initialized durable generation is continuity loss.
func (s *Store) InstallPolicy(ctx context.Context, scope PolicyScope, allowCreate bool, ttl time.Duration) error {
	keys, err := s.policyKeys(scope)
	if err != nil || ttl < domain.MinQueueEntryTTL || ttl > domain.MaxQueueEntryTTL+cleanupMargin {
		return ErrInvalidInput
	}
	result, err := s.install.Run(ctx, s.client, []string{keys.PolicyVersion, keys.Continuity},
		scope.Version, boolInt(allowCreate), ttl.Milliseconds()).Text()
	if err != nil {
		return backendError(err)
	}
	return resultError(result)
}

func (s *Store) Join(ctx context.Context, request JoinRequest) (domain.WaitingRoomEntry, bool, error) {
	keys, err := s.policyKeys(request.Scope)
	if err != nil || uuid.Validate(request.EntryID) != nil || request.FromStopIndex < 0 ||
		request.ToStopIndex <= request.FromStopIndex || request.PassengerCount < 1 ||
		request.PassengerCount > domain.MaxAdmissionPassengers || request.MaxQueueSize < 1 ||
		request.MaxQueueSize > domain.MaxQueueSize || request.EntryTTL < domain.MinQueueEntryTTL ||
		request.EntryTTL > domain.MaxQueueEntryTTL {
		return domain.WaitingRoomEntry{}, false, ErrInvalidInput
	}
	args := []any{
		request.Scope.Version, hex32(request.OwnerHash), request.EntryID, hex32(request.AdmissionFingerprint),
		request.Scope.PolicyID, request.Scope.TrainRunID, strings.ToLower(request.Scope.SeatClass),
		request.FromStopIndex, request.ToStopIndex, request.PassengerCount,
		request.MaxQueueSize, request.EntryTTL.Milliseconds(), (request.EntryTTL + cleanupMargin).Milliseconds(),
	}
	value, err := s.join.Run(ctx, s.client, []string{
		keys.PolicyVersion, keys.Continuity, keys.Queue, keys.Sequence, keys.Entries, keys.Users, keys.Leases,
	}, args...).Slice()
	if err != nil {
		return domain.WaitingRoomEntry{}, false, backendError(err)
	}
	return parseJoinResult(value)
}

func parseJoinResult(value []any) (domain.WaitingRoomEntry, bool, error) {
	if len(value) < 1 {
		return domain.WaitingRoomEntry{}, false, ErrBackendUnavailable
	}
	code := stringValue(value[0])
	if code != "joined" && code != "duplicate" {
		return domain.WaitingRoomEntry{}, false, resultError(code)
	}
	if len(value) != 17 {
		return domain.WaitingRoomEntry{}, false, ErrBackendUnavailable
	}
	entry, err := parseEntrySlice(value[1:])
	return entry, code == "duplicate", err
}

func (s *Store) Get(ctx context.Context, scope PolicyScope, entryID string, ownerHash [sha256.Size]byte) (domain.WaitingRoomEntry, error) {
	keys, err := s.policyKeys(scope)
	if err != nil || uuid.Validate(entryID) != nil {
		return domain.WaitingRoomEntry{}, ErrInvalidInput
	}
	value, err := s.get.Run(ctx, s.client, []string{
		keys.PolicyVersion, keys.Continuity, keys.Queue, keys.Entries, keys.Users,
		keys.Tokens, keys.Inflight, keys.Leases,
	}, scope.Version, entryID, hex32(ownerHash)).Slice()
	if err != nil {
		return domain.WaitingRoomEntry{}, backendError(err)
	}
	if len(value) < 2 || stringValue(value[0]) != "ok" {
		if len(value) > 0 {
			return domain.WaitingRoomEntry{}, resultError(stringValue(value[0]))
		}
		return domain.WaitingRoomEntry{}, ErrBackendUnavailable
	}
	return parseEntrySlice(value[1:])
}

// InspectDelivery returns the immutable delivery metadata without changing the
// at-most-once delivery marker. Callers reconstruct and hash-verify the bearer
// with their keyring before invoking ClaimDelivery.
func (s *Store) InspectDelivery(ctx context.Context, scope PolicyScope, entryID string, ownerHash [sha256.Size]byte) (domain.TokenDeliveryFields, error) {
	keys, err := s.policyKeys(scope)
	if err != nil || uuid.Validate(entryID) != nil {
		return domain.TokenDeliveryFields{}, ErrInvalidInput
	}
	value, err := s.preflight.Run(ctx, s.client, []string{
		keys.PolicyVersion, keys.Continuity, keys.Entries, keys.Tokens,
	}, scope.Version, entryID, hex32(ownerHash)).Slice()
	if err != nil {
		return domain.TokenDeliveryFields{}, backendError(err)
	}
	if len(value) != 11 || stringValue(value[0]) != "delivery" {
		if len(value) > 0 {
			return domain.TokenDeliveryFields{}, resultError(stringValue(value[0]))
		}
		return domain.TokenDeliveryFields{}, ErrBackendUnavailable
	}
	return parseDelivery(value[1:])
}

// ClaimDelivery atomically changes the delivery flag before returning signed
// fields. Network loss after this call is intentionally at-most-once.
func (s *Store) ClaimDelivery(ctx context.Context, scope PolicyScope, entryID string, ownerHash [sha256.Size]byte) (domain.TokenDeliveryFields, error) {
	keys, err := s.policyKeys(scope)
	if err != nil || uuid.Validate(entryID) != nil {
		return domain.TokenDeliveryFields{}, ErrInvalidInput
	}
	value, err := s.get.Run(ctx, s.client, []string{
		keys.PolicyVersion, keys.Continuity, keys.Queue, keys.Entries, keys.Users,
		keys.Tokens, keys.Inflight, keys.Leases,
	}, scope.Version, entryID, hex32(ownerHash), 1).Slice()
	if err != nil {
		return domain.TokenDeliveryFields{}, backendError(err)
	}
	if len(value) != 11 || stringValue(value[0]) != "delivery" {
		if len(value) > 0 {
			return domain.TokenDeliveryFields{}, resultError(stringValue(value[0]))
		}
		return domain.TokenDeliveryFields{}, ErrBackendUnavailable
	}
	return parseDelivery(value[1:])
}

func (s *Store) Cancel(
	ctx context.Context,
	scope PolicyScope,
	entryID string,
	ownerHash [sha256.Size]byte,
) (CancelResult, error) {
	keys, err := s.policyKeys(scope)
	if err != nil || uuid.Validate(entryID) != nil {
		return CancelResult{}, ErrInvalidInput
	}
	result, err := s.cancel.Run(ctx, s.client, []string{
		keys.PolicyVersion, keys.Continuity, keys.Queue, keys.Entries, keys.Users,
		keys.Tokens, keys.Inflight, keys.Leases,
	}, scope.Version, entryID, hex32(ownerHash)).Text()
	if err != nil {
		return CancelResult{}, backendError(err)
	}
	if err := resultError(result); err != nil {
		return CancelResult{}, err
	}
	cancelled := CancelResult{EntryDeleted: result == "cancelled_deleted"}
	if cancelled.EntryDeleted {
		cleanupContext, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), locatorCleanupTimeout)
		defer cancelCleanup()
		if err := s.DeleteEntryLocator(cleanupContext, entryID); err != nil {
			// The policy-generation mutation already committed. Preserve that
			// authoritative success and expose the bounded locator cleanup lag
			// for metrics; the locator retains its original finite TTL.
			cancelled.LocatorCleanupPending = true
		}
	}
	return cancelled, nil
}

func (s *Store) PeekQueued(ctx context.Context, scope PolicyScope, limit int) ([]domain.WaitingRoomEntry, error) {
	keys, err := s.policyKeys(scope)
	if err != nil || limit < 1 || limit > MaxAdmissionBatch {
		return nil, ErrInvalidInput
	}
	value, err := s.peek.Run(ctx, s.client, []string{
		keys.PolicyVersion, keys.Continuity, keys.Queue, keys.Entries, keys.Users,
	}, scope.Version, limit).Slice()
	if err != nil {
		return nil, backendError(err)
	}
	if len(value) < 1 || stringValue(value[0]) != "ok" {
		if len(value) > 0 {
			return nil, resultError(stringValue(value[0]))
		}
		return nil, ErrBackendUnavailable
	}
	entries := make([]domain.WaitingRoomEntry, 0, (len(value)-1)/16)
	for index := 1; index+15 < len(value); index += 16 {
		entry, parseErr := parseEntrySlice(value[index : index+16])
		if parseErr != nil {
			return nil, parseErr
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (s *Store) Issue(ctx context.Context, request IssueRequest) (IssueResult, error) {
	keys, err := s.policyKeys(request.Scope)
	if err != nil || request.AdmissionRatePerSecond < 1 || request.AdmissionRatePerSecond > domain.MaxAdmissionRatePerSecond ||
		request.MaxInflightAdmissions < 1 || request.MaxInflightAdmissions > domain.MaxInflightAdmissions ||
		request.TokenTTL < domain.MinAdmissionTokenTTL || request.TokenTTL > domain.MaxAdmissionTokenTTL ||
		request.GenerationTTL < domain.MinQueueEntryTTL || request.GenerationTTL > domain.MaxQueueEntryTTL+cleanupMargin ||
		len(request.Candidates) > MaxAdmissionBatch {
		return IssueResult{}, ErrInvalidInput
	}
	cleanupLimit := request.CleanupLimit
	if cleanupLimit == 0 {
		cleanupLimit = len(request.Candidates)
	}
	if cleanupLimit < 1 || cleanupLimit > MaxAdmissionBatch || len(request.Candidates) < 1 {
		return IssueResult{}, ErrInvalidInput
	}
	args := []any{
		request.Scope.Version, request.AdmissionRatePerSecond, request.MaxInflightAdmissions,
		request.TokenTTL.Milliseconds(), request.GenerationTTL.Milliseconds(), len(request.Candidates), cleanupLimit,
	}
	for _, candidate := range request.Candidates {
		fields := candidate.Token
		if uuid.Validate(candidate.EntryID) != nil ||
			fields.TokenHash == ([sha256.Size]byte{}) ||
			fields.Claims.EntryID != candidate.EntryID || fields.Claims.PolicyID != request.Scope.PolicyID ||
			fields.Claims.PolicyVersion != request.Scope.Version {
			return IssueResult{}, ErrInvalidInput
		}
		args = append(args,
			candidate.EntryID, hex32(fields.TokenHash), fields.Claims.KeyID,
			base64.RawURLEncoding.EncodeToString(fields.Nonce[:]),
			fields.Claims.IssuedAt.UTC().UnixMilli(), fields.Claims.ExpiresAt.UTC().UnixMilli(),
			hex32(fields.Claims.OwnerHash), hex32(fields.Claims.AdmissionFingerprint),
		)
	}
	value, err := s.issue.Run(ctx, s.client, []string{
		keys.PolicyVersion, keys.Continuity, keys.Queue, keys.Entries, keys.Users,
		keys.Tokens, keys.Inflight, keys.Rate, keys.Leases,
	}, args...).Slice()
	if err != nil {
		return IssueResult{}, backendError(err)
	}
	return parseIssueResult(value)
}

// Maintain reclaims a bounded number of expired leases and token records even
// when no queue entries or issuance candidates exist.
func (s *Store) Maintain(ctx context.Context, scope PolicyScope, cleanupLimit int, generationTTL time.Duration) (MaintenanceResult, error) {
	keys, err := s.policyKeys(scope)
	if err != nil || cleanupLimit < 1 || cleanupLimit > MaxAdmissionBatch ||
		generationTTL < domain.MinQueueEntryTTL || generationTTL > domain.MaxQueueEntryTTL+cleanupMargin {
		return MaintenanceResult{}, ErrInvalidInput
	}
	value, err := s.issue.Run(ctx, s.client, []string{
		keys.PolicyVersion, keys.Continuity, keys.Queue, keys.Entries, keys.Users,
		keys.Tokens, keys.Inflight, keys.Rate, keys.Leases,
	}, scope.Version, 1, 1, 1, generationTTL.Milliseconds(), 0, cleanupLimit).Slice()
	if err != nil {
		return MaintenanceResult{}, backendError(err)
	}
	return parseMaintenanceResult(value)
}

func parseIssueResult(value []any) (IssueResult, error) {
	if len(value) < 4 {
		if len(value) > 0 && stringValue(value[0]) != "ok" {
			return IssueResult{}, resultError(stringValue(value[0]))
		}
		return IssueResult{}, ErrBackendUnavailable
	}
	if stringValue(value[0]) != "ok" {
		return IssueResult{}, resultError(stringValue(value[0]))
	}
	maintenance, err := parseMaintenanceResult(value[:4])
	if err != nil {
		return IssueResult{}, err
	}
	result := IssueResult{
		IssuedEntryIDs:  make([]string, 0, len(value)-4),
		RecoveredLeases: maintenance.RecoveredLeases,
		ExpiredTokens:   maintenance.ExpiredTokens,
		ExpiredEntries:  maintenance.ExpiredEntries,
	}
	for _, item := range value[4:] {
		entryID := stringValue(item)
		if uuid.Validate(entryID) != nil {
			return IssueResult{}, ErrBackendUnavailable
		}
		result.IssuedEntryIDs = append(result.IssuedEntryIDs, entryID)
	}
	return result, nil
}

func parseMaintenanceResult(value []any) (MaintenanceResult, error) {
	if len(value) != 4 {
		if len(value) > 0 && stringValue(value[0]) != "ok" {
			return MaintenanceResult{}, resultError(stringValue(value[0]))
		}
		return MaintenanceResult{}, ErrBackendUnavailable
	}
	if stringValue(value[0]) != "ok" {
		return MaintenanceResult{}, resultError(stringValue(value[0]))
	}
	recovered, firstErr := int64Value(value[1])
	expiredTokens, secondErr := int64Value(value[2])
	expiredEntries, thirdErr := int64Value(value[3])
	if errors.Join(firstErr, secondErr, thirdErr) != nil ||
		recovered < 0 || expiredTokens < 0 || expiredEntries < 0 {
		return MaintenanceResult{}, ErrBackendUnavailable
	}
	return MaintenanceResult{
		RecoveredLeases: recovered,
		ExpiredTokens:   expiredTokens,
		ExpiredEntries:  expiredEntries,
	}, nil
}

// Time returns Redis server time for preparing issuance-MAC timestamps. The
// issue script rejects future-skewed or expired candidates but accepts an older
// still-valid candidate after bounded write-ahead work. Redis TIME remains
// authoritative for rate, inflight, expiry, and lease decisions.
func (s *Store) Time(ctx context.Context) (time.Time, error) {
	if s == nil || s.client == nil {
		return time.Time{}, ErrInvalidInput
	}
	value, err := s.client.Time(ctx).Result()
	if err != nil {
		return time.Time{}, backendError(err)
	}
	return value.UTC(), nil
}

// InspectToken returns immutable Redis token metadata without claiming
// delivery or mutating token state. Callers must recompute the bearer with the
// keyring and compare its digest with TokenHash before invoking Acquire.
func (s *Store) InspectToken(ctx context.Context, scope PolicyScope, tokenHash [sha256.Size]byte) (domain.TokenDeliveryFields, error) {
	keys, err := s.policyKeys(scope)
	if err != nil {
		return domain.TokenDeliveryFields{}, ErrInvalidInput
	}
	value, err := s.inspect.Run(ctx, s.client, []string{
		keys.PolicyVersion, keys.Continuity, keys.Tokens,
	}, scope.Version, hex32(tokenHash)).Slice()
	if err != nil {
		return domain.TokenDeliveryFields{}, backendError(err)
	}
	if len(value) != 11 || stringValue(value[0]) != "token" {
		if len(value) > 0 {
			return domain.TokenDeliveryFields{}, resultError(stringValue(value[0]))
		}
		return domain.TokenDeliveryFields{}, ErrBackendUnavailable
	}
	return parseDelivery(value[1:])
}

// InspectState performs one bounded, read-only reconciliation page over exact
// policy-generation keys. Callers iterate NextCursor until Truncated is false.
func (s *Store) InspectState(
	ctx context.Context,
	scope PolicyScope,
	cursor StateInspectionCursor,
	limit int,
) (StateInspection, error) {
	keys, err := s.policyKeys(scope)
	if err != nil || limit < 1 || limit > MaxAdmissionBatch ||
		(cursor.EntriesDone && cursor.Entries != 0) ||
		(cursor.InflightDone && cursor.Inflight != 0) ||
		(cursor.LeasesDone && cursor.Leases != 0) {
		return StateInspection{}, ErrInvalidInput
	}
	value, err := s.reconcile.Run(ctx, s.client, []string{
		keys.PolicyVersion, keys.Continuity, keys.Entries, keys.Users,
		keys.Tokens, keys.Inflight, keys.Leases,
	}, scope.Version, cursor.Entries, cursor.Inflight, cursor.Leases,
		boolInt(cursor.EntriesDone), boolInt(cursor.InflightDone), boolInt(cursor.LeasesDone), limit).Slice()
	if err != nil {
		return StateInspection{}, backendError(err)
	}
	if len(value) != 9 || stringValue(value[0]) != "ok" {
		if len(value) > 0 {
			return StateInspection{}, resultError(stringValue(value[0]))
		}
		return StateInspection{}, ErrBackendUnavailable
	}
	counts := make([]int64, 5)
	for index := range counts {
		counts[index], err = int64Value(value[index+1])
		if err != nil {
			return StateInspection{}, ErrBackendUnavailable
		}
	}
	entryCursor, firstCursorErr := uint64Value(value[6])
	inflightCursor, secondCursorErr := uint64Value(value[7])
	leaseCursor, thirdCursorErr := uint64Value(value[8])
	if errors.Join(firstCursorErr, secondCursorErr, thirdCursorErr) != nil {
		return StateInspection{}, ErrBackendUnavailable
	}
	next := StateInspectionCursor{
		Entries: entryCursor, Inflight: inflightCursor, Leases: leaseCursor,
		EntriesDone:  cursor.EntriesDone || entryCursor == 0,
		InflightDone: cursor.InflightDone || inflightCursor == 0,
		LeasesDone:   cursor.LeasesDone || leaseCursor == 0,
	}
	return StateInspection{
		DuplicateActiveUsers: counts[0], InflightTokenMismatch: counts[1],
		ExpiredInflightTokens: counts[2], ExpiredProcessingLeases: counts[3],
		TokenEntryOwnerMismatch: counts[4], NextCursor: next,
		Truncated: !(next.EntriesDone && next.InflightDone && next.LeasesDone),
	}, nil
}

// PutEntryLocator writes one exact, bounded-TTL locator for a joined entry or
// bounded admission candidate. Only a canonical server-generated UUID becomes
// a key.
func (s *Store) PutEntryLocator(ctx context.Context, entryID string, scope PolicyScope, ttl time.Duration) error {
	key, err := s.keys.EntryLocator(entryID)
	_, scopeErr := s.policyKeys(scope)
	policyID, policyErr := uuid.Parse(strings.TrimSpace(scope.PolicyID))
	runID, runErr := uuid.Parse(strings.TrimSpace(scope.TrainRunID))
	if err != nil || scopeErr != nil || policyErr != nil || runErr != nil ||
		ttl < domain.MinQueueEntryTTL || ttl > domain.MaxQueueEntryTTL+cleanupMargin {
		return ErrInvalidInput
	}
	value := fmt.Sprintf("%s|%s|%s|%d", policyID.String(), runID.String(), strings.ToLower(scope.SeatClass), scope.Version)
	result, scriptErr := s.locate.Run(ctx, s.client, []string{key}, value, ttl.Milliseconds()).Text()
	if scriptErr != nil {
		return backendError(scriptErr)
	}
	return resultError(result)
}

// DeleteEntryLocator removes the exact scope locator after a queued entry was
// atomically cancelled and physically deleted from its policy generation.
func (s *Store) DeleteEntryLocator(ctx context.Context, entryID string) error {
	if s == nil || s.client == nil {
		return ErrInvalidInput
	}
	key, err := s.keys.EntryLocator(entryID)
	if err != nil {
		return ErrInvalidInput
	}
	if err := s.client.Unlink(ctx, key).Err(); err != nil {
		return backendError(err)
	}
	return nil
}

// ResolveEntryLocator performs one exact GET and strictly validates every
// bounded component. It never uses SCAN or KEYS.
func (s *Store) ResolveEntryLocator(ctx context.Context, entryID string) (PolicyScope, error) {
	key, err := s.keys.EntryLocator(entryID)
	if err != nil {
		return PolicyScope{}, ErrInvalidInput
	}
	value, err := s.client.Get(ctx, key).Result()
	if errors.Is(err, goredis.Nil) {
		return PolicyScope{}, ErrNotFound
	}
	if err != nil {
		return PolicyScope{}, backendError(err)
	}
	if len(value) > 256 {
		return PolicyScope{}, ErrBackendUnavailable
	}
	parts := strings.Split(value, "|")
	if len(parts) != 4 {
		return PolicyScope{}, ErrBackendUnavailable
	}
	policyID, policyErr := uuid.Parse(parts[0])
	runID, runErr := uuid.Parse(parts[1])
	version, versionErr := strconv.ParseInt(parts[3], 10, 64)
	if policyErr != nil || runErr != nil || versionErr != nil {
		return PolicyScope{}, ErrBackendUnavailable
	}
	scope := PolicyScope{PolicyID: policyID.String(), TrainRunID: runID.String(), SeatClass: parts[2], Version: version}
	if parts[0] != scope.PolicyID || parts[1] != scope.TrainRunID ||
		parts[2] != strings.ToLower(parts[2]) {
		return PolicyScope{}, ErrBackendUnavailable
	}
	if _, keyErr := s.policyKeys(scope); keyErr != nil {
		return PolicyScope{}, ErrBackendUnavailable
	}
	return scope, nil
}

func (s *Store) Acquire(ctx context.Context, request AcquireRequest) (AcquireResult, error) {
	keys, err := s.policyKeys(request.Scope)
	if err != nil || request.LeaseOwner == "" || uuid.Validate(request.LeaseOwner) != nil ||
		request.ProcessingLease < domain.MinProcessingLease || request.ProcessingLease > domain.MaxProcessingLease ||
		request.FromStopIndex < 0 || request.ToStopIndex <= request.FromStopIndex ||
		request.PassengerCount < 1 || request.PassengerCount > domain.MaxAdmissionPassengers {
		return AcquireResult{}, ErrInvalidInput
	}
	value, err := s.acquire.Run(ctx, s.client, []string{
		keys.PolicyVersion, keys.Continuity, keys.Tokens, keys.Inflight, keys.Leases,
		keys.Entries, keys.Users,
	}, request.Scope.Version, hex32(request.TokenHash), hex32(request.OwnerHash),
		hex32(request.AdmissionFingerprint), hex32(request.BookingFingerprint),
		hex32(request.IdempotencyKeyHash), request.FromStopIndex, request.ToStopIndex,
		strings.ToLower(request.Scope.SeatClass), request.PassengerCount,
		request.LeaseOwner, request.ProcessingLease.Milliseconds()).Slice()
	if err != nil {
		return AcquireResult{}, backendError(err)
	}
	if len(value) < 1 {
		return AcquireResult{}, ErrBackendUnavailable
	}
	code := stringValue(value[0])
	switch code {
	case string(domain.DecisionAcquired):
		if len(value) != 3 {
			return AcquireResult{}, ErrBackendUnavailable
		}
		generation, parseErr := int64Value(value[2])
		return AcquireResult{Decision: domain.DecisionAcquired, LeaseOwner: stringValue(value[1]), LeaseGeneration: generation}, parseErr
	case string(domain.DecisionRetryAllowed):
		retry, parseErr := int64Value(value[1])
		return AcquireResult{Decision: domain.DecisionRetryAllowed, RetryAfter: time.Duration(retry) * time.Millisecond}, parseErr
	case string(domain.DecisionReplayAllowed):
		return AcquireResult{Decision: domain.DecisionReplayAllowed}, nil
	default:
		return AcquireResult{}, resultError(code)
	}
}

func (s *Store) Release(ctx context.Context, mutation LeaseMutation, permanent bool) error {
	return s.mutateLease(ctx, mutation, boolInt(permanent), s.release)
}

func (s *Store) Finalize(ctx context.Context, mutation LeaseMutation) error {
	return s.mutateLease(ctx, mutation, 0, s.finalize)
}

func (s *Store) mutateLease(ctx context.Context, mutation LeaseMutation, action int, script *goredis.Script) error {
	keys, err := s.policyKeys(mutation.Scope)
	if err != nil || mutation.LeaseOwner == "" || mutation.LeaseGeneration < 1 {
		return ErrInvalidInput
	}
	result, err := script.Run(ctx, s.client, []string{
		keys.PolicyVersion, keys.Continuity, keys.Tokens, keys.Inflight, keys.Leases,
		keys.Entries, keys.Users,
	}, mutation.Scope.Version, hex32(mutation.TokenHash), hex32(mutation.OwnerHash),
		hex32(mutation.BookingFingerprint), hex32(mutation.IdempotencyKeyHash),
		mutation.LeaseOwner, mutation.LeaseGeneration, action).Text()
	if err != nil {
		return backendError(err)
	}
	return resultError(result)
}

func (s *Store) policyKeys(scope PolicyScope) (PolicyKeys, error) {
	if s == nil || s.client == nil || scope.PolicyID == "" {
		return PolicyKeys{}, ErrInvalidInput
	}
	return s.keys.ForPolicy(scope.TrainRunID, scope.SeatClass, scope.Version)
}

func backendError(err error) error {
	return fmt.Errorf("%w: %v", ErrBackendUnavailable, err)
}

func resultError(code string) error {
	switch code {
	case "ok", "joined", "duplicate", "cancelled", "cancelled_deleted", "released", "finalized":
		return nil
	case "policy_mismatch":
		return ErrPolicyMismatch
	case "continuity_lost":
		return ErrContinuityLost
	case "queue_full":
		return ErrQueueFull
	case "join_conflict":
		return ErrJoinConflict
	case "not_found", "not_admitted", "already_delivered":
		return ErrNotFound
	case "owner_mismatch":
		return ErrOwnerMismatch
	case "terminal", "expired":
		return ErrTerminal
	case "in_progress":
		return ErrInProgress
	case "mismatch", "stale_lease":
		return ErrTokenMismatch
	default:
		return ErrBackendUnavailable
	}
}

func parseEntrySlice(value []any) (domain.WaitingRoomEntry, error) {
	if len(value) != 16 {
		return domain.WaitingRoomEntry{}, ErrBackendUnavailable
	}
	sequence, err := int64Value(value[1])
	joinedMillis, err2 := int64Value(value[7])
	expiresMillis, err3 := int64Value(value[8])
	position, err4 := int64Value(value[9])
	from, err5 := int64Value(value[10])
	to, err6 := int64Value(value[11])
	count, err7 := int64Value(value[12])
	version, err8 := int64Value(value[14])
	if errors.Join(err, err2, err3, err4, err5, err6, err7, err8) != nil {
		return domain.WaitingRoomEntry{}, ErrBackendUnavailable
	}
	owner, err := parseHex32(stringValue(value[2]))
	if err != nil {
		return domain.WaitingRoomEntry{}, err
	}
	fingerprint, err := parseHex32(stringValue(value[3]))
	if err != nil {
		return domain.WaitingRoomEntry{}, err
	}
	entry := domain.WaitingRoomEntry{
		ID: stringValue(value[0]), Sequence: sequence, OwnerHash: owner,
		AdmissionFingerprint: fingerprint, Status: domain.EntryStatus(stringValue(value[4])),
		PolicyID: stringValue(value[5]), TrainRunID: stringValue(value[6]),
		JoinedAt: time.UnixMilli(joinedMillis).UTC(), ExpiresAt: time.UnixMilli(expiresMillis).UTC(),
		Position: domain.QueuePosition{Approximate: position}, FromStopIndex: int(from),
		ToStopIndex: int(to), PassengerCount: int(count),
		SeatClass: stringValue(value[13]), PolicyVersion: version,
	}
	if admittedValue := stringValue(value[15]); admittedValue != "" {
		admittedMillis, admittedErr := strconv.ParseInt(admittedValue, 10, 64)
		if admittedErr != nil {
			return domain.WaitingRoomEntry{}, ErrBackendUnavailable
		}
		admittedAt := time.UnixMilli(admittedMillis).UTC()
		entry.AdmittedAt = &admittedAt
	}
	return entry, nil
}

func parseDelivery(value []any) (domain.TokenDeliveryFields, error) {
	if len(value) != 10 {
		return domain.TokenDeliveryFields{}, ErrBackendUnavailable
	}
	version, err := int64Value(value[2])
	issued, err2 := int64Value(value[7])
	expires, err3 := int64Value(value[8])
	owner, err4 := parseHex32(stringValue(value[5]))
	fingerprint, err5 := parseHex32(stringValue(value[6]))
	nonceBytes, err6 := base64.RawURLEncoding.Strict().DecodeString(stringValue(value[9]))
	tokenHash, err7 := parseHex32(stringValue(value[0]))
	if errors.Join(err, err2, err3, err4, err5, err6, err7) != nil ||
		len(nonceBytes) != 32 {
		return domain.TokenDeliveryFields{}, ErrBackendUnavailable
	}
	var nonce [32]byte
	copy(nonce[:], nonceBytes)
	return domain.TokenDeliveryFields{
		Claims: domain.TokenClaims{
			KeyID: stringValue(value[1]), PolicyVersion: version, PolicyID: stringValue(value[3]),
			EntryID: stringValue(value[4]), OwnerHash: owner, AdmissionFingerprint: fingerprint, IssuedAt: time.UnixMilli(issued).UTC(),
			ExpiresAt: time.UnixMilli(expires).UTC(),
		},
		Nonce: nonce, TokenHash: tokenHash,
	}, nil
}

func parseHex32(value string) ([sha256.Size]byte, error) {
	var result [sha256.Size]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return result, ErrBackendUnavailable
	}
	copy(result[:], decoded)
	return result, nil
}

func int64Value(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return 0, ErrBackendUnavailable
	}
}

func uint64Value(value any) (uint64, error) {
	switch typed := value.(type) {
	case int64:
		if typed < 0 {
			return 0, ErrBackendUnavailable
		}
		return uint64(typed), nil
	case string:
		return strconv.ParseUint(typed, 10, 64)
	case []byte:
		return strconv.ParseUint(string(typed), 10, 64)
	default:
		return 0, ErrBackendUnavailable
	}
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(value)
	}
}

func hex32(value [sha256.Size]byte) string {
	return hex.EncodeToString(value[:])
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
