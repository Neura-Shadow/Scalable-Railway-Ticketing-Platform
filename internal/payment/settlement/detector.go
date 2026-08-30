package settlement

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidDetector       = errors.New("invalid settlement detector")
	ErrInvalidDetectionScope = errors.New("invalid settlement detection scope")
	ErrInvalidComparison     = errors.New("invalid settlement comparison")
	ErrDetectionPageLimit    = errors.New("settlement detection page exceeds configured limit")
	ErrDetectionCursor       = errors.New("settlement detection cursor did not advance")
)

type DetectionScopeKind string

const (
	ScopePayment    DetectionScopeKind = "payment"
	ScopePeriod     DetectionScopeKind = "period"
	ScopeSettlement DetectionScopeKind = "settlement"
	ScopePayout     DetectionScopeKind = "payout"
)

func (kind DetectionScopeKind) Valid() bool {
	switch kind {
	case ScopePayment, ScopePeriod, ScopeSettlement, ScopePayout:
		return true
	default:
		return false
	}
}

type DetectionScope struct {
	Kind  DetectionScopeKind
	Value string
}

type EvidenceKind string

const (
	EvidenceProvider   EvidenceKind = "provider"
	EvidenceOperation  EvidenceKind = "payment_operation"
	EvidenceLedger     EvidenceKind = "ledger"
	EvidenceSettlement EvidenceKind = "settlement"
	EvidencePayout     EvidenceKind = "payout"
)

func (kind EvidenceKind) Valid() bool {
	switch kind {
	case EvidenceProvider, EvidenceOperation, EvidenceLedger, EvidenceSettlement, EvidencePayout:
		return true
	default:
		return false
	}
}

type Evidence struct {
	Present     bool
	AmountMinor int64
	FeeMinor    int64
	Currency    string
}

// Comparison contains already normalized, bounded evidence. Detector owns the
// mismatch rules and never receives a mutation or provider-operation gateway.
type Comparison struct {
	Correlation            string
	Kind                   EvidenceKind
	Expected               Evidence
	Observed               Evidence
	DuplicateCount         int
	Aged                   bool
	EventConflict          bool
	BalanceChecked         bool
	LedgerBalanced         bool
	PayoutLifecycleInvalid bool
}

type FindingReason string

const (
	FindingMissing         FindingReason = "missing"
	FindingUnexpected      FindingReason = "unexpected"
	FindingAmount          FindingReason = "amount"
	FindingCurrency        FindingReason = "currency"
	FindingFee             FindingReason = "fee"
	FindingDuplicate       FindingReason = "duplicate"
	FindingAge             FindingReason = "age"
	FindingEventConflict   FindingReason = "event_conflict"
	FindingImbalance       FindingReason = "ledger_imbalance"
	FindingPayoutLifecycle FindingReason = "payout_lifecycle"
)

type Finding struct {
	Correlation string
	Kind        EvidenceKind
	Reason      FindingReason
}

type DetectionPage struct {
	Comparisons []Comparison
	NextCursor  string
	Done        bool
}

type DetectionRun struct {
	ID          uuid.UUID
	Scope       DetectionScope
	StartedAt   time.Time
	CompletedAt time.Time
	Pages       int
	Examined    int
	Completed   bool
	Bounded     bool
	Findings    []Finding
}

// DetectionStore is intentionally read-plus-append-only. There is no repair,
// provider mutation, ledger rewrite, booking, ticket, or inventory method.
type DetectionStore interface {
	ReadDetectionPage(context.Context, DetectionScope, string, int) (DetectionPage, error)
	AppendDetectionRun(context.Context, DetectionRun) error
}

type DetectorConfig struct {
	PageSize int
	MaxPages int
	Clock    Clock
}

type DetectionReport struct {
	RunID     uuid.UUID
	Pages     int
	Examined  int
	Completed bool
	Bounded   bool
	Findings  []Finding
}

type Detector struct {
	store    DetectionStore
	pageSize int
	maxPages int
	clock    Clock
}

func NewDetector(store DetectionStore, config DetectorConfig) (*Detector, error) {
	if store == nil || config.PageSize <= 0 || config.PageSize > maxPageSize || config.MaxPages <= 0 || config.MaxPages > maxPages {
		return nil, ErrInvalidDetector
	}
	clock := config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	return &Detector{store: store, pageSize: config.PageSize, maxPages: config.MaxPages, clock: clock}, nil
}

// RunOnce is Detector's only public behavior. It reads bounded evidence and
// appends an immutable run; it cannot execute or schedule a repair.
func (detector *Detector) RunOnce(ctx context.Context, scope DetectionScope) (DetectionReport, error) {
	if detector == nil || detector.store == nil || detector.clock == nil {
		return DetectionReport{}, ErrInvalidDetector
	}
	scope, err := normalizeDetectionScope(scope)
	if err != nil {
		return DetectionReport{}, err
	}
	startedAt := detector.clock.Now().UTC()
	report := DetectionReport{RunID: uuid.New()}
	cursor := ""
	for pageIndex := 0; pageIndex < detector.maxPages; pageIndex++ {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		page, err := detector.store.ReadDetectionPage(ctx, scope, cursor, detector.pageSize)
		if err != nil {
			return report, err
		}
		if len(page.Comparisons) > detector.pageSize {
			return report, ErrDetectionPageLimit
		}
		nextCursor := strings.TrimSpace(page.NextCursor)
		if !validCursor(nextCursor) || (!page.Done && (nextCursor == "" || nextCursor == cursor)) ||
			(page.Done && len(page.Comparisons) > 0 && (nextCursor == "" || nextCursor == cursor)) {
			return report, ErrDetectionCursor
		}
		for _, comparison := range page.Comparisons {
			normalized, err := normalizeComparison(comparison)
			if err != nil {
				return report, err
			}
			report.Findings = append(report.Findings, detect(normalized)...)
			report.Examined++
		}
		report.Pages++
		cursor = nextCursor
		if page.Done {
			report.Completed = true
			break
		}
	}
	if !report.Completed {
		report.Bounded = true
	}
	run := DetectionRun{
		ID: report.RunID, Scope: scope, StartedAt: startedAt, CompletedAt: detector.clock.Now().UTC(),
		Pages: report.Pages, Examined: report.Examined, Completed: report.Completed, Bounded: report.Bounded,
		Findings: append([]Finding(nil), report.Findings...),
	}
	if err := detector.store.AppendDetectionRun(ctx, run); err != nil {
		return report, err
	}
	report.Findings = append([]Finding(nil), report.Findings...)
	return report, nil
}

func normalizeDetectionScope(scope DetectionScope) (DetectionScope, error) {
	scope.Value = strings.TrimSpace(scope.Value)
	if !scope.Kind.Valid() || !validField(scope.Value, false) {
		return DetectionScope{}, ErrInvalidDetectionScope
	}
	return scope, nil
}

func normalizeComparison(comparison Comparison) (Comparison, error) {
	comparison.Correlation = strings.TrimSpace(comparison.Correlation)
	if !validField(comparison.Correlation, false) || !comparison.Kind.Valid() || comparison.DuplicateCount < 0 ||
		!validEvidence(comparison.Expected) || !validEvidence(comparison.Observed) {
		return Comparison{}, ErrInvalidComparison
	}
	return comparison, nil
}

func validEvidence(evidence Evidence) bool {
	if !evidence.Present {
		return evidence.AmountMinor == 0 && evidence.FeeMinor == 0 && evidence.Currency == ""
	}
	return validCurrency(evidence.Currency)
}

func detect(comparison Comparison) []Finding {
	findings := make([]Finding, 0, 8)
	add := func(reason FindingReason) {
		findings = append(findings, Finding{Correlation: comparison.Correlation, Kind: comparison.Kind, Reason: reason})
	}
	switch {
	case comparison.Expected.Present && !comparison.Observed.Present:
		add(FindingMissing)
	case !comparison.Expected.Present && comparison.Observed.Present:
		add(FindingUnexpected)
	case comparison.Expected.Present && comparison.Observed.Present:
		if comparison.Expected.AmountMinor != comparison.Observed.AmountMinor {
			add(FindingAmount)
		}
		if comparison.Expected.Currency != comparison.Observed.Currency {
			add(FindingCurrency)
		}
		if comparison.Expected.FeeMinor != comparison.Observed.FeeMinor {
			add(FindingFee)
		}
	}
	if comparison.DuplicateCount > 1 {
		add(FindingDuplicate)
	}
	if comparison.Aged {
		add(FindingAge)
	}
	if comparison.EventConflict {
		add(FindingEventConflict)
	}
	if comparison.BalanceChecked && !comparison.LedgerBalanced {
		add(FindingImbalance)
	}
	if comparison.PayoutLifecycleInvalid {
		add(FindingPayoutLifecycle)
	}
	return findings
}
