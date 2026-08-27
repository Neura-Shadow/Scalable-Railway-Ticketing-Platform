// Package settlement imports normalized provider financial evidence and
// performs detect-only reconciliation. It has no booking or repair capability.
package settlement

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"
)

const (
	maxFieldBytes  = 200
	maxCursorBytes = 1024
	maxPageSize    = 1000
	maxPages       = 100
)

var (
	ErrInvalidImporter    = errors.New("invalid settlement importer")
	ErrInvalidScope       = errors.New("invalid settlement account scope")
	ErrInvalidRecord      = errors.New("invalid settlement record")
	ErrPageLimit          = errors.New("settlement page exceeds configured limit")
	ErrCursorStalled      = errors.New("settlement cursor did not advance")
	ErrCheckpointConflict = errors.New("settlement checkpoint conflict")
	ErrPayloadConflict    = errors.New("settlement payload conflict")
)

type AccountScope struct {
	Provider  string
	AccountID string
}

type RecordKind string

const (
	RecordBalanceTransaction RecordKind = "balance_transaction"
	RecordSettlementBatch    RecordKind = "settlement_batch"
	RecordSettlementLine     RecordKind = "settlement_line"
	RecordPayout             RecordKind = "payout"
	RecordPayoutLine         RecordKind = "payout_line"
)

func (kind RecordKind) Valid() bool {
	switch kind {
	case RecordBalanceTransaction, RecordSettlementBatch, RecordSettlementLine, RecordPayout, RecordPayoutLine:
		return true
	default:
		return false
	}
}

type Operation string

const (
	OperationCapture    Operation = "capture"
	OperationRefund     Operation = "refund"
	OperationFee        Operation = "fee"
	OperationSettlement Operation = "settlement"
	OperationPayout     Operation = "payout"
)

func (operation Operation) Valid() bool {
	switch operation {
	case OperationCapture, OperationRefund, OperationFee, OperationSettlement, OperationPayout:
		return true
	default:
		return false
	}
}

// Record is normalized evidence only. Raw provider reports and credentials are
// intentionally absent from this type.
type Record struct {
	Kind               RecordKind
	ProviderID         string
	PaymentCorrelation string
	Operation          Operation
	GrossMinor         int64
	FeeMinor           int64
	NetMinor           int64
	Currency           string
	AvailableAt        time.Time
	CreatedAt          time.Time
	SettlementID       string
	PayoutID           string
	PayoutStatus       string
}

type PayloadHash [sha256.Size]byte

type ImportedRecord struct {
	Record
	PayloadHash PayloadHash
	ImportedAt  time.Time
}

type Page struct {
	Records    []Record
	NextCursor string
	Done       bool
}

// Source is the provider boundary. ListPage must perform its network work
// outside any ImportStore transaction.
type Source interface {
	ListPage(context.Context, AccountScope, string, int) (Page, error)
}

type PageCommit struct {
	Scope          AccountScope
	ExpectedCursor string
	NextCursor     string
	Records        []ImportedRecord
}

type CommitResult struct {
	Inserted  int
	Replayed  int
	Conflicts int
}

// ImportStore commits a page and its cursor atomically. Changed content under
// one provider identity is retained as conflict evidence and never overwrites
// the first record.
type ImportStore interface {
	Checkpoint(context.Context, AccountScope) (string, error)
	CommitPage(context.Context, PageCommit) (CommitResult, error)
}

type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

type ImporterConfig struct {
	PageSize int
	MaxPages int
	Clock    Clock
}

type ImportReport struct {
	Pages       int
	Examined    int
	Inserted    int
	Replayed    int
	Conflicts   int
	StartCursor string
	EndCursor   string
	Completed   bool
	Bounded     bool
}

type Importer struct {
	source   Source
	store    ImportStore
	pageSize int
	maxPages int
	clock    Clock
}

func NewImporter(source Source, store ImportStore, config ImporterConfig) (*Importer, error) {
	if source == nil || store == nil || config.PageSize <= 0 || config.PageSize > maxPageSize || config.MaxPages <= 0 || config.MaxPages > maxPages {
		return nil, ErrInvalidImporter
	}
	clock := config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	return &Importer{source: source, store: store, pageSize: config.PageSize, maxPages: config.MaxPages, clock: clock}, nil
}

func (importer *Importer) RunOnce(ctx context.Context, scope AccountScope) (ImportReport, error) {
	if importer == nil || importer.source == nil || importer.store == nil || importer.clock == nil {
		return ImportReport{}, ErrInvalidImporter
	}
	scope, err := normalizeScope(scope)
	if err != nil {
		return ImportReport{}, err
	}
	cursor, err := importer.store.Checkpoint(ctx, scope)
	if err != nil {
		return ImportReport{}, err
	}
	if !validCursor(cursor) {
		return ImportReport{}, ErrCheckpointConflict
	}
	report := ImportReport{StartCursor: cursor, EndCursor: cursor}
	for pageIndex := 0; pageIndex < importer.maxPages; pageIndex++ {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		page, err := importer.source.ListPage(ctx, scope, cursor, importer.pageSize)
		if err != nil {
			return report, err
		}
		if len(page.Records) > importer.pageSize {
			return report, ErrPageLimit
		}
		nextCursor := strings.TrimSpace(page.NextCursor)
		if !validCursor(nextCursor) {
			return report, ErrCursorStalled
		}
		if !page.Done && (nextCursor == "" || nextCursor == cursor) {
			return report, ErrCursorStalled
		}
		if page.Done && len(page.Records) > 0 && (nextCursor == "" || nextCursor == cursor) {
			return report, ErrCursorStalled
		}
		if page.Done && len(page.Records) == 0 && nextCursor == "" {
			nextCursor = cursor
		}
		imported := make([]ImportedRecord, len(page.Records))
		importedAt := importer.clock.Now().UTC()
		for index, record := range page.Records {
			normalized, err := normalizeRecord(record)
			if err != nil {
				return report, err
			}
			hash, err := recordHash(normalized)
			if err != nil {
				return report, errors.Join(ErrInvalidRecord, err)
			}
			imported[index] = ImportedRecord{Record: normalized, PayloadHash: hash, ImportedAt: importedAt}
		}
		result, err := importer.store.CommitPage(ctx, PageCommit{
			Scope: scope, ExpectedCursor: cursor, NextCursor: nextCursor, Records: imported,
		})
		if err != nil {
			return report, err
		}
		report.Pages++
		report.Examined += len(imported)
		report.Inserted += result.Inserted
		report.Replayed += result.Replayed
		report.Conflicts += result.Conflicts
		cursor = nextCursor
		report.EndCursor = cursor
		if page.Done {
			report.Completed = true
			break
		}
	}
	if !report.Completed {
		report.Bounded = true
	}
	if report.Conflicts > 0 {
		return report, ErrPayloadConflict
	}
	return report, nil
}

func normalizeScope(scope AccountScope) (AccountScope, error) {
	scope.Provider = strings.TrimSpace(scope.Provider)
	scope.AccountID = strings.TrimSpace(scope.AccountID)
	if !validField(scope.Provider, false) || !validField(scope.AccountID, false) {
		return AccountScope{}, ErrInvalidScope
	}
	return scope, nil
}

func normalizeRecord(record Record) (Record, error) {
	record.ProviderID = strings.TrimSpace(record.ProviderID)
	record.PaymentCorrelation = strings.TrimSpace(record.PaymentCorrelation)
	record.SettlementID = strings.TrimSpace(record.SettlementID)
	record.PayoutID = strings.TrimSpace(record.PayoutID)
	record.PayoutStatus = strings.TrimSpace(record.PayoutStatus)
	if !record.Kind.Valid() || !record.Operation.Valid() || !validField(record.ProviderID, false) ||
		!validField(record.PaymentCorrelation, true) || !validField(record.SettlementID, true) ||
		!validField(record.PayoutID, true) || !validPayoutStatus(record.PayoutStatus) ||
		!validCurrency(record.Currency) || record.CreatedAt.IsZero() || !validProviderArithmetic(record) {
		return Record{}, ErrInvalidRecord
	}
	if record.Kind == RecordPayout && record.PayoutStatus == "" {
		return Record{}, ErrInvalidRecord
	}
	record.CreatedAt = record.CreatedAt.UTC()
	if !record.AvailableAt.IsZero() {
		record.AvailableAt = record.AvailableAt.UTC()
	}
	return record, nil
}

// Provider money uses signed gross, non-negative fee, and signed
// net=gross-fee. MinInt64 is excluded because reconciliation needs a checked
// absolute magnitude for signed refund evidence.
func validProviderArithmetic(record Record) bool {
	return record.FeeMinor >= 0 && record.GrossMinor != math.MinInt64 && record.NetMinor != math.MinInt64 &&
		record.GrossMinor >= math.MinInt64+record.FeeMinor && record.GrossMinor-record.FeeMinor == record.NetMinor
}

func validPayoutStatus(status string) bool {
	if status == "" {
		return true
	}
	if len(status) > 64 || status[0] < 'a' || status[0] > 'z' {
		return false
	}
	for index := 1; index < len(status); index++ {
		value := status[index]
		if (value < 'a' || value > 'z') && (value < '0' || value > '9') && value != '_' {
			return false
		}
	}
	return true
}

func recordHash(record Record) (PayloadHash, error) {
	encoded, err := json.Marshal(record)
	if err != nil {
		return PayloadHash{}, err
	}
	return sha256.Sum256(encoded), nil
}

func validField(value string, optional bool) bool {
	if value == "" {
		return optional
	}
	return len(value) <= maxFieldBytes
}

func validCursor(cursor string) bool { return len(cursor) <= maxCursorBytes }

func validCurrency(currency string) bool {
	if len(currency) != 3 {
		return false
	}
	for index := range currency {
		if currency[index] < 'A' || currency[index] > 'Z' {
			return false
		}
	}
	return true
}
