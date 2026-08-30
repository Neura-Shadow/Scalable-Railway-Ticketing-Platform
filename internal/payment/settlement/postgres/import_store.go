// Package postgres persists normalized settlement evidence and detect-only
// reconciliation results in PostgreSQL.
package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	authoritypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var ErrInvalidStore = errors.New("invalid settlement postgres store")

var (
	providerPattern     = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	identityPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$`)
	payoutStatusPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Store struct {
	db     DB
	writer *authoritypostgres.ControlWriter
}

type storeOptions struct {
	deployment *authority.Deployment
}

type Option func(*storeOptions)

func WithRegionalAuthority(deployment authority.Deployment) Option {
	return func(options *storeOptions) {
		options.deployment = &deployment
	}
}

func New(db DB, options ...Option) (*Store, error) {
	if db == nil {
		return nil, ErrInvalidStore
	}
	configured := storeOptions{}
	for _, apply := range options {
		if apply == nil {
			return nil, ErrInvalidStore
		}
		apply(&configured)
	}
	store := &Store{db: db}
	if configured.deployment == nil {
		return store, nil
	}
	writer, err := authoritypostgres.NewControlWriter(
		db,
		*configured.deployment,
		pgx.TxOptions{IsoLevel: pgx.Serializable},
	)
	if err != nil {
		return nil, ErrInvalidStore
	}
	store.writer = writer
	return store, nil
}

const checkpointSQL = `
SELECT COALESCE((
 SELECT cursor FROM public.provider_settlement_import_checkpoints
 WHERE provider=$1 AND provider_account_id=$2
),'')`

const ensureCheckpointSQL = `
INSERT INTO public.provider_settlement_import_checkpoints(
 provider,provider_account_id,cursor,updated_at
) VALUES($1,$2,'',clock_timestamp())
ON CONFLICT(provider,provider_account_id) DO NOTHING`

const lockCheckpointSQL = `
SELECT cursor FROM public.provider_settlement_import_checkpoints
WHERE provider=$1 AND provider_account_id=$2 AND lease_token IS NULL FOR UPDATE`

const updateCheckpointSQL = `
UPDATE public.provider_settlement_import_checkpoints
SET cursor=$3,updated_at=clock_timestamp()
WHERE provider=$1 AND provider_account_id=$2 AND cursor=$4 AND lease_token IS NULL`

const insertConflictSQL = `
INSERT INTO public.provider_settlement_import_conflicts(
 conflict_id,provider,provider_account_id,record_kind,provider_record_id,
 stored_hash,incoming_hash,detected_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT(provider,provider_account_id,record_kind,provider_record_id,incoming_hash)
DO NOTHING`

var evidenceTables = map[settlement.RecordKind]string{
	settlement.RecordBalanceTransaction: "provider_balance_transactions",
	settlement.RecordSettlementBatch:    "provider_settlement_batches",
	settlement.RecordSettlementLine:     "provider_settlement_lines",
	settlement.RecordPayout:             "provider_payouts",
	settlement.RecordPayoutLine:         "provider_payout_lines",
}

func (store *Store) Checkpoint(ctx context.Context, scope settlement.AccountScope) (string, error) {
	if store == nil || store.db == nil || !validScope(scope) {
		return "", settlement.ErrInvalidScope
	}
	var cursor string
	if err := store.db.QueryRow(ctx, checkpointSQL, scope.Provider, scope.AccountID).Scan(&cursor); err != nil {
		return "", err
	}
	if len(cursor) > 1024 {
		return "", settlement.ErrCheckpointConflict
	}
	return cursor, nil
}

func (store *Store) CommitPage(ctx context.Context, commit settlement.PageCommit) (settlement.CommitResult, error) {
	if store == nil || store.db == nil || !validScope(commit.Scope) || len(commit.ExpectedCursor) > 1024 || len(commit.NextCursor) > 1024 {
		return settlement.CommitResult{}, ErrInvalidStore
	}
	for _, record := range commit.Records {
		if !validRecord(record) {
			return settlement.CommitResult{}, settlement.ErrInvalidRecord
		}
	}
	if store.writer == nil {
		return settlement.CommitResult{}, ErrInvalidStore
	}
	var result settlement.CommitResult
	err := store.writer.Write(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = commitPage(ctx, tx, commit)
		return err
	})
	return result, err
}

func commitPage(ctx context.Context, tx pgx.Tx, commit settlement.PageCommit) (settlement.CommitResult, error) {
	if _, err := tx.Exec(ctx, ensureCheckpointSQL, commit.Scope.Provider, commit.Scope.AccountID); err != nil {
		return settlement.CommitResult{}, err
	}
	var current string
	if err := tx.QueryRow(ctx, lockCheckpointSQL, commit.Scope.Provider, commit.Scope.AccountID).Scan(&current); err != nil {
		return settlement.CommitResult{}, err
	}
	if current != commit.ExpectedCursor {
		return settlement.CommitResult{}, settlement.ErrCheckpointConflict
	}
	result, err := commitRecords(ctx, tx, commit)
	if err != nil {
		return settlement.CommitResult{}, err
	}
	tag, err := tx.Exec(ctx, updateCheckpointSQL,
		commit.Scope.Provider, commit.Scope.AccountID, commit.NextCursor, commit.ExpectedCursor,
	)
	if err != nil {
		return settlement.CommitResult{}, err
	}
	if tag.RowsAffected() != 1 {
		return settlement.CommitResult{}, settlement.ErrCheckpointConflict
	}
	return result, nil
}

func commitRecords(ctx context.Context, tx pgx.Tx, commit settlement.PageCommit) (settlement.CommitResult, error) {
	result := settlement.CommitResult{}
	for _, record := range commit.Records {
		table := evidenceTables[record.Kind]
		insertSQL := evidenceInsertSQL(table)
		tag, err := tx.Exec(ctx, insertSQL, evidenceArgs(commit.Scope, record)...)
		if err != nil {
			return settlement.CommitResult{}, err
		}
		if tag.RowsAffected() == 1 {
			if err := appendSettlementLedger(ctx, tx, commit.Scope, record); err != nil {
				return settlement.CommitResult{}, err
			}
			result.Inserted++
			continue
		}
		var storedHash []byte
		hashArguments := []any{commit.Scope.Provider, commit.Scope.AccountID, record.ProviderID}
		if record.Kind == settlement.RecordPayout {
			hashArguments = append(hashArguments, record.PayoutStatus)
		}
		if err := tx.QueryRow(ctx, evidenceHashSQL(table), hashArguments...).Scan(&storedHash); err != nil {
			return settlement.CommitResult{}, err
		}
		if bytes.Equal(storedHash, record.PayloadHash[:]) {
			result.Replayed++
			continue
		}
		if _, err := tx.Exec(ctx, insertConflictSQL,
			uuid.New(), commit.Scope.Provider, commit.Scope.AccountID, record.Kind,
			record.ProviderID, storedHash, record.PayloadHash[:], record.ImportedAt.UTC(),
		); err != nil {
			return settlement.CommitResult{}, err
		}
		result.Conflicts++
	}
	return result, nil
}

func evidenceInsertSQL(table string) string {
	conflictTarget := "provider,provider_account_id,provider_record_id"
	if table == "provider_payouts" {
		conflictTarget += ",payout_status"
	}
	return fmt.Sprintf(`
INSERT INTO public.%s(
 provider,provider_account_id,provider_record_id,payment_correlation,
 operation_type,gross_minor,fee_minor,net_minor,currency,available_at,
 provider_created_at,provider_settlement_id,provider_payout_id,payout_status,
 payload_hash,imported_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
ON CONFLICT(%s) DO NOTHING`, table, conflictTarget)
}

func evidenceHashSQL(table string) string {
	if table == "provider_payouts" {
		return `SELECT payload_hash FROM public.provider_payouts
WHERE provider=$1 AND provider_account_id=$2 AND provider_record_id=$3 AND payout_status=$4`
	}
	return fmt.Sprintf(`SELECT payload_hash FROM public.%s
WHERE provider=$1 AND provider_account_id=$2 AND provider_record_id=$3`, table)
}

func evidenceArgs(scope settlement.AccountScope, imported settlement.ImportedRecord) []any {
	payoutID := imported.PayoutID
	if imported.Kind == settlement.RecordPayout && payoutID == "" {
		payoutID = imported.ProviderID
	}
	return []any{
		scope.Provider, scope.AccountID, imported.ProviderID, nullableString(imported.PaymentCorrelation),
		imported.Operation, imported.GrossMinor, imported.FeeMinor, imported.NetMinor, imported.Currency,
		nullableTime(imported.AvailableAt), imported.CreatedAt.UTC(), nullableString(imported.SettlementID),
		nullableString(payoutID), nullableString(imported.PayoutStatus), imported.PayloadHash[:], imported.ImportedAt.UTC(),
	}
}

// Payout reports are mutable snapshots at the provider. Persist each bounded
// status as an immutable lifecycle event, while keeping payout_id as the
// canonical provider identity used by detection and operator scopes.
func settlementEvidenceID(imported settlement.ImportedRecord) string {
	if imported.Kind == settlement.RecordPayout && imported.PayoutStatus != "" {
		return imported.ProviderID + ":" + imported.PayoutStatus
	}
	return imported.ProviderID
}

func validScope(scope settlement.AccountScope) bool {
	return providerPattern.MatchString(scope.Provider) && identityPattern.MatchString(scope.AccountID)
}

// The normalized sign convention is signed gross, non-negative fee, and
// signed net=gross-fee. MinInt64 is rejected because absolute-value
// reconciliation cannot represent it safely in bigint.
func validRecord(imported settlement.ImportedRecord) bool {
	if _, ok := evidenceTables[imported.Kind]; !ok {
		return false
	}
	if imported.Kind == settlement.RecordPayout && imported.PayoutStatus == "" {
		return false
	}
	if !identityPattern.MatchString(imported.ProviderID) || !imported.Operation.Valid() ||
		!validOptionalIdentity(imported.PaymentCorrelation) || !validOptionalIdentity(imported.SettlementID) ||
		!validOptionalIdentity(imported.PayoutID) || !validPayoutStatus(imported.PayoutStatus) ||
		!validCurrency(imported.Currency) || imported.CreatedAt.IsZero() || imported.ImportedAt.IsZero() ||
		imported.FeeMinor < 0 || imported.GrossMinor == math.MinInt64 || imported.NetMinor == math.MinInt64 ||
		imported.GrossMinor < math.MinInt64+imported.FeeMinor || imported.GrossMinor-imported.FeeMinor != imported.NetMinor {
		return false
	}
	var zero settlement.PayloadHash
	return imported.PayloadHash != zero
}

func validPayoutStatus(value string) bool {
	return value == "" || payoutStatusPattern.MatchString(value)
}

func validOptionalIdentity(value string) bool {
	return value == "" || identityPattern.MatchString(value)
}

func validCurrency(currency string) bool {
	if len(currency) != 3 {
		return false
	}
	for _, value := range currency {
		if value < 'A' || value > 'Z' {
			return false
		}
	}
	return true
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

var _ settlement.ImportStore = (*Store)(nil)
