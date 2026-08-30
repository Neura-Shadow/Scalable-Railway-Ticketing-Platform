package postgres

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/settlement"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	minimumImportLease = time.Second
	maximumImportLease = 15 * time.Minute
	maximumImportDelay = 24 * time.Hour
)

var leaseOwnerPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

const ensureImportLeaseCheckpointSQL = `
INSERT INTO public.provider_settlement_import_checkpoints(
 provider,provider_account_id,cursor,next_attempt_at,updated_at
) VALUES($1,$2,'',clock_timestamp(),clock_timestamp())
ON CONFLICT(provider,provider_account_id) DO NOTHING`

const claimImportLeaseSQL = `
UPDATE public.provider_settlement_import_checkpoints
SET lease_owner=$3,lease_token=$4,
    lease_until=clock_timestamp()+make_interval(secs => $5),
    updated_at=clock_timestamp()
WHERE provider=$1 AND provider_account_id=$2
  AND next_attempt_at <= clock_timestamp()
  AND (lease_until IS NULL OR lease_until <= clock_timestamp())
RETURNING cursor,lease_until`

const finishImportLeaseSQL = `
UPDATE public.provider_settlement_import_checkpoints
SET lease_owner=NULL,lease_token=NULL,lease_until=NULL,
    next_attempt_at=clock_timestamp()+make_interval(secs => $5),updated_at=clock_timestamp()
WHERE provider=$1 AND provider_account_id=$2
  AND lease_owner=$3 AND lease_token=$4`

const lockClaimedCheckpointSQL = `
SELECT cursor FROM public.provider_settlement_import_checkpoints
WHERE provider=$1 AND provider_account_id=$2
  AND lease_owner=$3 AND lease_token=$4
  AND lease_until > clock_timestamp()
FOR UPDATE`

const updateClaimedCheckpointSQL = `
UPDATE public.provider_settlement_import_checkpoints
SET cursor=$5,updated_at=clock_timestamp()
WHERE provider=$1 AND provider_account_id=$2
  AND lease_owner=$3 AND lease_token=$4 AND cursor=$6
  AND lease_until > clock_timestamp()`

type claimedStore struct {
	parent *Store
	lease  settlement.ImportLease
}

func (store *Store) ClaimDue(
	ctx context.Context,
	scope settlement.AccountScope,
	owner string,
	now time.Time,
	duration time.Duration,
) (settlement.ImportStore, settlement.ImportLease, bool, error) {
	if store == nil || store.writer == nil || !validScope(scope) || !leaseOwnerPattern.MatchString(owner) ||
		now.IsZero() || duration < minimumImportLease || duration > maximumImportLease {
		return nil, settlement.ImportLease{}, false, settlement.ErrInvalidImportLease
	}
	lease := settlement.ImportLease{Scope: scope, Owner: owner, Token: uuid.New()}
	claimed := false
	err := store.writer.Write(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, ensureImportLeaseCheckpointSQL, scope.Provider, scope.AccountID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, claimImportLeaseSQL,
			scope.Provider, scope.AccountID, owner, lease.Token, int(duration/time.Second),
		).Scan(&lease.Cursor, &lease.LeaseUntil); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return err
		}
		claimed = true
		return nil
	})
	if err != nil || !claimed {
		return nil, settlement.ImportLease{}, false, err
	}
	return &claimedStore{parent: store, lease: lease}, lease, true, nil
}

func (store *Store) FinishLease(ctx context.Context, lease settlement.ImportLease, nextDelay time.Duration) error {
	if store == nil || store.writer == nil || !validLease(lease) || nextDelay < 0 || nextDelay > maximumImportDelay {
		return settlement.ErrInvalidImportLease
	}
	return store.writer.Write(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, finishImportLeaseSQL,
			lease.Scope.Provider, lease.Scope.AccountID, lease.Owner, lease.Token, int(nextDelay/time.Second),
		)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return settlement.ErrImportLeaseLost
		}
		return nil
	})
}

func (store *claimedStore) Checkpoint(ctx context.Context, scope settlement.AccountScope) (string, error) {
	if store == nil || store.parent == nil || scope != store.lease.Scope || !validLease(store.lease) {
		return "", settlement.ErrInvalidImportLease
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return store.lease.Cursor, nil
}

func (store *claimedStore) CommitPage(ctx context.Context, commit settlement.PageCommit) (settlement.CommitResult, error) {
	if store == nil || store.parent == nil || commit.Scope != store.lease.Scope || !validLease(store.lease) {
		return settlement.CommitResult{}, settlement.ErrInvalidImportLease
	}
	for _, record := range commit.Records {
		if !validRecord(record) {
			return settlement.CommitResult{}, settlement.ErrInvalidRecord
		}
	}
	var result settlement.CommitResult
	err := store.parent.writer.Write(ctx, func(tx pgx.Tx) error {
		var current string
		if err := tx.QueryRow(ctx, lockClaimedCheckpointSQL,
			store.lease.Scope.Provider, store.lease.Scope.AccountID, store.lease.Owner, store.lease.Token,
		).Scan(&current); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return settlement.ErrImportLeaseLost
			}
			return err
		}
		if current != commit.ExpectedCursor {
			return settlement.ErrCheckpointConflict
		}
		var err error
		result, err = commitClaimedPage(ctx, tx, commit, store.lease)
		return err
	})
	if err == nil {
		store.lease.Cursor = commit.NextCursor
	}
	return result, err
}

func commitClaimedPage(ctx context.Context, tx pgx.Tx, commit settlement.PageCommit, lease settlement.ImportLease) (settlement.CommitResult, error) {
	result, err := commitRecords(ctx, tx, commit)
	if err != nil {
		return settlement.CommitResult{}, err
	}
	tag, err := tx.Exec(ctx, updateClaimedCheckpointSQL,
		commit.Scope.Provider, commit.Scope.AccountID, lease.Owner, lease.Token,
		commit.NextCursor, commit.ExpectedCursor,
	)
	if err != nil {
		return settlement.CommitResult{}, err
	}
	if tag.RowsAffected() != 1 {
		return settlement.CommitResult{}, settlement.ErrImportLeaseLost
	}
	return result, nil
}

func validLease(lease settlement.ImportLease) bool {
	return validScope(lease.Scope) && leaseOwnerPattern.MatchString(lease.Owner) &&
		lease.Token != uuid.Nil && !lease.LeaseUntil.IsZero() && len(lease.Cursor) <= 1024
}

var _ settlement.ImportLeaser = (*Store)(nil)
var _ settlement.ImportStore = (*claimedStore)(nil)
