package postgres

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/webhook"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var keyringIdentityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$`)

const retainedRetiredWebhookKeys = 8

// SynchronizeKeyring durably binds configured secret identities to lifecycle
// metadata. Active writers may advance the plan; passive/recovery processes
// may only prove their independently provisioned key set matches replication.
func (repository *Repository) SynchronizeKeyring(
	ctx context.Context,
	provider, accountID string,
	desired webhook.DesiredKeyring,
) (webhook.KeyringPlan, error) {
	if repository == nil || repository.pool == nil || repository.deployment == nil || ctx == nil ||
		!keyringIdentityPattern.MatchString(provider) || !keyringIdentityPattern.MatchString(accountID) {
		return webhook.KeyringPlan{}, webhook.ErrInvalidKeyring
	}
	if repository.deployment.Role() != authority.RoleActive || !repository.deployment.WritesEnabled() {
		return repository.validateKeyring(ctx, provider, accountID, desired)
	}
	var plan webhook.KeyringPlan
	err := repository.writer.Write(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1 || ':' || $2,0))`, provider, accountID); err != nil {
			return webhook.ErrPersistence
		}
		current, err := loadKeyVersions(ctx, tx, provider, accountID, desired.AcceptedKeyIDs, true)
		if err != nil {
			return err
		}
		plan, err = webhook.PlanKeyring(current, desired)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
UPDATE public.payment_webhook_key_versions
SET state='accepted',retirement_not_before=$4,retired_at=NULL
WHERE provider=$1 AND provider_account_id=$2 AND state='primary' AND key_id<>$3`,
			provider, accountID, desired.PrimaryKeyID, desired.Now.Add(desired.Grace).UTC()); err != nil {
			return webhook.ErrPersistence
		}
		for _, version := range plan.Versions {
			if _, err := tx.Exec(ctx, `
			INSERT INTO public.payment_webhook_key_versions(
			 provider,provider_account_id,key_id,state,activated_at,material_proof,
			 retirement_not_before,retired_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT(provider,provider_account_id,key_id) DO UPDATE
SET state=EXCLUDED.state,
    retirement_not_before=EXCLUDED.retirement_not_before,
    retired_at=EXCLUDED.retired_at
			WHERE payment_webhook_key_versions.material_proof=EXCLUDED.material_proof`, provider, accountID, version.KeyID, version.State,
				version.ActivatedAt.UTC(), version.SecretProof[:], nullableTime(version.RetirementNotBefore), nullableTime(version.RetiredAt)); err != nil {
				return webhook.ErrPersistence
			}
		}
		currentByID := make(map[string]webhook.KeyVersion, len(current))
		for _, version := range current {
			currentByID[version.KeyID] = version
		}
		for _, version := range plan.Versions {
			previous, existed := currentByID[version.KeyID]
			if existed && previous.State == version.State &&
				sameOptionalTime(previous.RetirementNotBefore, version.RetirementNotBefore) &&
				sameOptionalTime(previous.RetiredAt, version.RetiredAt) {
				continue
			}
			var fromState any
			if existed {
				fromState = previous.State
			}
			actor := fmt.Sprintf("api:%s:epoch-%d", repository.deployment.Region().String(), repository.deployment.Epoch().Uint64())
			if _, err := tx.Exec(ctx, `
INSERT INTO public.payment_webhook_key_rotation_audit(
 audit_id,provider,provider_account_id,key_id,from_state,to_state,
 actor,reason,result,occurred_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'committed',$9)`, uuid.New(), provider, accountID,
				version.KeyID, fromState, version.State, actor,
				"configured_keyring", desired.Now.UTC()); err != nil {
				return webhook.ErrPersistence
			}
		}
		return archiveRetiredKeyHistory(ctx, tx, provider, accountID,
			fmt.Sprintf("api:%s:epoch-%d", repository.deployment.Region().String(), repository.deployment.Epoch().Uint64()),
			desired.Now.UTC())
	})
	if err != nil {
		return webhook.KeyringPlan{}, err
	}
	return plan, nil
}

func (repository *Repository) validateKeyring(
	ctx context.Context,
	provider, accountID string,
	desired webhook.DesiredKeyring,
) (webhook.KeyringPlan, error) {
	tx, err := repository.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return webhook.KeyringPlan{}, webhook.ErrPersistence
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	current, err := loadKeyVersions(ctx, tx, provider, accountID, desired.AcceptedKeyIDs, false)
	if err != nil {
		return webhook.KeyringPlan{}, err
	}
	plan, err := webhook.PlanKeyring(current, desired)
	if err != nil || !sameKeyVersions(current, plan.Versions) {
		return webhook.KeyringPlan{}, webhook.ErrKeyringConflict
	}
	for _, version := range current {
		if version.State == webhook.KeyAccepted && version.RetirementNotBefore != nil &&
			!desired.Now.Before(*version.RetirementNotBefore) {
			return webhook.KeyringPlan{}, webhook.ErrKeyringConflict
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return webhook.KeyringPlan{}, webhook.ErrPersistence
	}
	return plan, nil
}

// ValidateKeyring is a read-only readiness proof that this process's
// independently provisioned key IDs and secret proofs match durable metadata.
func (repository *Repository) ValidateKeyring(
	ctx context.Context,
	provider, accountID string,
	desired webhook.DesiredKeyring,
) error {
	if repository == nil || repository.pool == nil || ctx == nil ||
		!keyringIdentityPattern.MatchString(provider) || !keyringIdentityPattern.MatchString(accountID) {
		return webhook.ErrInvalidKeyring
	}
	_, err := repository.validateKeyring(ctx, provider, accountID, desired)
	return err
}

// ValidateVerifiedKey fail-closes every Stripe request against the current
// durable lifecycle so a stale process cannot outlive a demoted key's grace.
func (repository *Repository) ValidateVerifiedKey(
	ctx context.Context,
	provider, accountID, keyID string,
	verifiedAt time.Time,
) error {
	if repository == nil || repository.pool == nil || ctx == nil || verifiedAt.IsZero() ||
		!keyringIdentityPattern.MatchString(provider) || !keyringIdentityPattern.MatchString(accountID) ||
		!keyringIdentityPattern.MatchString(keyID) {
		return webhook.ErrKeyringConflict
	}
	var state string
	var retirementNotBefore pgtype.Timestamptz
	tx, err := repository.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return webhook.ErrPersistence
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := tx.QueryRow(ctx, `
SELECT state,retirement_not_before
FROM public.payment_webhook_key_versions
WHERE provider=$1 AND provider_account_id=$2 AND key_id=$3`, provider, accountID, keyID).Scan(
		&state, &retirementNotBefore,
	); err != nil {
		if err == pgx.ErrNoRows {
			return webhook.ErrKeyringConflict
		}
		return webhook.ErrPersistence
	}
	valid := false
	switch webhook.KeyState(state) {
	case webhook.KeyPrimary:
		valid = true
	case webhook.KeyAccepted:
		if !retirementNotBefore.Valid || verifiedAt.Before(retirementNotBefore.Time.UTC()) {
			valid = true
		}
	}
	if !valid {
		return webhook.ErrKeyringConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return webhook.ErrPersistence
	}
	return nil
}

type keyVersionQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func loadKeyVersions(ctx context.Context, queryer keyVersionQueryer, provider, accountID string, desiredKeyIDs []string, lock bool) ([]webhook.KeyVersion, error) {
	query := `
SELECT key_id,state,activated_at,retirement_not_before,retired_at,material_proof
FROM public.payment_webhook_key_versions
WHERE provider=$1 AND provider_account_id=$2
  AND (state<>'retired' OR key_id=ANY($3::text[]))
ORDER BY key_id`
	if lock {
		query += " FOR UPDATE"
	}
	rows, err := queryer.Query(ctx, query, provider, accountID, desiredKeyIDs)
	if err != nil {
		return nil, webhook.ErrPersistence
	}
	defer rows.Close()
	versions := make([]webhook.KeyVersion, 0, 2)
	for rows.Next() {
		var (
			version                      webhook.KeyVersion
			state                        string
			retirementNotBefore, retired pgtype.Timestamptz
			secretProof                  []byte
		)
		if err := rows.Scan(&version.KeyID, &state, &version.ActivatedAt, &retirementNotBefore, &retired, &secretProof); err != nil || len(secretProof) != len(version.SecretProof) {
			return nil, webhook.ErrPersistence
		}
		copy(version.SecretProof[:], secretProof)
		version.State = webhook.KeyState(state)
		if retirementNotBefore.Valid {
			value := retirementNotBefore.Time.UTC()
			version.RetirementNotBefore = &value
		}
		if retired.Valid {
			value := retired.Time.UTC()
			version.RetiredAt = &value
		}
		versions = append(versions, version)
		if len(versions) > 4 {
			return nil, webhook.ErrKeyringConflict
		}
	}
	if rows.Err() != nil {
		return nil, webhook.ErrPersistence
	}
	return versions, nil
}

func archiveRetiredKeyHistory(ctx context.Context, tx pgx.Tx, provider, accountID, actor string, archivedAt time.Time) error {
	if _, err := tx.Exec(ctx, `
INSERT INTO public.payment_webhook_key_version_archive(
 provider,provider_account_id,key_id,state,activated_at,material_proof,
 retirement_not_before,retired_at,created_at,updated_at,archived_at,archived_by
)
SELECT version.provider,version.provider_account_id,version.key_id,version.state,
       version.activated_at,version.material_proof,version.retirement_not_before,
       version.retired_at,version.created_at,version.updated_at,$4,$5
FROM public.payment_webhook_key_versions AS version
WHERE version.provider=$1 AND version.provider_account_id=$2 AND version.state='retired'
  AND version.key_id NOT IN (
      SELECT recent.key_id
      FROM public.payment_webhook_key_versions AS recent
      WHERE recent.provider=$1 AND recent.provider_account_id=$2 AND recent.state='retired'
      ORDER BY recent.retired_at DESC,recent.key_id DESC
      LIMIT $3
  )
ON CONFLICT(provider,provider_account_id,key_id) DO NOTHING`, provider, accountID,
		retainedRetiredWebhookKeys, archivedAt, actor); err != nil {
		return webhook.ErrPersistence
	}
	if _, err := tx.Exec(ctx, `
DELETE FROM public.payment_webhook_key_versions AS version
WHERE version.provider=$1 AND version.provider_account_id=$2 AND version.state='retired'
  AND version.key_id NOT IN (
      SELECT recent.key_id
      FROM public.payment_webhook_key_versions AS recent
      WHERE recent.provider=$1 AND recent.provider_account_id=$2 AND recent.state='retired'
      ORDER BY recent.retired_at DESC,recent.key_id DESC
      LIMIT $3
  )
  AND EXISTS (
      SELECT 1 FROM public.payment_webhook_key_version_archive AS archive
      WHERE archive.provider=version.provider
        AND archive.provider_account_id=version.provider_account_id
        AND archive.key_id=version.key_id
        AND archive.state=version.state
        AND archive.activated_at=version.activated_at
        AND archive.material_proof=version.material_proof
        AND archive.retirement_not_before IS NOT DISTINCT FROM version.retirement_not_before
        AND archive.retired_at=version.retired_at
  )`, provider, accountID, retainedRetiredWebhookKeys); err != nil {
		return webhook.ErrPersistence
	}
	return nil
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return value.UTC()
}

func sameKeyVersions(left, right []webhook.KeyVersion) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].KeyID != right[index].KeyID || left[index].State != right[index].State ||
			!left[index].ActivatedAt.Equal(right[index].ActivatedAt) ||
			!sameOptionalTime(left[index].RetirementNotBefore, right[index].RetirementNotBefore) ||
			!sameOptionalTime(left[index].RetiredAt, right[index].RetiredAt) {
			return false
		}
	}
	return true
}

func sameOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}
