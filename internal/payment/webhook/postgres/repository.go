// Package postgres persists verified provider events in the control-plane
// webhook inbox without retaining request bodies or authentication material.
package postgres

import (
	"bytes"
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/webhook"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	authoritypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Repository struct {
	pool       authoritypostgres.Beginner
	writer     *authoritypostgres.ControlWriter
	deployment *authority.Deployment
}

type repositoryOptions struct {
	deployment *authority.Deployment
}

type Option func(*repositoryOptions)

// WithRegionalAuthority binds every control-plane mutation to immutable
// process region, role, epoch, and writes-enabled configuration.
func WithRegionalAuthority(deployment authority.Deployment) Option {
	return func(options *repositoryOptions) {
		options.deployment = &deployment
	}
}

func NewRepository(pool authoritypostgres.Beginner, options ...Option) (*Repository, error) {
	if pool == nil {
		return nil, webhook.ErrInvalidConfiguration
	}
	configured := repositoryOptions{}
	for _, apply := range options {
		if apply == nil {
			return nil, webhook.ErrInvalidConfiguration
		}
		apply(&configured)
	}
	repository := &Repository{pool: pool}
	if configured.deployment == nil {
		return repository, nil
	}
	if _, err := authority.NewDeployment(
		configured.deployment.Region(),
		configured.deployment.Role(),
		configured.deployment.Epoch(),
		configured.deployment.WritesEnabled(),
	); err != nil {
		return nil, webhook.ErrInvalidConfiguration
	}
	writer, err := authoritypostgres.NewControlWriter(
		pool,
		*configured.deployment,
		pgx.TxOptions{IsoLevel: pgx.ReadCommitted},
	)
	if err != nil {
		return nil, webhook.ErrInvalidConfiguration
	}
	repository.writer = writer
	deployment := *configured.deployment
	repository.deployment = &deployment
	return repository, nil
}

// StoreVerified serializes on the provider event identity. The first digest is
// canonical, an identical digest is a replay, and a changed digest is durable
// conflict evidence. All outcomes commit in the same control transaction.
func (repository *Repository) StoreVerified(ctx context.Context, record webhook.Record) (webhook.StoreResult, error) {
	if repository == nil || repository.pool == nil || repository.writer == nil || ctx == nil {
		return "", webhook.ErrPersistence
	}
	var result webhook.StoreResult
	err := repository.writer.Write(ctx, func(tx pgx.Tx) error {
		var inserted bool
		err := tx.QueryRow(ctx, `
INSERT INTO public.payment_webhook_inbox(
 inbox_id,provider,provider_event_id,provider_account_id,provider_environment,
 event_type,provider_payment_id,
 payload_hash,verified_key_id,event_created_at,signature_verified_at,
 received_at,body_size_bytes
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
ON CONFLICT(provider,provider_event_id) DO NOTHING
RETURNING true`, record.ID, record.Provider, record.ProviderEventID,
			nullableString(record.ProviderAccountID), nullableString(record.ProviderEnvironment),
			record.EventType, nullableString(record.ProviderPaymentID), record.PayloadHash[:], record.VerifiedKeyID,
			record.EventCreatedAt, record.SignatureVerifiedAt, record.ReceivedAt,
			record.BodySizeBytes).Scan(&inserted)
		if err == nil && inserted {
			if record.Ignored {
				tag, updateErr := tx.Exec(ctx, `
UPDATE public.payment_webhook_inbox
SET state='ignored',processed_at=$3
WHERE provider=$1 AND provider_event_id=$2 AND state='received'`,
					record.Provider, record.ProviderEventID, record.ReceivedAt)
				if updateErr != nil || tag.RowsAffected() != 1 {
					return webhook.ErrPersistence
				}
			}
			result = webhook.StoreAccepted
			return nil
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return webhook.ErrPersistence
		}

		var canonicalHash []byte
		var canonicalState string
		if err := tx.QueryRow(ctx, `
SELECT payload_hash,state
FROM public.payment_webhook_inbox
WHERE provider=$1 AND provider_event_id=$2
FOR UPDATE`, record.Provider, record.ProviderEventID).Scan(&canonicalHash, &canonicalState); err != nil {
			return webhook.ErrPersistence
		}
		if bytes.Equal(canonicalHash, record.PayloadHash[:]) {
			result = webhook.StoreDuplicate
			return nil
		}

		_, err = tx.Exec(ctx, `
INSERT INTO public.payment_provider_event_conflicts(
 conflict_id,provider,provider_event_id,canonical_payload_hash,
 conflicting_payload_hash,first_detected_at,last_detected_at
) VALUES($1,$2,$3,$4,$5,$6,$6)
ON CONFLICT(provider,provider_event_id,conflicting_payload_hash) DO UPDATE
SET occurrence_count=LEAST(
      payment_provider_event_conflicts.occurrence_count+1,1000000
    ),
    last_detected_at=GREATEST(
      payment_provider_event_conflicts.last_detected_at,EXCLUDED.last_detected_at
    )`, uuid.New(), record.Provider, record.ProviderEventID, canonicalHash,
			record.PayloadHash[:], record.ReceivedAt)
		if err != nil {
			return webhook.ErrPersistence
		}
		if webhookConflictQuarantinable(canonicalState) {
			tag, updateErr := tx.Exec(ctx, `
UPDATE public.payment_webhook_inbox
SET state='security_conflict',bounded_error_category='payload_hash_conflict',
	    lease_owner=NULL,lease_until=NULL,processed_at=$3
WHERE provider=$1 AND provider_event_id=$2 AND state=$4`,
				record.Provider, record.ProviderEventID, record.ReceivedAt, canonicalState)
			if updateErr != nil || tag.RowsAffected() != 1 {
				return webhook.ErrPersistence
			}
		}
		result = webhook.StoreConflict
		return nil
	})
	if err != nil {
		if errors.Is(err, authority.ErrRoleNotActive) || errors.Is(err, authority.ErrWritesDisabled) ||
			errors.Is(err, authority.ErrRegionMismatch) || errors.Is(err, authority.ErrEpochMismatch) ||
			errors.Is(err, authority.ErrAuthorityNotActive) {
			return "", err
		}
		return "", webhook.ErrPersistence
	}
	return result, nil
}

func webhookConflictQuarantinable(state string) bool {
	return state == "received" || state == "processing" || state == "failed_retryable"
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

var _ webhook.Repository = (*Repository)(nil)
