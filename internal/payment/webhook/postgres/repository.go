// Package postgres persists verified provider events in the control-plane
// webhook inbox without retaining request bodies or authentication material.
package postgres

import (
	"bytes"
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/webhook"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) (*Repository, error) {
	if pool == nil {
		return nil, webhook.ErrInvalidConfiguration
	}
	return &Repository{pool: pool}, nil
}

// StoreVerified serializes on the provider event identity. The first digest is
// canonical, an identical digest is a replay, and a changed digest is durable
// conflict evidence. All outcomes commit in the same control transaction.
func (repository *Repository) StoreVerified(ctx context.Context, record webhook.Record) (webhook.StoreResult, error) {
	if repository == nil || repository.pool == nil || ctx == nil {
		return "", webhook.ErrPersistence
	}
	tx, err := repository.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", webhook.ErrPersistence
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var inserted bool
	err = tx.QueryRow(ctx, `
INSERT INTO public.payment_webhook_inbox(
 inbox_id,provider,provider_event_id,event_type,provider_payment_id,
 payload_hash,verified_key_id,event_created_at,signature_verified_at,
 received_at,body_size_bytes
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT(provider,provider_event_id) DO NOTHING
RETURNING true`, record.ID, record.Provider, record.ProviderEventID, record.EventType,
		nullableString(record.ProviderPaymentID), record.PayloadHash[:], record.VerifiedKeyID,
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
				return "", webhook.ErrPersistence
			}
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return "", webhook.ErrPersistence
		}
		return webhook.StoreAccepted, nil
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", webhook.ErrPersistence
	}

	var canonicalHash []byte
	var canonicalState string
	if err := tx.QueryRow(ctx, `
SELECT payload_hash,state
FROM public.payment_webhook_inbox
WHERE provider=$1 AND provider_event_id=$2
FOR UPDATE`, record.Provider, record.ProviderEventID).Scan(&canonicalHash, &canonicalState); err != nil {
		return "", webhook.ErrPersistence
	}
	if bytes.Equal(canonicalHash, record.PayloadHash[:]) {
		if err := tx.Commit(ctx); err != nil {
			return "", webhook.ErrPersistence
		}
		return webhook.StoreDuplicate, nil
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
		return "", webhook.ErrPersistence
	}
	if webhookConflictQuarantinable(canonicalState) {
		tag, updateErr := tx.Exec(ctx, `
UPDATE public.payment_webhook_inbox
SET state='security_conflict',bounded_error_category='payload_hash_conflict',
	    lease_owner=NULL,lease_until=NULL,processed_at=$3
WHERE provider=$1 AND provider_event_id=$2 AND state=$4`,
			record.Provider, record.ProviderEventID, record.ReceivedAt, canonicalState)
		if updateErr != nil || tag.RowsAffected() != 1 {
			return "", webhook.ErrPersistence
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return "", webhook.ErrPersistence
	}
	return webhook.StoreConflict, nil
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
