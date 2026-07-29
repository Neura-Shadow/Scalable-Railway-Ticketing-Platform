// Package postgres persists bounded booking-command recovery leases and
// control-plane repairs. It never opens a booking-shard connection.
package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/reconcile"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var ErrControlStore = errors.New("booking command reconciliation control store failed")

type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type Store struct {
	db DB
}

func NewStore(db DB) (*Store, error) {
	if nilInterface(db) {
		return nil, reconcile.ErrInvalidOptions
	}
	return &Store{db: db}, nil
}

func (store *Store) Claim(ctx context.Context, options reconcile.ClaimOptions) ([]reconcile.Candidate, error) {
	if store == nil || ctx == nil || options.BatchSize < 1 || options.LeaseTTL <= 0 {
		return nil, ErrControlStore
	}
	tx, err := store.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, ErrControlStore
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	rows, err := tx.Query(ctx, `
WITH claimable AS (
    SELECT command_row.command_id
    FROM public.booking_commands AS command_row
    JOIN public.booking_quota_leases AS quota
      ON quota.command_id = command_row.command_id
    JOIN public.reservation_directory AS directory
      ON directory.command_id = command_row.command_id
    WHERE command_row.operation = 'reservation.create'
      AND (
          command_row.state IN (
              'reserved', 'executing', 'committed_on_shard', 'needs_repair'
          )
          OR (
              command_row.state = 'finalized'
              AND (quota.state <> 'active_hold' OR directory.state <> 'active')
          )
      )
      AND (
          command_row.lease_until IS NULL
          OR command_row.lease_until < clock_timestamp()
      )
    ORDER BY command_row.updated_at, command_row.command_id
    FOR UPDATE OF command_row SKIP LOCKED
    LIMIT $1
), claimed AS (
    UPDATE public.booking_commands AS command_row
    SET lease_owner = $2,
        lease_until = clock_timestamp() + ($3::bigint * interval '1 millisecond')
    FROM claimable
    WHERE command_row.command_id = claimable.command_id
    RETURNING command_row.*
)
SELECT claimed.command_id, claimed.operation, claimed.owner_user_id,
       claimed.train_run_id, claimed.reservation_id, claimed.target_shard_id,
       claimed.assignment_generation, claimed.request_fingerprint,
       claimed.state, quota.expires_at
FROM claimed
JOIN public.booking_quota_leases AS quota
  ON quota.command_id = claimed.command_id
ORDER BY claimed.updated_at, claimed.command_id`, options.BatchSize, options.WorkerID, options.LeaseTTL.Milliseconds())
	if err != nil {
		return nil, ErrControlStore
	}
	defer rows.Close()
	candidates := make([]reconcile.Candidate, 0, options.BatchSize)
	for rows.Next() {
		var (
			candidate      reconcile.Candidate
			rawShardID     string
			rawGeneration  int64
			rawFingerprint []byte
			rawState       string
		)
		if err := rows.Scan(
			&candidate.Command.ID, &candidate.Command.Operation, &candidate.Command.OwnerUserID,
			&candidate.Command.TrainRunID, &candidate.Command.ReservationID, &rawShardID,
			&rawGeneration, &rawFingerprint, &rawState, &candidate.QuotaExpiresAt,
		); err != nil || len(rawFingerprint) != 32 {
			return nil, ErrControlStore
		}
		shardID, err := sharding.ParseShardID(rawShardID)
		if err != nil || (shardID != sharding.ShardPhysicalZero && shardID != sharding.ShardPhysicalOne) {
			return nil, ErrControlStore
		}
		generation, err := sharding.NewAssignmentGeneration(rawGeneration)
		if err != nil {
			return nil, ErrControlStore
		}
		candidate.Command.Route, err = sharding.NewShardRoute(candidate.Command.TrainRunID, shardID, generation)
		if err != nil {
			return nil, ErrControlStore
		}
		copy(candidate.Command.RequestFingerprint[:], rawFingerprint)
		candidate.Command.State = command.State(rawState)
		candidates = append(candidates, candidate)
	}
	if rows.Err() != nil {
		return nil, ErrControlStore
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, ErrControlStore
	}
	return candidates, nil
}

func (store *Store) Finalize(ctx context.Context, candidate reconcile.Candidate, receipt command.Receipt) error {
	if store == nil || ctx == nil || receipt.Status != command.ReceiptCommitted ||
		receipt.CommandID != candidate.Command.ID || receipt.ResultResourceID != candidate.Command.ReservationID ||
		receipt.RequestFingerprint != candidate.Command.RequestFingerprint {
		return ErrControlStore
	}
	return store.mutate(ctx, candidate, func(tx pgx.Tx) error {
		state, err := lockAndVerify(ctx, tx, candidate)
		if err != nil || state == command.StateFailed || state == command.StateExpired {
			return ErrControlStore
		}
		if err := execOne(ctx, tx, `
UPDATE public.booking_commands
SET state = 'finalized', result_resource_id = $2,
    finalized_at = COALESCE(finalized_at, clock_timestamp()),
    lease_owner = NULL, lease_until = NULL, bounded_error_category = NULL
WHERE command_id = $1`, candidate.Command.ID, candidate.Command.ReservationID); err != nil {
			return err
		}
		if err := execOne(ctx, tx, `
UPDATE public.reservation_directory
SET state = 'active', active_at = COALESCE(active_at, clock_timestamp()),
    last_known_shard_id = $2, last_known_generation = $3,
    bounded_error_category = NULL, tombstoned_at = NULL
WHERE command_id = $1
  AND reservation_id = $4
  AND state IN ('pending', 'active', 'failed')`, candidate.Command.ID,
			candidate.Command.Route.ShardID().String(), candidate.Command.Route.Generation().Int64(),
			candidate.Command.ReservationID); err != nil {
			return err
		}
		if err := execOne(ctx, tx, `
UPDATE public.booking_quota_leases
SET state = 'active_hold', released_at = NULL
WHERE command_id = $1`, candidate.Command.ID); err != nil {
			return err
		}
		payload, err := json.Marshal(struct {
			CommandID     uuid.UUID `json:"command_id"`
			ReservationID uuid.UUID `json:"reservation_id"`
		}{CommandID: candidate.Command.ID, ReservationID: candidate.Command.ReservationID})
		if err != nil {
			return ErrControlStore
		}
		_, err = tx.Exec(ctx, `
INSERT INTO public.outbox_events (
    id, aggregate_type, aggregate_id, event_type, event_version, payload
) VALUES ($1, 'booking_command', $2, 'booking_command.finalized', 1, $3)
ON CONFLICT (id) DO NOTHING`,
			uuid.NewSHA1(uuid.NameSpaceOID, []byte("booking-command.finalized:"+candidate.Command.ID.String())),
			candidate.Command.ID, payload)
		if err != nil {
			return ErrControlStore
		}
		return nil
	})
}

func (store *Store) Fail(
	ctx context.Context,
	candidate reconcile.Candidate,
	category reconcile.FailureCategory,
) error {
	if category != reconcile.FailureShardRejected {
		return ErrControlStore
	}
	return store.terminal(ctx, candidate, command.StateFailed, string(category), "released")
}

func (store *Store) Expire(ctx context.Context, candidate reconcile.Candidate) error {
	return store.terminal(ctx, candidate, command.StateExpired, "command_expired", "expired")
}

func (store *Store) terminal(
	ctx context.Context,
	candidate reconcile.Candidate,
	target command.State,
	category string,
	quotaState string,
) error {
	if store == nil || ctx == nil || (target != command.StateFailed && target != command.StateExpired) {
		return ErrControlStore
	}
	return store.mutate(ctx, candidate, func(tx pgx.Tx) error {
		state, err := lockAndVerify(ctx, tx, candidate)
		if err != nil || state == command.StateFinalized {
			return ErrControlStore
		}
		if target == command.StateExpired {
			var expired bool
			if err := tx.QueryRow(ctx, `
SELECT expires_at <= clock_timestamp()
FROM public.booking_quota_leases
WHERE command_id = $1
FOR UPDATE`, candidate.Command.ID).Scan(&expired); err != nil || !expired {
				return ErrControlStore
			}
		}
		if err := execOne(ctx, tx, `
UPDATE public.booking_commands
SET state = $2, result_resource_id = NULL, finalized_at = NULL,
    lease_owner = NULL, lease_until = NULL, bounded_error_category = $3
WHERE command_id = $1`, candidate.Command.ID, target, category); err != nil {
			return err
		}
		if err := execOne(ctx, tx, `
UPDATE public.reservation_directory
SET state = 'failed', bounded_error_category = $2,
    active_at = NULL, tombstoned_at = NULL
WHERE command_id = $1
  AND reservation_id = $3
  AND state IN ('pending', 'failed')`, candidate.Command.ID, category, candidate.Command.ReservationID); err != nil {
			return err
		}
		if err := execOne(ctx, tx, `
UPDATE public.booking_quota_leases
SET state = $2, released_at = COALESCE(released_at, clock_timestamp())
WHERE command_id = $1
  AND state IN ('pending', 'active_hold', 'repair_required', 'released', 'expired')`,
			candidate.Command.ID, quotaState); err != nil {
			return err
		}
		return nil
	})
}

func (store *Store) mutate(ctx context.Context, candidate reconcile.Candidate, apply func(pgx.Tx) error) error {
	if store == nil || ctx == nil || apply == nil {
		return ErrControlStore
	}
	tx, err := store.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ErrControlStore
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := apply(tx); err != nil {
		return ErrControlStore
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrControlStore
	}
	return nil
}

func lockAndVerify(ctx context.Context, tx pgx.Tx, candidate reconcile.Candidate) (command.State, error) {
	var (
		fingerprint   []byte
		reservationID uuid.UUID
		state         string
	)
	if err := tx.QueryRow(ctx, `
SELECT request_fingerprint, reservation_id, state
FROM public.booking_commands
WHERE command_id = $1
FOR UPDATE`, candidate.Command.ID).Scan(&fingerprint, &reservationID, &state); err != nil ||
		len(fingerprint) != 32 || !bytes.Equal(fingerprint, candidate.Command.RequestFingerprint[:]) ||
		reservationID != candidate.Command.ReservationID {
		return "", ErrControlStore
	}
	return command.State(state), nil
}

func execOne(ctx context.Context, tx pgx.Tx, query string, arguments ...any) error {
	tag, err := tx.Exec(ctx, query, arguments...)
	if err != nil || tag.RowsAffected() != 1 {
		return ErrControlStore
	}
	return nil
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}
