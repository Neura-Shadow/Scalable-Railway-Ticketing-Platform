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
WITH eligible AS (
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
    UNION ALL
    SELECT command_row.command_id
    FROM public.booking_commands AS command_row
    WHERE command_row.operation IN ('reservation.confirm','reservation.cancel')
      AND command_row.state IN ('reserved','executing','committed_on_shard','needs_repair')
      AND (command_row.lease_until IS NULL OR command_row.lease_until < clock_timestamp())
), claimable AS (
    SELECT command_row.command_id
    FROM public.booking_commands AS command_row
    JOIN eligible ON eligible.command_id=command_row.command_id
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
       claimed.state, COALESCE(quota.expires_at,clock_timestamp()+interval '100 years')
FROM claimed
LEFT JOIN public.booking_quota_leases AS quota
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
	if candidate.Command.Operation == command.OperationConfirmReservation ||
		candidate.Command.Operation == command.OperationCancelReservation {
		return store.finalizeLifecycle(ctx, candidate, receipt)
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
		locatorTag, err := tx.Exec(ctx, `
INSERT INTO public.reservation_shard_locators(
 reservation_id,train_run_id,shard_id,assignment_generation,owner_user_id
) VALUES($1,$2,$3,$4,$5)
ON CONFLICT(reservation_id) DO UPDATE SET train_run_id=EXCLUDED.train_run_id,
 shard_id=EXCLUDED.shard_id,assignment_generation=EXCLUDED.assignment_generation,
 owner_user_id=EXCLUDED.owner_user_id
WHERE reservation_shard_locators.train_run_id=EXCLUDED.train_run_id
 AND reservation_shard_locators.shard_id=EXCLUDED.shard_id
 AND reservation_shard_locators.assignment_generation=EXCLUDED.assignment_generation
 AND reservation_shard_locators.owner_user_id=EXCLUDED.owner_user_id`, candidate.Command.ReservationID,
			candidate.Command.TrainRunID, candidate.Command.Route.ShardID().String(),
			candidate.Command.Route.Generation().Int64(), candidate.Command.OwnerUserID)
		if err != nil || locatorTag.RowsAffected() != 1 {
			return ErrControlStore
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

func (store *Store) finalizeLifecycle(ctx context.Context, candidate reconcile.Candidate, receipt command.Receipt) error {
	return store.mutate(ctx, candidate, func(tx pgx.Tx) error {
		state, err := lockAndVerify(ctx, tx, candidate)
		if err != nil || state == command.StateFailed || state == command.StateExpired {
			return ErrControlStore
		}
		if err := execOne(ctx, tx, `
UPDATE public.booking_commands
SET state='finalized',result_resource_id=$2,
    finalized_at=COALESCE(finalized_at,clock_timestamp()),
    lease_owner=NULL,lease_until=NULL,bounded_error_category=NULL
WHERE command_id=$1`, candidate.Command.ID, candidate.Command.ReservationID); err != nil {
			return err
		}
		lifecycleStatus, err := advanceReconciledLifecycle(ctx, tx, candidate.Command)
		if err != nil {
			return err
		}
		if candidate.Command.Operation == command.OperationConfirmReservation {
			if !validConfirmationReceipt(receipt) || receipt.TotalAmountMinor < 0 ||
				len(receipt.Currency) != 3 || receipt.OrderCreatedAt.IsZero() {
				return ErrControlStore
			}
			locatorTag, err := tx.Exec(ctx, `
INSERT INTO public.ticket_order_shard_locators(
 ticket_order_id,reservation_id,train_run_id,shard_id,assignment_generation,
 owner_user_id,status,total_amount_minor,currency,created_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT(ticket_order_id) DO UPDATE SET status=EXCLUDED.status,
 total_amount_minor=EXCLUDED.total_amount_minor,currency=EXCLUDED.currency,created_at=EXCLUDED.created_at
WHERE ticket_order_shard_locators.reservation_id=EXCLUDED.reservation_id
 AND ticket_order_shard_locators.owner_user_id=EXCLUDED.owner_user_id
 AND ticket_order_shard_locators.train_run_id=EXCLUDED.train_run_id
 AND ticket_order_shard_locators.shard_id=EXCLUDED.shard_id
 AND ticket_order_shard_locators.assignment_generation=EXCLUDED.assignment_generation`,
				receipt.TicketOrderID, candidate.Command.ReservationID, candidate.Command.TrainRunID,
				candidate.Command.Route.ShardID().String(), candidate.Command.Route.Generation().Int64(),
				candidate.Command.OwnerUserID, lifecycleStatus, receipt.TotalAmountMinor, receipt.Currency, receipt.OrderCreatedAt.UTC())
			if err != nil || locatorTag.RowsAffected() != 1 {
				return ErrControlStore
			}
			if err := insertTicketLocators(ctx, tx, candidate, receipt); err != nil {
				return err
			}
		} else if candidate.Command.Operation == command.OperationCancelReservation {
			if _, err := tx.Exec(ctx, `UPDATE public.ticket_order_shard_locators SET status=$3
WHERE reservation_id=$1 AND owner_user_id=$2`, candidate.Command.ReservationID, candidate.Command.OwnerUserID, lifecycleStatus); err != nil {
				return ErrControlStore
			}
		}
		if _, err := tx.Exec(ctx, `
UPDATE public.booking_quota_leases AS quota
SET state='released',released_at=COALESCE(released_at,clock_timestamp())
FROM public.reservation_directory AS directory
WHERE directory.reservation_id=$1 AND quota.command_id=directory.command_id
  AND quota.state IN ('pending','active_hold','repair_required','released')`, candidate.Command.ReservationID); err != nil {
			return ErrControlStore
		}
		payload, err := json.Marshal(struct {
			CommandID     uuid.UUID         `json:"command_id"`
			ReservationID uuid.UUID         `json:"reservation_id"`
			Operation     command.Operation `json:"operation"`
		}{candidate.Command.ID, candidate.Command.ReservationID, candidate.Command.Operation})
		if err != nil {
			return ErrControlStore
		}
		_, err = tx.Exec(ctx, `
INSERT INTO public.outbox_events(id,aggregate_type,aggregate_id,event_type,event_version,payload)
VALUES($1,'booking_command',$2,'booking_command.finalized',1,$3)
ON CONFLICT(id) DO NOTHING`, uuid.NewSHA1(uuid.NameSpaceOID, []byte("booking-command.finalized:"+candidate.Command.ID.String())), candidate.Command.ID, payload)
		return err
	})
}

func advanceReconciledLifecycle(ctx context.Context, tx pgx.Tx, bookingCommand command.Command) (string, error) {
	status, rank := "confirmed", int16(1)
	if bookingCommand.Operation == command.OperationCancelReservation {
		status, rank = "cancelled", 2
	}
	var storedStatus string
	err := tx.QueryRow(ctx, `
WITH advanced AS (
 INSERT INTO public.reservation_lifecycle_states(
  reservation_id,owner_user_id,status,lifecycle_rank,last_command_id
 ) VALUES($1,$2,$3,$4,$5)
 ON CONFLICT(reservation_id) DO UPDATE SET
  status=EXCLUDED.status,lifecycle_rank=EXCLUDED.lifecycle_rank,
  last_command_id=EXCLUDED.last_command_id
 WHERE reservation_lifecycle_states.owner_user_id=EXCLUDED.owner_user_id
   AND reservation_lifecycle_states.lifecycle_rank<=EXCLUDED.lifecycle_rank
 RETURNING status
)
SELECT status FROM advanced
UNION ALL
SELECT status FROM public.reservation_lifecycle_states
WHERE reservation_id=$1 AND owner_user_id=$2 AND NOT EXISTS(SELECT 1 FROM advanced)
LIMIT 1`, bookingCommand.ReservationID, bookingCommand.OwnerUserID, status, rank, bookingCommand.ID).Scan(&storedStatus)
	if err != nil || (storedStatus != "confirmed" && storedStatus != "cancelled") {
		return "", ErrControlStore
	}
	return storedStatus, nil
}

func validConfirmationReceipt(receipt command.Receipt) bool {
	if receipt.TicketOrderID == uuid.Nil || receipt.TicketCount < 1 ||
		receipt.TicketCount > command.MaxReceiptTickets || len(receipt.TicketIDs) != receipt.TicketCount {
		return false
	}
	seen := make(map[uuid.UUID]struct{}, len(receipt.TicketIDs))
	for _, ticketID := range receipt.TicketIDs {
		if ticketID == uuid.Nil {
			return false
		}
		if _, duplicate := seen[ticketID]; duplicate {
			return false
		}
		seen[ticketID] = struct{}{}
	}
	return true
}

func insertTicketLocators(ctx context.Context, tx pgx.Tx, candidate reconcile.Candidate, receipt command.Receipt) error {
	for _, ticketID := range receipt.TicketIDs {
		tag, err := tx.Exec(ctx, `
INSERT INTO public.ticket_shard_locators(
 ticket_id,ticket_order_id,reservation_id,train_run_id,shard_id,assignment_generation,owner_user_id
) VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT(ticket_id) DO UPDATE SET ticket_order_id=EXCLUDED.ticket_order_id
WHERE ticket_shard_locators.ticket_order_id=EXCLUDED.ticket_order_id
  AND ticket_shard_locators.reservation_id=EXCLUDED.reservation_id
  AND ticket_shard_locators.owner_user_id=EXCLUDED.owner_user_id
  AND ticket_shard_locators.train_run_id=EXCLUDED.train_run_id
  AND ticket_shard_locators.shard_id=EXCLUDED.shard_id
  AND ticket_shard_locators.assignment_generation=EXCLUDED.assignment_generation`,
			ticketID, receipt.TicketOrderID, candidate.Command.ReservationID, candidate.Command.TrainRunID,
			candidate.Command.Route.ShardID().String(), candidate.Command.Route.Generation().Int64(), candidate.Command.OwnerUserID)
		if err != nil || tag.RowsAffected() != 1 {
			return ErrControlStore
		}
	}
	return nil
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
		if candidate.Command.Operation != command.OperationCreateReservation {
			return nil
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
