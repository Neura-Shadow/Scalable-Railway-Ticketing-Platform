// Package physical inspects immutable booking command receipts on the exact
// physical shard selected by the control command.
package physical

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/booking/command/reconcile"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type HandleResolver interface {
	ResolveHandle(context.Context, sharding.ShardID) (shardphysical.Handle, error)
}

type Inspector struct {
	resolver HandleResolver
}

func NewInspector(resolver HandleResolver) (*Inspector, error) {
	if resolver == nil {
		return nil, reconcile.ErrInvalidOptions
	}
	return &Inspector{resolver: resolver}, nil
}

type CatalogReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// CatalogHandleResolver resolves the immutable shard recorded on the command,
// not the train run's current assignment. A command committed immediately
// before cutover must remain repairable after the control route moves.
type CatalogHandleResolver struct {
	catalog  CatalogReader
	registry *shardphysical.Registry
}

func NewCatalogHandleResolver(catalog CatalogReader, registry *shardphysical.Registry) (*CatalogHandleResolver, error) {
	if catalog == nil || registry == nil {
		return nil, reconcile.ErrInvalidOptions
	}
	return &CatalogHandleResolver{catalog: catalog, registry: registry}, nil
}

func (resolver *CatalogHandleResolver) ResolveHandle(ctx context.Context, shardID sharding.ShardID) (shardphysical.Handle, error) {
	if resolver == nil || ctx == nil || (shardID != sharding.ShardPhysicalZero && shardID != sharding.ShardPhysicalOne) {
		return shardphysical.Handle{}, reconcile.ErrShardUnreachable
	}
	var entry shardphysical.CatalogEntry
	var rawShardID, storageKind, healthState, state string
	err := resolver.catalog.QueryRow(ctx, `
SELECT shard_id, storage_kind, connection_ref, protocol_version,
       schema_version, enabled, write_enabled, health_state, state
FROM public.booking_shards
WHERE shard_id = $1`, shardID.String()).Scan(
		&rawShardID, &storageKind, &entry.ConnectionRef, &entry.ProtocolVersion,
		&entry.SchemaVersion, &entry.Enabled, &entry.WriteEnabled, &healthState, &state,
	)
	if err != nil || rawShardID != shardID.String() {
		return shardphysical.Handle{}, reconcile.ErrShardUnreachable
	}
	entry.ShardID = shardID
	entry.StorageKind = shardphysical.StorageKind(storageKind)
	entry.HealthState = shardphysical.HealthState(healthState)
	entry.State = shardphysical.CatalogState(state)
	handle, err := resolver.registry.Resolve(entry)
	if err != nil {
		return shardphysical.Handle{}, reconcile.ErrShardUnreachable
	}
	return handle, nil
}

func (inspector *Inspector) Inspect(ctx context.Context, candidate reconcile.Candidate) (reconcile.Observation, error) {
	if inspector == nil || ctx == nil {
		return reconcile.Observation{}, reconcile.ErrInvalidCandidate
	}
	handle, err := inspector.resolver.ResolveHandle(ctx, candidate.Command.Route.ShardID())
	if err != nil || handle.Pool() == nil {
		return reconcile.Observation{}, reconcile.ErrShardUnreachable
	}
	tx, err := handle.Pool().BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return reconcile.Observation{}, reconcile.ErrShardUnreachable
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var (
		commandID   uuid.UUID
		trainRunID  uuid.UUID
		generation  int64
		fingerprint []byte
		commandType string
		status      string
		resultID    pgtype.UUID
		errorCode   pgtype.Text
	)
	err = tx.QueryRow(ctx, `
SELECT command_id, train_run_id, assignment_generation, command_type, request_fingerprint,
       status, result_id, error_code
FROM booking_command_receipts
WHERE command_id = $1`, candidate.Command.ID).Scan(
		&commandID, &trainRunID, &generation, &commandType, &fingerprint, &status, &resultID, &errorCode,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return reconcile.Observation{}, reconcile.ErrShardUnreachable
		}
		return reconcile.Observation{Kind: reconcile.ObservationMissing}, nil
	}
	if err != nil || trainRunID != candidate.Command.TrainRunID ||
		generation != candidate.Command.Route.Generation().Int64() || command.Operation(commandType) != candidate.Command.Operation || len(fingerprint) != 32 {
		return reconcile.Observation{}, reconcile.ErrShardUnreachable
	}
	var storedFingerprint [32]byte
	copy(storedFingerprint[:], fingerprint)
	observation := reconcile.Observation{
		CommandID: commandID, RequestFingerprint: storedFingerprint,
	}
	switch status {
	case "started":
		observation.Kind = reconcile.ObservationStarted
	case "succeeded":
		if !resultID.Valid {
			return reconcile.Observation{}, reconcile.ErrReceiptMismatch
		}
		observation.Kind = reconcile.ObservationCommitted
		observation.ResultResourceID = uuid.UUID(resultID.Bytes)
		observation.Receipt = command.Receipt{
			CommandID: commandID, RequestFingerprint: storedFingerprint,
			ResultResourceID: observation.ResultResourceID, Status: command.ReceiptCommitted,
		}
		if candidate.Command.Operation == command.OperationConfirmReservation {
			if err := tx.QueryRow(ctx, `
SELECT id,count(ticket.id)::integer,orders.total_amount_minor,orders.currency,orders.created_at
FROM ticket_orders AS orders
JOIN tickets AS ticket ON ticket.ticket_order_id=orders.id
WHERE orders.reservation_id=$1 AND orders.user_id=$2
GROUP BY orders.id`, candidate.Command.ReservationID, candidate.Command.OwnerUserID).Scan(
				&observation.Receipt.TicketOrderID, &observation.Receipt.TicketCount,
				&observation.Receipt.TotalAmountMinor, &observation.Receipt.Currency,
				&observation.Receipt.OrderCreatedAt,
			); err != nil || observation.Receipt.TicketOrderID == uuid.Nil ||
				observation.Receipt.TicketCount < 1 || observation.Receipt.TicketCount > command.MaxReceiptTickets ||
				len(observation.Receipt.Currency) != 3 ||
				observation.Receipt.OrderCreatedAt.IsZero() {
				return reconcile.Observation{}, reconcile.ErrReceiptMismatch
			}
			rows, queryErr := tx.Query(ctx, `SELECT id FROM tickets WHERE ticket_order_id=$1 ORDER BY id LIMIT $2`,
				observation.Receipt.TicketOrderID, command.MaxReceiptTickets+1)
			if queryErr != nil {
				return reconcile.Observation{}, reconcile.ErrReceiptMismatch
			}
			for rows.Next() {
				var ticketID uuid.UUID
				if scanErr := rows.Scan(&ticketID); scanErr != nil || ticketID == uuid.Nil {
					rows.Close()
					return reconcile.Observation{}, reconcile.ErrReceiptMismatch
				}
				observation.Receipt.TicketIDs = append(observation.Receipt.TicketIDs, ticketID)
			}
			rowsErr := rows.Err()
			rows.Close()
			if rowsErr != nil || len(observation.Receipt.TicketIDs) != observation.Receipt.TicketCount {
				return reconcile.Observation{}, reconcile.ErrReceiptMismatch
			}
		}
	case "rejected":
		if !errorCode.Valid {
			return reconcile.Observation{}, reconcile.ErrReceiptMismatch
		}
		observation.Kind = reconcile.ObservationRejected
		observation.ErrorCode = errorCode.String
	default:
		return reconcile.Observation{}, reconcile.ErrReceiptMismatch
	}
	if err := tx.Commit(ctx); err != nil {
		return reconcile.Observation{}, reconcile.ErrShardUnreachable
	}
	return observation, nil
}
