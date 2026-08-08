package app

import (
	"context"
	"errors"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type physicalTicketControl interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type ticketLocator struct {
	record         TicketOrderRecord
	trainRunID     uuid.UUID
	shardID        string
	generation     int64
	storageKind    string
	directoryState string
}

// HybridTicketReader uses a bounded control locator page, then verifies every
// item against exactly the shard named by its owner-scoped reservation route.
type HybridTicketReader struct {
	control physicalTicketControl
	legacy  ticketReadStore
	router  physicalRouteResolver
}

func NewHybridTicketReader(control physicalTicketControl, legacy ticketReadStore, router physicalRouteResolver) (*HybridTicketReader, error) {
	if control == nil || legacy == nil || router == nil {
		return nil, ErrReadNotFound
	}
	return &HybridTicketReader{control: control, legacy: legacy, router: router}, nil
}

func (reader *HybridTicketReader) ListTicketOrderRecords(ctx context.Context, owner uuid.UUID, page httpapi.PageRequest) (TicketOrderRecords, error) {
	if reader == nil || ctx == nil || owner == uuid.Nil {
		return TicketOrderRecords{}, ErrReadNotFound
	}
	p, l := normalizePage(page.Page), normalizeLimit(page.Limit)
	orderBy, ok := ticketOrderLocatorBy(page.Sort)
	if !ok {
		return TicketOrderRecords{}, httpapi.ErrInvalidInput
	}
	rows, err := reader.control.Query(ctx, `
SELECT locator.ticket_order_id::text,locator.reservation_id::text,locator.status,
       locator.total_amount_minor,locator.currency,locator.created_at,
       locator.train_run_id,locator.shard_id,locator.assignment_generation,
       shard.storage_kind,directory.state,count(*) OVER()
FROM public.ticket_order_shard_locators AS locator
JOIN public.reservation_directory AS directory
  ON directory.reservation_id=locator.reservation_id
 AND directory.owner_user_id=locator.owner_user_id
 AND directory.train_run_id=locator.train_run_id
 AND directory.last_known_shard_id=locator.shard_id
 AND directory.last_known_generation=locator.assignment_generation
JOIN public.booking_shards AS shard ON shard.shard_id=locator.shard_id
WHERE locator.owner_user_id=$1
ORDER BY `+orderBy+` LIMIT $2 OFFSET $3`, owner, l, (p-1)*l)
	if err != nil {
		return TicketOrderRecords{}, err
	}
	locators := make([]ticketLocator, 0, l)
	var total int64
	for rows.Next() {
		var locator ticketLocator
		if err := rows.Scan(&locator.record.ID, &locator.record.ReservationID, &locator.record.Status,
			&locator.record.TotalAmountMinor, &locator.record.Currency, &locator.record.CreatedAt,
			&locator.trainRunID, &locator.shardID, &locator.generation, &locator.storageKind,
			&locator.directoryState, &total); err != nil {
			rows.Close()
			return TicketOrderRecords{}, err
		}
		locator.record.CreatedAt = locator.record.CreatedAt.UTC()
		locators = append(locators, locator)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return TicketOrderRecords{}, err
	}
	rows.Close()
	result := TicketOrderRecords{Items: make([]TicketOrderRecord, 0, len(locators)), Total: total}
	for _, locator := range locators {
		record, err := reader.load(ctx, owner, locator)
		if err != nil {
			return TicketOrderRecords{}, err
		}
		result.Items = append(result.Items, record)
	}
	return result, nil
}

func (reader *HybridTicketReader) GetTicketOrderRecord(ctx context.Context, owner, orderID uuid.UUID) (TicketOrderRecord, error) {
	if reader == nil || ctx == nil || owner == uuid.Nil || orderID == uuid.Nil {
		return TicketOrderRecord{}, ErrReadNotFound
	}
	var locator ticketLocator
	err := reader.control.QueryRow(ctx, `
SELECT locator.ticket_order_id::text,locator.reservation_id::text,locator.status,
       locator.total_amount_minor,locator.currency,locator.created_at,
       locator.train_run_id,locator.shard_id,locator.assignment_generation,
       shard.storage_kind,directory.state
FROM public.ticket_order_shard_locators AS locator
JOIN public.reservation_directory AS directory
  ON directory.reservation_id=locator.reservation_id
 AND directory.owner_user_id=locator.owner_user_id
 AND directory.last_known_shard_id=locator.shard_id
 AND directory.last_known_generation=locator.assignment_generation
JOIN public.booking_shards AS shard ON shard.shard_id=locator.shard_id
WHERE locator.ticket_order_id=$1 AND locator.owner_user_id=$2`, orderID, owner).Scan(
		&locator.record.ID, &locator.record.ReservationID, &locator.record.Status,
		&locator.record.TotalAmountMinor, &locator.record.Currency, &locator.record.CreatedAt,
		&locator.trainRunID, &locator.shardID, &locator.generation, &locator.storageKind, &locator.directoryState)
	if errors.Is(err, pgx.ErrNoRows) {
		return TicketOrderRecord{}, ErrReadNotFound
	}
	if err != nil {
		return TicketOrderRecord{}, err
	}
	locator.record.CreatedAt = locator.record.CreatedAt.UTC()
	return reader.load(ctx, owner, locator)
}

func (reader *HybridTicketReader) GetTicketRecord(ctx context.Context, owner, ticketID uuid.UUID) (TicketRecord, error) {
	if reader == nil || ctx == nil || owner == uuid.Nil || ticketID == uuid.Nil {
		return TicketRecord{}, ErrReadNotFound
	}
	var (
		orderID        uuid.UUID
		reservationID  uuid.UUID
		trainRunID     uuid.UUID
		shardID        string
		generation     int64
		storageKind    string
		directoryState string
	)
	err := reader.control.QueryRow(ctx, `
SELECT locator.ticket_order_id,locator.reservation_id,locator.train_run_id,
       locator.shard_id,locator.assignment_generation,shard.storage_kind,
       directory.state
FROM public.ticket_shard_locators AS locator
JOIN public.reservation_directory AS directory
  ON directory.reservation_id=locator.reservation_id
 AND directory.owner_user_id=locator.owner_user_id
 AND directory.last_known_shard_id=locator.shard_id
 AND directory.last_known_generation=locator.assignment_generation
JOIN public.booking_shards AS shard ON shard.shard_id=locator.shard_id
WHERE locator.ticket_id=$1 AND locator.owner_user_id=$2`, ticketID, owner).Scan(
		&orderID, &reservationID, &trainRunID, &shardID, &generation, &storageKind, &directoryState)
	if errors.Is(err, pgx.ErrNoRows) {
		return TicketRecord{}, ErrReadNotFound
	}
	if err != nil {
		return TicketRecord{}, err
	}
	if storageKind != "postgres" {
		if storageKind == "legacy_schema" || storageKind == "logical_schema" {
			return reader.legacy.GetTicketRecord(ctx, owner, ticketID)
		}
		return TicketRecord{}, sharding.ErrShardUnavailable
	}
	if directoryState != "active" {
		return TicketRecord{}, sharding.ErrWriteFenced
	}
	parsedShard, err := sharding.ParseShardID(shardID)
	if err != nil || (parsedShard != sharding.ShardPhysicalZero && parsedShard != sharding.ShardPhysicalOne) ||
		orderID == uuid.Nil || reservationID == uuid.Nil || trainRunID == uuid.Nil || generation <= 0 {
		return TicketRecord{}, sharding.ErrShardUnavailable
	}
	resolved, err := reader.router.Resolve(ctx, trainRunID, false)
	if err != nil || resolved.Route.ShardID() != parsedShard || resolved.Route.Generation().Int64() != generation {
		resolved, err = reader.router.Resolve(ctx, trainRunID, true)
		if err != nil || resolved.Route.ShardID() != parsedShard || resolved.Route.Generation().Int64() != generation {
			return TicketRecord{}, sharding.ErrAssignmentStale
		}
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return TicketRecord{}, sharding.ErrShardUnavailable
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var ticket TicketRecord
	err = tx.QueryRow(ctx, `SELECT ticket.id::text,ticket.ticket_code,seat.passenger_id::text,seat.seat_id::text,ticket.status
FROM tickets AS ticket
JOIN ticket_orders AS ticket_order ON ticket_order.id=ticket.ticket_order_id
JOIN reservation_seats AS seat ON seat.id=ticket.reservation_seat_id
JOIN reservations AS reservation
  ON reservation.id=ticket_order.reservation_id
 AND reservation.user_id=ticket_order.user_id
 AND reservation.train_run_id=ticket_order.train_run_id
 AND reservation.assignment_generation=ticket_order.assignment_generation
WHERE ticket.id=$1
  AND ticket.ticket_order_id=$2
  AND ticket_order.reservation_id=$3
  AND ticket_order.user_id=$4
  AND reservation.id=$3
  AND seat.reservation_id=reservation.id
  AND seat.train_run_id=$5
  AND seat.assignment_generation=$6
  AND ticket.train_run_id=$5
  AND ticket.assignment_generation=$6
  AND ticket_order.train_run_id=$5
  AND ticket_order.assignment_generation=$6`, ticketID, orderID, reservationID, owner, trainRunID, generation).Scan(
		&ticket.ID, &ticket.TicketCode, &ticket.PassengerID, &ticket.SeatID, &ticket.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return TicketRecord{}, ErrReadNotFound
	}
	if err != nil {
		return TicketRecord{}, sharding.ErrShardUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return TicketRecord{}, sharding.ErrShardUnavailable
	}
	return ticket, nil
}

func (reader *HybridTicketReader) load(ctx context.Context, owner uuid.UUID, locator ticketLocator) (TicketOrderRecord, error) {
	orderID, err := uuid.Parse(locator.record.ID)
	if err != nil || locator.trainRunID == uuid.Nil || locator.generation <= 0 {
		return TicketOrderRecord{}, ErrReadNotFound
	}
	if locator.storageKind != "postgres" {
		if locator.storageKind == "legacy_schema" || locator.storageKind == "logical_schema" {
			return reader.legacy.GetTicketOrderRecord(ctx, owner, orderID)
		}
		return TicketOrderRecord{}, sharding.ErrShardUnavailable
	}
	if locator.directoryState != "active" {
		return TicketOrderRecord{}, sharding.ErrWriteFenced
	}
	parsedShard, err := sharding.ParseShardID(locator.shardID)
	if err != nil || (parsedShard != sharding.ShardPhysicalZero && parsedShard != sharding.ShardPhysicalOne) {
		return TicketOrderRecord{}, sharding.ErrShardUnavailable
	}
	resolved, err := reader.router.Resolve(ctx, locator.trainRunID, false)
	if err != nil || resolved.Route.ShardID() != parsedShard || resolved.Route.Generation().Int64() != locator.generation {
		resolved, err = reader.router.Resolve(ctx, locator.trainRunID, true)
		if err != nil || resolved.Route.ShardID() != parsedShard || resolved.Route.Generation().Int64() != locator.generation {
			return TicketOrderRecord{}, sharding.ErrAssignmentStale
		}
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return TicketOrderRecord{}, sharding.ErrShardUnavailable
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var authoritative TicketOrderRecord
	err = tx.QueryRow(ctx, `SELECT id::text,reservation_id::text,status,total_amount_minor,currency,created_at
FROM ticket_orders WHERE id=$1 AND user_id=$2 AND train_run_id=$3 AND assignment_generation=$4`,
		orderID, owner, locator.trainRunID, locator.generation).Scan(&authoritative.ID, &authoritative.ReservationID,
		&authoritative.Status, &authoritative.TotalAmountMinor, &authoritative.Currency, &authoritative.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return TicketOrderRecord{}, ErrReadNotFound
	}
	if err != nil {
		return TicketOrderRecord{}, sharding.ErrShardUnavailable
	}
	authoritative.CreatedAt = authoritative.CreatedAt.UTC()
	if !sameTicketOrderSummary(locator.record, authoritative) {
		return TicketOrderRecord{}, ErrReadNotFound
	}
	rows, err := tx.Query(ctx, `SELECT ticket.id::text,ticket.ticket_code,seat.passenger_id::text,seat.seat_id::text,ticket.status
FROM tickets AS ticket
JOIN reservation_seats AS seat ON seat.id=ticket.reservation_seat_id
WHERE ticket.ticket_order_id=$1 ORDER BY ticket.id`, orderID)
	if err != nil {
		return TicketOrderRecord{}, sharding.ErrShardUnavailable
	}
	defer rows.Close()
	for rows.Next() {
		var ticket TicketRecord
		if err := rows.Scan(&ticket.ID, &ticket.TicketCode, &ticket.PassengerID, &ticket.SeatID, &ticket.Status); err != nil {
			return TicketOrderRecord{}, sharding.ErrShardUnavailable
		}
		authoritative.Tickets = append(authoritative.Tickets, ticket)
	}
	if rows.Err() != nil || len(authoritative.Tickets) == 0 {
		return TicketOrderRecord{}, ErrReadNotFound
	}
	if err := tx.Commit(ctx); err != nil {
		return TicketOrderRecord{}, sharding.ErrShardUnavailable
	}
	return authoritative, nil
}

var _ ticketReadStore = (*HybridTicketReader)(nil)
