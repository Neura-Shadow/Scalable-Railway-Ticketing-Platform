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

type ticketLocatorGroupKey struct {
	shardID    sharding.ShardID
	generation int64
}

type indexedTicketLocator struct {
	index   int
	locator ticketLocator
	orderID uuid.UUID
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
	items, err := reader.loadLocatorPage(ctx, owner, locators)
	if err != nil {
		return TicketOrderRecords{}, err
	}
	return TicketOrderRecords{Items: items, Total: total}, nil
}

func (reader *HybridTicketReader) loadLocatorPage(ctx context.Context, owner uuid.UUID, locators []ticketLocator) ([]TicketOrderRecord, error) {
	items := make([]TicketOrderRecord, len(locators))
	groups := make(map[ticketLocatorGroupKey][]indexedTicketLocator, len(locators))
	for index, locator := range locators {
		orderID, err := uuid.Parse(locator.record.ID)
		if err != nil || locator.trainRunID == uuid.Nil || locator.generation <= 0 {
			return nil, ErrReadNotFound
		}
		if locator.storageKind != "postgres" {
			if locator.storageKind != "legacy_schema" && locator.storageKind != "logical_schema" {
				return nil, sharding.ErrShardUnavailable
			}
			record, err := reader.legacy.GetTicketOrderRecord(ctx, owner, orderID)
			if err != nil {
				return nil, err
			}
			items[index] = record
			continue
		}
		if locator.directoryState != "active" {
			return nil, sharding.ErrWriteFenced
		}
		parsedShard, err := sharding.ParseShardID(locator.shardID)
		if err != nil || (parsedShard != sharding.ShardPhysicalZero && parsedShard != sharding.ShardPhysicalOne) {
			return nil, sharding.ErrShardUnavailable
		}
		key := ticketLocatorGroupKey{shardID: parsedShard, generation: locator.generation}
		groups[key] = append(groups[key], indexedTicketLocator{index: index, locator: locator, orderID: orderID})
	}
	for key, group := range groups {
		records, err := reader.loadPhysicalTicketGroup(ctx, owner, key, group)
		if err != nil {
			return nil, err
		}
		for index, record := range records {
			items[group[index].index] = record
		}
	}
	return items, nil
}

func (reader *HybridTicketReader) loadPhysicalTicketGroup(ctx context.Context, owner uuid.UUID, key ticketLocatorGroupKey, group []indexedTicketLocator) ([]TicketOrderRecord, error) {
	if len(group) == 0 || len(group) > 100 {
		return nil, sharding.ErrShardUnavailable
	}
	trainRunID := group[0].locator.trainRunID
	resolved, err := reader.router.Resolve(ctx, trainRunID, false)
	if err != nil || resolved.Route.ShardID() != key.shardID || resolved.Route.Generation().Int64() != key.generation {
		resolved, err = reader.router.Resolve(ctx, trainRunID, true)
		if err != nil || resolved.Route.ShardID() != key.shardID || resolved.Route.Generation().Int64() != key.generation {
			return nil, sharding.ErrAssignmentStale
		}
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, sharding.ErrShardUnavailable
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	orderIDs := make([]uuid.UUID, len(group))
	expectedTrainRuns := make(map[uuid.UUID]uuid.UUID, len(group))
	for index := range group {
		orderIDs[index] = group[index].orderID
		expectedTrainRuns[group[index].orderID] = group[index].locator.trainRunID
	}
	rows, err := tx.Query(ctx, `
SELECT id::text,reservation_id::text,status,total_amount_minor,currency,created_at,train_run_id
FROM ticket_orders
WHERE id=ANY($1::uuid[]) AND user_id=$2
  AND assignment_generation=$3
ORDER BY id`, orderIDs, owner, key.generation)
	if err != nil {
		return nil, sharding.ErrShardUnavailable
	}
	authoritative := make(map[uuid.UUID]TicketOrderRecord, len(group))
	for rows.Next() {
		var record TicketOrderRecord
		var authoritativeTrainRun uuid.UUID
		if err := rows.Scan(&record.ID, &record.ReservationID, &record.Status,
			&record.TotalAmountMinor, &record.Currency, &record.CreatedAt, &authoritativeTrainRun); err != nil {
			rows.Close()
			return nil, sharding.ErrShardUnavailable
		}
		id, err := uuid.Parse(record.ID)
		if err != nil {
			rows.Close()
			return nil, ErrReadNotFound
		}
		if _, duplicate := authoritative[id]; duplicate || expectedTrainRuns[id] != authoritativeTrainRun {
			rows.Close()
			return nil, ErrReadNotFound
		}
		record.CreatedAt = record.CreatedAt.UTC()
		authoritative[id] = record
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, sharding.ErrShardUnavailable
	}
	rows.Close()
	if len(authoritative) != len(group) {
		return nil, ErrReadNotFound
	}
	for _, item := range group {
		record, found := authoritative[item.orderID]
		if !found || !sameTicketOrderSummary(item.locator.record, record) {
			return nil, ErrReadNotFound
		}
	}
	rows, err = tx.Query(ctx, `
SELECT ticket.ticket_order_id::text,ticket.id::text,ticket.ticket_code,
       seat.passenger_id::text,seat.seat_id::text,ticket.status
FROM tickets AS ticket
JOIN ticket_orders AS ticket_order
  ON ticket_order.id=ticket.ticket_order_id
 AND ticket_order.user_id=$2
 AND ticket_order.assignment_generation=$3
JOIN reservations AS reservation
  ON reservation.id=ticket_order.reservation_id
 AND reservation.user_id=ticket_order.user_id
 AND reservation.train_run_id=ticket_order.train_run_id
 AND reservation.assignment_generation=ticket_order.assignment_generation
JOIN reservation_seats AS seat
  ON seat.id=ticket.reservation_seat_id
 AND seat.reservation_id=reservation.id
 AND seat.train_run_id=reservation.train_run_id
 AND seat.assignment_generation=$3
WHERE ticket.ticket_order_id=ANY($1::uuid[])
  AND ticket.train_run_id=ticket_order.train_run_id
  AND ticket.assignment_generation=$3
ORDER BY ticket.ticket_order_id,ticket.id`, orderIDs, owner, key.generation)
	if err != nil {
		return nil, sharding.ErrShardUnavailable
	}
	for rows.Next() {
		var orderID string
		var ticket TicketRecord
		if err := rows.Scan(&orderID, &ticket.ID, &ticket.TicketCode,
			&ticket.PassengerID, &ticket.SeatID, &ticket.Status); err != nil {
			rows.Close()
			return nil, sharding.ErrShardUnavailable
		}
		parsedOrderID, err := uuid.Parse(orderID)
		if err != nil {
			rows.Close()
			return nil, ErrReadNotFound
		}
		record, found := authoritative[parsedOrderID]
		if !found {
			rows.Close()
			return nil, ErrReadNotFound
		}
		record.Tickets = append(record.Tickets, ticket)
		authoritative[parsedOrderID] = record
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, sharding.ErrShardUnavailable
	}
	rows.Close()
	result := make([]TicketOrderRecord, len(group))
	for index, item := range group {
		record := authoritative[item.orderID]
		if len(record.Tickets) == 0 {
			return nil, ErrReadNotFound
		}
		result[index] = record
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, sharding.ErrShardUnavailable
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
