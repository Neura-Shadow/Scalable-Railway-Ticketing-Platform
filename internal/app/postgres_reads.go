package app

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresReads implements the owner-scoped reservation and ticket read
// models that are intentionally absent from the command stores.
type PostgresReads struct{ pool *pgxpool.Pool }

func NewPostgresReads(pool *pgxpool.Pool) *PostgresReads { return &PostgresReads{pool: pool} }

func (r *PostgresReads) GetReservationDetail(ctx context.Context, owner, reservation uuid.UUID) (ReservationDetail, error) {
	if r == nil || r.pool == nil || owner == uuid.Nil || reservation == uuid.Nil {
		return ReservationDetail{}, ErrReadNotFound
	}
	var d ReservationDetail
	var expires time.Time
	err := r.pool.QueryRow(ctx, `
SELECT r.id::text, r.status, r.train_run_id::text,
       origin_station.code, destination_station.code, r.seat_class, r.expires_at,
       COALESCE(array_agg(rs.passenger_id::text ORDER BY rs.passenger_id::text)
                FILTER (WHERE rs.passenger_id IS NOT NULL), '{}')
FROM reservations r
JOIN train_runs tr ON tr.id = r.train_run_id
JOIN route_stops origin ON origin.route_id = tr.route_id AND origin.stop_index = r.from_stop_index
JOIN stations origin_station ON origin_station.id = origin.station_id
JOIN route_stops destination ON destination.route_id = tr.route_id AND destination.stop_index = r.to_stop_index
JOIN stations destination_station ON destination_station.id = destination.station_id
LEFT JOIN reservation_seats rs ON rs.reservation_id = r.id
WHERE r.id = $1 AND r.user_id = $2
GROUP BY r.id, origin_station.code, destination_station.code`, reservation, owner).Scan(&d.ID, &d.Status, &d.TrainRunID, &d.OriginStationCode, &d.DestinationStationCode, &d.SeatClass, &expires, &d.PassengerIDs)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReservationDetail{}, ErrReadNotFound
	}
	if err != nil {
		return ReservationDetail{}, err
	}
	expires = expires.UTC()
	d.ExpiresAt = &expires
	return d, nil
}

func (r *PostgresReads) ListTicketOrderRecords(ctx context.Context, owner uuid.UUID, page httpapi.PageRequest) (TicketOrderRecords, error) {
	if r == nil || r.pool == nil || owner == uuid.Nil {
		return TicketOrderRecords{}, ErrReadNotFound
	}
	p, l := normalizePage(page.Page), normalizeLimit(page.Limit)
	orderBy, ok := ticketOrderBy(page.Sort)
	if !ok {
		return TicketOrderRecords{}, httpapi.ErrInvalidInput
	}
	rows, err := r.pool.Query(ctx, `
SELECT id::text, reservation_id::text, status, total_amount_minor, currency, created_at,
       count(*) OVER()
FROM ticket_orders WHERE user_id = $1
ORDER BY `+orderBy+` LIMIT $2 OFFSET $3`, owner, l, (p-1)*l)
	if err != nil {
		return TicketOrderRecords{}, err
	}
	defer rows.Close()
	result := TicketOrderRecords{Items: make([]TicketOrderRecord, 0)}
	for rows.Next() {
		var record TicketOrderRecord
		if err := rows.Scan(&record.ID, &record.ReservationID, &record.Status, &record.TotalAmountMinor, &record.Currency, &record.CreatedAt, &result.Total); err != nil {
			return TicketOrderRecords{}, err
		}
		record.CreatedAt = record.CreatedAt.UTC()
		record.Tickets, err = r.loadTickets(ctx, record.ID)
		if err != nil {
			return TicketOrderRecords{}, err
		}
		result.Items = append(result.Items, record)
	}
	if err := rows.Err(); err != nil {
		return TicketOrderRecords{}, err
	}
	return result, nil
}
func (r *PostgresReads) GetTicketOrderRecord(ctx context.Context, owner, id uuid.UUID) (TicketOrderRecord, error) {
	if r == nil || r.pool == nil || owner == uuid.Nil || id == uuid.Nil {
		return TicketOrderRecord{}, ErrReadNotFound
	}
	var record TicketOrderRecord
	err := r.pool.QueryRow(ctx, `SELECT id::text, reservation_id::text, status, total_amount_minor, currency, created_at FROM ticket_orders WHERE id=$1 AND user_id=$2`, id, owner).Scan(&record.ID, &record.ReservationID, &record.Status, &record.TotalAmountMinor, &record.Currency, &record.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return TicketOrderRecord{}, ErrReadNotFound
	}
	if err != nil {
		return TicketOrderRecord{}, err
	}
	record.CreatedAt = record.CreatedAt.UTC()
	record.Tickets, err = r.loadTickets(ctx, record.ID)
	return record, err
}
func (r *PostgresReads) loadTickets(ctx context.Context, orderID string) ([]TicketRecord, error) {
	rows, err := r.pool.Query(ctx, `
SELECT t.id::text,t.ticket_code,rs.passenger_id::text,rs.seat_id::text,t.status
FROM tickets t JOIN reservation_seats rs ON rs.id=t.reservation_seat_id
WHERE t.ticket_order_id=$1 ORDER BY t.id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]TicketRecord, 0)
	for rows.Next() {
		var item TicketRecord
		if err := rows.Scan(&item.ID, &item.TicketCode, &item.PassengerID, &item.SeatID, &item.Status); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
func ticketOrderBy(raw string) (string, bool) {
	switch strings.TrimSpace(raw) {
	case "", "-created_at":
		return "created_at DESC, id DESC", true
	case "created_at":
		return "created_at ASC, id ASC", true
	case "status":
		return "status ASC, created_at DESC, id DESC", true
	case "-status":
		return "status DESC, created_at DESC, id DESC", true
	default:
		return "", false
	}
}

var (
	_ reservationReader = (*PostgresReads)(nil)
	_ ticketReadStore   = (*PostgresReads)(nil)
)
