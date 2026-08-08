package app

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

var ErrReadNotFound = errors.New("app read model not found")

type TicketRecord struct{ ID, TicketCode, PassengerID, SeatID, Status string }
type TicketOrderRecord struct {
	ID, ReservationID, Status, Currency string
	TotalAmountMinor                    int64
	Tickets                             []TicketRecord
	CreatedAt                           time.Time
}
type TicketOrderRecords struct {
	Items []TicketOrderRecord
	Total int64
}
type ticketReadStore interface {
	ListTicketOrderRecords(context.Context, uuid.UUID, httpapi.PageRequest) (TicketOrderRecords, error)
	GetTicketOrderRecord(context.Context, uuid.UUID, uuid.UUID) (TicketOrderRecord, error)
	GetTicketRecord(context.Context, uuid.UUID, uuid.UUID) (TicketRecord, error)
}
type TicketQueries struct{ store ticketReadStore }

func NewTicketQueries(store ticketReadStore) *TicketQueries { return &TicketQueries{store: store} }
func (q *TicketQueries) ListTicketOrders(ctx context.Context, ownerID string, page httpapi.PageRequest) (httpapi.TicketOrderPage, error) {
	owner, err := uuid.Parse(ownerID)
	if err != nil {
		return httpapi.TicketOrderPage{}, httpapi.ErrInvalidInput
	}
	if q == nil || q.store == nil {
		return httpapi.TicketOrderPage{}, httpapi.ErrUnavailable
	}
	records, err := q.store.ListTicketOrderRecords(ctx, owner, page)
	if err != nil {
		return httpapi.TicketOrderPage{}, mapReadError(err)
	}
	items := make([]httpapi.TicketOrderView, 0, len(records.Items))
	for _, record := range records.Items {
		items = append(items, ticketOrderView(record))
	}
	return httpapi.TicketOrderPage{Items: items, Page: normalizePage(page.Page), Limit: normalizeLimit(page.Limit), Total: records.Total}, nil
}
func (q *TicketQueries) GetTicketOrder(ctx context.Context, ownerID, ticketOrderID string) (httpapi.TicketOrderView, error) {
	owner, err := uuid.Parse(ownerID)
	if err != nil {
		return httpapi.TicketOrderView{}, httpapi.ErrInvalidInput
	}
	id, err := uuid.Parse(ticketOrderID)
	if err != nil {
		return httpapi.TicketOrderView{}, httpapi.ErrInvalidInput
	}
	if q == nil || q.store == nil {
		return httpapi.TicketOrderView{}, httpapi.ErrUnavailable
	}
	record, err := q.store.GetTicketOrderRecord(ctx, owner, id)
	if err != nil {
		return httpapi.TicketOrderView{}, mapReadError(err)
	}
	return ticketOrderView(record), nil
}
func (q *TicketQueries) GetTicket(ctx context.Context, ownerID, ticketID string) (httpapi.TicketView, error) {
	owner, err := uuid.Parse(ownerID)
	if err != nil {
		return httpapi.TicketView{}, httpapi.ErrInvalidInput
	}
	id, err := uuid.Parse(ticketID)
	if err != nil {
		return httpapi.TicketView{}, httpapi.ErrInvalidInput
	}
	if q == nil || q.store == nil {
		return httpapi.TicketView{}, httpapi.ErrUnavailable
	}
	record, err := q.store.GetTicketRecord(ctx, owner, id)
	if err != nil {
		return httpapi.TicketView{}, mapReadError(err)
	}
	return ticketView(record), nil
}
func ticketOrderView(record TicketOrderRecord) httpapi.TicketOrderView {
	tickets := make([]httpapi.TicketView, 0, len(record.Tickets))
	for _, ticket := range record.Tickets {
		tickets = append(tickets, ticketView(ticket))
	}
	return httpapi.TicketOrderView{ID: record.ID, ReservationID: record.ReservationID, Status: record.Status, TotalAmountMinor: record.TotalAmountMinor, Currency: record.Currency, Tickets: tickets, CreatedAt: record.CreatedAt.UTC()}
}
func ticketView(ticket TicketRecord) httpapi.TicketView {
	return httpapi.TicketView{ID: ticket.ID, TicketCode: ticket.TicketCode, PassengerID: ticket.PassengerID, SeatID: ticket.SeatID, Status: ticket.Status}
}

func sameTicketOrderSummary(locator, authoritative TicketOrderRecord) bool {
	return locator.ID == authoritative.ID &&
		locator.ReservationID == authoritative.ReservationID &&
		locator.Status == authoritative.Status &&
		locator.TotalAmountMinor == authoritative.TotalAmountMinor &&
		locator.Currency == authoritative.Currency &&
		locator.CreatedAt.Equal(authoritative.CreatedAt)
}

func ticketOrderLocatorBy(raw string) (string, bool) {
	switch strings.TrimSpace(raw) {
	case "", "-created_at":
		return "created_at DESC, ticket_order_id DESC", true
	case "created_at":
		return "created_at ASC, ticket_order_id ASC", true
	case "status":
		return "status ASC, created_at DESC, ticket_order_id DESC", true
	case "-status":
		return "status DESC, created_at DESC, ticket_order_id DESC", true
	default:
		return "", false
	}
}
func mapReadError(err error) error {
	if errors.Is(err, httpapi.ErrInvalidInput) {
		return httpapi.ErrInvalidInput
	}
	if errors.Is(err, ErrReadNotFound) {
		return httpapi.ErrNotFound
	}
	return httpapi.ErrUnavailable
}

var _ httpapi.TicketQueries = (*TicketQueries)(nil)
