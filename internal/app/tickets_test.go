package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

type ticketReadStoreFake struct {
	owner  uuid.UUID
	id     uuid.UUID
	page   TicketOrderRecords
	record TicketOrderRecord
	err    error
}

func (f *ticketReadStoreFake) ListTicketOrderRecords(_ context.Context, owner uuid.UUID, _ httpapi.PageRequest) (TicketOrderRecords, error) {
	f.owner = owner
	return f.page, f.err
}
func (f *ticketReadStoreFake) GetTicketOrderRecord(_ context.Context, owner, id uuid.UUID) (TicketOrderRecord, error) {
	f.owner, f.id = owner, id
	return f.record, f.err
}

func TestTicketQueriesKeepOwnerScopeAndMapNestedTickets(t *testing.T) {
	owner, order := uuid.New(), uuid.New()
	created := time.Now().UTC()
	store := &ticketReadStoreFake{page: TicketOrderRecords{Items: []TicketOrderRecord{{ID: order.String(), ReservationID: "reservation", Status: "confirmed", TotalAmountMinor: 1200, Currency: "TWD", CreatedAt: created, Tickets: []TicketRecord{{ID: "ticket", TicketCode: "TKT-code", PassengerID: "passenger", SeatID: "seat", Status: "active"}}}}, Total: 1}}
	page, err := NewTicketQueries(store).ListTicketOrders(context.Background(), owner.String(), httpapi.PageRequest{Page: 1, Limit: 20})
	if err != nil || store.owner != owner || len(page.Items) != 1 || page.Items[0].Tickets[0].PassengerID != "passenger" {
		t.Fatalf("page=%#v owner=%v err=%v", page, store.owner, err)
	}
}

func TestTicketQueriesHideCrossOwnerAndPersistenceDetails(t *testing.T) {
	owner, order := uuid.New(), uuid.New()
	store := &ticketReadStoreFake{err: ErrReadNotFound}
	_, err := NewTicketQueries(store).GetTicketOrder(context.Background(), owner.String(), order.String())
	if err != httpapi.ErrNotFound {
		t.Fatalf("not found error=%v", err)
	}
	store.err = errors.New("sql secret")
	_, err = NewTicketQueries(store).GetTicketOrder(context.Background(), owner.String(), order.String())
	if err != httpapi.ErrUnavailable {
		t.Fatalf("persistence error=%v", err)
	}
}
