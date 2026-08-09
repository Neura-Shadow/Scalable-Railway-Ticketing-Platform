package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

type ticketQueriesStub struct {
	ownerID  string
	ticketID string
}

func (*ticketQueriesStub) ListTicketOrders(context.Context, string, httpapi.PageRequest) (httpapi.TicketOrderPage, error) {
	return httpapi.TicketOrderPage{}, nil
}
func (*ticketQueriesStub) GetTicketOrder(context.Context, string, string) (httpapi.TicketOrderView, error) {
	return httpapi.TicketOrderView{}, nil
}
func (stub *ticketQueriesStub) GetTicket(_ context.Context, ownerID, ticketID string) (httpapi.TicketView, error) {
	stub.ownerID = ownerID
	stub.ticketID = ticketID
	return httpapi.TicketView{
		ID: ticketID, TicketCode: "opaque-ticket-code", PassengerID: "passenger-id",
		SeatID: "seat-id", Status: "refund_pending",
	}, nil
}

func TestGetTicketUsesAuthenticatedOwnerAndSafeContract(t *testing.T) {
	t.Parallel()
	queries := &ticketQueriesStub{}
	router := httpapi.New(httpapi.Dependencies{
		TokenParser: &tokenParserStub{identity: httpapi.Identity{Subject: "owner-from-jwt", Role: httpapi.RoleCustomer}},
		Tickets:     queries,
	})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/tickets/8fb87770-53b0-4868-a10b-3fa256c0ef63", nil)
	request.Header.Set("Authorization", "Bearer signed-token")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if queries.ownerID != "owner-from-jwt" || queries.ticketID != "8fb87770-53b0-4868-a10b-3fa256c0ef63" {
		t.Fatalf("owner=%q ticket=%q", queries.ownerID, queries.ticketID)
	}
	body := response.Body.String()
	for _, forbidden := range []string{"provider", "shard", "assignment_generation", "payment_operation"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("response exposed internal field %q: %s", forbidden, body)
		}
	}
}
