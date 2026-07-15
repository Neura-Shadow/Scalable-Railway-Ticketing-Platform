package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

type adminCommandsStub struct {
	command httpapi.AdminCommand
}

func (s *adminCommandsStub) ExecuteAdmin(_ context.Context, command httpapi.AdminCommand) (httpapi.ResourceView, error) {
	s.command = command
	return httpapi.ResourceView{ID: "route-1"}, nil
}

func TestAdminRouteHTTPAcceptsCompleteTimetableShape(t *testing.T) {
	admin := &adminCommandsStub{}
	router := httpapi.New(httpapi.Dependencies{
		TokenParser: &tokenParserStub{identity: httpapi.Identity{Subject: "admin-1", Role: httpapi.RoleAdmin}},
		Admin:       admin,
	})
	body := []byte(`{"code":"WEST","name":"Western Line","operating_timezone":"Asia/Taipei","stops":[{"station_code":"TPE","stop_index":0,"arrival_offset_minutes":0,"departure_offset_minutes":5},{"station_code":"KHH","stop_index":1,"arrival_offset_minutes":120,"departure_offset_minutes":125}]}`)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/routes", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer signed-token")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("create route status = %d body=%s", response.Code, response.Body.String())
	}
	var result httpapi.ResourceView
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.ID != "route-1" {
		t.Fatalf("create route result=%#v error=%v", result, err)
	}
	if admin.command.ActorID != "admin-1" || admin.command.Route == nil || len(admin.command.Route.Stops) != 2 || admin.command.Route.OperatingTimezone != "Asia/Taipei" {
		t.Fatalf("admin command = %#v", admin.command)
	}
}
