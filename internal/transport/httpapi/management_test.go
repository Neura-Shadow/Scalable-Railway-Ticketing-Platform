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

type operatorBookingStateStub struct {
	query httpapi.OperatorBookingStateQuery
}

type operatorCommandsStub struct {
	command httpapi.OperatorCommand
	calls   int
}

func (stub *operatorCommandsStub) ExecuteOperator(_ context.Context, command httpapi.OperatorCommand) (httpapi.ResourceView, error) {
	stub.calls++
	stub.command = command
	return httpapi.ResourceView{ID: command.ResourceID}, nil
}

func (stub *operatorBookingStateStub) GetOperatorBookingState(_ context.Context, query httpapi.OperatorBookingStateQuery) (httpapi.OperatorBookingStateView, error) {
	stub.query = query
	return httpapi.OperatorBookingStateView{Kind: query.Kind, TrainRunID: query.TrainRunID,
		ResourceID: query.ResourceID, AssignmentGeneration: 5, SourceVersion: 7}, nil
}

func (s *adminCommandsStub) ExecuteAdmin(_ context.Context, command httpapi.AdminCommand) (httpapi.ResourceView, error) {
	s.command = command
	return httpapi.ResourceView{ID: "route-1"}, nil
}

func TestOperatorBookingStateGETIsRBACProtectedAndReturnsAuthoritativeVersion(t *testing.T) {
	trainRunID := "21000000-0000-4000-8000-000000000401"
	resourceID := "21000000-0000-4000-8000-000000000501"
	tests := []struct {
		path string
		kind httpapi.OperatorBookingStateKind
	}{
		{path: "/api/v1/operator/train-runs/" + trainRunID + "/fares/" + resourceID, kind: httpapi.OperatorBookingFareState},
		{path: "/api/v1/operator/train-runs/" + trainRunID + "/seats/" + resourceID + "/booking-state", kind: httpapi.OperatorBookingSeatState},
		{path: "/api/v1/operator/train-runs/" + trainRunID + "/booking-policy-version", kind: httpapi.OperatorBookingPolicyState},
	}
	for _, testCase := range tests {
		t.Run(string(testCase.kind), func(t *testing.T) {
			state := &operatorBookingStateStub{}
			router := httpapi.New(httpapi.Dependencies{
				TokenParser:          &tokenParserStub{identity: httpapi.Identity{Subject: "operator-1", Role: httpapi.RoleOperator}},
				OperatorBookingState: state,
			})
			request := httptest.NewRequest(http.MethodGet, testCase.path, nil)
			request.Header.Set("Authorization", "Bearer signed-token")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != http.StatusOK || response.Header().Get("ETag") != `"source-7"` {
				t.Fatalf("status=%d etag=%q body=%s", response.Code, response.Header().Get("ETag"), response.Body.String())
			}
			if state.query.Kind != testCase.kind || state.query.TrainRunID != trainRunID ||
				(testCase.kind != httpapi.OperatorBookingPolicyState && state.query.ResourceID != resourceID) {
				t.Fatalf("query = %+v", state.query)
			}
		})
	}
}

func TestOperatorFareMutationRequiresBoundedRateLimitAndIdempotency(t *testing.T) {
	trainRunID := "21000000-0000-4000-8000-000000000401"
	fareID := "21000000-0000-4000-8000-000000000501"
	commands := &operatorCommandsStub{}
	limiter := &rateLimiterStub{allowed: true}
	router := httpapi.New(httpapi.Dependencies{
		TokenParser: &tokenParserStub{identity: httpapi.Identity{Subject: "a61f534f-ff82-4659-81d8-64de3c99746c", Role: httpapi.RoleOperator}},
		Operator:    commands, RateLimiter: limiter, MaxRequestBodyBytes: 1024,
	})
	request := httptest.NewRequest(http.MethodPatch,
		"/api/v1/operator/train-runs/"+trainRunID+"/fares/"+fareID,
		bytes.NewBufferString(`{"expected_source_version":4,"from_stop_index":0,"to_stop_index":2,"seat_class":"standard","amount_minor":1200,"currency":"TWD"}`))
	request.Header.Set("Authorization", "Bearer signed-token")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "operator-fare-key-1")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || commands.calls != 1 {
		t.Fatalf("status=%d calls=%d body=%s", response.Code, commands.calls, response.Body.String())
	}
	if limiter.input != (httpapi.RateLimitRequest{Scope: httpapi.RateLimitOperatorBooking,
		Key: "a61f534f-ff82-4659-81d8-64de3c99746c"}) {
		t.Fatalf("rate limit input=%+v", limiter.input)
	}
	if commands.command.Action != httpapi.OperatorInstallFareSnapshot || commands.command.TrainRunID != trainRunID ||
		commands.command.ResourceID != fareID || commands.command.IdempotencyKey != "operator-fare-key-1" ||
		commands.command.FareSnapshot == nil || commands.command.FareSnapshot.ExpectedSourceVersion != 4 {
		t.Fatalf("command=%+v", commands.command)
	}
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
