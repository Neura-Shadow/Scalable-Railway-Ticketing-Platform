package httpapi_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

type waitingRoomServiceStub struct {
	joined      httpapi.JoinWaitingRoomCommand
	lookedUpBy  string
	cancelledBy string
	entryID     string
	result      httpapi.WaitingRoomEntryView
	err         error
}

func (s *waitingRoomServiceStub) JoinWaitingRoom(_ context.Context, command httpapi.JoinWaitingRoomCommand) (httpapi.WaitingRoomEntryView, error) {
	s.joined = command
	return s.result, s.err
}

func (s *waitingRoomServiceStub) GetWaitingRoomEntry(_ context.Context, ownerID, entryID string) (httpapi.WaitingRoomEntryView, error) {
	s.lookedUpBy = ownerID
	s.entryID = entryID
	return s.result, s.err
}

func (s *waitingRoomServiceStub) CancelWaitingRoomEntry(_ context.Context, ownerID, entryID string) (httpapi.WaitingRoomEntryView, error) {
	s.cancelledBy = ownerID
	s.entryID = entryID
	return s.result, s.err
}

func TestWaitingRoomJoinUsesJWTIdentityAndNormalizedRequest(t *testing.T) {
	t.Parallel()

	joinedAt := time.Date(2026, time.July, 18, 2, 0, 0, 0, time.UTC)
	service := &waitingRoomServiceStub{result: httpapi.WaitingRoomEntryView{
		EntryID:             "9b0bc22f-2e71-4fb6-a496-d4fd1c13c2f8",
		Status:              "queued",
		JoinedAt:            joinedAt,
		ExpiresAt:           joinedAt.Add(time.Hour),
		ApproximatePosition: 1,
		RetryAfterSeconds:   2,
	}}
	router := httpapi.New(httpapi.Dependencies{
		TokenParser: &tokenParserStub{identity: httpapi.Identity{
			Subject: "a61f534f-ff82-4659-81d8-64de3c99746c",
			Role:    httpapi.RoleCustomer,
		}},
		WaitingRoom: service,
	})
	body := []byte(`{"train_run_id":"30a8705f-5750-4d18-9f7f-58bb80eb1c2e","origin_station_code":" tpe ","destination_station_code":" khh ","seat_class":" STANDARD ","passenger_count":2}`)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/waiting-room/entries", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer signed-token")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("POST waiting-room entry status = %d, body = %s", response.Code, response.Body.String())
	}
	if service.joined.OwnerID != "a61f534f-ff82-4659-81d8-64de3c99746c" {
		t.Fatalf("owner = %q, want JWT subject", service.joined.OwnerID)
	}
	if service.joined.OriginStationCode != "TPE" || service.joined.DestinationStationCode != "KHH" || service.joined.SeatClass != "standard" {
		t.Fatalf("normalized command = %+v", service.joined)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store, private" {
		t.Fatalf("Cache-Control = %q", got)
	}
}

func TestWaitingRoomStatusDeliversAdmissionTokenOnlyInHeaderOnce(t *testing.T) {
	t.Parallel()

	const (
		ownerID = "a61f534f-ff82-4659-81d8-64de3c99746c"
		entryID = "9b0bc22f-2e71-4fb6-a496-d4fd1c13c2f8"
	)
	raw := strings.Repeat("A", 88)
	service := &waitingRoomServiceStub{result: httpapi.WaitingRoomEntryView{
		EntryID:             entryID,
		Status:              "admitted",
		ApproximatePosition: 0,
		AdmissionToken:      raw,
	}}
	router := httpapi.New(httpapi.Dependencies{
		TokenParser: &tokenParserStub{identity: httpapi.Identity{Subject: ownerID, Role: httpapi.RoleCustomer}},
		WaitingRoom: service,
	})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/waiting-room/entries/"+entryID, nil)
	request.Header.Set("Authorization", "Bearer signed-token")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("GET waiting-room entry status = %d, body = %s", response.Code, response.Body.String())
	}
	if service.lookedUpBy != ownerID || service.entryID != entryID {
		t.Fatalf("ownership lookup = owner %q entry %q", service.lookedUpBy, service.entryID)
	}
	if got := response.Header().Get("X-Admission-Token"); got != raw {
		t.Fatalf("X-Admission-Token = %q", got)
	}
	if bytes.Contains(response.Body.Bytes(), []byte(raw)) || bytes.Contains(response.Body.Bytes(), []byte("admission_token")) {
		t.Fatal("response body exposed admission token")
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store, private" {
		t.Fatalf("Cache-Control = %q", got)
	}
}

func TestWaitingRoomCancelUsesJWTIdentityAndReturnsTerminalEntry(t *testing.T) {
	t.Parallel()

	const (
		ownerID = "a61f534f-ff82-4659-81d8-64de3c99746c"
		entryID = "9b0bc22f-2e71-4fb6-a496-d4fd1c13c2f8"
	)
	service := &waitingRoomServiceStub{result: httpapi.WaitingRoomEntryView{
		EntryID: entryID,
		Status:  "cancelled",
	}}
	router := httpapi.New(httpapi.Dependencies{
		TokenParser: &tokenParserStub{identity: httpapi.Identity{Subject: ownerID, Role: httpapi.RoleCustomer}},
		WaitingRoom: service,
	})
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/waiting-room/entries/"+entryID, nil)
	request.Header.Set("Authorization", "Bearer signed-token")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("DELETE waiting-room entry status = %d, body = %s", response.Code, response.Body.String())
	}
	if service.cancelledBy != ownerID || service.entryID != entryID {
		t.Fatalf("ownership cancellation = owner %q entry %q", service.cancelledBy, service.entryID)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store, private" {
		t.Fatalf("Cache-Control = %q", got)
	}
}

func TestWaitingRoomRoutesRejectInvalidIdentityAndUnboundedInputBeforeApplication(t *testing.T) {
	t.Parallel()

	const validBody = `{"train_run_id":"30a8705f-5750-4d18-9f7f-58bb80eb1c2e","origin_station_code":"TPE","destination_station_code":"KHH","seat_class":"standard","passenger_count":1}`
	tests := []struct {
		name       string
		role       httpapi.Role
		body       string
		bodyLimit  int64
		wantStatus int
	}{
		{
			name:       "operator cannot join customer queue",
			role:       httpapi.RoleOperator,
			body:       validBody,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "body limit",
			role:       httpapi.RoleCustomer,
			body:       validBody,
			bodyLimit:  64,
			wantStatus: http.StatusRequestEntityTooLarge,
		},
		{
			name:       "train run must be canonical UUID",
			role:       httpapi.RoleCustomer,
			body:       `{"train_run_id":"run-1","origin_station_code":"TPE","destination_station_code":"KHH","seat_class":"standard","passenger_count":1}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "seat class is bounded enum",
			role:       httpapi.RoleCustomer,
			body:       `{"train_run_id":"30a8705f-5750-4d18-9f7f-58bb80eb1c2e","origin_station_code":"TPE","destination_station_code":"KHH","seat_class":"vip","passenger_count":1}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "identity cannot come from body",
			role:       httpapi.RoleCustomer,
			body:       `{"owner_id":"attacker","train_run_id":"30a8705f-5750-4d18-9f7f-58bb80eb1c2e","origin_station_code":"TPE","destination_station_code":"KHH","seat_class":"standard","passenger_count":1}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "admission token cannot come from body",
			role:       httpapi.RoleCustomer,
			body:       `{"admission_token":"body-token","train_run_id":"30a8705f-5750-4d18-9f7f-58bb80eb1c2e","origin_station_code":"TPE","destination_station_code":"KHH","seat_class":"standard","passenger_count":1}`,
			wantStatus: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			service := &waitingRoomServiceStub{}
			router := httpapi.New(httpapi.Dependencies{
				TokenParser:         &tokenParserStub{identity: httpapi.Identity{Subject: "a61f534f-ff82-4659-81d8-64de3c99746c", Role: tt.role}},
				WaitingRoom:         service,
				MaxRequestBodyBytes: tt.bodyLimit,
			})
			request := httptest.NewRequest(http.MethodPost, "/api/v1/waiting-room/entries", bytes.NewBufferString(tt.body))
			request.Header.Set("Authorization", "Bearer signed-token")
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()

			router.ServeHTTP(response, request)

			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, tt.wantStatus, response.Body.String())
			}
			if service.joined.OwnerID != "" {
				t.Fatalf("application called with %+v", service.joined)
			}
			if got := response.Header().Get("Cache-Control"); got != "no-store, private" {
				t.Fatalf("Cache-Control = %q", got)
			}
		})
	}
}

func TestWaitingRoomQueueFullUsesTypedErrorAndBoundedRetryAfter(t *testing.T) {
	t.Parallel()

	service := &waitingRoomServiceStub{err: httpapi.WithRetryAfter(httpapi.ErrQueueFull, 600)}
	router := httpapi.New(httpapi.Dependencies{
		TokenParser: &tokenParserStub{identity: httpapi.Identity{
			Subject: "a61f534f-ff82-4659-81d8-64de3c99746c",
			Role:    httpapi.RoleCustomer,
		}},
		WaitingRoom: service,
	})
	body := []byte(`{"train_run_id":"30a8705f-5750-4d18-9f7f-58bb80eb1c2e","origin_station_code":"TPE","destination_station_code":"KHH","seat_class":"standard","passenger_count":1}`)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/waiting-room/entries", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer signed-token")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "60" {
		t.Fatalf("queue full status = %d Retry-After = %q, body = %s", response.Code, response.Header().Get("Retry-After"), response.Body.String())
	}
	if !bytes.Contains(response.Body.Bytes(), []byte(`"code":"queue_full"`)) {
		t.Fatalf("queue full error contract = %s", response.Body.String())
	}
}

func TestWaitingRoomNeverEmitsTokenForNonAdmittedEntry(t *testing.T) {
	t.Parallel()

	const entryID = "9b0bc22f-2e71-4fb6-a496-d4fd1c13c2f8"
	service := &waitingRoomServiceStub{result: httpapi.WaitingRoomEntryView{
		EntryID: entryID, Status: "queued", AdmissionToken: "must_not_escape",
	}}
	router := httpapi.New(httpapi.Dependencies{
		TokenParser: &tokenParserStub{identity: httpapi.Identity{
			Subject: "a61f534f-ff82-4659-81d8-64de3c99746c",
			Role:    httpapi.RoleCustomer,
		}},
		WaitingRoom: service,
	})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/waiting-room/entries/"+entryID, nil)
	request.Header.Set("Authorization", "Bearer signed-token")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("invalid token delivery status = %d, body = %s", response.Code, response.Body.String())
	}
	if response.Header().Get("X-Admission-Token") != "" || bytes.Contains(response.Body.Bytes(), []byte("must_not_escape")) {
		t.Fatal("non-admitted response exposed admission token")
	}
}
