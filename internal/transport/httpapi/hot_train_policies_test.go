package httpapi_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

type hotTrainPolicyServiceStub struct {
	created  httpapi.CreateHotTrainPolicyCommand
	updated  httpapi.UpdateHotTrainPolicyCommand
	disabled httpapi.DisableHotTrainPolicyCommand
	page     httpapi.PageRequest
	policyID string
	actorID  string
	result   httpapi.HotTrainPolicyView
	list     httpapi.HotTrainPolicyPage
	err      error
}

func (s *hotTrainPolicyServiceStub) ListHotTrainPolicies(_ context.Context, page httpapi.PageRequest) (httpapi.HotTrainPolicyPage, error) {
	s.page = page
	return s.list, s.err
}

func (s *hotTrainPolicyServiceStub) GetHotTrainPolicy(_ context.Context, actorID, policyID string) (httpapi.HotTrainPolicyView, error) {
	s.actorID = actorID
	s.policyID = policyID
	return s.result, s.err
}

func (s *hotTrainPolicyServiceStub) CreateHotTrainPolicy(_ context.Context, command httpapi.CreateHotTrainPolicyCommand) (httpapi.HotTrainPolicyView, error) {
	s.created = command
	return s.result, s.err
}

func (s *hotTrainPolicyServiceStub) UpdateHotTrainPolicy(_ context.Context, command httpapi.UpdateHotTrainPolicyCommand) (httpapi.HotTrainPolicyView, error) {
	s.updated = command
	return s.result, s.err
}

func (s *hotTrainPolicyServiceStub) DisableHotTrainPolicy(_ context.Context, command httpapi.DisableHotTrainPolicyCommand) (httpapi.HotTrainPolicyView, error) {
	s.disabled = command
	return s.result, s.err
}

func TestOperatorCreatesBoundedHotTrainPolicy(t *testing.T) {
	t.Parallel()

	const (
		actorID   = "a61f534f-ff82-4659-81d8-64de3c99746c"
		runID     = "30a8705f-5750-4d18-9f7f-58bb80eb1c2e"
		policyID  = "854f3ec1-7874-4a4a-aaf8-13b8a0ca3009"
		requestID = "policy-create-1"
	)
	service := &hotTrainPolicyServiceStub{result: httpapi.HotTrainPolicyView{
		ID: policyID, TrainRunID: runID, SeatClass: "standard", Enabled: true, Version: 1,
	}}
	limiter := &rateLimiterStub{allowed: true}
	router := httpapi.New(httpapi.Dependencies{
		TokenParser:      &tokenParserStub{identity: httpapi.Identity{Subject: actorID, Role: httpapi.RoleOperator}},
		HotTrainPolicies: service,
		RateLimiter:      limiter,
	})
	body := []byte(`{"train_run_id":"30a8705f-5750-4d18-9f7f-58bb80eb1c2e","seat_class":" STANDARD ","max_queue_size":1000,"admission_rate_per_second":50,"max_inflight_admissions":100,"admission_token_ttl_seconds":60,"processing_lease_seconds":10,"queue_entry_ttl_seconds":3600}`)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/operator/hot-train-policies", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer signed-token")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Correlation-ID", requestID)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("POST policy status = %d, body = %s", response.Code, response.Body.String())
	}
	if service.created.ActorID != actorID || service.created.CorrelationID != requestID {
		t.Fatalf("mutation metadata = %+v", service.created)
	}
	if service.created.TrainRunID != runID || service.created.SeatClass != "standard" {
		t.Fatalf("normalized policy command = %+v", service.created)
	}
	if service.created.MaxQueueSize != 1000 || service.created.ProcessingLeaseSeconds != 10 {
		t.Fatalf("policy limits = %+v", service.created)
	}
	if limiter.input != (httpapi.RateLimitRequest{Scope: httpapi.RateLimitPolicyMutation, Key: actorID}) {
		t.Fatalf("rate-limit input = %+v", limiter.input)
	}
}

func TestAdminListsHotTrainPoliciesWithBoundedPagination(t *testing.T) {
	t.Parallel()

	const actorID = "a61f534f-ff82-4659-81d8-64de3c99746c"
	service := &hotTrainPolicyServiceStub{list: httpapi.HotTrainPolicyPage{
		Items: []httpapi.HotTrainPolicyView{{
			ID: "854f3ec1-7874-4a4a-aaf8-13b8a0ca3009",
		}},
		Page: 2, Limit: 25, Total: 26,
	}}
	router := httpapi.New(httpapi.Dependencies{
		TokenParser:      &tokenParserStub{identity: httpapi.Identity{Subject: actorID, Role: httpapi.RoleAdmin}},
		HotTrainPolicies: service,
	})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/operator/hot-train-policies?page=2&limit=25&sort=-updated_at", nil)
	request.Header.Set("Authorization", "Bearer signed-token")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("GET policy list status = %d, body = %s", response.Code, response.Body.String())
	}
	if service.page != (httpapi.PageRequest{Page: 2, Limit: 25, Sort: "-updated_at"}) {
		t.Fatalf("page request = %+v", service.page)
	}
}

func TestOperatorGetsOneHotTrainPolicyByCanonicalID(t *testing.T) {
	t.Parallel()

	const (
		actorID  = "a61f534f-ff82-4659-81d8-64de3c99746c"
		policyID = "854f3ec1-7874-4a4a-aaf8-13b8a0ca3009"
	)
	service := &hotTrainPolicyServiceStub{result: httpapi.HotTrainPolicyView{ID: policyID}}
	router := httpapi.New(httpapi.Dependencies{
		TokenParser:      &tokenParserStub{identity: httpapi.Identity{Subject: actorID, Role: httpapi.RoleOperator}},
		HotTrainPolicies: service,
	})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/operator/hot-train-policies/"+policyID, nil)
	request.Header.Set("Authorization", "Bearer signed-token")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("GET policy status = %d, body = %s", response.Code, response.Body.String())
	}
	if service.actorID != actorID || service.policyID != policyID {
		t.Fatalf("policy lookup = actor %q policy %q", service.actorID, service.policyID)
	}
}

func TestAdminUpdatesHotTrainPolicyWithExpectedVersion(t *testing.T) {
	t.Parallel()

	const (
		actorID   = "a61f534f-ff82-4659-81d8-64de3c99746c"
		policyID  = "854f3ec1-7874-4a4a-aaf8-13b8a0ca3009"
		requestID = "policy-update-1"
	)
	service := &hotTrainPolicyServiceStub{result: httpapi.HotTrainPolicyView{ID: policyID, Version: 4}}
	router := httpapi.New(httpapi.Dependencies{
		TokenParser:      &tokenParserStub{identity: httpapi.Identity{Subject: actorID, Role: httpapi.RoleAdmin}},
		HotTrainPolicies: service,
		RateLimiter:      &rateLimiterStub{allowed: true},
	})
	body := []byte(`{"expected_version":3,"enabled":true,"max_queue_size":2000,"admission_rate_per_second":60,"max_inflight_admissions":120,"admission_token_ttl_seconds":90,"processing_lease_seconds":15,"queue_entry_ttl_seconds":3600}`)
	request := httptest.NewRequest(http.MethodPut, "/api/v1/operator/hot-train-policies/"+policyID, bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer signed-token")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Correlation-ID", requestID)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("PUT policy status = %d, body = %s", response.Code, response.Body.String())
	}
	if service.updated.ActorID != actorID || service.updated.PolicyID != policyID ||
		service.updated.CorrelationID != requestID || service.updated.ExpectedVersion != 3 {
		t.Fatalf("update command = %+v", service.updated)
	}
	if service.updated.Enabled == nil || !*service.updated.Enabled {
		t.Fatalf("update enabled = %#v, want true", service.updated.Enabled)
	}
}

func TestOperatorDisablesHotTrainPolicyAsSoftDelete(t *testing.T) {
	t.Parallel()

	const (
		actorID   = "a61f534f-ff82-4659-81d8-64de3c99746c"
		policyID  = "854f3ec1-7874-4a4a-aaf8-13b8a0ca3009"
		requestID = "policy-disable-1"
	)
	service := &hotTrainPolicyServiceStub{result: httpapi.HotTrainPolicyView{ID: policyID, Enabled: false, Version: 4}}
	router := httpapi.New(httpapi.Dependencies{
		TokenParser:      &tokenParserStub{identity: httpapi.Identity{Subject: actorID, Role: httpapi.RoleOperator}},
		HotTrainPolicies: service,
		RateLimiter:      &rateLimiterStub{allowed: true},
	})
	request := httptest.NewRequest(
		http.MethodDelete,
		"/api/v1/operator/hot-train-policies/"+policyID,
		bytes.NewReader([]byte(`{"expected_version":3}`)),
	)
	request.Header.Set("Authorization", "Bearer signed-token")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Correlation-ID", requestID)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("DELETE policy status = %d, body = %s", response.Code, response.Body.String())
	}
	if service.disabled.ActorID != actorID || service.disabled.PolicyID != policyID ||
		service.disabled.CorrelationID != requestID || service.disabled.ExpectedVersion != 3 {
		t.Fatalf("disable command = %+v", service.disabled)
	}
}

func TestHotTrainPolicyMutationsFailClosedWhenRateLimiterIsMissingUnavailableOrExceeded(t *testing.T) {
	t.Parallel()

	const (
		actorID = "a61f534f-ff82-4659-81d8-64de3c99746c"
		body    = `{"train_run_id":"30a8705f-5750-4d18-9f7f-58bb80eb1c2e","seat_class":"standard","max_queue_size":1000,"admission_rate_per_second":50,"max_inflight_admissions":100,"admission_token_ttl_seconds":60,"processing_lease_seconds":10,"queue_entry_ttl_seconds":3600}`
	)
	tests := []struct {
		name       string
		limiter    httpapi.RateLimiter
		wantStatus int
	}{
		{name: "missing", wantStatus: http.StatusServiceUnavailable},
		{name: "unavailable", limiter: &rateLimiterStub{err: errors.New("redis unavailable")}, wantStatus: http.StatusServiceUnavailable},
		{name: "exceeded", limiter: &rateLimiterStub{allowed: false}, wantStatus: http.StatusTooManyRequests},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service := &hotTrainPolicyServiceStub{}
			router := httpapi.New(httpapi.Dependencies{
				TokenParser:      &tokenParserStub{identity: httpapi.Identity{Subject: actorID, Role: httpapi.RoleOperator}},
				HotTrainPolicies: service,
				RateLimiter:      test.limiter,
			})
			request := httptest.NewRequest(http.MethodPost, "/api/v1/operator/hot-train-policies", bytes.NewBufferString(body))
			request.Header.Set("Authorization", "Bearer signed-token")
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()

			router.ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, test.wantStatus, response.Body.String())
			}
			if service.created.ActorID != "" {
				t.Fatalf("application called with %+v", service.created)
			}
		})
	}
}

func TestHotTrainPolicyCreateRejectsUnauthorizedOrUnboundedInputBeforeApplication(t *testing.T) {
	t.Parallel()

	const validBody = `{"train_run_id":"30a8705f-5750-4d18-9f7f-58bb80eb1c2e","seat_class":"standard","max_queue_size":1000,"admission_rate_per_second":50,"max_inflight_admissions":100,"admission_token_ttl_seconds":60,"processing_lease_seconds":10,"queue_entry_ttl_seconds":3600}`
	tests := []struct {
		name          string
		role          httpapi.Role
		body          string
		bodyLimit     int64
		correlationID string
		wantStatus    int
	}{
		{
			name:       "customer forbidden",
			role:       httpapi.RoleCustomer,
			body:       validBody,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "body limit",
			role:       httpapi.RoleOperator,
			body:       validBody,
			bodyLimit:  64,
			wantStatus: http.StatusRequestEntityTooLarge,
		},
		{
			name:       "train run must be canonical UUID",
			role:       httpapi.RoleOperator,
			body:       `{"train_run_id":"run-1","seat_class":"standard","max_queue_size":1000,"admission_rate_per_second":50,"max_inflight_admissions":100,"admission_token_ttl_seconds":60,"processing_lease_seconds":10,"queue_entry_ttl_seconds":3600}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "seat class is bounded enum",
			role:       httpapi.RoleOperator,
			body:       `{"train_run_id":"30a8705f-5750-4d18-9f7f-58bb80eb1c2e","seat_class":"vip","max_queue_size":1000,"admission_rate_per_second":50,"max_inflight_admissions":100,"admission_token_ttl_seconds":60,"processing_lease_seconds":10,"queue_entry_ttl_seconds":3600}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unsafe lease rejected",
			role:       httpapi.RoleOperator,
			body:       `{"train_run_id":"30a8705f-5750-4d18-9f7f-58bb80eb1c2e","seat_class":"standard","max_queue_size":1000,"admission_rate_per_second":50,"max_inflight_admissions":100,"admission_token_ttl_seconds":60,"processing_lease_seconds":60,"queue_entry_ttl_seconds":3600}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:          "correlation header bounded",
			role:          httpapi.RoleOperator,
			body:          validBody,
			correlationID: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
			wantStatus:    http.StatusBadRequest,
		},
		{
			name:       "unknown body field rejected",
			role:       httpapi.RoleOperator,
			body:       `{"unexpected":true,"train_run_id":"30a8705f-5750-4d18-9f7f-58bb80eb1c2e","seat_class":"standard","max_queue_size":1000,"admission_rate_per_second":50,"max_inflight_admissions":100,"admission_token_ttl_seconds":60,"processing_lease_seconds":10,"queue_entry_ttl_seconds":3600}`,
			wantStatus: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			service := &hotTrainPolicyServiceStub{}
			router := httpapi.New(httpapi.Dependencies{
				TokenParser:         &tokenParserStub{identity: httpapi.Identity{Subject: "a61f534f-ff82-4659-81d8-64de3c99746c", Role: tt.role}},
				HotTrainPolicies:    service,
				MaxRequestBodyBytes: tt.bodyLimit,
			})
			request := httptest.NewRequest(http.MethodPost, "/api/v1/operator/hot-train-policies", bytes.NewBufferString(tt.body))
			request.Header.Set("Authorization", "Bearer signed-token")
			request.Header.Set("Content-Type", "application/json")
			if tt.correlationID != "" {
				request.Header.Set("X-Correlation-ID", tt.correlationID)
			}
			response := httptest.NewRecorder()

			router.ServeHTTP(response, request)

			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, tt.wantStatus, response.Body.String())
			}
			if service.created.ActorID != "" {
				t.Fatalf("application called with %+v", service.created)
			}
		})
	}
}

func TestHotTrainPolicyMutationRejectsInvalidIDVersionAndFalseUpdate(t *testing.T) {
	t.Parallel()

	const validLimits = `"max_queue_size":1000,"admission_rate_per_second":50,"max_inflight_admissions":100,"admission_token_ttl_seconds":60,"processing_lease_seconds":10,"queue_entry_ttl_seconds":3600`
	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{
			name:       "get invalid policy ID",
			method:     http.MethodGet,
			path:       "/api/v1/operator/hot-train-policies/not-a-uuid",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "update missing expected version",
			method:     http.MethodPut,
			path:       "/api/v1/operator/hot-train-policies/854f3ec1-7874-4a4a-aaf8-13b8a0ca3009",
			body:       `{` + validLimits + `}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "update false must use disable route",
			method:     http.MethodPut,
			path:       "/api/v1/operator/hot-train-policies/854f3ec1-7874-4a4a-aaf8-13b8a0ca3009",
			body:       `{"expected_version":1,"enabled":false,` + validLimits + `}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "disable requires positive expected version",
			method:     http.MethodDelete,
			path:       "/api/v1/operator/hot-train-policies/854f3ec1-7874-4a4a-aaf8-13b8a0ca3009",
			body:       `{"expected_version":0}`,
			wantStatus: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			service := &hotTrainPolicyServiceStub{}
			router := httpapi.New(httpapi.Dependencies{
				TokenParser:      &tokenParserStub{identity: httpapi.Identity{Subject: "a61f534f-ff82-4659-81d8-64de3c99746c", Role: httpapi.RoleOperator}},
				HotTrainPolicies: service,
			})
			request := httptest.NewRequest(tt.method, tt.path, bytes.NewBufferString(tt.body))
			request.Header.Set("Authorization", "Bearer signed-token")
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()

			router.ServeHTTP(response, request)

			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, tt.wantStatus, response.Body.String())
			}
			if service.policyID != "" || service.updated.ActorID != "" || service.disabled.ActorID != "" {
				t.Fatal("application called for invalid mutation")
			}
		})
	}
}
