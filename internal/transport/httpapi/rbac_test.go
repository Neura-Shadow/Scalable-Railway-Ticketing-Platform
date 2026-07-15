package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
)

func TestAdminAndOperatorWriteRoutesEnforceExplicitRoles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		role httpapi.Role
		path string
	}{
		{"operator cannot write admin station", httpapi.RoleOperator, "/api/v1/admin/stations"},
		{"admin cannot initialize operator inventory", httpapi.RoleAdmin, "/api/v1/operator/train-runs/run-1/inventory"},
		{"customer cannot write admin station", httpapi.RoleCustomer, "/api/v1/admin/stations"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := &tokenParserStub{identity: httpapi.Identity{Subject: "actor-1", Role: tt.role}}
			router := httpapi.New(httpapi.Dependencies{TokenParser: parser})
			request := httptest.NewRequest(http.MethodPost, tt.path, nil)
			request.Header.Set("Authorization", "Bearer signed-token")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want 403", response.Code)
			}
		})
	}
}
