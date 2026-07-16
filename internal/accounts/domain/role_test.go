package domain_test

import (
	"errors"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/accounts/domain"
)

func TestParseRoleAcceptsOnlyMilestoneOneRoles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		want    domain.Role
		wantErr error
	}{
		{name: "customer", value: "customer", want: domain.RoleCustomer},
		{name: "admin", value: "admin", want: domain.RoleAdmin},
		{name: "operator", value: "operator", want: domain.RoleOperator},
		{name: "empty", value: "", wantErr: domain.ErrInvalidRole},
		{name: "unknown", value: "super-admin", wantErr: domain.ErrInvalidRole},
		{name: "case sensitive", value: "Customer", wantErr: domain.ErrInvalidRole},
		{name: "no whitespace normalization", value: " customer ", wantErr: domain.ErrInvalidRole},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := domain.ParseRole(tt.value)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ParseRole(%q) error = %v, want %v", tt.value, err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("ParseRole(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}
