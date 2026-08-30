package main

import (
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/platform/config"
)

func TestPaymentPassRunsOnlyForActiveWriteRole(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		role   config.DeploymentRole
		writes bool
		want   bool
	}{
		{name: "active writer", role: config.DeploymentRoleActive, writes: true, want: true},
		{name: "active fenced", role: config.DeploymentRoleActive, writes: false, want: false},
		{name: "passive", role: config.DeploymentRolePassive, writes: false, want: false},
		{name: "recovery", role: config.DeploymentRoleRecovery, writes: false, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := paymentPassEnabled(config.Config{DeploymentRole: test.role, RegionalWritesEnabled: test.writes})
			if got != test.want {
				t.Fatalf("paymentPassEnabled() = %v, want %v", got, test.want)
			}
		})
	}
}
