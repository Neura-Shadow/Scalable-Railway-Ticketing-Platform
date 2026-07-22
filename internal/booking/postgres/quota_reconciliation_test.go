package postgres

import "testing"

func TestReservationQuotaReconciliationCountsViolations(t *testing.T) {
	t.Parallel()
	result := ReservationQuotaReconciliation{
		UsersOverHoldLimit: 2, UserRunsOverHoldLimit: 3, UsersOverPassengerLimit: 4,
	}
	if result.Violations() != 9 {
		t.Fatalf("Violations() = %d, want 9", result.Violations())
	}
}
