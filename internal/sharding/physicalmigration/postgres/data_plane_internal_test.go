package postgres

import (
	"slices"
	"testing"
)

func TestPhysicalMigrationCopiesOperatorReceiptVersions(t *testing.T) {
	t.Parallel()

	for _, spec := range migrationTables {
		if spec.name != "booking_command_receipts" {
			continue
		}
		if !slices.Contains(spec.columns, "result_source_version") {
			t.Fatal("physical booking-command receipt copy omits result_source_version")
		}
		if !slices.Contains(spec.columns, "result_booking_policy_version") {
			t.Fatal("physical booking-command receipt copy omits result_booking_policy_version")
		}
		return
	}
	t.Fatal("physical migration omits booking_command_receipts")
}
