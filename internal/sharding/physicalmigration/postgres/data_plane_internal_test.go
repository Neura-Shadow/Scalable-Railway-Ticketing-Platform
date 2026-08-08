package postgres

import (
	"slices"
	"testing"
)

func TestPhysicalMigrationV2FixedTableBoundary(t *testing.T) {
	t.Parallel()

	want := []string{
		"train_run_booking_snapshots", "booking_seat_catalog", "booking_fare_snapshots",
		"seat_inventory", "reservations", "reservation_seats", "ticket_orders", "tickets",
		"idempotency_records", "booking_command_receipts", "payment_command_receipts",
		"ticket_issuance_receipts", "payment_refund_receipts",
		"payment_compensation_receipts", "outbox_events",
	}
	got := make([]string, 0, len(migrationTables))
	for _, spec := range migrationTables {
		got = append(got, spec.name)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("physical v2 migration table order = %v, want %v", got, want)
	}
}

func TestPhysicalMigrationV2CopiesPaymentSnapshots(t *testing.T) {
	t.Parallel()

	assertColumns := func(table string, want ...string) {
		t.Helper()
		for _, spec := range migrationTables {
			if spec.name != table {
				continue
			}
			for _, column := range want {
				if !slices.Contains(spec.columns, column) {
					t.Fatalf("physical v2 %s copy omits %s", table, column)
				}
			}
			return
		}
		t.Fatalf("physical v2 migration omits %s", table)
	}

	assertColumns("reservations", "payment_intent_id", "payment_amount_minor",
		"payment_currency", "payment_grace_expires_at")
	assertColumns("ticket_orders", "payment_intent_id", "payment_currency",
		"authorized_amount_minor", "captured_amount_minor", "refunded_amount_minor")
	assertColumns("payment_command_receipts", "command_id", "payment_intent_id",
		"request_fingerprint", "committed_at")
	assertColumns("ticket_issuance_receipts", "issuance_id", "payment_operation_id",
		"capture_proof_hash", "issued_ticket_count")
	assertColumns("payment_refund_receipts", "refund_operation_id", "refund_proof_hash",
		"captured_amount_minor", "refunded_amount_minor")
	assertColumns("payment_compensation_receipts", "compensation_id", "refund_receipt_id",
		"released_seat_count", "cancelled_ticket_count")
}

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
