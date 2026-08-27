package postgres

import (
	"slices"
	"strings"
	"testing"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physicalmigration"
	"github.com/google/uuid"
)

func TestPhysicalMigrationV3FixedTableBoundary(t *testing.T) {
	t.Parallel()

	want := []string{
		"train_run_booking_snapshots", "booking_seat_catalog", "booking_fare_snapshots",
		"seat_inventory", "reservations", "reservation_seats", "ticket_orders", "tickets",
		"idempotency_records", "booking_command_receipts", "payment_command_receipts",
		"ticket_issuance_receipts", "payment_refund_receipts",
		"payment_compensation_receipts", "ticket_refund_prepare_receipts", "outbox_events",
		"ticket_refund_compensation_receipts", "selected_ticket_refund_receipts",
	}
	got := make([]string, 0, len(migrationTables))
	for _, spec := range migrationTables {
		got = append(got, spec.name)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("physical v3 migration table order = %v, want %v", got, want)
	}
}

func TestPhysicalMigrationV3CopiesSelectedTicketRefundEvidence(t *testing.T) {
	t.Parallel()

	assertColumns := func(table string, want ...string) {
		t.Helper()
		for _, spec := range migrationTables {
			if spec.name != table {
				continue
			}
			for _, column := range want {
				if !slices.Contains(spec.columns, column) {
					t.Fatalf("physical v3 %s copy omits %s", table, column)
				}
			}
			return
		}
		t.Fatalf("physical v3 migration omits %s", table)
	}

	assertColumns("ticket_refund_compensation_receipts", "command_id", "refund_request_id",
		"refund_operation_id", "request_fingerprint", "provider_proof_hash", "amount_minor",
		"selected_ticket_count", "released_seat_count", "resulting_active_ticket_count",
		"resulting_order_state", "committed_at")
	assertColumns("ticket_refund_prepare_receipts", "command_id", "refund_request_id",
		"refund_operation_id", "request_fingerprint", "ticket_ids", "state",
		"requested_at", "eligibility_cutoff_at", "prepared_at")
	assertColumns("selected_ticket_refund_receipts", "compensation_receipt_id",
		"refund_request_id", "ticket_id", "reservation_seat_id", "fare_amount_minor",
		"segment_mask_hash", "released_at")
}

func TestPhysicalMigrationV3RetriesImmutableEvidenceWithoutUpdatingIt(t *testing.T) {
	t.Parallel()

	for _, table := range []string{
		"ticket_refund_compensation_receipts", "selected_ticket_refund_receipts",
	} {
		spec, found := findTable(table)
		if !found {
			t.Fatalf("physical v3 migration omits %s", table)
		}
		sql := upsertSQL(spec)
		if !strings.Contains(sql, "ON CONFLICT (id) DO NOTHING") || strings.Contains(sql, "DO UPDATE") {
			t.Fatalf("immutable %s retry can mutate committed evidence: %s", table, sql)
		}
	}
}

func TestPhysicalMigrationV3MergesPrepareReceiptsMonotonically(t *testing.T) {
	t.Parallel()

	spec, found := findTable("ticket_refund_prepare_receipts")
	if !found {
		t.Fatal("physical v3 migration omits prepare receipts")
	}
	sql := upsertSQL(spec)
	for _, fragment := range []string{
		"ticket_refund_prepare_receipts.state = 'prepared'",
		"EXCLUDED.state IN ('released', 'applied')",
		"IS DISTINCT FROM",
		"ticket_refund_prepare_receipts.request_fingerprint",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("prepare merge omits %q: %s", fragment, sql)
		}
	}
}

func TestJournalFingerprintBindsHydratedAuthoritativePayload(t *testing.T) {
	t.Parallel()

	entry := physicalmigration.JournalEntry{
		ID: uuid.New(), Sequence: 41, TableName: "ticket_refund_prepare_receipts",
		Operation: "UPDATE", EntityID: uuid.New(), PrimaryKey: []byte(`{"id":"bounded"}`),
	}
	prepared := entry
	prepared.Payload = []byte(`{"state":"prepared","resolved_at":null}`)
	applied := entry
	applied.Payload = []byte(`{"state":"applied","resolved_at":"2026-08-17T00:00:00Z"}`)
	if journalFingerprint(prepared) == journalFingerprint(applied) {
		t.Fatal("journal apply fingerprint does not bind authoritative prepare state")
	}
}

func TestPhysicalMigrationV3PreservesV2PaymentSnapshots(t *testing.T) {
	t.Parallel()

	assertColumns := func(table string, want ...string) {
		t.Helper()
		for _, spec := range migrationTables {
			if spec.name != table {
				continue
			}
			for _, column := range want {
				if !slices.Contains(spec.columns, column) {
					t.Fatalf("physical v3 %s copy omits %s", table, column)
				}
			}
			return
		}
		t.Fatalf("physical v3 migration omits %s", table)
	}

	assertColumns("reservations", "payment_intent_id", "payment_amount_minor",
		"payment_currency", "payment_grace_expires_at")
	assertColumns("train_run_booking_snapshots", "scheduled_departure_at")
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
