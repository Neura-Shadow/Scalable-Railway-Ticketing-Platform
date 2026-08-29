package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/domain"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/ledger"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestClaimsUseShortReadCommittedSkipLockedTransactions(t *testing.T) {
	t.Parallel()
	options := worker.ClaimOptions{
		WorkerID: "payment-test", BatchSize: 17, MaxAttempts: 8,
		LeaseTTL: 30 * time.Second, Now: time.Date(2026, 8, 8, 1, 0, 0, 0, time.UTC),
	}
	for _, test := range []struct {
		name string
		run  func(*Store) error
	}{
		{name: "operations", run: func(store *Store) error {
			_, err := store.ClaimOperations(context.Background(), options)
			return err
		}},
		{name: "webhooks", run: func(store *Store) error {
			_, err := store.ClaimWebhooks(context.Background(), options)
			return err
		}},
		{name: "actions", run: func(store *Store) error {
			_, err := store.ClaimActions(context.Background(), options)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tx := &recordingTx{}
			store, err := New(&recordingDB{tx: tx})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if err := test.run(store); err != nil {
				t.Fatalf("claim error = %v", err)
			}
			if !tx.committed || tx.rolledBack {
				t.Fatalf("transaction = committed %v rolledBack %v", tx.committed, tx.rolledBack)
			}
			if tx.options.IsoLevel != pgx.ReadCommitted {
				t.Fatalf("isolation = %s, want read committed", tx.options.IsoLevel)
			}
			joined := strings.Join(tx.queries, "\n")
			if !strings.Contains(joined, "FOR UPDATE") || !strings.Contains(joined, "SKIP LOCKED") ||
				!strings.Contains(joined, "lease_until") {
				t.Fatalf("claim SQL does not contain lock/lease controls:\n%s", joined)
			}
			if test.name == "webhooks" && !strings.Contains(joined, "event_created_at") {
				t.Fatalf("webhook claim does not load immutable provider event time:\n%s", joined)
			}
			if test.name == "operations" && (!strings.Contains(joined, "claimed.attempts") ||
				!strings.Contains(joined, "claimed.lease_owner") || !strings.Contains(joined, "leased.lease_owner")) {
				t.Fatalf("operation claim reads pre-update lease values instead of CTE RETURNING values:\n%s", joined)
			}
		})
	}
}

func TestOperationClaimRecoversInFlightAsUncertainNotRetryable(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, err = store.ClaimOperations(context.Background(), worker.ClaimOptions{
		WorkerID: "payment-test", BatchSize: 1, MaxAttempts: 3,
		LeaseTTL: time.Minute, Now: time.Date(2026, 8, 8, 1, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("ClaimOperations() error = %v", err)
	}
	joined := strings.Join(tx.execs, "\n")
	if !strings.Contains(joined, "state='claimed' THEN 'pending' ELSE 'uncertain'") ||
		!strings.Contains(joined, "worker_lease_expired") {
		t.Fatalf("stale recovery SQL can blind-retry an in-flight side effect:\n%s", joined)
	}
}

func TestClaimOperationsQueuesBoundedStaleAwaitingCustomerStatusQuery(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 8, 1, 0, 0, 0, time.UTC)
	intentID := uuid.New()
	tx := &recordingTx{rows: []pgx.Rows{
		&staticRows{values: [][]any{{intentID, "sandbox", int64(2500), "TWD", now.Add(-10 * time.Minute)}}},
		&emptyRows{},
		&emptyRows{},
	}}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ClaimOperations(context.Background(), worker.ClaimOptions{
		WorkerID: "payment-test", BatchSize: 1, MaxAttempts: 3,
		LeaseTTL: time.Minute, Now: now,
	})
	if err != nil {
		t.Fatalf("ClaimOperations() error = %v", err)
	}
	joined := strings.Join(tx.execs, "\n")
	if !strings.Contains(joined, "INSERT INTO public.payment_operations") ||
		!strings.Contains(joined, "'query_status'") ||
		!strings.Contains(strings.Join(tx.queries, "\n"), "intent.state='awaiting_customer'") ||
		!strings.Contains(strings.Join(tx.queries, "\n"), "$1::timestamptz-") {
		t.Fatalf("stale recovery did not enqueue a durable query operation:\nqueries=%s\nexecs=%s",
			strings.Join(tx.queries, "\n"), joined)
	}
}

func TestShardActionsUsePurposeBuiltDurableAttemptAndLeaseBudget(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ClaimActions(context.Background(), worker.ClaimOptions{
		WorkerID: "payment-test", BatchSize: 4, MaxAttempts: 8,
		LeaseTTL: time.Minute, Now: time.Date(2026, 8, 8, 1, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("ClaimActions() error = %v", err)
	}
	joined := strings.Join(append(append([]string{}, tx.execs...), tx.queries...), "\n")
	if !strings.Contains(joined, "payment_saga_actions") || !strings.Contains(joined, "action.attempts") ||
		!strings.Contains(joined, "action.lease_owner") || strings.Contains(joined, "SET lease_owner=$3,lease_until=$1") {
		t.Fatalf("shard actions still inherit saga retry/lease state:\n%s", joined)
	}
}

func TestClaimActionsSeedsNewActionAtRunClock(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 8, 1, 0, 0, 0, time.UTC)
	tx := &recordingTx{}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ClaimActions(context.Background(), worker.ClaimOptions{
		WorkerID: "payment-test", BatchSize: 1, MaxAttempts: 3,
		LeaseTTL: time.Minute, Now: now,
	})
	if err != nil {
		t.Fatalf("ClaimActions() error = %v", err)
	}
	if len(tx.execs) == 0 || !strings.Contains(tx.execs[0], "action_type,next_attempt_at") ||
		!strings.Contains(tx.execs[0], "END,$1") || len(tx.execArgs[0]) != 1 || tx.execArgs[0][0] != now {
		t.Fatalf("new action was not made eligible at the fixed run clock: sql=%q args=%v", tx.execs[0], tx.execArgs[0])
	}
}

func TestShardActionFinalAttemptCrashGetsOneBoundedReceiptRecovery(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ClaimActions(context.Background(), worker.ClaimOptions{
		WorkerID: "payment-test", BatchSize: 1, MaxAttempts: 1,
		LeaseTTL: time.Minute, Now: time.Date(2026, 8, 8, 1, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("ClaimActions() error = %v", err)
	}
	joined := strings.Join(append(append([]string{}, tx.execs...), tx.queries...), "\n")
	for _, required := range []string{
		"worker_lease_expired_final_attempt",
		"FOR UPDATE OF action,saga",
		"action.attempts=$5",
		"action.bounded_error_category='worker_lease_expired_final_attempt'",
		"attempts>$2",
		"action_recovery_exhausted",
		"payment_manual_review_cases",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("final-attempt action crash recovery omitted %q:\n%s", required, joined)
		}
	}
}

func TestUnknownMutationFailurePersistsUncertainRegardlessOfRetryability(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	claim := worker.OperationClaim{
		OperationID: uuid.New(), PaymentIntentID: uuid.New(), Type: domain.OperationCapture,
		PreviousState: domain.OperationPending, LeaseOwner: "payment-test",
	}
	if err := store.FailOperation(context.Background(), claim, worker.Failure{
		Category: "provider_outcome_unknown", Uncertain: true, NextAttemptAt: time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatalf("FailOperation() error = %v", err)
	}
	if len(tx.execArgs) == 0 || len(tx.execArgs[0]) < 4 || tx.execArgs[0][3] != "uncertain" || !tx.committed {
		t.Fatalf("finalize args=%v committed=%v, want durable uncertain state", tx.execArgs, tx.committed)
	}
	joined := strings.ToLower(strings.Join(tx.execs, "\n"))
	if strings.Contains(joined, "payment_manual_review_cases") {
		t.Fatalf("unknown outcome opened manual review:\n%s", joined)
	}
}

func TestCompleteTicketIssuanceWritesAllLocatorsInSameControlTransaction(t *testing.T) {
	t.Parallel()
	trainRunID, ownerID := uuid.New(), uuid.New()
	tx := &recordingTx{row: staticRow{values: []any{trainRunID, "physical-shard-0", int64(7), ownerID}}}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	sagaID := uuid.MustParse("79000000-0000-4000-8000-000000000001")
	intentID := uuid.MustParse("75000000-0000-4000-8000-000000000001")
	issuanceID := shard.DeterministicIssuanceID(sagaID)
	claim := worker.ActionClaim{
		ActionID: uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"), SagaID: sagaID, Type: worker.ActionIssueTickets, Provider: "sandbox",
		LeaseOwner: "payment-test", LeaseUntil: time.Now().Add(time.Minute),
		Issue: shard.IssueTicketsCommand{
			IssuanceID: issuanceID, PaymentIntentID: intentID,
			ReservationID: uuid.New(), TrainRunID: trainRunID, OwnerID: ownerID,
			AmountMinor: 12500, Currency: "TWD",
		},
	}
	receipt := shard.IssueTicketsReceipt{
		TicketOrderID: uuid.New(), TicketIDs: []uuid.UUID{uuid.New(), uuid.New()},
		TicketCodes: []string{"ticket_code_000001", "ticket_code_000002"},
		AmountMinor: 12500, Currency: "TWD",
		OrderCreatedAt: time.Now().Add(-time.Minute).UTC(), IssuedAt: time.Now().UTC(),
	}
	if err := store.CompleteAction(context.Background(), claim, worker.ActionEvidence{Issue: receipt}); err != nil {
		t.Fatalf("CompleteAction() error = %v", err)
	}
	joined := strings.ToLower(strings.Join(tx.execs, "\n"))
	if strings.Count(joined, "insert into public.ticket_order_shard_locators") != 1 ||
		strings.Count(joined, "insert into public.ticket_shard_locators") != len(receipt.TicketIDs) ||
		strings.Count(joined, "insert into public.ticket_code_directory") != len(receipt.TicketIDs) ||
		strings.Count(joined, "insert into public.financial_ledger_transactions") != 1 ||
		strings.Count(joined, "insert into public.financial_ledger_postings") != 2 ||
		!strings.Contains(joined, "update public.payment_intents") ||
		!strings.Contains(joined, "update public.payment_sagas") || !tx.committed {
		t.Fatalf("control finalize SQL missing atomic locator writes:\n%s", joined)
	}
	for index, query := range tx.execs {
		if !strings.Contains(query, "INSERT INTO public.financial_ledger_transactions") {
			continue
		}
		args := tx.execArgs[index]
		if args[0] != uuid.MustParse("f020d39d-d1cb-5f81-955e-e84dc3fa6244") ||
			args[1] != "ticket_issuance:ddb62b09-9c50-526a-adb4-e32a16aa7c66" ||
			args[2] != "payment:75000000-0000-4000-8000-000000000001" {
			t.Fatalf("worker issuance ledger identity args = %+v", args)
		}
		if got := stringHexBytes(args[5].([]byte)); got != "d9e0ae58551a9829103246f613d371b754af2a68fec8bf8c011b36d1ce459227" {
			t.Fatalf("worker issuance fingerprint = %s", got)
		}
		return
	}
	t.Fatal("worker issuance ledger insert not found")
}

func stringHexBytes(value []byte) string {
	const digits = "0123456789abcdef"
	encoded := make([]byte, len(value)*2)
	for index, item := range value {
		encoded[index*2] = digits[item>>4]
		encoded[index*2+1] = digits[item&0x0f]
	}
	return string(encoded)
}

func TestCompleteTicketIssuanceRejectsDuplicateGlobalCodeClaims(t *testing.T) {
	t.Parallel()
	trainRunID, ownerID := uuid.New(), uuid.New()
	tx := &recordingTx{row: staticRow{values: []any{trainRunID, "physical-shard-0", int64(7), ownerID}}}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	claim := worker.ActionClaim{
		ActionID: uuid.New(), SagaID: uuid.New(), Type: worker.ActionIssueTickets, LeaseOwner: "payment-test",
		Issue: shard.IssueTicketsCommand{ReservationID: uuid.New(), TrainRunID: trainRunID, OwnerID: ownerID},
	}
	receipt := shard.IssueTicketsReceipt{
		TicketOrderID: uuid.New(), TicketIDs: []uuid.UUID{uuid.New(), uuid.New()},
		TicketCodes:    []string{"duplicate_code_001", "duplicate_code_001"},
		OrderCreatedAt: time.Now().Add(-time.Minute).UTC(), IssuedAt: time.Now().UTC(),
	}
	if err := store.CompleteAction(context.Background(), claim, worker.ActionEvidence{Issue: receipt}); !errors.Is(err, worker.ErrStoreUnavailable) {
		t.Fatalf("CompleteAction() error = %v, want store unavailable", err)
	}
	if tx.committed {
		t.Fatal("duplicate ticket code claims committed")
	}
}

func TestCompleteCompensationCancelsTicketOrderLocator(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 1")}}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	claim := worker.ActionClaim{
		ActionID: uuid.New(), SagaID: uuid.New(), Type: worker.ActionCompensate, Provider: "sandbox",
		LeaseOwner: "payment-test", LeaseUntil: time.Now().Add(time.Minute),
		Compensation: shard.ApplyRefundCompensationCommand{ReservationID: uuid.New(), OwnerID: uuid.New()},
	}
	evidence := worker.ActionEvidence{Compensation: shard.ApplyRefundCompensationReceipt{
		TicketOrderID: uuid.New(), CancelledTicketCount: 1,
	}}
	if err := store.CompleteAction(context.Background(), claim, evidence); err != nil {
		t.Fatalf("CompleteAction() error = %v", err)
	}
	if !strings.Contains(strings.ToLower(strings.Join(tx.execs, "\n")), "update public.ticket_order_shard_locators") {
		t.Fatal("compensation did not cancel ticket-order locator")
	}
}

func TestCompleteCompensationWithoutIssuedTicketsRequiresNoControlLocator(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 1"), pgconn.NewCommandTag("UPDATE 0")}}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	claim := worker.ActionClaim{
		ActionID: uuid.New(), SagaID: uuid.New(), Type: worker.ActionCompensate, Provider: "sandbox",
		LeaseOwner: "payment-test", LeaseUntil: time.Now().Add(time.Minute),
		Compensation: shard.ApplyRefundCompensationCommand{ReservationID: uuid.New(), OwnerID: uuid.New()},
	}
	evidence := worker.ActionEvidence{Compensation: shard.ApplyRefundCompensationReceipt{
		TicketOrderID: uuid.New(), CancelledTicketCount: 0,
	}}
	if err := store.CompleteAction(context.Background(), claim, evidence); err != nil {
		t.Fatalf("CompleteAction() error = %v", err)
	}
	if !tx.committed {
		t.Fatal("zero-ticket compensation did not finalize without an impossible locator")
	}
}

func TestVoidActionIdentityIsStableAndBindsProviderOperation(t *testing.T) {
	t.Parallel()
	sagaID, intentID, reservationID, trainRunID, ownerID, voidID :=
		uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	proof := make([]byte, 32)
	proof[0] = 7
	voidedAt := pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	base := worker.ActionClaim{ActionID: uuid.New(), SagaID: sagaID, Provider: "sandbox", Attempts: 1, LeaseOwner: "worker", LeaseUntil: time.Now().Add(time.Minute)}
	first, err := buildActionClaim(base, "compensating", "compensate", intentID, reservationID,
		trainRunID, ownerID, 2500, "TWD", pgtype.UUID{}, nil,
		pgtype.UUID{Bytes: voidID, Valid: true}, proof, voidedAt, pgtype.UUID{}, nil, pgtype.Timestamptz{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildActionClaim(base, "compensating", "compensate", intentID, reservationID,
		trainRunID, ownerID, 2500, "TWD", pgtype.UUID{}, nil,
		pgtype.UUID{Bytes: voidID, Valid: true}, proof, voidedAt, pgtype.UUID{}, nil, pgtype.Timestamptz{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Type != worker.ActionCancelVoided || first.CancelVoided.CommandID != second.CancelVoided.CommandID ||
		first.CancelVoided.RequestFingerprint != second.CancelVoided.RequestFingerprint {
		t.Fatalf("claims are not stable: first=%+v second=%+v", first, second)
	}
	changed, err := buildActionClaim(base, "compensating", "compensate", intentID, reservationID,
		trainRunID, ownerID, 2500, "TWD", pgtype.UUID{}, nil,
		pgtype.UUID{Bytes: uuid.New(), Valid: true}, proof, voidedAt, pgtype.UUID{}, nil, pgtype.Timestamptz{})
	if err != nil {
		t.Fatal(err)
	}
	if changed.CancelVoided.CommandID != first.CancelVoided.CommandID ||
		changed.CancelVoided.RequestFingerprint == first.CancelVoided.RequestFingerprint {
		t.Fatal("provider operation identity is not bound into the stable command receipt fingerprint")
	}
}

func TestCompleteVoidCancellationFinalizesOnlyCompensatingSaga(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	claim := worker.ActionClaim{
		ActionID: uuid.New(), SagaID: uuid.New(), Type: worker.ActionCancelVoided, Provider: "sandbox",
		LeaseOwner: "payment-test", LeaseUntil: time.Now().Add(time.Minute),
	}
	if err := store.CompleteAction(context.Background(), claim, worker.ActionEvidence{
		CancelVoided: shard.CancelVoidedReservationReceipt{TicketOrderID: uuid.New()},
	}); err != nil {
		t.Fatalf("CompleteAction() error = %v", err)
	}
	joined := strings.ToLower(strings.Join(tx.execs, "\n"))
	if !strings.Contains(joined, "intent.state='voided'") ||
		!strings.Contains(joined, "state='compensating'") ||
		!strings.Contains(joined, "state='compensated'") || !tx.committed {
		t.Fatalf("void finalization SQL = %s", joined)
	}
}

func TestVoidProviderEvidenceRejectsCapturedOrRefundedMoney(t *testing.T) {
	t.Parallel()
	claim := worker.OperationClaim{Type: domain.OperationVoid, AmountMinor: 2500, Currency: "TWD"}
	base := worker.OperationEvidence{
		ProviderPaymentID: "pay-1", ProviderOperationID: "void-1",
		AmountMinor: 2500, Currency: "TWD",
	}
	if !validProviderEvidence(claim, base) {
		t.Fatal("uncaptured void evidence was rejected")
	}
	base.CapturedMinor = 2500
	if validProviderEvidence(claim, base) {
		t.Fatal("captured void evidence was accepted")
	}
	base.CapturedMinor = 0
	base.RefundedMinor = 1
	if validProviderEvidence(claim, base) {
		t.Fatal("refunded void evidence was accepted")
	}
}

func TestProviderVoidSuccessSchedulesShardCancellation(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{}
	claim := worker.OperationClaim{
		OperationID: uuid.New(), PaymentIntentID: uuid.New(), ReservationID: uuid.New(), Provider: "sandbox",
		Type: domain.OperationVoid, AmountMinor: 2500, Currency: "TWD",
	}
	if err := applyOperationSuccess(context.Background(), tx, claim, worker.OperationEvidence{}); err != nil {
		t.Fatalf("applyOperationSuccess() error = %v", err)
	}
	assertVoidSchedulesShardAction(t, tx.execs)
}

func TestVerifiedVoidWebhookSchedulesSameShardCancellation(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{row: staticRow{values: []any{uuid.New(), "pending"}}}
	if err := applyProviderConfirmation(context.Background(), tx, uuid.New(), uuid.New(), "sandbox", "void_pending", 2500, "TWD", worker.WebhookEvidence{
		Status: provider.StatusVoided, AmountMinor: 2500, Currency: "TWD",
	}); err != nil {
		t.Fatalf("applyProviderConfirmation() error = %v", err)
	}
	assertVoidSchedulesShardAction(t, tx.execs)
}

func TestVerifiedCapturedWebhookRacingVoidConvergesToRefund(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{row: staticRow{values: []any{uuid.New(), "pending"}}}
	if err := applyProviderConfirmation(context.Background(), tx, uuid.New(), uuid.New(), "sandbox", "void_pending", 2500, "TWD", worker.WebhookEvidence{
		Status: provider.StatusCaptured, AmountMinor: 2500, Currency: "TWD", CapturedMinor: 2500,
	}); err != nil {
		t.Fatalf("applyProviderConfirmation() error = %v", err)
	}
	joined := strings.ToLower(strings.Join(tx.execs, "\n"))
	for _, expected := range []string{
		"superseded_by_capture", "state='refund_pending'", "state='refunding'",
		"current_step='refund'", "operation_type",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("captured webhook did not converge to refund (%s missing):\n%s", expected, joined)
		}
	}
	if strings.Contains(joined, "issue_tickets") || strings.Contains(joined, "state='issuing_tickets'") {
		t.Fatalf("captured webhook regressed into issuance:\n%s", joined)
	}
}

func TestCaptureLedgerIsExactReplayAcrossWebhookAndVoidRace(t *testing.T) {
	t.Parallel()
	intentID := uuid.New()
	reservationID := uuid.New()
	operationID := deterministicID(intentID, "provider_operation:"+string(domain.OperationCapture))

	webhookTx := &recordingTx{queryRows: []pgx.Row{staticRow{values: []any{operationID, "pending"}}}}
	evidence := worker.WebhookEvidence{
		Status: provider.StatusCaptured, AmountMinor: 2500, Currency: "TWD", CapturedMinor: 2500,
	}
	if err := applyProviderConfirmation(context.Background(), webhookTx, intentID, reservationID, "sandbox", "authorization_pending", 2500, "TWD", evidence); err != nil {
		t.Fatalf("applyProviderConfirmation() error = %v", err)
	}
	if got := countSQL(webhookTx.execs, "INSERT INTO public.financial_ledger_transactions"); got != 1 {
		t.Fatalf("first capture inserted %d ledger transactions, want 1: %v", got, webhookTx.execs)
	}
	if got := countSQL(webhookTx.execs, "INSERT INTO public.financial_ledger_postings"); got != 2 {
		t.Fatalf("first capture inserted %d ledger postings, want 2: %v", got, webhookTx.execs)
	}

	stored, err := ledger.PrepareAppend(ledger.AppendRequest{
		EventID: "capture:" + operationID.String(), Correlation: "payment:" + intentID.String(),
		Purpose: ledger.PurposeCapture, Currency: "TWD",
		Postings: []ledger.Posting{
			{Account: ledger.AccountProviderReceivable, Side: ledger.Debit, AmountMinor: 2500, Currency: "TWD"},
			{Account: ledger.AccountCustomerFundsPending, Side: ledger.Credit, AmountMinor: 2500, Currency: "TWD"},
		},
	}, time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	voidRaceTx := &recordingTx{
		queryRows:  []pgx.Row{staticRow{values: []any{operationID, "succeeded"}}},
		ledgerRows: []pgx.Row{ledgerRow(t, stored)},
	}
	if err := applyProviderConfirmation(context.Background(), voidRaceTx, intentID, reservationID, "sandbox", "void_pending", 2500, "TWD", evidence); err != nil {
		t.Fatalf("void-race applyProviderConfirmation() replay error = %v\nqueries: %v\nexecs: %v", err, voidRaceTx.queryRowSQL, voidRaceTx.execs)
	}
	if got := countSQL(voidRaceTx.execs, "INSERT INTO public.financial_ledger_transactions"); got != 0 {
		t.Fatalf("exact replay inserted %d ledger transactions, want 0: %v", got, voidRaceTx.execs)
	}
	if got := countSQL(voidRaceTx.execs, "INSERT INTO public.financial_ledger_postings"); got != 0 {
		t.Fatalf("exact replay inserted %d ledger postings, want 0: %v", got, voidRaceTx.execs)
	}
}

func TestUncertainVoidCapturePivotIsOneControlTransaction(t *testing.T) {
	t.Parallel()
	leaseOwner := "payment-test"
	tx := &recordingTx{queryRows: []pgx.Row{
		staticRow{values: []any{"void_pending", "compensating", "void", leaseOwner}},
		staticRow{values: []any{uuid.New(), "pending"}},
	}}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	claim := worker.OperationClaim{
		OperationID: uuid.New(), PaymentIntentID: uuid.New(), Provider: "sandbox",
		Type: domain.OperationVoid, PreviousState: domain.OperationUncertain,
		ProviderPaymentID: "pay-1", AmountMinor: 2500, Currency: "TWD",
		LeaseOwner: leaseOwner, LeaseUntil: time.Now().Add(time.Minute),
	}
	evidence := worker.OperationEvidence{
		ProviderPaymentID: "pay-1", Status: provider.StatusCaptured,
		AmountMinor: 2500, Currency: "TWD", CapturedMinor: 2500,
		ResponseFingerprint: [32]byte{1},
	}
	if err := store.SupersedeVoidWithRefund(context.Background(), claim, evidence); err != nil {
		t.Fatalf("SupersedeVoidWithRefund() error = %v", err)
	}
	joined := strings.ToLower(strings.Join(tx.execs, "\n"))
	for _, expected := range []string{
		"superseded_by_capture", "state='failed_permanent'", "state='refund_pending'",
		"state='refunding'", "current_step='refund'", "'refund'",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("atomic pivot missing %s:\n%s", expected, joined)
		}
	}
	if strings.Contains(joined, "payment_manual_review_cases") || !tx.committed {
		t.Fatalf("pivot opened a review case or did not commit: committed=%v\n%s", tx.committed, joined)
	}
}

func assertVoidSchedulesShardAction(t *testing.T, statements []string) {
	t.Helper()
	joined := strings.ToLower(strings.Join(statements, "\n"))
	if !strings.Contains(joined, "state='voided'") ||
		!strings.Contains(joined, "current_step='compensate'") ||
		!strings.Contains(joined, "next_attempt_at=clock_timestamp()") {
		t.Fatalf("void did not schedule shard action:\n%s", joined)
	}
	if strings.Contains(joined, "state='manual_review'") || strings.Contains(joined, "payment_manual_review_cases") {
		t.Fatalf("void incorrectly entered manual review:\n%s", joined)
	}
}

type recordingDB struct{ tx *recordingTx }

func (db *recordingDB) BeginTx(_ context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	db.tx.options = options
	return db.tx, nil
}

type recordingTx struct {
	pgx.Tx
	options      pgx.TxOptions
	execs        []string
	execArgs     [][]any
	queries      []string
	committed    bool
	rolledBack   bool
	row          pgx.Row
	queryRows    []pgx.Row
	ledgerRows   []pgx.Row
	ledgerIndex  int
	queryRowSQL  []string
	queryRowArgs [][]any
	rowIndex     int
	execTags     []pgconn.CommandTag
	execIndex    int
	rows         []pgx.Rows
	queryIndex   int
}

func TestCompleteActionConsumesActionLeaseWithoutSagaAttemptLease(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	claim := worker.ActionClaim{
		ActionID: uuid.New(), SagaID: uuid.New(), Type: worker.ActionMarkRefundPending,
		Provider: "sandbox", Attempts: 1, LeaseOwner: "payment-test", LeaseUntil: time.Now().Add(time.Minute),
	}
	if err := store.CompleteAction(context.Background(), claim, worker.ActionEvidence{}); err != nil {
		t.Fatalf("CompleteAction() error = %v", err)
	}
	joined := strings.Join(tx.execs, "\n")
	if !strings.Contains(joined, "UPDATE public.payment_saga_actions") ||
		!strings.Contains(joined, "action_id=$1") || !strings.Contains(joined, "state='processing'") ||
		strings.Contains(joined, "saga.lease_owner=$2") {
		t.Fatalf("action completion did not consume its independent lease:\n%s", joined)
	}
}

func TestRetryableActionFailurePersistsOnlyOnIndependentActionBudget(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	claim := worker.ActionClaim{
		ActionID: uuid.New(), SagaID: uuid.New(), Type: worker.ActionIssueTickets,
		Provider: "sandbox", Attempts: 1, LeaseOwner: "payment-test", LeaseUntil: time.Now().Add(time.Minute),
	}
	failure := worker.Failure{Category: "shard_command_failed", NextAttemptAt: time.Now().Add(time.Minute)}
	if err := store.FailAction(context.Background(), claim, failure); err != nil {
		t.Fatalf("FailAction() error = %v", err)
	}
	joined := strings.Join(tx.execs, "\n")
	if !strings.Contains(joined, "UPDATE public.payment_saga_actions") ||
		!strings.Contains(joined, "state='failed_retryable'") ||
		strings.Contains(joined, "UPDATE public.payment_sagas") {
		t.Fatalf("retryable shard action failure leaked into saga attempt state:\n%s", joined)
	}
}

func TestAuthorizedStatusRecoverySchedulesCaptureWithoutShardMutation(t *testing.T) {
	t.Parallel()
	tx := &recordingTx{}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	claim := worker.OperationClaim{
		OperationID: uuid.New(), PaymentIntentID: uuid.New(), Provider: "sandbox",
		Type: domain.OperationQueryStatus, PreviousState: domain.OperationPending,
		ProviderPaymentID: "pay-1", AmountMinor: 2500, Currency: "TWD", LeaseOwner: "payment-test",
	}
	evidence := worker.OperationEvidence{
		Disposition: worker.DispositionApplied, ProviderPaymentID: claim.ProviderPaymentID,
		Status: provider.StatusAuthorized, AmountMinor: claim.AmountMinor, Currency: claim.Currency,
		ResponseFingerprint: sha256.Sum256([]byte("authorized-status-recovery")),
	}
	if err := store.CompleteOperation(context.Background(), claim, evidence); err != nil {
		t.Fatalf("CompleteOperation() error = %v", err)
	}
	joined := strings.Join(tx.execs, "\n")
	for _, required := range []string{"state='authorization_pending'", "state='authorized'", "INSERT INTO public.payment_operations"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("authorized recovery omitted %q:\n%s", required, joined)
		}
	}
	for _, forbidden := range []string{"ticket_order_shard_locators", "reservation_shard_locators", "ticket_shard_locators"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("status recovery directly mutated shard/control locator %q:\n%s", forbidden, joined)
		}
	}
}

func TestCapturedStatusRecoverySchedulesDurableIssuanceWithoutShardMutation(t *testing.T) {
	t.Parallel()
	captureID := uuid.New()
	tx := &recordingTx{queryRows: []pgx.Row{staticRow{values: []any{captureID, "pending"}}}}
	store, err := New(&recordingDB{tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	claim := worker.OperationClaim{
		OperationID: uuid.New(), PaymentIntentID: uuid.New(), Provider: "sandbox",
		Type: domain.OperationQueryStatus, PreviousState: domain.OperationPending,
		ProviderPaymentID: "pay-1", AmountMinor: 2500, Currency: "TWD", LeaseOwner: "payment-test",
	}
	evidence := worker.OperationEvidence{
		Disposition: worker.DispositionApplied, ProviderPaymentID: claim.ProviderPaymentID,
		Status: provider.StatusCaptured, AmountMinor: claim.AmountMinor, Currency: claim.Currency,
		CapturedMinor:       claim.AmountMinor,
		ResponseFingerprint: sha256.Sum256([]byte("captured-status-recovery")),
	}
	if err := store.CompleteOperation(context.Background(), claim, evidence); err != nil {
		t.Fatalf("CompleteOperation() error = %v", err)
	}
	joined := strings.Join(tx.execs, "\n")
	for _, required := range []string{
		"provider_operation_id=$2,normalized_provider_state=$3", "state='ticket_issue_pending'",
		"state='issuing_tickets'", "INSERT INTO public.financial_ledger_transactions",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("captured recovery omitted %q:\n%s", required, joined)
		}
	}
	for _, forbidden := range []string{"ticket_order_shard_locators", "reservation_shard_locators", "ticket_shard_locators"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("captured recovery directly mutated shard/control locator %q:\n%s", forbidden, joined)
		}
	}
}

func TestRefundSuccessSelectsLedgerSourceFromIssuanceState(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		issued     bool
		wantSource ledger.Account
		reject     ledger.Account
	}{
		{name: "pre-issued reverses pending funds", issued: false, wantSource: ledger.AccountCustomerFundsPending, reject: ledger.AccountTicketSales},
		{name: "issued reverses ticket sales", issued: true, wantSource: ledger.AccountTicketSales, reject: ledger.AccountCustomerFundsPending},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tx := &recordingTx{row: staticRow{values: []any{test.issued}}}
			store, err := New(&recordingDB{tx: tx})
			if err != nil {
				t.Fatal(err)
			}
			claim := worker.OperationClaim{
				OperationID: uuid.New(), PaymentIntentID: uuid.New(), ReservationID: uuid.New(), Provider: "sandbox",
				Type: domain.OperationRefund, PreviousState: domain.OperationPending,
				ProviderPaymentID: "pay-1", AmountMinor: 2500, Currency: "TWD", LeaseOwner: "payment-test",
			}
			evidence := worker.OperationEvidence{
				Disposition: worker.DispositionApplied, ProviderPaymentID: claim.ProviderPaymentID,
				Status: provider.StatusRefunded, AmountMinor: claim.AmountMinor, Currency: claim.Currency,
				CapturedMinor: claim.AmountMinor, RefundedMinor: claim.AmountMinor,
				ResponseFingerprint: sha256.Sum256([]byte("refund-ledger-" + test.name)),
			}
			if err := store.CompleteOperation(context.Background(), claim, evidence); err != nil {
				t.Fatalf("CompleteOperation() error = %v", err)
			}
			joined := strings.Join(tx.execs, "\n")
			if !strings.Contains(joined, "INSERT INTO public.financial_ledger_transactions") ||
				strings.Count(joined, "INSERT INTO public.financial_ledger_postings") != 2 {
				t.Fatalf("refund ledger was not atomic with control finalization:\n%s", joined)
			}
			var sawSourceDebit, sawRejectedDebit, sawProviderRefundCredit bool
			for _, args := range tx.execArgs {
				if len(args) != 6 {
					continue
				}
				account, side := args[2], args[3]
				sawSourceDebit = sawSourceDebit || account == test.wantSource && side == ledger.Debit
				sawRejectedDebit = sawRejectedDebit || account == test.reject && side == ledger.Debit
				sawProviderRefundCredit = sawProviderRefundCredit || account == ledger.AccountProviderRefundReceivable && side == ledger.Credit
			}
			if !sawSourceDebit || sawRejectedDebit || !sawProviderRefundCredit {
				t.Fatalf("refund posting direction mismatch: %+v", tx.execArgs)
			}
		})
	}
}

func (tx *recordingTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.execs = append(tx.execs, sql)
	tx.execArgs = append(tx.execArgs, append([]any(nil), args...))
	if tx.execIndex < len(tx.execTags) {
		tag := tx.execTags[tx.execIndex]
		tx.execIndex++
		return tag, nil
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (tx *recordingTx) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	tx.queries = append(tx.queries, sql)
	if tx.queryIndex < len(tx.rows) {
		rows := tx.rows[tx.queryIndex]
		tx.queryIndex++
		return rows, nil
	}
	return &emptyRows{}, nil
}

func (tx *recordingTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	tx.queryRowSQL = append(tx.queryRowSQL, sql)
	tx.queryRowArgs = append(tx.queryRowArgs, append([]any(nil), args...))
	if strings.Contains(sql, "FROM public.financial_ledger_transactions") {
		if tx.ledgerIndex < len(tx.ledgerRows) {
			row := tx.ledgerRows[tx.ledgerIndex]
			tx.ledgerIndex++
			return row
		}
		return staticRow{err: pgx.ErrNoRows}
	}
	if tx.rowIndex < len(tx.queryRows) {
		row := tx.queryRows[tx.rowIndex]
		tx.rowIndex++
		return row
	}
	if tx.row != nil {
		return tx.row
	}
	return staticRow{err: pgx.ErrNoRows}
}

func countSQL(statements []string, fragment string) int {
	count := 0
	for _, statement := range statements {
		if strings.Contains(statement, fragment) {
			count++
		}
	}
	return count
}

func ledgerRow(t *testing.T, transaction ledger.Transaction) pgx.Row {
	t.Helper()
	postings := make([]map[string]any, len(transaction.Postings))
	for index, posting := range transaction.Postings {
		postings[index] = map[string]any{
			"account": posting.Account, "side": posting.Side,
			"amount_minor": posting.AmountMinor, "currency": posting.Currency,
		}
	}
	encoded, err := json.Marshal(postings)
	if err != nil {
		t.Fatal(err)
	}
	return staticRow{values: []any{
		transaction.ID, transaction.EventID, transaction.Correlation, string(transaction.Purpose),
		transaction.Currency, transaction.Fingerprint[:], transaction.CreatedAt, transaction.ReversalOf, encoded,
	}}
}

func (tx *recordingTx) Commit(context.Context) error {
	tx.committed = true
	return nil
}

func (tx *recordingTx) Rollback(context.Context) error {
	if !tx.committed {
		tx.rolledBack = true
	}
	return nil
}

type emptyRows struct{ pgx.Rows }

func (*emptyRows) Close()                                       {}
func (*emptyRows) Err() error                                   { return nil }
func (*emptyRows) Next() bool                                   { return false }
func (*emptyRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (*emptyRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (*emptyRows) Values() ([]any, error)                       { return nil, nil }
func (*emptyRows) RawValues() [][]byte                          { return nil }
func (*emptyRows) Conn() *pgx.Conn                              { return nil }

type staticRows struct {
	pgx.Rows
	values [][]any
	index  int
}

func (*staticRows) Close()                                       {}
func (*staticRows) Err() error                                   { return nil }
func (rows *staticRows) Next() bool                              { rows.index++; return rows.index <= len(rows.values) }
func (*staticRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (*staticRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (*staticRows) Values() ([]any, error)                       { return nil, nil }
func (*staticRows) RawValues() [][]byte                          { return nil }
func (*staticRows) Conn() *pgx.Conn                              { return nil }
func (rows *staticRows) Scan(destinations ...any) error {
	if rows.index < 1 || rows.index > len(rows.values) || len(destinations) != len(rows.values[rows.index-1]) {
		return errors.New("unexpected scan width")
	}
	for index := range destinations {
		reflect.ValueOf(destinations[index]).Elem().Set(reflect.ValueOf(rows.values[rows.index-1][index]))
	}
	return nil
}

type staticRow struct {
	values []any
	err    error
}

func (row staticRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != len(row.values) {
		return errors.New("unexpected scan width")
	}
	for index := range destinations {
		reflect.ValueOf(destinations[index]).Elem().Set(reflect.ValueOf(row.values[index]))
	}
	return nil
}
