package reconcile

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/provider"
	"github.com/google/uuid"
)

type Reconciler struct {
	store     Store
	providers ProviderRegistry
	repairer  Repairer
	config    Config
}

func New(store Store, providers ProviderRegistry, repairer Repairer, config Config) (*Reconciler, error) {
	if store == nil || config.BatchSize < 1 || config.BatchSize > MaxBatchSize ||
		config.StaleAfter <= 0 || config.ReviewDue <= 0 || config.Now == nil {
		return nil, ErrInvalidConfiguration
	}
	return &Reconciler{store: store, providers: providers, repairer: repairer, config: config}, nil
}

func (r *Reconciler) RunOnce(ctx context.Context) (Result, error) {
	return r.ReconcileAll(ctx, Options{Scope: ScopeAll, Limit: r.config.BatchSize})
}

func (r *Reconciler) InspectPayment(ctx context.Context, paymentIntentID uuid.UUID) (Report, error) {
	return r.inspect(ctx, paymentIntentID, ScopeAll)
}

func (r *Reconciler) ReconcilePayment(ctx context.Context, paymentIntentID uuid.UUID) (Result, error) {
	if paymentIntentID == uuid.Nil {
		return Result{}, ErrInvalidRequest
	}
	return r.reconcileIDs(ctx, ScopeAll, []uuid.UUID{paymentIntentID}, false, false, false)
}

func (r *Reconciler) ReconcileAll(ctx context.Context, options Options) (Result, error) {
	if r == nil || ctx == nil || !options.Scope.Valid() || options.Limit < 1 || options.Limit > MaxBatchSize {
		return Result{}, ErrInvalidRequest
	}
	if options.Repair && !options.ConfirmRepair {
		return Result{}, ErrRepairConfirmation
	}
	if options.Repair && r.repairer == nil {
		return Result{}, ErrRepairUnavailable
	}
	ids, truncated, err := r.store.CandidateIntentIDs(ctx, options.Scope, r.now().Add(-r.config.StaleAfter), options.Limit)
	if err != nil {
		return Result{}, fmt.Errorf("list payment reconciliation candidates: %w", err)
	}
	if len(ids) > options.Limit {
		truncated = true
	}
	ids = canonicalIDs(ids, options.Limit)
	if len(ids) == 0 {
		return r.reconcileIDs(ctx, options.Scope, nil, truncated, options.Repair, options.ConfirmRepair)
	}
	result := Result{Scope: options.Scope, ReadOnly: !options.Repair, Truncated: truncated, Reports: make([]Report, 0, len(ids))}
	for index, id := range ids {
		partial, err := r.reconcileIDs(ctx, options.Scope, []uuid.UUID{id}, truncated && index == len(ids)-1, options.Repair, options.ConfirmRepair)
		result.RowsExamined += partial.RowsExamined
		result.MismatchCount += partial.MismatchCount
		result.RepairCount += partial.RepairCount
		result.ManualReviews += partial.ManualReviews
		result.Reports = append(result.Reports, partial.Reports...)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func (r *Reconciler) reconcileIDs(ctx context.Context, scope Scope, ids []uuid.UUID, truncated, repair, confirmed bool) (result Result, returnErr error) {
	if r == nil || ctx == nil || !scope.Valid() || len(ids) > MaxBatchSize || repair != confirmed {
		return Result{}, ErrInvalidRequest
	}
	now := r.now()
	checkpoint, err := r.store.StartCheckpoint(ctx, scope, singleIntent(ids), repair, now)
	if err != nil {
		return Result{}, fmt.Errorf("start payment reconciliation checkpoint: %w", err)
	}
	result = Result{Scope: scope, ReadOnly: !repair, Truncated: truncated, Reports: make([]Report, 0, len(ids))}
	defer func() {
		finished := CheckpointResult{
			RowsExamined: result.RowsExamined, MismatchCount: result.MismatchCount,
			RepairCount: result.RepairCount, Truncated: result.Truncated,
			Failed: returnErr != nil, CompletedAt: r.now(),
		}
		if returnErr != nil {
			finished.ErrorCategory = "reconciliation_failed"
		}
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if finishErr := r.store.FinishCheckpoint(finishCtx, checkpoint, finished); finishErr != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("finish payment reconciliation checkpoint: %w", finishErr))
		}
	}()

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		report, err := r.inspect(ctx, id, scope)
		if err != nil {
			return result, fmt.Errorf("inspect payment intent: %w", err)
		}
		result.RowsExamined += report.RowsExamined
		result.MismatchCount += len(report.Findings)
		replayedCommands := make(map[uuid.UUID]struct{}, len(report.Findings))
		for index := range report.Findings {
			finding := &report.Findings[index]
			repaired := false
			if repair && finding.Repairable {
				command, ok := commandForFinding(*finding)
				if ok {
					if _, alreadyReplayed := replayedCommands[command.ID]; !alreadyReplayed {
						if err := validateRecordedCommand(command); err != nil {
							return result, err
						}
						if err := r.repairer.ReplayRecordedCommand(ctx, id, command); err != nil {
							return result, fmt.Errorf("replay recorded payment command: %w", err)
						}
						replayedCommands[command.ID] = struct{}{}
						report.RepairsApplied++
						result.RepairCount++
					}
					finding.CommandID = command.ID
					repaired = true
				}
			}
			if repaired {
				continue
			}
			created, err := r.store.EscalateManualReview(ctx, checkpoint.ID, id, finding.Code, now.Add(r.config.ReviewDue))
			if err != nil {
				return result, fmt.Errorf("escalate payment reconciliation mismatch: %w", err)
			}
			if created {
				result.ManualReviews++
			}
		}
		result.Reports = append(result.Reports, report)
	}
	return result, nil
}

func (r *Reconciler) inspect(ctx context.Context, id uuid.UUID, scope Scope) (Report, error) {
	if r == nil || ctx == nil || id == uuid.Nil || !scope.Valid() {
		return Report{}, ErrInvalidRequest
	}
	control, err := r.store.LoadControlSnapshot(ctx, id)
	if err != nil {
		return Report{}, err
	}
	if control.Intent.ID != id {
		return Report{}, ErrInvalidRequest
	}
	var shard ShardSnapshot
	if scope == ScopeIntents || scope == ScopeTickets || scope == ScopeAll {
		shard, err = r.store.LoadShardSnapshot(ctx, id)
		if err != nil {
			return Report{}, err
		}
	}
	report := Report{PaymentIntentID: id, Scope: scope, RowsExamined: 1}
	seenFindings := make(map[string]struct{}, 16)
	add := func(code string, repairable bool) {
		if _, exists := seenFindings[code]; exists {
			return
		}
		if len(report.Findings) >= MaxFindingsPerIntent {
			report.Truncated = true
			return
		}
		seenFindings[code] = struct{}{}
		report.Findings = append(report.Findings, Finding{Code: code, Repairable: repairable})
	}
	checkControl(scope, control, add)
	checkShard(scope, control, shard, add)
	if shouldQueryProvider(scope, control) {
		report.ProviderQueried = true
		checkProvider(ctx, r.providers, control, add)
	}
	for i := range report.Findings {
		if command, ok := matchingRecordedCommand(report.Findings[i].Code, shard.RecordedCommands); ok {
			report.Findings[i].Repairable = true
			report.Findings[i].CommandID = command.ID
			report.Findings[i].commandFingerprint = command.Fingerprint
		}
	}
	sort.Slice(report.Findings, func(i, j int) bool { return report.Findings[i].Code < report.Findings[j].Code })
	if report.RowsExamined < len(report.Findings) {
		report.RowsExamined = len(report.Findings)
	}
	return report, nil
}

func checkControl(scope Scope, snapshot ControlSnapshot, add func(string, bool)) {
	if scope == ScopeIntents || scope == ScopeAll {
		if snapshot.Intent.ActiveForReservation > 1 {
			add("multiple_active_payment_intents", false)
		}
		if snapshot.Saga.ActiveCount > 1 {
			add("multiple_active_payment_sagas", false)
		}
		if snapshot.Saga.ID == uuid.Nil {
			add("payment_saga_missing", false)
		}
		if snapshot.Intent.State == "manual_review" && snapshot.OpenManualReviewCases == 0 {
			add("manual_review_not_visible", false)
		}
	}
	if scope == ScopeWebhooks || scope == ScopeAll {
		if snapshot.DuplicateProviderEventIDs > 0 {
			add("duplicate_provider_event_id", false)
		}
		if snapshot.ProviderEventHashConflicts > 0 {
			add("provider_event_payload_conflict", false)
		}
	}
	if scope != ScopeOperations && scope != ScopeAll {
		return
	}
	captured, refunded := int64(0), int64(0)
	captures, refunds := 0, 0
	providerOperations := map[string]struct{}{}
	uncertain := false
	for _, operation := range snapshot.Operations {
		if operation.State == "uncertain" {
			uncertain = true
		}
		if operation.ProviderOperationID != "" {
			if _, exists := providerOperations[operation.ProviderOperationID]; exists {
				add("duplicate_provider_operation_id", false)
			}
			providerOperations[operation.ProviderOperationID] = struct{}{}
		}
		switch operation.Type {
		case "capture":
			captures++
			if operation.State == "succeeded" {
				captured += operation.AmountMinor
			}
		case "refund":
			refunds++
			if operation.State == "succeeded" {
				refunded += operation.AmountMinor
			}
		}
	}
	if captures > 1 {
		add("duplicate_capture_operation", false)
	}
	if refunds > 1 {
		add("duplicate_refund_operation", false)
	}
	if captures > 0 && captured != snapshot.Intent.AmountMinor {
		add("captured_amount_mismatch", false)
	}
	if refunded > captured {
		add("refund_exceeds_capture", false)
	}
	if snapshot.Saga.State == "completed" && (captures != 1 || captured != snapshot.Intent.AmountMinor) {
		add("completed_saga_without_full_capture", false)
	}
	if uncertain && snapshot.ActiveReconciliationCases == 0 {
		add("uncertain_operation_without_reconciliation", false)
	}
}

func checkShard(scope Scope, control ControlSnapshot, shard ShardSnapshot, add func(string, bool)) {
	if scope != ScopeIntents && scope != ScopeTickets && scope != ScopeAll {
		return
	}
	if !shard.DirectoryResolved {
		add("reservation_directory_unresolved", false)
		return
	}
	if !shard.Found {
		add("shard_payment_state_missing", false)
		return
	}
	if control.Intent.AmountMinor != shard.ReservationAmountMinor {
		add("reservation_amount_mismatch", false)
	}
	if control.Intent.Currency != shard.ReservationCurrency {
		add("reservation_currency_mismatch", false)
	}
	captured := hasSucceeded(control.Operations, "capture")
	refunded := hasSucceeded(control.Operations, "refund")
	if scope == ScopeIntents || scope == ScopeAll {
		if captured && shard.ReservationState != "payment_pending" && shard.ReservationState != "confirmed" && shard.ReservationState != "refund_pending" && shard.ReservationState != "cancelled" {
			add("captured_payment_reservation_state_mismatch", false)
		}
	}
	if scope != ScopeTickets && scope != ScopeAll {
		return
	}
	if control.Intent.State == "completed" && (!shard.TicketOrderFound || shard.TicketOrderState != "issued") {
		add("completed_payment_without_issued_ticket_order", false)
	}
	if captured && (!shard.TicketOrderFound || shard.TicketOrderState != "issued") {
		add("captured_payment_without_ticket", false)
	}
	if !captured && (shard.TicketOrderState == "issued" || shard.ActiveTicketCount > 0) {
		add("ticket_without_captured_payment", false)
	}
	if shard.TicketOrderFound && shard.TicketOrderAmountMinor != control.Intent.AmountMinor {
		add("ticket_order_amount_mismatch", false)
	}
	if shard.TicketOrderFound && shard.TicketOrderCurrency != control.Intent.Currency {
		add("ticket_order_currency_mismatch", false)
	}
	if shard.TicketOrderState == "issued" && !shard.IssuanceReceiptFound {
		add("issued_ticket_order_without_receipt", false)
	}
	if shard.IssuanceReceiptFound && shard.IssuancePaymentIntentID != control.Intent.ID {
		add("issuance_receipt_intent_mismatch", false)
	}
	if shard.ReservationState == "confirmed" && shard.ActiveTicketCount == 0 {
		add("confirmed_reservation_without_active_ticket", false)
	} else if shard.TicketOrderState == "issued" && shard.ActiveTicketCount != shard.ReservationSeatCount {
		add("ticket_count_mismatch", false)
	}
	if refunded && shard.ActiveTicketCount > 0 {
		add("fully_refunded_payment_with_active_ticket", false)
	}
	if shard.CancelledTicketCount > 0 && captured && !refunded && control.Intent.State != "refund_pending" {
		add("cancelled_ticket_without_required_refund", false)
	}
	if shard.DuplicateTicketCodeCount > 0 {
		add("duplicate_ticket_code", false)
	}
	if shard.IssuanceReceiptFound && shard.ReceiptFingerprint != ([sha256.Size]byte{}) && shard.ReceiptFingerprint != control.Intent.Fingerprint {
		add("shard_control_fingerprint_mismatch", false)
	}
}

func checkProvider(ctx context.Context, registry ProviderRegistry, control ControlSnapshot, add func(string, bool)) {
	if registry == nil || control.Intent.ProviderPaymentID == "" {
		add("provider_status_unavailable", false)
		return
	}
	client, ok := registry.Provider(control.Intent.Provider)
	if !ok || client == nil {
		add("provider_status_unavailable", false)
		return
	}
	status, err := client.GetPaymentStatus(ctx, control.Intent.ProviderPaymentID)
	if err != nil {
		add("provider_status_query_failed", false)
		return
	}
	if status.AmountMinor != control.Intent.AmountMinor || status.Currency != control.Intent.Currency {
		add("provider_money_mismatch", false)
	}
	localCaptured := succeededAmount(control.Operations, "capture")
	localRefunded := succeededAmount(control.Operations, "refund")
	if status.CapturedMinor != localCaptured || hasSucceeded(control.Operations, "capture") && status.Status != provider.StatusCaptured && status.Status != provider.StatusRefunded {
		add("provider_capture_mismatch", false)
	}
	if status.RefundedMinor != localRefunded || hasSucceeded(control.Operations, "refund") && status.Status != provider.StatusRefunded {
		add("provider_refund_mismatch", false)
	}
}

func shouldQueryProvider(scope Scope, control ControlSnapshot) bool {
	if scope == ScopeProvider || scope == ScopeAll {
		return true
	}
	for _, operation := range control.Operations {
		if operation.State == "uncertain" {
			return true
		}
	}
	return false
}

func hasSucceeded(operations []Operation, kind string) bool {
	for _, operation := range operations {
		if operation.Type == kind && operation.State == "succeeded" {
			return true
		}
	}
	return false
}

func succeededAmount(operations []Operation, kind string) int64 {
	var total int64
	for _, operation := range operations {
		if operation.Type == kind && operation.State == "succeeded" {
			total += operation.AmountMinor
		}
	}
	return total
}

func matchingRecordedCommand(code string, commands []RecordedCommand) (RecordedCommand, bool) {
	wanted := ""
	switch code {
	case "captured_payment_without_ticket", "completed_payment_without_issued_ticket_order", "issued_ticket_order_without_receipt", "confirmed_reservation_without_active_ticket":
		wanted = "issue_tickets"
	case "fully_refunded_payment_with_active_ticket":
		wanted = "apply_refund_compensation"
	}
	for _, command := range commands {
		if command.Kind == wanted && validateRecordedCommand(command) == nil {
			return command, true
		}
	}
	return RecordedCommand{}, false
}

func commandForFinding(finding Finding) (RecordedCommand, bool) {
	if finding.CommandID != uuid.Nil && finding.commandFingerprint != ([sha256.Size]byte{}) {
		return RecordedCommand{ID: finding.CommandID, Kind: repairKind(finding.Code), Fingerprint: finding.commandFingerprint}, true
	}
	return RecordedCommand{}, false
}

func repairKind(code string) string {
	switch code {
	case "captured_payment_without_ticket", "completed_payment_without_issued_ticket_order", "issued_ticket_order_without_receipt", "confirmed_reservation_without_active_ticket":
		return "issue_tickets"
	case "fully_refunded_payment_with_active_ticket":
		return "apply_refund_compensation"
	default:
		return ""
	}
}

func validateRecordedCommand(command RecordedCommand) error {
	if command.ID == uuid.Nil || (command.Kind != "issue_tickets" && command.Kind != "apply_refund_compensation") || command.Fingerprint == ([sha256.Size]byte{}) {
		return ErrInvalidRequest
	}
	return nil
}

func canonicalIDs(ids []uuid.UUID, limit int) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{}, len(ids))
	result := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		if id == uuid.Nil {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return strings.Compare(result[i].String(), result[j].String()) < 0 })
	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

func singleIntent(ids []uuid.UUID) uuid.UUID {
	if len(ids) == 1 {
		return ids[0]
	}
	return uuid.Nil
}

func (r *Reconciler) now() time.Time { return r.config.Now().UTC() }
