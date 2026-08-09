package postgres

import (
	"context"
	"errors"

	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ClaimTicketCodes reserves immutable global identities before any shard-local
// ticket transaction is attempted. A claim is a tombstone and is deliberately
// retained if the later shard command never commits.
func (store *Store) ClaimTicketCodes(ctx context.Context, command paymentshard.IssueTicketsCommand, plan paymentshard.TicketIdentityPlan) error {
	if store == nil || ctx == nil || command.PaymentIntentID == uuid.Nil || command.ReservationID == uuid.Nil ||
		len(plan.TicketIDs) == 0 || len(plan.TicketIDs) > 100 || len(plan.TicketIDs) != len(plan.TicketCodes) {
		return paymentapp.ErrPaymentUnavailable
	}
	seenIDs := make(map[uuid.UUID]struct{}, len(plan.TicketIDs))
	seenCodes := make(map[string]struct{}, len(plan.TicketCodes))
	for index, ticketID := range plan.TicketIDs {
		code := plan.TicketCodes[index]
		if ticketID == uuid.Nil || !paymentshard.ValidTicketCode(code) {
			return paymentapp.ErrPaymentUnavailable
		}
		if _, exists := seenIDs[ticketID]; exists {
			return paymentapp.ErrPaymentConflict
		}
		if _, exists := seenCodes[code]; exists {
			return paymentapp.ErrPaymentConflict
		}
		seenIDs[ticketID] = struct{}{}
		seenCodes[code] = struct{}{}
	}
	tx, err := store.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return paymentapp.ErrPaymentUnavailable
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var authorized bool
	if err := tx.QueryRow(ctx, `SELECT true
FROM public.ticket_code_claim_readiness AS readiness
WHERE readiness.singleton
  AND readiness.state='ready'
  AND EXISTS (
      SELECT 1 FROM public.payment_intents AS intent
      JOIN public.payment_sagas AS saga ON saga.payment_intent_id=intent.payment_intent_id
      WHERE intent.payment_intent_id=$1 AND intent.reservation_id=$2
        AND intent.state='ticket_issue_pending'
        AND saga.state='issuing_tickets'
  )
FOR UPDATE`, command.PaymentIntentID, command.ReservationID).Scan(&authorized); err != nil {
		return errors.Join(paymentapp.ErrPaymentUnavailable, paymentshard.ErrTicketClaimReadFailed, classifyTicketClaimReadError(err))
	} else if !authorized {
		return errors.Join(paymentapp.ErrPaymentUnavailable, paymentshard.ErrTicketClaimUnauthorized)
	}
	for index, ticketID := range plan.TicketIDs {
		tag, err := tx.Exec(ctx, `INSERT INTO public.ticket_code_directory(ticket_code,ticket_id)
VALUES($1,$2)
ON CONFLICT(ticket_code) DO UPDATE SET ticket_id=EXCLUDED.ticket_id
			WHERE ticket_code_directory.ticket_id=EXCLUDED.ticket_id`, plan.TicketCodes[index], ticketID)
		if err != nil || tag.RowsAffected() != 1 {
			return errors.Join(paymentapp.ErrPaymentConflict, paymentshard.ErrTicketClaimConflict)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.Join(paymentapp.ErrPaymentUnavailable, paymentshard.ErrTicketClaimCommitFailed)
	}
	return nil
}

func classifyTicketClaimReadError(err error) error {
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return paymentshard.ErrTicketClaimUnauthorized
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return paymentshard.ErrTicketClaimReadTimeout
	default:
		var scanError pgx.ScanArgError
		if errors.As(err, &scanError) {
			return paymentshard.ErrTicketClaimScanFailed
		}
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) {
			return paymentshard.ErrTicketClaimSQLFailed
		}
		return paymentshard.ErrTicketClaimDecodeFailed
	}
}

var _ paymentshard.TicketCodeClaimer = (*Store)(nil)
