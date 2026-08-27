package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	paymentapp "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/application"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	authoritypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type RouteResolver interface {
	Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error)
}

func (store *Store) PlanTicketIssue(ctx context.Context, route sharding.ShardRoute, command paymentshard.IssueTicketsCommand) (paymentshard.TicketIdentityPlan, error) {
	if !validIssueCommandBase(route, command) {
		return paymentshard.TicketIdentityPlan{}, paymentapp.ErrReservationNotPayable
	}
	resolved, err := store.resolve(ctx, route, false, true)
	if err != nil {
		return paymentshard.TicketIdentityPlan{}, err
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return paymentshard.TicketIdentityPlan{}, paymentshard.ErrShardPaymentUnavailable
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var owner uuid.UUID
	var status string
	var amount int64
	var currency string
	var intentID pgtype.UUID
	if err := tx.QueryRow(ctx, `SELECT user_id,status,total_amount_minor,currency,payment_intent_id
FROM public.reservations
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3`, command.ReservationID,
		route.TrainRunID(), route.Generation().Int64()).Scan(&owner, &status, &amount, &currency, &intentID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return paymentshard.TicketIdentityPlan{}, paymentapp.ErrPaymentNotFound
		}
		return paymentshard.TicketIdentityPlan{}, paymentshard.ErrShardPaymentUnavailable
	}
	if owner != command.OwnerID || (status != "payment_pending" && status != "payment_review") ||
		!intentID.Valid || intentID.Bytes != command.PaymentIntentID || amount != command.AmountMinor || currency != command.Currency {
		return paymentshard.TicketIdentityPlan{}, paymentapp.ErrReservationNotPayable
	}
	rows, err := tx.Query(ctx, `SELECT id FROM public.reservation_seats
WHERE reservation_id=$1 AND train_run_id=$2 AND assignment_generation=$3 ORDER BY id`,
		command.ReservationID, route.TrainRunID(), route.Generation().Int64())
	if err != nil {
		return paymentshard.TicketIdentityPlan{}, paymentshard.ErrShardPaymentUnavailable
	}
	defer rows.Close()
	plan := paymentshard.TicketIdentityPlan{TicketIDs: make([]uuid.UUID, 0, 8), TicketCodes: make([]string, 0, 8)}
	for rows.Next() {
		var seatID uuid.UUID
		if err := rows.Scan(&seatID); err != nil || seatID == uuid.Nil || len(plan.TicketIDs) >= 100 {
			return paymentshard.TicketIdentityPlan{}, paymentshard.ErrShardPaymentUnavailable
		}
		plan.TicketIDs = append(plan.TicketIDs, uuid.NewSHA1(command.IssuanceID, seatID[:]))
		plan.TicketCodes = append(plan.TicketCodes, opaqueTicketCode(command.IssuanceID, seatID))
	}
	if rows.Err() != nil || len(plan.TicketIDs) == 0 {
		return paymentshard.TicketIdentityPlan{}, paymentshard.ErrShardPaymentUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentshard.TicketIdentityPlan{}, paymentshard.ErrShardPaymentUnavailable
	}
	return plan, nil
}

func (store *Store) IssueTickets(ctx context.Context, route sharding.ShardRoute, command paymentshard.IssueTicketsCommand) (paymentshard.IssueTicketsReceipt, error) {
	if !validIssueCommand(route, command) {
		return paymentshard.IssueTicketsReceipt{}, paymentapp.ErrReservationNotPayable
	}
	resolved, err := store.resolve(ctx, route, false, true)
	if err != nil {
		return paymentshard.IssueTicketsReceipt{}, err
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return paymentshard.IssueTicketsReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	rollback := func(result error) (paymentshard.IssueTicketsReceipt, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return paymentshard.IssueTicketsReceipt{}, result
	}
	if receipt, found, err := loadIssuanceReceipt(ctx, tx, command); err != nil {
		return rollback(err)
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return paymentshard.IssueTicketsReceipt{}, paymentshard.ErrShardPaymentUnavailable
		}
		return receipt, nil
	}
	if err := store.authorizeRegional(ctx, tx); err != nil {
		return rollback(err)
	}
	if err := lockFence(ctx, tx, route); err != nil {
		return rollback(err)
	}
	// A peer may have committed the same command while this transaction was
	// waiting for the train-run fence. Re-read the receipt under the fence so
	// concurrent replicas converge on the durable result instead of attempting
	// a second issuance.
	if receipt, found, err := loadIssuanceReceipt(ctx, tx, command); err != nil {
		return rollback(err)
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return paymentshard.IssueTicketsReceipt{}, paymentshard.ErrShardPaymentUnavailable
		}
		return receipt, nil
	}
	var (
		owner          uuid.UUID
		status         string
		amountMinor    int64
		currency       string
		storedIntentID pgtype.UUID
	)
	err = tx.QueryRow(ctx, `
SELECT user_id,status,total_amount_minor,currency,payment_intent_id
FROM public.reservations
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3
FOR UPDATE`, command.ReservationID, route.TrainRunID(), route.Generation().Int64()).Scan(
		&owner, &status, &amountMinor, &currency, &storedIntentID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return rollback(paymentapp.ErrPaymentNotFound)
	}
	if err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if owner != command.OwnerID {
		return rollback(paymentapp.ErrPaymentNotFound)
	}
	if (status != "payment_pending" && status != "payment_review") || !storedIntentID.Valid ||
		storedIntentID.Bytes != command.PaymentIntentID || amountMinor != command.AmountMinor || currency != command.Currency {
		return rollback(paymentapp.ErrReservationNotPayable)
	}
	ticketOrderID := uuid.NewSHA1(command.PaymentIntentID, []byte("ticket-order"))
	var orderStatus string
	var orderCreatedAt time.Time
	err = tx.QueryRow(ctx, `
SELECT status,created_at FROM public.ticket_orders
WHERE id=$1 AND reservation_id=$2 AND payment_intent_id=$3
  AND train_run_id=$4 AND assignment_generation=$5
  AND total_amount_minor=$6 AND currency=$7
FOR UPDATE`, ticketOrderID, command.ReservationID, command.PaymentIntentID,
		route.TrainRunID(), route.Generation().Int64(), command.AmountMinor, command.Currency).Scan(&orderStatus, &orderCreatedAt)
	if err != nil || (orderStatus != "payment_pending" && orderStatus != "payment_authorized" && orderStatus != "payment_captured" && orderStatus != "issuance_pending") {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if orderCreatedAt.IsZero() {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	commandReceiptID := uuid.NewSHA1(command.CommandID, []byte("payment-command-receipt"))
	if err := execOne(ctx, tx, `
INSERT INTO public.payment_command_receipts(
 id,command_id,payment_intent_id,reservation_id,train_run_id,
 assignment_generation,operation,request_fingerprint,amount_minor,currency,status
) VALUES($1,$2,$3,$4,$5,$6,'payment.capture_recorded',$7,$8,$9,'started')`,
		commandReceiptID, command.CommandID, command.PaymentIntentID, command.ReservationID,
		route.TrainRunID(), route.Generation().Int64(), command.RequestFingerprint[:],
		command.AmountMinor, command.Currency); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `
UPDATE public.ticket_orders
SET status='issued',authorized_amount_minor=total_amount_minor,
    captured_amount_minor=total_amount_minor
WHERE id=$1 AND status IN('payment_pending','payment_authorized','payment_captured','issuance_pending')`, ticketOrderID); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `
UPDATE public.reservations SET status='confirmed'
WHERE id=$1 AND status IN('payment_pending','payment_review')`, command.ReservationID); err != nil {
		return rollback(err)
	}
	rows, err := tx.Query(ctx, `
SELECT id FROM public.reservation_seats
WHERE reservation_id=$1 AND train_run_id=$2 AND assignment_generation=$3
ORDER BY id`, command.ReservationID, route.TrainRunID(), route.Generation().Int64())
	if err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	seatIDs := make([]uuid.UUID, 0, 8)
	for rows.Next() {
		var seatID uuid.UUID
		if err := rows.Scan(&seatID); err != nil || seatID == uuid.Nil {
			rows.Close()
			return rollback(paymentshard.ErrShardPaymentUnavailable)
		}
		seatIDs = append(seatIDs, seatID)
	}
	if rows.Err() != nil || len(seatIDs) == 0 || len(seatIDs) > 100 {
		rows.Close()
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	rows.Close()
	if len(command.PlannedTicketIDs) != len(seatIDs) || len(command.PlannedTicketCodes) != len(seatIDs) {
		return rollback(paymentapp.ErrPaymentConflict)
	}
	ticketIDs := make([]uuid.UUID, 0, len(seatIDs))
	ticketCodes := make([]string, 0, len(seatIDs))
	for index, seatID := range seatIDs {
		ticketID := uuid.NewSHA1(command.IssuanceID, seatID[:])
		ticketCode := opaqueTicketCode(command.IssuanceID, seatID)
		if command.PlannedTicketIDs[index] != ticketID || command.PlannedTicketCodes[index] != ticketCode {
			return rollback(paymentapp.ErrPaymentConflict)
		}
		if err := execOne(ctx, tx, `
INSERT INTO public.tickets(
 id,ticket_order_id,reservation_seat_id,train_run_id,
 assignment_generation,ticket_code,status
) VALUES($1,$2,$3,$4,$5,$6,'active')`, ticketID, ticketOrderID, seatID,
			route.TrainRunID(), route.Generation().Int64(), ticketCode); err != nil {
			return rollback(err)
		}
		if err := appendOutbox(ctx, tx, route, ticketID, "ticket", "ticket.issued", map[string]any{
			"payment_intent_id": command.PaymentIntentID, "reservation_id": command.ReservationID,
			"ticket_order_id": ticketOrderID, "ticket_id": ticketID,
		}); err != nil {
			return rollback(err)
		}
		ticketIDs = append(ticketIDs, ticketID)
		ticketCodes = append(ticketCodes, ticketCode)
	}
	issuanceReceiptID := uuid.NewSHA1(command.IssuanceID, []byte("ticket-issuance-receipt"))
	var issuedAt time.Time
	if err := tx.QueryRow(ctx, `
INSERT INTO public.ticket_issuance_receipts(
 id,issuance_id,payment_intent_id,reservation_id,payment_operation_id,
 ticket_order_id,train_run_id,assignment_generation,capture_proof_hash,
 amount_minor,currency,issued_ticket_count
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
RETURNING created_at`,
		issuanceReceiptID, command.IssuanceID, command.PaymentIntentID, command.ReservationID,
		command.PaymentOperationID, ticketOrderID, route.TrainRunID(), route.Generation().Int64(),
		command.CaptureProofHash[:], command.AmountMinor, command.Currency, len(ticketIDs)).Scan(&issuedAt); err != nil || issuedAt.IsZero() {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if err := appendOutbox(ctx, tx, route, command.ReservationID, "reservation", "reservation.confirmed", map[string]any{
		"payment_intent_id": command.PaymentIntentID, "reservation_id": command.ReservationID,
		"ticket_order_id": ticketOrderID, "ticket_count": len(ticketIDs),
	}); err != nil {
		return rollback(err)
	}
	if err := appendOutbox(ctx, tx, route, ticketOrderID, "ticket_order", "ticket_order.issued", map[string]any{
		"payment_intent_id": command.PaymentIntentID, "reservation_id": command.ReservationID,
		"ticket_order_id": ticketOrderID, "ticket_count": len(ticketIDs),
	}); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `
UPDATE public.payment_command_receipts
SET status='succeeded',result_resource_id=$2,result_status='confirmed',
    committed_at=clock_timestamp()
WHERE command_id=$1 AND status='started'`, command.CommandID, ticketOrderID); err != nil {
		return rollback(err)
	}
	if err := recordTargetWrite(ctx, tx, route, command.CommandID); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentshard.IssueTicketsReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	return paymentshard.IssueTicketsReceipt{
		CommandID: command.CommandID, IssuanceID: command.IssuanceID, PaymentIntentID: command.PaymentIntentID,
		ReservationID: command.ReservationID, TicketOrderID: ticketOrderID, TicketIDs: ticketIDs, TicketCodes: ticketCodes,
		AmountMinor: command.AmountMinor, Currency: command.Currency,
		OrderCreatedAt: orderCreatedAt.UTC(), IssuedAt: issuedAt.UTC(),
	}, nil
}

func validIssueCommand(route sharding.ShardRoute, command paymentshard.IssueTicketsCommand) bool {
	return validIssueCommandBase(route, command) && len(command.PlannedTicketIDs) > 0 &&
		len(command.PlannedTicketIDs) == len(command.PlannedTicketCodes)
}

func validIssueCommandBase(route sharding.ShardRoute, command paymentshard.IssueTicketsCommand) bool {
	return command.CommandID != uuid.Nil && command.IssuanceID != uuid.Nil && command.PaymentIntentID != uuid.Nil &&
		command.PaymentOperationID != uuid.Nil && command.ReservationID != uuid.Nil && command.OwnerID != uuid.Nil &&
		command.TrainRunID == route.TrainRunID() && command.AmountMinor >= 0 && len(command.Currency) == 3 &&
		command.CaptureProofHash != [32]byte{} && command.RequestFingerprint != [32]byte{}
}

func opaqueTicketCode(issuanceID, seatID uuid.UUID) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte("m6-ticket-v1"))
	_, _ = digest.Write(issuanceID[:])
	_, _ = digest.Write(seatID[:])
	return base64.RawURLEncoding.EncodeToString(digest.Sum(nil)[:24])
}

func appendOutbox(ctx context.Context, tx pgx.Tx, route sharding.ShardRoute, aggregateID uuid.UUID, aggregateType, eventType string, fields map[string]any) error {
	payload, err := json.Marshal(fields)
	if err != nil {
		return paymentshard.ErrShardPaymentUnavailable
	}
	eventID := uuid.NewSHA1(aggregateID, []byte(eventType))
	return execOne(ctx, tx, `
INSERT INTO public.outbox_events(
 id,train_run_id,assignment_generation,aggregate_type,aggregate_id,event_type,payload
) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb)`, eventID, route.TrainRunID(),
		route.Generation().Int64(), aggregateType, aggregateID, eventType, string(payload))
}

func loadIssuanceReceipt(ctx context.Context, tx pgx.Tx, command paymentshard.IssueTicketsCommand) (paymentshard.IssueTicketsReceipt, bool, error) {
	var (
		paymentIntentID, reservationID, operationID, orderID uuid.UUID
		proof                                                []byte
		amount                                               int64
		currency                                             string
		count                                                int
		createdAt                                            pgtype.Timestamptz
	)
	err := tx.QueryRow(ctx, `
SELECT payment_intent_id,reservation_id,payment_operation_id,ticket_order_id,
       capture_proof_hash,amount_minor,currency,issued_ticket_count,created_at
FROM public.ticket_issuance_receipts WHERE issuance_id=$1`, command.IssuanceID).Scan(
		&paymentIntentID, &reservationID, &operationID, &orderID, &proof, &amount, &currency, &count, &createdAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return paymentshard.IssueTicketsReceipt{}, false, nil
	}
	if err != nil || paymentIntentID != command.PaymentIntentID || reservationID != command.ReservationID ||
		operationID != command.PaymentOperationID || amount != command.AmountMinor || currency != command.Currency ||
		len(proof) != 32 || count < 1 || count > 100 || !createdAt.Valid {
		return paymentshard.IssueTicketsReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	var storedProof [32]byte
	copy(storedProof[:], proof)
	if storedProof != command.CaptureProofHash {
		return paymentshard.IssueTicketsReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	var (
		commandIntentID    uuid.UUID
		commandFingerprint []byte
		commandStatus      string
		commandResultID    pgtype.UUID
	)
	if err := tx.QueryRow(ctx, `
SELECT payment_intent_id,request_fingerprint,status,result_resource_id
FROM public.payment_command_receipts
WHERE command_id=$1 AND operation='payment.capture_recorded'`, command.CommandID).Scan(
		&commandIntentID, &commandFingerprint, &commandStatus, &commandResultID,
	); err != nil || commandIntentID != command.PaymentIntentID || len(commandFingerprint) != 32 ||
		commandStatus != "succeeded" || !commandResultID.Valid || commandResultID.Bytes != orderID {
		return paymentshard.IssueTicketsReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	var storedFingerprint [32]byte
	copy(storedFingerprint[:], commandFingerprint)
	if storedFingerprint != command.RequestFingerprint {
		return paymentshard.IssueTicketsReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	var orderCreatedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT created_at
FROM public.ticket_orders
WHERE id=$1 AND reservation_id=$2 AND payment_intent_id=$3
  AND status='issued' AND total_amount_minor=$4 AND currency=$5`,
		orderID, command.ReservationID, command.PaymentIntentID,
		command.AmountMinor, command.Currency).Scan(&orderCreatedAt); err != nil || orderCreatedAt.IsZero() {
		return paymentshard.IssueTicketsReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	rows, err := tx.Query(ctx, `SELECT id,ticket_code FROM public.tickets WHERE ticket_order_id=$1 ORDER BY id LIMIT 101`, orderID)
	if err != nil {
		return paymentshard.IssueTicketsReceipt{}, false, paymentshard.ErrShardPaymentUnavailable
	}
	defer rows.Close()
	ticketIDs := make([]uuid.UUID, 0, count)
	ticketCodes := make([]string, 0, count)
	for rows.Next() {
		var ticketID uuid.UUID
		var ticketCode string
		if err := rows.Scan(&ticketID, &ticketCode); err != nil || ticketID == uuid.Nil || !paymentshard.ValidTicketCode(ticketCode) {
			return paymentshard.IssueTicketsReceipt{}, false, paymentshard.ErrShardPaymentUnavailable
		}
		ticketIDs = append(ticketIDs, ticketID)
		ticketCodes = append(ticketCodes, ticketCode)
	}
	if rows.Err() != nil || len(ticketIDs) != count {
		return paymentshard.IssueTicketsReceipt{}, false, paymentshard.ErrShardPaymentUnavailable
	}
	return paymentshard.IssueTicketsReceipt{
		CommandID: command.CommandID, IssuanceID: command.IssuanceID, PaymentIntentID: paymentIntentID,
		ReservationID: reservationID, TicketOrderID: orderID, TicketIDs: ticketIDs, TicketCodes: ticketCodes,
		AmountMinor: amount, Currency: currency,
		OrderCreatedAt: orderCreatedAt.UTC(), IssuedAt: createdAt.Time.UTC(),
	}, true, nil
}

type Store struct {
	router     RouteResolver
	deployment *authority.Deployment
}

type StoreOption func(*Store) error

func WithRegionalAuthority(deployment authority.Deployment) StoreOption {
	return func(store *Store) error {
		store.deployment = &deployment
		return nil
	}
}

func NewStore(router RouteResolver, options ...StoreOption) (*Store, error) {
	if router == nil {
		return nil, paymentshard.ErrInvalidGateway
	}
	store := &Store{router: router}
	for _, option := range options {
		if option == nil || option(store) != nil {
			return nil, paymentshard.ErrInvalidGateway
		}
	}
	return store, nil
}

func (store *Store) GetPayableReservation(ctx context.Context, route sharding.ShardRoute, reservationID uuid.UUID) (paymentapp.ReservationSnapshot, error) {
	resolved, err := store.resolve(ctx, route, false, false)
	if err != nil {
		return paymentapp.ReservationSnapshot{}, err
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return paymentapp.ReservationSnapshot{}, paymentshard.ErrShardPaymentUnavailable
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var snapshot paymentapp.ReservationSnapshot
	err = tx.QueryRow(ctx, `
SELECT id,user_id,train_run_id,status,total_amount_minor,currency,expires_at
FROM public.reservations
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3`,
		reservationID, route.TrainRunID(), route.Generation().Int64()).Scan(
		&snapshot.ID, &snapshot.OwnerID, &snapshot.TrainRunID, &snapshot.Status,
		&snapshot.AmountMinor, &snapshot.Currency, &snapshot.ExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return paymentapp.ReservationSnapshot{}, paymentapp.ErrPaymentNotFound
	}
	if err != nil || snapshot.ID != reservationID || snapshot.TrainRunID != route.TrainRunID() {
		return paymentapp.ReservationSnapshot{}, paymentshard.ErrShardPaymentUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentapp.ReservationSnapshot{}, paymentshard.ErrShardPaymentUnavailable
	}
	snapshot.ExpiresAt = snapshot.ExpiresAt.UTC()
	return snapshot, nil
}

func (store *Store) BeginPayment(ctx context.Context, route sharding.ShardRoute, command paymentapp.BeginPaymentCommand) (paymentapp.BeginPaymentReceipt, error) {
	if !validBeginCommand(route, command) {
		return paymentapp.BeginPaymentReceipt{}, paymentapp.ErrReservationNotPayable
	}
	resolved, err := store.resolve(ctx, route, false, true)
	if err != nil {
		return paymentapp.BeginPaymentReceipt{}, err
	}
	tx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return paymentapp.BeginPaymentReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	rollback := func(result error) (paymentapp.BeginPaymentReceipt, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return paymentapp.BeginPaymentReceipt{}, result
	}
	if receipt, found, err := loadBeginReceipt(ctx, tx, command); err != nil {
		return rollback(err)
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return paymentapp.BeginPaymentReceipt{}, paymentshard.ErrShardPaymentUnavailable
		}
		return receipt, nil
	}
	if err := store.authorizeRegional(ctx, tx); err != nil {
		return rollback(err)
	}
	if err := lockFence(ctx, tx, route); err != nil {
		return rollback(err)
	}
	// Recheck after acquiring the fence: another replica may have completed
	// this idempotent command while this transaction was blocked.
	if receipt, found, err := loadBeginReceipt(ctx, tx, command); err != nil {
		return rollback(err)
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return paymentapp.BeginPaymentReceipt{}, paymentshard.ErrShardPaymentUnavailable
		}
		return receipt, nil
	}
	var (
		owner          uuid.UUID
		status         string
		expiresAt      pgtype.Timestamptz
		amountMinor    int64
		currency       string
		storedIntentID pgtype.UUID
		unexpired      bool
	)
	err = tx.QueryRow(ctx, `
SELECT user_id,status,expires_at,total_amount_minor,currency,payment_intent_id,
       expires_at > clock_timestamp()
FROM public.reservations
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3
FOR UPDATE`, command.ReservationID, route.TrainRunID(), route.Generation().Int64()).Scan(
		&owner, &status, &expiresAt, &amountMinor, &currency, &storedIntentID, &unexpired,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return rollback(paymentapp.ErrPaymentNotFound)
	}
	if err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if owner != command.OwnerID {
		return rollback(paymentapp.ErrPaymentNotFound)
	}
	if status != "held" || !expiresAt.Valid || !unexpired || !command.GraceExpiresAt.After(expiresAt.Time) ||
		amountMinor != command.AmountMinor || currency != command.Currency || storedIntentID.Valid {
		return rollback(paymentapp.ErrReservationNotPayable)
	}
	receiptID := uuid.NewSHA1(command.CommandID, []byte("payment-command-receipt"))
	if err := execOne(ctx, tx, `
UPDATE public.reservations
SET status='payment_pending',payment_intent_id=$2,payment_amount_minor=$3,
    payment_currency=$4,payment_grace_expires_at=$5
WHERE id=$1 AND train_run_id=$6 AND assignment_generation=$7
  AND status='held' AND payment_intent_id IS NULL
  AND expires_at > clock_timestamp()`, command.ReservationID, command.PaymentIntentID,
		command.AmountMinor, command.Currency, command.GraceExpiresAt.UTC(),
		route.TrainRunID(), route.Generation().Int64()); err != nil {
		return rollback(err)
	}
	// The receipt FK includes the immutable payment authority snapshot. Persist
	// it only after the reservation row carries that snapshot; both writes are
	// still atomic in this transaction, while immediate FK checking remains a
	// fail-closed guard against mismatched intent or money.
	if err := execOne(ctx, tx, `
INSERT INTO public.payment_command_receipts(
 id,command_id,payment_intent_id,reservation_id,train_run_id,
 assignment_generation,operation,request_fingerprint,amount_minor,currency,status
) VALUES($1,$2,$3,$4,$5,$6,'reservation.payment_begin',$7,$8,$9,'started')`,
		receiptID, command.CommandID, command.PaymentIntentID, command.ReservationID,
		route.TrainRunID(), route.Generation().Int64(), command.RequestFingerprint[:],
		command.AmountMinor, command.Currency); err != nil {
		return rollback(err)
	}
	ticketOrderID := uuid.NewSHA1(command.PaymentIntentID, []byte("ticket-order"))
	if err := execOne(ctx, tx, `
INSERT INTO public.ticket_orders(
 id,reservation_id,user_id,train_run_id,assignment_generation,status,
 total_amount_minor,currency,payment_intent_id,payment_currency
) VALUES($1,$2,$3,$4,$5,'payment_pending',$6,$7,$8,$7)`,
		ticketOrderID, command.ReservationID, command.OwnerID, route.TrainRunID(),
		route.Generation().Int64(), command.AmountMinor, command.Currency, command.PaymentIntentID); err != nil {
		return rollback(err)
	}
	payload, err := json.Marshal(map[string]any{
		"command_id": command.CommandID, "payment_intent_id": command.PaymentIntentID,
		"reservation_id": command.ReservationID, "status": "payment_pending",
	})
	if err != nil {
		return rollback(paymentshard.ErrShardPaymentUnavailable)
	}
	if err := execOne(ctx, tx, `
INSERT INTO public.outbox_events(
 id,train_run_id,assignment_generation,aggregate_type,aggregate_id,event_type,payload
) VALUES($1,$2,$3,'reservation',$4,'reservation.payment_pending',$5::jsonb)`,
		uuid.NewSHA1(command.CommandID, []byte("reservation.payment_pending")), route.TrainRunID(),
		route.Generation().Int64(), command.ReservationID, string(payload)); err != nil {
		return rollback(err)
	}
	if err := execOne(ctx, tx, `
UPDATE public.payment_command_receipts
SET status='succeeded',result_resource_id=$2,result_status='payment_pending',
    committed_at=clock_timestamp()
WHERE command_id=$1 AND status='started'`, command.CommandID, command.ReservationID); err != nil {
		return rollback(err)
	}
	if err := recordTargetWrite(ctx, tx, route, command.CommandID); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return paymentapp.BeginPaymentReceipt{}, paymentshard.ErrShardPaymentUnavailable
	}
	return paymentapp.BeginPaymentReceipt{CommandID: command.CommandID, PaymentIntentID: command.PaymentIntentID, RequestFingerprint: command.RequestFingerprint}, nil
}

func (store *Store) resolve(ctx context.Context, route sharding.ShardRoute, refresh, requireWrite bool) (shardphysical.Resolution, error) {
	if store == nil || store.router == nil || ctx == nil {
		return shardphysical.Resolution{}, paymentshard.ErrShardPaymentUnavailable
	}
	resolved, err := store.router.Resolve(ctx, route.TrainRunID(), refresh)
	if err != nil || resolved.Handle.Pool() == nil {
		return shardphysical.Resolution{}, paymentshard.ErrShardPaymentUnavailable
	}
	if resolved.Route.TrainRunID() != route.TrainRunID() || resolved.Route.ShardID() != route.ShardID() || resolved.Route.Generation() != route.Generation() {
		return shardphysical.Resolution{}, sharding.ErrAssignmentStale
	}
	if requireWrite && !resolved.Handle.WriteEnabled() {
		return shardphysical.Resolution{}, sharding.ErrWriteFenced
	}
	return resolved, nil
}

func validBeginCommand(route sharding.ShardRoute, command paymentapp.BeginPaymentCommand) bool {
	return command.CommandID != uuid.Nil && command.PaymentIntentID != uuid.Nil && command.ReservationID != uuid.Nil &&
		command.OwnerID != uuid.Nil && command.TrainRunID == route.TrainRunID() && command.AmountMinor > 0 &&
		len(command.Currency) == 3 && !command.GraceExpiresAt.IsZero() && command.RequestFingerprint != [32]byte{}
}

func loadBeginReceipt(ctx context.Context, tx pgx.Tx, command paymentapp.BeginPaymentCommand) (paymentapp.BeginPaymentReceipt, bool, error) {
	var (
		storedIntent uuid.UUID
		fingerprint  []byte
		status       string
		resultID     pgtype.UUID
		resultState  pgtype.Text
	)
	err := tx.QueryRow(ctx, `
SELECT payment_intent_id,request_fingerprint,status,result_resource_id,result_status
FROM public.payment_command_receipts WHERE command_id=$1`, command.CommandID).Scan(
		&storedIntent, &fingerprint, &status, &resultID, &resultState,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return paymentapp.BeginPaymentReceipt{}, false, nil
	}
	if err != nil || len(fingerprint) != 32 || storedIntent != command.PaymentIntentID || status != "succeeded" ||
		!resultID.Valid || resultID.Bytes != command.ReservationID || !resultState.Valid || resultState.String != "payment_pending" {
		return paymentapp.BeginPaymentReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	var stored [32]byte
	copy(stored[:], fingerprint)
	if stored != command.RequestFingerprint {
		return paymentapp.BeginPaymentReceipt{}, false, paymentapp.ErrPaymentConflict
	}
	return paymentapp.BeginPaymentReceipt{CommandID: command.CommandID, PaymentIntentID: command.PaymentIntentID, RequestFingerprint: stored}, true, nil
}

func lockFence(ctx context.Context, tx pgx.Tx, route sharding.ShardRoute) error {
	var generation int64
	var writeEnabled bool
	var state string
	if err := tx.QueryRow(ctx, `
SELECT assignment_generation,write_enabled,state
FROM public.train_run_write_fences
WHERE train_run_id=$1 FOR UPDATE`, route.TrainRunID()).Scan(&generation, &writeEnabled, &state); err != nil {
		return paymentshard.ErrShardPaymentUnavailable
	}
	if generation != route.Generation().Int64() {
		return sharding.ErrAssignmentStale
	}
	if !writeEnabled || state != "active" {
		return sharding.ErrWriteFenced
	}
	return nil
}

func (store *Store) authorizeRegional(ctx context.Context, tx pgx.Tx) error {
	if store == nil || store.deployment == nil {
		return nil
	}
	if err := authoritypostgres.AuthorizeControlTransaction(ctx, tx, *store.deployment); err != nil {
		return sharding.ErrWriteFenced
	}
	return nil
}

func execOne(ctx context.Context, tx pgx.Tx, query string, args ...any) error {
	tag, err := tx.Exec(ctx, query, args...)
	if err != nil || tag.RowsAffected() != 1 {
		return paymentshard.ErrShardPaymentUnavailable
	}
	return nil
}

func recordTargetWrite(ctx context.Context, tx pgx.Tx, route sharding.ShardRoute, commandID uuid.UUID) error {
	return execOne(ctx, tx, `
INSERT INTO public.train_run_target_write_evidence(
 id,train_run_id,assignment_generation,successful_write_count,
 first_successful_write_at,last_successful_write_at,last_command_id
) VALUES($1,$2,$3,1,clock_timestamp(),clock_timestamp(),$4)
ON CONFLICT(train_run_id,assignment_generation) DO UPDATE
SET successful_write_count=train_run_target_write_evidence.successful_write_count+1,
    first_successful_write_at=COALESCE(train_run_target_write_evidence.first_successful_write_at,EXCLUDED.first_successful_write_at),
    last_successful_write_at=EXCLUDED.last_successful_write_at,last_command_id=EXCLUDED.last_command_id`,
		uuid.NewSHA1(route.TrainRunID(), []byte("target-write-evidence:"+route.ShardID().String()+":"+strconv.FormatInt(route.Generation().Int64(), 10))),
		route.TrainRunID(), route.Generation().Int64(), commandID)
}

var _ paymentshard.Directory = (*Directory)(nil)
var _ paymentshard.Store = (*Store)(nil)
