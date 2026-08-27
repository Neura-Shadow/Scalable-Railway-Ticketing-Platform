// Package postgres persists whole-ticket subset refund requests in the
// control database while deriving ticket facts from the current physical
// shard. Raw idempotency keys and provider payloads are never stored.
package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"time"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/refund"
	paymentshard "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/payment/shard"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority"
	authoritypostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/regional/authority/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type ControlDB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type ShardReader interface {
	LoadRefundOrder(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (paymentshard.RefundOrderSnapshot, error)
}

type Config struct {
	PartialRefundProviders map[string]bool
	RegionalAuthority      *authority.Deployment
}

type Store struct {
	db         ControlDB
	shard      ShardReader
	providers  map[string]bool
	deployment *authority.Deployment
}

func NewStore(db ControlDB, shard ShardReader, config Config) (*Store, error) {
	if db == nil || shard == nil || len(config.PartialRefundProviders) == 0 {
		return nil, refund.ErrInvalidService
	}
	providers := make(map[string]bool, len(config.PartialRefundProviders))
	for provider, supported := range config.PartialRefundProviders {
		if provider == "" {
			return nil, refund.ErrInvalidService
		}
		providers[provider] = supported
	}
	store := &Store{db: db, shard: shard, providers: providers}
	if config.RegionalAuthority != nil {
		deployment := *config.RegionalAuthority
		store.deployment = &deployment
	}
	return store, nil
}

func ProviderIdempotencyKey(operationID uuid.UUID) (string, refund.Hash) {
	if operationID == uuid.Nil {
		return "", refund.Hash{}
	}
	key := "ticket-refund-" + operationID.String()
	return key, sha256.Sum256([]byte(key))
}

func (store *Store) LoadOrder(ctx context.Context, ownerID, orderID uuid.UUID) (refund.OrderSnapshot, bool, error) {
	if store == nil || store.db == nil || store.shard == nil || ctx == nil || ownerID == uuid.Nil || orderID == uuid.Nil {
		return refund.OrderSnapshot{}, false, refund.ErrNotFound
	}
	var (
		reservationID, trainRunID, locatorOwnerID uuid.UUID
		shardID, locatorState, locatorCurrency    string
		generation, locatorAmount                 int64
		departureAt                               time.Time
	)
	err := store.db.QueryRow(ctx, `
SELECT locator.reservation_id,locator.train_run_id,locator.shard_id,
       locator.assignment_generation,locator.owner_user_id,locator.status,
       locator.total_amount_minor,locator.currency,run.scheduled_departure_at
FROM public.ticket_order_shard_locators AS locator
JOIN public.train_runs AS run ON run.id=locator.train_run_id
WHERE locator.ticket_order_id=$1 AND locator.owner_user_id=$2`, orderID, ownerID).Scan(
		&reservationID, &trainRunID, &shardID, &generation, &locatorOwnerID, &locatorState,
		&locatorAmount, &locatorCurrency, &departureAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return refund.OrderSnapshot{}, false, nil
	}
	if err != nil {
		return refund.OrderSnapshot{}, false, err
	}
	if generation <= 0 || locatorState != "confirmed" || departureAt.IsZero() {
		return refund.OrderSnapshot{}, false, refund.ErrSnapshotConflict
	}
	physical, err := store.shard.LoadRefundOrder(ctx, reservationID, orderID, ownerID)
	if err != nil {
		return refund.OrderSnapshot{}, false, err
	}
	if physical.TicketOrderID != orderID || physical.ReservationID != reservationID ||
		physical.TrainRunID != trainRunID || locatorOwnerID != ownerID || physical.OwnerID != ownerID ||
		physical.AssignmentGeneration != uint64(generation) || physical.CapturedMinor != locatorAmount ||
		physical.Currency != locatorCurrency {
		return refund.OrderSnapshot{}, false, refund.ErrSnapshotConflict
	}
	var (
		intentReservation, intentTrainRun, intentOwner           uuid.UUID
		provider, providerPaymentID, intentCurrency, intentState string
		intentAmount                                             int64
	)
	err = store.db.QueryRow(ctx, `
SELECT reservation_id,train_run_id,owner_user_id,provider,provider_payment_id,
       amount_minor,currency,state
FROM public.payment_intents WHERE payment_intent_id=$1`, physical.PaymentIntentID).Scan(
		&intentReservation, &intentTrainRun, &intentOwner, &provider, &providerPaymentID,
		&intentAmount, &intentCurrency, &intentState,
	)
	if err != nil || intentReservation != reservationID || intentTrainRun != trainRunID || intentOwner != ownerID ||
		intentAmount != physical.CapturedMinor || intentCurrency != physical.Currency || intentState != "completed" ||
		providerPaymentID == "" {
		if errors.Is(err, pgx.ErrNoRows) {
			return refund.OrderSnapshot{}, false, nil
		}
		return refund.OrderSnapshot{}, false, refund.ErrSnapshotConflict
	}
	tickets := make([]refund.TicketSnapshot, 0, len(physical.Tickets))
	for _, ticket := range physical.Tickets {
		tickets = append(tickets, refund.TicketSnapshot{
			ID: ticket.TicketID, State: refund.TicketState(ticket.State), FareMinor: ticket.FareMinor, Currency: ticket.Currency,
		})
	}
	return refund.OrderSnapshot{
		ID: orderID, OwnerID: ownerID, Version: uint64(generation), PaymentIntentID: physical.PaymentIntentID,
		ReservationID: reservationID, TrainRunID: trainRunID, ProviderPaymentID: providerPaymentID,
		DepartureAt: departureAt.UTC(), CapturedMinor: physical.CapturedMinor, RefundedMinor: physical.RefundedMinor,
		Currency: physical.Currency, Provider: provider, ShardID: shardID,
		PartialRefundSupported: store.providers[provider], Tickets: tickets,
	}, true, nil
}

func (store *Store) FindRequest(ctx context.Context, lookup refund.Lookup) (refund.RefundRequest, refund.Hash, bool, error) {
	request, found, err := loadRequest(ctx, store.db, `request.owner_user_id=$1 AND request.idempotency_key_hash=$2`, lookup.OwnerID, lookup.IdempotencyHash[:])
	if err != nil || !found {
		return refund.RefundRequest{}, refund.Hash{}, found, err
	}
	fingerprint, err := refund.SelectionFingerprint(request.OwnerID, request.OrderID, request.TicketIDs)
	if err != nil {
		return refund.RefundRequest{}, refund.Hash{}, false, refund.ErrSnapshotConflict
	}
	return request, fingerprint, true, nil
}

func (store *Store) GetRequest(ctx context.Context, ownerID, requestID uuid.UUID) (refund.RefundRequest, bool, error) {
	return loadRequest(ctx, store.db, `request.owner_user_id=$1 AND request.refund_request_id=$2`, ownerID, requestID)
}

func (store *Store) CreateRequest(ctx context.Context, command refund.CreateCommand) (refund.RefundRequest, bool, error) {
	if store == nil || store.db == nil || ctx == nil || !validCreateCommand(command) {
		return refund.RefundRequest{}, false, refund.ErrInvalidRequest
	}
	tx, err := store.beginControlTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return refund.RefundRequest{}, false, err
	}
	rollback := func(result error) (refund.RefundRequest, bool, error) {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return refund.RefundRequest{}, false, result
	}
	if existing, found, err := loadRequest(ctx, tx, `request.owner_user_id=$1 AND request.idempotency_key_hash=$2`, command.OwnerID, command.IdempotencyHash[:]); err != nil {
		return rollback(err)
	} else if found {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		selection, hashErr := refund.SelectionFingerprint(existing.OwnerID, existing.OrderID, existing.TicketIDs)
		if hashErr != nil || selection != command.SelectionFingerprint {
			return refund.RefundRequest{}, false, refund.ErrIdempotencyConflict
		}
		return existing, false, nil
	}
	var (
		reservationID, trainRunID, ownerID uuid.UUID
		shardID, state, currency           string
		generation, total                  int64
	)
	if err := tx.QueryRow(ctx, `
SELECT reservation_id,train_run_id,shard_id,assignment_generation,
       owner_user_id,status,total_amount_minor,currency
FROM public.ticket_order_shard_locators
WHERE ticket_order_id=$1 FOR UPDATE`, command.OrderID).Scan(
		&reservationID, &trainRunID, &shardID, &generation, &ownerID, &state, &total, &currency,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return rollback(refund.ErrNotFound)
		}
		return rollback(err)
	}
	request := command.Request
	if ownerID != command.OwnerID || state != "confirmed" || generation != int64(command.ExpectedVersion) ||
		reservationID != request.ReservationID || trainRunID != request.TrainRunID || shardID != request.ShardID ||
		total != request.CapturedMinor || currency != request.Currency {
		return rollback(refund.ErrSnapshotConflict)
	}
	var provider, providerPaymentID, intentCurrency, intentState string
	var intentReservation, intentTrainRun, intentOwner uuid.UUID
	var intentAmount int64
	var fullRefundExists bool
	if err := tx.QueryRow(ctx, `
SELECT reservation_id,train_run_id,owner_user_id,provider,provider_payment_id,
       amount_minor,currency,state,
       EXISTS(SELECT 1 FROM public.payment_operations AS operation
              WHERE operation.payment_intent_id=payment_intents.payment_intent_id
                AND operation.operation_type='refund')
FROM public.payment_intents WHERE payment_intent_id=$1 FOR SHARE`, request.PaymentIntentID).Scan(
		&intentReservation, &intentTrainRun, &intentOwner, &provider, &providerPaymentID,
		&intentAmount, &intentCurrency, &intentState, &fullRefundExists,
	); err != nil {
		return rollback(refund.ErrSnapshotConflict)
	}
	if intentReservation != request.ReservationID || intentTrainRun != request.TrainRunID || intentOwner != request.OwnerID ||
		provider != request.Provider || providerPaymentID != request.ProviderPaymentID ||
		intentAmount != request.CapturedMinor || intentCurrency != request.Currency || intentState != "completed" ||
		!store.providers[provider] || fullRefundExists {
		return rollback(refund.ErrSnapshotConflict)
	}
	rows, err := tx.Query(ctx, `
SELECT ticket_id FROM public.ticket_shard_locators
WHERE ticket_order_id=$1 AND owner_user_id=$2 AND train_run_id=$3
  AND shard_id=$4 AND assignment_generation=$5
  AND ticket_id=ANY($6::uuid[])
ORDER BY ticket_id FOR SHARE`, request.OrderID, request.OwnerID, request.TrainRunID,
		request.ShardID, generation, request.TicketIDs)
	if err != nil {
		return rollback(err)
	}
	index := 0
	for rows.Next() {
		var ticketID uuid.UUID
		if err := rows.Scan(&ticketID); err != nil || index >= len(request.TicketIDs) || ticketID != request.TicketIDs[index] {
			rows.Close()
			return rollback(refund.ErrSnapshotConflict)
		}
		index++
	}
	rows.Close()
	if rows.Err() != nil || index != len(request.TicketIDs) {
		return rollback(refund.ErrSnapshotConflict)
	}
	if err := insertRequest(ctx, tx, command); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			existing, selection, found, findErr := store.FindRequest(ctx, command.Lookup)
			if findErr != nil {
				return refund.RefundRequest{}, false, findErr
			}
			if found && selection == command.SelectionFingerprint {
				return existing, false, nil
			}
			return refund.RefundRequest{}, false, refund.ErrIdempotencyConflict
		}
		return refund.RefundRequest{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return refund.RefundRequest{}, false, err
	}
	return request, true, nil
}

func (store *Store) beginControlTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	if store == nil || store.db == nil || ctx == nil {
		return nil, refund.ErrInvalidService
	}
	tx, err := store.db.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	if store.deployment != nil {
		if err := authoritypostgres.AuthorizeControlTransaction(ctx, tx, *store.deployment); err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
			return nil, refund.ErrInvalidProcessorState
		}
	}
	return tx, nil
}

func insertRequest(ctx context.Context, tx pgx.Tx, command refund.CreateCommand) error {
	request := command.Request
	if _, err := tx.Exec(ctx, `
INSERT INTO public.ticket_refund_requests(
 refund_request_id,payment_intent_id,reservation_id,ticket_order_id,train_run_id,
 owner_user_id,provider,idempotency_key_hash,request_fingerprint,
 amount_minor,currency,eligibility_cutoff_at,state,created_at,updated_at
) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'created',$13,$13)`,
		request.ID, request.PaymentIntentID, request.ReservationID, request.OrderID, request.TrainRunID,
		request.OwnerID, request.Provider, command.IdempotencyHash[:], request.Fingerprint[:],
		request.AmountMinor, request.Currency, request.EligibilityCutoffAt.UTC(), request.CreatedAt.UTC()); err != nil {
		return err
	}
	for _, item := range request.Items {
		if _, err := tx.Exec(ctx, `INSERT INTO public.ticket_refund_request_items(
 refund_request_id,ticket_id,fare_amount_minor,currency,state,created_at,updated_at
) VALUES($1,$2,$3,$4,'selected',$5,$5)`, request.ID, item.TicketID, item.FareMinor, request.Currency, request.CreatedAt.UTC()); err != nil {
			return err
		}
	}
	operationID := uuid.NewSHA1(request.ID, []byte("ticket-refund-operation"))
	_, providerKeyHash := ProviderIdempotencyKey(operationID)
	if _, err := tx.Exec(ctx, `INSERT INTO public.ticket_refund_operations(
 refund_operation_id,refund_request_id,provider,provider_payment_id,
 provider_idempotency_key_hash,amount_minor,currency,state,
 captured_total_minor,refunded_total_minor,created_at,updated_at
) VALUES($1,$2,$3,$4,$5,$6,$7,'pending',$8,$9,$10,$10)`, operationID,
		request.ID, request.Provider, request.ProviderPaymentID, providerKeyHash[:],
		request.AmountMinor, request.Currency, request.CapturedMinor, request.RefundedBeforeMinor, request.CreatedAt.UTC()); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO public.ticket_refund_sagas(
 refund_saga_id,refund_request_id,current_step,state,next_attempt_at,created_at,updated_at
) VALUES($1,$2,'validate','created',$3,$3,$3)`,
		uuid.NewSHA1(request.ID, []byte("ticket-refund-saga")), request.ID, request.CreatedAt.UTC())
	return err
}

type queryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func loadRequest(ctx context.Context, db queryer, predicate string, arguments ...any) (refund.RefundRequest, bool, error) {
	if db == nil {
		return refund.RefundRequest{}, false, refund.ErrNotFound
	}
	query := `
SELECT request.refund_request_id,request.ticket_order_id,request.owner_user_id,
       request.payment_intent_id,request.reservation_id,request.train_run_id,
       intent.provider_payment_id,locator.assignment_generation,locator.shard_id,
       request.amount_minor,operation.captured_total_minor,operation.refunded_total_minor,
       request.currency,request.provider,request.request_fingerprint,request.state,
       request.created_at,request.updated_at,request.completed_at,request.eligibility_cutoff_at
FROM public.ticket_refund_requests AS request
JOIN public.ticket_refund_operations AS operation
  ON operation.refund_request_id=request.refund_request_id
JOIN public.payment_intents AS intent
  ON intent.payment_intent_id=request.payment_intent_id
JOIN public.ticket_order_shard_locators AS locator
  ON locator.ticket_order_id=request.ticket_order_id
WHERE ` + predicate
	var request refund.RefundRequest
	var fingerprint []byte
	var state string
	var completed pgtype.Timestamptz
	err := db.QueryRow(ctx, query, arguments...).Scan(
		&request.ID, &request.OrderID, &request.OwnerID, &request.PaymentIntentID,
		&request.ReservationID, &request.TrainRunID, &request.ProviderPaymentID,
		&request.AssignmentGeneration, &request.ShardID, &request.AmountMinor,
		&request.CapturedMinor, &request.RefundedBeforeMinor, &request.Currency,
		&request.Provider, &fingerprint, &state, &request.CreatedAt, &request.UpdatedAt, &completed, &request.EligibilityCutoffAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return refund.RefundRequest{}, false, nil
	}
	if err != nil || len(fingerprint) != sha256.Size {
		return refund.RefundRequest{}, false, err
	}
	copy(request.Fingerprint[:], fingerprint)
	request.State = refund.SagaState(state)
	if completed.Valid {
		value := completed.Time.UTC()
		request.CompletedAt = &value
	}
	rows, err := db.Query(ctx, `SELECT ticket_id,fare_amount_minor
FROM public.ticket_refund_request_items WHERE refund_request_id=$1 ORDER BY ticket_id`, request.ID)
	if err != nil {
		return refund.RefundRequest{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var item refund.RefundItem
		if err := rows.Scan(&item.TicketID, &item.FareMinor); err != nil {
			return refund.RefundRequest{}, false, err
		}
		request.Items = append(request.Items, item)
		request.TicketIDs = append(request.TicketIDs, item.TicketID)
	}
	if rows.Err() != nil || len(request.Items) == 0 {
		return refund.RefundRequest{}, false, rows.Err()
	}
	request.CreatedAt = request.CreatedAt.UTC()
	request.UpdatedAt = request.UpdatedAt.UTC()
	return request, true, nil
}

func validCreateCommand(command refund.CreateCommand) bool {
	request := command.Request
	if command.OwnerID == uuid.Nil || command.OrderID == uuid.Nil || command.ExpectedVersion == 0 || command.ExpectedVersion > math.MaxInt64 ||
		command.IdempotencyHash == (refund.Hash{}) || command.SelectionFingerprint == (refund.Hash{}) ||
		request.ID == uuid.Nil || request.OwnerID != command.OwnerID || request.OrderID != command.OrderID ||
		request.PaymentIntentID == uuid.Nil || request.ReservationID == uuid.Nil || request.TrainRunID == uuid.Nil ||
		request.AssignmentGeneration != command.ExpectedVersion || request.AmountMinor <= 0 || request.CapturedMinor <= 0 ||
		request.RefundedBeforeMinor < 0 || request.RefundedBeforeMinor > request.CapturedMinor ||
		len(request.TicketIDs) == 0 || len(request.TicketIDs) != len(request.Items) || request.CreatedAt.IsZero() ||
		request.EligibilityCutoffAt.IsZero() || !request.CreatedAt.Before(request.EligibilityCutoffAt) {
		return false
	}
	for index, item := range request.Items {
		if item.TicketID != request.TicketIDs[index] || item.FareMinor <= 0 {
			return false
		}
	}
	return true
}

var _ refund.Store = (*Store)(nil)
