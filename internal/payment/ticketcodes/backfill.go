// Package ticketcodes owns the explicit M6 rollout gate for pre-existing
// ticket identities. It is intentionally operator-driven and read-only on
// booking shards.
package ticketcodes

import (
	"context"
	"errors"
	"unicode/utf8"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding"
	shardphysical "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/physical"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidBackfill = errors.New("invalid ticket code backfill")
	ErrBackfillBound   = errors.New("ticket code backfill exceeds bound")
	ErrTicketMissing   = errors.New("located ticket identity is missing")
	ErrCodeCollision   = errors.New("global ticket code collision")
)

type Control interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type Router interface {
	Resolve(context.Context, uuid.UUID, bool) (shardphysical.Resolution, error)
}

type Backfiller struct {
	control Control
	router  Router
}

type Result struct {
	Missing int
	Claimed int
	Total   int64
	Ready   bool
}

type locator struct {
	ticketID   uuid.UUID
	trainRunID uuid.UUID
	shardID    string
	generation int64
}

func NewBackfiller(control Control, router Router) (*Backfiller, error) {
	if control == nil || router == nil {
		return nil, ErrInvalidBackfill
	}
	return &Backfiller{control: control, router: router}, nil
}

func (backfill *Backfiller) Inspect(ctx context.Context, limit int) (Result, error) {
	return backfill.run(ctx, limit, false)
}

func (backfill *Backfiller) Backfill(ctx context.Context, limit int) (Result, error) {
	return backfill.run(ctx, limit, true)
}

func (backfill *Backfiller) run(ctx context.Context, limit int, mutate bool) (Result, error) {
	if backfill == nil || ctx == nil || limit < 1 || limit > 10000 {
		return Result{}, ErrInvalidBackfill
	}
	tx, err := backfill.control.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SELECT public.lock_ticket_code_claims()`); err != nil {
		return Result{}, err
	}
	rows, err := tx.Query(ctx, `SELECT locator.ticket_id,locator.train_run_id,
       locator.shard_id,locator.assignment_generation
FROM public.ticket_shard_locators AS locator
LEFT JOIN public.ticket_code_directory AS directory ON directory.ticket_id=locator.ticket_id
WHERE directory.ticket_id IS NULL
ORDER BY locator.ticket_id
LIMIT $1`, limit+1)
	if err != nil {
		return Result{}, err
	}
	missing := make([]locator, 0, limit)
	for rows.Next() {
		var item locator
		if err := rows.Scan(&item.ticketID, &item.trainRunID, &item.shardID, &item.generation); err != nil {
			rows.Close()
			return Result{}, err
		}
		missing = append(missing, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return Result{}, err
	}
	rows.Close()
	if len(missing) > limit {
		return Result{Missing: len(missing), Ready: false}, ErrBackfillBound
	}
	result := Result{Missing: len(missing)}
	if !mutate {
		return result, nil
	}
	for _, item := range missing {
		code, err := backfill.loadCode(ctx, tx, item)
		if err != nil {
			return Result{}, err
		}
		tag, err := tx.Exec(ctx, `INSERT INTO public.ticket_code_directory(ticket_code,ticket_id)
VALUES($1,$2)
ON CONFLICT(ticket_code) DO UPDATE SET ticket_id=EXCLUDED.ticket_id
WHERE ticket_code_directory.ticket_id=EXCLUDED.ticket_id`, code, item.ticketID)
		if err != nil || tag.RowsAffected() != 1 {
			return Result{}, ErrCodeCollision
		}
		result.Claimed++
	}
	var remaining int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM public.ticket_shard_locators AS locator
LEFT JOIN public.ticket_code_directory AS directory ON directory.ticket_id=locator.ticket_id
WHERE directory.ticket_id IS NULL`).Scan(&remaining); err != nil {
		return Result{}, err
	}
	if remaining != 0 {
		return Result{}, ErrBackfillBound
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM public.ticket_shard_locators`).Scan(&result.Total); err != nil {
		return Result{}, err
	}
	tag, err := tx.Exec(ctx, `UPDATE public.ticket_code_claim_readiness
SET state='ready',claimed_ticket_count=$1,verified_at=clock_timestamp()
WHERE singleton`, result.Total)
	if err != nil || tag.RowsAffected() != 1 {
		return Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, err
	}
	result.Ready = true
	return result, nil
}

func (backfill *Backfiller) loadCode(ctx context.Context, control pgx.Tx, item locator) (string, error) {
	if item.ticketID == uuid.Nil || item.trainRunID == uuid.Nil || item.generation < 1 {
		return "", ErrTicketMissing
	}
	var row pgx.Row
	switch item.shardID {
	case "legacy":
		row = control.QueryRow(ctx, `SELECT ticket_code FROM public.tickets WHERE id=$1`, item.ticketID)
	case "shard-0":
		row = control.QueryRow(ctx, `SELECT ticket_code FROM booking_shard_0.tickets WHERE id=$1`, item.ticketID)
	case "shard-1":
		row = control.QueryRow(ctx, `SELECT ticket_code FROM booking_shard_1.tickets WHERE id=$1`, item.ticketID)
	case "physical-shard-0", "physical-shard-1":
		wanted, err := sharding.ParseShardID(item.shardID)
		if err != nil {
			return "", ErrTicketMissing
		}
		resolved, err := backfill.router.Resolve(ctx, item.trainRunID, true)
		if err != nil || resolved.Route.ShardID() != wanted ||
			resolved.Route.Generation().Int64() != item.generation || resolved.Handle.ShardID() != wanted {
			return "", ErrTicketMissing
		}
		physicalTx, err := resolved.Handle.Pool().BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			return "", err
		}
		defer func() { _ = physicalTx.Rollback(context.WithoutCancel(ctx)) }()
		var code string
		if err := physicalTx.QueryRow(ctx, `SELECT ticket_code FROM public.tickets
WHERE id=$1 AND train_run_id=$2 AND assignment_generation=$3`, item.ticketID, item.trainRunID, item.generation).Scan(&code); err != nil {
			return "", ErrTicketMissing
		}
		if err := physicalTx.Commit(ctx); err != nil {
			return "", err
		}
		if !validExistingTicketCode(code) {
			return "", ErrTicketMissing
		}
		return code, nil
	default:
		return "", ErrTicketMissing
	}
	var code string
	if err := row.Scan(&code); err != nil || !validExistingTicketCode(code) {
		return "", ErrTicketMissing
	}
	return code, nil
}

func validExistingTicketCode(code string) bool {
	if !utf8.ValidString(code) {
		return false
	}
	length := utf8.RuneCountInString(code)
	return length >= 16 && length <= 64
}
