// Package policylock owns the PostgreSQL advisory-lock namespace used to
// serialize durable hot-train policy mutations with Booking's final policy
// recheck. Keeping the key expression in one package prevents the two bounded
// contexts from silently drifting to different lock tuples.
package policylock

import (
	"context"
	"errors"
	"fmt"
	"strings"

	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

var ErrInvalidScope = errors.New("invalid hot-train policy lock scope")

const sharedAdvisoryLockSQL = `
SELECT pg_advisory_xact_lock_shared(
    hashtextextended(
        'admission:hot-train-policy:v1|' || $1::text || '|' || $2::text,
        0
    )
)`

const exclusiveAdvisoryLockSQL = `
SELECT pg_advisory_xact_lock(
    hashtextextended(
        'admission:hot-train-policy:v1|' || $1::text || '|' || $2::text,
        0
    )
)`

type transactionExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// AcquireBookingRead takes a shared transaction-scoped lock. Concurrent
// bookings for one hot train remain parallel, while a policy mutation cannot
// activate or advance that scope until every earlier booking has committed.
func AcquireBookingRead(
	ctx context.Context,
	tx transactionExecutor,
	trainRunID uuid.UUID,
	rawSeatClass string,
) error {
	return acquire(ctx, tx, trainRunID, rawSeatClass, sharedAdvisoryLockSQL)
}

// AcquirePolicyMutation takes the exclusive side of the same lock protocol.
func AcquirePolicyMutation(
	ctx context.Context,
	tx transactionExecutor,
	trainRunID uuid.UUID,
	rawSeatClass string,
) error {
	return acquire(ctx, tx, trainRunID, rawSeatClass, exclusiveAdvisoryLockSQL)
}

func acquire(
	ctx context.Context,
	tx transactionExecutor,
	trainRunID uuid.UUID,
	rawSeatClass string,
	query string,
) error {
	seatClass, err := offeringdomain.ParseSeatClass(strings.TrimSpace(rawSeatClass))
	if tx == nil || trainRunID == uuid.Nil || err != nil {
		return ErrInvalidScope
	}
	if _, err := tx.Exec(ctx, query, trainRunID, seatClass.String()); err != nil {
		return fmt.Errorf("lock hot-train policy scope: %w", err)
	}
	return nil
}
