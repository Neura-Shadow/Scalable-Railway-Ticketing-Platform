package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type ExpirationStore interface {
	ExpireDue(ctx context.Context, now time.Time, batchSize int) ([]uuid.UUID, error)
}

type ExpirationClock interface {
	Now() time.Time
}

type ReservationMetrics interface {
	RecordReservation(operation, result, reason string)
}

type HoldExpirer struct {
	store     ExpirationStore
	clock     ExpirationClock
	metrics   ReservationMetrics
	batchSize int
}

type ExpirationResult struct {
	Expired int
}

func NewHoldExpirer(store ExpirationStore, clock ExpirationClock, metrics ReservationMetrics, batchSize int) (*HoldExpirer, error) {
	if store == nil || clock == nil || batchSize <= 0 {
		return nil, errors.New("invalid hold expirer configuration")
	}
	return &HoldExpirer{store: store, clock: clock, metrics: metrics, batchSize: batchSize}, nil
}

// RunOnce delegates the reservation and inventory transition to one database
// transaction per claimed hold. Store implementations must use SKIP LOCKED and
// the shared reservation -> reservation_seat -> seat_inventory lock order.
func (w *HoldExpirer) RunOnce(ctx context.Context) (ExpirationResult, error) {
	ids, err := w.store.ExpireDue(ctx, w.clock.Now().UTC(), w.batchSize)
	if err != nil {
		if w.metrics != nil {
			w.metrics.RecordReservation("expire", "failure", "database")
		}
		return ExpirationResult{}, fmt.Errorf("expire reservation holds: %w", err)
	}
	if w.metrics != nil {
		for range ids {
			w.metrics.RecordReservation("expire", "success", "none")
		}
	}
	return ExpirationResult{Expired: len(ids)}, nil
}
