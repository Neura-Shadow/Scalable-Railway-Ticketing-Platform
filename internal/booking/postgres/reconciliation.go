package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// ReconcileTrainRun verifies the authoritative invariant for every inventory
// row: its occupied mask is the non-overlapping union of held/confirmed
// reservation-seat snapshots. It is an audit gate, not a repair operation.
func (s *Store) ReconcileTrainRun(ctx context.Context, trainRunID uuid.UUID) error {
	if s == nil || s.pool == nil || trainRunID == uuid.Nil {
		return ErrInvalidArgument
	}
	var inventoryViolations, orphanedActiveSeats int
	err := s.pool.QueryRow(ctx, `
SELECT count(*)::integer,
       (
           SELECT count(*)::integer
           FROM reservation_seats AS orphan_rs
           JOIN reservations AS orphan_r ON orphan_r.id = orphan_rs.reservation_id
           LEFT JOIN seat_inventory AS orphan_si
             ON orphan_si.train_run_id = orphan_r.train_run_id
            AND orphan_si.seat_id = orphan_rs.seat_id
           WHERE orphan_r.train_run_id = $1
             AND orphan_r.status IN ('held', 'confirmed')
             AND orphan_si.seat_id IS NULL
       )
FROM seat_inventory AS si
LEFT JOIN LATERAL (
    SELECT count(*)::integer AS total_masks,
           count(*) FILTER (
               WHERE bit_length(rs.segment_mask) = bit_length(si.occupied_segments)
           )::integer AS matching_masks,
           bit_or(rs.segment_mask) FILTER (
               WHERE bit_length(rs.segment_mask) = bit_length(si.occupied_segments)
           ) AS expected_mask,
           sum(bit_count(rs.segment_mask)) FILTER (
               WHERE bit_length(rs.segment_mask) = bit_length(si.occupied_segments)
           ) AS individual_bit_count
    FROM reservation_seats AS rs
    JOIN reservations AS r ON r.id = rs.reservation_id
    WHERE r.train_run_id = si.train_run_id
      AND rs.seat_id = si.seat_id
      AND r.status IN ('held', 'confirmed')
) AS active ON true
WHERE si.train_run_id = $1
  AND NOT CASE
        WHEN active.total_masks = 0
        THEN bit_count(si.occupied_segments) = 0
        WHEN active.total_masks = active.matching_masks
        THEN si.occupied_segments = active.expected_mask
         AND active.individual_bit_count = bit_count(active.expected_mask)
        ELSE false
      END`, trainRunID).Scan(&inventoryViolations, &orphanedActiveSeats)
	if err != nil {
		return fmt.Errorf("reconcile train-run inventory: %w", err)
	}
	if inventoryViolations != 0 || orphanedActiveSeats != 0 {
		return fmt.Errorf("%w: train run %s has %d inconsistent inventory rows and %d orphaned active reservation seats", ErrPersistenceInvariant, trainRunID, inventoryViolations, orphanedActiveSeats)
	}
	return nil
}
