package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"strings"

	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/sharding/control"
	"github.com/google/uuid"
)

func (tx *Transaction) Validate(ctx context.Context, request control.ValidationRequest) (control.ValidationSnapshot, error) {
	if tx == nil || tx.tx == nil || request.MigrationID == uuid.Nil || request.TrainRunID == uuid.Nil ||
		request.RowCap <= 0 || request.Source.TrainRunID() != request.TrainRunID ||
		request.Target.TrainRunID() != request.TrainRunID || request.Source.ShardID() == request.Target.ShardID() ||
		request.Target.Generation().Int64() <= request.Source.Generation().Int64() {
		return control.ValidationSnapshot{}, control.ErrInvalidInput
	}
	sourceSchema, err := schemaForShard(request.Source.ShardID())
	if err != nil {
		return control.ValidationSnapshot{}, control.ErrInvalidInput
	}
	targetSchema, err := schemaForShard(request.Target.ShardID())
	if err != nil {
		return control.ValidationSnapshot{}, control.ErrInvalidInput
	}

	snapshot := control.ValidationSnapshot{}
	snapshot.Source, snapshot.Truncated, err = tx.digestDataset(ctx, sourceSchema, request.TrainRunID, request.RowCap, &snapshot.RowsExamined)
	if err != nil || snapshot.Truncated {
		return snapshot, err
	}
	snapshot.Target, snapshot.Truncated, err = tx.digestDataset(ctx, targetSchema, request.TrainRunID, request.RowCap, &snapshot.RowsExamined)
	if err != nil || snapshot.Truncated {
		return snapshot, err
	}

	err = tx.tx.QueryRow(ctx, validationInvariantSQL(targetSchema),
		request.TrainRunID,
		request.Source.ShardID().String(),
		request.Source.Generation().Int64(),
	).Scan(
		&snapshot.InvariantViolations,
		&snapshot.MissingReservationLocators,
		&snapshot.MissingTicketOrderLocators,
		&snapshot.MissingTicketLocators,
	)
	if err != nil {
		return control.ValidationSnapshot{}, ErrPersistence
	}
	if snapshot.InvariantViolations < 0 || snapshot.MissingReservationLocators < 0 ||
		snapshot.MissingTicketOrderLocators < 0 || snapshot.MissingTicketLocators < 0 {
		return control.ValidationSnapshot{}, ErrPersistence
	}
	return snapshot, nil
}

func (tx *Transaction) digestDataset(
	ctx context.Context,
	schema string,
	trainRunID uuid.UUID,
	rowCap int64,
	rowsExamined *int64,
) (control.DatasetDigest, bool, error) {
	digest := control.DatasetDigest{Tables: make([]control.TableDigest, 0, len(copySpecs))}
	for _, spec := range copySpecs {
		query := canonicalRowsSQL(schema, spec)
		rows, err := tx.tx.Query(ctx, query, trainRunID)
		if err != nil {
			return control.DatasetDigest{}, false, ErrPersistence
		}
		hasher := sha256.New()
		var count int64
		truncated := false
		for rows.Next() {
			if *rowsExamined >= rowCap {
				truncated = true
				break
			}
			var canonical string
			if err := rows.Scan(&canonical); err != nil {
				rows.Close()
				return control.DatasetDigest{}, false, ErrPersistence
			}
			writeFramedHash(hasher, canonical)
			count++
			*rowsExamined++
		}
		rowsErr := rows.Err()
		rows.Close()
		if rowsErr != nil {
			return control.DatasetDigest{}, false, ErrPersistence
		}
		digest.Tables = append(digest.Tables, control.TableDigest{
			Name: spec.table, Rows: count, Checksum: hex.EncodeToString(hasher.Sum(nil)),
		})
		if truncated {
			return digest, true, nil
		}
	}
	return digest, false, nil
}

func writeFramedHash(hasher hash.Hash, canonical string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(canonical)))
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write([]byte(canonical))
}

func canonicalRowsSQL(schema string, spec copySpec) string {
	selected := make([]string, 0, len(spec.columns))
	for _, column := range spec.columns {
		selected = append(selected, "s."+column)
	}
	joins := ""
	if spec.scopeJoins != "" {
		count := strings.Count(spec.scopeJoins, "%s")
		values := make([]any, count)
		for index := range values {
			values[index] = schema
		}
		joins = fmt.Sprintf(spec.scopeJoins, values...)
	}
	return fmt.Sprintf(`
SELECT row_to_json(scoped)::text
FROM (
    SELECT %s
    FROM %s.%s AS s
    %s
    WHERE %s
    ORDER BY s.%s
) AS scoped`, strings.Join(selected, ", "), schema, spec.table, joins, spec.scopeClause, spec.key)
}

func validationInvariantSQL(targetSchema string) string {
	return fmt.Sprintf(`
WITH reservation_seat_violations AS (
    SELECT rs.id
    FROM %[1]s.reservation_seats AS rs
    LEFT JOIN %[1]s.reservations AS r
      ON r.id = rs.reservation_id
     AND r.train_run_id = rs.train_run_id
     AND r.segment_count = rs.segment_count
    LEFT JOIN %[1]s.seat_inventory AS inventory
      ON inventory.train_run_id = rs.train_run_id
     AND inventory.seat_id = rs.seat_id
    LEFT JOIN public.passengers AS passenger
      ON passenger.id = rs.passenger_id
    WHERE rs.train_run_id = $1
      AND (
          r.id IS NULL OR inventory.seat_id IS NULL OR passenger.id IS NULL
          OR passenger.user_id <> r.user_id
          OR inventory.seat_class <> r.seat_class
          OR rs.currency <> r.currency
          OR rs.segment_mask <> repeat('0', r.from_stop_index)::bit varying
             || repeat('1', r.to_stop_index-r.from_stop_index)::bit varying
             || repeat('0', r.segment_count-r.to_stop_index)::bit varying
      )
), ticket_violations AS (
    SELECT ticket.id
    FROM %[1]s.tickets AS ticket
    LEFT JOIN %[1]s.ticket_orders AS ticket_order
      ON ticket_order.id = ticket.ticket_order_id
    LEFT JOIN %[1]s.reservations AS reservation
      ON reservation.id = ticket_order.reservation_id
    LEFT JOIN %[1]s.reservation_seats AS reservation_seat
      ON reservation_seat.id = ticket.reservation_seat_id
     AND reservation_seat.reservation_id = reservation.id
    WHERE reservation.train_run_id = $1
      AND (ticket_order.id IS NULL OR reservation_seat.id IS NULL
           OR ticket_order.user_id <> reservation.user_id
           OR ticket_order.currency <> reservation.currency)
), claim_violations AS (
    SELECT local.id
    FROM %[1]s.idempotency_records AS local
    LEFT JOIN public.booking_idempotency_key_claims AS claim
      ON claim.user_id = local.user_id
     AND claim.operation = local.operation
     AND claim.key_hash = local.key_hash
     AND claim.request_fingerprint = local.request_fingerprint
     AND claim.train_run_id = local.train_run_id
     AND claim.expires_at = local.expires_at
    WHERE local.train_run_id = $1 AND claim.id IS NULL
), quota_violations AS (
    SELECT reservation.id
    FROM %[1]s.reservations AS reservation
    LEFT JOIN public.reservation_quota_claims AS quota
      ON quota.reservation_id = reservation.id
     AND quota.user_id = reservation.user_id
     AND quota.train_run_id = reservation.train_run_id
    WHERE reservation.train_run_id = $1
      AND (quota.reservation_id IS NULL
       OR quota.passenger_count <> (
          SELECT count(*)::integer
          FROM %[1]s.reservation_seats AS seat
          WHERE seat.reservation_id = reservation.id
      )
       OR quota.active <> (reservation.status = 'held'))
    UNION ALL
    SELECT quota.reservation_id
    FROM public.reservation_quota_claims AS quota
    LEFT JOIN %[1]s.reservations AS reservation
      ON reservation.id = quota.reservation_id
     AND reservation.user_id = quota.user_id
     AND reservation.train_run_id = quota.train_run_id
    WHERE quota.train_run_id = $1
      AND reservation.id IS NULL
), reservation_locator_violations AS (
    SELECT reservation.id
    FROM %[1]s.reservations AS reservation
    JOIN public.reservation_shard_locators AS locator
      ON locator.reservation_id = reservation.id
    WHERE reservation.train_run_id = $1
      AND (locator.train_run_id <> reservation.train_run_id
       OR locator.shard_id <> $2
       OR locator.assignment_generation <> $3
       OR locator.owner_user_id <> reservation.user_id)
), order_locator_violations AS (
    SELECT ticket_order.id
    FROM %[1]s.ticket_orders AS ticket_order
    JOIN %[1]s.reservations AS reservation
      ON reservation.id = ticket_order.reservation_id
    JOIN public.ticket_order_shard_locators AS locator
      ON locator.ticket_order_id = ticket_order.id
    WHERE reservation.train_run_id = $1
      AND (locator.reservation_id <> ticket_order.reservation_id
       OR locator.train_run_id <> reservation.train_run_id
       OR locator.shard_id <> $2
       OR locator.assignment_generation <> $3
       OR locator.owner_user_id <> ticket_order.user_id
       OR locator.status <> ticket_order.status
       OR locator.total_amount_minor <> ticket_order.total_amount_minor
       OR locator.currency <> ticket_order.currency
       OR locator.created_at <> ticket_order.created_at)
), ticket_locator_violations AS (
    SELECT ticket.id
    FROM %[1]s.tickets AS ticket
    JOIN %[1]s.ticket_orders AS ticket_order
      ON ticket_order.id = ticket.ticket_order_id
    JOIN %[1]s.reservations AS reservation
      ON reservation.id = ticket_order.reservation_id
    JOIN public.ticket_shard_locators AS locator
      ON locator.ticket_id = ticket.id
    WHERE reservation.train_run_id = $1
      AND (locator.ticket_order_id <> ticket.ticket_order_id
       OR locator.reservation_id <> ticket_order.reservation_id
       OR locator.train_run_id <> reservation.train_run_id
       OR locator.shard_id <> $2
       OR locator.assignment_generation <> $3
       OR locator.owner_user_id <> ticket_order.user_id)
), outbox_violations AS (
    SELECT event.id
    FROM public.outbox_events AS event
    WHERE event.train_run_id = $1
      AND event.aggregate_type IN ('reservation', 'ticket')
      AND (
          event.shard_id NOT IN ('legacy', 'shard-0', 'shard-1')
          OR event.assignment_generation IS NULL
          OR event.assignment_generation <= 0
          OR (event.aggregate_type = 'reservation' AND (
              event.event_type NOT IN (
                  'reservation.held', 'reservation.confirmed',
                  'reservation.expired', 'reservation.cancelled'
              )
              OR NOT EXISTS (
                  SELECT 1
                  FROM %[1]s.reservations AS reservation
                  WHERE reservation.train_run_id = $1
                    AND reservation.id = event.aggregate_id
              )
          ))
          OR (event.aggregate_type = 'ticket' AND (
              event.event_type <> 'ticket.created'
              OR NOT EXISTS (
                  SELECT 1
                  FROM %[1]s.tickets AS ticket
                  JOIN %[1]s.ticket_orders AS ticket_order
                    ON ticket_order.id = ticket.ticket_order_id
                  JOIN %[1]s.reservations AS reservation
                    ON reservation.id = ticket_order.reservation_id
                  WHERE reservation.train_run_id = $1
                    AND ticket.id = event.aggregate_id
              )
          ))
      )
), missing_outbox_intent AS (
    SELECT reservation.id
    FROM %[1]s.reservations AS reservation
    WHERE reservation.train_run_id = $1
      AND (
          NOT EXISTS (
              SELECT 1
              FROM public.outbox_events AS event
              WHERE event.train_run_id = reservation.train_run_id
                AND event.aggregate_type = 'reservation'
                AND event.aggregate_id = reservation.id
                AND event.event_type = 'reservation.held'
          )
          OR (reservation.status <> 'held' AND NOT EXISTS (
              SELECT 1
              FROM public.outbox_events AS event
              WHERE event.train_run_id = reservation.train_run_id
                AND event.aggregate_type = 'reservation'
                AND event.aggregate_id = reservation.id
                AND event.event_type = 'reservation.' || reservation.status
          ))
          OR (EXISTS (
              SELECT 1
              FROM %[1]s.ticket_orders AS ticket_order
              WHERE ticket_order.reservation_id = reservation.id
          ) AND NOT EXISTS (
              SELECT 1
              FROM public.outbox_events AS event
              WHERE event.train_run_id = reservation.train_run_id
                AND event.aggregate_type = 'reservation'
                AND event.aggregate_id = reservation.id
                AND event.event_type = 'reservation.confirmed'
          ))
      )
    UNION ALL
    SELECT ticket.id
    FROM %[1]s.tickets AS ticket
    JOIN %[1]s.ticket_orders AS ticket_order
      ON ticket_order.id = ticket.ticket_order_id
    JOIN %[1]s.reservations AS reservation
      ON reservation.id = ticket_order.reservation_id
    WHERE reservation.train_run_id = $1
      AND NOT EXISTS (
          SELECT 1
          FROM public.outbox_events AS event
          WHERE event.train_run_id = reservation.train_run_id
            AND event.aggregate_type = 'ticket'
            AND event.aggregate_id = ticket.id
            AND event.event_type = 'ticket.created'
      )
), missing_reservation_locators AS (
    SELECT reservation.id
    FROM %[1]s.reservations AS reservation
    LEFT JOIN public.reservation_shard_locators AS locator
      ON locator.reservation_id = reservation.id
    WHERE reservation.train_run_id = $1 AND locator.reservation_id IS NULL
), missing_order_locators AS (
    SELECT ticket_order.id
    FROM %[1]s.ticket_orders AS ticket_order
    JOIN %[1]s.reservations AS reservation ON reservation.id = ticket_order.reservation_id
    LEFT JOIN public.ticket_order_shard_locators AS locator
      ON locator.ticket_order_id = ticket_order.id
    WHERE reservation.train_run_id = $1 AND locator.ticket_order_id IS NULL
), missing_ticket_locators AS (
    SELECT ticket.id
    FROM %[1]s.tickets AS ticket
    JOIN %[1]s.ticket_orders AS ticket_order ON ticket_order.id = ticket.ticket_order_id
    JOIN %[1]s.reservations AS reservation ON reservation.id = ticket_order.reservation_id
    LEFT JOIN public.ticket_shard_locators AS locator
      ON locator.ticket_id = ticket.id
    WHERE reservation.train_run_id = $1 AND locator.ticket_id IS NULL
)
SELECT (
           (SELECT count(*) FROM reservation_seat_violations)
         + (SELECT count(*) FROM ticket_violations)
         + (SELECT count(*) FROM claim_violations)
         + (SELECT count(*) FROM quota_violations)
         + (SELECT count(*) FROM reservation_locator_violations)
         + (SELECT count(*) FROM order_locator_violations)
         + (SELECT count(*) FROM ticket_locator_violations)
         + (SELECT count(*) FROM outbox_violations)
         + (SELECT count(*) FROM missing_outbox_intent)
       )::bigint,
       (SELECT count(*)::bigint FROM missing_reservation_locators),
       (SELECT count(*)::bigint FROM missing_order_locators),
       (SELECT count(*)::bigint FROM missing_ticket_locators)`, targetSchema)
}
