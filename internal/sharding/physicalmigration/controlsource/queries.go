package controlsource

func sourceQuery(table string) (string, bool) {
	switch table {
	case "train_run_booking_snapshots":
		return sourceSnapshotSQL, true
	case "booking_seat_catalog":
		return sourceSeatCatalogSQL, true
	case "booking_fare_snapshots":
		return sourceFareSnapshotSQL, true
	case "seat_inventory":
		return sourceInventorySQL, true
	case "reservations":
		return sourceReservationSQL, true
	case "reservation_seats":
		return sourceReservationSeatSQL, true
	case "ticket_orders":
		return sourceTicketOrderSQL, true
	case "tickets":
		return sourceTicketSQL, true
	case "idempotency_records":
		return sourceIdempotencySQL, true
	case "outbox_events":
		return sourceOutboxSQL, true
	default:
		return "", false
	}
}

const sourceSnapshotSQL = `
WITH transformed AS (
    SELECT public.physical_source_entity_id(run.id,'snapshot',run.id) AS id,
           run.id AS train_run_id,
           assignment.assignment_generation,
           run.train_id,run.route_id,run.service_date,run.segment_count,
           1::bigint AS route_version,
           1::bigint AS booking_policy_version,
           GREATEST(1::bigint,(extract(epoch FROM run.updated_at)*1000000)::bigint) AS source_version,
           run.status,
           (run.status='scheduled') AS bookable,
           true AS active,
           run.updated_at AS source_updated_at,
           run.created_at,run.updated_at
    FROM public.train_runs AS run
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=run.id
    WHERE run.id=$1 AND assignment.shard_id=$2
      AND assignment.assignment_generation=$3
)
SELECT id,to_jsonb(transformed)
FROM transformed
WHERE ($5::uuid IS NULL AND id>$4) OR id=$5
ORDER BY id LIMIT $6`

const sourceSeatCatalogSQL = `
WITH ordered AS (
    SELECT seat.*,coach.train_id,coach.seat_class,
           row_number() OVER (ORDER BY coach.coach_number,coach.id)-1 AS coach_order,
           row_number() OVER (PARTITION BY coach.id ORDER BY seat.seat_number,seat.id)-1 AS seat_order
    FROM public.seats AS seat
    JOIN public.coaches AS coach ON coach.id=seat.coach_id
), transformed AS (
    SELECT public.physical_source_entity_id(run.id,'seat',seat.id) AS id,
           run.id AS train_run_id,assignment.assignment_generation,
           run.train_id,seat.coach_id,seat.id AS seat_id,
           seat.coach_order::integer,seat.seat_order::integer,seat.seat_class,
           seat.active,
           GREATEST(1::bigint,(extract(epoch FROM seat.updated_at)*1000000)::bigint) AS source_version,
           seat.updated_at AS source_updated_at,seat.created_at,seat.updated_at
    FROM public.train_runs AS run
    JOIN public.train_run_shard_assignments AS assignment ON assignment.train_run_id=run.id
    JOIN ordered AS seat ON seat.train_id=run.train_id
    WHERE run.id=$1 AND assignment.shard_id=$2
      AND assignment.assignment_generation=$3
)
SELECT id,to_jsonb(transformed)
FROM transformed
WHERE ($5::uuid IS NULL AND id>$4) OR id=$5
ORDER BY id LIMIT $6`

const sourceFareSnapshotSQL = `
WITH applicable_fares AS (
    SELECT fare.id,run.id AS train_run_id,assignment.assignment_generation,
           run.segment_count,fare.from_stop_index,fare.to_stop_index,
           fare.seat_class,fare.amount_minor,fare.currency,fare.active,
           fare.source_version,
           fare.created_at,fare.updated_at
    FROM public.train_runs AS run
    JOIN public.train_run_shard_assignments AS assignment ON assignment.train_run_id=run.id
    JOIN public.fares AS fare
      ON fare.active
     AND (fare.train_run_id=run.id OR (fare.train_run_id IS NULL AND fare.route_id=run.route_id))
    WHERE run.id=$1 AND assignment.shard_id=$2
      AND assignment.assignment_generation=$3
      AND (fare.train_run_id IS NOT NULL OR NOT EXISTS (
          SELECT 1 FROM public.fares AS direct
          WHERE direct.active AND direct.train_run_id=run.id
            AND direct.from_stop_index=fare.from_stop_index
            AND direct.to_stop_index=fare.to_stop_index
            AND direct.seat_class=fare.seat_class
      ))
), transformed AS (
    SELECT public.physical_source_entity_id(fare.train_run_id,'fare',fare.id) AS id,
           fare.train_run_id,fare.assignment_generation,fare.segment_count,
           fare.from_stop_index,fare.to_stop_index,fare.seat_class,
           fare.amount_minor,fare.currency,
           fare.source_version,
           true AS active,fare.updated_at AS source_updated_at,
           fare.created_at,fare.updated_at
    FROM applicable_fares AS fare
    UNION ALL
    SELECT public.physical_source_entity_id(reservation.train_run_id,'reservation-fare',reservation.id) AS id,
           reservation.train_run_id,assignment.assignment_generation,
           reservation.segment_count,reservation.from_stop_index,
           reservation.to_stop_index,reservation.seat_class,
           CASE WHEN seat_count.count_value=0 THEN 0
                ELSE reservation.total_amount_minor/seat_count.count_value END,
           reservation.currency,1::bigint AS source_version,false AS active,
           reservation.updated_at AS source_updated_at,
           reservation.created_at,reservation.updated_at
    FROM public.physical_source_reservation_rows AS reservation
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=reservation.train_run_id
    CROSS JOIN LATERAL (
        SELECT count(*)::bigint AS count_value
        FROM public.physical_source_reservation_seat_rows AS seat
        WHERE seat.source_shard_id=reservation.source_shard_id
          AND seat.reservation_id=reservation.id
    ) AS seat_count
    WHERE reservation.train_run_id=$1 AND reservation.source_shard_id=$2
      AND assignment.assignment_generation=$3 AND seat_count.count_value>0
)
SELECT id,to_jsonb(transformed)
FROM transformed
WHERE ($5::uuid IS NULL AND id>$4) OR id=$5
ORDER BY id LIMIT $6`

const sourceInventorySQL = `
WITH transformed AS (
    SELECT public.physical_source_entity_id(inventory.train_run_id,'inventory',inventory.seat_id) AS id,
           inventory.train_run_id,assignment.assignment_generation,
           inventory.segment_count,inventory.seat_id,inventory.seat_class,
           inventory.occupied_segments,inventory.version,
           inventory.created_at,inventory.updated_at
    FROM public.physical_source_seat_inventory_rows AS inventory
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=inventory.train_run_id
    WHERE inventory.train_run_id=$1 AND inventory.source_shard_id=$2
      AND assignment.assignment_generation=$3
)
SELECT id,to_jsonb(transformed)
FROM transformed
WHERE ($5::uuid IS NULL AND id>$4) OR id=$5
ORDER BY id LIMIT $6`

const sourceReservationSQL = `
WITH transformed AS (
    SELECT reservation.id,reservation.user_id,reservation.train_run_id,
           assignment.assignment_generation,reservation.segment_count,
           reservation.from_stop_index,reservation.to_stop_index,
           reservation.seat_class,reservation.status,reservation.expires_at,
           reservation.total_amount_minor,reservation.currency,
           reservation.created_at,reservation.updated_at
    FROM public.physical_source_reservation_rows AS reservation
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=reservation.train_run_id
    WHERE reservation.train_run_id=$1 AND reservation.source_shard_id=$2
      AND assignment.assignment_generation=$3
)
SELECT id,to_jsonb(transformed)
FROM transformed
WHERE ($5::uuid IS NULL AND id>$4) OR id=$5
ORDER BY id LIMIT $6`

const sourceReservationSeatSQL = `
WITH transformed AS (
    SELECT seat.id,seat.reservation_id,seat.train_run_id,
           assignment.assignment_generation,seat.segment_count,seat.seat_id,
           seat.passenger_id,
           public.physical_source_entity_id(seat.train_run_id,'reservation-fare',seat.reservation_id) AS fare_snapshot_id,
           seat.segment_mask,seat.fare_amount_minor,seat.currency,
           seat.created_at,seat.created_at AS updated_at
    FROM public.physical_source_reservation_seat_rows AS seat
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=seat.train_run_id
    WHERE seat.train_run_id=$1 AND seat.source_shard_id=$2
      AND assignment.assignment_generation=$3
)
SELECT id,to_jsonb(transformed)
FROM transformed
WHERE ($5::uuid IS NULL AND id>$4) OR id=$5
ORDER BY id LIMIT $6`

const sourceTicketOrderSQL = `
WITH transformed AS (
    SELECT orders.id,orders.reservation_id,orders.user_id,orders.train_run_id,
           assignment.assignment_generation,orders.status,
           orders.total_amount_minor,orders.currency,
           orders.created_at,orders.updated_at
    FROM public.physical_source_ticket_order_rows AS orders
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=orders.train_run_id
    WHERE orders.train_run_id=$1 AND orders.source_shard_id=$2
      AND assignment.assignment_generation=$3
)
SELECT id,to_jsonb(transformed)
FROM transformed
WHERE ($5::uuid IS NULL AND id>$4) OR id=$5
ORDER BY id LIMIT $6`

const sourceTicketSQL = `
WITH transformed AS (
    SELECT ticket.id,ticket.ticket_order_id,ticket.reservation_seat_id,
           ticket.train_run_id,assignment.assignment_generation,
           ticket.ticket_code,ticket.status,ticket.created_at,ticket.updated_at
    FROM public.physical_source_ticket_rows AS ticket
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=ticket.train_run_id
    WHERE ticket.train_run_id=$1 AND ticket.source_shard_id=$2
      AND assignment.assignment_generation=$3
)
SELECT id,to_jsonb(transformed)
FROM transformed
WHERE ($5::uuid IS NULL AND id>$4) OR id=$5
ORDER BY id LIMIT $6`

const sourceIdempotencySQL = `
WITH transformed AS (
    SELECT record.id,record.train_run_id,assignment.assignment_generation,
           record.user_id,record.operation,record.key_hash,
           record.request_fingerprint,record.status,record.resource_type,
           record.resource_id,record.expires_at,record.created_at,record.updated_at
    FROM public.physical_source_idempotency_rows AS record
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=record.train_run_id
    WHERE record.train_run_id=$1 AND record.source_shard_id=$2
      AND assignment.assignment_generation=$3
)
SELECT id,to_jsonb(transformed)
FROM transformed
WHERE ($5::uuid IS NULL AND id>$4) OR id=$5
ORDER BY id LIMIT $6`

const sourceOutboxSQL = `
WITH transformed AS (
    SELECT event.id,event.train_run_id,assignment.assignment_generation,
           event.aggregate_type,event.aggregate_id,event.event_type,
           event.event_version,event.payload,
           CASE WHEN event.status='processing' THEN 'pending' ELSE event.status END AS status,
           event.attempts,event.next_attempt_at,
           NULL::timestamptz AS locked_at,NULL::text AS locked_by,
           NULL::uuid AS lease_token,event.created_at,event.created_at AS updated_at,
           event.published_at
    FROM public.physical_source_outbox_rows AS event
    JOIN public.train_run_shard_assignments AS assignment
      ON assignment.train_run_id=event.train_run_id
    WHERE event.train_run_id=$1 AND event.source_shard_id=$2
      AND assignment.assignment_generation=$3
)
SELECT id,to_jsonb(transformed)
FROM transformed
WHERE ($5::uuid IS NULL AND id>$4) OR id=$5
ORDER BY id LIMIT $6`

func targetQuery(table string) (string, bool) {
	switch table {
	case "train_run_booking_snapshots":
		return `SELECT id,to_jsonb(row_value) FROM (SELECT * FROM public.train_run_booking_snapshots WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3) AS row_value`, true
	case "booking_seat_catalog":
		return `SELECT id,to_jsonb(row_value) FROM (SELECT * FROM public.booking_seat_catalog WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3) AS row_value`, true
	case "booking_fare_snapshots":
		return `SELECT id,to_jsonb(row_value) FROM (SELECT * FROM public.booking_fare_snapshots WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3) AS row_value`, true
	case "seat_inventory":
		return `SELECT id,to_jsonb(row_value) FROM (SELECT * FROM public.seat_inventory WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3) AS row_value`, true
	case "reservations":
		return `SELECT id,to_jsonb(row_value) FROM (SELECT * FROM public.reservations WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3) AS row_value`, true
	case "reservation_seats":
		return `SELECT id,to_jsonb(row_value) FROM (SELECT * FROM public.reservation_seats WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3) AS row_value`, true
	case "ticket_orders":
		return `SELECT id,to_jsonb(row_value) FROM (SELECT * FROM public.ticket_orders WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3) AS row_value`, true
	case "tickets":
		return `SELECT id,to_jsonb(row_value) FROM (SELECT * FROM public.tickets WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3) AS row_value`, true
	case "idempotency_records":
		return `SELECT id,to_jsonb(row_value) FROM (SELECT * FROM public.idempotency_records WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3) AS row_value`, true
	case "booking_command_receipts":
		return `SELECT id,to_jsonb(row_value) FROM (SELECT * FROM public.booking_command_receipts WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3) AS row_value`, true
	case "outbox_events":
		return `SELECT id,to_jsonb(row_value) FROM (SELECT * FROM public.outbox_events WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3) AS row_value`, true
	default:
		return "", false
	}
}
