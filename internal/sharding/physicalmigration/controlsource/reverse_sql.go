package controlsource

func reverseUpsertSQL(targetID, table string) string {
	if table == "outbox_events" && validSource(targetID) {
		return reverseOutboxUpsertSQL
	}
	switch targetID {
	case SourceLegacy:
		switch table {
		case "seat_inventory":
			return reverseLegacyInventoryUpsertSQL
		case "reservations":
			return reverseLegacyReservationUpsertSQL
		case "reservation_seats":
			return reverseLegacyReservationSeatUpsertSQL
		case "ticket_orders":
			return reverseLegacyTicketOrderUpsertSQL
		case "tickets":
			return reverseLegacyTicketUpsertSQL
		case "idempotency_records":
			return reverseLegacyIdempotencyUpsertSQL
		}
	case SourceZero:
		switch table {
		case "seat_inventory":
			return reverseZeroInventoryUpsertSQL
		case "reservations":
			return reverseZeroReservationUpsertSQL
		case "reservation_seats":
			return reverseZeroReservationSeatUpsertSQL
		case "ticket_orders":
			return reverseZeroTicketOrderUpsertSQL
		case "tickets":
			return reverseZeroTicketUpsertSQL
		case "idempotency_records":
			return reverseZeroIdempotencyUpsertSQL
		}
	case SourceOne:
		switch table {
		case "seat_inventory":
			return reverseOneInventoryUpsertSQL
		case "reservations":
			return reverseOneReservationUpsertSQL
		case "reservation_seats":
			return reverseOneReservationSeatUpsertSQL
		case "ticket_orders":
			return reverseOneTicketOrderUpsertSQL
		case "tickets":
			return reverseOneTicketUpsertSQL
		case "idempotency_records":
			return reverseOneIdempotencyUpsertSQL
		}
	}
	return ""
}

const reverseLegacyInventoryUpsertSQL = `
INSERT INTO public.seat_inventory
SELECT (jsonb_populate_record(NULL::public.seat_inventory,$1::jsonb)).*
ON CONFLICT (train_run_id,seat_id) DO UPDATE SET
 segment_count=EXCLUDED.segment_count,seat_class=EXCLUDED.seat_class,
 occupied_segments=EXCLUDED.occupied_segments,version=EXCLUDED.version,
 created_at=EXCLUDED.created_at,updated_at=EXCLUDED.updated_at`

const reverseZeroInventoryUpsertSQL = `
INSERT INTO booking_shard_0.seat_inventory
SELECT (jsonb_populate_record(NULL::booking_shard_0.seat_inventory,$1::jsonb)).*
ON CONFLICT (train_run_id,seat_id) DO UPDATE SET
 segment_count=EXCLUDED.segment_count,seat_class=EXCLUDED.seat_class,
 occupied_segments=EXCLUDED.occupied_segments,version=EXCLUDED.version,
 created_at=EXCLUDED.created_at,updated_at=EXCLUDED.updated_at`

const reverseOneInventoryUpsertSQL = `
INSERT INTO booking_shard_1.seat_inventory
SELECT (jsonb_populate_record(NULL::booking_shard_1.seat_inventory,$1::jsonb)).*
ON CONFLICT (train_run_id,seat_id) DO UPDATE SET
 segment_count=EXCLUDED.segment_count,seat_class=EXCLUDED.seat_class,
 occupied_segments=EXCLUDED.occupied_segments,version=EXCLUDED.version,
 created_at=EXCLUDED.created_at,updated_at=EXCLUDED.updated_at`

const reverseLegacyReservationUpsertSQL = `
INSERT INTO public.reservations
SELECT (jsonb_populate_record(NULL::public.reservations,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET
 user_id=EXCLUDED.user_id,train_run_id=EXCLUDED.train_run_id,
 segment_count=EXCLUDED.segment_count,from_stop_index=EXCLUDED.from_stop_index,
 to_stop_index=EXCLUDED.to_stop_index,seat_class=EXCLUDED.seat_class,
 status=EXCLUDED.status,expires_at=EXCLUDED.expires_at,
 total_amount_minor=EXCLUDED.total_amount_minor,currency=EXCLUDED.currency,
 created_at=EXCLUDED.created_at,updated_at=EXCLUDED.updated_at`

const reverseZeroReservationUpsertSQL = `
INSERT INTO booking_shard_0.reservations
SELECT (jsonb_populate_record(NULL::booking_shard_0.reservations,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET
 user_id=EXCLUDED.user_id,train_run_id=EXCLUDED.train_run_id,
 segment_count=EXCLUDED.segment_count,from_stop_index=EXCLUDED.from_stop_index,
 to_stop_index=EXCLUDED.to_stop_index,seat_class=EXCLUDED.seat_class,
 status=EXCLUDED.status,expires_at=EXCLUDED.expires_at,
 total_amount_minor=EXCLUDED.total_amount_minor,currency=EXCLUDED.currency,
 created_at=EXCLUDED.created_at,updated_at=EXCLUDED.updated_at`

const reverseOneReservationUpsertSQL = `
INSERT INTO booking_shard_1.reservations
SELECT (jsonb_populate_record(NULL::booking_shard_1.reservations,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET
 user_id=EXCLUDED.user_id,train_run_id=EXCLUDED.train_run_id,
 segment_count=EXCLUDED.segment_count,from_stop_index=EXCLUDED.from_stop_index,
 to_stop_index=EXCLUDED.to_stop_index,seat_class=EXCLUDED.seat_class,
 status=EXCLUDED.status,expires_at=EXCLUDED.expires_at,
 total_amount_minor=EXCLUDED.total_amount_minor,currency=EXCLUDED.currency,
 created_at=EXCLUDED.created_at,updated_at=EXCLUDED.updated_at`

const reverseLegacyReservationSeatUpsertSQL = `
INSERT INTO public.reservation_seats
SELECT (jsonb_populate_record(NULL::public.reservation_seats,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET
 reservation_id=EXCLUDED.reservation_id,segment_count=EXCLUDED.segment_count,
 seat_id=EXCLUDED.seat_id,passenger_id=EXCLUDED.passenger_id,
 segment_mask=EXCLUDED.segment_mask,fare_amount_minor=EXCLUDED.fare_amount_minor,
 currency=EXCLUDED.currency,created_at=EXCLUDED.created_at,train_run_id=EXCLUDED.train_run_id`

const reverseZeroReservationSeatUpsertSQL = `
INSERT INTO booking_shard_0.reservation_seats
SELECT (jsonb_populate_record(NULL::booking_shard_0.reservation_seats,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET
 reservation_id=EXCLUDED.reservation_id,segment_count=EXCLUDED.segment_count,
 seat_id=EXCLUDED.seat_id,passenger_id=EXCLUDED.passenger_id,
 segment_mask=EXCLUDED.segment_mask,fare_amount_minor=EXCLUDED.fare_amount_minor,
 currency=EXCLUDED.currency,created_at=EXCLUDED.created_at,train_run_id=EXCLUDED.train_run_id`

const reverseOneReservationSeatUpsertSQL = `
INSERT INTO booking_shard_1.reservation_seats
SELECT (jsonb_populate_record(NULL::booking_shard_1.reservation_seats,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET
 reservation_id=EXCLUDED.reservation_id,segment_count=EXCLUDED.segment_count,
 seat_id=EXCLUDED.seat_id,passenger_id=EXCLUDED.passenger_id,
 segment_mask=EXCLUDED.segment_mask,fare_amount_minor=EXCLUDED.fare_amount_minor,
 currency=EXCLUDED.currency,created_at=EXCLUDED.created_at,train_run_id=EXCLUDED.train_run_id`

const reverseLegacyTicketOrderUpsertSQL = `
INSERT INTO public.ticket_orders
SELECT (jsonb_populate_record(NULL::public.ticket_orders,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET reservation_id=EXCLUDED.reservation_id,
 user_id=EXCLUDED.user_id,status=EXCLUDED.status,total_amount_minor=EXCLUDED.total_amount_minor,
 currency=EXCLUDED.currency,created_at=EXCLUDED.created_at,updated_at=EXCLUDED.updated_at`

const reverseZeroTicketOrderUpsertSQL = `
INSERT INTO booking_shard_0.ticket_orders
SELECT (jsonb_populate_record(NULL::booking_shard_0.ticket_orders,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET reservation_id=EXCLUDED.reservation_id,
 user_id=EXCLUDED.user_id,status=EXCLUDED.status,total_amount_minor=EXCLUDED.total_amount_minor,
 currency=EXCLUDED.currency,created_at=EXCLUDED.created_at,updated_at=EXCLUDED.updated_at`

const reverseOneTicketOrderUpsertSQL = `
INSERT INTO booking_shard_1.ticket_orders
SELECT (jsonb_populate_record(NULL::booking_shard_1.ticket_orders,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET reservation_id=EXCLUDED.reservation_id,
 user_id=EXCLUDED.user_id,status=EXCLUDED.status,total_amount_minor=EXCLUDED.total_amount_minor,
 currency=EXCLUDED.currency,created_at=EXCLUDED.created_at,updated_at=EXCLUDED.updated_at`

const reverseLegacyTicketUpsertSQL = `
INSERT INTO public.tickets
SELECT (jsonb_populate_record(NULL::public.tickets,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET ticket_order_id=EXCLUDED.ticket_order_id,
 reservation_seat_id=EXCLUDED.reservation_seat_id,ticket_code=EXCLUDED.ticket_code,
 status=EXCLUDED.status,created_at=EXCLUDED.created_at,updated_at=EXCLUDED.updated_at`

const reverseZeroTicketUpsertSQL = `
INSERT INTO booking_shard_0.tickets
SELECT (jsonb_populate_record(NULL::booking_shard_0.tickets,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET ticket_order_id=EXCLUDED.ticket_order_id,
 reservation_seat_id=EXCLUDED.reservation_seat_id,ticket_code=EXCLUDED.ticket_code,
 status=EXCLUDED.status,created_at=EXCLUDED.created_at,updated_at=EXCLUDED.updated_at`

const reverseOneTicketUpsertSQL = `
INSERT INTO booking_shard_1.tickets
SELECT (jsonb_populate_record(NULL::booking_shard_1.tickets,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET ticket_order_id=EXCLUDED.ticket_order_id,
 reservation_seat_id=EXCLUDED.reservation_seat_id,ticket_code=EXCLUDED.ticket_code,
 status=EXCLUDED.status,created_at=EXCLUDED.created_at,updated_at=EXCLUDED.updated_at`

const reverseLegacyIdempotencyUpsertSQL = `
INSERT INTO public.idempotency_records
SELECT (jsonb_populate_record(NULL::public.idempotency_records,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET user_id=EXCLUDED.user_id,operation=EXCLUDED.operation,
 key_hash=EXCLUDED.key_hash,request_fingerprint=EXCLUDED.request_fingerprint,
 status=EXCLUDED.status,resource_type=EXCLUDED.resource_type,resource_id=EXCLUDED.resource_id,
 expires_at=EXCLUDED.expires_at,created_at=EXCLUDED.created_at,
 updated_at=EXCLUDED.updated_at,train_run_id=EXCLUDED.train_run_id`

const reverseZeroIdempotencyUpsertSQL = `
INSERT INTO booking_shard_0.idempotency_records
SELECT (jsonb_populate_record(NULL::booking_shard_0.idempotency_records,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET user_id=EXCLUDED.user_id,operation=EXCLUDED.operation,
 key_hash=EXCLUDED.key_hash,request_fingerprint=EXCLUDED.request_fingerprint,
 status=EXCLUDED.status,resource_type=EXCLUDED.resource_type,resource_id=EXCLUDED.resource_id,
 expires_at=EXCLUDED.expires_at,created_at=EXCLUDED.created_at,
 updated_at=EXCLUDED.updated_at,train_run_id=EXCLUDED.train_run_id`

const reverseOneIdempotencyUpsertSQL = `
INSERT INTO booking_shard_1.idempotency_records
SELECT (jsonb_populate_record(NULL::booking_shard_1.idempotency_records,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET user_id=EXCLUDED.user_id,operation=EXCLUDED.operation,
 key_hash=EXCLUDED.key_hash,request_fingerprint=EXCLUDED.request_fingerprint,
 status=EXCLUDED.status,resource_type=EXCLUDED.resource_type,resource_id=EXCLUDED.resource_id,
 expires_at=EXCLUDED.expires_at,created_at=EXCLUDED.created_at,
 updated_at=EXCLUDED.updated_at,train_run_id=EXCLUDED.train_run_id`

const reverseOutboxUpsertSQL = `
INSERT INTO public.outbox_events
SELECT (jsonb_populate_record(NULL::public.outbox_events,$1::jsonb)).*
ON CONFLICT (id) DO UPDATE SET aggregate_type=EXCLUDED.aggregate_type,
 aggregate_id=EXCLUDED.aggregate_id,event_type=EXCLUDED.event_type,
 event_version=EXCLUDED.event_version,payload=EXCLUDED.payload,status=EXCLUDED.status,
 attempts=EXCLUDED.attempts,next_attempt_at=EXCLUDED.next_attempt_at,
 locked_at=EXCLUDED.locked_at,locked_by=EXCLUDED.locked_by,
 created_at=EXCLUDED.created_at,published_at=EXCLUDED.published_at,
 train_run_id=EXCLUDED.train_run_id,shard_id=EXCLUDED.shard_id,
 assignment_generation=EXCLUDED.assignment_generation`

func reverseDeleteSQL(targetID, table string) string {
	if table == "outbox_events" && validSource(targetID) {
		return `DELETE FROM public.outbox_events WHERE id=$2 AND train_run_id=$1 AND shard_id=$3`
	}
	switch targetID {
	case SourceLegacy:
		switch table {
		case "seat_inventory":
			return `DELETE FROM public.seat_inventory WHERE train_run_id=$1 AND public.physical_source_entity_id($1,'inventory',seat_id)=$2`
		case "reservations":
			return `DELETE FROM public.reservations WHERE train_run_id=$1 AND id=$2`
		case "reservation_seats":
			return `DELETE FROM public.reservation_seats WHERE train_run_id=$1 AND id=$2`
		case "ticket_orders":
			return `DELETE FROM public.ticket_orders WHERE id=$2 AND reservation_id IN (SELECT id FROM public.reservations WHERE train_run_id=$1)`
		case "tickets":
			return `DELETE FROM public.tickets WHERE id=$2 AND reservation_seat_id IN (SELECT id FROM public.reservation_seats WHERE train_run_id=$1)`
		case "idempotency_records":
			return `DELETE FROM public.idempotency_records WHERE train_run_id=$1 AND id=$2`
		}
	case SourceZero:
		switch table {
		case "seat_inventory":
			return `DELETE FROM booking_shard_0.seat_inventory WHERE train_run_id=$1 AND public.physical_source_entity_id($1,'inventory',seat_id)=$2`
		case "reservations":
			return `DELETE FROM booking_shard_0.reservations WHERE train_run_id=$1 AND id=$2`
		case "reservation_seats":
			return `DELETE FROM booking_shard_0.reservation_seats WHERE train_run_id=$1 AND id=$2`
		case "ticket_orders":
			return `DELETE FROM booking_shard_0.ticket_orders WHERE id=$2 AND reservation_id IN (SELECT id FROM booking_shard_0.reservations WHERE train_run_id=$1)`
		case "tickets":
			return `DELETE FROM booking_shard_0.tickets WHERE id=$2 AND reservation_seat_id IN (SELECT id FROM booking_shard_0.reservation_seats WHERE train_run_id=$1)`
		case "idempotency_records":
			return `DELETE FROM booking_shard_0.idempotency_records WHERE train_run_id=$1 AND id=$2`
		}
	case SourceOne:
		switch table {
		case "seat_inventory":
			return `DELETE FROM booking_shard_1.seat_inventory WHERE train_run_id=$1 AND public.physical_source_entity_id($1,'inventory',seat_id)=$2`
		case "reservations":
			return `DELETE FROM booking_shard_1.reservations WHERE train_run_id=$1 AND id=$2`
		case "reservation_seats":
			return `DELETE FROM booking_shard_1.reservation_seats WHERE train_run_id=$1 AND id=$2`
		case "ticket_orders":
			return `DELETE FROM booking_shard_1.ticket_orders WHERE id=$2 AND reservation_id IN (SELECT id FROM booking_shard_1.reservations WHERE train_run_id=$1)`
		case "tickets":
			return `DELETE FROM booking_shard_1.tickets WHERE id=$2 AND reservation_seat_id IN (SELECT id FROM booking_shard_1.reservation_seats WHERE train_run_id=$1)`
		case "idempotency_records":
			return `DELETE FROM booking_shard_1.idempotency_records WHERE train_run_id=$1 AND id=$2`
		}
	}
	return ""
}

func reverseCleanupCountSQL(targetID string) string {
	switch targetID {
	case SourceLegacy:
		return `SELECT
 (SELECT count(*) FROM public.tickets WHERE reservation_seat_id IN (SELECT id FROM public.reservation_seats WHERE train_run_id=$1))+
 (SELECT count(*) FROM public.ticket_orders WHERE reservation_id IN (SELECT id FROM public.reservations WHERE train_run_id=$1))+
 (SELECT count(*) FROM public.reservation_seats WHERE train_run_id=$1)+
 (SELECT count(*) FROM public.reservations WHERE train_run_id=$1)+
 (SELECT count(*) FROM public.idempotency_records WHERE train_run_id=$1)+
 (SELECT count(*) FROM public.seat_inventory WHERE train_run_id=$1)+
 (SELECT count(*) FROM public.outbox_events WHERE train_run_id=$1 AND shard_id=$2)`
	case SourceZero:
		return `SELECT
 (SELECT count(*) FROM booking_shard_0.tickets WHERE reservation_seat_id IN (SELECT id FROM booking_shard_0.reservation_seats WHERE train_run_id=$1))+
 (SELECT count(*) FROM booking_shard_0.ticket_orders WHERE reservation_id IN (SELECT id FROM booking_shard_0.reservations WHERE train_run_id=$1))+
 (SELECT count(*) FROM booking_shard_0.reservation_seats WHERE train_run_id=$1)+
 (SELECT count(*) FROM booking_shard_0.reservations WHERE train_run_id=$1)+
 (SELECT count(*) FROM booking_shard_0.idempotency_records WHERE train_run_id=$1)+
 (SELECT count(*) FROM booking_shard_0.seat_inventory WHERE train_run_id=$1)+
 (SELECT count(*) FROM public.outbox_events WHERE train_run_id=$1 AND shard_id=$2)`
	case SourceOne:
		return `SELECT
 (SELECT count(*) FROM booking_shard_1.tickets WHERE reservation_seat_id IN (SELECT id FROM booking_shard_1.reservation_seats WHERE train_run_id=$1))+
 (SELECT count(*) FROM booking_shard_1.ticket_orders WHERE reservation_id IN (SELECT id FROM booking_shard_1.reservations WHERE train_run_id=$1))+
 (SELECT count(*) FROM booking_shard_1.reservation_seats WHERE train_run_id=$1)+
 (SELECT count(*) FROM booking_shard_1.reservations WHERE train_run_id=$1)+
 (SELECT count(*) FROM booking_shard_1.idempotency_records WHERE train_run_id=$1)+
 (SELECT count(*) FROM booking_shard_1.seat_inventory WHERE train_run_id=$1)+
 (SELECT count(*) FROM public.outbox_events WHERE train_run_id=$1 AND shard_id=$2)`
	default:
		return `SELECT -1`
	}
}

func reverseCleanupStatements(targetID string) []string {
	switch targetID {
	case SourceLegacy:
		return []string{
			`DELETE FROM public.tickets WHERE reservation_seat_id IN (SELECT id FROM public.reservation_seats WHERE train_run_id=$1)`,
			`DELETE FROM public.ticket_orders WHERE reservation_id IN (SELECT id FROM public.reservations WHERE train_run_id=$1)`,
			`DELETE FROM public.reservation_seats WHERE train_run_id=$1`,
			`DELETE FROM public.reservations WHERE train_run_id=$1`,
			`DELETE FROM public.idempotency_records WHERE train_run_id=$1`,
			`DELETE FROM public.seat_inventory WHERE train_run_id=$1`,
			`DELETE FROM public.outbox_events WHERE train_run_id=$1 AND shard_id=$2`,
		}
	case SourceZero:
		return []string{
			`DELETE FROM booking_shard_0.tickets WHERE reservation_seat_id IN (SELECT id FROM booking_shard_0.reservation_seats WHERE train_run_id=$1)`,
			`DELETE FROM booking_shard_0.ticket_orders WHERE reservation_id IN (SELECT id FROM booking_shard_0.reservations WHERE train_run_id=$1)`,
			`DELETE FROM booking_shard_0.reservation_seats WHERE train_run_id=$1`,
			`DELETE FROM booking_shard_0.reservations WHERE train_run_id=$1`,
			`DELETE FROM booking_shard_0.idempotency_records WHERE train_run_id=$1`,
			`DELETE FROM booking_shard_0.seat_inventory WHERE train_run_id=$1`,
			`DELETE FROM public.outbox_events WHERE train_run_id=$1 AND shard_id=$2`,
		}
	case SourceOne:
		return []string{
			`DELETE FROM booking_shard_1.tickets WHERE reservation_seat_id IN (SELECT id FROM booking_shard_1.reservation_seats WHERE train_run_id=$1)`,
			`DELETE FROM booking_shard_1.ticket_orders WHERE reservation_id IN (SELECT id FROM booking_shard_1.reservations WHERE train_run_id=$1)`,
			`DELETE FROM booking_shard_1.reservation_seats WHERE train_run_id=$1`,
			`DELETE FROM booking_shard_1.reservations WHERE train_run_id=$1`,
			`DELETE FROM booking_shard_1.idempotency_records WHERE train_run_id=$1`,
			`DELETE FROM booking_shard_1.seat_inventory WHERE train_run_id=$1`,
			`DELETE FROM public.outbox_events WHERE train_run_id=$1 AND shard_id=$2`,
		}
	default:
		return nil
	}
}

func reverseTargetFencePrepareSQL(targetID string) string {
	switch targetID {
	case SourceLegacy:
		return `INSERT INTO public.train_run_write_fences(train_run_id,assignment_generation,write_enabled)
VALUES($1,$2,false) ON CONFLICT(train_run_id) DO UPDATE SET assignment_generation=EXCLUDED.assignment_generation,
write_enabled=false WHERE NOT public.train_run_write_fences.write_enabled`
	case SourceZero:
		return `INSERT INTO booking_shard_0.train_run_write_fences(train_run_id,assignment_generation,write_enabled)
VALUES($1,$2,false) ON CONFLICT(train_run_id) DO UPDATE SET assignment_generation=EXCLUDED.assignment_generation,
write_enabled=false WHERE NOT booking_shard_0.train_run_write_fences.write_enabled`
	case SourceOne:
		return `INSERT INTO booking_shard_1.train_run_write_fences(train_run_id,assignment_generation,write_enabled)
VALUES($1,$2,false) ON CONFLICT(train_run_id) DO UPDATE SET assignment_generation=EXCLUDED.assignment_generation,
write_enabled=false WHERE NOT booking_shard_1.train_run_write_fences.write_enabled`
	default:
		return `SELECT 0 WHERE false`
	}
}

func reverseTargetFenceEnableSQL(targetID string) string {
	switch targetID {
	case SourceLegacy:
		return `UPDATE public.train_run_write_fences SET write_enabled=true WHERE train_run_id=$1 AND assignment_generation=$2 AND NOT write_enabled`
	case SourceZero:
		return `UPDATE booking_shard_0.train_run_write_fences SET write_enabled=true WHERE train_run_id=$1 AND assignment_generation=$2 AND NOT write_enabled`
	case SourceOne:
		return `UPDATE booking_shard_1.train_run_write_fences SET write_enabled=true WHERE train_run_id=$1 AND assignment_generation=$2 AND NOT write_enabled`
	default:
		return `SELECT 0 WHERE false`
	}
}

func reverseTargetFenceDisableSQL(targetID string) string {
	switch targetID {
	case SourceLegacy:
		return `UPDATE public.train_run_write_fences SET write_enabled=false WHERE train_run_id=$1 AND assignment_generation=$2 AND write_enabled`
	case SourceZero:
		return `UPDATE booking_shard_0.train_run_write_fences SET write_enabled=false WHERE train_run_id=$1 AND assignment_generation=$2 AND write_enabled`
	case SourceOne:
		return `UPDATE booking_shard_1.train_run_write_fences SET write_enabled=false WHERE train_run_id=$1 AND assignment_generation=$2 AND write_enabled`
	default:
		return `SELECT 0 WHERE false`
	}
}

func reverseSourceValidationSQL(table string) string {
	switch table {
	case "seat_inventory":
		return `SELECT id,to_jsonb(source_row) FROM public.seat_inventory AS source_row WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3`
	case "reservations":
		return `SELECT id,to_jsonb(source_row) FROM public.reservations AS source_row WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3`
	case "reservation_seats":
		return `SELECT id,to_jsonb(source_row) FROM public.reservation_seats AS source_row WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3`
	case "ticket_orders":
		return `SELECT id,to_jsonb(source_row) FROM public.ticket_orders AS source_row WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3`
	case "tickets":
		return `SELECT id,to_jsonb(source_row) FROM public.tickets AS source_row WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3`
	case "idempotency_records":
		return `SELECT id,to_jsonb(source_row) FROM public.idempotency_records AS source_row WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3`
	case "outbox_events":
		return `SELECT id,to_jsonb(source_row) FROM public.outbox_events AS source_row WHERE train_run_id=$1 AND assignment_generation=$2 ORDER BY id LIMIT $3`
	default:
		return ""
	}
}

func reverseTargetValidationSQL(targetID, table string) string {
	if table == "outbox_events" && validSource(targetID) {
		return `SELECT id,to_jsonb(target_row) FROM public.outbox_events AS target_row WHERE train_run_id=$1 AND shard_id=$2 AND assignment_generation=$3 ORDER BY id LIMIT $4`
	}
	switch targetID {
	case SourceLegacy:
		switch table {
		case "seat_inventory":
			return `SELECT public.physical_source_entity_id($1,'inventory',seat_id),to_jsonb(target_row) FROM public.seat_inventory AS target_row WHERE train_run_id=$1 ORDER BY seat_id LIMIT $2`
		case "reservations":
			return `SELECT id,to_jsonb(target_row) FROM public.reservations AS target_row WHERE train_run_id=$1 ORDER BY id LIMIT $2`
		case "reservation_seats":
			return `SELECT id,to_jsonb(target_row) FROM public.reservation_seats AS target_row WHERE train_run_id=$1 ORDER BY id LIMIT $2`
		case "ticket_orders":
			return `SELECT id,to_jsonb(target_row) FROM public.ticket_orders AS target_row WHERE reservation_id IN (SELECT id FROM public.reservations WHERE train_run_id=$1) ORDER BY id LIMIT $2`
		case "tickets":
			return `SELECT id,to_jsonb(target_row) FROM public.tickets AS target_row WHERE reservation_seat_id IN (SELECT id FROM public.reservation_seats WHERE train_run_id=$1) ORDER BY id LIMIT $2`
		case "idempotency_records":
			return `SELECT id,to_jsonb(target_row) FROM public.idempotency_records AS target_row WHERE train_run_id=$1 ORDER BY id LIMIT $2`
		}
	case SourceZero:
		switch table {
		case "seat_inventory":
			return `SELECT public.physical_source_entity_id($1,'inventory',seat_id),to_jsonb(target_row) FROM booking_shard_0.seat_inventory AS target_row WHERE train_run_id=$1 ORDER BY seat_id LIMIT $2`
		case "reservations":
			return `SELECT id,to_jsonb(target_row) FROM booking_shard_0.reservations AS target_row WHERE train_run_id=$1 ORDER BY id LIMIT $2`
		case "reservation_seats":
			return `SELECT id,to_jsonb(target_row) FROM booking_shard_0.reservation_seats AS target_row WHERE train_run_id=$1 ORDER BY id LIMIT $2`
		case "ticket_orders":
			return `SELECT id,to_jsonb(target_row) FROM booking_shard_0.ticket_orders AS target_row WHERE reservation_id IN (SELECT id FROM booking_shard_0.reservations WHERE train_run_id=$1) ORDER BY id LIMIT $2`
		case "tickets":
			return `SELECT id,to_jsonb(target_row) FROM booking_shard_0.tickets AS target_row WHERE reservation_seat_id IN (SELECT id FROM booking_shard_0.reservation_seats WHERE train_run_id=$1) ORDER BY id LIMIT $2`
		case "idempotency_records":
			return `SELECT id,to_jsonb(target_row) FROM booking_shard_0.idempotency_records AS target_row WHERE train_run_id=$1 ORDER BY id LIMIT $2`
		}
	case SourceOne:
		switch table {
		case "seat_inventory":
			return `SELECT public.physical_source_entity_id($1,'inventory',seat_id),to_jsonb(target_row) FROM booking_shard_1.seat_inventory AS target_row WHERE train_run_id=$1 ORDER BY seat_id LIMIT $2`
		case "reservations":
			return `SELECT id,to_jsonb(target_row) FROM booking_shard_1.reservations AS target_row WHERE train_run_id=$1 ORDER BY id LIMIT $2`
		case "reservation_seats":
			return `SELECT id,to_jsonb(target_row) FROM booking_shard_1.reservation_seats AS target_row WHERE train_run_id=$1 ORDER BY id LIMIT $2`
		case "ticket_orders":
			return `SELECT id,to_jsonb(target_row) FROM booking_shard_1.ticket_orders AS target_row WHERE reservation_id IN (SELECT id FROM booking_shard_1.reservations WHERE train_run_id=$1) ORDER BY id LIMIT $2`
		case "tickets":
			return `SELECT id,to_jsonb(target_row) FROM booking_shard_1.tickets AS target_row WHERE reservation_seat_id IN (SELECT id FROM booking_shard_1.reservation_seats WHERE train_run_id=$1) ORDER BY id LIMIT $2`
		case "idempotency_records":
			return `SELECT id,to_jsonb(target_row) FROM booking_shard_1.idempotency_records AS target_row WHERE train_run_id=$1 ORDER BY id LIMIT $2`
		}
	}
	return ""
}
