# Transactional Outbox Events

## Envelope

```json
{
  "eventId": "uuid",
  "eventType": "reservation.held",
  "eventVersion": 1,
  "occurredAt": "2026-07-15T00:00:00Z",
  "aggregateType": "reservation",
  "aggregateId": "uuid",
  "data": {}
}
```

Payload data is event-specific, versioned, size-limited, and minimized. Full payloads are never logged. Event IDs provide downstream idempotency.

## Events

| Event | Producer transaction | Minimum data |
|---|---|---|
| `reservation.held` | create hold | reservation ID, train-run ID, status, expiry, seat count |
| `reservation.confirmed` | confirm | reservation ID, ticket-order ID, status |
| `ticket.created` | confirm | ticket ID, order ID, reservation ID; no passenger identity |
| `reservation.expired` | expire | reservation ID, status, released seat count |
| `reservation.cancelled` | cancel | reservation ID, status, released seat count |
| `trainrun.cancelled` | operator status change | train-run ID, status |

Unknown persisted event types are rejected by producers. Publisher/metric adapters map unknown external values to `unknown`.

## Processing lifecycle

`pending -> processing -> published` is the success path. Retryable failure returns to `pending` with bounded exponential backoff and jitter. Exhausted/non-retryable failure enters `dead_letter`. A stale processing lease can be reclaimed.

Claim and finalize are separate short PostgreSQL transactions. Publication happens outside a database transaction. Finalization requires matching worker ownership, preventing an obsolete worker from finalizing a reclaimed event.

## Publisher adapters

- `log`: default development adapter; logs event metadata only.
- `redis_stream`: optional and disabled by default; sends the minimized envelope with explicit stream length management.

No email, SMS, payment, or notification consumer exists in Milestone 1.

## Observability

Metrics count created, claimed, claim failures, publish successes/failures, finalize failures, and dead letters using bounded event type/result/reason labels. Backlog age/count and stale locks are operational signals, not direct readiness gates.
