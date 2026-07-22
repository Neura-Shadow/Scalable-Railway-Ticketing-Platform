# Durable Reservation Quotas

Milestone 2 prevents one account from accumulating an unbounded number of
active holds. PostgreSQL is the quota authority; Redis counters cannot grant or
deny a durable booking.

## Configured bounds

The API owns three positive bounded settings:

```text
RESERVATION_MAX_ACTIVE_HOLDS_PER_USER
RESERVATION_MAX_ACTIVE_HOLDS_PER_USER_PER_TRAIN_RUN
RESERVATION_MAX_ACTIVE_PASSENGERS_PER_USER
```

Only reservations currently in `held` state count. Confirmation frees
active-hold quota while keeping the confirmed seat occupied. Cancellation and
expiration also free quota by leaving `held`.

## Transaction order

Create-hold uses the existing PostgreSQL transaction and lock order:

```text
completed idempotency replay
-> per-user advisory transaction lock
-> indexed counts of authoritative held rows and reservation seats
-> existing train-run/passenger/fare validation
-> existing PostgreSQL VARBIT allocation
-> reservation, seats, idempotency completion, outbox
-> commit
```

The advisory transaction lock is derived from the canonical user UUID in a
Booking-owned namespace. A digest collision can serialize unrelated users but
cannot bypass a quota. Indexed counts avoid a second counter state machine and
remain derived from the committed reservation state.

Completed same-fingerprint idempotency replay returns before current quota
evaluation. A reservation cannot reject its own retry.

## Rejection invariant

If adding the pending hold or passengers would exceed any bound, the transaction
returns `reservation_quota_exceeded` as `429 Too Many Requests` with bounded
`Retry-After`. The rejected transaction:

- changes no seat mask;
- creates no reservation or reservation-seat row;
- does not complete durable idempotency;
- emits no outbox event; and
- cannot be bypassed by Redis loss or another API replica.

Migration 6 adds partial/indexed lookup support, not a quota counter table.
Reconciliation compares the same authoritative held rows against configured
limits and is detect-only. There is no counter drift to repair.

The design choice and rejected alternatives are recorded in ADR 014.
