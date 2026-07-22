# Hot-Train Protection

Milestone 2 positions this project as a single-region railway booking backend
with Redis-backed hot-train waiting-room admission and
PostgreSQL-authoritative segment seat allocation.

## Admission gate

An operator or admin can enable one durable policy for a train-run and seat
class. The policy bounds queue size, global issuance rate, global inflight
admissions, token TTL, processing lease, and queue-entry TTL. Policy mutations
use optimistic versions, a fail-closed per-actor Redis rate limit of 20
mutations per hour, and transactional outbox events.

For an enabled policy, reservation creation requires a valid
`X-Admission-Token`. The token must match the authenticated owner, current
policy, train run, route interval, seat class, passenger count, booking
fingerprint, and idempotency identity. It is acquired atomically before a local
non-blocking execution slot and the PostgreSQL transaction.

For an absent or disabled policy, the existing Milestone 1 booking flow remains
in place and no admission token is required.

## Independent protection layers

| Layer | Scope | Authority | Full behavior |
|---|---|---|---|
| Queue capacity | One hot policy | Redis Lua | Reject join with bounded `429` |
| Admission rate | One hot policy, all workers | Redis Lua and Redis `TIME` | Issue no more than the configured window |
| Inflight admissions | One hot policy, all workers | Redis Lua | Do not exceed the configured active-token bound |
| Booking concurrency | One API instance | Non-blocking in-process slot | Reject immediately with bounded `503` |
| Active-hold quota | One user | PostgreSQL transaction | Reject with `429` before inventory mutation |
| Segment overlap and seat count | One train run | PostgreSQL | Existing VARBIT allocation remains authoritative |

The waiting room is the only demand queue. None of these layers adds an
unbounded internal work queue.

## Failure behavior

Hot-run Redis failure, stale state, or lost continuity fails closed with `503`
and bounded retry guidance. The API does not downgrade an enabled policy,
create a private per-instance queue, or send an uncontrolled burst to
PostgreSQL.

Non-hot reservation behavior preserves the existing Redis limiter fallback and
still executes the authoritative PostgreSQL booking transaction. PostgreSQL
failure makes reservation and policy management unavailable. A token is never
reported consumed without a committed booking.

Redis AOF or equivalent managed persistence is an operational requirement.
Complete Redis loss can lose queue continuity and tokens. It cannot change
PostgreSQL inventory, reservations, quotas, tickets, or durable idempotency.

Policy mutation responses return the new durable version but do not estimate
the entries or inflight tokens invalidated in the previous generation. That
point-in-time impact preview remains deferred; admission counters and bounded
detect-only reconciliation are the current observability paths.

## Honest scope

Admission permits a booking attempt; it does not guarantee a seat. More users
may be admitted than eventual inventory supports, and PostgreSQL may return the
existing safe conflict. The system remains single-region and does not claim
global fairness, complete anti-bot protection, payment processing,
multi-region active-active writes, national-scale capacity, or
12306-equivalent throughput.
