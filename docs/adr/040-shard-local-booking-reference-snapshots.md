# ADR 040: Booking-Critical Reference State Is Versioned and Shard Local

- Status: Accepted for Milestone 5 implementation
- Date: 2026-07-29

## Context

The current create-reservation transaction reads global train-run status,
service topology, passengers, fares, admission policy, coaches, seats, and
users while mutating schema-local inventory. Schema-local booking tables also
use foreign keys to public users, passengers, train runs, and seats. Those
queries and constraints cannot cross independent PostgreSQL databases.

Booking correctness cannot rely on a synchronous control read performed before
the shard transaction. A train run may be cancelled, a fare changed, or a seat
disabled between that read and local allocation. Conversely, updating a global
read model before the physical shard can enforce an operator change could let
control report cancellation while the shard still accepts a new booking.

## Decision

Each physical booking shard stores versioned reference snapshots sufficient to
make every booking decision inside its local transaction. The initial modules
are:

- `train_run_booking_snapshots` for train-run identity, service date, segment
  count, route/version identity, bookable status, booking-policy version, source
  version/time, assignment generation, and active state;
- `booking_seat_catalog` for bounded train/coach/seat identifiers, deterministic
  coach and seat order, seat class, active state, source version/time,
  assignment generation, and active state; and
- `booking_fare_snapshots` for train-run or approved route fare identity,
  segment range, seat class, amount, currency, source version/time, assignment
  generation, and active state.

Snapshot identifiers are immutable and bounded. Passenger PII is not copied
into these reference snapshots. Reservation and reservation-seat records may
store only the immutable passenger identity and explicitly documented ticket
display snapshot required for the booking record; the control database remains
the authority for passenger profile and ownership changes.

Before allocating inventory, the shard transaction locks or otherwise validates
the required snapshot versions together with the local generation fence. A
missing, inactive, incompatible-generation, or stale required snapshot fails
closed before command receipt success, seat mutation, or outbox intent. The
global search/read model remains a disposable projection and never authorizes a
booking.

Snapshot installation is explicit, idempotent, monotonically versioned, and
performed through a shard-aware operator command. A repeated command with the
same version and fingerprint has no new effect; an older or conflicting version
is rejected. The update commits locally with shard outbox intent and mutation
journal capture where migration is active.

Operator changes that affect booking follow this ordering:

1. create a durable control-plane command for the affected train run and
   version;
2. resolve the current assignment and expected generation;
3. apply the versioned snapshot change to exactly one authoritative shard under
   its local fence;
4. record the shard-local receipt and outbox intent atomically with the
   snapshot; and
5. finalize control/global projection state idempotently from the receipt.

For safety-reducing changes such as cancellation, booking disablement, or seat
disablement, control does not announce the state as effective for booking until
the authoritative shard has durably installed it. If shard application is
unknown or unavailable, new affected booking work fails closed or the operator
command remains pending; control must not claim cancellation while the shard
still accepts creates.

Fare changes are versioned in the same way. A reservation records the exact
local fare snapshot and currency used, so later changes do not rewrite an
existing amount. Seat disablement and train-run cancellation serialize with
new booking through local snapshot/fence locks and inventory predicates.

Migration bootstraps target snapshots before base copy and keeps the target
write fence disabled. Snapshot updates on the authoritative source are included
in mutation capture and replayed idempotently. Final validation compares
snapshot identities, versions, active states, ordering, fares, currencies, and
assignment generation before target enablement. A snapshot update racing
cutover therefore either belongs to the final source journal sequence and is
applied, or observes the newer fence and retries against the target; it cannot
silently exist only in control.

Legacy and logical-schema routes retain their existing global-reference adapter
while physical routes use local snapshots. Both adapters present the same
booking rules and test cases; physical selection is opt-in and does not rewrite
the PostgreSQL VARBIT seat-allocation predicate.

## Required concurrency evidence

- Train-run cancellation versus new booking cannot commit a booking after the
  effective local cancellation version.
- Fare update versus booking records either the complete old or complete new
  fare version, never a mixed amount/currency result.
- Seat disablement versus booking cannot allocate a seat after its effective
  local disable version.
- Snapshot update versus migration cutover converges on the target before it is
  write-enabled.
- Stale and missing snapshots fail closed without completing command receipts,
  consuming inventory, or publishing successful booking intent.

## Consequences

- Physical booking transactions no longer query control PostgreSQL or assume
  cross-database foreign keys.
- Reference data is duplicated deliberately, with source version and
  reconciliation making drift observable.
- Operator changes gain a durable command/finalization journey and may be
  delayed when a shard is unavailable.
- Booking remains available from local authoritative state when unrelated
  control read-model work is delayed, provided route authority has already been
  established safely.
- Storage and migration work increase because snapshots move and reconcile with
  the train-run booking dataset.
- No passenger PII replication, production-wide catalog replication, or
  distributed referential-integrity claim is introduced.

## Rejected alternatives

- Read control synchronously inside the shard transaction: rejected because no
  cross-database transaction or lock makes the observation atomic.
- Validate global data before opening the shard transaction: rejected because
  operator changes can race the local mutation.
- Copy the entire global catalog and passenger profiles to every shard:
  rejected because it expands PII, update, and authority scope unnecessarily.
- Treat the global read model as booking authority: rejected because it is
  asynchronous and disposable.
- Apply operator state only in control and repair shards later: rejected because
  the shard could continue accepting writes forbidden by control.
- Redesign segment-mask inventory while extracting snapshots: rejected because
  placement changes must preserve the proven PostgreSQL allocation predicate.
