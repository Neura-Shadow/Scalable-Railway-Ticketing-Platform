# Domain Model

Milestone 3 is a production-minded, single-region railway booking backend with bounded hot-train admission plus disposable PostgreSQL journey projections and optional Redis read caches. PostgreSQL is authoritative for train-run status, hot-train policy, seat occupancy, reservation lifecycle, durable quotas, tickets, durable idempotency, and outbox events. Redis owns only ephemeral waiting-room/admission state and non-authoritative read hints.

## Bounded contexts

| Context | Responsibilities | Owned concepts | Dependencies |
|---|---|---|---|
| Accounts | Credentials, JWT lifecycle, RBAC, customer ownership | User, Passenger | Platform clock/config; PostgreSQL adapter |
| Railway Offering | Topology, rolling stock, fares, dated services, bookable status, commissioning | Station, Route, RouteStop, Train, Coach, Seat, SeatClass, TrainRun, Fare | PostgreSQL adapter |
| Admission | Hot-train classification, bounded waiting-room admission, token lifecycle, admission reconciliation | HotTrainPolicy, WaitingRoomEntry, AdmissionToken, AdmissionDecision, QueuePosition | Railway Offering journey-resolution port; PostgreSQL policy adapter; Redis queue/token adapter |
| Booking | Atomic occupancy/allocation, lifecycle commands, tickets, idempotency, reconciliation | SegmentMask, SeatInventory, Reservation, ReservationSeat, TicketOrder, Ticket, IdempotencyRecord | Accounts ownership port; Railway Offering sales-input port; event append port |
| Query (supporting) | Browse/search/availability hints; projection rebuild and non-authoritative Redis caches | Disposable journey projection and cache namespaces | Authoritative PostgreSQL source adapter; Redis cache decorator |
| Event Relay (supporting) | Claim, publish, retry, recover, finalize | OutboxEvent delivery state | PostgreSQL and publisher adapters |
| Platform (supporting) | Technology adapters and process lifecycle | Config, database/Redis pools, middleware, response, metrics, clock | No railway business rules |

Contexts are package and ownership groupings, not network services. Admission remains inside the modular monolith even though its worker can run as a separate process. Booking owns every reservation transaction. Transactional outbox insertion is a Booking/Railway Offering/Admission command responsibility; Event Relay owns only asynchronous delivery.

## Aggregate and transaction ownership

### Route

A Route owns the ordered RouteStop definition. Stops are contiguous and zero-based, stations do not repeat, and arrival/departure offsets are non-decreasing. N stops define N-1 segments.

### Train and seating plan

A Train owns Coaches; a Coach owns Seats. Coach numbers are unique per train and seat numbers are unique per coach. A seat's sellable class is inherited from its coach and its active status must be true when inventory is initialized.

### TrainRun and SeatInventory

A TrainRun binds one Train and Route to an operator-local service date and UTC schedule. Its copied `segment_count` is immutable after inventory initialization. Only `scheduled` is bookable by default. Railway Offering's `CommissionTrainRun` creates the run and every Booking SeatInventory row in one transaction before exposing it as bookable. SeatInventory has the primary key `(train_run_id, seat_id)` and an occupied mask whose bit length equals `segment_count`.

### HotTrainPolicy and waiting-room admission

A HotTrainPolicy classifies one `(train_run_id, seat_class)` pair and stores bounded queue, issuance, inflight, token, lease, and entry limits in PostgreSQL. An enabled policy requires Redis-backed admission; a disabled or absent policy follows the existing non-hot booking path.

WaitingRoomEntry and AdmissionToken are bounded-TTL Redis control-plane records. They order admission attempts and protect PostgreSQL from uncontrolled bursts; they never reserve or promise inventory. Queue ordering uses a Redis-generated monotonic sequence. Admission tokens are customer-, journey-, class-, passenger-count-, request-, and idempotency-bound, and a successful PostgreSQL booking consumes one token. PostgreSQL still rechecks every train-run, quota, passenger, fare, and segment-allocation invariant.

### Reservation

A Reservation is the lifecycle aggregate for one customer, one train run, one ordered interval, and one seat class. ReservationSeat children snapshot the unique Passenger, physical Seat, exact SegmentMask, fare minor units, and currency. The aggregate state is held, confirmed, expired, or cancelled.

### TicketOrder

Confirmation creates one TicketOrder and one Ticket for each ReservationSeat. These artifacts prove domain confirmation only; no real payment state or gateway exists.

### Durable command

An IdempotencyRecord is transaction metadata, not a customer aggregate. It scopes a hashed key to user and operation, binds the canonical fingerprint, and stores the resulting resource reference.

### OutboxEvent

An OutboxEvent is the committed intent to publish a bounded domain event. Publication is at least once and consumer deduplication uses event ID.

## Entity summary

| Entity | Key fields and constraints |
|---|---|
| User | unique email, bcrypt password hash, role in customer/admin/operator, token version, active flag |
| Passenger | owner user, validated display name, no government identifier |
| Station | unique normalized code, name, IANA timezone, active flag |
| Route | unique code, name, active flag |
| RouteStop | unique `(route_id, stop_index)` and `(route_id, station_id)`, non-negative offsets |
| Train | unique code, name, active flag |
| Coach | unique `(train_id, coach_number)`, bounded seat class |
| Seat | unique `(coach_id, seat_number)`, bounded seat type, active flag |
| TrainRun | train, route, service date, UTC departure, bounded status, positive immutable segment count |
| HotTrainPolicy | unique train run/class, enabled flag, bounded queue/rate/inflight/token/lease/entry limits, audit timestamps |
| WaitingRoomEntry | Redis-only entry ID, owner, run/class/journey/count fingerprint, monotonic sequence, bounded status and expiry |
| AdmissionToken | Redis-only SHA-256 token hash, entry binding, first-acquire booking/idempotency binding, bounded status and lease |
| Fare | run-or-route scope, `[from,to)` interval, class, non-negative minor amount, ISO-style currency; run-specific active fare takes precedence over route fallback |
| SeatInventory | `(train_run_id, seat_id)`, class snapshot, VARBIT occupancy, monotonic version |
| Reservation | owner, run, interval, class, state, expiry, checked total/currency |
| ReservationSeat | unique reservation/passenger and reservation/seat, exact mask and fare snapshot |
| TicketOrder | unique reservation, owner, state, checked total/currency |
| Ticket | unique ReservationSeat, unguessable unique code, active/cancelled state |
| IdempotencyRecord | unique user/operation/key hash, fingerprint, state, optional resource, retention expiry |
| OutboxEvent | aggregate metadata, bounded event type, versioned JSON payload, claim/retry state |

## Value objects

### SegmentMask

Variable-length bits in route order. It validates construction, length compatibility, final-byte padding, overlap, union, subtraction, equality, zero, and stable display. It has no PostgreSQL or pgx dependency.

### Money

`AmountMinor int64` plus normalized three-letter `Currency`. Addition/multiplication require compatible currency and reject overflow. No floating-point arithmetic enters domain or persistence code.

### StationCode and SeatClass

Station codes are normalized and validated once. SeatClass is a bounded enumeration (`standard`, `business`, `first`) shared by Fleet, Train Operations, and Booking domain packages without importing transport concerns.

### ServiceDate

An operator-local date plus route/operator IANA timezone; it is distinct from a UTC instant. Overnight route offsets may exceed 1,440 minutes.

## Invariants

1. A requested interval satisfies `0 <= from < to <= segment_count`.
2. The origin and destination stations occupy those ordered route positions.
3. Segment masks combined in any operation have identical positive length.
4. Every ReservationSeat has one unique owned passenger, one unique class-matching seat, and the reservation's exact interval mask; one passenger cannot have two held/confirmed reservations on the same train run.
5. All reservation seat allocations commit or roll back together.
6. Only held and confirmed ReservationSeats contribute to SeatInventory occupancy.
7. Confirmation retains occupancy; cancellation and expiration clear exact stored masks once.
8. Reservation totals equal checked sums of immutable fare snapshots.
9. Durable command completion and outbox creation commit with the domain mutation.
10. Reconciliation proves stored occupancy equals the active-mask union and that active masks do not overlap per seat.
11. One user has at most one active waiting-room entry per enabled train-run/class policy.
12. Admission permits one booking attempt; it never guarantees a seat or mutates `SeatInventory`.
13. Enabled hot-run booking fails closed when Redis admission state cannot be validated.
14. Durable per-user hold quotas are serialized and evaluated in the same PostgreSQL transaction before inventory mutation.

Passenger deletion is restricted once a ReservationSeat references the passenger. Train-run status advances only through `scheduled -> boarding -> departed -> completed`, with cancellation allowed before departure and terminal states unable to reopen. Cancellation stops new holds but does not infer mass cancellation or automatic release of existing held/confirmed reservations in Milestone 1.

## Dependency rule

HTTP transports parse and validate transport shapes, application modules orchestrate use cases, domain modules decide rules, and adapters handle PostgreSQL, Redis, Prometheus, or publication. Domain modules never import those technologies.
