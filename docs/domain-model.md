# Domain Model

Milestone 1 is a production-minded, single-region railway booking backend. PostgreSQL is authoritative for train-run status, seat occupancy, reservation lifecycle, tickets, durable idempotency, and outbox events.

## Bounded contexts

| Context | Responsibilities | Owned concepts | Dependencies |
|---|---|---|---|
| Accounts | Credentials, JWT lifecycle, RBAC, customer ownership | User, Passenger | Platform clock/config; PostgreSQL adapter |
| Railway Offering | Topology, rolling stock, fares, dated services, bookable status, commissioning | Station, Route, RouteStop, Train, Coach, Seat, SeatClass, TrainRun, Fare | PostgreSQL adapter |
| Booking | Atomic occupancy/allocation, lifecycle commands, tickets, idempotency, reconciliation | SegmentMask, SeatInventory, Reservation, ReservationSeat, TicketOrder, Ticket, IdempotencyRecord | Accounts ownership port; Railway Offering sales-input port; event append port |
| Query (supporting) | Browse/search/availability hints and Redis decorators | Read models only | Read-only PostgreSQL and Redis adapters |
| Event Relay (supporting) | Claim, publish, retry, recover, finalize | OutboxEvent delivery state | PostgreSQL and publisher adapters |
| Platform (supporting) | Technology adapters and process lifecycle | Config, database/Redis pools, middleware, response, metrics, clock | No railway business rules |

Contexts are package and ownership groupings, not network services. Booking owns every reservation transaction. Transactional outbox insertion is a Booking/Railway Offering command responsibility; Event Relay owns only asynchronous delivery.

## Aggregate and transaction ownership

### Route

A Route owns the ordered RouteStop definition. Stops are contiguous and zero-based, stations do not repeat, and arrival/departure offsets are non-decreasing. N stops define N-1 segments.

### Train and seating plan

A Train owns Coaches; a Coach owns Seats. Coach numbers are unique per train and seat numbers are unique per coach. A seat's sellable class is inherited from its coach and its active status must be true when inventory is initialized.

### TrainRun and SeatInventory

A TrainRun binds one Train and Route to an operator-local service date and UTC schedule. Its copied `segment_count` is immutable after inventory initialization. Only `scheduled` is bookable by default. Railway Offering's `CommissionTrainRun` creates the run and every Booking SeatInventory row in one transaction before exposing it as bookable. SeatInventory has the primary key `(train_run_id, seat_id)` and an occupied mask whose bit length equals `segment_count`.

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
4. Every ReservationSeat has one unique owned passenger, one unique class-matching seat, and the reservation's exact interval mask.
5. All reservation seat allocations commit or roll back together.
6. Only held and confirmed ReservationSeats contribute to SeatInventory occupancy.
7. Confirmation retains occupancy; cancellation and expiration clear exact stored masks once.
8. Reservation totals equal checked sums of immutable fare snapshots.
9. Durable command completion and outbox creation commit with the domain mutation.
10. Reconciliation proves stored occupancy equals the active-mask union and that active masks do not overlap per seat.

Passenger deletion is restricted once a ReservationSeat references the passenger. Train-run cancellation stops new holds but does not infer mass cancellation or automatic release of existing held/confirmed reservations in Milestone 1.

## Dependency rule

HTTP transports parse and validate transport shapes, application modules orchestrate use cases, domain modules decide rules, and adapters handle PostgreSQL, Redis, Prometheus, or publication. Domain modules never import those technologies.
