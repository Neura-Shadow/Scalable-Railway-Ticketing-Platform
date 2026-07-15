# Segment-Based Seat Inventory

## Route-to-mask mapping

For `A -> B -> C -> D`, the segments are:

| Bit index | Segment |
|---:|---|
| 0 | A-B |
| 1 | B-C |
| 2 | C-D |

Bit index 0 is the leftmost stored/display bit. A request sets bits from `from_stop_index` through `to_stop_index - 1`:

```text
A -> C = 110
B -> D = 011
C -> D = 001
```

Thus A-C overlaps B-D, while A-C does not overlap C-D.

## Domain representation

The Go `SegmentMask` stores a byte slice and explicit bit length. It supports arbitrary positive lengths, including more than 64 segments. Unused low bits in the final byte are zero.

The module exposes construction and algebra while hiding byte layout. Different-length masks return a typed mismatch error; they are never padded implicitly.

## PostgreSQL representation

`seat_inventory` stores one `VARBIT` occupancy mask per `(train_run_id, seat_id)`. `reservation_seats` stores the exact requested mask for release and reconciliation. A constraint trigger validates both lengths against `train_runs.segment_count`.

Allocation uses a `CASE`-guarded equal-length bitwise intersection and union in one conditional update. SQL generates its own zero mask and rejects a zero request. Release uses `occupied & ~exact_reserved_mask` only after the locked active reservation transition and a subset check. The adapter binds/scans validated `pgtype.Bits`, preserving bytes and length.

## Multi-seat selection

One booking transaction:

1. validates run, route interval, class, fare, passengers, and idempotency;
2. derives one requested mask;
3. selects available class-matching seats in coach/seat/ID order with row locks and `SKIP LOCKED`;
4. gates the update on the candidate count exactly matching the passenger count;
5. independently verifies the returned count;
6. inserts the reservation and immutable reservation-seat snapshots;
7. completes idempotency and inserts the outbox event; and
8. commits, or rolls back everything on any shortfall.

`SKIP LOCKED` may yield a conservative conflict while another transaction holds seats. It cannot yield partial success because row-count mismatch aborts the transaction.

## Reconciliation

For each train run and seat:

```text
seat_inventory.occupied_segments
==
bitwise union of reservation_seats.segment_mask
where reservation.status in (held, confirmed)
```

The verifier additionally checks that active masks for the same seat never overlap each other. It runs after concurrency suites and is an acceptance gate.

## Alternatives

See ADR 003 for the comparison with one-row-per-seat-segment and range/exclusion designs. Quantity inventory is not a valid alternative for this domain.
