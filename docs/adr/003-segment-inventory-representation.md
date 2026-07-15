# ADR 003: Represent Occupied Route Segments with PostgreSQL VARBIT

- Status: Accepted
- Date: 2026-07-15

## Context

A seat on `A -> B -> C -> D` can be sold for `A -> C` and `C -> D`, but not for `A -> C` and `B -> D`. With N route stops there are N-1 directed segments. A request occupies stop indices `[from_stop_index, to_stop_index)`.

Milestone 1 must support routes longer than 64 segments and must not collapse inventory to a quantity.

## Decision drivers

- Correct overlap detection in one atomic predicate.
- All-or-nothing multi-seat mutation with deterministic row locks.
- Storage proportional to seats and route length.
- Explicit mask-length validation.
- A domain representation independent from PostgreSQL encoding.

## Options considered

### PostgreSQL VARBIT per seat and train run

One row stores the occupied bits for a physical seat on a train run. Intersection, union, and complement are native operations. One row lock serializes all journeys that contend for a seat, while non-overlapping journeys can update that same row sequentially and both commit.

Benefits: compact storage, one atomic overlap predicate, bounded row count, and natural reconciliation. Costs: one hot row per popular seat, careful bit ordering/encoding, and equal-length requirements for bitwise operators.

### One row per seat segment allocation

Each active segment becomes a row protected by uniqueness. This makes overlap intuitive, but a long journey multiplies rows and locks by segment count. Multi-passenger transactions create many insert/delete operations and reconciliation joins. Storage and index churn are larger.

### Range or exclusion constraints

An integer range per seat allocation plus a GiST exclusion constraint can reject overlapping intervals elegantly. It represents a single contiguous journey well, but temporary lifecycle state, exact releases, active-status filtering, multi-seat selection, and reconciliation require additional rows/indexes and subtle partial-constraint behavior. It also does not provide the compact per-seat occupancy snapshot desired by Milestone 1.

## Decision

Use PostgreSQL `BIT VARYING` (`VARBIT`) in `seat_inventory.occupied_segments` and `reservation_seats.segment_mask`.

Bit ordering is route order: segment index 0 is the leftmost bit. The Go domain value's bit index 0 maps to the high bit of `Bytes[0]`, matching pgx `pgtype.Bits` and PostgreSQL `get_bit(..., 0)`, whose first bit is leftmost.

Examples for `A -> B -> C -> D`:

```text
A -> C = 110
B -> D = 011
C -> D = 001
```

The domain `SegmentMask` owns:

```text
NewSegmentMask(segmentCount, fromIndex, toIndex)
Overlaps
Union
Subtract
IsZero
BitLength
Equal
String
```

Operations reject different lengths. `Subtract` means `left AND NOT right` and preserves unrelated bits. The implementation uses `[]byte` plus an explicit bit length; unused low bits in the final byte are always zero and validated.

The pgx adapter converts only through `pgtype.Bits{Bytes, Len, Valid}` and validates both directions. Outbound values require `Len > 0`, `len(Bytes) == (Len+7)/8`, `Valid`, and zero unused low bits in the final byte. PostgreSQL's official pgx type represents `bit`/`varbit` as bytes plus a bit count, so no text parsing is required.

The allocation predicate is:

```sql
WITH p AS MATERIALIZED (
    SELECT $3::varbit AS mask
)
UPDATE seat_inventory AS si
SET occupied_segments = si.occupied_segments | p.mask,
    version = si.version + 1,
    updated_at = clock_timestamp()
FROM p
WHERE si.train_run_id = $1
  AND si.seat_id = $2
  AND p.mask IS NOT NULL
  AND p.mask <> repeat('0', bit_length(p.mask))::varbit
  AND CASE
        WHEN bit_length(si.occupied_segments) = bit_length(p.mask)
        THEN (si.occupied_segments & p.mask) =
             repeat('0', bit_length(p.mask))::varbit
        ELSE false
      END
RETURNING si.occupied_segments, si.version;
```

The `CASE` is required because PostgreSQL does not promise left-to-right evaluation of `AND` terms, and unequal bitwise operand lengths raise an error. The zero mask is generated from the requested length instead of being a caller parameter.

The release expression is `occupied_segments & (~$3::varbit)`, guarded by the same `CASE` length check, an `(occupied_segments & mask) = mask` subset check, and the locked reservation's active transition. It reads the immutable mask from `reservation_seats`; it never accepts a caller-invented release mask. A terminal repeat performs no inventory update, preventing a stale duplicate release from clearing a segment that has since been reallocated.

For multi-seat selection, a materialized CTE selects deterministic candidates with `LIMIT $count FOR UPDATE OF si SKIP LOCKED`, then an `exact` CTE gates the update on `count(*) = $count`. The caller also verifies the returned row count and rolls back if it differs from the passenger count. `SKIP LOCKED` is acceptable only for candidate allocation because an incomplete batch becomes a distinct bounded contention/availability conflict; it is not used for availability reporting or general reads. The hot-train load scenario measures this conservative false-scarcity tradeoff.

`seat_inventory` duplicates immutable `segment_count`, enforces `CHECK (bit_length(occupied_segments) = segment_count)`, and references a unique `(train_run_id, segment_count)` pair. This makes inventory length declarative. Reservation-seat length still requires a deferred constraint trigger because the train run is reached through its reservation.

## Official references

- [PostgreSQL bit string operators](https://www.postgresql.org/docs/17/functions-bitstring.html): `&`, `|`, and XOR require equal-length inputs; `~` preserves the bit string, and bit index 0 is leftmost.
- [PostgreSQL bit string types](https://www.postgresql.org/docs/16/datatype-bit.html): `BIT VARYING` stores variable-length bit strings; explicit bounded casts may truncate and therefore are avoided.
- [PostgreSQL SELECT locking clause](https://www.postgresql.org/docs/17/sql-select.html#SQL-FOR-UPDATE-SHARE): `SKIP LOCKED` provides an inconsistent view appropriate to queue-like/candidate work, not general reads.
- [PostgreSQL UPDATE](https://www.postgresql.org/docs/current/sql-update.html): bounded update-through-CTE and deterministic ordering are supported patterns.
- [PostgreSQL expression evaluation](https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-EXPRESS-EVAL): Boolean subexpression evaluation order is not defined, so `CASE` guards unequal-length bitwise operations.
- [PostgreSQL data-modifying CTEs](https://www.postgresql.org/docs/current/queries-with.html#QUERIES-WITH-MODIFYING): sibling data modifications share a snapshot and must communicate through `RETURNING`; the allocation design uses a locking `SELECT` CTE feeding one update.
- [pgx pgtype.Bits](https://pkg.go.dev/github.com/jackc/pgx/v5/pgtype#Bits): pgx exposes the byte slice, explicit bit length, and validity required by the adapter.

## Consequences

- Routes are not limited to 64 segments.
- PostgreSQL remains responsible for atomic overlap checks; the Go value object provides the same algebra for validation and tests.
- Exact bit ordering and mask-length tests become release gates.
- Popular seats can be hot rows; selection distributes allocation deterministically across available seats, and Milestone 2 may revisit hot-run protection without changing the authority hierarchy.
