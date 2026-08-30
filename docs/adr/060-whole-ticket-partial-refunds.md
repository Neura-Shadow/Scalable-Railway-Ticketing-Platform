# ADR 060: Whole-Ticket Partial Refunds

- Status: Accepted for Milestone 7 implementation
- Date: 2026-08-11

## Context

Milestone 6 supports one full refund and cancels every ticket/seat in the order.
Milestone 7 must refund a selected set of complete tickets without weakening
the existing full-compensation uniqueness or accepting client-priced money.

## Decision

Add request-scoped partial-refund state rather than relaxing the Milestone 6
full-refund operation. The customer interface accepts an owned ticket-order ID,
a canonical selected ticket-ID set, and an idempotency identity. Amount,
currency, provider, fee, shard, fare, and masks are server-derived.

The current authoritative shard first locks reservation, order, selected
tickets, and reservation seats in deterministic order. It verifies owner,
cutoff, active state, immutable fare/currency, cumulative refund, region/epoch,
generation, and exact masks; moves only selected tickets to `refund_pending`;
and writes a prepare receipt, migration journal, and outbox without releasing
inventory.

Control creates one immutable request/items/saga/provider operation from the
verified receipt. Unknown provider outcome is queried before retry. Proven
success and its ledger posting authorize one selected-ticket apply command.
That shard transaction binds the prepare receipt, provider proof, exact set,
money, masks, route, region, and epoch; marks only selected tickets refunded;
releases only their segment masks; updates aggregates; and records one apply
receipt/journal/outbox. Replay returns the original result.

## Invariants

- Partial refund is whole-ticket only; no segment, passenger, exchange, fee, or
  client amount customization exists.
- Cumulative refund is monotonic and never exceeds captured amount.
- Unselected tickets and masks remain unchanged.
- Provider failure before refund permits an explicit receipt-bound abort; an
  unknown result retains `refund_pending` and inventory.

## Consequences

- Booking-shard schema v3 and physical migration/reverse validation must include
  every request/receipt/state/mask change.
- Full captured-but-unissued compensation remains its existing purpose-built
  path.
- Customer cancellation during issuance remains serialized behind receipt-
  backed issuance finalization.

## Rejected alternatives

- Add ticket filters to full compensation: rejected because it terminalizes the
  entire order/reservation and releases every seat.
- Loosen the one-full-refund index: rejected because it weakens proven M6 replay
  protection.
- Release masks before provider proof: rejected because the refund may fail or
  be unknown.

