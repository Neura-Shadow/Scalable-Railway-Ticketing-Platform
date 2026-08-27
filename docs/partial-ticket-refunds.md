# Whole-Ticket Partial Refunds

## Customer contract

A customer may request a refund for one or more complete active tickets in one
owned reservation. The request contains only selected ticket UUIDs plus the
standard `Idempotency-Key` header. It cannot supply amount, currency, fee,
exchange rate, provider refund ID, shard identity, or routing generation.

The server derives ownership, active ticket state, immutable fare snapshots,
currency, departure cutoff, current physical assignment, captured and already
refunded totals, and the request fingerprint. Every selected ticket must be
active, owned, same-currency, strictly positive value, before the configured
cutoff, and not already participating in another active refund. The provider
must advertise partial-refund support. Milestone 7 has no cancellation fee and
does not refund an arbitrary portion of one ticket.

## State and uncertainty

The control plane owns the refund request, immutable items, saga, provider
operation, and review evidence. Same owner and idempotency identity with the
same fingerprint replays; a different selection conflicts. Provider uncertainty
retains every selected ticket and seat mask. Cumulative refunds cannot exceed
the captured amount.

After provider success, one current-shard command locks the reservation and
selected tickets in deterministic order, verifies regional and train-run
generation fences, exact request/proof/money, writes an immutable receipt,
transitions only selected tickets, releases only their segment masks, updates
the order/reservation aggregate, emits outbox facts, and commits atomically.
Unselected tickets and seat masks remain unchanged.

If active tickets remain, the reservation becomes `partially_refunded`; if all
tickets are refunded, it becomes `cancelled`. A shard-commit/control-finalize
failure is repaired only from the immutable receipt and recorded command.
