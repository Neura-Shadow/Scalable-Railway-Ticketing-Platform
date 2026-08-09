# Payment Under Physical Sharding

## Split of authority

Control PostgreSQL owns provider-facing orchestration: payment intents, sagas,
operations, webhook inbox/conflicts, reconciliation checkpoints, manual review,
and control events. It stores stable reservation and train-run identities but
does not bind an intent to a physical shard.

The current authoritative booking shard owns every mutation that must remain
atomic with inventory: reservation payment state and grace deadline, exact seat
masks, ticket order/tickets, financial command snapshots, payment command
receipts, issuance/refund/compensation receipts, local idempotency results, and
shard-local outbox events. No cross-database foreign key or transaction exists.

## Command routing and fencing

Every begin-payment, issuance, refund-pending, and compensation command:

1. resolves the reservation directory and current train-run assignment;
2. obtains a bounded configured `ShardHandle`, never a caller-supplied endpoint;
3. begins one local transaction;
4. validates storage kind, shard identity, generation, protocol/schema version,
   migration permission, and the database-local write fence;
5. locks and validates the reservation and immutable command fingerprint;
6. commits local mutation, globally unique receipt, and outbox event together.

A stale route fails before mutation. The coordinator may use the existing one
bounded assignment refresh, then replay the same command. It never routes
randomly, scans all shards, or creates a new provider operation because routing
changed.

## Booking-shard schema version 2

Version 2 extends reservations with constrained `payment_pending`,
`payment_review`, and `refund_pending` states, `payment_intent_id` snapshot and
`payment_grace_expires_at`; it extends ticket order/ticket payment states and
opaque ticket codes; and it adds payment command, ticket issuance, and
refund/compensation receipts plus required local financial snapshots.

Cross-plane identifiers are application-generated globally unique values. The
`payment_intent_id` has no cross-database foreign key. Constraints prevent
duplicate begin-payment, confirmation, ticket order, ticket per reservation
seat, issuance receipt, refund completion, and ticket code. Existing booking
rows remain valid after the populated v1-to-v2 migration.

## Migration coverage

Forward and reverse physical migration includes:

- reservation status, intent snapshot, immutable money, and grace deadline;
- ticket-order payment status and all tickets/codes;
- payment command, issuance, refund and compensation receipts;
- relevant local idempotency results and exact inventory relationships;
- shard-local outbox rows and event identities; and
- mutation journal entries/apply receipts for every payment-state mutation.

Base copy uses deterministic dependency ordering. Source-local mutation capture
commits with each payment/ticket mutation. Replay is receipt-idempotent.
Validation compares identities, fingerprints, money, states, seat/ticket
relationships, receipts, and outbox events, not just row counts. Cutover retains
the existing source-off/target-on/generation ordering and never enables two
writers.

## Overlap scenarios

- **Awaiting customer / authorized:** state and receipts copy; subsequent
  capture is control-local and its shard command resolves the post-cutover
  target.
- **Capture pending/unknown:** control operation identity is unchanged; migration
  cannot cause a second provider call. Status is queried before retry.
- **Captured before issuance:** durable captured proof stays in control;
  `payment_pending` and receipt state move; issuance occurs once on target.
- **Refund pending:** control refund remains stable; local pending state and
  compensation receipt move; seat stays retained until proof.
- **Issued ticket:** order, tickets, codes, issuance receipt, exact masks, and
  outbox identities survive forward and reverse migration.
- **Stale worker after cutover:** source fence rejects it; refresh routes the
  same command to target.

Required deterministic tests cover migration while awaiting customer,
authorized, capture pending, captured-before-issue, refund pending, reverse
migration after issue, and stale route execution.

## Failure isolation

One booking-shard outage delays only its routed intents and cannot corrupt or
starve a healthy shard. Verified webhooks can still persist in control. Control
outage prevents new operations/finalization but does not invalidate shard
receipts or tickets. Provider outage affects control operations, not shard
authority. No dependency failure triggers random shard fallback or inventory
release.

## Events

Local events such as `reservation.payment_pending`,
`reservation.confirmed`, `reservation.refund_pending`,
`reservation.cancelled`, `ticket_order.issued`, `ticket.issued`,
`ticket.cancelled`, and `payment.compensation_applied` commit with their local
mutation. Control payment events commit with control state. Delivery remains at
least once with globally unique IDs and idempotent consumers; migration
preserves event identity and does not claim exactly-once delivery.
