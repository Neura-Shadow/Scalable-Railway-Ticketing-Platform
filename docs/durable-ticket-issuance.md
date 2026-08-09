# Durable Ticket Issuance

## Authority and preconditions

Tickets are issued only on the current authoritative physical booking shard
after control PostgreSQL durably records a captured provider result with exact
reservation amount and currency. A browser callback, unprocessed webhook,
authorization alone, or in-flight capture response is insufficient.

The issuance command is application-generated, globally unique, immutable, and
stable across retries. It contains bounded snapshots of payment intent,
reservation, capture operation, amount, and currency, but no provider secret,
raw webhook, passenger PII, or physical endpoint.

## One local transaction

The coordinator resolves the current assignment before execution. The shard
transaction then:

1. validates storage identity and current generation fence;
2. locks the reservation and its reserved seats in deterministic order;
3. verifies `payment_pending` or an explicitly issuable `payment_review` state;
4. verifies owner, payment intent, capture operation, amount, and currency
   against immutable local command snapshots;
5. acquires the unique payment-command receipt and issuance receipt identity;
6. locks or creates the unique ticket order for the reservation;
7. changes the reservation to `confirmed` and order to `issued`;
8. creates or activates exactly one ticket per reservation seat, with an opaque
   globally unique code unrelated to sequential IDs or passenger data;
9. stores the issuance receipt with order ID and issued-ticket count;
10. writes `payment.captured`, `reservation.confirmed`,
    `ticket_order.issued`, and bounded `ticket.issued` local outbox intents; and
11. commits all changes atomically.

No control query or provider call occurs inside this transaction. A constraint
on reservation/order/seat identity plus unique issuance/command receipts and
ticket codes prevents duplicate order, ticket, confirmation, or receipt.

The verified issuance receipt carries every immutable ticket ID/code pair to
control. A single control transaction inserts the unique route-independent code
claim with the ticket locator before completing the saga. Forward and reverse
migration therefore update only the locator; the code claim remains stable.

## Replay behavior

A repeat with the same command identity and fingerprint returns the existing
ticket order, tickets, and committed receipt. It emits no new logical event.
A reused identity with different intent, reservation, capture, money, seat set,
or fingerprint conflicts without mutation. At-least-once relay may redeliver
the same globally unique outbox event; consumers deduplicate by event ID.

If the local commit succeeds but its response is lost, a retry first loads the
receipt. It never confirms again. If the local commit succeeds but control
finalization fails, tickets remain authoritative and owner-readable through the
current routed shard; control reconciliation validates the receipt and
finalizes without another capture or issuance.

## Failure after capture

A retryable failure before local commit retains `payment_pending` or
`payment_review`, keeps the seat, and retries the same command. A permanent
invariant failure moves the saga to compensation. It must not silently abandon
a captured payment or release inventory.

Compensation performs a full idempotent refund (or proves a valid pre-capture
void), then one shard-local command cancels the reservation/order/tickets and
releases exact masks. An unknown refund outcome stays under review with the seat
retained. There is no partial ticket issuance: transaction rollback leaves no
active ticket and no confirmation.

## Migration and routing

The issuance command never stores a physical shard binding. During forward or
reverse migration it resolves current assignment, and the shard fence rejects
stale execution before mutation. Base copy, journal replay, validation, and
reverse migration include orders, tickets, payment command receipts, issuance
receipts, relevant local idempotency, and outbox event identities. A
captured-but-not-issued reservation can move and then issue once on the target.

## Retrieval

`GET /api/v1/ticket-orders`, `GET /api/v1/ticket-orders/:id`, and
`GET /api/v1/tickets/:id` use authenticated ownership and the stable global
directory/read model to route to one current shard. They never scan all shards,
accept a ticket code as ownership proof, or expose provider operations,
credentials, shard identity, or more passenger data than the approved existing
contract. Refunded/cancelled state is visible. Offline verification, PDFs,
email, SMS, and signed boarding credentials are outside Milestone 6.
