# Payment Webhooks

## Endpoint boundary

`POST /webhooks/payments/:provider` is intentionally outside customer JWT
authentication. Authentication is the configured provider allowlist plus a
timestamped message authentication code. The endpoint's only business action
is to store verified bounded evidence in control PostgreSQL; ticket issuance,
provider queries, and saga transitions run asynchronously.

## Request controls

The handler applies this order:

1. reject a provider name outside the bounded configured allowlist;
2. enforce the exact supported content type, request deadline, and strict body
   limit before parsing;
3. select an accepted key by bounded key ID without exposing key existence;
4. parse a bounded timestamp and reject values outside configured clock skew;
5. compute HMAC-SHA256 over the documented timestamp/raw-body framing and use
   constant-time comparison;
6. compute a cryptographic payload hash;
7. parse only bounded normalized event ID/type, provider payment ID, provider
   event time, status, amount, and currency;
8. insert the inbox record in one transaction, commit, and return promptly.

Keyrings support overlapping accepted key IDs for rotation. Unknown IDs,
malformed encodings, expired timestamps, bad signatures, oversized bodies, and
unsupported content types are rejected with bounded responses. Raw body,
signature, secret, credential, or verification detail is never logged.

## Inbox idempotency and conflicts

The database enforces unique `(provider, provider_event_id)`.

- Same event ID and same payload hash returns success and increments only a
  bounded duplicate metric. It does not enqueue a second logical application.
- Same event ID and a different hash creates a
  `payment_provider_event_conflict` record and security metric, returns the
  documented bounded conflict response, and requires operator review. Neither
  version advances payment state automatically.
- A validly signed but unknown event type is stored as `ignored` or
  `unsupported`, returns success to stop endless provider retries, and never
  advances the saga.

The preferred inbox record stores normalized fields and the payload hash, not
the full body. If a future provider requires evidence retention, raw payload
storage needs separate encryption, access, retention, privacy, and incident
review; it is not implied here.

## Processing

The payment worker claims inbox rows in bounded batches using short leases and
`FOR UPDATE SKIP LOCKED`, commits the claim, then applies a normalized event
through expected-state transitions. `RunOnce(ctx)` is deterministic and safe
across replicas. Success, retryable failure, permanent unsupported state, and
manual-review escalation are durable.

HTTP acknowledgement means only that authentic bounded evidence is durable. It
does not mean the event has been applied, the provider has been queried, a
payment is captured, or a ticket is issued.

## Duplicate and out-of-order semantics

Provider event ID is inbox idempotency; event timestamp is not local ordering
authority. A stale event never regresses a terminal or more authoritative
state. An event that conflicts with the current transition schedules a provider
status query using the known provider payment ID.

- `completed` never regresses to `awaiting_customer`.
- `refunded` never regresses to `captured`.
- `voided` cannot become `captured` without a distinct verified provider
  operation.
- Amount/currency mismatch always enters manual review.
- Capture/refund conflict requires current provider confirmation.

The provider response, query, and webhook may race. All converge through the
same intent/operation expected-state and immutable amount/currency checks.

## Failure behavior

Control PostgreSQL unavailability fails webhook persistence; it is preferable
for the provider to retry than for the handler to acknowledge undurable data.
A booking-shard outage does not prevent a verified event from entering the
control inbox. Its processing is retried and may escalate while healthy shards
continue. A worker crash leaves a reclaimable lease and no inline HTTP side
effect.

## Observability

Metrics include bounded totals for accepted, invalid signature, duplicate,
changed-payload conflict, processing result/duration, and event lag. Labels are
bounded provider/result/reason/state only. Event IDs, payment IDs, signatures,
owners, endpoints, and payload contents are not labels or log fields.
