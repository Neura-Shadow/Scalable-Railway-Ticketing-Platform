# Production Webhook Operations

## Acknowledgement contract

The public webhook path performs only a bounded ingress transaction:

1. accept TLS traffic for a configured provider and account;
2. read the exact raw body under a hard byte and time limit;
3. verify the provider signature using the provider-specific algorithm;
4. validate environment, account, event identity, and bounded type;
5. transactionally insert immutable normalized inbox evidence or an identity
   conflict record;
6. commit;
7. return 2xx only after that durable commit.

Signature failure, malformed or oversized content, wrong environment/account,
or database failure returns non-2xx. Business processing, provider queries,
ticket changes, refunds, and ledger posting never occur inline.

## Delivery and replay

Provider delivery is at-least-once and may be out of order. Durable provider
event identity plus normalized object/type identity protects replay; current
provider status and the common financial evaluator remain financial authority.
The ingress does not depend on Redis or a booking shard. A failover run switches
the single public route only after the target inbox is writable and verified.
Passive-region key distribution is verified by a valid signature reaching the
fenced persistence path and receiving 5xx; passive replicas never acknowledge a
normal delivery until promotion makes the control inbox writable.

Raw webhook bodies, authorization headers, signatures, secrets, and provider
response payloads are not stored. Logs and metrics use bounded provider,
event-category, outcome, and reason labels without event or customer IDs.

## Key lifecycle and regional readiness

Webhook secret bytes remain in the deployment secret source. Durable metadata
binds each provider/account/key ID to a one-way material proof, state, activation
time, demotion-based retirement deadline, and retirement time. There is exactly
one primary and no more than two live versions. Every Stripe request must match
exactly one configured secret and must then pass a current-database lifecycle
check; a stale replica cannot extend an expired grace period from memory.

Active rotation writes immutable transition audit rows carrying region and
epoch actor evidence. Eight recent retired versions remain in the hot table;
older versions are removed only after exact immutable archival. Passive and
recovery processes are read-only: readiness proves that their independently
provisioned ID-to-secret mappings equal replicated durable metadata. See
`docs/webhook-key-rotation.md` for the stage, demote, grace, retire, and regional
replay procedure.

## Failure handling

A committed duplicate returns success without duplicating work. The same event
identity with changed evidence is quarantined and acknowledged with 2xx after
that conflict commit; it never overwrites the original or repeats business work.
Workers lease inbox rows, query provider status for recognized financial events,
and finalize idempotently. Missing webhook recovery is provided by stale-state
provider reconciliation rather than assuming delivery is guaranteed.
