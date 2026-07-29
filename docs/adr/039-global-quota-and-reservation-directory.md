# ADR 039: Global Quota Is Conservative and Reservation Directory Is Repairable

- Status: Accepted for Milestone 5 implementation
- Date: 2026-07-29

## Context

Milestone 4 updates the global reservation quota claim and reservation locator
in the same transaction as local reservation state. Across physical databases,
control cannot know atomically whether a shard committed. Releasing quota on an
ambiguous timeout can permit the user-wide limits to be exceeded; activating a
directory before a local commit can expose a nonexistent reservation. Waiting
for certainty may instead reject safe work temporarily.

Migration also makes a locator's stored shard and generation stale by design.
Rewriting every directory row atomically with a physical cutover is neither
required nor safe. A missing or stale directory must not trigger an unbounded
scan across shard databases.

## Decision

### Conservative global quota leases

Replace physical-path quota claims with control-owned
`booking_quota_leases`. A lease is uniquely linked to one booking command and
contains globally unique lease identity, owner, train run, passenger count,
state, expiry, and bounded timestamps. States include `pending`, `active_hold`,
`released`, `expired`, and `repair_required`.

The control transaction serializes quota decisions for one canonical user by
using the same stable PostgreSQL advisory-lock namespace as legacy and logical
booking paths. Active `reservation_quota_claims` and physical `pending` or
`active_hold` leases count together against per-user,
per-train-run, and passenger limits. Concurrent commands therefore cannot
exceed the configured limits before contacting separate paths or shards. When
reverse-migration overlap exposes both representations for one reservation,
reservation identity de-duplicates the count conservatively.

Shard commit converts the pending lease to `active_hold` during idempotent
control finalization. Cancellation and expiration release it from durable
shard events or reconciliation. A delayed event may cause a safe false
rejection. Redis state, request timeout, a stale directory hint, or uncertain
assignment never authorizes release. Shard outage may temporarily over-reserve
quota but must never undercount pending or active work.

After an execution lease expires, release is allowed only when the reconciler
can prove no committed receipt/resource exists or can verify a permanent shard
failure outcome. If the assigned shard is unreachable, the quota lease remains
conservative and enters a repairable state rather than being guessed away.

Reconciliation compares commands, leases, local active holds, released local
reservations, passenger counts, missing or duplicate leases, and state
transitions. It is idempotent and detect-first; it never changes seat inventory.

### Stable, repairable reservation directory

Evolve the global locator into a control-owned reservation directory keyed by
the globally unique reservation ID. Its authoritative information is train run,
owner, command, state, and bounded timestamps. States include `pending`,
`active`, `failed`, `moving`, and `tombstoned`.

A stored shard ID and generation are last-known routing hints only. Lookup is:

1. resolve reservation ID to train run and owner in the directory;
2. enforce early owner rejection without treating the directory as final
   authorization;
3. resolve the train run's current control assignment;
4. access exactly one approved shard; and
5. recheck resource existence and owner in authoritative local state.

The control reservation transaction creates `pending` before shard execution.
The committed shard receipt authorizes idempotent transition to `active`.
Pending state retains enough command identity to route bounded retry or repair.
A proven permanent failure transitions it to `failed`; missing or malformed
state fails safely and never scans all shards.

Physical migration may mark affected directory state `moving`, but ordinary
routing follows the current train-run assignment rather than mass-rewriting
every reservation's shard hint. Stale hints are refreshable. Operator repair is
explicit, bounded by resource or train run, owner-safe, audited, and cannot
expose physical topology through the public interface.

Ticket and ticket-order lookup retain a documented control-owned directory or
global read projection with globally unique IDs and owner-safe routing. Their
last-known storage fields are likewise hints, not authority.

## Invariants

- Pending plus active leases never exceed configured global quota at the moment
  a new lease is granted.
- Uncertainty may reduce availability but cannot free quota prematurely.
- An active directory entry is justified by a committed shard receipt and
  corresponding local resource.
- A pending directory entry never claims that booking succeeded.
- Directory corruption or absence never triggers topology-wide probing.
- Current assignment plus database-local verification decides the read target;
  a stale hint cannot grant write or read authority.
- Public responses and tokens do not expose shard identity, generation,
  connection reference, or database location.

## Consequences

- User-wide quotas remain safe across two independently committing booking
  shards at the cost of temporary false rejection during uncertainty.
- Reservation lookup remains bounded to control plus one current shard.
- Physical cutover no longer requires a dangerous all-or-nothing directory
  rewrite; train-run assignment remains the routing authority.
- Control finalization and event delay become observable repair states.
- The quota and directory modules require reconciliation indexes, bounded
  leases, metrics, and operator inspection.
- These protocols preserve compatibility with legacy/logical routes while
  making physical partial failure explicit.

## Rejected alternatives

- Eventually consistent counters that authorize holds: rejected because lag
  can undercount concurrent work on different shards.
- Redis-authoritative quota: rejected because Redis cannot commit with control
  leases or shard reservations and may lose state.
- Release on HTTP timeout or lease expiry alone: rejected because the shard may
  have committed before the response was lost.
- Store shard ID as final directory authority: rejected because migration makes
  it stale and local fencing plus current assignment decide authority.
- Scan all shards on a missing directory entry: rejected because work and
  information disclosure would grow with topology and could hide corruption.
- Make directory activation precede shard commit: rejected because it could
  assert a resource that does not exist.
