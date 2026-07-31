# Shard-Local Outbox

## Transaction boundary

An authoritative booking mutation and its event intent commit in the same
physical-shard transaction. Booking-shard schema version 1 stores globally
unique event IDs, aggregate identity, train run, generation, bounded event
type/version, payload, publication state, attempt count, and lease metadata.
Database constraints reject oversized objects and sensitive payload keys.

Delivery is at least once. A relay may publish a row more than once around a
crash, so consumers deduplicate by global event ID and apply resource versions
monotonically. This is not exactly-once distributed processing.

## Relay policy

The relay enumerates only configured shard handles in stable fair rotation. It
uses per-shard batch, lease, statement, connection, retry, and total concurrency
bounds. A failed shard records an independent error/backlog and cannot starve a
healthy shard. Lease recovery is idempotent; raw payloads and credentials are
not printed by status commands.

Migration drains or accounts for source events and preserves event identities.
Replaying a journal entry cannot invent a second logical event. The global read
model remains a non-authoritative projection and reloads current routed state
when an event indicates a stale generation.

Migration copies outbox rows from one read-only repeatable-read source snapshot
in batches of at most 64 rows and 5 MiB, with a 64 MiB total snapshot cap. Each
batch enters migration-specific
target staging in a short transaction. Only after the full bounded snapshot is
captured does one target transaction replace the selected generation and
promote staged rows; a failed promote leaves the previous authoritative target
outbox intact. Transient relay lease fields are reset during normalization.
All concurrent migration staging on one target shard is serialized only around
its short staging transactions and constrained to a 256 MiB aggregate cap. An
abandoned migration can therefore consume bounded capacity but cannot grow the
table without limit; operators must verify the migration is inactive in the
control ledger before deleting its UUID, because age alone is not proof that a
slow migration has stopped.

See [ADR 041](adr/041-shard-local-outbox-and-event-relay.md) and
[the failure policy](physical-shard-failure-policy.md).
