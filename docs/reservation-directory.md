# Reservation Directory

## Purpose and authority

`reservation_directory` is the stable control-plane lookup from a globally
unique reservation ID to owner and train run. It prevents cross-shard scans and
allows authorization before a booking database is contacted. Its shard ID and
generation are refreshable route hints, not independent write authority.

States are `pending`, `active`, `failed`, `moving`, and `tombstoned`.
Creation starts as `pending` in the command reservation transaction and becomes
`active` only after a durable shard success receipt. Existing Milestone 4
locators are backfilled losslessly as active legacy-imported entries by control
migration 9.

## Lookup

1. Read the directory row by reservation ID.
2. Verify current-user ownership before returning protected state.
3. Resolve the train run's current control assignment.
4. Refresh a stale directory route hint when permitted.
5. Read exactly one authoritative shard and verify the local row.

A missing, pending, contradictory, or stale row is an explicit bounded error;
the API must not fan out across shards. Migration may mark the hint `moving`,
but authority continues to come from the assignment plus local fence. No PII is
duplicated into shard routing metadata.

The reconciler compares directory, command, shard receipt, and local resource
identity. It repairs control metadata only when durable evidence is decisive;
it never fabricates a successful booking. See [ADR 039](adr/039-global-quota-and-reservation-directory.md).
