# Regional Write Fencing

Every authoritative database has one durable regional authority row containing
bounded region, role, monotonic epoch, writes-enabled state, and update evidence.
All control mutations enter one control writer; all shard mutations enter one
shard writer that verifies regional authority before the existing train-run
generation fence. The authority row and affected domain rows are locked and
validated in the same transaction.

Configuration supplies the expected region and epoch. A passive, recovery,
disabled, stale, or future mismatch fails before mutation with a bounded safe
error. A repository guard prevents new production callers from beginning write
transactions outside the deep writer modules.

This database check cannot fence an isolated old primary that retains its own
stale row. External fencing of old ingress, processes, credentials, and network
access is therefore a mandatory, independently attested precondition. Promotion
without valid external evidence is rejected. Epoch increments once per
successful authority transfer and never rolls back or reuses a value.

Attestations bind operation ID, source/target region, source/target epoch,
incident/operator identity, observed database positions, fence mechanism,
timestamp, and content hash. They contain no secret or arbitrary command. The
DR runner accepts only the fixed database set and typed phases; it is not a
generic remote-execution engine.
