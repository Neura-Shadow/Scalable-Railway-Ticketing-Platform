# Regional Failover

## Preconditions

Failover is operator-controlled. Before advancing, the runner requires a stable
operation identity, fixed source and target regions, independently protected
external-fence evidence, the source WAL positions, target replay positions,
schema compatibility, backup/archive health, and a passive target. If the old
region is not fenced, the operation cannot promote.

Fence evidence is an Ed25519-signed, operation/incident/source-epoch/purpose-bound
attestation with a purpose-scoped unique nonce and a maximum ten-minute lifetime. The
signed purpose is one of `initial_fence`, `ongoing_source_fence`,
`retained_source_fence`, or `failback_validation`; relabeling a valid record
for another phase is rejected. `dr-admin`
loads the expected issuer, key ID, and raw public key from independent runtime
configuration; missing configuration, a forged signature, an expired record,
or reuse of the initial nonce for the retained-source proof fails closed. The retained
proof must also be observed strictly after both the initial proof and the durable
complete-set activation timestamp.

Expiry is rechecked from the independently configured verifier clock every
time `advance-phase` loads a checkpoint and immediately before the runner
promotes a database or changes an authority row. A long-running operation must
obtain an `ongoing_source_fence` attestation and persist it through the
same-stage `refresh-fence` CAS command. An expired durable proof is never
grandfathered merely because it was valid when first journaled.

## Ordered phases

The durable fixed state machine verifies fencing, records replication
positions, disables target write readiness, promotes control then shard 0 then
shard 1, verifies roles and timelines, advances the regional epoch, updates
authority rows, resets pools, starts recovery-mode APIs, runs control/shard/
payment/ticket/ledger/settlement reconciliation, activates both shard
authorities and then control as the final complete-set marker, enables workers,
starts the regional proxy, switches the
single webhook/global ingress, configures customer writes behind the readiness
gate, atomically marks the target active with the durable phase CAS, verifies
customer-write readiness, records RTO and RPO, and retains the source fence.

Recovery mode never starts mutating workers or the regional proxy. Before the
activation commit, the evidence runner explicitly verifies those services are
not running. API `/livez` remains available for bounded recovery observation;
API and worker `/readyz` require matching active authority and primary identity
on every database the process can mutate.

Each phase is idempotent and validates observed external state before marking
it complete. A crash resumes at the first incomplete phase; it does not assume
a prior promote, route change, or worker start succeeded. Failure preserves the
old fence and target recovery mode. Health checks never auto-advance a phase.
The evidence driver treats service fencing and PostgreSQL promotion as
re-observable external effects: a resumed boundary first reads Compose or
PostgreSQL state, repeats only an incomplete idempotent effect, and records the
post-effect observation before the durable phase marker advances.

The external-fence, source-position, and passive-readiness markers are written
to the source journal and replayed to the target before any PostgreSQL promote
command is permitted. Terminal activation evidence is a complete typed set for
control, shard 0, and shard 1; no control-only activation marker exists.

The resumable executor is one `dr-admin advance-phase` process per phase. The
crash probe exits that process after strict evidence verification but before
the checkpoint CAS save; a new process reloads the durable version, re-observes
the effect, and advances once. The outer PowerShell evidence harness itself is
not a general-purpose resume engine and intentionally rejects reuse of an
existing evidence directory. Publication evidence must therefore distinguish
the proven per-phase process restart contract from a whole-harness host crash.

Failback is planned before any old-primary volume is removed or reseeded. Each
database then supplies strict provenance for source region/epoch, reseed start
and completion, fenced source position, replayed position, and read-only
reconciliation. `dr-admin validate-failback` also requires a fresh signed
source fence issued after all three reseeds completed; promotion is not reached
when any provenance member is missing or inconsistent.

RTO begins at incident declaration or the first service-unavailable boundary
before fencing, and ends only after reconciliation and customer-write readiness.
RPO is measured separately for all three databases and reported as both
available record/position loss and elapsed window, with the worst required
database as the aggregate.
