# Encrypted Backup and Point-in-Time Recovery

## Tool and trust boundary

Milestone 7 selects pgBackRest 2.59.0 as the single backup, WAL archive,
verification, retention, and PITR mechanism for the three PostgreSQL databases.
Each database has its own repository volume and client-side repository cipher,
outside every PGDATA volume. Control, shard 0, and shard 1 database processes
cannot mount or decrypt a peer repository. A DR pair shares only its own
database repository so WAL remains available across promotion. The private
backup administrator selects one stanza, repository path, and cipher per
invocation. Keys are not stored in Git, Compose, PostgreSQL, artifacts, or a
repository volume.

Replication and backup solve different failures. Streaming reduces recovery
time; encrypted full backups plus WAL provide independent restore history.
Checksums and repository verification detect corruption, but they are not
restore proof.

Automatic pgBackRest expiration is disabled (`repo1-expire-auto=n`). Retention
inspection is read-only; a deletion is allowed only after a persisted dry run,
an operator-bound confirmation, and a durable `confirmed` transition. The
backup administration process invokes `/etc/railway/pgbackrest-secret.sh`, not
the raw binary, and its PostgreSQL identity is a dedicated non-superuser with
SELECT/INSERT/UPDATE only on the five backup metadata relations. It has no
DELETE privilege and application identities have no access to those relations.

Every backup mutation requires a caller-generated UUID, for example:

```text
backup-admin backup-control --repository repo-dr --operation-id 44444444-4444-4444-8444-444444444444
```

The intent is inserted before pgBackRest runs and completed only after the
artifact metadata is durable. A repeated UUID is rejected, so a process crash
cannot silently launch a second external backup. An unfinished `planned`
record is explicitly uncertain and must be reconciled against the repository
inventory before an operator chooses a new UUID.

## Restore validation

Restore targets are a fixed allowlist of isolated, project-scoped data
directories with no customer ingress or application write credentials. The
operator verifies repository metadata and checksum, restores to the requested
bounded target/time, boots PostgreSQL, records achieved timeline/LSN/time, and
runs schema plus payment, ticket, refund, ledger, settlement, and regional-
authority invariants. Only this boot/read/reconcile result is restore evidence.

The PITR target is mandatory, canonical UTC RFC3339, and becomes fixed
pgBackRest arguments (`--type=time`, `--target=...`,
`--target-action=promote`). Example:

```text
backup-admin restore-validation --database control --repository repo-dr --backup-set 20260811-130000F --target validation-control --pitr-target 2026-08-11T13:05:00Z --confirm --operation-id 55555555-5555-4555-8555-555555555555
```

Restore intent is persisted as `running` before touching the isolated target;
success changes it once to `passed`. Expiration changes `dry_run` to
`confirmed`, then to `executing`, before pgBackRest deletes anything. The
runner performs a separate bounded repository inventory after deletion and
carries the artifact checksum and dry-run plan digest into that postcondition.
Only observed absence changes `executing` to `expired`. On restart, an
`executing` operation inventories first: a present set resumes the one bound
deletion, while an absent set completes only the missing journal transition.
It never blindly repeats a destructive command after an uncertain exit.

The restore runner boots the restored, allowlisted PGDATA with no TCP listener
and connects over a private temporary Unix socket in a repeatable-read,
read-only transaction. It independently observes the clean schema version and
WAL timeline, then verifies the required payment, ticket, refund, operational
ledger, settlement, and regional-authority relations and invariants (the
control-only ledger and settlement facts are not invented for shard restores).
Missing tables, a dirty/wrong schema, recovery mode, an invalid timeline, or
any required false/missing fact fails closed; pgBackRest output alone is never
recorded as reconciliation evidence.

Identity columns and completed evidence are guarded against update or
deletion. Interrupted `planned`, `running`, `confirmed`, or `executing`
operations are deliberately visible recovery work and are never reported as
successful.

Backup writer, restore/decryption operator, replication, application, and DR
promotion credentials are separate. Destructive expiration requires explicit
confirmation and cannot be performed by routine services. Because pgBackRest
does not document in-place repository cipher rotation, key rotation creates a
new encrypted repository and verifies migration/retention before retiring the
old one.
