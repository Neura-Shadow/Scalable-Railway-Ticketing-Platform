#!/bin/sh
set -eu

: "${PGDATA:=/var/lib/postgresql/restore}"
: "${PGBACKREST_STANZA:?PGBACKREST_STANZA is required}"
: "${PGBACKREST_PITR_TARGET:?PGBACKREST_PITR_TARGET is required}"
: "${RESTORE_CONFIRM:?RESTORE_CONFIRM is required}"
if [ "$PGDATA" != '/var/lib/postgresql/restore' ]; then
  echo 'isolated restore target path is invalid' >&2
  exit 1
fi
if [ "$(id -u)" = '0' ]; then
  install -d -o postgres -g postgres -m 0700 "$PGDATA"
  exec gosu postgres "$0" "$@"
fi
if [ "$RESTORE_CONFIRM" != 'restore-to-isolated-validation' ]; then
  echo 'isolated restore confirmation is invalid' >&2
  exit 1
fi
case "$PGBACKREST_STANZA" in
  railway-control|railway-shard-0|railway-shard-1) ;;
  *) echo 'pgBackRest stanza is invalid' >&2; exit 1 ;;
esac
if ! printf '%s' "$PGBACKREST_PITR_TARGET" | grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'; then
  echo 'PITR target must be canonical UTC RFC3339 seconds' >&2
  exit 1
fi
if [ -e "$PGDATA/PG_VERSION" ]; then
  echo 'isolated restore target is not empty' >&2
  exit 1
fi
set -- --stanza="$PGBACKREST_STANZA" --pg1-path="$PGDATA" --delta \
  --type=time --target="$PGBACKREST_PITR_TARGET" --target-action=promote
if [ -n "${PGBACKREST_SET:-}" ]; then
  case "$PGBACKREST_SET" in
    *[!A-Za-z0-9._:-]*) echo 'backup set is malformed' >&2; exit 1 ;;
  esac
  set -- "$@" --set="$PGBACKREST_SET"
fi
/etc/railway/pgbackrest-secret.sh "$@" restore
rm -f "$PGDATA/standby.signal"
printf "\nrestore_command = '/etc/railway/pgbackrest-secret.sh --stanza=%s --pg1-path=%s archive-get %%f %%p'\n" \
	"$PGBACKREST_STANZA" "$PGDATA" >> "$PGDATA/postgresql.auto.conf"
exec docker-entrypoint.sh postgres \
  -D "$PGDATA" \
  -c listen_addresses='' \
  -c archive_mode=off \
  -c max_connections=160 \
  -c statement_timeout=15000
