#!/bin/sh
set -eu

: "${PRIMARY_HOST:?PRIMARY_HOST is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD_FILE:?POSTGRES_PASSWORD_FILE is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${REPLICATION_SLOT:?REPLICATION_SLOT is required}"
: "${PGBACKREST_STANZA:?PGBACKREST_STANZA is required}"
: "${DR_REPLICATION_TLS_ROOT_CERT_FILE:?DR_REPLICATION_TLS_ROOT_CERT_FILE is required}"
: "${DR_REPLICATION_TLS_CERT_FILE:?DR_REPLICATION_TLS_CERT_FILE is required}"
: "${DR_REPLICATION_TLS_KEY_FILE:?DR_REPLICATION_TLS_KEY_FILE is required}"

if [ "$(id -u)" = '0' ]; then
  install -d -o postgres -g postgres -m 0700 "$PGDATA"
  install -d -o postgres -g postgres -m 0700 /var/lib/postgresql/tls
  for source in "$DR_REPLICATION_TLS_ROOT_CERT_FILE" "$DR_REPLICATION_TLS_CERT_FILE" "$DR_REPLICATION_TLS_KEY_FILE"; do
    if [ ! -s "$source" ]; then
      echo 'replication TLS material is unavailable' >&2
      exit 1
    fi
  done
  exec gosu postgres "$0" "$@"
fi

for identity in "$PRIMARY_HOST" "$POSTGRES_USER" "$POSTGRES_DB" "$REPLICATION_SLOT" "$PGBACKREST_STANZA"; do
  case "$identity" in
    ''|*[!A-Za-z0-9_.-]*) echo 'standby identity is malformed' >&2; exit 1 ;;
  esac
done
if [ ! -f "$POSTGRES_PASSWORD_FILE" ]; then
  echo 'replication password secret is unavailable' >&2
  exit 1
fi
POSTGRES_PASSWORD=$(head -n 1 "$POSTGRES_PASSWORD_FILE")
if [ "${#POSTGRES_PASSWORD}" -lt 24 ] || [ "${#POSTGRES_PASSWORD}" -gt 128 ]; then
  echo 'replication password length is invalid' >&2
  exit 1
fi

if [ ! -s "$DR_REPLICATION_TLS_ROOT_CERT_FILE" ]; then
  echo 'replication TLS root certificate is unavailable' >&2
  exit 1
fi

export PGPASSWORD="$POSTGRES_PASSWORD"
export PGSSLMODE=verify-full
export PGSSLROOTCERT="$DR_REPLICATION_TLS_ROOT_CERT_FILE"

primary_psql() {
  psql --host "$PRIMARY_HOST" --port 5432 --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" --set=ON_ERROR_STOP=1 "$@"
}

attempt=0
until primary_psql --tuples-only --no-align --command='SELECT 1' >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 120 ]; then
    echo 'TLS-authenticated primary did not become ready within the bounded wait' >&2
    exit 1
  fi
  sleep 1
done

if [ -s "$PGDATA/PG_VERSION" ] && [ ! -f "$PGDATA/standby.signal" ]; then
  if [ "${DR_FORCE_RESEED:-}" != 'confirm-discard-standby' ]; then
    echo 'initialized data directory is not a standby; set DR_FORCE_RESEED=confirm-discard-standby to reseed it' >&2
    exit 1
  fi
  find "$PGDATA" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
fi

slot_record=$(primary_psql --tuples-only --no-align --set=slot_name="$REPLICATION_SLOT" <<'SQL'
SELECT concat_ws('|', active::text, slot_type, coalesce(wal_status, ''), coalesce(restart_lsn::text, ''))
FROM pg_replication_slots
WHERE slot_name=:'slot_name';
SQL
)
create_slot='false'
if [ -z "$slot_record" ]; then
  create_slot='true'
else
  old_ifs=$IFS
  IFS='|'
  set -- $slot_record
  IFS=$old_ifs
  slot_active=$1
  slot_type=$2
  slot_wal_status=$3
  slot_restart_lsn=${4:-}
  if [ "$slot_active" = 'true' ]; then
    echo 'replication slot is active on another process' >&2
    exit 1
  fi
  if [ "$slot_type" != 'physical' ]; then
    echo 'replication slot exists with the wrong type' >&2
    exit 1
  fi
  if [ "$slot_wal_status" = 'lost' ] || [ -z "$slot_restart_lsn" ]; then
    primary_psql --set=slot_name="$REPLICATION_SLOT" <<'SQL'
SELECT pg_drop_replication_slot(:'slot_name');
SQL
    create_slot='true'
  fi
fi

basebackup_marker="$(dirname "$PGDATA")/.${REPLICATION_SLOT}.basebackup-in-progress"
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  umask 077
  basebackup_identity="$PRIMARY_HOST|$POSTGRES_DB|$POSTGRES_USER|$REPLICATION_SLOT"
  if [ -e "$basebackup_marker" ]; then
    if [ ! -f "$basebackup_marker" ] || [ "$(head -n 1 "$basebackup_marker")" != "$basebackup_identity" ]; then
      echo 'interrupted base-backup marker does not match the requested replication pair' >&2
      exit 1
    fi
    echo 'resuming interrupted base backup from a clean partial data directory' >&2
  fi
  printf '%s\n' "$basebackup_identity" > "$basebackup_marker"
  find "$PGDATA" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  set -- pg_basebackup --host "$PRIMARY_HOST" --port 5432 --username "$POSTGRES_USER" \
    --pgdata "$PGDATA" --write-recovery-conf --wal-method=stream --checkpoint=fast \
    --slot "$REPLICATION_SLOT"
  if [ "$create_slot" = 'true' ]; then
    set -- "$@" --create-slot
  fi
  "$@"
  rm -f -- "$basebackup_marker"
elif [ "$create_slot" = 'true' ]; then
  primary_psql --set=slot_name="$REPLICATION_SLOT" <<'SQL'
SELECT pg_create_physical_replication_slot(:'slot_name', true, false);
SQL
fi

checksum_version=$(pg_controldata "$PGDATA" | awk -F: '/Data page checksum version/ { gsub(/[[:space:]]/, "", $2); print $2 }')
if [ "$checksum_version" != '1' ]; then
  echo 'standby data directory does not have PostgreSQL data checksums enabled' >&2
  exit 1
fi

install -m 0600 "$DR_REPLICATION_TLS_CERT_FILE" /var/lib/postgresql/tls/replication-server.crt
install -m 0600 "$DR_REPLICATION_TLS_KEY_FILE" /var/lib/postgresql/tls/replication-server.key
install -m 0600 "$DR_REPLICATION_TLS_ROOT_CERT_FILE" /var/lib/postgresql/tls/replication-root.crt

umask 077
printf '%s:5432:*:%s:%s\n' "$PRIMARY_HOST" "$POSTGRES_USER" "$POSTGRES_PASSWORD" > "$PGDATA/.pgpass"
chmod 0600 "$PGDATA/.pgpass"
chmod 0700 "$PGDATA"
unset POSTGRES_PASSWORD PGPASSWORD

exec docker-entrypoint.sh postgres \
  -c ssl=on \
  -c ssl_cert_file=/var/lib/postgresql/tls/replication-server.crt \
  -c ssl_key_file=/var/lib/postgresql/tls/replication-server.key \
  -c ssl_ca_file=/var/lib/postgresql/tls/replication-root.crt \
  -c max_connections=160 \
  -c hot_standby=on \
  -c max_standby_streaming_delay=30s \
  -c archive_mode=on \
  -c archive_command="/etc/railway/pgbackrest-secret.sh --stanza=$PGBACKREST_STANZA archive-push %p" \
  -c restore_command="/etc/railway/pgbackrest-secret.sh --stanza=$PGBACKREST_STANZA archive-get %f %p" \
  -c recovery_target_timeline=latest \
  -c primary_conninfo="host=$PRIMARY_HOST port=5432 user=$POSTGRES_USER passfile=$PGDATA/.pgpass sslmode=verify-full sslrootcert=$DR_REPLICATION_TLS_ROOT_CERT_FILE application_name=$REPLICATION_SLOT" \
  -c primary_slot_name="$REPLICATION_SLOT"
