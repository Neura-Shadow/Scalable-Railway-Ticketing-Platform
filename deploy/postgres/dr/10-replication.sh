#!/bin/sh
set -eu

: "${DR_REPLICATION_USER:?DR_REPLICATION_USER is required}"
: "${DR_REPLICATION_PASSWORD_FILE:?DR_REPLICATION_PASSWORD_FILE is required}"
: "${DR_REPLICATION_SOURCE_CIDR:?DR_REPLICATION_SOURCE_CIDR is required}"

case "$DR_REPLICATION_USER" in
  *[!A-Za-z0-9_]*|'') exit 1 ;;
esac
case "$DR_REPLICATION_SOURCE_CIDR" in
  *[!A-Fa-f0-9:./]*|''|*/*/*)
    echo 'replication source CIDR is malformed' >&2
    exit 1
    ;;
esac
if [ ! -f "$DR_REPLICATION_PASSWORD_FILE" ]; then
  echo 'replication password secret is unavailable' >&2
  exit 1
fi
DR_REPLICATION_PASSWORD=$(head -n 1 "$DR_REPLICATION_PASSWORD_FILE")
if [ "${#DR_REPLICATION_PASSWORD}" -lt 24 ] || [ "${#DR_REPLICATION_PASSWORD}" -gt 128 ]; then
  echo 'replication password length is invalid' >&2
  exit 1
fi

psql --set=ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=replication_user="$DR_REPLICATION_USER" --set=replication_password="$DR_REPLICATION_PASSWORD" <<'SQL'
SELECT 1 / CASE WHEN NOT EXISTS (
  SELECT 1 FROM pg_roles WHERE rolname=:'replication_user' AND rolsuper
) THEN 1 ELSE 0 END;
SELECT format('CREATE ROLE %I LOGIN REPLICATION PASSWORD %L', :'replication_user', :'replication_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:'replication_user') \gexec
SELECT format('ALTER ROLE %I WITH LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD %L', :'replication_user', :'replication_password') \gexec
SQL

if [ "$(psql --set=ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --tuples-only --no-align --command='SHOW data_checksums')" != 'on' ]; then
  echo 'PostgreSQL data checksums must be enabled at initdb time' >&2
  exit 1
fi

hba_include="$PGDATA/pg_hba.replication.conf"
hba_include_tmp="$hba_include.tmp"
printf 'hostssl replication %s %s scram-sha-256\n' \
  "$DR_REPLICATION_USER" "$DR_REPLICATION_SOURCE_CIDR" > "$hba_include_tmp"
chmod 0600 "$hba_include_tmp"
mv -f "$hba_include_tmp" "$hba_include"
if [ "$(id -u)" = '0' ]; then
  chown postgres:postgres "$hba_include"
fi
if ! grep -Fqx "include_if_exists 'pg_hba.replication.conf'" "$PGDATA/pg_hba.conf"; then
  printf "\ninclude_if_exists 'pg_hba.replication.conf'\n" >> "$PGDATA/pg_hba.conf"
fi
psql --set=ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --command='SELECT pg_reload_conf()' >/dev/null
install -d -o postgres -g postgres -m 0700 /var/lib/postgresql/wal-archive
install -d -o postgres -g postgres -m 0750 /var/lib/pgbackrest
