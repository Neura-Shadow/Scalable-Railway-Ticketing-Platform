#!/bin/sh
set -eu

result=$(psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --tuples-only --no-align --set=ON_ERROR_STOP=1 <<'SQL'
SELECT concat_ws('|',
  (NOT pg_is_in_recovery())::text,
  current_setting('data_checksums'),
  current_setting('ssl'),
  CASE
    WHEN last_failed_time IS NULL THEN 'true'
    WHEN last_archived_time IS NOT NULL AND last_archived_time >= last_failed_time THEN 'true'
    ELSE 'false'
  END)
FROM pg_stat_archiver;
SQL
)

if [ "$result" != 'true|on|on|true' ]; then
  echo 'primary role, checksums, TLS, or WAL archive health check failed' >&2
  exit 1
fi
