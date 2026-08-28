#!/bin/sh
set -eu

: "${REPLICATION_SLOT:?REPLICATION_SLOT is required}"
: "${DR_MAX_REPLAY_STALENESS_SECONDS:=30}"
case "$DR_MAX_REPLAY_STALENESS_SECONDS" in
  ''|*[!0-9]*) echo 'standby replay staleness bound is invalid' >&2; exit 1 ;;
esac

result=$(psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --tuples-only --no-align \
  --set=ON_ERROR_STOP=1 --set=slot_name="$REPLICATION_SLOT" --set=max_staleness="$DR_MAX_REPLAY_STALENESS_SECONDS" <<'SQL'
SELECT concat_ws('|',
  pg_is_in_recovery()::text,
  current_setting('data_checksums'),
  current_setting('primary_slot_name'),
  (pg_last_wal_receive_lsn() IS NOT NULL)::text,
  CASE
    WHEN pg_last_wal_receive_lsn() IS NOT NULL
         AND pg_last_wal_replay_lsn() >= pg_last_wal_receive_lsn() THEN 'true'
    WHEN pg_last_xact_replay_timestamp() IS NULL THEN 'false'
    WHEN clock_timestamp() - pg_last_xact_replay_timestamp() <= make_interval(secs => :'max_staleness'::integer) THEN 'true'
    ELSE 'false'
  END,
  (((pg_control_checkpoint()).timeline_id) > 0)::text);
SQL
)

if [ "$result" != "true|on|$REPLICATION_SLOT|true|true|true" ]; then
  echo 'standby recovery, slot, WAL receipt, replay freshness, checksum, or timeline health check failed' >&2
  exit 1
fi
