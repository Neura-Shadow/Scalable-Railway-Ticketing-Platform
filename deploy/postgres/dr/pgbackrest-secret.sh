#!/bin/sh
set -eu

if [ "$(id -u)" -eq 0 ]; then
  exec gosu postgres "$0" "$@"
fi

stanza=''
for argument in "$@"; do
  case "$argument" in
    --stanza=*) stanza=${argument#--stanza=} ;;
  esac
done

# Database processes receive only their own generic cipher file and repository
# mount. The private backup-admin may receive all three, but the selected
# stanza maps to exactly one cipher and repository path for each invocation.
if [ -n "${PGBACKREST_CONTROL_CIPHER_FILE:-}${PGBACKREST_SHARD_0_CIPHER_FILE:-}${PGBACKREST_SHARD_1_CIPHER_FILE:-}" ]; then
  case "$stanza" in
    railway-control)
      PGBACKREST_CIPHER_FILE=${PGBACKREST_CONTROL_CIPHER_FILE:?control backup cipher is unavailable}
      PGBACKREST_REPO1_PATH=${PGBACKREST_CONTROL_REPO_PATH:?control backup repository is unavailable}
      ;;
    railway-shard-0)
      PGBACKREST_CIPHER_FILE=${PGBACKREST_SHARD_0_CIPHER_FILE:?shard-0 backup cipher is unavailable}
      PGBACKREST_REPO1_PATH=${PGBACKREST_SHARD_0_REPO_PATH:?shard-0 backup repository is unavailable}
      ;;
    railway-shard-1)
      PGBACKREST_CIPHER_FILE=${PGBACKREST_SHARD_1_CIPHER_FILE:?shard-1 backup cipher is unavailable}
      PGBACKREST_REPO1_PATH=${PGBACKREST_SHARD_1_REPO_PATH:?shard-1 backup repository is unavailable}
      ;;
    *) echo 'pgBackRest stanza is required for isolated repository selection' >&2; exit 1 ;;
  esac
  case "$PGBACKREST_REPO1_PATH" in
    /var/lib/pgbackrest/*) ;;
    *) echo 'pgBackRest repository path is outside the allowlist' >&2; exit 1 ;;
  esac
  export PGBACKREST_REPO1_PATH
else
  : "${PGBACKREST_CIPHER_FILE:=/run/secrets/pgbackrest_cipher_pass}"
fi
if [ ! -f "$PGBACKREST_CIPHER_FILE" ]; then
  echo 'pgBackRest cipher secret is unavailable' >&2
  exit 1
fi
PGBACKREST_REPO1_CIPHER_PASS=$(head -n 1 "$PGBACKREST_CIPHER_FILE")
case "$PGBACKREST_REPO1_CIPHER_PASS" in
  ''|*[!A-Za-z0-9._:@/+\=-]*) echo 'pgBackRest cipher secret is malformed' >&2; exit 1 ;;
esac
if [ "${#PGBACKREST_REPO1_CIPHER_PASS}" -lt 32 ] || [ "${#PGBACKREST_REPO1_CIPHER_PASS}" -gt 128 ]; then
  echo 'pgBackRest cipher secret length is invalid' >&2
  exit 1
fi
export PGBACKREST_REPO1_CIPHER_PASS
unset PGBACKREST_CIPHER_FILE PGBACKREST_CONTROL_CIPHER_FILE PGBACKREST_SHARD_0_CIPHER_FILE PGBACKREST_SHARD_1_CIPHER_FILE
exec /usr/bin/pgbackrest --config=/etc/pgbackrest/pgbackrest.conf "$@"
