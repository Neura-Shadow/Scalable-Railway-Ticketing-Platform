#!/bin/sh
set -eu

: "${DR_REPLICATION_TLS_CERT_FILE:?DR_REPLICATION_TLS_CERT_FILE is required}"
: "${DR_REPLICATION_TLS_KEY_FILE:?DR_REPLICATION_TLS_KEY_FILE is required}"
: "${DR_REPLICATION_TLS_ROOT_CERT_FILE:?DR_REPLICATION_TLS_ROOT_CERT_FILE is required}"

if [ "$(id -u)" = '0' ]; then
  install -d -o postgres -g postgres -m 0700 "$PGDATA"
  install -d -o postgres -g postgres -m 0700 /var/lib/postgresql/tls
  for source in "$DR_REPLICATION_TLS_CERT_FILE" "$DR_REPLICATION_TLS_KEY_FILE" "$DR_REPLICATION_TLS_ROOT_CERT_FILE"; do
    if [ ! -s "$source" ]; then
      echo 'replication TLS material is unavailable' >&2
      exit 1
    fi
  done
  install -o postgres -g postgres -m 0600 "$DR_REPLICATION_TLS_CERT_FILE" /var/lib/postgresql/tls/replication-server.crt
  install -o postgres -g postgres -m 0600 "$DR_REPLICATION_TLS_KEY_FILE" /var/lib/postgresql/tls/replication-server.key
  install -o postgres -g postgres -m 0600 "$DR_REPLICATION_TLS_ROOT_CERT_FILE" /var/lib/postgresql/tls/replication-root.crt
  exec docker-entrypoint.sh "$@" \
    -c ssl=on \
    -c ssl_cert_file=/var/lib/postgresql/tls/replication-server.crt \
    -c ssl_key_file=/var/lib/postgresql/tls/replication-server.key \
    -c ssl_ca_file=/var/lib/postgresql/tls/replication-root.crt
fi

echo 'primary bootstrap must start as root so TLS files can be installed with PostgreSQL ownership' >&2
exit 1
