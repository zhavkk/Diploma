#!/bin/bash
set -euo pipefail

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
PG_PRIMARY_HOST="${PG_PRIMARY_HOST:-pg-primary}"
REPLICATION_PASSWORD="${REPLICATION_PASSWORD:-replicator}"
PG_USER="${POSTGRES_USER:-postgres}"
PG_PASSWORD="${POSTGRES_PASSWORD:-postgres}"

if [ ! -f "${PGDATA}/PG_VERSION" ]; then
    echo "[replica-init] PGDATA is empty. Initializing standby from primary '${PG_PRIMARY_HOST}'..."
    mkdir -p "${PGDATA}"
    chown -R postgres:postgres "${PGDATA}"
    chmod 700 "${PGDATA}"
    until PGPASSWORD="${PG_PASSWORD}" pg_isready \
        -h "${PG_PRIMARY_HOST}" \
        -p 5432 \
        -U "${PG_USER}" \
        -t 1 2>/dev/null; do
        echo "[replica-init] Waiting for primary at ${PG_PRIMARY_HOST}:5432 ..."
        sleep 2
    done

    echo "[replica-init] Primary is ready. Running pg_basebackup..."
    su - postgres -c "PGPASSWORD='${REPLICATION_PASSWORD}' pg_basebackup \
        -h '${PG_PRIMARY_HOST}' \
        -D '${PGDATA}' \
        -U replicator \
        -Fp \
        -Xs \
        -P \
        -R \
        --checkpoint=fast"

    echo "[replica-init] pg_basebackup completed. Standby configured."
    if [ -f "${PGDATA}/postgresql.conf" ]; then
        sed -i "s/^max_connections = 100/max_connections = 200/" "${PGDATA}/postgresql.conf"
        echo "[replica-init] Updated max_connections to 200 in postgresql.conf"
    fi
else
    echo "[replica-init] PGDATA already initialized, skipping pg_basebackup."
fi
exec /usr/local/bin/docker-entrypoint.sh postgres
