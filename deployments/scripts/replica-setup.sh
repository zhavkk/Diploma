#!/bin/bash
set -euo pipefail

# Replica initialization script.
# Used as the container entrypoint for PostgreSQL replica nodes.
# If PGDATA is empty, runs pg_basebackup from the primary, creating a streaming standby.
# Then hands off to the standard docker-entrypoint.sh.

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
PG_PRIMARY_HOST="${PG_PRIMARY_HOST:-pg-primary}"
REPLICATION_PASSWORD="${REPLICATION_PASSWORD:-replicator}"
PG_USER="${POSTGRES_USER:-postgres}"
PG_PASSWORD="${POSTGRES_PASSWORD:-postgres}"

if [ ! -f "${PGDATA}/PG_VERSION" ]; then
    echo "[replica-init] PGDATA is empty. Initializing standby from primary '${PG_PRIMARY_HOST}'..."

    # Ensure PGDATA directory exists and has correct ownership/permissions
    mkdir -p "${PGDATA}"
    chown -R postgres:postgres "${PGDATA}"
    chmod 700 "${PGDATA}"

    # Wait until the primary is ready to accept connections
    until PGPASSWORD="${PG_PASSWORD}" pg_isready \
        -h "${PG_PRIMARY_HOST}" \
        -p 5432 \
        -U "${PG_USER}" \
        -t 1 2>/dev/null; do
        echo "[replica-init] Waiting for primary at ${PG_PRIMARY_HOST}:5432 ..."
        sleep 2
    done

    echo "[replica-init] Primary is ready. Running pg_basebackup..."

    # Run pg_basebackup as the postgres OS user.
    # -R: writes standby.signal + primary_conninfo into postgresql.auto.conf
    # -Xs: stream WAL during backup (avoids missing WAL segments)
    # --checkpoint=fast: don't wait for a scheduled checkpoint
    gosu postgres \
        PGPASSWORD="${REPLICATION_PASSWORD}" pg_basebackup \
            -h "${PG_PRIMARY_HOST}" \
            -D "${PGDATA}" \
            -U replicator \
            -Fp \
            -Xs \
            -P \
            -R \
            --checkpoint=fast

    echo "[replica-init] pg_basebackup completed. Standby configured."
else
    echo "[replica-init] PGDATA already initialized, skipping pg_basebackup."
fi

# Hand off to the official postgres docker entrypoint (will start postgres normally)
exec docker-entrypoint.sh postgres
