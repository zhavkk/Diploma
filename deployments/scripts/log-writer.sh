#!/usr/bin/env bash
set -u

table="ha_random_logs"

echo "log-writer: writing to ${PGHOST}:${PGPORT}/${PGDATABASE} as ${PGUSER}"

while true; do
  if psql -v ON_ERROR_STOP=1 \
    -c "CREATE TABLE IF NOT EXISTS ${table} (
          id bigserial PRIMARY KEY,
          created_at timestamptz NOT NULL DEFAULT now(),
          source text NOT NULL,
          level text NOT NULL,
          message text NOT NULL
        );" >/dev/null 2>&1; then
    echo "log-writer: table ${table} is ready"
    break
  fi

  echo "log-writer: waiting for write endpoint..."
  sleep 1
done

while true; do
  level=$(
    case "$((RANDOM % 4))" in
      0) echo "debug" ;;
      1) echo "info" ;;
      2) echo "warn" ;;
      *) echo "error" ;;
    esac
  )
  message="random local workload log ${RANDOM}-${RANDOM}"

  if psql -v ON_ERROR_STOP=1 \
    -c "INSERT INTO ${table} (source, level, message) VALUES ('log-writer', '${level}', '${message}');" >/dev/null 2>&1; then
    echo "log-writer: inserted ${level} ${message}"
  else
    echo "log-writer: write failed, retrying"
  fi

  sleep 1
done
