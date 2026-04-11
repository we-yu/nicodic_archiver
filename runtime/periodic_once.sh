#!/bin/bash
set -eu

COMPOSE_FILE_PATH=${COMPOSE_FILE_PATH:-docker-compose.runtime.yml}
COMPOSE_SERVICE_NAME=${COMPOSE_SERVICE_NAME:-personal_runtime}
TARGET_DB_PATH=${TARGET_DB_PATH:-/app/data/nicodic.db}
LOCK_DIR_PATH=${LOCK_DIR_PATH:-runtime/logs/periodic_once.lock}
HOST_CRON_LOG_PATH=${HOST_CRON_LOG_PATH:-/runtime/logs/host_cron.log}

mkdir -p "$(dirname "$LOCK_DIR_PATH")"

if mkdir "$LOCK_DIR_PATH" 2>/dev/null; then
  trap 'rm -rf "$LOCK_DIR_PATH"' EXIT INT TERM
  printf '%s\n' "$$" > "$LOCK_DIR_PATH/pid"
else
  echo "[periodic-once] Skip: lock already held at $LOCK_DIR_PATH"
  exit 0
fi

echo "[periodic-once] Starting one periodic cycle"
echo "[periodic-once] target_db_path=$TARGET_DB_PATH"

docker compose -f "$COMPOSE_FILE_PATH" exec -T "$COMPOSE_SERVICE_NAME" \
  env HOST_CRON_LOG_PATH="$HOST_CRON_LOG_PATH" \
  python main.py periodic-once "$TARGET_DB_PATH"

echo "[periodic-once] Finished one periodic cycle"

