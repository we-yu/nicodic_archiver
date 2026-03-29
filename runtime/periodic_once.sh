#!/bin/bash
set -eu

COMPOSE_FILE_PATH=${COMPOSE_FILE_PATH:-docker-compose.runtime.yml}
COMPOSE_SERVICE_NAME=${COMPOSE_SERVICE_NAME:-personal_runtime}
LOCK_DIR_PATH=${LOCK_DIR_PATH:-runtime/logs/periodic_once.lock}

mkdir -p "$(dirname "$LOCK_DIR_PATH")"

if mkdir "$LOCK_DIR_PATH" 2>/dev/null; then
  trap 'rm -rf "$LOCK_DIR_PATH"' EXIT INT TERM
  printf '%s\n' "$$" > "$LOCK_DIR_PATH/pid"
else
  echo "[periodic-once] Skip: lock already held at $LOCK_DIR_PATH"
  exit 0
fi

echo "[periodic-once] Starting one periodic cycle"
echo "[periodic-once] target_source=sqlite_target_table"

docker compose -f "$COMPOSE_FILE_PATH" exec -T "$COMPOSE_SERVICE_NAME" \
  python main.py periodic-once

echo "[periodic-once] Finished one periodic cycle"

