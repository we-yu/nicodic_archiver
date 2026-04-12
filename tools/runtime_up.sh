#!/bin/bash
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
COMPOSE_FILE_PATH=${COMPOSE_FILE_PATH:-docker-compose.runtime.yml}
COMPOSE_SERVICE_NAME=${COMPOSE_SERVICE_NAME:-personal_runtime}

cd "$REPO_ROOT"

. "$SCRIPT_DIR/runtime_env.sh"

runtime_local_load
runtime_local_validate
runtime_local_warn_port_clash
runtime_local_print_summary

echo "[runtime-up] Starting runtime with build + force-recreate"
echo "[runtime-up] compose_file=$COMPOSE_FILE_PATH"
echo "[runtime-up] service=$COMPOSE_SERVICE_NAME"

docker compose -f "$COMPOSE_FILE_PATH" up -d --build --force-recreate \
  "$COMPOSE_SERVICE_NAME"

