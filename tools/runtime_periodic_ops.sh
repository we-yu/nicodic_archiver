#!/bin/bash
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

STOP_FILE=${SOFT_TERMINATE_FILE:-runtime/control/stop_after_current}
LOCK_FILE=${PERIODIC_LOCK_FILE:-runtime/logs/periodic_once.lock}
COMPOSE_FILE_PATH=${COMPOSE_FILE_PATH:-docker-compose.runtime.yml}
COMPOSE_SERVICE_NAME=${COMPOSE_SERVICE_NAME:-personal_runtime}
MAX_STOP_COUNT=255
SCRAPE_PROCESS_PATTERN=$(
  printf '%s' \
    'runtime/periodic_once\.sh|python(3)? .*main\.py ' \
    '(periodic|periodic-once|batch)|docker compose ' \
    '.*periodic_once\.sh|docker-compose .*periodic_once\.sh'
)

cd "$REPO_ROOT"

usage() {
  cat <<'EOF'
Usage: bash tools/runtime_periodic_ops.sh <subcommand> [args]

Subcommands:
  status           Show lock, stop-file, process, and bounded docker status
  stop-once        Create the soft-stop file with countdown 1
  stop-count N     Create or replace the soft-stop file with countdown N
  show-stop        Show the current soft-stop file state
  clear-stop       Remove the soft-stop file if present
  clear-lock       Remove periodic lock only when no work appears active
EOF
}

is_natural_number() {
  [[ "$1" =~ ^[0-9]+$ ]] && (( $1 >= 1 ))
}

clamp_stop_count() {
  local requested=$1

  if (( requested > MAX_STOP_COUNT )); then
    printf '%s\n' "$MAX_STOP_COUNT"
    return
  fi

  printf '%s\n' "$requested"
}

read_process_lines() {
  if [[ -n "${RUNTIME_PERIODIC_OPS_PS_FILE:-}" ]]; then
    cat "$RUNTIME_PERIODIC_OPS_PS_FILE"
    return
  fi

  ps -eo pid=,command= 2>/dev/null || true
}

list_scrape_like_processes() {
  read_process_lines | grep -E "$SCRAPE_PROCESS_PATTERN" || true
}

list_container_scrape_like_processes() {
  if [[ "${RUNTIME_PERIODIC_OPS_SKIP_DOCKER:-0}" == "1" ]]; then
    return
  fi

  if ! command -v docker >/dev/null 2>&1; then
    return
  fi

  if ! docker compose -f "$COMPOSE_FILE_PATH" ps "$COMPOSE_SERVICE_NAME" \
    >/dev/null 2>&1; then
    return
  fi

  docker compose -f "$COMPOSE_FILE_PATH" exec -T "$COMPOSE_SERVICE_NAME" \
    ps -eo pid=,command= 2>/dev/null | grep -E "$SCRAPE_PROCESS_PATTERN" || true
}

has_active_scrape_like_processes() {
  local host_lines
  local container_lines

  host_lines=$(list_scrape_like_processes)
  if [[ -n "$host_lines" ]]; then
    return 0
  fi

  container_lines=$(list_container_scrape_like_processes)
  [[ -n "$container_lines" ]]
}

show_lock_state() {
  if [[ -e "$LOCK_FILE" ]]; then
    echo "[runtime-ops] periodic_lock_exists=yes"
  else
    echo "[runtime-ops] periodic_lock_exists=no"
  fi
  echo "[runtime-ops] periodic_lock_path=$LOCK_FILE"
}

show_stop_state() {
  if [[ ! -e "$STOP_FILE" ]]; then
    echo "[runtime-ops] soft_stop_exists=no"
    echo "[runtime-ops] soft_stop_path=$STOP_FILE"
    return
  fi

  echo "[runtime-ops] soft_stop_exists=yes"
  echo "[runtime-ops] soft_stop_path=$STOP_FILE"

  if [[ ! -s "$STOP_FILE" ]]; then
    echo "[runtime-ops] soft_stop_content=<empty>"
    return
  fi

  local content
  content=$(cat "$STOP_FILE")
  echo "[runtime-ops] soft_stop_content=$content"
  if [[ "$content" =~ ^[0-9]+$ ]]; then
    echo "[runtime-ops] soft_stop_countdown=$content"
  fi
}

show_process_state() {
  local lines
  lines=$(list_scrape_like_processes)
  if [[ -z "$lines" ]]; then
    echo "[runtime-ops] scrape_like_processes=none"
    return
  fi

  echo "[runtime-ops] scrape_like_processes=active"
  printf '%s\n' "$lines"
}

show_container_process_state() {
  local lines

  if [[ "${RUNTIME_PERIODIC_OPS_SKIP_DOCKER:-0}" == "1" ]]; then
    echo "[runtime-ops] docker_status=skipped"
    return
  fi

  if ! command -v docker >/dev/null 2>&1; then
    echo "[runtime-ops] docker_status=unavailable"
    return
  fi

  if ! docker compose -f "$COMPOSE_FILE_PATH" ps "$COMPOSE_SERVICE_NAME" \
    >/dev/null 2>&1; then
    echo "[runtime-ops] docker_status=not_running"
    return
  fi

  lines=$(list_container_scrape_like_processes)
  if [[ -z "$lines" ]]; then
    echo "[runtime-ops] container_scrape_like_processes=none"
    return
  fi

  echo "[runtime-ops] container_scrape_like_processes=active"
  printf '%s\n' "$lines"
}

write_stop_count() {
  local raw_count=$1
  local clamped_count

  if ! is_natural_number "$raw_count"; then
    echo "[runtime-ops] stop-count requires a natural number >= 1"
    exit 1
  fi

  clamped_count=$(clamp_stop_count "$raw_count")
  mkdir -p "$(dirname "$STOP_FILE")"
  printf '%s\n' "$clamped_count" > "$STOP_FILE"
  echo "[runtime-ops] Wrote soft-stop countdown=$clamped_count"
  echo "[runtime-ops] soft-stop path=$STOP_FILE"
}

clear_stop_file() {
  rm -f "$STOP_FILE"
  echo "[runtime-ops] Cleared soft-stop file: $STOP_FILE"
}

clear_lock_file() {
  if has_active_scrape_like_processes; then
    echo "[runtime-ops] Refusing to clear lock; scrape-like work appears active"
    exit 1
  fi

  if [[ ! -e "$LOCK_FILE" ]]; then
    echo "[runtime-ops] Lock file already absent: $LOCK_FILE"
    return
  fi

  rm -f "$LOCK_FILE"
  echo "[runtime-ops] Cleared periodic lock: $LOCK_FILE"
}

subcommand=${1:-}

case "$subcommand" in
  status)
    show_lock_state
    show_stop_state
    show_process_state
    show_container_process_state
    ;;
  stop-once)
    write_stop_count 1
    ;;
  stop-count)
    if [[ $# -ne 2 ]]; then
      usage
      exit 1
    fi
    write_stop_count "$2"
    ;;
  show-stop)
    show_stop_state
    ;;
  clear-stop)
    clear_stop_file
    ;;
  clear-lock)
    clear_lock_file
    ;;
  ""|-h|--help|help)
    usage
    ;;
  *)
    usage
    exit 1
    ;;
esac
