#!/bin/bash

runtime_local_env_file() {
  printf '%s\n' "${RUNTIME_LOCAL_ENV_FILE:-.env.runtime.local}"
}

runtime_local_load() {
  local env_file
  env_file=$(runtime_local_env_file)

  if [[ -f "$env_file" ]]; then
    set -a
    . "$env_file"
    set +a
    echo "[runtime-env] Loaded $env_file"
  else
    echo "[runtime-env] Local runtime env not found: $env_file"
    echo "[runtime-env] Falling back to safe local defaults"
  fi

  : "${WEB_BIND_HOST:=127.0.0.1}"
  : "${WEB_PORT:=8000}"

  if [[ -z "${LOCAL_UID:-}" ]]; then
    LOCAL_UID=$(id -u)
    export LOCAL_UID
    echo "[runtime-env] LOCAL_UID was unset; detected $LOCAL_UID"
  fi

  if [[ -z "${LOCAL_GID:-}" ]]; then
    LOCAL_GID=$(id -g)
    export LOCAL_GID
    echo "[runtime-env] LOCAL_GID was unset; detected $LOCAL_GID"
  fi
}

runtime_local_validate() {
  if ! [[ "$WEB_PORT" =~ ^[0-9]+$ ]]; then
    echo "[runtime-env] Invalid WEB_PORT: $WEB_PORT"
    echo "[runtime-env] Use a numeric host port in .env.runtime.local"
    return 1
  fi

  if (( WEB_PORT < 1 || WEB_PORT > 65535 )); then
    echo "[runtime-env] WEB_PORT out of range: $WEB_PORT"
    echo "[runtime-env] Use a port between 1 and 65535"
    return 1
  fi

  if ! [[ "$LOCAL_UID" =~ ^[0-9]+$ ]]; then
    echo "[runtime-env] Invalid LOCAL_UID: $LOCAL_UID"
    return 1
  fi

  if ! [[ "$LOCAL_GID" =~ ^[0-9]+$ ]]; then
    echo "[runtime-env] Invalid LOCAL_GID: $LOCAL_GID"
    return 1
  fi

  return 0
}

runtime_local_warn_port_clash() {
  if ! command -v ss >/dev/null 2>&1; then
    return 0
  fi

  if ss -ltn | grep -E "[\[.:]${WEB_PORT}[[:space:]]" >/dev/null 2>&1; then
    echo "[runtime-env] Port $WEB_PORT already appears busy on the host"
    echo "[runtime-env] If startup fails, change WEB_PORT or stop the other"
    echo "[runtime-env] listener and recreate with bash tools/runtime_up.sh"
  fi
}

runtime_local_print_summary() {
  echo "[runtime-env] WEB_BIND_HOST=$WEB_BIND_HOST"
  echo "[runtime-env] WEB_PORT=$WEB_PORT"
  echo "[runtime-env] LOCAL_UID=$LOCAL_UID"
  echo "[runtime-env] LOCAL_GID=$LOCAL_GID"
}

