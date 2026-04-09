#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

COMPOSE_FILE="$REPO_ROOT/docker-compose.runtime.yml"
CONTAINER_NAME="personal_runtime"
EXPORT_DIR="$REPO_ROOT/runtime/data/exports"
SMOKE_ROOT="/app/data/smoke"

DROP_LAST="${DROP_LAST:-5}"
RUN_BUILD="${RUN_BUILD:-1}"

UNIX_URL="https://dic.nicovideo.jp/a/unix"
FAMILYMART_URL="https://dic.nicovideo.jp/a/%E3%83%95%E3%82%A1%E3%83%9F%E3%83%AA%E3%83%BC%E3%83%9E%E3%83%BC%E3%83%88"

UNIX_STATE="$SMOKE_ROOT/manual-unix-builtin"
FAMILYMART_STATE="$SMOKE_ROOT/manual-familymart-builtin"

mkdir -p "$EXPORT_DIR"

log() {
    echo "[KGS_DUAL_SMOKE] $*"
}

fail() {
    echo "[KGS_DUAL_SMOKE][ERROR] $*" >&2
    exit 1
}

ensure_runtime() {
    if [ "$RUN_BUILD" = "1" ]; then
        log "Starting runtime container with build"
        cd "$REPO_ROOT"
        LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)" \
            docker compose -f "$COMPOSE_FILE" up -d --build
    else
        log "Starting runtime container without build"
        cd "$REPO_ROOT"
        LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)" \
            docker compose -f "$COMPOSE_FILE" up -d
    fi
}

reset_state() {
    local state_dir="$1"
    log "Resetting state: $state_dir"
    cd "$REPO_ROOT"
    docker compose -f "$COMPOSE_FILE" exec -T "$CONTAINER_NAME" \
        sh -lc "rm -rf '$state_dir'"
}

run_smoke() {
    local name="$1"
    local url="$2"
    local state_dir="$3"
    local log_file="$EXPORT_DIR/${name}-kgs-dual-smoke.log"

    log "Running smoke for: $name"
    cd "$REPO_ROOT"
    docker compose -f "$COMPOSE_FILE" exec "$CONTAINER_NAME" \
        python main.py verify kgs fetch "$url" \
        --state-dir "$state_dir" \
        --followup-drop-last "$DROP_LAST" | tee "$log_file"
}

extract_first_match() {
    local pattern="$1"
    local file="$2"
    grep -m 1 "$pattern" "$file" || true
}

assert_contains() {
    local pattern="$1"
    local file="$2"
    if ! grep -q "$pattern" "$file"; then
        return 1
    fi
    return 0
}

summarize_case() {
    local name="$1"
    local log_file="$2"

    local identity_line
    local canonical_line
    local trim_target_line
    local before_line
    local removed_line
    local after_line
    local followup_line
    local resume_line
    local saved_line

    identity_line="$(extract_first_match "KGS Debug: identity_article_id=" \
        "$log_file")"
    canonical_line="$(extract_first_match "KGS Debug: canonical_article_id=" \
        "$log_file")"
    trim_target_line="$(extract_first_match "KGS Trim Debug: article_id=" \
        "$log_file")"
    before_line="$(extract_first_match \
        "KGS Trim Debug: saved_response_count_before=" "$log_file")"
    removed_line="$(extract_first_match \
        "KGS Trim Debug: actual_removed=" "$log_file")"
    after_line="$(extract_first_match \
        "KGS Trim Debug: saved_response_count_after=" "$log_file")"
    followup_line="$(extract_first_match \
        "Follow-Up Trimmed Responses:" "$log_file")"
    resume_line="$(extract_first_match \
        "Saved article detected; resuming from max_saved_res_no=" "$log_file")"
    saved_line="$(grep "Saved Responses:" "$log_file" | tail -n 1 || true)"

    echo "----- $name -----"
    echo "${identity_line:-identity: missing}"
    echo "${canonical_line:-canonical: missing}"
    echo "${trim_target_line:-trim_target: missing}"
    echo "${before_line:-before_count: missing}"
    echo "${removed_line:-removed: missing}"
    echo "${after_line:-after_count: missing}"
    echo "${followup_line:-followup: missing}"
    echo "${resume_line:-resume: missing}"
    echo "${saved_line:-saved: missing}"
}

judge_case() {
    local name="$1"
    local log_file="$2"

    if ! assert_contains "Follow-Up Trimmed Responses: $DROP_LAST" "$log_file"
    then
        echo "FAIL"
        return
    fi

    if ! assert_contains "KGS Trim Debug: actual_removed=$DROP_LAST" "$log_file"
    then
        echo "FAIL"
        return
    fi

    if ! assert_contains "Result: bounded-follow-up passed" "$log_file"; then
        echo "FAIL"
        return
    fi

    if ! assert_contains "KGS Summary: pass" "$log_file"; then
        echo "FAIL"
        return
    fi

    echo "PASS"
}

main() {
    local unix_log="$EXPORT_DIR/unix-kgs-dual-smoke.log"
    local familymart_log="$EXPORT_DIR/familymart-kgs-dual-smoke.log"

    ensure_runtime

    reset_state "$UNIX_STATE"
    reset_state "$FAMILYMART_STATE"

    run_smoke "unix" "$UNIX_URL" "$UNIX_STATE"
    run_smoke "familymart" "$FAMILYMART_URL" "$FAMILYMART_STATE"

    echo
    echo "===== KGS DUAL SMOKE SUMMARY ====="
    summarize_case "unix" "$unix_log"
    echo
    summarize_case "familymart" "$familymart_log"
    echo

    local unix_result
    local familymart_result
    unix_result="$(judge_case "unix" "$unix_log")"
    familymart_result="$(judge_case "familymart" "$familymart_log")"

    echo "===== KGS DUAL SMOKE RESULT ====="
    echo "unix: $unix_result"
    echo "familymart: $familymart_result"

    if [ "$unix_result" = "PASS" ] && [ "$familymart_result" = "PASS" ]; then
        echo "overall: PASS"
        exit 0
    fi

    echo "overall: FAIL"
    exit 1
}

main "$@"

