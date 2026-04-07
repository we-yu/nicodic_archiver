#!/usr/bin/env bash
set -euo pipefail

# 使い方:
#   bash tools/kgs_smoke.sh <canonical_article_url>
#
# 例:
#   bash tools/kgs_smoke.sh https://dic.nicovideo.jp/a/unix
#   bash tools/kgs_smoke.sh https://dic.nicovideo.jp/a/%E3%83%95%E3%82%A1%E3%83%9F%E3%83%AA%E3%83%BC%E3%83%9E%E3%83%BC%E3%83%88
#
# 環境変数:
#   DROP_LAST=5   末尾から削るレス数（既定 5）
#   KEEP_STATE=1  成功時も isolated state を残す
#
# このスクリプトは:
# - docker compose runtime を必要なら起動する
# - isolated smoke state を作る
# - 初回 KGS fetch を行う
# - isolated DB と isolated JSON の末尾 N 件を削る
# - 再度 KGS fetch を行い、増分取得で復旧できるかを見る
# - telemetry CSV を書き出す
# - 成功時は isolated state を削除する
# - 失敗時は state を残して調査しやすくする

ARTICLE_URL="${1:-}"
DROP_LAST="${DROP_LAST:-5}"
KEEP_STATE="${KEEP_STATE:-0}"

if [[ -z "${ARTICLE_URL}" ]]; then
  echo "[ERROR] canonical article URL を指定してください" >&2
  echo "Usage: bash tools/kgs_smoke.sh <canonical_article_url>" >&2
  exit 2
fi

case "${ARTICLE_URL}" in
  https://dic.nicovideo.jp/a/*)
    ;;
  *)
    echo "[ERROR] canonical article URL だけを受け付けます: ${ARTICLE_URL}" >&2
    exit 2
    ;;
esac

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="docker-compose.runtime.yml"
SERVICE="personal_runtime"

cd "${REPO_ROOT}"

mkdir -p runtime/data/smoke runtime/data/exports

SLUG="$(printf '%s' "${ARTICLE_URL}" | sha1sum | awk '{print substr($1,1,12)}')"
STATE_HOST="runtime/data/smoke/kgs-${SLUG}"
STATE_CONTAINER="/app/data/smoke/kgs-${SLUG}"
DB_CONTAINER="${STATE_CONTAINER}/data/nicodic.db"
DATA_DIR_CONTAINER="${STATE_CONTAINER}/data"
EXPORT_HOST="runtime/data/exports/kgs-${SLUG}-telemetry.csv"
EXPORT_CONTAINER="/app/data/exports/kgs-${SLUG}-telemetry.csv"

cleanup_on_exit() {
  rc="$1"
  if [[ "${rc}" -eq 0 ]]; then
    if [[ "${KEEP_STATE}" = "1" ]]; then
      echo "[INFO] KEEP_STATE=1 のため isolated state を保持します: ${STATE_HOST}"
    else
      rm -rf "${STATE_HOST}"
      echo "[INFO] isolated state を削除しました: ${STATE_HOST}"
    fi
  else
    echo "[WARN] エラー終了のため isolated state を保持します: ${STATE_HOST}" >&2
  fi
}
trap 'cleanup_on_exit $?' EXIT

run_cmd() {
  echo
  echo "[RUN] $*"
  "$@"
}

ensure_runtime_up() {
  if docker compose -f "${COMPOSE_FILE}" ps --status running | grep -q "${SERVICE}"; then
    echo "[INFO] runtime container は起動済みです"
    return 0
  fi

  echo "[INFO] runtime container を起動します"
  LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)" docker compose -f "${COMPOSE_FILE}" up -d --build
}

read_article_state() {
  docker compose -f "${COMPOSE_FILE}" exec -T "${SERVICE}" python - "${DB_CONTAINER}" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

row = conn.execute(
    "SELECT article_id, article_type, title, canonical_url FROM articles ORDER BY created_at DESC LIMIT 1"
).fetchone()

if row is None:
    print("NO_ARTICLE")
    raise SystemExit(0)

count_row = conn.execute(
    "SELECT COUNT(*) AS c, COALESCE(MAX(res_no), 0) AS m FROM responses WHERE article_id=? AND article_type=?",
    (row["article_id"], row["article_type"]),
).fetchone()

print(
    f'{row["article_id"]}\t{row["article_type"]}\t{row["title"]}\t{row["canonical_url"] or ""}\t{count_row["c"]}\t{count_row["m"]}'
)
PY
}

trim_db_and_json_tail() {
  docker compose -f "${COMPOSE_FILE}" exec -T "${SERVICE}" python - "${DB_CONTAINER}" "${DATA_DIR_CONTAINER}" "${DROP_LAST}" <<'PY'
import json
import sqlite3
import sys
from pathlib import Path

db_path = sys.argv[1]
data_dir = Path(sys.argv[2])
drop_last = int(sys.argv[3])

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

row = conn.execute(
    "SELECT article_id, article_type, title FROM articles ORDER BY created_at DESC LIMIT 1"
).fetchone()

if row is None:
    print("NO_ARTICLE")
    raise SystemExit(0)

article_id = row["article_id"]
article_type = row["article_type"]

res_rows = conn.execute(
    "SELECT res_no FROM responses WHERE article_id=? AND article_type=? ORDER BY res_no DESC LIMIT ?",
    (article_id, article_type, drop_last),
).fetchall()

actual = len(res_rows)
res_nos = [r["res_no"] for r in res_rows]

if actual > 0:
    conn.executemany(
        "DELETE FROM responses WHERE article_id=? AND article_type=? AND res_no=?",
        [(article_id, article_type, res_no) for res_no in res_nos],
    )
    conn.commit()

new_max = conn.execute(
    "SELECT COALESCE(MAX(res_no), 0) FROM responses WHERE article_id=? AND article_type=?",
    (article_id, article_type),
).fetchone()[0]

json_path = None
json_trimmed = 0
for candidate in sorted(data_dir.glob("*.json")):
    try:
        payload = json.loads(candidate.read_text(encoding="utf-8"))
    except Exception:
        continue

    p_article_id = str(payload.get("article_id", ""))
    p_article_type = str(payload.get("article_type", ""))
    if p_article_id != article_id or p_article_type != article_type:
        continue

    responses = payload.get("responses")
    if isinstance(responses, list):
        json_trimmed = min(drop_last, len(responses))
        if json_trimmed > 0:
            payload["responses"] = responses[:-json_trimmed]
        payload["response_count"] = len(payload.get("responses", []))
        candidate.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        json_path = str(candidate)
        break

print(
    f"{article_id}\t{article_type}\t{actual}\t{new_max}\t{json_trimmed}\t{json_path or ''}"
)
PY
}

ensure_runtime_up

echo "[INFO] 対象記事: ${ARTICLE_URL}"
echo "[INFO] isolated state: ${STATE_HOST}"
echo "[INFO] telemetry export: ${EXPORT_HOST}"

run_cmd docker compose -f "${COMPOSE_FILE}" exec "${SERVICE}" python main.py verify kgs fetch "${ARTICLE_URL}" --state-dir "${STATE_CONTAINER}"

STATE1="$(read_article_state)"
if [[ "${STATE1}" = "NO_ARTICLE" ]]; then
  echo "[ERROR] 初回 fetch 後に isolated DB から article を読めませんでした" >&2
  exit 1
fi

IFS=$'\t' read -r ARTICLE_ID ARTICLE_TYPE ARTICLE_TITLE ARTICLE_CANONICAL BEFORE_COUNT BEFORE_MAX <<< "${STATE1}"

echo "[INFO] 初回保存後 article_id=${ARTICLE_ID} article_type=${ARTICLE_TYPE} title=${ARTICLE_TITLE}"
echo "[INFO] 初回保存後 response_count=${BEFORE_COUNT} max_res_no=${BEFORE_MAX}"

TRIM_RESULT="$(trim_db_and_json_tail)"
if [[ "${TRIM_RESULT}" = "NO_ARTICLE" ]]; then
  echo "[ERROR] trim 対象 article を見つけられませんでした" >&2
  exit 1
fi

IFS=$'\t' read -r TRIM_ARTICLE_ID TRIM_ARTICLE_TYPE DB_TRIMMED_COUNT AFTER_DELETE_MAX JSON_TRIMMED_COUNT JSON_PATH <<< "${TRIM_RESULT}"

echo "[INFO] DB 末尾レス削除件数=${DB_TRIMMED_COUNT}"
echo "[INFO] JSON 末尾レス削除件数=${JSON_TRIMMED_COUNT}"
echo "[INFO] 削除後 max_res_no=${AFTER_DELETE_MAX}"

if [[ "${DB_TRIMMED_COUNT}" = "0" ]]; then
  echo "[ERROR] DB 側の末尾レス削除が 0 件でした。今回の smoke は意図どおりの follow-up 検証になっていません。" >&2
  exit 1
fi

if [[ "${JSON_TRIMMED_COUNT}" = "0" ]]; then
  echo "[ERROR] JSON 側の末尾レス削除が 0 件でした。resume 元が残るため follow-up 検証になりません。" >&2
  exit 1
fi

run_cmd docker compose -f "${COMPOSE_FILE}" exec "${SERVICE}" python main.py verify kgs fetch "${ARTICLE_URL}" --state-dir "${STATE_CONTAINER}"

STATE2="$(read_article_state)"
if [[ "${STATE2}" = "NO_ARTICLE" ]]; then
  echo "[ERROR] follow-up fetch 後に isolated DB から article を読めませんでした" >&2
  exit 1
fi

IFS=$'\t' read -r ARTICLE_ID2 ARTICLE_TYPE2 ARTICLE_TITLE2 ARTICLE_CANONICAL2 AFTER_COUNT AFTER_MAX <<< "${STATE2}"

echo "[INFO] 再取得後 response_count=${AFTER_COUNT} max_res_no=${AFTER_MAX}"

if (( AFTER_COUNT < BEFORE_COUNT )); then
  echo "[ERROR] 再取得後 response_count が初回保存時より少ないです: before=${BEFORE_COUNT} after=${AFTER_COUNT}" >&2
  exit 1
fi

if (( AFTER_MAX < BEFORE_MAX )); then
  echo "[ERROR] 再取得後 max_res_no が初回保存時より小さいです: before=${BEFORE_MAX} after=${AFTER_MAX}" >&2
  exit 1
fi

run_cmd docker compose -f "${COMPOSE_FILE}" exec "${SERVICE}" python main.py verify telemetry export --db "${DB_CONTAINER}" --output "${EXPORT_CONTAINER}"

echo
echo "[PASS] KGS smoke が完了しました"
echo "[PASS] article_id=${ARTICLE_ID2} article_type=${ARTICLE_TYPE2}"
echo "[PASS] before_count=${BEFORE_COUNT} after_count=${AFTER_COUNT}"
echo "[PASS] before_max=${BEFORE_MAX} after_max=${AFTER_MAX}"
echo "[PASS] telemetry=${EXPORT_HOST}"


