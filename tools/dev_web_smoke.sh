#!/usr/bin/env bash
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEFAULT_DB_PATH="runtime/data/nicodic.db"
DEFAULT_MAX_DB_BYTES=$((10 * 1024 * 1024))
DEFAULT_RESPONSE_CAP=200

DB_PATH="${DEV_WEB_SMOKE_DB_PATH:-$DEFAULT_DB_PATH}"
MAX_DB_BYTES="${DEV_WEB_SMOKE_MAX_DB_BYTES:-$DEFAULT_MAX_DB_BYTES}"
RESPONSE_CAP="${DEV_WEB_SMOKE_RESPONSE_CAP:-$DEFAULT_RESPONSE_CAP}"

cd "$REPO_ROOT"

echo "[dev-web-smoke] repo_root=$REPO_ROOT"
echo "[dev-web-smoke] db_path=$DB_PATH"

if [[ ! -f "$DB_PATH" ]]; then
  echo "[dev-web-smoke] Missing dev sample DB: $DB_PATH" >&2
  echo "[dev-web-smoke] This child repo does not build sample DBs." >&2
  echo "[dev-web-smoke] Re-run root/meta RuntimeOps-build-dev-sample-db" >&2
  echo "[dev-web-smoke] and distribute the generated sample DB here." >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "[dev-web-smoke] python3 is required for read-only SQLite checks." >&2
  exit 1
fi

python3 - "$DB_PATH" "$MAX_DB_BYTES" "$RESPONSE_CAP" <<'PY'
import sqlite3
import sys
from pathlib import Path


def fail(message: str) -> None:
    print(f"[dev-web-smoke] {message}", file=sys.stderr)
    raise SystemExit(1)


db_path = Path(sys.argv[1])
max_db_bytes = int(sys.argv[2])
response_cap = int(sys.argv[3])
file_size = db_path.stat().st_size

print(f"[dev-web-smoke] db_size_bytes={file_size}")
print(f"[dev-web-smoke] expected_response_cap={response_cap}")

if file_size <= 0:
    fail("DB exists but is empty.")

if file_size > max_db_bytes:
    fail(
        "DB looks too large for the dev sample "
        f"({file_size} bytes > {max_db_bytes} bytes)."
    )

conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
cur = conn.cursor()

cur.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
)
table_names = {row[0] for row in cur.fetchall()}
required_tables = {"articles", "responses", "target"}
missing_tables = sorted(required_tables - table_names)
if missing_tables:
    fail("Missing required tables: " + ", ".join(missing_tables))

checks = {
    "article_count": "SELECT COUNT(*) FROM articles",
    "response_count": "SELECT COUNT(*) FROM responses",
    "target_count": "SELECT COUNT(*) FROM target",
    "active_target_count": (
        "SELECT COUNT(*) FROM target WHERE is_active = 1"
    ),
    "titled_article_count": (
        "SELECT COUNT(*) FROM articles WHERE title IS NOT NULL AND title <> ''"
    ),
    "forbidden_delete_request_responses": (
        "SELECT COUNT(*) FROM responses "
        "WHERE article_id = '5511090' AND article_type = 'a'"
    ),
    "max_saved_responses_per_article": (
        "SELECT COALESCE(MAX(cnt), 0) FROM ("
        "SELECT COUNT(*) AS cnt FROM responses GROUP BY article_id, article_type"
        ")"
    ),
}

results: dict[str, int] = {}
for label, sql in checks.items():
    cur.execute(sql)
    results[label] = int(cur.fetchone()[0])

conn.close()

if results["article_count"] <= 0:
    fail("No saved articles found; Web smoke checks are not ready.")

if results["response_count"] <= 0:
    fail("No saved responses found; Web smoke checks are not ready.")

if results["target_count"] <= 0 or results["active_target_count"] <= 0:
    fail("No active targets found; registry-backed smoke checks are too thin.")

if results["titled_article_count"] <= 0:
    fail("No titled articles found; Web-facing smoke checks are too thin.")

if results["forbidden_delete_request_responses"] != 0:
    fail(
        "Found responses for article_id=5511090 article_type=a; "
        "dev sample DB is not acceptable."
    )

if results["max_saved_responses_per_article"] > response_cap:
    fail(
        "At least one article exceeds the expected dev sample response cap "
        f"({results['max_saved_responses_per_article']} > {response_cap})."
    )

for label in (
    "article_count",
    "response_count",
    "target_count",
    "active_target_count",
    "titled_article_count",
    "forbidden_delete_request_responses",
    "max_saved_responses_per_article",
):
    print(f"[dev-web-smoke] {label}={results[label]}")

print("[dev-web-smoke] smoke_ready=yes")
PY
