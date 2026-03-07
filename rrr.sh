#!/bin/bash
set -e

ARTICLE_URL="https://dic.nicovideo.jp/a/プロイセン(APヘタリア)"
ARTICLE_ID="4470620"
ARTICLE_TYPE="a"

echo "=== RESET DB ==="
sudo rm -f data/nicodic.db || true

echo "=== BUILD CONTAINER ==="
docker compose build > /dev/null

echo "=== SCRAPE ==="
docker compose run --rm scraper python main.py "$ARTICLE_URL"

echo "=== CHECK JSON ==="
JSON_COUNT=$(jq '.response_count' data/*.json)

echo "JSON responses: $JSON_COUNT"

echo "=== CHECK SQLITE ==="
DB_COUNT=$(sqlite3 data/nicodic.db "SELECT COUNT(*) FROM responses WHERE article_id='$ARTICLE_ID';")

echo "DB responses: $DB_COUNT"

echo "=== VERIFY ==="

if [ "$JSON_COUNT" = "$DB_COUNT" ]; then
  echo "OK: JSON and DB counts match"
else
  echo "ERROR: mismatch"
  exit 1
fi

echo "=== LAST RESPONSES ==="
docker compose run --rm scraper python main.py inspect $ARTICLE_ID $ARTICLE_TYPE --last 10

echo "=== TEST COMPLETE ==="

