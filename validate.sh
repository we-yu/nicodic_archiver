#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================================"
echo "VALIDATE: $(basename "$(cd "$(dirname "$0")" && pwd)")"
echo "repo   : ${REPO_DIR}"
echo "scope  : repo-local lint and test only"
echo "============================================================"
echo "AI-REMINDER: container-oriented repo; do not invent host venv or pip install flows"
echo

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is required for repo-local validation." >&2
  exit 1
fi

if [[ ! -f "${REPO_DIR}/docker-compose.yml" ]]; then
  echo "ERROR: docker-compose.yml not found in ${REPO_DIR}." >&2
  exit 1
fi

cd "${REPO_DIR}"
docker compose run --rm scraper sh -lc 'flake8 . && pytest'