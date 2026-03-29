#!/usr/bin/env sh
# Forward to bounded operator CLI (target registry + saved archive).
# Usage: from repo root, e.g. ./tools/nico-operator.sh target list data/foo.db
set -e
SCRIPT_DIR=$(CDPATH="" cd "$(dirname "$0")" && pwd)
ROOT=$(CDPATH="" cd "$SCRIPT_DIR/.." && pwd)
cd "$ROOT" || exit 1
exec python3 main.py operator "$@"
