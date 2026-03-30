#!/usr/bin/env sh
# Forward to bounded verification CLI (TASK033).
set -eu

SCRIPT_DIR=$(CDPATH="" cd "$(dirname "$0")" && pwd)
ROOT=$(CDPATH="" cd "$SCRIPT_DIR/.." && pwd)
cd "$ROOT" || exit 1

exec python3 main.py verify "$@"

