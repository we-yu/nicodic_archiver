#!/usr/bin/env sh
set -eu

# One-shot periodic wrapper for external schedulers.
# This script intentionally does not implement cron/scheduler behavior.

if [ "$#" -lt 1 ]; then
  echo "Usage: runtime/scripts/periodic_one_shot.sh <target_list_path> [lock_path]"
  exit 1
fi

TARGET_LIST_PATH="$1"
LOCK_PATH="${2:-data/periodic_one_shot.lock}"

python3 main.py periodic-one-shot "$TARGET_LIST_PATH" --lock-path "$LOCK_PATH"
