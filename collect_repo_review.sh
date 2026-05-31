#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================================"
echo "REVIEW SNAPSHOT: copilot"
echo "repo   : ${REPO_DIR}"
echo "branch : $(git -C "${REPO_DIR}" branch --show-current)"
echo "============================================================"
echo
echo "-- git status --short"
git -C "${REPO_DIR}" status --short || true
echo
echo "-- staged diff stat"
git -C "${REPO_DIR}" diff --cached --stat || true
echo
echo "-- unstaged diff stat"
git -C "${REPO_DIR}" diff --stat || true
echo
echo "-- recent log"
git -C "${REPO_DIR}" log --oneline --decorate --graph -5 || true
echo