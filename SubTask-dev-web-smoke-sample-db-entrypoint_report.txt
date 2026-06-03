Editor AI model: GPT-5.4

Summary
- Added a repo-local, editor-friendly helper at tools/dev_web_smoke.sh for
  bounded read-only verification of the child-repo Dev sample DB used by Web
  smoke checks.
- The helper makes missing, stale-looking, or oversized DB situations clearer
  without copying, rebuilding, or modifying any SQLite DB.
- Added focused tests for missing-DB guidance and successful read-only sample
  DB validation, plus a short runtime doc entry.

Files changed
- tools/dev_web_smoke.sh
- tests/test_runtime_local_ops.py
- docs/PERSONAL_RUNTIME.md
- SubTask-dev-web-smoke-sample-db-entrypoint_report.txt

Helper name and behavior
- Helper: tools/dev_web_smoke.sh
- Runs from the child repo root.
- Prints the repo root and DB path it will inspect.
- Opens the target DB read-only through Python stdlib sqlite3 with
  mode=ro.
- Fails fast with clear guidance if the DB is missing, empty, too large, or
  missing required smoke-check data.
- Does not copy, build, migrate, or modify any DB.

Expected Dev sample DB path
- Default path: runtime/data/nicodic.db
- Test override: DEV_WEB_SMOKE_DB_PATH

What read-only DB checks are performed
- DB file exists.
- DB file size is non-zero.
- DB file size does not exceed the helper's dev-sample upper bound.
- SQLite can open the DB in read-only mode.
- Required tables exist: articles, responses, target.
- Article count is non-zero.
- Response count is non-zero.
- Target count is non-zero.
- Active target count is non-zero.
- Titled article count is non-zero.
- No responses exist for article_id=5511090, article_type=a.
- No article exceeds the expected per-article response cap.

What happens when the DB is missing
- The helper exits non-zero.
- It prints the DB path it tried to inspect.
- It explains that this child repo does not build sample DBs.
- It points the operator/editor back to root/meta
  RuntimeOps-build-dev-sample-db.

How this relates to root/meta RuntimeOps-build-dev-sample-db
- This SubTask does not build or distribute the sample DB.
- It assumes root/meta tooling owns sample DB extraction and distribution.
- The child-repo helper only verifies that the distributed DB looks suitable
  for lightweight Web smoke checks.

Tests or validation run
- chmod +x tools/dev_web_smoke.sh
- bash -n tools/dev_web_smoke.sh
- bash tools/dev_web_smoke.sh
  Result: passed against runtime/data/nicodic.db
- Manual missing-DB check using DEV_WEB_SMOKE_DB_PATH to a temp missing path
  Result: passed, emitted expected guidance and non-zero exit
- Manual read-only temp SQLite DB check using DEV_WEB_SMOKE_DB_PATH
  Result: passed, emitted smoke_ready=yes
- Python compile() syntax check for tests/test_runtime_local_ops.py
  Result: passed
- awk line-length check for edited/created files
  Result: passed
- git diff --check
  Result: passed
- Full ./validate.sh
  Result: not run for this SubTask

Safety boundaries preserved
- No runtime DB copy.
- No production DB backup.
- No live scraping.
- No schema migration.
- No Web behavior redesign.
- No Docker restart.
- No cron changes.
- No runtime checkout edits.
- No root/meta edits.
- No cursor edits.
- runtime/data/nicodic.db was only read, never modified.

Known limitations
- The helper uses Python stdlib sqlite3 and therefore requires python3 on the
  host shell.
- The helper enforces a bounded size ceiling and response-cap expectation from
  child-repo-local assumptions; if root/meta intentionally changes the sample
  profile, these thresholds may need a matching update here.
- The focused tests were added in-repo, but host pytest was not used because
  this task explicitly avoids ad-hoc host pytest flows.

Recommended human follow-up
- If the helper reports a missing or oversized DB, regenerate and redistribute
  the Dev sample DB via root/meta RuntimeOps-build-dev-sample-db.
- If the distributed sample profile changes, update the helper thresholds and
  this child-repo documentation together.
Human follow-up validation:
- ./validate_helix.sh
  Result: copilot 458 passed, cursor 456 passed
- tools/dev_web_smoke.sh
  Result: passed against copilot/runtime/data/nicodic.db with smoke_ready=yes
