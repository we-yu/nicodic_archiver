# Verification / Smoke Tooling (TASK033)

This guide documents bounded, human-friendly **verification / smoke**
helpers for one single operator.

The helpers **reuse existing seams**:
- target registry: SQLite-backed target `list/inspect` reads
- archive read/export: `list-articles`, `inspect`, `export` functions
- batch/periodic baseline: existing `batch` / `periodic` code paths
- telemetry CSV export: `format_run_telemetry_csv_wide` via the storage layer

They are intentionally **not** a platform or admin console.

## Goal

Single-operator daily use should follow a safe, read-first flow:

1. **See current state** (target registry + saved archive)
2. Do a **light one-shot check** for a specific article
3. If needed, run a **bounded one-shot smoke** in isolated state
4. Use **telemetry CSV** as a human-readable follow-up reference

## What you can verify

- Target registry state: active vs inactive targets, and specific row details
- Saved archive state: whether an article exists and a bounded inspection of
  its latest responses
- Batch readiness: which active targets already exist in the saved archive
- One-shot archive creation: fetch/scrape one bounded target in isolated state
- Telemetry CSV: human-readable evidence of scrape runs (in isolated state
  or a DB you pass explicitly)

## Entry points

- Primary: `python main.py verify ...`
- Optional wrapper: `sh tools/nico-verify.sh ...`

Run `python main.py verify help` to see command patterns.

## Read-first flow (recommended)

### 1) See current state

```sh
python main.py verify state --target-db data/nicodic.db
```

### 2) Confirm one article in the saved archive (light check)

```sh
python main.py verify article check https://dic.nicovideo.jp/a/12345
```

If you want a bounded terminal view of the latest responses:

```sh
python main.py verify article check https://dic.nicovideo.jp/a/12345 \
  --show-last 5
```

### 3) Optional: bounded live smoke (isolated only)

Live scraping is opt-in and runs only inside isolated smoke state:

```sh
python main.py verify batch smoke \
  --known-good-url https://dic.nicovideo.jp/a/12345 \
  --isolated
```

For periodic semantics:

```sh
python main.py verify periodic smoke \
  --known-good-url https://dic.nicovideo.jp/a/12345 \
  --isolated
```

Notes:
- `--isolated` is required for live smoke to avoid touching main DB/archive.
- The smoke target must be a low-volume, human-safe article you trust.
- Smoke uses an isolated cwd and overrides `NICODIC_DB_PATH` for telemetry
  safety.

### 4) Inspect telemetry CSV (support layer)

After a smoke run (or from any DB you pass):

```sh
python main.py verify telemetry csv --db data/nicodic.db --head-lines 20
```

## Command reference (bounded operator seam)

### Target registry confirmation

```sh
python main.py verify targets list <target_db_path> [--all] [--limit N]
python main.py verify targets inspect <target_db_path> <article_id> <article_type>
```

### Saved archive confirmation

```sh
python main.py verify archive list
python main.py verify archive inspect <article_id> <article_type> [--last N]
```

### One-shot article fetch helper (safe by default)

`verify article fetch` is live scraping and therefore **requires isolated**
mode:

```sh
python main.py verify article fetch <url_or_full_title> --isolated
```

Optional bounded controls:

- `--response-cap N`: scrape cap used by the existing orchestrator
- `--inspect-last N`: bounded post-fetch terminal view
- `--follow-up`: run the fetch twice in the same isolated state to check
  bounded resume/idempotence

### One-shot batch / periodic confirmation

Offline (no scraping):

```sh
python main.py verify batch check --target-db <target_db_path> \
  [--max-targets N]
```

Live smoke in isolated state:

- Batch:
  ```sh
  python main.py verify batch smoke \
    --known-good-url <canonical_url> --isolated
  ```
- Periodic:
  ```sh
  python main.py verify periodic smoke \
    --known-good-url <canonical_url> --isolated
  ```

### Telemetry CSV export (verification follow-up)

```sh
python main.py verify telemetry csv [--db PATH] [--head-lines N] \
  [--output PATH]
```

This CSV is for human inspection only (not gating / not a source of truth).

## Non-goals (intentional boundary)

This verification tooling does **not**:
- add a new source-of-truth
- redesign registry, archive, telemetry, or execution semantics
- schedule repeated jobs / add cron-like orchestration
- add dashboard/analytics/alerting
- add web admin / GUI admin
- perform destructive actions (delete, requeue, re-fetch maintenance)

## Live smoke boundedness (explicit)

Live smoke:
- is opt-in via `--isolated`
- uses isolated cwd so archive read/export works without touching main
- overrides `NICODIC_DB_PATH` during the command
- prints phase markers and a concise success/failure summary to stdout

