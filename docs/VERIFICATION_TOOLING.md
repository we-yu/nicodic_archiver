# Verification Tooling

This document describes the bounded verification / smoke tooling for a single
operator.

It exists to make existing seams easy to check safely and readably.

It reuses:

- the existing one-shot scrape entry
- the existing target registry seam
- the existing batch / periodic baseline
- the existing telemetry / CSV export support layer
- the existing operator tooling seam

It does not add:

- a scheduler platform
- a dashboard or analytics platform
- destructive maintenance actions
- a default validation gate
- a new source-of-truth

## First Interface

Use either of these equivalent repo-local entry points:

```sh
python main.py verify ...
sh tools/verify.sh ...
```

The shell wrapper is intentionally thin. The Python CLI remains the source of
behavior.

## Read-First Verification Flow

Use this order unless you have a specific reason not to:

1. check current registry state
2. run one light one-shot article fetch when needed
3. inspect the saved archive through the existing operator tooling
4. run one one-shot batch verification only when you need a broader pass
5. export telemetry CSV only when you need verification evidence or follow-up review

This keeps verification read-first, one-shot, and non-ambiguous.

## Helpers

### Registry Check

List current registry state:

```sh
sh tools/verify.sh registry list
```

List active targets only:

```sh
sh tools/verify.sh registry list --active-only
```

Inspect one target by canonical identity:

```sh
sh tools/verify.sh registry inspect 12345 a
```

Use `--db PATH` when you need an explicit registry DB path.

### One-Shot Article Fetch

Run a single verification fetch for one canonical article URL:

```sh
sh tools/verify.sh fetch https://dic.nicovideo.jp/a/12345
```

This reuses the existing scrape flow and then prints a concise saved-archive
summary when the fetch succeeds.

Input is intentionally narrow:

- canonical Nicopedia article URL only

That keeps the helper action-oriented and avoids mixing verification fetch with
title-resolution behavior.

### Archive Follow-Up

Archive follow-up remains on the existing operator tooling seam:

```sh
sh tools/operator.sh archive inspect 12345 a --last 20
sh tools/operator.sh archive export 12345 a --format md --output exports/12345a.md
```

Verification tooling intentionally reuses that existing archive read / export
interface instead of creating a second archive management path.

### One-Shot Batch Verification

Run one bounded batch verification pass:

```sh
sh tools/verify.sh batch run
```

Use an explicit target registry DB path when needed:

```sh
sh tools/verify.sh batch run --db runtime/data/nicodic.db
```

This is a one-shot helper. It is not a scheduler or orchestration framework.

### Telemetry CSV Reference

Export the existing telemetry CSV to stdout:

```sh
sh tools/verify.sh telemetry export
```

Write the CSV to a file for later review:

```sh
sh tools/verify.sh telemetry export --output exports/run_telemetry.csv
```

Use an explicit telemetry DB path when needed:

```sh
sh tools/verify.sh telemetry export --db runtime/data/nicodic.db --output exports/run_telemetry.csv
```

This helper stays in the support layer. It does not become a dashboard.

## What This Tooling Does Not Do

- It does not introduce destructive maintenance.
- It does not introduce a retry / worker / scheduler framework.
- It does not introduce a healthcheck platform or gating CI flow.
- It does not replace the existing operator tooling for archive inspection and export.
- It does not broaden into Web admin.

## Live Smoke Scope

TASK033 does not add a separate live smoke / KGS helper.

Verification remains manual and opt-in through the one-shot helpers above.
This keeps the repo-local implementation bounded and avoids introducing a new
execution path or isolated smoke state layer that would need to be maintained.