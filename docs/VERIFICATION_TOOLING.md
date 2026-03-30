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

## KGS Positioning

Known-good smoke (KGS) is part of this verification tooling.

It is:

- manual
- opt-in
- non-gating
- isolated from the main working state

It is not:

- a default validation gate
- a scheduler platform
- a destructive maintenance flow
- a new execution platform

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
3. run KGS only when you need live smoke on isolated state
4. inspect the saved archive through the existing operator tooling
5. run one one-shot batch verification only when you need a broader pass
6. export telemetry CSV only when you need verification evidence or follow-up review

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

### Known-Good Smoke (KGS)

Run a one-shot live smoke fetch on isolated state:

```sh
sh tools/verify.sh kgs fetch https://dic.nicovideo.jp/a/12345
```

Run the same fetch with an explicit isolated state directory:

```sh
sh tools/verify.sh kgs fetch https://dic.nicovideo.jp/a/12345 \
	--state-dir runtime/smoke/my_kgs
```

Run a bounded incremental follow-up smoke on isolated state only:

```sh
sh tools/verify.sh kgs fetch https://dic.nicovideo.jp/a/12345 --followup-drop-last 3
```

Run a one-shot KGS batch verification on isolated state:

```sh
sh tools/verify.sh kgs batch https://dic.nicovideo.jp/a/12345
```

KGS rules:

- the known-good target is configurable per invocation
- the helper uses isolated smoke state, not the main working DB/archive
- KGS-specific guidance messages are stdout-only
- KGS remains non-gating and manual

Default isolated state directory:

```text
runtime/smoke/kgs
```

Inside that state directory the helper uses:

- `data/nicodic.db` for the isolated DB/archive state
- `logs/` for the existing batch log behavior when KGS batch is used

The helper does not add a new KGS-only persistent log layer. KGS-specific phase
and status guidance is printed to stdout only.

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
sh tools/verify.sh telemetry export --db runtime/data/nicodic.db \
	--output exports/run_telemetry.csv
```

This helper stays in the support layer. It does not become a dashboard.

After a KGS run, point telemetry export at the isolated DB when you want to
review smoke telemetry:

```sh
sh tools/verify.sh telemetry export --db runtime/smoke/kgs/data/nicodic.db
```

## What This Tooling Does Not Do

- It does not introduce destructive maintenance.
- It does not introduce a retry / worker / scheduler framework.
- It does not introduce a healthcheck platform or gating CI flow.
- It does not replace the existing operator tooling for archive inspection and export.
- It does not broaden into Web admin.

## Live Smoke Scope

TASK033B adds a bounded KGS helper inside the verification tooling.

Its boundaries are strict:

- manual / opt-in only
- isolated state only
- non-gating only
- stdout-only KGS helper guidance
- no broad redesign of runtime, batch, or telemetry behavior