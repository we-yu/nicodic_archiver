# Operator Tooling

This document describes the bounded CLI / shell tooling for a single operator.

It covers only:

- target registry management
- saved archive management

It does not cover:

- destructive delete actions
- archive requeue or re-fetch flows
- telemetry analysis or dashboard expansion
- Web admin expansion

## First Interface

The first operator interface is repo-local CLI / shell tooling.

Use either of these equivalent entry points:

```sh
python main.py operator ...
sh tools/operator.sh ...
```

The shell wrapper is intentionally thin. All behavior lives in the Python CLI.

## Target Registry

The target registry is SQLite-backed and non-destructive.

Supported actions:

- list
- inspect
- add
- deactivate
- reactivate

### List Targets

Show all registered targets with explicit active / inactive status:

```sh
sh tools/operator.sh target list
```

Show active entries only:

```sh
sh tools/operator.sh target list --active-only
```

Use an explicit registry DB path when needed:

```sh
sh tools/operator.sh target list --db runtime/data/nicodic.db
```

### Inspect One Target

Inspect one registered target by canonical identity:

```sh
sh tools/operator.sh target inspect 12345 a
```

The inspect action is identity-based on purpose:

- article_id
- article_type

This avoids ambiguous lookup behavior.

### Add One Canonical Target

Add one canonical Nicopedia article URL to the registry:

```sh
sh tools/operator.sh target add https://dic.nicovideo.jp/a/12345
```

Accepted input is intentionally narrow:

- canonical article URL only

Title resolution is a separate flow and is not mixed into registry management.

### Deactivate Or Reactivate

Deactivate one target without deleting it:

```sh
sh tools/operator.sh target deactivate 12345 a
```

Reactivate the same target later:

```sh
sh tools/operator.sh target reactivate 12345 a
```

These actions only flip the active state. They do not remove history.

## Saved Archive

The saved archive tooling is read-focused.

Supported actions:

- list
- inspect
- export

### List Saved Archives

```sh
sh tools/operator.sh archive list
```

This prints a concise human-readable summary per saved article:

- article_id
- article_type
- title
- response count
- created_at

### Inspect One Saved Archive

Inspect all saved responses for one article:

```sh
sh tools/operator.sh archive inspect 12345 a
```

Inspect only the latest responses when you need a bounded view:

```sh
sh tools/operator.sh archive inspect 12345 a --last 20
```

### Export One Saved Archive

Write a practical export to a file:

```sh
sh tools/operator.sh archive export 12345 a --format txt --output exports/12345a.txt
sh tools/operator.sh archive export 12345 a --format md --output exports/12345a.md
```

When no output path is provided, export is written to stdout.

## Daily Operator Shape

Daily or periodic operator use should usually look like this:

1. list current targets
2. inspect a target if status or identity needs confirmation
3. add a new canonical target when needed
4. deactivate a target instead of deleting it
5. list saved archives after normal scrape flow runs
6. inspect or export one archive when follow-up review is needed

This keeps registry management and archive review bounded, readable, and
non-ambiguous.
