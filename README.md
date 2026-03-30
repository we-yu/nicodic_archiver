# nicodic_archiver

Nicopedia BBS comment archiver with a bounded SQLite-backed target registry,
saved archive reads, and operator-first CLI tooling.

## Scope

This repository centers on three bounded seams:

- scrape and save Nicopedia BBS threads
- keep a DB-backed target registry for periodic operation
- inspect and export saved archives without turning the product into a full admin console

The operator interface for target registry and saved archive management is CLI-first.
Web admin expansion is intentionally out of scope.

## Operator Entry Points

Two repo-local entry points expose the same bounded operator tooling:

- python main.py operator ...
- sh tools/operator.sh ...

Supported target actions:

- target list
- target inspect
- target add
- target deactivate
- target reactivate

Supported archive actions:

- archive list
- archive inspect
- archive export

Examples:

```sh
sh tools/operator.sh target list
sh tools/operator.sh target inspect 12345 a
sh tools/operator.sh target add https://dic.nicovideo.jp/a/12345
sh tools/operator.sh target deactivate 12345 a
sh tools/operator.sh target reactivate 12345 a
sh tools/operator.sh archive list
sh tools/operator.sh archive inspect 12345 a --last 20
sh tools/operator.sh archive export 12345 a --format md --output exports/12345a.md
```

The target add action accepts canonical Nicopedia article URLs only. This keeps
operator intent non-ambiguous and avoids mixing registry management with article
resolution flow.

## Verification Entry Points

This repository also includes bounded verification / smoke helpers for a single
operator.

Two repo-local entry points expose that verification tooling:

- python main.py verify ...
- sh tools/verify.sh ...

Supported verification actions:

- fetch one canonical article
- check current target registry state
- run one one-shot batch verification pass
- export telemetry CSV for verification review

Read-first verification flow:

1. check current registry state
2. run one light one-shot fetch when needed
3. inspect the saved archive through the existing operator tooling
4. run one one-shot batch verification only when needed
5. export telemetry CSV only when needed for review

## Existing Flow Commands

The original bounded entry points remain available:

- python main.py <article_url>
- python main.py batch <target_db_path>
- python main.py periodic <target_db_path> <interval_seconds>
- python main.py web [--host HOST] [--port PORT] [--target-db-path PATH]
- python main.py export-run-telemetry-csv [--db PATH] [--output PATH]

Telemetry CSV export remains a support layer. It is not the primary operator
interface for registry or archive management.

## Docs

See the operator guide in docs/OPERATOR_TOOLING.md for daily and periodic
registry/archive management.

See docs/VERIFICATION_TOOLING.md for bounded verification / smoke tooling and
the read-first verification flow.

See docs/PERSONAL_RUNTIME.md for runtime-container-specific notes.
