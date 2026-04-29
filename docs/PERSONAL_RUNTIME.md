# Personal Runtime Profile

This repository includes a separate provisional runtime profile for
personal-use operation.

It is distinct from the lightweight development container settings and is
intended to keep a long-lived container available for manual CLI use.

What this profile does:
- builds a runtime image from Dockerfile.runtime
- keeps one single-operator web runtime running on a host IP:port
- preserves the target registry, SQLite data, archives, and batch logs through mounts

What this profile does not do yet:
- cron or repeated scheduling inside the container
- Web admin expansion beyond the existing bounded archive-check flow

What this profile now adds for periodic operation:
- a host-side one-shot wrapper that calls the existing periodic path
- simple lock + skip handling to avoid overlap
- a scheduler-friendly non-interactive invocation shape
- a structured host cron log at runtime/logs/host_cron.log
- daily rollover into host_cron.YYYYMMDD.log on the next run start
- weekly tar.gz compaction for older daily host cron logs
- a local-only runtime env file for host bind/port and UID/GID settings
- a recreate wrapper with bounded runtime preflight checks

What this profile now adds for web operation:
- starts the existing web app inside the runtime container
- publishes the web app on a host-visible IP:port mapping
- keeps target intake bounded to canonical article URLs in SQLite

## Mounted Paths

The runtime profile uses these child-repo-local paths:
- legacy import source: runtime/targets
- SQLite and saved JSON archives: runtime/data
- batch and periodic logs: runtime/logs

Container-side path mapping:
- runtime/targets -> /runtime/targets
- runtime/data -> /app/data
- runtime/logs -> /runtime/logs

Because the application already reads and writes under data/ and uses the
BATCH_LOG_DIR environment variable for batch logs, no product behavior needs
to change.

The runtime profile sets TARGET_DB_PATH to /app/data/nicodic.db for the web
process and periodic wrapper.

## Local Runtime Env

Use `.env.runtime.local` as the local-only runtime config for this repo.

Expected keys:
- `WEB_BIND_HOST`
- `WEB_PORT`
- `LOCAL_UID`
- `LOCAL_GID`
- `SCRAPE_PAGE_DELAY_SECONDS`
- `BBS_RESPONSES_PER_PAGE`

The file is intentionally local-only and should not be committed. Use the
tracked template as a starting point:

`cp .env.runtime.local.example .env.runtime.local`

The runtime helper scripts automatically load this file when it exists. If the
file is missing, the helper falls back to safe defaults and auto-detects the
host UID/GID to reduce common permission mismatches.

`SCRAPE_PAGE_DELAY_SECONDS` controls the delay between BBS page fetches during
scrape pagination. If it is unset or invalid, the application falls back to
`5.0` seconds.

`BBS_RESPONSES_PER_PAGE` controls the BBS page boundary size used for resume
and later-page progression. If it is unset or invalid, the application falls
back to `30` responses per page.

## Host UID/GID Handling

To keep generated files writable from the host shell, the runtime helper loads
`LOCAL_UID` and `LOCAL_GID` from `.env.runtime.local` when present. If they are
unset, the helper auto-detects the current host UID/GID before `docker compose`
starts.

## Build And Start

Build and start the provisional runtime container:

`bash tools/runtime_up.sh`

This wrapper performs bounded local preflight work before compose startup:
- loads `.env.runtime.local` when present
- auto-detects `LOCAL_UID` / `LOCAL_GID` if missing
- validates `WEB_PORT`
- warns when the requested host port already looks busy
- uses `--build --force-recreate` to reduce stale container code

The container starts the existing web app on port 8000 inside the container.
By default, docker-compose.runtime.yml publishes that as 127.0.0.1:8000 on
the host.

Open the web UI from the host browser at:

`http://127.0.0.1:8000`

To bind a different host IP or port without editing files:

edit `.env.runtime.local`, then run:

`bash tools/runtime_up.sh`

The web app remains a thin entrypoint. A web-side registration adds only the
canonical article URL to the target table in /app/data/nicodic.db. It does not
enqueue or scrape immediately; the next periodic or batch pass performs the
actual scrape.

The web UI is intentionally separated from immediate execution. It performs
bounded validation / existence checks, target registration, and saved article
TXT download only.

## Stop

Stop and remove the provisional runtime container:

`docker compose -f docker-compose.runtime.yml down`

## Target Registry Location

The authoritative scrape target registry now lives in:

`runtime/data/nicodic.db`

The active scrape target source-of-truth is the SQLite `target` table.

The web UI writes only to that table when a resolved article is added through
the Add To Target Registry action.

The legacy plain-text file remains available only as an admin import source:

`runtime/targets/targets.txt`

Manual one-shot import example:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	python main.py import-targets /runtime/targets/targets.txt /app/data/nicodic.db
```

## Initial Smoke Test

On a fresh runtime data directory, run one scrape or batch pass before using
`list-articles`.

Reason:
- the runtime profile itself can start cleanly with an empty mounted data area
- but `list-articles` expects the SQLite archive schema to already exist
- the first scrape or batch pass creates the initial archive DB state

Recommended first-pass smoke test:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	python main.py batch /app/data/nicodic.db
```

After that, this command should work as expected:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	python main.py list-articles
```

## Common Commands

For bounded registry/archive management inside the runtime container, prefer the
operator CLI documented in docs/OPERATOR_TOOLING.md.

Follow the published web runtime logs:

`docker compose -f docker-compose.runtime.yml logs -f personal_runtime`

Show the active targets currently stored in the registry:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	sh tools/operator.sh target list --active-only --db /app/data/nicodic.db
```

Inspect one registered target:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	sh tools/operator.sh target inspect 12345 a --db /app/data/nicodic.db
```

Add one canonical target:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	sh tools/operator.sh target add \
		https://dic.nicovideo.jp/a/12345 --db /app/data/nicodic.db
```

Deactivate one target without deleting it:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	sh tools/operator.sh target deactivate 12345 a \
		--db /app/data/nicodic.db
```

Run one batch pass:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	python main.py batch /app/data/nicodic.db
```

Run the current periodic CLI entrypoint manually:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	python main.py periodic /app/data/nicodic.db 300
```

Run the web app manually with an explicit target DB path if needed:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	python main.py web --host 0.0.0.0 --port 8000 \
	--target-db-path /app/data/nicodic.db
```

Run one scheduler-friendly periodic cycle through the wrapper:

`./runtime/periodic_once.sh`

The wrapper acquires a simple host-side lock under `runtime/logs`. If another
run is already active, it prints a skip message and exits without starting a
second overlapping periodic pass.

When the wrapper invokes `periodic-once`, it also passes a mounted log path for
the host cron log. That path is used for bounded host-side log hygiene:
- active log: `runtime/logs/host_cron.log`
- daily rollover: `runtime/logs/host_cron.YYYYMMDD.log`
- weekly archive: `runtime/logs/host_cron.YYYYMMDD-YYYYMMDD.tar.gz`

The active log is rotated only at the start of the next cron run. A long run
that crosses midnight stays in the log for its start date, and the next run
performs the rollover before writing a new block.

Daily logs for the most recent 14 days stay uncompressed for readability.
Older daily logs are compacted by calendar week into `.tar.gz` archives. The
archive cleanup is conservative: originals are removed only after a successful
archive write.

Each cron run is written as one readable block with these tags:
- `[RUN]`
- `[INFO]`
- `[STEP]`
- `[OK]`
- `[WARN]`
- `[ERROR]`
- `[SUMMARY]`
- `[ERROR SUMMARY]`

Article progress lines are compressed to one line per page, for example:

```text
	[STEP] 1/3 title=UNIX url=https://dic.nicovideo.jp/a/694740
		[INFO] page=https://dic.nicovideo.jp/b/a/694740/1- collected=30 total=30
	[OK] UNIX success total_collected=30
```

Useful environment overrides for external schedulers:
- `TARGET_DB_PATH` defaults to `/app/data/nicodic.db`
- `COMPOSE_FILE_PATH` defaults to `docker-compose.runtime.yml`
- `COMPOSE_SERVICE_NAME` defaults to `personal_runtime`
- `LOCK_DIR_PATH` defaults to `runtime/logs/periodic_once.lock`
- `RUNTIME_LOCAL_ENV_FILE` defaults to `.env.runtime.local`

Example scheduler-facing invocation shape:

`TARGET_DB_PATH=/app/data/nicodic.db ./runtime/periodic_once.sh`

List saved articles:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	sh tools/operator.sh archive list
```

Inspect one saved archive:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	sh tools/operator.sh archive inspect 12345 a --last 20
```

Export all saved articles:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	python main.py export-all-articles --format txt
```

Export one saved archive to a file:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	sh tools/operator.sh archive export 12345 a --format md \
	--output /runtime/data/exports/12345a.md
```

## Cleanup After A Smoke Test

If you want to discard temporary runtime data after a smoke test:

`docker compose -f docker-compose.runtime.yml down`

`find runtime/data -mindepth 1 ! -name '.gitkeep' -delete`

`find runtime/logs -mindepth 1 ! -name '.gitkeep' -delete`

`rm -f runtime/targets/targets.txt`

With the inline host UID/GID setup above, these cleanup commands should
normally work without requiring `sudo chown`.

## Persistence Expectations

The following repository-local directories act as persistence anchors for the
runtime profile:
- runtime/targets: optional legacy import source files
- runtime/data: SQLite database plus saved article archives
- runtime/logs: batch and periodic run logs

These directories are included only as a provisional runtime shape. A later
task can package scheduling or host deployment more tightly if needed.
