# Personal Runtime Profile

This repository includes a separate provisional runtime profile for
personal-use operation.

It is distinct from the lightweight development container settings and is
intended to keep a long-lived container available for manual CLI use.

What this profile does:
- builds a runtime image from Dockerfile.runtime
- keeps one single-operator web runtime running on a host IP:port
- preserves targets, SQLite data, archives, and batch logs through mounts

What this profile does not do yet:
- cron or repeated scheduling inside the container
- target storage migration away from the text file

What this profile now adds for periodic operation:
- a host-side one-shot wrapper that calls the existing periodic path
- simple lock + skip handling to avoid overlap
- a scheduler-friendly non-interactive invocation shape

What this profile now adds for web operation:
- starts the existing web app inside the runtime container
- publishes the web app on a host-visible IP:port mapping
- keeps target intake bounded to canonical article URLs in the text target list

## Mounted Paths

The runtime profile uses these child-repo-local paths:
- target list: runtime/targets
- SQLite and saved JSON archives: runtime/data
- batch and periodic logs: runtime/logs

Container-side path mapping:
- runtime/targets -> /runtime/targets
- runtime/data -> /app/data
- runtime/logs -> /runtime/logs

Because the application already reads and writes under data/ and uses the
BATCH_LOG_DIR environment variable for batch logs, no product behavior needs
to change.

The runtime profile also sets TARGET_LIST_PATH to /runtime/targets/targets.txt
for the web process.

## Host UID/GID Handling

To keep generated files writable from the host shell, pass the host UID/GID
only when starting the runtime container.

Recommended start command:

```sh
LOCAL_UID=$(id -u) LOCAL_GID=$(id -g) \
	docker compose -f docker-compose.runtime.yml up -d --build
```

This avoids relying on persistent host-side environment variable setup.
The values are resolved from the current host at container start time.

## Build And Start

Build and start the provisional runtime container:

```sh
LOCAL_UID=$(id -u) LOCAL_GID=$(id -g) \
	docker compose -f docker-compose.runtime.yml up -d --build
```

The container starts the existing web app on port 8000 inside the container.
By default, docker-compose.runtime.yml publishes that as 127.0.0.1:8000 on
the host.

Open the web UI from the host browser at:

`http://127.0.0.1:8000`

To bind a different host IP or port without editing files:

```sh
WEB_BIND_HOST=0.0.0.0 WEB_PORT=8010 \
LOCAL_UID=$(id -u) LOCAL_GID=$(id -g) \
	docker compose -f docker-compose.runtime.yml up -d --build
```

The web app remains a thin entrypoint. A web-side registration adds only the
canonical article URL to runtime/targets/targets.txt. It does not enqueue or
scrape immediately; the next periodic or batch pass performs the actual scrape.

The web UI is intentionally separated from immediate execution. It performs
bounded validation / existence checks, target-list registration, and saved
article TXT download only.

## Stop

Stop and remove the provisional runtime container:

`docker compose -f docker-compose.runtime.yml down`

## Target List Location

Create or edit the target list at:

`runtime/targets/targets.txt`

That file remains a human-editable plain text target source.

The web UI writes the same file when a resolved article is added through the
Add To Target List action.

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
	python main.py batch /runtime/targets/targets.txt
```

After that, this command should work as expected:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	python main.py list-articles
```

## Common Commands

Follow the published web runtime logs:

`docker compose -f docker-compose.runtime.yml logs -f personal_runtime`

Show the target list currently mounted into the runtime:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	python main.py targets /runtime/targets/targets.txt
```

Run one batch pass:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	python main.py batch /runtime/targets/targets.txt
```

Run the current periodic CLI entrypoint manually:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	python main.py periodic /runtime/targets/targets.txt 300
```

Run the web app manually with an explicit target list path if needed:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	python main.py web --host 0.0.0.0 --port 8000 \
	--target-list-path /runtime/targets/targets.txt
```

Run one scheduler-friendly periodic cycle through the wrapper:

`./runtime/periodic_once.sh`

The wrapper acquires a simple host-side lock under `runtime/logs`. If another
run is already active, it prints a skip message and exits without starting a
second overlapping periodic pass.

Useful environment overrides for external schedulers:
- `TARGET_LIST_PATH` defaults to `/runtime/targets/targets.txt`
- `COMPOSE_FILE_PATH` defaults to `docker-compose.runtime.yml`
- `COMPOSE_SERVICE_NAME` defaults to `personal_runtime`
- `LOCK_DIR_PATH` defaults to `runtime/logs/periodic_once.lock`

Example scheduler-facing invocation shape:

`TARGET_LIST_PATH=/runtime/targets/targets.txt ./runtime/periodic_once.sh`

List saved articles:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	python main.py list-articles
```

Export all saved articles:

```sh
docker compose -f docker-compose.runtime.yml exec personal_runtime \
	python main.py export-all-articles --format txt
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
- runtime/targets: the target list file you maintain
- runtime/data: SQLite database plus saved article archives
- runtime/logs: batch and periodic run logs

These directories are included only as a provisional runtime shape. A later
task can package scheduling or host deployment more tightly if needed.
