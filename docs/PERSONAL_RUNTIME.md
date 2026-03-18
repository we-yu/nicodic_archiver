# Personal Runtime Profile

This repository includes a separate provisional runtime profile for
personal-use operation.

It is distinct from the lightweight development container settings and is
intended to keep a long-lived container available for manual CLI use.

What this profile does:
- builds a runtime image from Dockerfile.runtime
- keeps one terminal-friendly container running
- preserves targets, SQLite data, archives, and batch logs through mounts

What this profile does not do yet:
- cron or repeated scheduling inside the container
- overlap or lock policy
- target storage migration away from the text file

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

## Host UID/GID Handling

To keep generated files writable from the host shell, pass the host UID/GID
only when starting the runtime container.

Recommended start command:

`LOCAL_UID=$(id -u) LOCAL_GID=$(id -g) docker compose -f docker-compose.runtime.yml up -d --build`

This avoids relying on persistent host-side environment variable setup.
The values are resolved from the current host at container start time.

## Build And Start

Build and start the provisional runtime container:

`LOCAL_UID=$(id -u) LOCAL_GID=$(id -g) docker compose -f docker-compose.runtime.yml up -d --build`

The container stays running with a simple foreground process so that human
operators can execute the existing CLI commands inside it.

## Stop

Stop and remove the provisional runtime container:

`docker compose -f docker-compose.runtime.yml down`

## Target List Location

Create or edit the target list at:

`runtime/targets/targets.txt`

That file remains a human-editable plain text target source.

## Initial Smoke Test

On a fresh runtime data directory, run one scrape or batch pass before using
`list-articles`.

Reason:
- the runtime profile itself can start cleanly with an empty mounted data area
- but `list-articles` expects the SQLite archive schema to already exist
- the first scrape or batch pass creates the initial archive DB state

Recommended first-pass smoke test:

`docker compose -f docker-compose.runtime.yml exec personal_runtime python main.py batch /runtime/targets/targets.txt`

After that, this command should work as expected:

`docker compose -f docker-compose.runtime.yml exec personal_runtime python main.py list-articles`

## Common Commands

Show the target list currently mounted into the runtime:

`docker compose -f docker-compose.runtime.yml exec personal_runtime python main.py targets /runtime/targets/targets.txt`

Run one batch pass:

`docker compose -f docker-compose.runtime.yml exec personal_runtime python main.py batch /runtime/targets/targets.txt`

Run the current periodic CLI entrypoint manually:

`docker compose -f docker-compose.runtime.yml exec personal_runtime python main.py periodic /runtime/targets/targets.txt 300`

List saved articles:

`docker compose -f docker-compose.runtime.yml exec personal_runtime python main.py list-articles`

Export all saved articles:

`docker compose -f docker-compose.runtime.yml exec personal_runtime python main.py export-all-articles --format txt`

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
