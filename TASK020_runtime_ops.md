# TASK020 Runtime Profile (personal-use)

This adds a separate, provisional runtime profile using Docker Compose.
It is intended for personal-use, production-like operation, while preserving
the existing CLI-centered behavior and current SQLite / file semantics.

## What is included

1. `docker-compose.runtime.yml` - a minimal runtime service.
2. This document - build/run/stop/update flow and persistence guidance.

## Persistence guidance (container -> host)

The application writes under `/app/data` inside the container.

- SQLite DB: `/app/data/nicodic.db`
- Batch run logs: `/app/data/batch_runs/*.log` (via `BATCH_LOG_DIR`)
- JSON export artifacts from scraping: `/app/data/*.json` (via main save)
- Target list file: supplied by the CLI, typically via an extra mount.

This profile mounts the child-repo-local `./data` directory:

- host `./data` -> container `/app/data`

If you deploy to another host-side directory, mount that directory to
`/app/data` instead.

## Build / run / stop / update

### Build

Build the runtime image from the child repo:

`docker compose -f docker-compose.runtime.yml build`

### Run (one-shot)

Each run executes the CLI and exits when the command finishes.

List:

```sh
docker compose -f docker-compose.runtime.yml run --rm scraper \
  python main.py list-articles
```

Scrape a single article:

```sh
docker compose -f docker-compose.runtime.yml run --rm scraper \
  python main.py <article_url>
```

Batch scrape:

```sh
docker compose -f docker-compose.runtime.yml run --rm scraper \
  python main.py batch /app/targets.txt
```

Periodic scrape (in-process loop, no cron/scheduler added here):

```sh
docker compose -f docker-compose.runtime.yml run --rm scraper \
  python main.py periodic /app/targets.txt 60 --max-runs 10
```

### Target list file mounting

The target list path given to the CLI must exist inside the container.
For example, if your host target list file is `./targets.txt`:

```sh
docker compose -f docker-compose.runtime.yml run --rm -v $(pwd)/targets.txt:\
  /app/targets.txt:ro scraper \
  python main.py batch /app/targets.txt
```

### Stop

For `docker compose run --rm`, Ctrl+C stops the running CLI process.

### Update

After changing code, rebuild and rerun:

- `docker compose -f docker-compose.runtime.yml build`
- rerun your desired `docker compose ... run --rm scraper ...` command

## Bounded behavior notes

This profile does not add:

- cron / scheduler packaging
- overlap/lock policy
- DB-backed target registry changes
- parser/http_client/orchestrator/storage redesign

It only provides a practical personal-use runtime envelope around the
existing CLI.

