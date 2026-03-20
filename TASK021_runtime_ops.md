# TASK021 periodic packaging (bounded)

This adds a minimal runtime packaging path for periodic operation.
It keeps the current text-file target list and existing periodic semantics.

## Added one-shot periodic entrypoint

- CLI:
  - `python main.py periodic-one-shot <target_list_path>`
  - `python main.py periodic-one-shot <target_list_path> --lock-path <path>`
- Wrapper:
  - `runtime/scripts/periodic_one_shot.sh <target_list_path> [lock_path]`

This wrapper is intended for external schedulers to invoke one run at a time.

## Simple lock + skip behavior

- Lock file default: `data/periodic_one_shot.lock`
- Acquisition method: atomic file create (`O_CREAT | O_EXCL`)
- If lock exists:
  - the run is skipped
  - process exits successfully (skip is not treated as hard failure)
- If lock acquired:
  - run one batch pass (`run_batch_scrape`)
  - remove lock file in `finally`

## External scheduling compatibility

Use external scheduler tooling (host cron/system timer/etc.) to call:

```sh
runtime/scripts/periodic_one_shot.sh /app/targets.txt /app/data/periodic.lock
```

No cron/scheduler framework is added inside this repository for TASK021.

## Smoke check helper note

No dedicated smoke helper binary was added.
The wrapper itself is the minimal callable path and is sufficient for
runtime-path smoke checks.

