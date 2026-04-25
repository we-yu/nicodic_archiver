# Architecture

This document is the canonical architecture description
for the nicodic_archiver project.

This file is the source-of-truth architecture document.

Copies of this file may be synchronized to:

- `copilot/docs/ARCHITECTURE.md`
- `cursor/docs/ARCHITECTURE.md`

Those copies exist for repository-local readability,
but this root file is the canonical version.

--------------------------------------------------

Project purpose

The project archives comment threads from the NicoNicoPedia BBS.

Goal:

- preserve comments locally
- keep access even if original comments are deleted
- store static information only

Stored data includes:

- response number
- poster name
- poster ID hash
- posted timestamp
- comment text
- comment HTML

Excluded data includes:

- GOOD / BAD counts
- other dynamic metrics

--------------------------------------------------

Current architecture

main.py
    CLI and bounded app/runtime entry point
    batch / periodic telemetry append trigger
    read-only telemetry CSV export entry

orchestrator.py
    Coordinates scraping flow and persistence order
    returns bounded scrape outcome semantics compatible with telemetry insertion
    persists normal archive data through SQLite-centered archive storage
    reads `SCRAPE_PAGE_DELAY_SECONDS` for runtime-configurable BBS page
    pagination delay, defaulting safely to 5.0 seconds

archive_read.py
    Archive-facing read / export seam
    provides saved article summaries, registered article list data,
    and bounded txt / md / csv export helpers

article_resolver.py
    Resolves bounded article input into canonical article targets

http_client.py
    HTTP fetch boundary
    resolves Nicopedia `/id/<number>` article URLs to effective `/a/<title>`
    URLs for target registration normalization
    does not define scrape persistence semantics

parser.py
    Extracts response data from HTML

storage.py
    Handles SQLite-centered persistence, including:
    - archive persistence
    - target-registry persistence
    - append-only scrape-run telemetry persistence
    - read-only telemetry CSV derivation
    - legacy JSON helper behavior for historical compatibility only

target_list.py
    target registry boundary
    validates and persists scrape targets
    normalizes incoming Nicopedia `/id/<number>` URLs to effective `/a/<title>`
    URLs before registration
    keeps parse / validate helpers syntax-only

host_cron.py
    Host-side cron log formatting / rollover / compaction seam for runtime
    operation

web_app.py
    Provides the bounded Web-facing archive-check / follow-up interface
    provides saved-download format selection and a read-only registered
    article list page

cli.py
    Provides CLI-facing archive output helpers

operator_cli.py
    Provides bounded operator-facing management and export helper behavior,
    including ad-hoc saved article export for `tools/show_scraped_res.sh`

tests/conftest.py
    Supports pytest import resolution

tests/test_main.py
    Covers CLI and bounded app entry dispatch behavior

tests/test_orchestrator.py
    Covers orchestration helper behavior

tests/test_storage.py
    Covers storage-layer persistence behavior including telemetry behavior

tests/test_target_list.py
    Covers target-registry seam behavior

tests/test_web_app.py
    Covers bounded Web-facing behavior

--------------------------------------------------

Current responsibility split

main.py
    CLI argument parsing
    bounded batch / periodic / web dispatch
    target-registry-backed target flow entrypoints
    telemetry append trigger wiring
    read-only telemetry CSV export wiring

orchestrator.py
    article metadata fetching
    BBS base URL generation
    paginated response collection
    SQLite-centered archive save flow
    bounded scrape outcome semantics
    runtime-configurable BBS page delay through `SCRAPE_PAGE_DELAY_SECONDS`

delete_request_feeder.py
    bounded delete-request candidate extraction and normalization
    bounded candidate sanitize hardening
    bounded self-heal of already-dirty candidate input at handoff time
    candidate-level failure containment for:
    - resolver failure
    - registration failure
    tiny-summary-only feeder outcome visibility

archive_read.py
    archive-facing read and export seam

article_resolver.py
    bounded canonical article input resolution

http_client.py
    HTTP fetch layer

parser.py
    HTML parsing layer

storage.py
    persistence layer
    SQLite-centered archive persistence
    queue persistence
    target-registry persistence
    append-only scrape-run telemetry persistence
    telemetry CSV derivation
    legacy JSON helper behavior for historical compatibility only

target_list.py
    target-source handling layer
    target registration
    active target listing
    bounded legacy `targets.txt` import
    bounded target identity parsing

host_cron.py
    host/runtime-facing cron log readability layer
    structured run-block emission
    bounded daily rollover
    bounded weekly log compaction

web_app.py
    bounded Web-facing archive-check and follow-up actions
    bounded target registration
    bounded saved-article download format selection
    bounded read-only registered article list page
    saved article download formats:
    - txt
    - md
    - csv
    default saved-download format:
    - txt

cli.py
    archive-facing CLI output formatting

tests/
    focused unit-test layer protecting current bounded responsibilities

--------------------------------------------------

Status after TASK003

- main.py is thinner than before
- orchestration has been extracted to orchestrator.py
- minimal unit tests now exist for main.py and orchestrator.py
- tests/conftest.py supports pytest import resolution
- production behaviour is intended to remain unchanged

--------------------------------------------------

Design principles

- keep CLI compatibility
- prefer small, explainable refactors
- preserve behavior unless a task explicitly changes it
- prefer stability over novelty
- keep module responsibilities clear
- compare AI implementations before adoption

--------------------------------------------------

Refactoring guidance

Good refactors:

- reduce responsibility overlap
- make main.py thinner without changing CLI behavior
- add focused tests around existing behavior
- improve workflow tooling when review friction is observed

Avoid by default:

- redesigning multiple layers at once
- expanding task scope silently
- introducing configuration systems without clear need
- changing behavior during structure-only tasks

--------------------------------------------------

Workflow context

This project uses a Double Helix Development Model.

For one task:

- Copilot implementation is produced in `copilot/`
- Cursor implementation is produced in `cursor/`
- both are reviewed
- one is adopted, or a hybrid is adopted
- both repos are then realigned to the same final state

For practical workflow steps, refer to:

- `WORKSPACE.md`
- `META/TASK_CYCLE_CHECKLIST.md`

For adoption history, refer to:

- `META/review_log/`

--------------------------------------------------

Future note

Unless a later task explicitly changes architecture,
this document should be treated as the post-SUBTASK009 baseline.

Important current interpretation:

- the bounded Web/runtime publication baseline exists
- the runtime Web surface is publicly reachable through bounded nginx front-door
  publication at:
  - `nicoarc-prelim.mimizuku.dev`
- the current Web saved-download baseline now includes bounded format selection
  for:
  - `txt`
  - `md`
  - `csv`
- default saved-download format remains `txt`
- the Web app now includes a bounded read-only registered article list page
- normal archive operation is now SQLite-centered
- always-on `data/*.json` archive output is no longer part of the current
  scrape persistence path
- existing JSON files should be treated as historical artifacts unless a later
  task explicitly reintroduces JSON output semantics
- operator ad-hoc article export now includes:
  - `tools/show_scraped_res.sh`
  - title input by default
  - explicit `--title TITLE`
  - explicit `--id ID`
  - `--txt`
  - `--md`
  - `--csv`
- HTML export remains deferred
- JSON export remains deferred
- append-only scrape-run telemetry still exists in bounded form
- telemetry CSV export remains a read-only derived artifact
- broader observability, GUI admin expansion, PostgreSQL migration,
  DB containerization, and wider dashboard/platform work remain deferred
  unless a later task explicitly brings them into scope
- incoming Nicopedia `/id/<number>` article URLs should not be persisted as
  scrape targets when they can be resolved to effective `/a/<title>` URLs
- target registration and target import should share the same URL
  normalization boundary
- target parsing and URL validation remain syntax-only and must not perform
  network access
- existing runtime `id` rows are historical data until a separate maintenance
  task handles them
- BBS page-to-page scrape delay is runtime-configurable through
  `SCRAPE_PAGE_DELAY_SECONDS`
- unset or invalid scrape delay configuration falls back to 5.0 seconds
- `.env.runtime.local` changes may require runtime container recreation before
  they affect the running runtime container
