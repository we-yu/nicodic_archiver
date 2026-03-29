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

orchestrator.py
    Coordinates scraping flow and persistence order

archive_read.py
    Archive-facing read / export seam

article_resolver.py
    Resolves bounded article input into canonical article targets

http_client.py
    Handles HTTP requests

parser.py
    Extracts response data from HTML

storage.py
    Handles SQLite and JSON persistence, including target-registry persistence

target_list.py
    Handles target registration, active-target reads, and legacy target import

web_app.py
    Provides the bounded Web-facing archive-check / follow-up interface

cli.py
    Provides CLI-facing archive output helpers

tests/conftest.py
    Supports pytest import resolution

tests/test_main.py
    Covers CLI and bounded app entry dispatch behavior

tests/test_orchestrator.py
    Covers orchestration helper behavior

tests/test_storage.py
    Covers storage-layer persistence behavior including target-registry behavior

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

orchestrator.py
    article metadata fetching
    BBS base URL generation
    paginated response collection
    JSON save + SQLite save flow

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
    archive persistence
    queue persistence
    target-registry persistence

target_list.py
    target-source handling layer
    target registration
    active target listing
    bounded legacy `targets.txt` import

web_app.py
    bounded Web-facing archive-check and follow-up actions
    bounded target registration
    saved article TXT download

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
this document should be treated as the post-TASK031 baseline.

Important current interpretation:

- the bounded Web/runtime publication baseline exists
- the target source of truth is now DB-backed in bounded form
- the next likely bounded step is not another source-of-truth migration,
  but an operator-facing target / archive management seam
- broader observability, GUI admin expansion, PostgreSQL migration,
  DB containerization, and wider dashboard/platform work remain deferred
  unless a later task explicitly brings them into scope
