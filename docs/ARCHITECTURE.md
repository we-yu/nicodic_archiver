# Architecture

The project is a Nicodic article archiver.

It downloads BBS comment pages from Nicovideo dictionary
and stores them locally.

--------------------------------------------------

Current architecture

main.py
    CLI entry point

orchestrator.py
    Coordinates scraping flow and persistence order

http_client.py
    Handles HTTP requests

parser.py
    Extracts response data from HTML

storage.py
    Handles SQLite and JSON persistence

cli.py
    Provides inspect command output

--------------------------------------------------

Current responsibility split

main.py
    CLI argument parsing
    inspect branch
    dispatch to orchestration

orchestrator.py
    article metadata fetching
    BBS base URL generation
    paginated response collection
    JSON save + SQLite save flow

http_client.py
    HTTP fetch layer

parser.py
    HTML parsing layer

storage.py
    persistence layer

cli.py
    DB inspection output

--------------------------------------------------

Rules for refactoring

• Do not change program behaviour
• Keep CLI compatible
• Prefer small, explainable refactors
• Keep Docker / pytest / flake8 working

--------------------------------------------------

Status after TASK002

• main.py is thinner than before
• orchestration has been extracted to orchestrator.py
• behaviour is intended to remain unchanged

--------------------------------------------------

Future direction

Further refactoring should treat this structure
as the post-TASK002 baseline unless a new task
explicitly changes it.

