# Architecture

The project is a Nicodic article archiver.

It downloads BBS comment pages from Nicovideo dictionary
and stores them locally.

The program currently exists as a single file:

main.py

The goal of refactoring is to separate responsibilities.

--------------------------------------------------

Target architecture

scraper/
    http_client.py
    parser.py

storage/
    database.py

core/
    archiver.py

cli/
    main.py

--------------------------------------------------

Module responsibilities

http_client.py
    Handles HTTP requests

parser.py
    Extracts information from HTML

database.py
    SQLite storage layer

archiver.py
    Coordinates scraping and persistence

main.py
    CLI entry point

--------------------------------------------------

Rules for refactoring

• Do not change program behaviour
• Only move logic into modules
• Keep CLI compatible
• Ensure tests continue to pass

--------------------------------------------------

Current features

• Fetch Nicodic BBS pages
• Parse comments
• Store JSON
• Store SQLite
• CLI inspect mode

--------------------------------------------------

Goal

Maintainable architecture
without changing functionality.

