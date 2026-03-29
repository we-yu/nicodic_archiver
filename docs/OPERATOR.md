# Operator tooling (TASK032)

This document describes the **bounded** CLI for one person maintaining the
**target registry** and **saved article archive**. It is not a full admin
console: there is no delete, re-fetch, requeue, or web operator UI here.

Telemetry CSV and other support commands stay on the existing `main.py`
entry points; this guide focuses on **target** and **archive** operator
workflows only.

## Where data lives

- **Target registry**: a SQLite file you choose (often `data/nicodic.db` or a
  dedicated path). Pass its path explicitly on every `operator target …`
  command.
- **Saved archive** (articles and responses): the same database file the
  scraper and `list-articles` use. Override with env `NICODIC_DB_PATH` if your
  deployment uses a non-default path. Operator `archive` commands do not take a
  `--db` flag; align cwd and env with how you run batch or web.

## Entry points

- **Primary**: `python main.py operator …`
- **Optional wrapper** (repo root as cwd): `tools/nico-operator.sh …` (forwards
  to the same Python entry).

Run `python main.py operator help` for a short usage summary.

## Daily and periodic habits

**Before or after a batch**

1. **See what will be scraped**: list active targets (default) or include
   inactive rows with `--all`.
2. **Confirm one row**: inspect a single target by id and type.
3. **Pause a thread without deleting**: deactivate; use reactivate when you
   want it back in batch.

**When checking stored content**

1. List saved articles (human-readable summary lines).
2. Inspect one article in the terminal (optional `--last N` for recent
   responses only).
3. Export to stdout as **txt** or **md** for logs, tickets, or offline review.

## Target registry commands

| Action | Command pattern |
|--------|-----------------|
| List (active only) | `python main.py operator target list <target_db_path>` |
| List incl. inactive | `python main.py operator target list <target_db_path> --all` |
| Inspect one row | `python main.py operator target inspect <target_db_path> <article_id> <article_type>` |
| Add / upsert active | `python main.py operator target add <target_db_path> <url_or_title>` |
| Deactivate | `python main.py operator target deactivate <target_db_path> <article_id> <article_type>` |
| Reactivate | `python main.py operator target reactivate <target_db_path> <article_id> <article_type>` |

**Add** accepts either a canonical Nicodic article URL (parsed directly) or a
string passed to the same resolver as `resolve-article` (exact title or URL).
It does **not** remove rows; duplicate active rows are reported as
no-op-style messages.

**Deactivate** only flips `is_active`; it does not delete history in the
archive DB.

## Archive commands

| Action | Command pattern |
|--------|-----------------|
| List summaries | `python main.py operator archive list` |
| Inspect body | `python main.py operator archive inspect <article_id> <article_type>` |
| Last N responses | `python main.py operator archive inspect <id> <type> --last N` |
| Export stdout | `python main.py operator archive export <id> <type> --format txt\|md` |

These call the same functions as `list-articles`, `inspect`, and `export` on
`main.py`, so behavior matches existing tooling.

## Out of scope (by design)

- Target or archive **delete**
- Requeue, re-fetch, or destructive maintenance
- PostgreSQL, container-only DB, or telemetry/dashboard expansion
- Web-based operator UI

If you need run telemetry as CSV, use the existing
`export-run-telemetry-csv` command on `main.py` (not covered in detail here).
