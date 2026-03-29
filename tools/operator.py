"""
Bounded operator CLI: target registry and saved archive (read / safe writes).

Destructive actions (delete, re-fetch, requeue) are intentionally out of scope.
"""

from __future__ import annotations

import sys

from article_resolver import resolve_article_input
from cli import export_article, inspect_article, list_articles
from storage import (
    fetch_target,
    init_db,
    list_targets,
    register_target,
    set_target_active,
)
from target_list import parse_target_identity


def _print_operator_usage() -> None:
    print("Usage:")
    print("  python main.py operator target list <target_db_path> [--all]")
    print(
        "  python main.py operator target inspect "
        "<target_db_path> <article_id> <article_type>"
    )
    print(
        "  python main.py operator target add "
        "<target_db_path> <article_url_or_title>"
    )
    print(
        "  python main.py operator target deactivate "
        "<target_db_path> <article_id> <article_type>"
    )
    print(
        "  python main.py operator target reactivate "
        "<target_db_path> <article_id> <article_type>"
    )
    print("  python main.py operator archive list")
    print(
        "  python main.py operator archive inspect "
        "<article_id> <article_type> [--last N]"
    )
    print(
        "  python main.py operator archive export "
        "<article_id> <article_type> --format txt|md"
    )
    print("")
    print("Archive commands use the same SQLite file as scrape/archive code.")
    print("Target commands use the registry file you pass as <target_db_path>.")


def _cmd_target_list(args: list[str]) -> int:
    if len(args) < 1:
        print("Usage: operator target list <target_db_path> [--all]")
        return 1
    db_path = args[0]
    active_only = "--all" not in args
    conn = init_db(db_path)
    try:
        entries = list_targets(conn, active_only=active_only)
    finally:
        conn.close()

    scope = "active targets only" if active_only else "active and inactive rows"
    print("=== TARGET REGISTRY ===")
    print(f"database: {db_path}")
    print(f"listing: {scope}")
    print(f"count: {len(entries)}")
    if not entries:
        print("(no rows)")
        return 0
    for e in entries:
        state = "active" if e["is_active"] else "inactive"
        print(
            f"id={e['id']}\t{e['article_type']}/{e['article_id']}\t"
            f"{state}\t{e['canonical_url']}\tcreated_at={e['created_at']}"
        )
    return 0


def _cmd_target_inspect(args: list[str]) -> int:
    if len(args) < 3:
        print(
            "Usage: operator target inspect "
            "<target_db_path> <article_id> <article_type>"
        )
        return 1
    db_path, article_id, article_type = args[0], args[1], args[2]
    conn = init_db(db_path)
    try:
        row = fetch_target(conn, article_id, article_type)
    finally:
        conn.close()
    if row is None:
        print("No target row found for this article_id and article_type.")
        return 1
    print("=== TARGET ROW ===")
    print(f"database: {db_path}")
    print(f"internal_id: {row['id']}")
    print(f"article_id: {row['article_id']}")
    print(f"article_type: {row['article_type']}")
    print(f"canonical_url: {row['canonical_url']}")
    print(f"is_active: {'yes' if row['is_active'] else 'no'}")
    print(f"created_at: {row['created_at']}")
    return 0


def _cmd_target_add(args: list[str]) -> int:
    if len(args) < 2:
        print(
            "Usage: operator target add "
            "<target_db_path> <article_url_or_title>"
        )
        return 1
    db_path = args[0]
    user_input = " ".join(args[1:]).strip()

    ident = parse_target_identity(user_input)
    if ident is not None:
        conn = init_db(db_path)
        try:
            result = register_target(
                conn,
                ident["article_id"],
                ident["article_type"],
                ident["canonical_url"],
            )
        finally:
            conn.close()
        status = result["status"]
        if status == "added":
            print("Added new target row (active).")
        elif status == "reactivated":
            print("Target existed inactive; reactivated and URL refreshed.")
        else:
            print("Target already present and active (no change needed).")
        return 0

    resolution = resolve_article_input(user_input)
    if not resolution["ok"]:
        print(f"Could not resolve title or URL: {resolution['failure_kind']}")
        print(f"Input: {resolution['normalized_input']}")
        return 1
    ct = resolution["canonical_target"]
    conn = init_db(db_path)
    try:
        result = register_target(
            conn,
            ct["article_id"],
            ct["article_type"],
            ct["article_url"],
        )
    finally:
        conn.close()
    status = result["status"]
    if status == "added":
        print("Added new target row from resolved article (active).")
    elif status == "reactivated":
        print("Target existed inactive; reactivated from resolved article.")
    else:
        print("Target already present and active (no change needed).")
    return 0


def _cmd_target_deactivate(args: list[str]) -> int:
    if len(args) < 3:
        print(
            "Usage: operator target deactivate "
            "<target_db_path> <article_id> <article_type>"
        )
        return 1
    db_path, article_id, article_type = args[0], args[1], args[2]
    conn = init_db(db_path)
    try:
        outcome = set_target_active(
            conn,
            article_id,
            article_type,
            is_active=False,
        )
    finally:
        conn.close()
    if outcome == "not_found":
        print("No target row found; nothing to deactivate.")
        return 1
    print("Target marked inactive (skipped by batch until reactivated).")
    return 0


def _cmd_target_reactivate(args: list[str]) -> int:
    if len(args) < 3:
        print(
            "Usage: operator target reactivate "
            "<target_db_path> <article_id> <article_type>"
        )
        return 1
    db_path, article_id, article_type = args[0], args[1], args[2]
    conn = init_db(db_path)
    try:
        outcome = set_target_active(
            conn,
            article_id,
            article_type,
            is_active=True,
        )
    finally:
        conn.close()
    if outcome == "not_found":
        print("No target row found; use operator target add to create one.")
        return 1
    print("Target marked active (included in batch again).")
    return 0


def _cmd_archive_list(_args: list[str]) -> int:
    list_articles()
    return 0


def _cmd_archive_inspect(args: list[str]) -> int:
    if len(args) < 2:
        print(
            "Usage: operator archive inspect "
            "<article_id> <article_type> [--last N]"
        )
        return 1
    article_id = args[0]
    article_type = args[1]
    last_n = None
    if "--last" in args:
        idx = args.index("--last")
        if idx + 1 >= len(args):
            print("Usage: --last requires a number.")
            return 1
        try:
            last_n = int(args[idx + 1])
        except ValueError:
            print("Usage: --last must be an integer.")
            return 1
    inspect_article(article_id, article_type, last_n=last_n)
    return 0


def _cmd_archive_export(args: list[str]) -> int:
    if len(args) < 5 or "--format" not in args:
        print(
            "Usage: operator archive export "
            "<article_id> <article_type> --format txt|md"
        )
        return 1
    article_id = args[0]
    article_type = args[1]
    fmt_idx = args.index("--format")
    if fmt_idx + 1 >= len(args):
        print("Usage: --format must be followed by txt or md.")
        return 1
    fmt = args[fmt_idx + 1]
    if not export_article(article_id, article_type, fmt):
        return 1
    return 0


def dispatch_operator(argv: list[str]) -> int:
    """Entry from ``main.py operator ...``; returns process exit code."""

    if not argv:
        _print_operator_usage()
        return 1

    realm = argv[0]
    if realm in {"-h", "--help", "help"}:
        _print_operator_usage()
        return 0

    if realm != "target" and realm != "archive":
        print(f"Unknown operator realm: {realm!r} (use target or archive).")
        _print_operator_usage()
        return 1

    if len(argv) < 2:
        print(f"Missing {realm} subcommand (list, inspect, ...).")
        return 1

    sub = argv[1]
    rest = argv[2:]

    if realm == "target":
        if sub == "list":
            return _cmd_target_list(rest)
        if sub == "inspect":
            return _cmd_target_inspect(rest)
        if sub == "add":
            return _cmd_target_add(rest)
        if sub == "deactivate":
            return _cmd_target_deactivate(rest)
        if sub == "reactivate":
            return _cmd_target_reactivate(rest)
        print(f"Unknown target subcommand: {sub!r}")
        return 1

    if sub == "list":
        return _cmd_archive_list(rest)
    if sub == "inspect":
        return _cmd_archive_inspect(rest)
    if sub == "export":
        return _cmd_archive_export(rest)
    print(f"Unknown archive subcommand: {sub!r}")
    return 1


def main() -> None:
    """Allow ``python -m tools.operator`` when cwd is repo root."""

    sys.exit(dispatch_operator(sys.argv[1:]))


if __name__ == "__main__":
    main()
