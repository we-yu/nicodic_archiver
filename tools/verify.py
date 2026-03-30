"""
Bounded human-friendly verification helpers (TASK033).

This is read-first operator tooling. Live fetching/scraping is available
only via explicit opt-in commands and (for live) uses isolated smoke state.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import archive_read
import cli
import orchestrator
from article_resolver import resolve_article_input
from storage import (
    DEFAULT_DB_PATH,
    format_run_telemetry_csv_wide,
    get_target,
    init_db,
    list_targets,
)
from target_list import list_active_target_urls, parse_target_identity


def _print_usage() -> None:
    print("Usage:")
    print("  python main.py verify state --target-db <path> [--all-targets]")
    print("  python main.py verify targets list <target_db_path> [--all] "
          "[--limit N]")
    print("  python main.py verify targets inspect <target_db_path> "
          "<article_id> <article_type>")
    print("  python main.py verify archive list")
    print("  python main.py verify archive inspect <article_id> <article_type> "
          "[--last N]")
    print(
        "  python main.py verify article check "
        "<url_or_full_title> [--show-last N]"
    )
    print(
        "  python main.py verify article fetch "
        "<url_or_full_title> --isolated "
        "[--response-cap N] [--inspect-last N] [--follow-up]"
    )
    print(
        "  python main.py verify batch check "
        "--target-db <path> [--max-targets N]"
    )
    print(
        "  python main.py verify batch smoke "
        "--known-good-url <canonical_url> --isolated "
        "[--inspect-last N] [--telemetry-head-lines N]"
    )
    print(
        "  python main.py verify periodic smoke "
        "--known-good-url <canonical_url> --isolated "
        "[--inspect-last N] [--telemetry-head-lines N]"
    )
    print("  python main.py verify telemetry csv "
          "[--db PATH] [--head-lines N] [--output PATH]")
    print("  python main.py verify kgs show [--kgs-file PATH]")
    print(
        "  python main.py verify kgs smoke --isolated "
        "[--known-good-url URL] [--kgs-file PATH] "
        "[--mode batch|periodic] [--inspect-last N] "
        "[--telemetry-head-lines N]"
    )
    print(
        "  python main.py verify kgs follow-up --isolated "
        "[--known-good-url URL] [--kgs-file PATH] "
        "[--drop-last N] [--inspect-last N]"
    )
    print("")
    print("Notes:")
    print("- Live scraping is opt-in and runs only inside isolated smoke dirs.")
    print(f"- Archive DB is cwd-relative (default: {DEFAULT_DB_PATH}).")
    print("- KGS URL resolution prefers: flag > file > env > tools/kgs.txt.")


def _read_flag_value(args: list[str], flag: str) -> str | None:
    if flag not in args:
        return None
    idx = args.index(flag)
    if idx + 1 >= len(args):
        return None
    return args[idx + 1]


def _tokens_before_flag(args: list[str], flag: str) -> list[str]:
    if flag not in args:
        return args
    idx = args.index(flag)
    return args[:idx]


def _read_flag_int(args: list[str], flag: str) -> int | None:
    val = _read_flag_value(args, flag)
    if val is None:
        return None
    try:
        return int(val)
    except ValueError:
        return None


def _load_kgs_url(argv: list[str]) -> str | None:
    """
    Resolve a known-good smoke (KGS) canonical URL.

    Priority:
    - --known-good-url
    - --kgs-file
    - env NICODIC_KGS_URL
    - tools/kgs.txt (if present)
    """

    direct = _read_flag_value(argv, "--known-good-url")
    if direct:
        return direct.strip()

    kgs_file = _read_flag_value(argv, "--kgs-file")
    if kgs_file is None:
        kgs_file = "tools/kgs.txt"

    p = Path(kgs_file)
    if p.exists():
        for raw in p.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            return line

    env_url = os.environ.get("NICODIC_KGS_URL")
    if env_url:
        return env_url.strip()

    return None


def _try_archive_file_exists() -> bool:
    return (Path("data") / "nicodic.db").exists()


def _archive_guard_or_hint() -> bool:
    if not _try_archive_file_exists():
        print("Archive DB is missing: data/nicodic.db")
        print("Run one bounded batch pass first, then re-run verification.")
        return False
    return True


def _safe_list_archives() -> bool:
    if not _archive_guard_or_hint():
        return False
    try:
        cli.list_articles()
        return True
    except sqlite3.Error as exc:
        print(f"Archive DB exists but cannot be read: {exc}")
        return False


def _safe_archive_summary(article_id: str, article_type: str) -> dict | None:
    if not _archive_guard_or_hint():
        return None
    try:
        return archive_read.get_saved_article_summary(article_id, article_type)
    except sqlite3.Error as exc:
        print(f"Failed to read archive summary: {exc}")
        return None


@contextmanager
def _isolated_smoke_state(isolated_dir: str | None, label: str):
    """
    Use isolated cwd + override NICODIC_DB_PATH for telemetry safety.

    This prevents smoke from writing into an operator's main DB even if
    NICODIC_DB_PATH is set.
    """

    old_cwd = Path.cwd()
    base_dir = Path("data") / "smoke"
    base_dir.mkdir(parents=True, exist_ok=True)

    if isolated_dir is None:
        unique = uuid.uuid4().hex[:8]
        ts = int(time.time())
        isolated_root = base_dir / f"{label}_{ts}_{unique}"
    else:
        isolated_root = Path(isolated_dir)

    isolated_root.mkdir(parents=True, exist_ok=True)
    (isolated_root / "data").mkdir(parents=True, exist_ok=True)

    iso_archive_db = isolated_root / "data" / "nicodic.db"

    old_nicodic_db_path = os.environ.get("NICODIC_DB_PATH")
    os.environ["NICODIC_DB_PATH"] = str(iso_archive_db)

    try:
        os.chdir(isolated_root)
        yield isolated_root
    finally:
        os.chdir(old_cwd)
        if old_nicodic_db_path is None:
            if "NICODIC_DB_PATH" in os.environ:
                del os.environ["NICODIC_DB_PATH"]
        else:
            os.environ["NICODIC_DB_PATH"] = old_nicodic_db_path


def _print_targets_list(target_db_path: str, *, active_only: bool, limit: int):
    conn = init_db(target_db_path)
    try:
        entries = list_targets(conn, active_only=active_only)
    finally:
        conn.close()

    scope = "active-only" if active_only else "all-status"
    print("=== TARGET REGISTRY (verification) ===")
    print(f"DB: {target_db_path}")
    print(f"Scope: {scope}")
    print(f"Count: {len(entries)}")
    if not entries:
        print("(no rows)")
        return

    shown = entries[:limit]
    for e in shown:
        state = "active" if e["is_active"] else "inactive"
        print(
            f"{state:8} id={e['id']} "
            f"{e['article_type']}/{e['article_id']} url={e['canonical_url']}"
        )
    if len(entries) > limit:
        print(f"... truncated to first {limit} row(s)")


def _print_target_inspect(target_db_path: str, article_id: str, article_type: str):
    conn = init_db(target_db_path)
    try:
        row = get_target(conn, article_id, article_type)
    finally:
        conn.close()
    if row is None:
        print("Target not found in registry")
        print(f"DB: {target_db_path}")
        print(f"ID: {article_id}")
        print(f"Type: {article_type}")
        return

    print("=== TARGET DETAIL (verification) ===")
    print(f"DB: {target_db_path}")
    print(f"Registry Row ID: {row['id']}")
    print(f"Status: {'active' if row['is_active'] else 'inactive'}")
    print(f"Article ID: {row['article_id']}")
    print(f"Article Type: {row['article_type']}")
    print(f"Canonical URL: {row['canonical_url']}")
    print(f"Created At: {row['created_at']}")


def _resolve_and_print_article_input(url_or_title: str, *, show_failures: bool):
    resolution = resolve_article_input(url_or_title)
    if not resolution["ok"]:
        if show_failures:
            print("Article resolution failed")
            print(f"Failure: {resolution['failure_kind']}")
            print(f"Input: {resolution['normalized_input']}")
        return None
    ct = resolution["canonical_target"]
    print("=== ARTICLE CANONICAL TARGET (verification) ===")
    print(f"Matched By: {resolution['matched_by']}")
    print(f"Title: {resolution['title']}")
    print(f"URL: {ct['article_url']}")
    print(f"ID: {ct['article_id']}")
    print(f"Type: {ct['article_type']}")
    return ct


def _cmd_verify_state(args: list[str]) -> int:
    if "--target-db" not in args:
        print("Usage: verify state --target-db <path> [--all-targets]")
        return 1
    idx = args.index("--target-db")
    if idx + 1 >= len(args):
        print("Missing value for --target-db")
        return 1
    target_db_path = args[idx + 1]
    active_only = "--all-targets" not in args
    limit = int(_read_flag_value(args, "--limit") or "50")

    _print_targets_list(
        target_db_path,
        active_only=active_only,
        limit=limit,
    )
    _safe_list_archives()
    return 0


def _cmd_verify_targets(args: list[str]) -> int:
    if not args:
        _print_usage()
        return 1

    sub = args[0]
    rest = args[1:]

    if sub == "list":
        if len(rest) < 1:
            print("Usage: verify targets list <target_db_path> [--all] "
                  "[--limit N]")
            return 1
        target_db_path = rest[0]
        active_only = "--all" not in rest
        limit = _read_flag_int(rest, "--limit")
        if limit is None:
            limit = 50
        _print_targets_list(
            target_db_path,
            active_only=active_only,
            limit=limit,
        )
        return 0

    if sub == "inspect":
        if len(rest) < 3:
            print(
                "Usage: verify targets inspect <target_db_path> "
                "<article_id> <article_type>"
            )
            return 1
        target_db_path, article_id, article_type = rest[0], rest[1], rest[2]
        _print_target_inspect(target_db_path, article_id, article_type)
        return 0

    print(f"Unknown verify targets subcommand: {sub!r}")
    return 1


def _cmd_verify_archive(args: list[str]) -> int:
    if not args:
        print("Usage: verify archive list|inspect ...")
        return 1

    sub = args[0]
    rest = args[1:]

    if sub == "list":
        _safe_list_archives()
        return 0

    if sub == "inspect":
        if len(rest) < 2:
            print(
                "Usage: verify archive inspect <article_id> <article_type> "
                "[--last N]"
            )
            return 1
        article_id, article_type = rest[0], rest[1]
        last_n = _read_flag_int(rest, "--last")
        cli.inspect_article(article_id, article_type, last_n=last_n)
        return 0

    print(f"Unknown verify archive subcommand: {sub!r}")
    return 1


def _cmd_verify_article(args: list[str]) -> int:
    if not args:
        print("Usage: verify article check|fetch ...")
        return 1

    sub = args[0]
    rest = args[1:]

    if sub == "check":
        if len(rest) < 1:
            print(
                "Usage: verify article check <url_or_full_title> "
                "[--show-last N]"
            )
            return 1
        url_or_title_tokens = _tokens_before_flag(rest, "--show-last")
        url_or_title = " ".join(url_or_title_tokens).strip()
        ct = _resolve_and_print_article_input(url_or_title, show_failures=True)
        if ct is None:
            return 1

        summary = _safe_archive_summary(ct["article_id"], ct["article_type"])
        if summary is None:
            return 1

        print("=== ARTICLE ARCHIVE STATUS (verification) ===")
        print(f"Found: {'yes' if summary['found'] else 'no'}")
        print(f"Response count: {summary['response_count']}")
        print(f"Created At: {summary['created_at']}")

        show_last = None
        if "--show-last" in args:
            show_last = _read_flag_int(args, "--show-last")
            if show_last is None:
                print("Usage: --show-last must be an integer.")
                return 1
        if summary["found"] and show_last is not None:
            cli.inspect_article(
                ct["article_id"],
                ct["article_type"],
                last_n=show_last,
            )
        return 0

    if sub == "fetch":
        if "--isolated" not in rest:
            print("Usage: verify article fetch ... --isolated [options]")
            return 1

        url_or_title_tokens = _tokens_before_flag(rest, "--isolated")
        url_or_title_str = " ".join(url_or_title_tokens).strip()
        if not url_or_title_str:
            print("Missing <url_or_full_title> for article fetch.")
            return 1

        response_cap = _read_flag_int(rest, "--response-cap")
        if "--response-cap" in rest and response_cap is None:
            print("Usage: --response-cap must be an integer.")
            return 1
        inspect_last = _read_flag_int(rest, "--inspect-last")
        if "--inspect-last" in rest and inspect_last is None:
            print("Usage: --inspect-last must be an integer.")
            return 1
        if inspect_last is None:
            inspect_last = 5
        follow_up = "--follow-up" in rest
        isolated_dir = _read_flag_value(rest, "--isolated-dir")

        ct = _resolve_and_print_article_input(
            url_or_title_str,
            show_failures=True,
        )
        if ct is None:
            return 1

        with _isolated_smoke_state(isolated_dir, label="verify_article_fetch"):
            print("=== ISOLATED ARCHIVE STATE ===")
            print(f"cwd: {Path.cwd()}")
            init_db()

            print("=== FETCH PHASE (initial) ===")
            orchestrator.run_scrape(
                ct["article_url"],
                response_cap=response_cap,
            )

            if follow_up:
                print("=== FETCH PHASE (follow-up) ===")
                orchestrator.run_scrape(
                    ct["article_url"],
                    response_cap=response_cap,
                )

            print("=== POST-FETCH ARCHIVE CHECK ===")
            cli.inspect_article(
                ct["article_id"],
                ct["article_type"],
                last_n=inspect_last,
            )
            summary = _safe_archive_summary(
                ct["article_id"],
                ct["article_type"],
            )
            if summary is not None:
                print(f"Found: {'yes' if summary['found'] else 'no'}")
                print(f"Response count: {summary['response_count']}")

        return 0

    print(f"Unknown verify article subcommand: {sub!r}")
    return 1


def _parse_smoke_known_good_url(known_good_url: str):
    ident = parse_target_identity(known_good_url)
    if ident is None:
        print("known-good-url must be a canonical Nicopedia article URL.")
        print(f"Input: {known_good_url}")
        return None
    return ident


def _delete_last_n_responses_isolated(
    *,
    article_id: str,
    article_type: str,
    drop_last: int,
) -> int:
    """
    Bounded destructive step for follow-up smoke.

    This must only be used inside isolated smoke state.
    Returns number of deleted response rows.
    """

    if drop_last <= 0:
        return 0

    db_path = Path("data") / "nicodic.db"
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM responses
            WHERE id IN (
                SELECT id
                FROM responses
                WHERE article_id=? AND article_type=?
                ORDER BY res_no DESC, id DESC
                LIMIT ?
            )
            """,
            (article_id, article_type, drop_last),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def _cmd_verify_batch_check(args: list[str]) -> int:
    if "--target-db" not in args:
        print("Usage: verify batch check --target-db <path> [--max-targets N]")
        return 1
    idx = args.index("--target-db")
    if idx + 1 >= len(args):
        print("Missing value for --target-db")
        return 1
    target_db_path = args[idx + 1]
    max_targets = _read_flag_int(args, "--max-targets")
    if max_targets is None:
        max_targets = 20

    if not Path("data").joinpath("nicodic.db").exists():
        print("Archive DB missing: data/nicodic.db")
        print("Run one batch pass first, then re-run verification.")
        return 1

    targets = list_active_target_urls(target_db_path)[:max_targets]
    print("=== BATCH READINESS CHECK (verification) ===")
    print(f"Target registry: {target_db_path}")
    print(f"Active targets checked: {len(targets)} (max {max_targets})")

    found_count = 0
    missing = []
    for target_url in targets:
        ident = parse_target_identity(target_url)
        if ident is None:
            continue
        summary = _safe_archive_summary(
            ident["article_id"],
            ident["article_type"],
        )
        if summary is None:
            return 1
        if summary["found"]:
            found_count += 1
        else:
            missing.append(target_url)

    print("=== SUMMARY ===")
    print(f"Saved in archive: {found_count}")
    print(f"Missing from archive: {len(missing)}")
    if missing:
        print("Missing sample:")
        for m in missing[:5]:
            print(f"- {m}")
        if len(missing) > 5:
            print("... truncated")
    return 0


def _telemetry_csv_head(csv_text: str, head_lines: int) -> str:
    lines = csv_text.splitlines()
    if head_lines <= 0:
        return ""
    return "\n".join(lines[:head_lines]) + "\n"


def _cmd_smoke_batch_like(
    *,
    periodic: bool,
    known_good_url: str,
    isolated_dir: str | None,
    inspect_last: int,
    telemetry_head_lines: int,
) -> int:
    ident = _parse_smoke_known_good_url(known_good_url)
    if ident is None:
        return 1

    with _isolated_smoke_state(isolated_dir, label="verify_smoke"):
        target_db_path = str(Path.cwd() / "target_registry.db")
        print("=== ISOLATED SMOKE STATE ===")
        print(f"cwd: {Path.cwd()}")
        print(f"isolated target registry: {target_db_path}")

        # Prepare an empty archive schema so read-only post checks are stable.
        init_db()

        # Create a dedicated registry containing only the chosen known-good.
        conn = init_db(target_db_path)
        try:
            from storage import register_target

            register_target(
                conn,
                ident["article_id"],
                ident["article_type"],
                ident["canonical_url"],
            )
        finally:
            conn.close()

        print("=== PRE-SMOKE STATE (read-first) ===")
        print(
            "Smoke target:",
            f"{ident['article_type']}/{ident['article_id']}",
            ident["canonical_url"],
        )
        print("Initial archive status:")
        cli.inspect_article(
            ident["article_id"],
            ident["article_type"],
            last_n=inspect_last,
        )

        import main as main_module

        if periodic:
            print("=== LIVE SMOKE: periodic-once ===")
            main_module.run_periodic_once(target_db_path)
        else:
            print("=== LIVE SMOKE: batch ===")
            main_module.run_batch_scrape(target_db_path)

        print("=== POST-SMOKE STATE ===")
        cli.inspect_article(
            ident["article_id"],
            ident["article_type"],
            last_n=inspect_last,
        )
        print("Saved archive list (summary):")
        _safe_list_archives()

        print("=== TELEMETRY CSV (head) ===")
        iso_db_path = str(Path.cwd() / "data" / "nicodic.db")
        conn2 = init_db(iso_db_path)
        try:
            csv_text = format_run_telemetry_csv_wide(conn2)
        finally:
            conn2.close()
        print(_telemetry_csv_head(csv_text, telemetry_head_lines))

    return 0


def _cmd_verify_batch_smoke(args: list[str]) -> int:
    known_good_url = _read_flag_value(args, "--known-good-url")
    if not known_good_url:
        print("Usage: verify batch smoke --known-good-url <url> --isolated ...")
        return 1
    if "--isolated" not in args:
        print("verify batch smoke requires --isolated to avoid touching main DB.")
        return 1
    isolated_dir = _read_flag_value(args, "--isolated-dir")
    inspect_last = _read_flag_int(args, "--inspect-last") or 5
    telemetry_head_lines = _read_flag_int(
        args,
        "--telemetry-head-lines",
    )
    if telemetry_head_lines is None:
        telemetry_head_lines = 25

    return _cmd_smoke_batch_like(
        periodic=False,
        known_good_url=known_good_url,
        isolated_dir=isolated_dir,
        inspect_last=inspect_last,
        telemetry_head_lines=telemetry_head_lines,
    )


def _cmd_verify_periodic_smoke(args: list[str]) -> int:
    known_good_url = _read_flag_value(args, "--known-good-url")
    if not known_good_url:
        print(
            "Usage: verify periodic smoke --known-good-url <url> --isolated ..."
        )
        return 1
    if "--isolated" not in args:
        print(
            "verify periodic smoke requires --isolated to avoid touching main "
            "DB."
        )
        return 1
    isolated_dir = _read_flag_value(args, "--isolated-dir")
    inspect_last = _read_flag_int(args, "--inspect-last") or 5
    telemetry_head_lines = _read_flag_int(
        args,
        "--telemetry-head-lines",
    )
    if telemetry_head_lines is None:
        telemetry_head_lines = 25

    return _cmd_smoke_batch_like(
        periodic=True,
        known_good_url=known_good_url,
        isolated_dir=isolated_dir,
        inspect_last=inspect_last,
        telemetry_head_lines=telemetry_head_lines,
    )


def _cmd_verify_telemetry(args: list[str]) -> int:
    if not args:
        print("Usage: verify telemetry csv [--db PATH] [--head-lines N]")
        return 1
    sub = args[0]
    rest = args[1:]

    if sub != "csv":
        print(f"Unknown verify telemetry subcommand: {sub!r}")
        return 1

    db_path = _read_flag_value(rest, "--db")
    if db_path is None:
        db_path = os.environ.get("NICODIC_DB_PATH", DEFAULT_DB_PATH)

    head_lines = _read_flag_int(rest, "--head-lines")
    if head_lines is None:
        head_lines = 20

    output_path = _read_flag_value(rest, "--output")

    conn = init_db(db_path)
    try:
        csv_text = format_run_telemetry_csv_wide(conn)
    finally:
        conn.close()

    csv_head = _telemetry_csv_head(csv_text, head_lines)
    if output_path:
        Path(output_path).write_text(csv_head, encoding="utf-8")
        print(f"Telemetry CSV head written to: {output_path}")
        return 0
    print(csv_head, end="")
    return 0


def _cmd_verify_kgs(args: list[str]) -> int:
    if not args:
        print("Usage: verify kgs show|smoke|follow-up ...")
        return 1

    sub = args[0]
    rest = args[1:]

    if sub == "show":
        kgs_url = _load_kgs_url(rest)
        if not kgs_url:
            print("No KGS URL configured.")
            print("Set one of:")
            print("- flag: --known-good-url <canonical_url>")
            print("- file: --kgs-file <path> (or tools/kgs.txt)")
            print("- env: NICODIC_KGS_URL")
            return 1
        ident = _parse_smoke_known_good_url(kgs_url)
        if ident is None:
            return 1
        print("=== KGS CONFIG ===")
        print(f"KGS URL: {ident['canonical_url']}")
        print(f"ID: {ident['article_id']}")
        print(f"Type: {ident['article_type']}")
        return 0

    if sub == "smoke":
        if "--isolated" not in rest:
            print("verify kgs smoke requires --isolated")
            return 1
        kgs_url = _load_kgs_url(rest)
        if not kgs_url:
            print("Missing KGS URL. Use --known-good-url or --kgs-file.")
            return 1
        mode = _read_flag_value(rest, "--mode") or "batch"
        if mode not in {"batch", "periodic"}:
            print("Usage: --mode must be batch or periodic.")
            return 1

        isolated_dir = _read_flag_value(rest, "--isolated-dir")
        inspect_last = _read_flag_int(rest, "--inspect-last")
        if "--inspect-last" in rest and inspect_last is None:
            print("Usage: --inspect-last must be an integer.")
            return 1
        if inspect_last is None:
            inspect_last = 5

        telemetry_head_lines = _read_flag_int(rest, "--telemetry-head-lines")
        if "--telemetry-head-lines" in rest and telemetry_head_lines is None:
            print("Usage: --telemetry-head-lines must be an integer.")
            return 1
        if telemetry_head_lines is None:
            telemetry_head_lines = 25

        return _cmd_smoke_batch_like(
            periodic=(mode == "periodic"),
            known_good_url=kgs_url,
            isolated_dir=isolated_dir,
            inspect_last=inspect_last,
            telemetry_head_lines=telemetry_head_lines,
        )

    if sub == "follow-up":
        if "--isolated" not in rest:
            print("verify kgs follow-up requires --isolated")
            return 1
        kgs_url = _load_kgs_url(rest)
        if not kgs_url:
            print("Missing KGS URL. Use --known-good-url or --kgs-file.")
            return 1
        ident = _parse_smoke_known_good_url(kgs_url)
        if ident is None:
            return 1

        drop_last = _read_flag_int(rest, "--drop-last")
        if "--drop-last" in rest and drop_last is None:
            print("Usage: --drop-last must be an integer.")
            return 1
        if drop_last is None:
            drop_last = 5
        if drop_last > 50:
            print("Usage: --drop-last must be <= 50 (bounded follow-up).")
            return 1

        inspect_last = _read_flag_int(rest, "--inspect-last")
        if "--inspect-last" in rest and inspect_last is None:
            print("Usage: --inspect-last must be an integer.")
            return 1
        if inspect_last is None:
            inspect_last = 10

        isolated_dir = _read_flag_value(rest, "--isolated-dir")
        response_cap = _read_flag_int(rest, "--response-cap")
        if "--response-cap" in rest and response_cap is None:
            print("Usage: --response-cap must be an integer.")
            return 1

        with _isolated_smoke_state(isolated_dir, label="kgs_follow_up"):
            print("=== KGS FOLLOW-UP (isolated) ===")
            print(f"cwd: {Path.cwd()}")
            print(f"KGS: {ident['canonical_url']}")

            init_db()
            print("=== PHASE: initial fetch ===")
            orchestrator.run_scrape(
                ident["canonical_url"],
                response_cap=response_cap,
            )

            print("=== PHASE: drop tail responses (bounded) ===")
            deleted = _delete_last_n_responses_isolated(
                article_id=ident["article_id"],
                article_type=ident["article_type"],
                drop_last=drop_last,
            )
            print(f"Deleted response rows: {deleted}")

            print("=== PHASE: follow-up fetch ===")
            orchestrator.run_scrape(
                ident["canonical_url"],
                response_cap=response_cap,
            )

            print("=== PHASE: post-check ===")
            cli.inspect_article(
                ident["article_id"],
                ident["article_type"],
                last_n=inspect_last,
            )

        return 0

    print(f"Unknown verify kgs subcommand: {sub!r}")
    return 1


def dispatch_verify(argv: list[str]) -> int:
    if not argv:
        _print_usage()
        return 1

    if argv[0] in {"-h", "--help", "help"}:
        _print_usage()
        return 0

    sub = argv[0]
    rest = argv[1:]

    if sub == "state":
        return _cmd_verify_state(rest)
    if sub == "targets":
        return _cmd_verify_targets(rest)
    if sub == "archive":
        return _cmd_verify_archive(rest)
    if sub == "article":
        return _cmd_verify_article(rest)
    if sub == "batch":
        if not rest:
            print("Usage: verify batch check|smoke ...")
            return 1
        if rest[0] == "check":
            return _cmd_verify_batch_check(rest[1:])
        if rest[0] == "smoke":
            return _cmd_verify_batch_smoke(rest[1:])
        print(f"Unknown verify batch subcommand: {rest[0]!r}")
        return 1
    if sub == "periodic":
        if not rest:
            print("Usage: verify periodic smoke ...")
            return 1
        if rest[0] == "smoke":
            return _cmd_verify_periodic_smoke(rest[1:])
        print(f"Unknown verify periodic subcommand: {rest[0]!r}")
        return 1
    if sub == "telemetry":
        return _cmd_verify_telemetry(rest)
    if sub == "kgs":
        return _cmd_verify_kgs(rest)

    print(f"Unknown verify subcommand: {sub!r}")
    _print_usage()
    return 1


if __name__ == "__main__":
    sys.exit(dispatch_verify(sys.argv[1:]))
