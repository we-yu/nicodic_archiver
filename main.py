import os
import sys
import uuid
import time
from datetime import datetime, timezone
from pathlib import Path

from article_resolver import resolve_article_input
from cli import export_all_articles, export_article, inspect_article, list_articles
from orchestrator import run_scrape
from storage import (
    DEFAULT_DB_PATH,
    append_scrape_run_observation,
    format_run_telemetry_csv_wide,
    init_db,
)
from target_list import (
    import_targets_from_text_file,
    list_active_target_urls,
    parse_target_identity,
    register_target_url,
)
from web_app import serve_web_app


DEFAULT_TARGET_DB_PATH = os.environ.get("TARGET_DB_PATH", "data/nicodic.db")

# Telemetry only: set True around run_batch_scrape from run_periodic_scrape.
_inside_periodic_batch: bool = False


def _telemetry_archive_db_path() -> str:
    return os.environ.get("NICODIC_DB_PATH", DEFAULT_DB_PATH)


def _record_scrape_run_observation(
    archive_db_path: str,
    run_id: str,
    run_started_at: str,
    run_kind: str,
    identity: dict,
    scrape_outcome: str,
) -> None:
    conn = init_db(archive_db_path)
    try:
        append_scrape_run_observation(
            conn,
            run_id=run_id,
            run_started_at=run_started_at,
            run_kind=run_kind,
            article_id=identity["article_id"],
            article_type=identity["article_type"],
            canonical_article_url=identity["canonical_url"],
            scrape_outcome=scrape_outcome,
        )
    finally:
        conn.close()


# ============================================================
# エントリポイント
# ============================================================


def run_batch_scrape(target_db_path: str) -> tuple[str, int]:
    """Run one full batch pass and return (final_status, failed_targets)."""

    run_kind = "periodic_batch" if _inside_periodic_batch else "batch"

    targets = list_active_target_urls(target_db_path)

    print(
        f"Loaded {len(targets)} active scrape target(s) "
        f"from target registry {target_db_path}"
    )

    run_id = uuid.uuid4().hex[:12]
    started_at = datetime.now(timezone.utc).isoformat()
    archive_db_path = _telemetry_archive_db_path()
    log_dir = Path(os.environ.get("BATCH_LOG_DIR", "data/batch_runs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"batch_{run_id}.log"

    with log_path.open("a", encoding="utf-8") as f:
        f.write("BATCH_RUN_START\n")
        f.write(f"run_id={run_id}\n")
        f.write(f"started_at={started_at}\n")
        f.write(f"target_db_path={target_db_path}\n")
        f.write("target_source=target_table\n")
        f.write(f"total_targets={len(targets)}\n")

    failed_targets = 0
    for idx, target in enumerate(targets, start=1):
        print(f"[{idx}/{len(targets)}] Scraping: {target}")
        identity = parse_target_identity(target)
        if identity is None:
            failed_targets += 1
            print(f"[FAIL] {target} (invalid target URL shape)")
            with log_path.open("a", encoding="utf-8") as f:
                f.write("FAIL\n")
                f.write(f"target={target}\n")
                f.write("short_reason=invalid_target_url_shape\n")
            continue

        scrape_outcome = "fail_exception"
        try:
            scrape_result = run_scrape(target)
            scrape_outcome = scrape_result.outcome
        except Exception as exc:
            failed_targets += 1
            print(f"[FAIL] {target} ({type(exc).__name__}: {exc})")
            with log_path.open("a", encoding="utf-8") as f:
                f.write("FAIL\n")
                f.write(f"target={target}\n")
                f.write(f"short_reason={type(exc).__name__}: {exc}\n")
            _record_scrape_run_observation(
                archive_db_path,
                run_id,
                started_at,
                run_kind,
                identity,
                scrape_outcome,
            )
            continue

        ok = bool(scrape_result)
        if not ok:
            failed_targets += 1
            print(f"[FAIL] {target}")
            with log_path.open("a", encoding="utf-8") as f:
                f.write("FAIL\n")
                f.write(f"target={target}\n")
                f.write("short_reason=run_scrape_returned_false\n")
        else:
            print(f"[OK] {target}")

        _record_scrape_run_observation(
            archive_db_path,
            run_id,
            started_at,
            run_kind,
            identity,
            scrape_outcome,
        )

    ended_at = datetime.now(timezone.utc).isoformat()
    total_targets = len(targets)
    if failed_targets == 0:
        final_status = "success"
    elif failed_targets == total_targets:
        final_status = "failure"
    else:
        final_status = "partial_failure"

    with log_path.open("a", encoding="utf-8") as f:
        f.write("BATCH_RUN_END\n")
        f.write(f"run_id={run_id}\n")
        f.write(f"started_at={started_at}\n")
        f.write(f"ended_at={ended_at}\n")
        f.write(f"total_targets={total_targets}\n")
        f.write(f"failed_targets={failed_targets}\n")
        f.write(f"final_status={final_status}\n")

    return final_status, failed_targets


def run_periodic_scrape(
    target_db_path: str,
    interval_seconds: float,
    max_runs: int | None = None,
) -> None:
    """Run full batch passes repeatedly with a fixed sleep interval."""

    global _inside_periodic_batch

    completed_runs = 0

    while max_runs is None or completed_runs < max_runs:
        run_number = completed_runs + 1
        print(f"[periodic] Run {run_number} starting")

        _inside_periodic_batch = True
        try:
            try:
                final_status, failed_targets = run_batch_scrape(target_db_path)
            except KeyboardInterrupt:
                print("Periodic execution interrupted. Exiting safely.")
                return
        finally:
            _inside_periodic_batch = False

        print(
            f"[periodic] Run {run_number} finished "
            f"with status={final_status} failed_targets={failed_targets}"
        )

        completed_runs += 1
        if max_runs is not None and completed_runs >= max_runs:
            return

        print(f"[periodic] Sleeping {interval_seconds} second(s)")
        try:
            time.sleep(interval_seconds)
        except KeyboardInterrupt:
            print("Periodic execution interrupted. Exiting safely.")
            return


def run_periodic_once(target_db_path: str) -> None:
    """Run one periodic cycle without requiring a sleep interval argument."""

    run_periodic_scrape(target_db_path, 0.0, max_runs=1)


def main():
    """
    CLIエントリポイント。
    - 通常: 記事URL指定でスクレイプ実行
    - inspect: DB内容表示
    """

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python main.py <article_url>")
        print("  python main.py inspect <article_id> <article_type> [--last N]")
        print("  python main.py export <article_id> <article_type> --format txt")
        print("  python main.py export <article_id> <article_type> --format md")
        print("  python main.py list-articles")
        print("  python main.py export-all-articles --format txt")
        print("  python main.py add-target <article_url> <target_db_path>")
        print(
            "  python main.py import-targets <targets_txt_path> "
            "<target_db_path>"
        )
        print("  python main.py resolve-article <article_url_or_full_title>")
        print("  python main.py targets <target_db_path>")
        print("  python main.py batch <target_db_path>")
        print("  python main.py periodic-once <target_db_path>")
        print(
            "  python main.py web [--host HOST] [--port PORT] "
            "[--target-db-path PATH]"
        )
        print(
            "  python main.py periodic <target_db_path> <interval_seconds> "
            "[--max-runs N]"
        )
        print(
            "  python main.py export-run-telemetry-csv "
            "[--db PATH] [--output PATH]"
        )
        print(
            "  python main.py operator ...  "
            "(target registry / archive; see docs/OPERATOR.md)"
        )
        sys.exit(1)

    if sys.argv[1] == "operator":
        from tools import operator as operator_cli

        sys.exit(operator_cli.dispatch_operator(sys.argv[2:]))

    # inspectモード
    if sys.argv[1] == "inspect":

        if len(sys.argv) < 4:
            print("Usage: inspect <article_id> <article_type> [--last N]")
            sys.exit(1)

        article_id = sys.argv[2]
        article_type = sys.argv[3]

        last_n = None
        if "--last" in sys.argv:
            idx = sys.argv.index("--last")
            last_n = int(sys.argv[idx + 1])

        inspect_article(article_id, article_type, last_n)
        return

    if sys.argv[1] == "export":

        if len(sys.argv) < 6 or sys.argv[4] != "--format":
            print("Usage: export <article_id> <article_type> --format txt|md")
            sys.exit(1)

        article_id = sys.argv[2]
        article_type = sys.argv[3]
        output_format = sys.argv[5]

        if not export_article(article_id, article_type, output_format):
            sys.exit(1)
        return

    if sys.argv[1] == "list-articles":
        list_articles()
        return

    if sys.argv[1] == "export-all-articles":

        if len(sys.argv) < 4 or sys.argv[2] != "--format":
            print("Usage: export-all-articles --format txt")
            sys.exit(1)

        if not export_all_articles(sys.argv[3]):
            sys.exit(1)
        return

    if sys.argv[1] == "export-run-telemetry-csv":
        db_path = _telemetry_archive_db_path()
        out_path = None
        idx = 2
        while idx < len(sys.argv):
            if sys.argv[idx] == "--db" and idx + 1 < len(sys.argv):
                db_path = sys.argv[idx + 1]
                idx += 2
                continue
            if sys.argv[idx] == "--output" and idx + 1 < len(sys.argv):
                out_path = sys.argv[idx + 1]
                idx += 2
                continue
            print(
                "Usage: export-run-telemetry-csv [--db PATH] [--output PATH]"
            )
            sys.exit(1)

        conn = init_db(db_path)
        try:
            csv_text = format_run_telemetry_csv_wide(conn)
        finally:
            conn.close()

        if out_path is not None:
            Path(out_path).write_text(csv_text, encoding="utf-8")
        else:
            print(csv_text, end="")
        return

    if sys.argv[1] == "add-target":

        if len(sys.argv) < 4:
            print("Usage: add-target <article_url> <target_db_path>")
            sys.exit(1)

        result = register_target_url(sys.argv[2], sys.argv[3])
        if result == "added":
            print(f"Added target: {sys.argv[2]}")
            return
        if result == "reactivated":
            print(f"Reactivated target: {sys.argv[2]}")
            return
        if result == "duplicate":
            print(f"Target already exists: {sys.argv[2]}")
            return

        print(f"Invalid target URL: {sys.argv[2]}")
        sys.exit(1)

    if sys.argv[1] == "import-targets":

        if len(sys.argv) < 4:
            print("Usage: import-targets <targets_txt_path> <target_db_path>")
            sys.exit(1)

        import_result = import_targets_from_text_file(sys.argv[2], sys.argv[3])
        print(
            f"Imported {import_result['processed']} target line(s) "
            f"from {import_result['source_path']} into "
            f"{import_result['target_db_path']}"
        )
        print(
            "added={added} duplicate={duplicate} "
            "reactivated={reactivated} invalid={invalid}".format(
                **import_result,
            )
        )
        return

    if sys.argv[1] == "resolve-article":

        if len(sys.argv) < 3:
            print("Usage: resolve-article <article_url_or_full_title>")
            sys.exit(1)

        result = resolve_article_input(sys.argv[2])
        if not result["ok"]:
            print(f"Article resolution failed: {result['failure_kind']}")
            print(f"Input: {result['normalized_input']}")
            sys.exit(1)

        print("Resolved article target")
        print(f"Input: {result['normalized_input']}")
        print(f"Matched By: {result['matched_by']}")
        print(f"Title: {result['title']}")
        print(f"URL: {result['canonical_target']['article_url']}")
        print(f"ID: {result['canonical_target']['article_id']}")
        print(f"Type: {result['canonical_target']['article_type']}")
        return

    if sys.argv[1] == "targets":

        if len(sys.argv) < 3:
            print("Usage: targets <target_db_path>")
            sys.exit(1)

        target_db_path = sys.argv[2]
        targets = list_active_target_urls(target_db_path)

        print(
            f"Loaded {len(targets)} active scrape target(s) "
            f"from target registry {target_db_path}"
        )
        for target in targets:
            print(target)
        return

    if sys.argv[1] == "batch":

        if len(sys.argv) < 3:
            print("Usage: batch <target_db_path>")
            sys.exit(1)

        _, failed_targets = run_batch_scrape(sys.argv[2])

        if failed_targets:
            sys.exit(1)
        return

    if sys.argv[1] == "periodic-once":

        if len(sys.argv) < 3:
            print("Usage: periodic-once <target_db_path>")
            sys.exit(1)

        run_periodic_once(sys.argv[2])
        return

    if sys.argv[1] == "periodic":

        if len(sys.argv) < 4:
            print(
                "Usage: periodic <target_db_path> <interval_seconds> "
                "[--max-runs N]"
            )
            sys.exit(1)

        target_db_path = sys.argv[2]
        interval_seconds = float(sys.argv[3])

        max_runs = None
        if "--max-runs" in sys.argv:
            idx = sys.argv.index("--max-runs")
            max_runs = int(sys.argv[idx + 1])

        run_periodic_scrape(target_db_path, interval_seconds, max_runs=max_runs)
        return

    if sys.argv[1] == "web":
        host = "127.0.0.1"
        port = 8000
        target_db_path = DEFAULT_TARGET_DB_PATH

        if "--host" in sys.argv:
            idx = sys.argv.index("--host")
            host = sys.argv[idx + 1]

        if "--port" in sys.argv:
            idx = sys.argv.index("--port")
            port = int(sys.argv[idx + 1])

        if "--target-db-path" in sys.argv:
            idx = sys.argv.index("--target-db-path")
            target_db_path = sys.argv[idx + 1]

        serve_web_app(
            host=host,
            port=port,
            target_db_path=target_db_path,
        )
        return

    # 通常スクレイプモード
    article_url = sys.argv[1]

    run_scrape(article_url)


if __name__ == "__main__":
    main()
