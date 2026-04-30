import os
import sys
import uuid
import time
from datetime import datetime, timezone
from pathlib import Path

from archive_read import write_scrape_targets_txt
from article_resolver import resolve_article_input
from cli import export_all_articles, export_article, inspect_article, list_articles
from delete_request_feeder import (
    DEFAULT_DELETE_REQUEST_FEED_STATE_PATH,
    append_batch_targets,
    format_delete_request_feed_inspect_lines,
    format_delete_request_feed_summary,
    inspect_delete_request_feed,
    run_delete_request_feeder,
)
from host_cron import HostCronReporter, compress_weekly_archives, local_now
from host_cron import rotate_active_log
from identity_merge import (
    format_merge_summary_lines,
    merge_canonical_url_identities,
)
from operator_cli import add_target_for_operator
from operator_cli import deactivate_target_for_operator
from operator_cli import export_archive_for_operator
from operator_cli import export_registered_articles_csv_for_operator
from operator_cli import inspect_archive_for_operator
from operator_cli import inspect_target_for_operator
from operator_cli import list_archives_for_operator, list_targets_for_operator
from operator_cli import reactivate_target_for_operator
from operator_cli import show_scraped_res_for_operator
from orchestrator import run_scrape
from storage import (
    DEFAULT_DB_PATH,
    append_scrape_run_observation,
    format_run_telemetry_csv_wide,
    init_db,
)
from target_list import (
    handoff_redirected_target,
    import_targets_from_text_file,
    list_active_target_urls,
    parse_target_identity,
    register_target_url,
)
from verification_cli import verify_one_shot_batch
from verification_cli import verify_one_shot_fetch
from verification_cli import verify_registry_inspect, verify_registry_list
from verification_cli import verify_telemetry_export
from verification_cli import DEFAULT_KGS_STATE_DIR
from verification_cli import verify_kgs_batch, verify_kgs_fetch
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


def _batch_log_value(value: object) -> str:
    if value is None:
        return "unknown"
    text = str(value)
    return text if text else "unknown"


def _batch_log_result(scrape_result) -> str:
    if not scrape_result:
        return "FAIL"
    return "SUCCESS"


def _append_batch_log_lines(log_path: Path, lines: list[str]) -> None:
    with log_path.open("a", encoding="utf-8") as f:
        for line in lines:
            f.write(f"{line}\n")


def _append_batch_run_start(
    log_path: Path,
    run_id: str,
    started_at: str,
    target_db_path: str,
    total_targets: int,
) -> None:
    _append_batch_log_lines(
        log_path,
        [
            "BATCH_RUN_START",
            f"  run_id={run_id}",
            f"  started_at={started_at}",
            f"  target_db_path={target_db_path}",
            "  target_source=target_table",
            f"  total_targets={total_targets}",
        ],
    )


def _append_batch_progress(
    log_path: Path,
    index: int,
    total: int,
    result: str,
    target_url: str,
    article_title: str,
    collected_response_count: int,
    observed_max_res_no: int | None,
) -> None:
    _append_batch_log_lines(
        log_path,
        [
            f"[PROGRESS = {index}/{total}]",
            f"  result={result}",
            f"  target_url={target_url}",
            f"  article_title={_batch_log_value(article_title)}",
            (
                "  collected_response_count="
                f"{_batch_log_value(collected_response_count)}"
            ),
            f"  observed_max_res_no={_batch_log_value(observed_max_res_no)}",
        ],
    )


def _append_batch_failure_detail(
    log_path: Path,
    index: int,
    total: int,
    target_url: str,
    article_title: str,
    failure_page: str | None,
    failure_cause: str | None,
    collected_response_count: int,
    observed_max_res_no: int | None,
    short_reason: str,
) -> None:
    _append_batch_log_lines(
        log_path,
        [
            "  FAILURE_DETAIL",
            f"    progress={index}/{total}",
            f"    target_url={target_url}",
            f"    article_title={_batch_log_value(article_title)}",
            f"    failure_page={_batch_log_value(failure_page)}",
            f"    failure_cause={_batch_log_value(failure_cause)}",
            (
                "    collected_response_count="
                f"{_batch_log_value(collected_response_count)}"
            ),
            (
                "    observed_max_res_no="
                f"{_batch_log_value(observed_max_res_no)}"
            ),
            f"    short_reason={short_reason}",
        ],
    )


def _append_batch_redirect_detail(
    log_path: Path,
    index: int,
    total: int,
    target_url: str,
    redirect_target_url: str,
    source_status: str,
    register_status: str,
) -> None:
    _append_batch_log_lines(
        log_path,
        [
            "  REDIRECT_DETAIL",
            f"    progress={index}/{total}",
            f"    source_target_url={target_url}",
            f"    redirect_target_url={redirect_target_url}",
            f"    source_status={source_status}",
            f"    register_status={register_status}",
        ],
    )


def _append_batch_run_end(
    log_path: Path,
    run_id: str,
    started_at: str,
    ended_at: str,
    duration_seconds: int,
    total_targets: int,
    success_targets: int,
    failed_targets: int,
    final_status: str,
) -> None:
    _append_batch_log_lines(
        log_path,
        [
            "BATCH_RUN_END",
            f"  run_id={run_id}",
            f"  started_at={started_at}",
            f"  ended_at={ended_at}",
            f"  duration_seconds={duration_seconds}",
            f"  total_targets={total_targets}",
            f"  success_targets={success_targets}",
            f"  failed_targets={failed_targets}",
            f"  final_status={final_status}",
        ],
    )


def _append_delete_request_feed_summary(log_path: Path, summary: dict) -> None:
    _append_batch_log_lines(
        log_path,
        [
            "DELETE_REQUEST_FEED",
            f"  {format_delete_request_feed_summary(summary)}",
        ],
    )


def _emit_delete_request_feed_summary(progress_reporter, summary: dict) -> None:
    if not hasattr(progress_reporter, "emit"):
        return

    progress_reporter.emit(
        "FEEDER",
        format_delete_request_feed_summary(summary),
        indent_level=1,
    )


def run_batch_scrape(
    target_db_path: str,
    progress_reporter=None,
) -> tuple[str, int]:
    """Run one full batch pass and return (final_status, failed_targets)."""

    run_kind = "periodic_batch" if _inside_periodic_batch else "batch"

    run_id = uuid.uuid4().hex[:12]
    started_at = datetime.now(timezone.utc).isoformat()
    archive_db_path = _telemetry_archive_db_path()
    log_dir = Path(os.environ.get("BATCH_LOG_DIR", "data/batch_runs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"batch_{run_id}.log"

    existing_targets = list_active_target_urls(target_db_path)
    delete_request_feed_summary = run_delete_request_feeder(
        target_db_path,
        archive_db_path=archive_db_path,
    )
    targets = append_batch_targets(
        existing_targets,
        delete_request_feed_summary["queued_target_urls"],
    )

    if progress_reporter is None:
        print(
            "[delete-request-feed] "
            f"{format_delete_request_feed_summary(delete_request_feed_summary)}"
        )
        print(
            f"Loaded {len(targets)} active scrape target(s) "
            f"from target registry {target_db_path}"
        )
    else:
        _emit_delete_request_feed_summary(
            progress_reporter,
            delete_request_feed_summary,
        )
        progress_reporter.note_targets_loaded(len(targets), target_db_path)

    _append_batch_run_start(
        log_path,
        run_id,
        started_at,
        target_db_path,
        len(targets),
    )
    _append_delete_request_feed_summary(log_path, delete_request_feed_summary)

    failed_targets = 0
    for idx, target in enumerate(targets, start=1):
        if progress_reporter is None:
            print(f"[{idx}/{len(targets)}] Scraping: {target}")
        identity = parse_target_identity(target)
        if identity is None:
            failed_targets += 1
            if progress_reporter is None:
                print(f"[FAIL] {target} (invalid target URL shape)")
            else:
                progress_reporter.finish_target(
                    "fail",
                    target,
                    0,
                    target,
                    reason="reason=invalid_target_url_shape",
                )
            _append_batch_progress(
                log_path,
                idx,
                len(targets),
                "FAIL",
                target,
                "unknown",
                0,
                None,
            )
            _append_batch_failure_detail(
                log_path,
                idx,
                len(targets),
                target,
                "unknown",
                "unknown",
                "invalid_target_url_shape",
                0,
                None,
                "invalid_target_url_shape",
            )
            continue

        scrape_outcome = "fail_exception"
        try:
            if progress_reporter is None:
                scrape_result = run_scrape(target)
            else:
                scrape_result = run_scrape(
                    target,
                    progress_reporter=progress_reporter,
                    target_index=idx,
                    target_total=len(targets),
                )
            scrape_outcome = scrape_result.outcome
        except Exception as exc:
            failed_targets += 1
            if progress_reporter is None:
                print(f"[FAIL] {target} ({type(exc).__name__}: {exc})")
            else:
                progress_reporter.finish_target(
                    "fail",
                    identity["article_id"],
                    0,
                    identity["article_id"],
                    reason=f"reason={type(exc).__name__}:{exc}",
                )
            short_reason = f"{type(exc).__name__}: {exc}"
            _append_batch_progress(
                log_path,
                idx,
                len(targets),
                "FAIL",
                target,
                "unknown",
                0,
                None,
            )
            _append_batch_failure_detail(
                log_path,
                idx,
                len(targets),
                target,
                "unknown",
                "unknown",
                type(exc).__name__,
                0,
                None,
                short_reason,
            )
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
        redirect_target_url = getattr(
            scrape_result,
            "redirect_target_url",
            None,
        )
        if redirect_target_url is not None:
            handoff_result = handoff_redirected_target(
                identity["article_id"],
                identity["article_type"],
                redirect_target_url,
                target_db_path,
            )
            redirect_status = handoff_result["status"]
            register_status = handoff_result.get("register_status", "unknown")

            if redirect_status != "redirected":
                failed_targets += 1
                scrape_outcome = "fail_exception"
                if progress_reporter is None:
                    print(
                        f"[FAIL] {target} "
                        f"(redirect handoff failed: {redirect_status})"
                    )
                else:
                    progress_reporter.finish_target(
                        "fail",
                        identity["article_id"],
                        0,
                        identity["article_id"],
                        reason=(
                            "reason=redirect_handoff_failed "
                            f"status={redirect_status}"
                        ),
                    )
                _append_batch_progress(
                    log_path,
                    idx,
                    len(targets),
                    "FAIL",
                    target,
                    getattr(scrape_result, "article_title", "unknown"),
                    0,
                    None,
                )
                _append_batch_failure_detail(
                    log_path,
                    idx,
                    len(targets),
                    target,
                    getattr(scrape_result, "article_title", "unknown"),
                    target,
                    "redirect_handoff_failed",
                    0,
                    None,
                    redirect_status,
                )
                _record_scrape_run_observation(
                    archive_db_path,
                    run_id,
                    started_at,
                    run_kind,
                    identity,
                    scrape_outcome,
                )
                continue

            if progress_reporter is None:
                print(
                    f"[OK] {target} "
                    f"(redirected -> {redirect_target_url}; {register_status})"
                )
            else:
                progress_reporter.finish_target(
                    "success",
                    getattr(scrape_result, "article_title", identity["article_id"]),
                    0,
                    identity["article_id"],
                    reason=(
                        "reason=redirect_detected "
                        f"redirect_target={redirect_target_url} "
                        f"handoff_status={register_status}"
                    ),
                )
            _append_batch_progress(
                log_path,
                idx,
                len(targets),
                "SUCCESS",
                target,
                getattr(scrape_result, "article_title", "unknown"),
                0,
                None,
            )
            _append_batch_redirect_detail(
                log_path,
                idx,
                len(targets),
                target,
                redirect_target_url,
                redirect_status,
                register_status,
            )
            _record_scrape_run_observation(
                archive_db_path,
                run_id,
                started_at,
                run_kind,
                identity,
                scrape_outcome,
            )
            continue

        if not ok:
            failed_targets += 1
            if progress_reporter is None:
                print(f"[FAIL] {target}")
            short_reason = getattr(
                scrape_result,
                "short_reason",
                None,
            ) or "run_scrape_returned_false"
            _append_batch_progress(
                log_path,
                idx,
                len(targets),
                "FAIL",
                target,
                getattr(scrape_result, "article_title", "unknown"),
                getattr(scrape_result, "collected_response_count", 0),
                getattr(scrape_result, "observed_max_res_no", None),
            )
            _append_batch_failure_detail(
                log_path,
                idx,
                len(targets),
                target,
                getattr(scrape_result, "article_title", "unknown"),
                getattr(scrape_result, "failure_page", None),
                getattr(scrape_result, "failure_cause", scrape_result.outcome),
                getattr(scrape_result, "collected_response_count", 0),
                getattr(scrape_result, "observed_max_res_no", None),
                short_reason,
            )
        else:
            if progress_reporter is None:
                print(f"[OK] {target}")
            _append_batch_progress(
                log_path,
                idx,
                len(targets),
                _batch_log_result(scrape_result),
                target,
                getattr(scrape_result, "article_title", "unknown"),
                getattr(scrape_result, "collected_response_count", 0),
                getattr(scrape_result, "observed_max_res_no", None),
            )

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

    duration_seconds = int(
        max(
            (
                datetime.fromisoformat(ended_at)
                - datetime.fromisoformat(started_at)
            ).total_seconds(),
            0,
        )
    )
    _append_batch_run_end(
        log_path,
        run_id,
        started_at,
        ended_at,
        duration_seconds,
        total_targets,
        total_targets - failed_targets,
        failed_targets,
        final_status,
    )

    try:
        write_scrape_targets_txt()
    except Exception:
        pass

    return final_status, failed_targets


def _run_periodic_once_with_host_cron(
    target_db_path: str,
    host_cron_log_path: str,
) -> None:
    log_path = Path(host_cron_log_path)
    run_now = local_now()
    warnings: list[str] = []

    try:
        rotation_outcome = rotate_active_log(log_path, run_now.date())
        if rotation_outcome.warning is not None:
            warnings.append(rotation_outcome.warning)
    except OSError as exc:
        warnings.append(
            "host_cron_rotation_failed "
            f"reason={type(exc).__name__}:{exc}"
        )

    try:
        warnings.extend(compress_weekly_archives(log_path.parent, run_now.date()))
    except OSError as exc:
        warnings.append(
            "host_cron_weekly_archive_failed "
            f"reason={type(exc).__name__}:{exc}"
        )

    try:
        stream = log_path.open("a", encoding="utf-8")
    except OSError:
        run_periodic_scrape(target_db_path, 0.0, max_runs=1)
        return

    reporter = HostCronReporter(stream)

    try:
        reporter.begin_run()
        for warning in warnings:
            reporter.note_maintenance_warning(warning)

        global _inside_periodic_batch
        _inside_periodic_batch = True
        try:
            final_status, _failed_targets = run_batch_scrape(
                target_db_path,
                progress_reporter=reporter,
            )
        except KeyboardInterrupt:
            reporter.note_maintenance_warning("periodic_execution_interrupted")
            reporter.finish_run("failure")
            return
        except Exception as exc:
            reporter.emit(
                "ERROR",
                f"periodic_once_unhandled reason={type(exc).__name__}:{exc}",
                indent_level=1,
            )
            reporter.finish_run("failure")
            raise
        finally:
            _inside_periodic_batch = False

        reporter.finish_run(final_status)
    finally:
        stream.close()


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

    host_cron_log_path = os.environ.get("HOST_CRON_LOG_PATH")
    if host_cron_log_path:
        _run_periodic_once_with_host_cron(target_db_path, host_cron_log_path)
        return

    run_periodic_scrape(target_db_path, 0.0, max_runs=1)


def _read_optional_flag(args, flag_name, default=None):
    if flag_name not in args:
        return default

    idx = args.index(flag_name)
    if idx + 1 >= len(args):
        raise ValueError(f"Missing value for {flag_name}")
    return args[idx + 1]


def _print_operator_usage():
    print("Operator usage:")
    print("  python main.py operator target list [--db PATH] [--active-only]")
    print(
        "  python main.py operator target inspect <article_id> "
        "<article_type> [--db PATH]"
    )
    print("  python main.py operator target add <canonical_article_url> [--db PATH]")
    print(
        "  python main.py operator target deactivate <article_id> "
        "<article_type> [--db PATH]"
    )
    print(
        "  python main.py operator target reactivate <article_id> "
        "<article_type> [--db PATH]"
    )
    print("  python main.py operator archive list")
    print(
        "  python main.py operator archive inspect <article_id> "
        "<article_type> [--last N]"
    )
    print(
        "  python main.py operator archive export <article_id> "
        "<article_type> --format txt|md [--output PATH]"
    )
    print(
        "  python main.py operator archive export-registered-csv "
        "[--output PATH]"
    )
    print(
        "  python main.py operator merge canonical-url "
        "--db PATH [--apply]"
    )


def _handle_show_scraped_res(args):
    is_id = False
    article_input = None
    requested_format = "txt"
    idx = 0

    while idx < len(args):
        if args[idx] == "--id" and idx + 1 < len(args):
            is_id = True
            article_input = args[idx + 1]
            idx += 2
        elif args[idx] == "--title" and idx + 1 < len(args):
            article_input = args[idx + 1]
            idx += 2
        elif args[idx] == "--txt":
            requested_format = "txt"
            idx += 1
        elif args[idx] == "--md":
            requested_format = "md"
            idx += 1
        elif args[idx] == "--csv":
            requested_format = "csv"
            idx += 1
        elif not args[idx].startswith("-"):
            article_input = args[idx]
            idx += 1
        else:
            print(f"Unknown argument: {args[idx]}", file=sys.stderr)
            sys.exit(1)

    if article_input is None:
        print(
            "Usage: show-scraped-res [TITLE] [--id ID] "
            "[--title TITLE] [--txt|--md|--csv]",
            file=sys.stderr,
        )
        sys.exit(1)

    if not show_scraped_res_for_operator(
        article_input,
        is_id=is_id,
        requested_format=requested_format,
    ):
        sys.exit(1)


def _print_verification_usage():
    print("Verification usage:")
    print("  python main.py verify fetch <canonical_article_url>")
    print(
        "  python main.py verify kgs fetch <canonical_article_url> "
        "[--state-dir PATH] [--followup-drop-last N]"
    )
    print(
        "  python main.py verify kgs batch <canonical_article_url> "
        "[--state-dir PATH]"
    )
    print(
        "  python main.py verify registry list "
        "[--db PATH] [--active-only]"
    )
    print(
        "  python main.py verify registry inspect <article_id> "
        "<article_type> [--db PATH]"
    )
    print("  python main.py verify batch run [--db PATH]")
    print(
        "  python main.py verify telemetry export "
        "[--db PATH] [--output PATH]"
    )


def _print_delete_request_feed_usage():
    print(
        "Usage: inspect-delete-request-feed "
        "[--archive-db PATH] [--state-path PATH] [--full-scan]"
    )


def _handle_operator_target(args):
    if not args:
        _print_operator_usage()
        sys.exit(1)

    action = args[0]
    target_db_path = _read_optional_flag(args, "--db", DEFAULT_TARGET_DB_PATH)

    if action == "list":
        active_only = "--active-only" in args[1:]
        if not list_targets_for_operator(target_db_path, active_only=active_only):
            sys.exit(1)
        return

    if action == "inspect":
        if len(args) < 3:
            print(
                "Usage: operator target inspect <article_id> "
                "<article_type> [--db PATH]"
            )
            sys.exit(1)
        if not inspect_target_for_operator(args[1], args[2], target_db_path):
            sys.exit(1)
        return

    if action == "add":
        if len(args) < 2:
            print("Usage: operator target add <canonical_article_url> [--db PATH]")
            sys.exit(1)
        if not add_target_for_operator(args[1], target_db_path):
            sys.exit(1)
        return

    if action == "deactivate":
        if len(args) < 3:
            print(
                "Usage: operator target deactivate <article_id> "
                "<article_type> [--db PATH]"
            )
            sys.exit(1)
        if not deactivate_target_for_operator(args[1], args[2], target_db_path):
            sys.exit(1)
        return

    if action == "reactivate":
        if len(args) < 3:
            print(
                "Usage: operator target reactivate <article_id> "
                "<article_type> [--db PATH]"
            )
            sys.exit(1)
        if not reactivate_target_for_operator(args[1], args[2], target_db_path):
            sys.exit(1)
        return

    _print_operator_usage()
    sys.exit(1)


def _handle_operator_archive(args):
    if not args:
        _print_operator_usage()
        sys.exit(1)

    action = args[0]

    if action == "list":
        if not list_archives_for_operator():
            sys.exit(1)
        return

    if action == "inspect":
        if len(args) < 3:
            print(
                "Usage: operator archive inspect <article_id> "
                "<article_type> [--last N]"
            )
            sys.exit(1)
        last_n = None
        if "--last" in args:
            try:
                last_n = int(_read_optional_flag(args, "--last"))
            except ValueError:
                print(
                    "Usage: operator archive inspect <article_id> "
                    "<article_type> [--last N]"
                )
                sys.exit(1)
        if not inspect_archive_for_operator(args[1], args[2], last_n=last_n):
            sys.exit(1)
        return

    if action == "export":
        if len(args) < 5 or "--format" not in args:
            print(
                "Usage: operator archive export <article_id> <article_type> "
                "--format txt|md [--output PATH]"
            )
            sys.exit(1)
        try:
            output_format = _read_optional_flag(args, "--format")
            output_path = _read_optional_flag(args, "--output", None)
        except ValueError:
            print(
                "Usage: operator archive export <article_id> <article_type> "
                "--format txt|md [--output PATH]"
            )
            sys.exit(1)

        if not export_archive_for_operator(
            args[1],
            args[2],
            output_format,
            output_path=output_path,
        ):
            sys.exit(1)
        return

    if action == "export-registered-csv":
        try:
            output_path = _read_optional_flag(args, "--output", None)
        except ValueError:
            print(
                "Usage: operator archive export-registered-csv "
                "[--output PATH]"
            )
            sys.exit(1)

        if not export_registered_articles_csv_for_operator(
            output_path=output_path,
        ):
            sys.exit(1)
        return

    _print_operator_usage()
    sys.exit(1)


def _handle_operator_merge(args):
    if not args or args[0] != "canonical-url":
        print(
            "Usage: operator merge canonical-url --db PATH [--apply]",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        db_path = _read_optional_flag(args, "--db", None)
    except ValueError:
        print(
            "Usage: operator merge canonical-url --db PATH [--apply]",
            file=sys.stderr,
        )
        sys.exit(1)

    if not db_path:
        print(
            "operator merge canonical-url requires an explicit --db PATH",
            file=sys.stderr,
        )
        print(
            "Refusing to use the runtime DB as an implicit default.",
            file=sys.stderr,
        )
        sys.exit(1)

    apply_flag = "--apply" in args[1:]

    try:
        summary = merge_canonical_url_identities(db_path, apply=apply_flag)
    except FileNotFoundError as exc:
        print(f"DB path does not exist: {exc}", file=sys.stderr)
        sys.exit(1)
    except ValueError as exc:
        print(f"Invalid input: {exc}", file=sys.stderr)
        sys.exit(1)

    for line in format_merge_summary_lines(db_path, summary):
        print(line)


def _handle_operator_cli(args):
    if len(args) < 2:
        _print_operator_usage()
        sys.exit(1)

    area = args[0]
    if area == "target":
        _handle_operator_target(args[1:])
        return
    if area == "archive":
        _handle_operator_archive(args[1:])
        return
    if area == "merge":
        _handle_operator_merge(args[1:])
        return

    _print_operator_usage()
    sys.exit(1)


def _handle_verification_registry(args):
    if not args:
        _print_verification_usage()
        sys.exit(1)

    action = args[0]
    target_db_path = _read_optional_flag(args, "--db", DEFAULT_TARGET_DB_PATH)

    if action == "list":
        active_only = "--active-only" in args[1:]
        if not verify_registry_list(target_db_path, active_only=active_only):
            sys.exit(1)
        return

    if action == "inspect":
        if len(args) < 3:
            print(
                "Usage: verify registry inspect <article_id> "
                "<article_type> [--db PATH]"
            )
            sys.exit(1)
        if not verify_registry_inspect(args[1], args[2], target_db_path):
            sys.exit(1)
        return

    _print_verification_usage()
    sys.exit(1)


def _handle_verification_batch(args):
    if not args or args[0] != "run":
        print("Usage: verify batch run [--db PATH]")
        sys.exit(1)

    target_db_path = _read_optional_flag(args, "--db", DEFAULT_TARGET_DB_PATH)
    if not verify_one_shot_batch(target_db_path, run_batch_scrape):
        sys.exit(1)


def _handle_verification_telemetry(args):
    if not args or args[0] != "export":
        print("Usage: verify telemetry export [--db PATH] [--output PATH]")
        sys.exit(1)

    try:
        db_path = _read_optional_flag(
            args,
            "--db",
            _telemetry_archive_db_path(),
        )
        output_path = _read_optional_flag(args, "--output", None)
    except ValueError:
        print("Usage: verify telemetry export [--db PATH] [--output PATH]")
        sys.exit(1)

    if not verify_telemetry_export(db_path, output_path=output_path):
        sys.exit(1)


def _handle_verification_kgs(args):
    if len(args) < 2:
        _print_verification_usage()
        sys.exit(1)

    action = args[0]
    article_url = args[1]
    state_dir = _read_optional_flag(args, "--state-dir", DEFAULT_KGS_STATE_DIR)

    if action == "fetch":
        followup_drop_last = 0
        if "--followup-drop-last" in args:
            try:
                followup_drop_last = int(
                    _read_optional_flag(args, "--followup-drop-last"),
                )
            except ValueError:
                print(
                    "Usage: verify kgs fetch <canonical_article_url> "
                    "[--state-dir PATH] [--followup-drop-last N]"
                )
                sys.exit(1)

        if not verify_kgs_fetch(
            article_url,
            state_dir,
            followup_drop_last=followup_drop_last,
        ):
            sys.exit(1)
        return

    if action == "batch":
        if not verify_kgs_batch(article_url, state_dir, run_batch_scrape):
            sys.exit(1)
        return

    _print_verification_usage()
    sys.exit(1)


def _handle_verification_cli(args):
    if not args:
        _print_verification_usage()
        sys.exit(1)

    area = args[0]
    if area == "fetch":
        if len(args) < 2:
            print("Usage: verify fetch <canonical_article_url>")
            sys.exit(1)
        if not verify_one_shot_fetch(args[1]):
            sys.exit(1)
        return

    if area == "kgs":
        _handle_verification_kgs(args[1:])
        return

    if area == "registry":
        _handle_verification_registry(args[1:])
        return

    if area == "batch":
        _handle_verification_batch(args[1:])
        return

    if area == "telemetry":
        _handle_verification_telemetry(args[1:])
        return

    _print_verification_usage()
    sys.exit(1)


def main():
    """
    CLIエントリポイント。
    - 通常: 記事URL指定でスクレイプ実行
    - inspect: DB内容表示
    """

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python main.py <article_url>")
        print("  python main.py operator <target|archive> ...")
        print(
            "  python main.py verify <fetch|kgs|registry|batch|telemetry> ..."
        )
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
            "  python main.py inspect-delete-request-feed "
            "[--archive-db PATH] [--state-path PATH] [--full-scan]"
        )
        print(
            "  python main.py show-scraped-res [TITLE] "
            "[--id ID] [--title TITLE] [--txt|--md|--csv]"
        )
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
        sys.exit(1)

    if sys.argv[1] == "operator":
        _handle_operator_cli(sys.argv[2:])
        return

    if sys.argv[1] == "verify":
        _handle_verification_cli(sys.argv[2:])
        return

    if sys.argv[1] == "show-scraped-res":
        _handle_show_scraped_res(sys.argv[2:])
        return

    if sys.argv[1] == "inspect-delete-request-feed":
        archive_db_path = _telemetry_archive_db_path()
        state_path = DEFAULT_DELETE_REQUEST_FEED_STATE_PATH
        full_scan = False

        idx = 2
        while idx < len(sys.argv):
            if sys.argv[idx] == "--archive-db" and idx + 1 < len(sys.argv):
                archive_db_path = sys.argv[idx + 1]
                idx += 2
                continue
            if sys.argv[idx] == "--state-path" and idx + 1 < len(sys.argv):
                state_path = sys.argv[idx + 1]
                idx += 2
                continue
            if sys.argv[idx] == "--full-scan":
                full_scan = True
                idx += 1
                continue

            _print_delete_request_feed_usage()
            sys.exit(1)

        scan_result = inspect_delete_request_feed(
            archive_db_path=archive_db_path,
            state_path=state_path,
            full_scan=full_scan,
        )
        for line in format_delete_request_feed_inspect_lines(scan_result):
            print(line)
        return

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
