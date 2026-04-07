import os
from contextlib import contextmanager
from pathlib import Path

from archive_read import get_saved_article_summary
from operator_cli import inspect_target_for_operator, list_targets_for_operator
from orchestrator import run_scrape
from storage import format_run_telemetry_csv_wide, init_db
from target_list import parse_target_identity, register_target_url


DEFAULT_KGS_STATE_DIR = "runtime/smoke/kgs"


def _kgs_db_path(state_dir):
    return str(Path(state_dir) / "data" / "nicodic.db")


def _kgs_log_dir(state_dir):
    return str(Path(state_dir) / "logs")


@contextmanager
def _isolated_smoke_environment(state_dir):
    state_path = Path(state_dir)
    data_dir = state_path / "data"
    log_dir = state_path / "logs"
    data_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    previous_cwd = Path.cwd()
    old_nicodic_db_path = os.environ.get("NICODIC_DB_PATH")
    old_batch_log_dir = os.environ.get("BATCH_LOG_DIR")

    os.environ["NICODIC_DB_PATH"] = str(data_dir / "nicodic.db")
    os.environ["BATCH_LOG_DIR"] = str(log_dir)
    os.chdir(state_path)

    try:
        yield {
            "state_dir": str(state_path),
            "db_path": str(data_dir / "nicodic.db"),
            "log_dir": str(log_dir),
        }
    finally:
        os.chdir(previous_cwd)

        if old_nicodic_db_path is None:
            os.environ.pop("NICODIC_DB_PATH", None)
        else:
            os.environ["NICODIC_DB_PATH"] = old_nicodic_db_path

        if old_batch_log_dir is None:
            os.environ.pop("BATCH_LOG_DIR", None)
        else:
            os.environ["BATCH_LOG_DIR"] = old_batch_log_dir


def _print_kgs_header(action, article_url, state):
    print("=== KGS LIVE SMOKE ===")
    print("Mode: manual opt-in non-gating helper")
    print(f"Action: {action}")
    print(f"Known-Good Target: {article_url}")
    print(f"Isolated State Dir: {state['state_dir']}")
    print(f"Isolated DB: {state['db_path']}")
    print(f"Existing Batch Logs: {state['log_dir']}")


def _print_kgs_saved_summary(article_id, article_type):
    summary = get_saved_article_summary(article_id, article_type)
    if not summary["found"]:
        print("Archive Summary: not found in isolated state")
        return

    print("Archive Summary: saved in isolated state")
    print(f"Article ID: {summary['article_id']}")
    print(f"Article Type: {summary['article_type']}")
    print(f"Title: {summary['title']}")
    print(f"Saved Responses: {summary['response_count']}")
    print(f"Archive URL: {summary['url']}")


def _resolve_saved_article_id(db_path, canonical_url, article_type):
    conn = init_db(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT article_id
            FROM articles
            WHERE canonical_url=? AND article_type=?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (canonical_url, article_type),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return row[0]
    finally:
        conn.close()


def _drop_latest_saved_responses(article_id, article_type, db_path, count):
    conn = init_db(db_path)
    try:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT COUNT(*), COALESCE(MAX(res_no), 0)
            FROM responses
            WHERE article_id=? AND article_type=?
            """,
            (article_id, article_type),
        )
        before_count, before_max = cur.fetchone()

        print("KGS Trim Debug: begin")
        print(f"KGS Trim Debug: article_id={article_id}")
        print(f"KGS Trim Debug: article_type={article_type}")
        print(f"KGS Trim Debug: db_path={db_path}")
        print(f"KGS Trim Debug: requested_drop_last={count}")
        print(f"KGS Trim Debug: saved_response_count_before={before_count}")
        print(f"KGS Trim Debug: max_res_no_before={before_max}")

        cur.execute(
            """
            SELECT res_no
            FROM responses
            WHERE article_id=? AND article_type=?
            ORDER BY res_no DESC
            LIMIT ?
            """,
            (article_id, article_type, count),
        )
        rows = cur.fetchall()
        if not rows:
            print("KGS Trim Debug: selected_res_nos=[]")
            print("KGS Trim Debug: actual_removed=0")
            print("KGS Trim Debug: end")
            return 0

        res_numbers = [row[0] for row in rows]
        print(f"KGS Trim Debug: selected_res_nos={res_numbers}")

        placeholders = ", ".join(["?"] * len(res_numbers))
        cur.execute(
            f"DELETE FROM responses WHERE article_id=? AND article_type=? "
            f"AND res_no IN ({placeholders})",
            (article_id, article_type, *res_numbers),
        )
        conn.commit()

        cur.execute(
            """
            SELECT COUNT(*), COALESCE(MAX(res_no), 0)
            FROM responses
            WHERE article_id=? AND article_type=?
            """,
            (article_id, article_type),
        )
        after_count, after_max = cur.fetchone()

        print(f"KGS Trim Debug: actual_removed={len(res_numbers)}")
        print(f"KGS Trim Debug: saved_response_count_after={after_count}")
        print(f"KGS Trim Debug: max_res_no_after={after_max}")
        print("KGS Trim Debug: end")

        return len(res_numbers)
    finally:
        conn.close()


def verify_one_shot_fetch(article_url):
    identity = parse_target_identity(article_url)
    if identity is None:
        print("Verification fetch rejected")
        print("Reason: input must be a canonical Nicopedia article URL")
        print(f"Input: {article_url}")
        return False

    print("=== VERIFICATION FETCH ===")
    print(f"Target: {identity['canonical_url']}")

    result = run_scrape(identity["canonical_url"])
    if not result:
        print("Result: fetch failed")
        print(f"Outcome: {result.outcome}")
        return False

    summary = get_saved_article_summary(
        identity["article_id"],
        identity["article_type"],
    )
    print("Result: fetch completed")
    print(f"Outcome: {result.outcome}")
    if summary["found"]:
        print(f"Article ID: {summary['article_id']}")
        print(f"Article Type: {summary['article_type']}")
        print(f"Title: {summary['title']}")
        print(f"Saved Responses: {summary['response_count']}")
        print(f"Archive URL: {summary['url']}")
    return True


def verify_registry_list(target_db_path, active_only=False):
    print("=== VERIFICATION REGISTRY CHECK ===")
    return list_targets_for_operator(target_db_path, active_only=active_only)


def verify_registry_inspect(article_id, article_type, target_db_path):
    print("=== VERIFICATION REGISTRY CHECK ===")
    return inspect_target_for_operator(article_id, article_type, target_db_path)


def verify_one_shot_batch(target_db_path, run_batch_scrape_func):
    print("=== VERIFICATION BATCH RUN ===")
    print(f"Target DB: {target_db_path}")

    final_status, failed_targets = run_batch_scrape_func(target_db_path)
    print("=== VERIFICATION BATCH SUMMARY ===")
    print(f"Final Status: {final_status}")
    print(f"Failed Targets: {failed_targets}")
    return failed_targets == 0


def verify_telemetry_export(db_path, output_path=None):
    conn = init_db(db_path)
    try:
        csv_text = format_run_telemetry_csv_wide(conn)
    finally:
        conn.close()

    if output_path is None:
        print(csv_text, end="")
        return True

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(csv_text, encoding="utf-8")

    print("Verification telemetry export written")
    print(f"DB: {db_path}")
    print(f"Output: {output_path}")
    return True


def verify_kgs_fetch(article_url, state_dir, followup_drop_last=0):
    identity = parse_target_identity(article_url)
    if identity is None:
        print("KGS smoke rejected")
        print("Reason: input must be a canonical Nicopedia article URL")
        print(f"Input: {article_url}")
        return False

    with _isolated_smoke_environment(state_dir) as state:
        _print_kgs_header("fetch", identity["canonical_url"], state)
        print("KGS messages are stdout-only for this helper")
        print("Phase: initial-fetch")

        result = run_scrape(identity["canonical_url"])
        if not result:
            print("Result: initial-fetch failed")
            print(f"Outcome: {result.outcome}")
            return False

        print("Result: initial-fetch passed")
        print(f"Outcome: {result.outcome}")
        _print_kgs_saved_summary(
            identity["article_id"],
            identity["article_type"],
        )

        if followup_drop_last > 0:
            canonical_id = _resolve_saved_article_id(
                state["db_path"],
                identity["canonical_url"],
                identity["article_type"],
            )

            print("Phase: bounded-follow-up")
            print(f"KGS Debug: identity_article_id={identity['article_id']}")
            print(f"KGS Debug: canonical_article_id={canonical_id}")
            print(f"KGS Debug: identity_type={identity['article_type']}")

            if canonical_id is None:
                print("Result: bounded-follow-up failed")
                print("Outcome: could not resolve canonical article id")
                return False

            removed = _drop_latest_saved_responses(
                canonical_id,
                identity["article_type"],
                state["db_path"],
                followup_drop_last,
            )
            print(f"Follow-Up Trimmed Responses: {removed}")

            result = run_scrape(identity["canonical_url"])
            if not result:
                print("Result: bounded-follow-up failed")
                print(f"Outcome: {result.outcome}")
                return False

            print("Result: bounded-follow-up passed")
            print(f"Outcome: {result.outcome}")
            _print_kgs_saved_summary(
                canonical_id,
                identity["article_type"],
            )

        print("KGS Summary: pass")
        print(f"Telemetry DB: {state['db_path']}")
        return True


def verify_kgs_batch(article_url, state_dir, run_batch_scrape_func):
    identity = parse_target_identity(article_url)
    if identity is None:
        print("KGS smoke rejected")
        print("Reason: input must be a canonical Nicopedia article URL")
        print(f"Input: {article_url}")
        return False

    with _isolated_smoke_environment(state_dir) as state:
        _print_kgs_header("batch", identity["canonical_url"], state)
        print("KGS messages are stdout-only for this helper")
        register_result = register_target_url(
            identity["canonical_url"],
            state["db_path"],
        )
        print(f"Phase: target-registration ({register_result})")
        print("Phase: batch-run")

        final_status, failed_targets = run_batch_scrape_func(state["db_path"])
        print("=== KGS BATCH SUMMARY ===")
        print(f"Final Status: {final_status}")
        print(f"Failed Targets: {failed_targets}")
        _print_kgs_saved_summary(
            identity["article_id"],
            identity["article_type"],
        )
        print(f"Telemetry DB: {state['db_path']}")
        return failed_targets == 0
