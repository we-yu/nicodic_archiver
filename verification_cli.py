from pathlib import Path

from archive_read import get_saved_article_summary
from operator_cli import inspect_target_for_operator, list_targets_for_operator
from orchestrator import run_scrape
from storage import format_run_telemetry_csv_wide, init_db
from target_list import parse_target_identity


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
