import re
import sys
from pathlib import Path
from urllib.parse import unquote

from archive_read import (
    export_registered_articles_csv,
    get_saved_article_export,
    get_saved_article_summary_by_exact_title,
    get_saved_article_summary_by_id,
    read_article_archive,
    read_article_summaries,
)
from cli import build_archive_export
from target_list import (
    deactivate_target,
    inspect_registered_target,
    list_registered_targets,
    reactivate_target,
    register_target_url,
)


def _target_status_label(entry):
    return "active" if entry["is_active"] else "inactive"


def list_targets_for_operator(target_db_path, active_only=False):
    entries = list_registered_targets(target_db_path, active_only=active_only)
    if not entries:
        if active_only:
            print(f"No active targets in registry: {target_db_path}")
        else:
            print(f"No targets in registry: {target_db_path}")
        return True

    scope_label = "active-only" if active_only else "all-status"
    print("=== TARGET REGISTRY ===")
    print(f"DB: {target_db_path}")
    print(f"Scope: {scope_label}")
    print(f"Count: {len(entries)}")
    for entry in entries:
        print(
            f"{_target_status_label(entry):8} "
            f"{entry['article_id']} {entry['article_type']} | "
            f"url={entry['canonical_url']} | "
            f"created_at={entry['created_at']}"
        )
    return True


def inspect_target_for_operator(article_id, article_type, target_db_path):
    entry = inspect_registered_target(article_id, article_type, target_db_path)
    if entry is None:
        print("Target not found in registry")
        print(f"DB: {target_db_path}")
        print(f"ID: {article_id}")
        print(f"Type: {article_type}")
        return False

    print("=== TARGET DETAIL ===")
    print(f"DB: {target_db_path}")
    print(f"Registry Row ID: {entry['id']}")
    print(f"Status: {_target_status_label(entry)}")
    print(f"Article ID: {entry['article_id']}")
    print(f"Article Type: {entry['article_type']}")
    print(f"Canonical URL: {entry['canonical_url']}")
    print(f"Created At: {entry['created_at']}")
    return True


def add_target_for_operator(article_url, target_db_path):
    result = register_target_url(article_url, target_db_path)
    if result == "added":
        print("Target registry updated")
        print("Action: add")
        print(f"DB: {target_db_path}")
        print(f"Canonical URL: {article_url}")
        print("Result: added")
        return True
    if result == "reactivated":
        print("Target registry updated")
        print("Action: add")
        print(f"DB: {target_db_path}")
        print(f"Canonical URL: {article_url}")
        print("Result: reactivated existing target")
        return True
    if result == "duplicate":
        print("Target registry unchanged")
        print("Action: add")
        print(f"DB: {target_db_path}")
        print(f"Canonical URL: {article_url}")
        print("Result: already active")
        return True

    print("Target registry update failed")
    print("Action: add")
    print(f"DB: {target_db_path}")
    print(f"Input: {article_url}")
    print("Reason: input must be a canonical Nicopedia article URL")
    return False


def _print_target_state_change(action, target_db_path, result):
    if not result["found"]:
        print("Target registry update failed")
        print(f"Action: {action}")
        print(f"DB: {target_db_path}")
        print(f"ID: {result['target_identity']['article_id']}")
        print(f"Type: {result['target_identity']['article_type']}")
        print("Reason: target not found")
        return False

    entry = result["entry"]
    print("Target registry updated")
    print(f"Action: {action}")
    print(f"DB: {target_db_path}")
    print(f"ID: {entry['article_id']}")
    print(f"Type: {entry['article_type']}")
    print(f"Canonical URL: {entry['canonical_url']}")
    print(f"Status: {_target_status_label(entry)}")

    if result["status"] == "unchanged":
        print("Result: unchanged")
    else:
        print(f"Result: {result['status']}")
    return True


def deactivate_target_for_operator(article_id, article_type, target_db_path):
    result = deactivate_target(article_id, article_type, target_db_path)
    return _print_target_state_change("deactivate", target_db_path, result)


def reactivate_target_for_operator(article_id, article_type, target_db_path):
    result = reactivate_target(article_id, article_type, target_db_path)
    return _print_target_state_change("reactivate", target_db_path, result)


def list_archives_for_operator():
    summaries = read_article_summaries()
    if not summaries:
        print("No saved archives found.")
        return True

    print("=== SAVED ARCHIVES ===")
    print(f"Count: {len(summaries)}")
    for summary in summaries:
        print(
            f"{summary['article_id']} {summary['article_type']} | "
            f"title={summary['title']} | "
            f"responses={summary['response_count']} | "
            f"created_at={summary['created_at']}"
        )
    return True


def inspect_archive_for_operator(article_id, article_type, last_n=None):
    archive = read_article_archive(article_id, article_type, last_n=last_n)
    if not archive:
        print("Saved archive not found")
        print(f"ID: {article_id}")
        print(f"Type: {article_type}")
        return False

    print("=== ARCHIVE DETAIL ===")
    print(f"ID: {archive['article_id']}")
    print(f"Type: {archive['article_type']}")
    print(f"Title: {archive['title']}")
    print(f"URL: {archive['url']}")
    print(f"Created: {archive['created_at']}")
    print(f"Shown Responses: {len(archive['responses'])}")
    if last_n is not None:
        print(f"Inspect Scope: last {last_n} response(s)")

    print("")
    print("=== RESPONSES ===")
    for (
        res_no,
        poster_name,
        posted_at,
        id_hash,
        content_text,
    ) in archive["responses"]:
        poster_name = poster_name or "unknown"
        posted_at = posted_at or "unknown"
        id_hash = id_hash or "unknown"
        print(f">{res_no} {poster_name} {posted_at} ID: {id_hash}")
        print(content_text or "")
        print("----")
    return True


def export_archive_for_operator(
    article_id,
    article_type,
    output_format,
    output_path=None,
):
    export_result = build_archive_export(article_id, article_type, output_format)
    if not export_result["found"]:
        print("Saved archive not found")
        print(f"ID: {article_id}")
        print(f"Type: {article_type}")
        return False

    if export_result["content"] is None:
        print(f"Unsupported export format: {output_format}")
        return False

    if output_path is None:
        print(export_result["content"])
        return True

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(export_result["content"], encoding="utf-8")

    print("Archive export written")
    print(f"ID: {article_id}")
    print(f"Type: {article_type}")
    print(f"Format: {output_format}")
    print(f"Output: {output_path}")
    return True


def _admin_export_filename(article_id, article_type, title, fmt):
    decoded_id = unquote(article_id)
    safe_id = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", decoded_id.strip())
    safe_id = re.sub(r"\s+", " ", safe_id).strip(" .") or "article"
    safe_t = re.sub(
        r'[<>:"/\\|?*\x00-\x1f]', "_", (title or "").strip()
    )
    safe_t = re.sub(r"\s+", " ", safe_t).strip(" .") or "article"
    return f"{safe_id}{article_type}_{safe_t}.{fmt}"


def show_scraped_res_for_operator(
    article_input,
    is_id=False,
    requested_format="txt",
):
    """Write saved archive to stdout; status and errors to stderr."""

    if is_id:
        summary = get_saved_article_summary_by_id(article_input)
    else:
        summary = get_saved_article_summary_by_exact_title(article_input)

    if not summary["found"]:
        kind = "id" if is_id else "title"
        print(f"not found: {kind}={article_input}", file=sys.stderr)
        return False

    article_id = summary["article_id"]
    article_type = summary["article_type"]

    export = get_saved_article_export(
        article_id,
        article_type,
        requested_format,
    )
    if not export["found"]:
        print(
            f"not found in archive: "
            f"article_id={article_id} article_type={article_type}",
            file=sys.stderr,
        )
        return False

    title = export.get("title") or summary.get("title") or ""
    filename = _admin_export_filename(
        article_id, article_type, title, requested_format
    )
    print(f"ok: {filename}", file=sys.stderr)
    sys.stdout.write(export["content"])
    return True


def export_registered_articles_csv_for_operator(output_path=None):
    """Export all registered articles to CSV for internal operator use.

    Writes CSV to stdout when output_path is None, or to the given
    file path otherwise.  This is the all-records internal CLI route
    and is separate from the user-facing web CSV (current-page only).
    """
    csv_text = export_registered_articles_csv()
    if output_path is None:
        sys.stdout.write(csv_text)
        return True
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(csv_text, encoding="utf-8")
    print("Registered articles CSV written.", file=sys.stderr)
    print(f"Output: {output_path}", file=sys.stderr)
    return True
