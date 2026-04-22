import json
import os
import re
import sqlite3
from pathlib import Path
from urllib.parse import urlparse

from article_resolver import resolve_article_input
from storage import DEFAULT_DB_PATH, init_db
from target_list import register_target_url


DELETE_REQUEST_ARTICLE_ID = "5511090"
DELETE_REQUEST_ARTICLE_TYPE = "a"
DEFAULT_DELETE_REQUEST_FEED_STATE_PATH = os.environ.get(
    "DELETE_REQUEST_FEED_STATE_PATH",
    "data/delete_request_feeder_state.json",
)

SUPPORTED_DELETE_REQUEST_URL_CATEGORIES = {
    "article_direct",
    "article_id",
    "article_board",
    "article_thread_board",
    "article_thread_direct",
}

_URL_PATTERN = re.compile(r"https?://dic\.nicovideo\.jp/[^\s<>'\"）】]+")
_TRAILING_URL_CHARS = ".,)]}>】）"
_CONTROL_ESCAPE_PATTERN = re.compile(r"(?i)%(?:0[0-9a-f]|1[0-9a-f]|7f)")


def extract_delete_request_urls(text: str) -> list[str]:
    urls: list[str] = []
    for raw_url in _URL_PATTERN.findall(text or ""):
        urls.append(raw_url.rstrip(_TRAILING_URL_CHARS))
    return urls


def sanitize_delete_request_candidate(value: str | None) -> str | None:
    if value is None:
        return None

    cleaned = value.strip()
    cleaned = "".join(
        character for character in cleaned if character >= " " and character != "\x7f"
    )
    cleaned = _CONTROL_ESCAPE_PATTERN.sub("", cleaned).strip()
    if not cleaned:
        return None

    if _looks_like_url_input(cleaned):
        if classify_delete_request_url(cleaned) == "malformed":
            return None

    return cleaned


def classify_delete_request_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return "malformed"
    if parsed.netloc != "dic.nicovideo.jp":
        return "malformed"

    parts = _split_path_parts(parsed.path)
    if not parts:
        return "malformed"

    if parts[0] == "a":
        if len(parts) < 2 or not parts[1]:
            return "malformed"
        return "article_direct"

    if parts[0] == "id":
        if len(parts) < 2 or not parts[1]:
            return "malformed"
        return "article_id"

    if parts[0] == "b":
        if len(parts) < 2:
            return "malformed"
        if parts[1] == "a":
            if len(parts) < 3 or not parts[2]:
                return "malformed"
            return "article_board"
        if parts[1] == "c":
            return "community_board"
        return "unsupported_board"

    if parts[0] == "t":
        if len(parts) < 2:
            return "malformed"
        if parts[1] == "a":
            if len(parts) < 3 or not parts[2]:
                return "malformed"
            return "article_thread_direct"
        if parts[1] == "b":
            if len(parts) < 3:
                return "malformed"
            if parts[2] == "a":
                if len(parts) < 4 or not parts[3]:
                    return "malformed"
                return "article_thread_board"
            if parts[2] == "c":
                return "community_thread_board"
            return "unsupported_thread_board"
        return "unsupported_thread"

    if parts[0] == "v":
        return "video"
    if parts[0] == "u":
        return "user"
    if parts[0] == "l":
        return "live"

    return "unsupported"


def normalize_supported_delete_request_input(
    url: str,
    category: str,
    article_id_resolver=None,
) -> str | None:
    parts = _split_path_parts(urlparse(url).path)

    if category == "article_direct" and len(parts) >= 2:
        return _build_article_url(parts[1])

    if category == "article_board" and len(parts) >= 3:
        return _build_article_url(parts[2])

    if category == "article_thread_board" and len(parts) >= 4:
        return _build_article_url(parts[3])

    if category == "article_thread_direct" and len(parts) >= 3:
        return _build_article_url(parts[2])

    if category == "article_id" and len(parts) >= 2:
        if article_id_resolver is None:
            return None
        return article_id_resolver(parts[1])

    return None


def load_last_processed_res_no(state_path: str) -> int:
    path = Path(state_path)
    if not path.exists():
        return 0

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return 0

    try:
        return max(int(payload.get("last_processed_res_no", 0)), 0)
    except (TypeError, ValueError):
        return 0


def save_last_processed_res_no(state_path: str, res_no: int) -> None:
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"last_processed_res_no": int(res_no)}
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    path.write_text(text, encoding="utf-8")


def resolve_internal_article_id_input(
    article_id: str,
    archive_db_path: str | None = None,
) -> str | None:
    archive_db_path = _resolve_archive_db_path(archive_db_path)
    if not _can_open_archive_db(archive_db_path):
        return None

    conn = init_db(archive_db_path)
    try:
        row = conn.execute(
            """
            SELECT canonical_url, title
            FROM articles
            WHERE article_id = ? AND article_type = ?
            LIMIT 1
            """,
            (article_id, "a"),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    canonical_url, title = row
    if canonical_url:
        category = classify_delete_request_url(str(canonical_url))
        if category != "article_id":
            normalized = normalize_supported_delete_request_input(
                str(canonical_url),
                category,
            )
            if normalized is not None:
                return normalized

    if title:
        return str(title)

    return None


def append_batch_targets(
    existing_targets: list[str],
    queued_target_urls: list[str],
) -> list[str]:
    merged = list(existing_targets)
    seen = set(existing_targets)

    for target_url in queued_target_urls:
        if target_url in seen:
            continue
        merged.append(target_url)
        seen.add(target_url)

    return merged


def scan_delete_request_feed(
    archive_db_path: str | None = None,
    state_path: str | None = None,
    full_scan: bool = False,
) -> dict:
    archive_db_path = _resolve_archive_db_path(archive_db_path)
    state_path = state_path or DEFAULT_DELETE_REQUEST_FEED_STATE_PATH
    last_processed_res_no = 0 if full_scan else load_last_processed_res_no(state_path)

    responses = _load_delete_request_responses(
        archive_db_path,
        last_processed_res_no,
    )

    candidates: list[dict] = []
    accepted_candidates = 0
    skipped_invalid_candidates = 0
    skipped_resolution_failures = 0
    max_res_no = last_processed_res_no

    for response in responses:
        res_no = response["res_no"]
        max_res_no = max(max_res_no, res_no)
        for raw_url in extract_delete_request_urls(response["body"]):
            category = classify_delete_request_url(raw_url)
            normalized_input = None
            failure_kind = None
            if category in SUPPORTED_DELETE_REQUEST_URL_CATEGORIES:
                try:
                    normalized_input = normalize_supported_delete_request_input(
                        raw_url,
                        category,
                        article_id_resolver=(
                            lambda value: resolve_internal_article_id_input(
                                value,
                                archive_db_path=archive_db_path,
                            )
                        ),
                    )
                except Exception:
                    failure_kind = "resolution_failure"
                    skipped_resolution_failures += 1

            normalized_input = sanitize_delete_request_candidate(
                normalized_input,
            )
            if category in SUPPORTED_DELETE_REQUEST_URL_CATEGORIES:
                if normalized_input is None and failure_kind is None:
                    failure_kind = "invalid_candidate"
                    skipped_invalid_candidates += 1

            accepted = normalized_input is not None
            if accepted:
                accepted_candidates += 1

            candidates.append(
                {
                    "res_no": res_no,
                    "raw_url": raw_url,
                    "category": category,
                    "accepted": accepted,
                    "normalized_input": normalized_input,
                    "failure_kind": failure_kind,
                }
            )

    checked_to_res_no = max_res_no if responses else None
    updated_last_processed_res_no = max_res_no if responses else last_processed_res_no

    return {
        "candidates": candidates,
        "summary": {
            "checked_from_res_no": last_processed_res_no + 1,
            "checked_to_res_no": checked_to_res_no,
            "responses_checked": len(responses),
            "extracted_candidates": len(candidates),
            "accepted_candidates": accepted_candidates,
            "skipped_invalid_candidates": skipped_invalid_candidates,
            "skipped_resolution_failures": skipped_resolution_failures,
            "skipped_registration_failures": 0,
            "handed_off_candidates": 0,
            "updated_last_processed_res_no": updated_last_processed_res_no,
            "queued_target_urls": [],
            "processed_candidates": 0,
            "registered_candidates": 0,
            "added_targets": 0,
            "reactivated_targets": 0,
            "duplicate_targets": 0,
            "invalid_targets": 0,
        },
    }


def inspect_delete_request_feed(
    archive_db_path: str | None = None,
    state_path: str | None = None,
    full_scan: bool = False,
) -> dict:
    return scan_delete_request_feed(
        archive_db_path=archive_db_path,
        state_path=state_path,
        full_scan=full_scan,
    )


def run_delete_request_feeder(
    target_db_path: str,
    archive_db_path: str | None = None,
    state_path: str | None = None,
) -> dict:
    state_path = state_path or DEFAULT_DELETE_REQUEST_FEED_STATE_PATH
    scan_result = scan_delete_request_feed(
        archive_db_path=archive_db_path,
        state_path=state_path,
        full_scan=False,
    )
    summary = dict(scan_result["summary"])

    handed_off_candidates = 0
    skipped_invalid_candidates = summary.get("skipped_invalid_candidates", 0)
    skipped_resolution_failures = summary.get(
        "skipped_resolution_failures",
        0,
    )
    skipped_registration_failures = 0
    added_targets = 0
    reactivated_targets = 0
    duplicate_targets = 0
    invalid_targets = 0
    queued_target_urls: list[str] = []
    seen_inputs: set[str] = set()

    for candidate in scan_result["candidates"]:
        normalized_input = sanitize_delete_request_candidate(
            candidate["normalized_input"],
        )
        if not normalized_input:
            if candidate.get("accepted"):
                skipped_invalid_candidates += 1
            continue
        if normalized_input in seen_inputs:
            continue
        seen_inputs.add(normalized_input)

        handed_off_candidates += 1
        try:
            resolution = resolve_article_input(normalized_input)
        except Exception:
            skipped_resolution_failures += 1
            continue

        if not resolution["ok"]:
            skipped_invalid_candidates += 1
            invalid_targets += 1
            continue

        canonical_url = resolution["canonical_target"]["article_url"]
        try:
            register_status = register_target_url(canonical_url, target_db_path)
        except Exception:
            skipped_registration_failures += 1
            continue

        if register_status == "added":
            added_targets += 1
            queued_target_urls.append(canonical_url)
            continue
        if register_status == "reactivated":
            reactivated_targets += 1
            queued_target_urls.append(canonical_url)
            continue
        if register_status == "duplicate":
            duplicate_targets += 1
            continue

        invalid_targets += 1

    summary["handed_off_candidates"] = handed_off_candidates
    summary["processed_candidates"] = handed_off_candidates
    summary["queued_target_urls"] = queued_target_urls
    summary["registered_candidates"] = added_targets + reactivated_targets
    summary["added_targets"] = added_targets
    summary["reactivated_targets"] = reactivated_targets
    summary["duplicate_targets"] = duplicate_targets
    summary["invalid_targets"] = invalid_targets
    summary["skipped_invalid_candidates"] = skipped_invalid_candidates
    summary["skipped_resolution_failures"] = skipped_resolution_failures
    summary["skipped_registration_failures"] = skipped_registration_failures

    if summary["checked_to_res_no"] is not None:
        save_last_processed_res_no(
            state_path,
            summary["updated_last_processed_res_no"],
        )

    return summary


def format_delete_request_feed_summary(summary: dict) -> str:
    checked_to_res_no = summary.get("checked_to_res_no")
    if checked_to_res_no is None:
        checked_range = "none"
    else:
        checked_range = (
            f"{summary.get('checked_from_res_no')}-{checked_to_res_no}"
        )

    return " ".join(
        [
            f"checked_range={checked_range}",
            f"responses_checked={summary.get('responses_checked', 0)}",
            f"extracted_candidates={summary.get('extracted_candidates', 0)}",
            f"processed_candidates={summary.get('processed_candidates', 0)}",
            f"registered_candidates={summary.get('registered_candidates', 0)}",
            f"handed_off_candidates={summary.get('handed_off_candidates', 0)}",
            (
                "skipped_invalid_candidates="
                f"{summary.get('skipped_invalid_candidates', 0)}"
            ),
            (
                "skipped_resolution_failures="
                f"{summary.get('skipped_resolution_failures', 0)}"
            ),
            (
                "skipped_registration_failures="
                f"{summary.get('skipped_registration_failures', 0)}"
            ),
            (
                "updated_last_processed_res_no="
                f"{summary.get('updated_last_processed_res_no', 0)}"
            ),
        ]
    )


def format_delete_request_feed_inspect_lines(scan_result: dict) -> list[str]:
    lines: list[str] = []

    for candidate in scan_result["candidates"]:
        status = "ACCEPT" if candidate["accepted"] else "REJECT"
        parts = [
            status,
            f"res_no={candidate['res_no']}",
            f"category={candidate['category']}",
            f"url={candidate['raw_url']}",
        ]
        if candidate["normalized_input"] is not None:
            parts.append(
                f"normalized_input={candidate['normalized_input']}"
            )
        lines.append(" ".join(parts))

    lines.append(
        "SUMMARY " + format_delete_request_feed_summary(scan_result["summary"])
    )
    return lines


def _build_article_url(slug: str) -> str:
    return f"https://dic.nicovideo.jp/a/{slug}"


def _resolve_archive_db_path(archive_db_path: str | None) -> str:
    if archive_db_path is not None:
        return archive_db_path
    return os.environ.get("NICODIC_DB_PATH", DEFAULT_DB_PATH)


def _split_path_parts(path: str) -> list[str]:
    return [part for part in path.split("/") if part]


def _looks_like_url_input(value: str) -> bool:
    parsed = urlparse(value)
    return bool(parsed.scheme or parsed.netloc)


def _can_open_archive_db(archive_db_path: str) -> bool:
    if archive_db_path == ":memory:":
        return True
    return Path(archive_db_path).exists()


def _load_delete_request_responses(
    archive_db_path: str,
    last_processed_res_no: int,
) -> list[dict]:
    if not _can_open_archive_db(archive_db_path):
        return []

    conn = init_db(archive_db_path)
    try:
        rows = conn.execute(
            """
            SELECT res_no, COALESCE(content_text, '')
            FROM responses
            WHERE article_id = ?
              AND article_type = ?
              AND res_no > ?
            ORDER BY res_no ASC
            """,
            (
                DELETE_REQUEST_ARTICLE_ID,
                DELETE_REQUEST_ARTICLE_TYPE,
                int(last_processed_res_no),
            ),
        ).fetchall()
    except sqlite3.Error:
        conn.close()
        return []

    conn.close()
    return [
        {"res_no": int(res_no), "body": str(body or "")}
        for res_no, body in rows
    ]
