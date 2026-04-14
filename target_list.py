from pathlib import Path
from urllib.parse import urlparse

from storage import get_target, init_db, list_targets, register_target
from storage import set_target_active_state
from storage import mark_target_redirected


def _parse_target_line(raw_line: str) -> str | None:
    line = raw_line.strip()

    if not line or line.startswith("#"):
        return None

    return line


def parse_target_identity(article_url: str) -> dict | None:
    """Return article_id, article_type, canonical_url for a valid target URL."""

    return _parse_target_identity(article_url)


def _parse_target_identity(article_url: str) -> dict | None:
    candidate = article_url.strip()
    if not candidate:
        return None

    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"}:
        return None
    if parsed.netloc != "dic.nicovideo.jp":
        return None

    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) != 2:
        return None

    article_type, article_id = path_parts
    if not article_type or not article_id:
        return None

    return {
        "article_id": article_id,
        "article_type": article_type,
        "canonical_url": (
            f"https://dic.nicovideo.jp/{article_type}/{article_id}"
        ),
    }


def list_active_target_urls(target_db_path: str) -> list[str]:
    """Load active target URLs from the SQLite-backed target registry."""

    conn = init_db(target_db_path)
    try:
        return [entry["canonical_url"] for entry in list_targets(conn)]
    finally:
        conn.close()


def list_registered_targets(
    target_db_path: str,
    *,
    active_only: bool = False,
) -> list[dict]:
    """Load registry entries for operator-facing list views."""

    conn = init_db(target_db_path)
    try:
        return list_targets(conn, active_only=active_only)
    finally:
        conn.close()


def validate_target_url(article_url: str) -> bool:
    return parse_target_identity(article_url) is not None


def register_target_url(article_url: str, target_db_path: str) -> str:
    target_identity = parse_target_identity(article_url)
    if target_identity is None:
        return "invalid"

    conn = init_db(target_db_path)
    try:
        result = register_target(
            conn,
            target_identity["article_id"],
            target_identity["article_type"],
            target_identity["canonical_url"],
        )
    finally:
        conn.close()

    return result["status"]


def inspect_registered_target(
    article_id: str,
    article_type: str,
    target_db_path: str,
) -> dict | None:
    """Load one target registry entry for operator-facing inspection."""

    conn = init_db(target_db_path)
    try:
        return get_target(conn, article_id, article_type)
    finally:
        conn.close()


def deactivate_target(
    article_id: str,
    article_type: str,
    target_db_path: str,
) -> dict:
    """Deactivate one target without removing it from the registry."""

    conn = init_db(target_db_path)
    try:
        return set_target_active_state(conn, article_id, article_type, False)
    finally:
        conn.close()


def reactivate_target(
    article_id: str,
    article_type: str,
    target_db_path: str,
) -> dict:
    """Reactivate one target already present in the registry."""

    conn = init_db(target_db_path)
    try:
        return set_target_active_state(conn, article_id, article_type, True)
    finally:
        conn.close()


def import_targets_from_text_file(
    source_path: str,
    target_db_path: str,
) -> dict:
    """Import legacy plain-text targets into the SQLite registry once."""

    counts = {
        "source_path": source_path,
        "target_db_path": target_db_path,
        "processed": 0,
        "added": 0,
        "duplicate": 0,
        "reactivated": 0,
        "invalid": 0,
    }

    lines = Path(source_path).read_text(encoding="utf-8").splitlines()

    conn = init_db(target_db_path)
    try:
        for raw_line in lines:
            line = _parse_target_line(raw_line)
            if line is None:
                continue

            counts["processed"] += 1
            target_identity = _parse_target_identity(line)
            if target_identity is None:
                counts["invalid"] += 1
                continue

            result = register_target(
                conn,
                target_identity["article_id"],
                target_identity["article_type"],
                target_identity["canonical_url"],
            )
            counts[result["status"]] += 1
    finally:
        conn.close()

    return counts


def handoff_redirected_target(
    source_article_id: str,
    source_article_type: str,
    redirect_target_url: str,
    *,
    target_db_path: str,
    detected_at: str,
) -> dict:
    """
    Persist redirect state and hand off to the redirect target.

    - mark source target redirected/inactive
    - register redirect target if it is a valid canonical target URL
    - suppress duplicate/loop handoffs
    """

    redirect_identity = parse_target_identity(redirect_target_url)
    if redirect_identity is None:
        return {
            "ok": False,
            "status": "invalid_redirect_target",
            "redirect_target_url": redirect_target_url,
            "redirect_register_status": None,
            "source_mark_status": None,
        }

    if (
        redirect_identity["article_id"] == source_article_id
        and redirect_identity["article_type"] == source_article_type
    ):
        return {
            "ok": False,
            "status": "redirect_loop",
            "redirect_target_url": redirect_identity["canonical_url"],
            "redirect_register_status": None,
            "source_mark_status": None,
        }

    conn = init_db(target_db_path)
    try:
        source_mark = mark_target_redirected(
            conn,
            source_article_id,
            source_article_type,
            redirect_url=redirect_identity["canonical_url"],
            detected_at=detected_at,
        )
        register_result = register_target(
            conn,
            redirect_identity["article_id"],
            redirect_identity["article_type"],
            redirect_identity["canonical_url"],
        )
    finally:
        conn.close()

    return {
        "ok": True,
        "status": "handed_off",
        "redirect_target_url": redirect_identity["canonical_url"],
        "redirect_register_status": register_result["status"],
        "source_mark_status": source_mark["status"],
        "source_entry": source_mark["entry"],
        "redirect_entry": register_result["entry"],
    }
