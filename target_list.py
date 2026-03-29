from pathlib import Path
from urllib.parse import urlparse

from storage import init_db, list_targets, register_target


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
