"""Parse Nicopedia dic.nicovideo.jp article URL shapes (syntax only).

This module must stay free of heavier resolution / registration imports.
"""

from urllib.parse import urlparse


def parse_target_identity(article_url: str) -> dict | None:
    """Return article_id path segment, type, canonical_url for dic URLs."""

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
