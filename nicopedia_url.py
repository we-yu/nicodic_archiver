from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse


NICO_DIC_HOST = "dic.nicovideo.jp"


@dataclass(frozen=True)
class NicopediaUrlClassification:
    raw_url: str
    category: str
    supported: bool
    normalized_article_url: str | None
    reject_reason: str | None = None


def _drop_query_and_fragment(candidate_url: str) -> str | None:
    parsed = urlparse(candidate_url.strip())
    if parsed.scheme not in {"http", "https"}:
        return None
    if parsed.netloc != NICO_DIC_HOST:
        return None
    if not parsed.path:
        return None
    return parsed._replace(query="", fragment="").geturl()


def classify_and_normalize_nicopedia_url(raw_url: str) -> NicopediaUrlClassification:
    """
    Classify and (if supported) normalize a Nicopedia URL for article targeting.

    Supported (accepted) inputs:
    - /a/<slug>
    - /id/<article_id>  (normalized_article_url is None; requires resolver)
    - /b/a/<slug>/...
    - /t/b/a/<slug>/...
    - /t/a/<slug>

    Unsupported (reject):
    - /v/...  (video/media)
    - /u/...  (user)
    - /l/...  (live/other)
    - /b/c/...  (community board)
    - malformed / incomplete
    """

    normalized = _drop_query_and_fragment(raw_url)
    if normalized is None:
        return NicopediaUrlClassification(
            raw_url=raw_url,
            category="malformed",
            supported=False,
            normalized_article_url=None,
            reject_reason="not_nicopedia_url",
        )

    parsed = urlparse(normalized)
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        return NicopediaUrlClassification(
            raw_url=raw_url,
            category="malformed",
            supported=False,
            normalized_article_url=None,
            reject_reason="empty_path",
        )

    head = parts[0]
    if head == "a":
        if len(parts) != 2 or not parts[1]:
            return NicopediaUrlClassification(
                raw_url=raw_url,
                category="malformed",
                supported=False,
                normalized_article_url=None,
                reject_reason="missing_article_slug",
            )
        return NicopediaUrlClassification(
            raw_url=raw_url,
            category="article_direct",
            supported=True,
            normalized_article_url=f"https://{NICO_DIC_HOST}/a/{parts[1]}",
        )

    if head == "id":
        if len(parts) != 2 or not parts[1]:
            return NicopediaUrlClassification(
                raw_url=raw_url,
                category="malformed",
                supported=False,
                normalized_article_url=None,
                reject_reason="missing_article_id",
            )
        return NicopediaUrlClassification(
            raw_url=raw_url,
            category="article_id",
            supported=True,
            normalized_article_url=None,
        )

    if head == "b":
        if len(parts) >= 3 and parts[1] == "a" and parts[2]:
            return NicopediaUrlClassification(
                raw_url=raw_url,
                category="article_board",
                supported=True,
                normalized_article_url=f"https://{NICO_DIC_HOST}/a/{parts[2]}",
            )
        if len(parts) >= 3 and parts[1] == "c":
            return NicopediaUrlClassification(
                raw_url=raw_url,
                category="community_board",
                supported=False,
                normalized_article_url=None,
                reject_reason="unsupported_board",
            )
        return NicopediaUrlClassification(
            raw_url=raw_url,
            category="unsupported_board",
            supported=False,
            normalized_article_url=None,
            reject_reason="unsupported_board",
        )

    if head == "t":
        if len(parts) >= 4 and parts[1] == "b" and parts[2] == "a" and parts[3]:
            return NicopediaUrlClassification(
                raw_url=raw_url,
                category="article_thread_board",
                supported=True,
                normalized_article_url=f"https://{NICO_DIC_HOST}/a/{parts[3]}",
            )
        if len(parts) == 3 and parts[1] == "a" and parts[2]:
            return NicopediaUrlClassification(
                raw_url=raw_url,
                category="article_thread_direct",
                supported=True,
                normalized_article_url=f"https://{NICO_DIC_HOST}/a/{parts[2]}",
            )
        return NicopediaUrlClassification(
            raw_url=raw_url,
            category="unsupported_thread",
            supported=False,
            normalized_article_url=None,
            reject_reason="unsupported_thread",
        )

    if head == "v":
        return NicopediaUrlClassification(
            raw_url=raw_url,
            category="video",
            supported=False,
            normalized_article_url=None,
            reject_reason="unsupported_video",
        )

    if head == "u":
        return NicopediaUrlClassification(
            raw_url=raw_url,
            category="user",
            supported=False,
            normalized_article_url=None,
            reject_reason="unsupported_user",
        )

    if head == "l":
        return NicopediaUrlClassification(
            raw_url=raw_url,
            category="live_or_other",
            supported=False,
            normalized_article_url=None,
            reject_reason="unsupported_live",
        )

    return NicopediaUrlClassification(
        raw_url=raw_url,
        category="unknown",
        supported=False,
        normalized_article_url=None,
        reject_reason="unknown_path",
    )
