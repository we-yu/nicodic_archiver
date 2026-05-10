"""
Shared extraction of canonical Nicopedia identity from article HTML metadata.

Registration and scraping both require a numeric NicoNicoPedia article ID and a
canonical /a/<slug> URL derived from meta tags—not the slash-segment slug.
"""

import re
from urllib.parse import urljoin

from storage import validate_saved_article_identity
from dicopedia_urls import parse_target_identity


class ArticleIdMismatchError(ValueError):
    """Metadata numeric article ID disagrees with a /id/<digits> URL input."""


_OG_URL_ID_RE = re.compile(r"/id/([0-9]+)(?:/|$)")


def extract_article_title_from_meta(soup) -> str | None:
    title_tag = soup.find("meta", property="og:title")
    if title_tag is None:
        return None
    raw = title_tag.get("content", "").strip()
    if not raw:
        return None
    return raw.split("とは")[0]


def normalize_redirect_target_url(
    article_url: str,
    candidate_url: str,
) -> str | None:
    raw_target = candidate_url.strip().strip("\"'")
    if not raw_target:
        return None
    resolved_target = urljoin(article_url, raw_target)
    target_identity = parse_target_identity(resolved_target)
    if target_identity is None:
        return None
    return target_identity["canonical_url"]


def extract_canonical_article_a_url(article_url: str, soup) -> str | None:
    canonical_tag = soup.find(
        "link",
        rel=lambda value: value and "canonical" in value,
    )
    if canonical_tag is not None:
        href = canonical_tag.get("href", "").strip()
        canonical_url = normalize_redirect_target_url(article_url, href)
        canonical_identity = parse_target_identity(canonical_url or "")
        if (
            canonical_identity is not None
            and canonical_identity["article_type"] == "a"
        ):
            return canonical_url

    og_tag = soup.find("meta", property="og:url")
    if og_tag is None:
        return None

    return normalize_redirect_target_url(
        article_url,
        og_tag.get("content", "").strip(),
    )


def extract_numeric_nicopedia_article_id(soup, canonical_article_url: str) -> str:
    og_url = soup.find("meta", property="og:url")
    og_content = og_url.get("content", "").strip() if og_url else ""
    if og_content:
        match = _OG_URL_ID_RE.search(og_content)
        if match is not None:
            numeric_id = match.group(1)
            if numeric_id and numeric_id.isdigit():
                return numeric_id

    canonical_identity = parse_target_identity(canonical_article_url)
    if canonical_identity is None:
        raise ValueError(
            f"invalid canonical_article_url for ID extraction: "
            f"{canonical_article_url!r}"
        )

    candidate = canonical_identity["article_id"]
    if candidate and candidate.isdigit():
        return candidate

    raise ValueError(
        "unable to extract numeric article ID from page metadata"
    )


def resolve_registration_identity_from_html(
    soup,
    fetched_url: str,
    *,
    input_id_numeric_candidate: str | None = None,
) -> tuple[dict[str, str], str]:
    """Return canonical_target dict and title string, or raise ValueError."""

    canonical_article_url = extract_canonical_article_a_url(fetched_url, soup)
    if canonical_article_url is None:
        raise ValueError("missing canonical Nicopedia article URL")

    tgt = parse_target_identity(canonical_article_url)
    if tgt is None or tgt["article_type"] != "a":
        raise ValueError("canonical metadata is not article type /a")

    numeric_article_id = extract_numeric_nicopedia_article_id(
        soup,
        canonical_article_url,
    )
    validate_saved_article_identity(numeric_article_id, "a")

    if (
        input_id_numeric_candidate is not None
        and input_id_numeric_candidate.isdigit()
        and input_id_numeric_candidate != numeric_article_id
    ):
        raise ArticleIdMismatchError(
            "metadata numeric article ID does not match /id/<digits> input"
        )

    title = extract_article_title_from_meta(soup)
    if title is None:
        raise ValueError("missing og:title metadata")

    return (
        {
            "article_url": tgt["canonical_url"],
            "article_id": numeric_article_id,
            "article_type": "a",
        },
        title,
    )
