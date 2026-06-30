"""Bounded HOT word target feeder.

Extracts recent article links from the NicoNicoPedia article
"今週のニコニコ大百科 HOTワード" and feeds them through the existing target
registration boundary so they can become normal scrape targets.

Scope is deliberately narrow and bounded:
- only the "過去のHOTワードbest3" section/table is inspected,
- only the most recent ``recent_weeks`` data rows are processed,
- only the 1st/2nd/3rd rank cells of each data row are read,
- candidate URLs are de-duplicated in first-seen order,
- registration reuses ``target_list.register_target_url`` (validation /
  normalization / denylist / duplicate-suppression all happen there).

This module never inserts into target tables directly and never raises out of
the extraction path on unexpected HTML.
"""

import os
from urllib.parse import urljoin

from dicopedia_urls import parse_target_identity
from http_client import fetch_page
from target_list import register_target_url


HOT_WORD_FEED_SOURCE_URL = os.environ.get(
    "HOT_WORD_FEED_SOURCE_URL",
    (
        "https://dic.nicovideo.jp/a/"
        "%E4%BB%8A%E9%80%B1%E3%81%AE%E3%83%8B%E3%82%B3%E3%83%8B%E3%82%B3"
        "%E5%A4%A7%E7%99%BE%E7%A7%91%20hot%E3%83%AF%E3%83%BC%E3%83%89"
    ),
)


def _resolve_default_recent_weeks(default: int = 12) -> int:
    raw = os.environ.get("HOT_WORD_FEED_RECENT_WEEKS")
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


DEFAULT_HOT_WORD_RECENT_WEEKS = _resolve_default_recent_weeks()

_BEST3_HEADING_MARKER = "過去のHOTワードbest3"
_DIC_BASE_URL = "https://dic.nicovideo.jp/"
_HEADING_TAGS = ("h1", "h2", "h3", "h4", "h5", "h6")
# Cell 0 of each data row is the 集計期間 (period / 第○回) cell. The article
# columns are the 1st/2nd/3rd rank cells, i.e. indices 1..3.
_RANK_CELL_SLICE = slice(1, 4)


def _find_best3_table(soup):
    """Locate the table following the 過去のHOTワードbest3 heading.

    Locating is structural: a real heading element (h1..h6) whose text carries
    the section marker, then the first table after it. The page-menu anchor
    that shares the same text is not a heading element, so it is ignored.
    """
    if soup is None:
        return None
    for heading in soup.find_all(_HEADING_TAGS):
        if _BEST3_HEADING_MARKER in heading.get_text(strip=True):
            return heading.find_next("table")
    return None


def _is_data_row(row) -> bool:
    """Data rows use td cells; the header row uses th and is excluded."""
    if row.find("th") is not None:
        return False
    return row.find("td") is not None


def _normalize_article_href(href):
    """Return the canonical dic article URL for a rank-cell link, else None.

    Accepts relative ``/a/...`` and absolute ``https://dic.nicovideo.jp/a/...``
    links. Non-dic links (e.g. ch.nicovideo.jp blomaga) and non-article types
    are rejected. This is a structural URL guard, not a display-text match.
    """
    if not href:
        return None
    candidate = href.strip()
    if not candidate:
        return None
    absolute = urljoin(_DIC_BASE_URL, candidate)
    identity = parse_target_identity(absolute)
    if identity is None:
        return None
    if identity["article_type"] != "a":
        return None
    return identity["canonical_url"]


def _extract_row_candidates(row) -> list[str]:
    """Extract article URLs from the 1st/2nd/3rd rank cells of one data row."""
    cells = row.find_all(["td", "th"], recursive=False)
    urls: list[str] = []
    for cell in cells[_RANK_CELL_SLICE]:
        for anchor in cell.find_all("a"):
            normalized = _normalize_article_href(anchor.get("href"))
            if normalized:
                urls.append(normalized)
    return urls


def _collect_candidates(soup, recent_weeks: int):
    """Return (unique_urls_first_seen, extracted_total) from the best3 table."""
    table = _find_best3_table(soup)
    if table is None:
        return [], 0
    seen: set[str] = set()
    ordered: list[str] = []
    extracted = 0
    rows_used = 0
    for row in table.find_all("tr"):
        if not _is_data_row(row):
            continue
        if rows_used >= recent_weeks:
            break
        rows_used += 1
        for url in _extract_row_candidates(row):
            extracted += 1
            if url in seen:
                continue
            seen.add(url)
            ordered.append(url)
    return ordered, extracted


def extract_hot_word_candidates(
    soup,
    recent_weeks: int = DEFAULT_HOT_WORD_RECENT_WEEKS,
) -> list[str]:
    """Return unique article URLs from the recent best3 rank cells.

    Never raises on unexpected HTML; returns [] instead.
    """
    try:
        ordered, _ = _collect_candidates(soup, recent_weeks)
        return ordered
    except Exception:
        return []


def scan_hot_word_feed(
    source_url: str = HOT_WORD_FEED_SOURCE_URL,
    recent_weeks: int = DEFAULT_HOT_WORD_RECENT_WEEKS,
    *,
    fetch=fetch_page,
) -> dict:
    """Fetch and extract candidates without registering anything.

    A source-page fetch failure is contained: ``fetch_ok`` is False and the
    candidate list is empty rather than raising.
    """
    fetch_ok = True
    candidate_urls: list[str] = []
    extracted = 0
    soup = None
    try:
        soup = fetch(source_url)
    except Exception:
        fetch_ok = False
    if soup is not None:
        try:
            candidate_urls, extracted = _collect_candidates(soup, recent_weeks)
        except Exception:
            candidate_urls, extracted = [], 0
    return {
        "source_url": source_url,
        "recent_weeks": recent_weeks,
        "fetch_ok": fetch_ok,
        "extracted_candidates": extracted,
        "unique_candidates": len(candidate_urls),
        "candidate_urls": candidate_urls,
    }


def _empty_register_counters() -> dict:
    return {
        "added_targets": 0,
        "reactivated_targets": 0,
        "duplicate_targets": 0,
        "denylisted_candidates": 0,
        "invalid_candidates": 0,
        "resolution_failures": 0,
        "registration_failures": 0,
        "queued_target_urls": [],
    }


def run_hot_word_feeder(
    target_db_path: str,
    source_url: str = HOT_WORD_FEED_SOURCE_URL,
    recent_weeks: int = DEFAULT_HOT_WORD_RECENT_WEEKS,
    *,
    fetch=fetch_page,
) -> dict:
    """Scan the source page and register candidates via the target boundary.

    Each candidate is registered independently; a per-candidate exception is
    contained and counted as a registration failure so one bad URL never
    aborts the batch run. Only newly added/reactivated targets are queued for
    same-shot inclusion.
    """
    scan = scan_hot_word_feed(source_url, recent_weeks, fetch=fetch)
    summary = {**scan, **_empty_register_counters()}
    for url in scan["candidate_urls"]:
        try:
            status = register_target_url(url, target_db_path)
        except Exception:
            summary["registration_failures"] += 1
            continue
        if status == "added":
            summary["added_targets"] += 1
            summary["queued_target_urls"].append(url)
        elif status == "reactivated":
            summary["reactivated_targets"] += 1
            summary["queued_target_urls"].append(url)
        elif status == "duplicate":
            summary["duplicate_targets"] += 1
        elif status == "denylisted":
            summary["denylisted_candidates"] += 1
        elif status == "resolution_failure":
            summary["resolution_failures"] += 1
        else:
            summary["invalid_candidates"] += 1
    return summary


def skipped_hot_word_feed_summary(
    source_url: str = HOT_WORD_FEED_SOURCE_URL,
    recent_weeks: int = DEFAULT_HOT_WORD_RECENT_WEEKS,
) -> dict:
    """Summary used when the feeder is disabled: no fetch, no registration."""
    return {
        "source_url": source_url,
        "recent_weeks": recent_weeks,
        "fetch_ok": False,
        "extracted_candidates": 0,
        "unique_candidates": 0,
        "candidate_urls": [],
        **_empty_register_counters(),
    }


def inspect_hot_word_feed(
    source_url: str = HOT_WORD_FEED_SOURCE_URL,
    recent_weeks: int = DEFAULT_HOT_WORD_RECENT_WEEKS,
    *,
    fetch=fetch_page,
) -> dict:
    """Scan-only entry point for operator inspection (no side effects)."""
    return scan_hot_word_feed(source_url, recent_weeks, fetch=fetch)


def format_hot_word_feed_summary(summary: dict) -> str:
    return " ".join(
        [
            f"fetch_ok={summary.get('fetch_ok', False)}",
            f"recent_weeks={summary.get('recent_weeks', 0)}",
            f"extracted={summary.get('extracted_candidates', 0)}",
            f"unique={summary.get('unique_candidates', 0)}",
            f"added={summary.get('added_targets', 0)}",
            f"reactivated={summary.get('reactivated_targets', 0)}",
            f"duplicate={summary.get('duplicate_targets', 0)}",
            f"denylisted={summary.get('denylisted_candidates', 0)}",
            f"invalid={summary.get('invalid_candidates', 0)}",
            f"resolution_failed={summary.get('resolution_failures', 0)}",
            f"register_failed={summary.get('registration_failures', 0)}",
        ]
    )


def format_hot_word_feed_inspect_lines(scan_result: dict) -> list[str]:
    lines = [
        f"SOURCE {scan_result.get('source_url', '')}",
        f"RECENT_WEEKS {scan_result.get('recent_weeks', 0)}",
        f"FETCH_OK {scan_result.get('fetch_ok', False)}",
        f"EXTRACTED {scan_result.get('extracted_candidates', 0)}",
        f"UNIQUE {scan_result.get('unique_candidates', 0)}",
    ]
    for url in scan_result.get("candidate_urls", []):
        lines.append(f"CANDIDATE {url}")
    lines.append("SUMMARY " + format_hot_word_feed_summary(scan_result))
    return lines
