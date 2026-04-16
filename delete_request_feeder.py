from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from archive_read import read_article_archive
from article_resolver import resolve_article_input
from nicopedia_url import classify_and_normalize_nicopedia_url
from target_list import register_target_url


DELETE_REQUEST_ARTICLE_ID = "5511090"
DELETE_REQUEST_ARTICLE_TYPE = "a"

_URL_RE = re.compile(r"https?://dic\.nicovideo\.jp/[^\s\u3000<>\"']+")
_BOARD_MARKER = "【掲示板URL】"


@dataclass(frozen=True)
class DeleteRequestCandidate:
    res_no: int
    raw_url: str
    category: str
    normalized_article_url: str | None


def _state_path_for_target_db(target_db_path: str) -> Path:
    return Path(target_db_path).with_suffix(".delete_request_feeder.json")


def _read_state(state_path: Path) -> dict:
    try:
        raw = state_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {"last_processed_res_no": 0}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {"last_processed_res_no": 0}
    if not isinstance(parsed, dict):
        return {"last_processed_res_no": 0}
    last = parsed.get("last_processed_res_no", 0)
    if not isinstance(last, int) or last < 0:
        last = 0
    return {"last_processed_res_no": last}


def _write_state(state_path: Path, last_processed_res_no: int) -> None:
    payload = {"last_processed_res_no": int(max(last_processed_res_no, 0))}
    state_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _extract_urls_near_marker(text: str) -> list[str]:
    if not text:
        return []

    lines = text.splitlines()
    urls: list[str] = []
    for idx, line in enumerate(lines):
        window = []
        if _BOARD_MARKER in line:
            window.append(line)
            if idx + 1 < len(lines):
                window.append(lines[idx + 1])
            if idx + 2 < len(lines):
                window.append(lines[idx + 2])
        elif "dic.nicovideo.jp/" in line:
            window.append(line)

        if not window:
            continue

        for chunk in window:
            for match in _URL_RE.findall(chunk):
                url = match.rstrip(").,、。］】＞>\"'")
                if url:
                    urls.append(url)
    return urls


def _iter_candidates(
    responses: Iterable[tuple],
    *,
    last_processed_res_no: int,
) -> tuple[list[DeleteRequestCandidate], int]:
    candidates: list[DeleteRequestCandidate] = []
    max_seen_res_no = last_processed_res_no

    for res_no, _poster_name, _posted_at, _id_hash, content_text in responses:
        if res_no is None:
            continue
        try:
            res_no_int = int(res_no)
        except (TypeError, ValueError):
            continue

        if res_no_int > max_seen_res_no:
            max_seen_res_no = res_no_int
        if res_no_int <= last_processed_res_no:
            continue

        for raw_url in _extract_urls_near_marker(content_text or ""):
            classification = classify_and_normalize_nicopedia_url(raw_url)
            if not classification.supported:
                continue

            candidates.append(
                DeleteRequestCandidate(
                    res_no=res_no_int,
                    raw_url=raw_url,
                    category=classification.category,
                    normalized_article_url=classification.normalized_article_url,
                )
            )

    return candidates, max_seen_res_no


def _resolve_article_id_url(candidate_url: str) -> str | None:
    """
    Internal-only helper for /id/<article_id> to canonical /a/<slug>.

    Bounded behavior:
    - delegate to existing article_resolver (the user-input resolution route)
    - return canonical article_url on success
    - otherwise return None (skip)
    """

    result = resolve_article_input(candidate_url)
    if not result.get("ok"):
        return None
    canonical = result.get("canonical_target") or {}
    article_url = canonical.get("article_url")
    if not isinstance(article_url, str) or not article_url:
        return None
    return article_url


def run_delete_request_feeder(
    target_db_path: str,
    *,
    inspect: bool = False,
    stdout=None,
) -> dict:
    """
    Scan saved delete-request responses and hand off supported URLs to registry.

    Normal execution is tiny-summary only. When inspect=True, print candidates.
    """

    state_path = _state_path_for_target_db(target_db_path)
    state = _read_state(state_path)
    last_processed = int(state["last_processed_res_no"])

    archive = read_article_archive(
        DELETE_REQUEST_ARTICLE_ID,
        DELETE_REQUEST_ARTICLE_TYPE,
    )
    if not archive:
        return {
            "ok": False,
            "reason": "missing_saved_article",
            "checked_res_no_range": (last_processed + 1, last_processed),
            "extracted_candidates": 0,
            "handoff_attempts": 0,
            "updated_last_processed_res_no": last_processed,
        }

    candidates, max_seen = _iter_candidates(
        archive["responses"],
        last_processed_res_no=last_processed,
    )

    accepted_urls: list[str] = []
    handoff_attempts = 0

    for cand in candidates:
        article_url = cand.normalized_article_url
        if cand.category == "article_id":
            article_url = _resolve_article_id_url(cand.raw_url)
        if not article_url:
            continue

        handoff_attempts += 1
        status = register_target_url(article_url, target_db_path)
        if status in {"added", "reactivated", "duplicate"}:
            accepted_urls.append(article_url)

        if inspect and stdout is not None:
            stdout.write(
                f"res_no={cand.res_no} category={cand.category} "
                f"raw={cand.raw_url} normalized={article_url} "
                f"handoff={status}\n"
            )

    _write_state(state_path, max_seen)

    checked_start = last_processed + 1
    checked_end = max_seen
    if checked_end < checked_start:
        checked_end = last_processed

    return {
        "ok": True,
        "checked_res_no_range": (checked_start, checked_end),
        "extracted_candidates": len(candidates),
        "handoff_attempts": handoff_attempts,
        "accepted_article_urls": accepted_urls if inspect else None,
        "updated_last_processed_res_no": max_seen,
    }
