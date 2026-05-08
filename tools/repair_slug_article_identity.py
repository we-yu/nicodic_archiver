"""
Repair legacy slug article_id rows for article_type='a' archives.

Problem:
- Legacy rows may have article_type='a' but article_id is a URL-encoded
  /a/<slug> value (non-digits).

Goal:
- Normalize those groups so saved archive identity uses the numeric
  NicoNicoPedia article ID (digits-only string), while preserving:
  - article_type = 'a'
  - canonical_url = canonical /a/<slug> URL

Safety:
- explicit db_path required; never uses runtime defaults
- dry-run is default
- apply requires explicit flag
- apply runs in a transaction; failures rollback
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from orchestrator import fetch_article_metadata_record  # noqa: E402
from storage import validate_saved_article_identity  # noqa: E402
from target_list import parse_target_identity  # noqa: E402


DEFAULT_NETWORK_RETRIES = 2
DEFAULT_NETWORK_RETRY_DELAY_SECONDS = 1.0


class UnresolvedNetworkError(RuntimeError):
    """Raised when metadata resolution still fails after bounded retries."""


def _is_digits_only(value: str) -> bool:
    return bool(value) and value.isdigit()


def _normalize_network_retries(value: int) -> int:
    if value < 0:
        raise ValueError(
            f"network_retries must be non-negative, got {value}"
        )
    return value


def _normalize_network_retry_delay_seconds(value: float) -> float:
    if value < 0:
        raise ValueError(
            "network_retry_delay_seconds must be non-negative, "
            f"got {value}"
        )
    return value


def _canonical_url_key(canonical_url: str | None) -> str | None:
    if not canonical_url:
        return None
    text = canonical_url.strip()
    return text if text else None


def _is_canonical_a_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    if parsed.netloc != "dic.nicovideo.jp":
        return False
    parts = [part for part in parsed.path.split("/") if part]
    return len(parts) == 2 and parts[0] == "a" and bool(parts[1])


def _decoded_canonical_a_slug(canonical_url: str | None) -> str | None:
    if canonical_url is None or not _is_canonical_a_url(canonical_url):
        return None
    parsed = urlparse(canonical_url)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) != 2:
        return None
    slug = unquote(parts[1]).strip()
    return slug or None


def _is_legacy_slug_identity(
    article_id: str | None,
    canonical_url: str | None,
) -> bool:
    normalized_id = (article_id or "").strip()
    if not normalized_id:
        return True
    if not _is_digits_only(normalized_id):
        return True
    slug = _decoded_canonical_a_slug(canonical_url)
    if slug is None or not _is_digits_only(slug):
        return False
    return normalized_id == slug


def _resolve_numeric_article_id_from_db_rows(
    rows: list[dict],
    *,
    canonical_url: str,
) -> str | None:
    for row in rows:
        article_id = row.get("article_id") or ""
        if (
            _is_digits_only(article_id)
            and not _is_legacy_slug_identity(article_id, canonical_url)
        ):
            return article_id
    return None


def _resolve_numeric_article_id_from_network(
    canonical_url: str,
    *,
    network_retries: int,
    network_retry_delay_seconds: float,
    require_id_url_proof: bool = False,
) -> dict:
    attempts = 0
    max_attempts = network_retries + 1
    last_error = "metadata record did not contain a usable numeric article_id"

    for attempt in range(max_attempts):
        attempts = attempt + 1
        try:
            record = fetch_article_metadata_record(canonical_url)
        except Exception as exc:
            last_error = str(exc) or exc.__class__.__name__
        else:
            metadata_article_url = (record.get("article_url") or "").strip()
            metadata_identity = parse_target_identity(metadata_article_url)
            article_id = ""
            has_id_url_proof = False
            if metadata_identity is not None:
                metadata_id = (metadata_identity.get("article_id") or "").strip()
                if (
                    metadata_identity.get("article_type") == "id"
                    and _is_digits_only(metadata_id)
                ):
                    has_id_url_proof = True
                    article_id = metadata_id

            if not article_id and not require_id_url_proof:
                article_id = (record.get("article_id") or "").strip()

            if _is_digits_only(article_id) and (
                has_id_url_proof or not require_id_url_proof
            ):
                return {
                    "numeric_id": article_id,
                    "attempts": attempts,
                    "error": None,
                    "reason": None,
                }
            value = article_id or "<missing>"
            last_error = (
                "metadata record did not contain a usable numeric "
                f"article_id: {value}"
            )

        if attempt + 1 < max_attempts and network_retry_delay_seconds > 0:
            time.sleep(network_retry_delay_seconds)

    return {
        "numeric_id": None,
        "attempts": attempts,
        "error": last_error,
        "reason": "network_failed",
    }


def _load_group_rows_from_articles(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT article_id, article_type, title, canonical_url
        FROM articles
        WHERE article_type='a'
              AND canonical_url IS NOT NULL
              AND canonical_url <> ''
        ORDER BY canonical_url ASC, article_id ASC
        """
    )

    by_url: dict[str, list[dict]] = {}
    for article_id, article_type, title, canonical_url in cur.fetchall():
        key = _canonical_url_key(canonical_url)
        if key is None:
            continue
        by_url.setdefault(key, []).append(
            {
                "article_id": article_id,
                "article_type": article_type,
                "title": title,
                "canonical_url": canonical_url,
                "row_origin": "article",
            }
        )

    return by_url


def _append_target_only_group_rows(
    conn: sqlite3.Connection,
    by_url: dict[str, list[dict]],
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT article_id, article_type, canonical_url, is_active
        FROM target
        WHERE article_type='a'
              AND canonical_url IS NOT NULL
              AND canonical_url <> ''
        ORDER BY canonical_url ASC, article_id ASC
        """
    )

    for article_id, article_type, canonical_url, is_active in cur.fetchall():
        key = _canonical_url_key(canonical_url)
        if key is None:
            continue
        by_url.setdefault(key, []).append(
            {
                "article_id": article_id,
                "article_type": article_type,
                "title": None,
                "canonical_url": canonical_url,
                "row_origin": "target",
                "is_active": int(is_active),
            }
        )


def _has_archive_rows(rows: list[dict]) -> bool:
    return any(row.get("row_origin") == "article" for row in rows)


def _requires_id_url_proof(rows: list[dict]) -> bool:
    if _has_archive_rows(rows):
        return False
    return any(
        _is_digits_only((row.get("article_id") or "").strip())
        and _is_legacy_slug_identity(
            row.get("article_id"),
            row.get("canonical_url"),
        )
        for row in rows
    )


def _list_slug_article_groups(conn: sqlite3.Connection) -> list[dict]:
    """
    Return groups keyed by canonical_url for legacy slug identities.

    Group definition:
    - article_type='a'
    - canonical_url is present
    - at least one row has a legacy slug identity
    """
    by_url = _load_group_rows_from_articles(conn)
    _append_target_only_group_rows(conn, by_url)

    groups: list[dict] = []
    for canonical_url, rows in by_url.items():
        if not any(
            _is_legacy_slug_identity(r.get("article_id"), canonical_url)
            for r in rows
        ):
            continue
        groups.append({"canonical_url": canonical_url, "rows": rows})
    return groups


def _count_legacy_slug_rows(conn: sqlite3.Connection) -> dict:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT article_id, canonical_url
        FROM articles
        WHERE article_type='a'
        """
    )
    article_count = sum(
        1
        for article_id, canonical_url in cur.fetchall()
        if _is_legacy_slug_identity(article_id, canonical_url)
    )

    cur.execute(
        """
        SELECT responses.article_id, articles.canonical_url
        FROM responses
        LEFT JOIN articles
          ON articles.article_id = responses.article_id
         AND articles.article_type = responses.article_type
        WHERE responses.article_type='a'
        """
    )
    response_count = sum(
        1
        for article_id, canonical_url in cur.fetchall()
        if _is_legacy_slug_identity(article_id, canonical_url)
    )

    cur.execute(
        """
        SELECT article_id, canonical_url
        FROM target
        WHERE article_type='a'
        """
    )
    target_count = sum(
        1
        for article_id, canonical_url in cur.fetchall()
        if _is_legacy_slug_identity(article_id, canonical_url)
    )

    return {
        "articles": article_count,
        "responses": response_count,
        "target": target_count,
    }


def _best_title(rows: list[dict]) -> str:
    for row in rows:
        slug = _decoded_canonical_a_slug(row.get("canonical_url"))
        if slug and _is_legacy_slug_identity(
            row.get("article_id"),
            row.get("canonical_url"),
        ):
            return slug
    for row in rows:
        title = (row.get("title") or "").strip()
        if title:
            return title
    for row in rows:
        slug = _decoded_canonical_a_slug(row.get("canonical_url"))
        if slug:
            return slug
    return "unknown"


def _list_group_sources(rows: list[dict]) -> list[str]:
    sources: list[str] = []
    for row in rows:
        article_id = (row.get("article_id") or "").strip()
        if not article_id:
            continue
        if _is_legacy_slug_identity(article_id, row.get("canonical_url")):
            sources.append(article_id)
    return sorted(set(sources))


def _list_existing_numeric_ids(rows: list[dict]) -> list[str]:
    ids: list[str] = []
    for row in rows:
        article_id = (row.get("article_id") or "").strip()
        if (
            _is_digits_only(article_id)
            and not _is_legacy_slug_identity(article_id, row.get("canonical_url"))
        ):
            ids.append(article_id)
    return sorted(set(ids))


def _build_unresolved_group(
    *,
    canonical_url: str,
    sources: list[str],
    existing_numeric: list[str],
    title: str,
    resolved_by: str,
    reason: str,
    error: str,
    attempts: int,
    has_archive_rows: bool,
) -> dict:
    return {
        "canonical_url": canonical_url,
        "source_article_ids": sources,
        "existing_numeric_article_ids": existing_numeric,
        "resolved_numeric_article_id": None,
        "resolved_by": resolved_by,
        "title": title,
        "unresolved_reason": reason,
        "unresolved_error": error,
        "attempts_made": attempts,
        "article_type": "a",
        "has_archive_rows": has_archive_rows,
    }


def _raise_for_unresolved_network_groups(groups: list[dict]) -> None:
    first = groups[0]
    sources = ", ".join(first.get("source_article_ids") or []) or "-"
    message = (
        "unresolved network metadata resolution for canonical_url="
        f"{first['canonical_url']} sources={sources} attempts="
        f"{first.get('attempts_made', 0)} error="
        f"{first.get('unresolved_error') or first.get('resolved_by')}"
    )
    raise UnresolvedNetworkError(message)


def _format_unresolved_report_lines(summary: dict) -> list[str]:
    groups = [
        group
        for group in summary.get("groups") or []
        if group.get("resolved_numeric_article_id") is None
    ]
    lines = [
        "=== UNRESOLVED SLUG ARTICLE IDENTITY GROUPS ===",
        f"Count: {len(groups)}",
    ]
    for group in groups:
        lines.append(f"- canonical_url: {group['canonical_url']}")
        lines.append(
            "  legacy_article_id: "
            f"{', '.join(group.get('source_article_ids') or []) or '-'}"
        )
        lines.append(
            f"  article_type: {group.get('article_type') or 'a'}"
        )
        lines.append(
            f"  reason: {group.get('unresolved_reason') or group['resolved_by']}"
        )
        lines.append(
            "  error: "
            f"{group.get('unresolved_error') or group['resolved_by']}"
        )
        lines.append(
            f"  attempts_made: {group.get('attempts_made', 0)}"
        )
        existing = group.get("existing_numeric_article_ids") or []
        lines.append(
            f"  existing_numeric_ids: {', '.join(existing) or '-'}"
        )
    return lines


def _group_needs_repair(group: dict) -> bool:
    numeric_id = group.get("resolved_numeric_article_id")
    if numeric_id is None:
        return False
    sources = group.get("source_article_ids") or []
    return any(source_id != numeric_id for source_id in sources)


def write_unresolved_report(report_path: str, summary: dict) -> None:
    unresolved_groups = [
        group
        for group in summary.get("groups") or []
        if group.get("resolved_numeric_article_id") is None
    ]
    if not unresolved_groups:
        return
    report = Path(report_path)
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(
        "\n".join(_format_unresolved_report_lines(summary)) + "\n",
        encoding="utf-8",
    )


def _ensure_numeric_article_row(
    conn: sqlite3.Connection,
    *,
    numeric_id: str,
    title: str,
    canonical_url: str,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO articles
        (article_id, article_type, title, canonical_url)
        VALUES (?, 'a', ?, ?)
        """,
        (numeric_id, title, canonical_url),
    )
    cur.execute(
        """
        UPDATE articles
        SET title = ?,
            canonical_url = ?
        WHERE article_id = ? AND article_type = 'a'
        """,
        (title, canonical_url, numeric_id),
    )


def _transfer_responses(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    dest_id: str,
) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO responses
        (article_id, article_type, res_no, id_hash, poster_name, posted_at,
         content_html, content_text, res_hidden, idhash_hidden, good_count,
         bad_count, scraped_at)
        SELECT ?, 'a', res_no, id_hash, poster_name, posted_at,
               content_html, content_text, res_hidden, idhash_hidden, good_count,
               bad_count, scraped_at
        FROM responses
        WHERE article_id = ? AND article_type = 'a'
        """,
        (dest_id, source_id),
    )
    return int(cur.rowcount or 0)


def _count_responses_for_id(conn: sqlite3.Connection, article_id: str) -> int:
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM responses"
        " WHERE article_id=? AND article_type='a'",
        (article_id,),
    )
    return int(cur.fetchone()[0])


def _missing_responses_count(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    dest_id: str,
) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM responses AS src
        WHERE src.article_id=? AND src.article_type='a'
              AND NOT EXISTS (
                  SELECT 1
                  FROM responses AS dst
                  WHERE dst.article_id=?
                        AND dst.article_type='a'
                        AND dst.res_no=src.res_no
              )
        """,
        (source_id, dest_id),
    )
    return int(cur.fetchone()[0])


def _cleanup_source_identity(
    conn: sqlite3.Connection,
    *,
    source_id: str,
) -> dict:
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM responses WHERE article_id=? AND article_type='a'",
        (source_id,),
    )
    deleted_responses = int(cur.rowcount or 0)
    cur.execute(
        "DELETE FROM articles WHERE article_id=? AND article_type='a'",
        (source_id,),
    )
    deleted_articles = int(cur.rowcount or 0)
    return {
        "deleted_articles": deleted_articles,
        "deleted_responses": deleted_responses,
    }


def _normalize_target_rows(
    conn: sqlite3.Connection,
    *,
    canonical_url: str,
    source_ids: list[str],
    dest_id: str,
) -> dict:
    """
    Target handling rules:
    - never delete-first
    - deactivate legacy slug targets
    - ensure a dest numeric target row exists, keeping it active if any source
      target was active
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT article_id, is_active
        FROM target
        WHERE article_type='a' AND canonical_url=?
        """,
        (canonical_url,),
    )
    existing = {row[0]: int(row[1]) for row in cur.fetchall()}

    desired_active = int(existing.get(dest_id, 0))
    for sid in source_ids:
        desired_active = max(desired_active, int(existing.get(sid, 0)))

    actions: list[dict] = []
    for sid in source_ids:
        if sid not in existing:
            actions.append({"article_id": sid, "action": "absent"})
            continue
        if existing[sid] == 0:
            actions.append({"article_id": sid, "action": "already_inactive"})
            continue
        cur.execute(
            """
            UPDATE target
            SET is_active=0, canonical_url=?
            WHERE article_id=? AND article_type='a'
            """,
            (canonical_url, sid),
        )
        actions.append({"article_id": sid, "action": "deactivated"})

    cur.execute(
        """
        INSERT OR IGNORE INTO target
        (article_id, article_type, canonical_url, is_active)
        VALUES (?, 'a', ?, ?)
        """,
        (dest_id, canonical_url, desired_active),
    )
    cur.execute(
        """
        UPDATE target
        SET canonical_url=?, is_active=?
        WHERE article_id=? AND article_type='a'
        """,
        (canonical_url, desired_active, dest_id),
    )

    return {
        "dest_is_active": bool(desired_active),
        "sources": actions,
    }


def plan_slug_article_identity_repair(
    conn: sqlite3.Connection,
    *,
    allow_network: bool,
    network_retries: int = DEFAULT_NETWORK_RETRIES,
    network_retry_delay_seconds: float = (
        DEFAULT_NETWORK_RETRY_DELAY_SECONDS
    ),
    skip_unresolved: bool = False,
    limit: int | None = None,
) -> dict:
    network_retries = _normalize_network_retries(network_retries)
    network_retry_delay_seconds = _normalize_network_retry_delay_seconds(
        network_retry_delay_seconds
    )
    all_groups = _list_slug_article_groups(conn)
    legacy_counts = _count_legacy_slug_rows(conn)
    total_detected = len(all_groups)
    groups = all_groups if limit is None else all_groups[:limit]
    plan: dict = {
        "dry_run": True,
        "allow_network": allow_network,
        "network_retries": network_retries,
        "network_retry_delay_seconds": network_retry_delay_seconds,
        "skip_unresolved": skip_unresolved,
        "legacy_counts": legacy_counts,
        "total_detected_groups": total_detected,
        "processed_groups": len(groups),
        "groups": [],
    }

    unresolved_network_groups: list[dict] = []

    for group in groups:
        canonical_url = group["canonical_url"]
        rows = group["rows"]
        sources = _list_group_sources(rows)
        existing_numeric = _list_existing_numeric_ids(rows)
        numeric_id = _resolve_numeric_article_id_from_db_rows(
            rows,
            canonical_url=canonical_url,
        )
        resolved_by = "db"
        attempts_made = 0
        unresolved_reason = None
        unresolved_error = None
        title = _best_title(rows)
        require_id_url_proof = _requires_id_url_proof(rows)

        if numeric_id is None:
            resolved_by = "network"
            if not allow_network:
                resolved_by = "skipped_network_disallowed"
                unresolved_reason = resolved_by
                unresolved_error = (
                    "network metadata resolution disabled; use "
                    "--allow-network to resolve this group"
                )
            elif not _is_canonical_a_url(canonical_url):
                resolved_by = "skipped_invalid_canonical_url"
                unresolved_reason = resolved_by
                unresolved_error = "canonical_url is not a valid /a/<slug> URL"
            else:
                network_result = _resolve_numeric_article_id_from_network(
                    canonical_url,
                    network_retries=network_retries,
                    network_retry_delay_seconds=(
                        network_retry_delay_seconds
                    ),
                    require_id_url_proof=require_id_url_proof,
                )
                numeric_id = network_result["numeric_id"]
                attempts_made = network_result["attempts"]
                if numeric_id is None:
                    resolved_by = network_result["reason"]
                    unresolved_reason = network_result["reason"]
                    unresolved_error = network_result["error"]

        if numeric_id is not None:
            validate_saved_article_identity(numeric_id, "a")
            plan["groups"].append(
                {
                    "canonical_url": canonical_url,
                    "source_article_ids": sources,
                    "existing_numeric_article_ids": existing_numeric,
                    "resolved_numeric_article_id": numeric_id,
                    "resolved_by": resolved_by,
                    "title": title,
                    "attempts_made": attempts_made,
                    "article_type": "a",
                    "has_archive_rows": _has_archive_rows(rows),
                    "needs_repair": any(
                        source_id != numeric_id for source_id in sources
                    ),
                }
            )
            continue

        unresolved_group = _build_unresolved_group(
            canonical_url=canonical_url,
            sources=sources,
            existing_numeric=existing_numeric,
            title=title,
            resolved_by=resolved_by,
            reason=unresolved_reason or resolved_by,
            error=unresolved_error or resolved_by,
            attempts=attempts_made,
            has_archive_rows=_has_archive_rows(rows),
        )
        plan["groups"].append(unresolved_group)
        if resolved_by == "network_failed":
            unresolved_network_groups.append(unresolved_group)

    resolved = sum(
        1 for g in plan["groups"]
        if g["resolved_numeric_article_id"] is not None
    )
    plan["resolved_groups"] = resolved
    plan["unresolved_groups"] = len(plan["groups"]) - resolved
    plan["skipped_groups"] = len(plan["groups"]) - resolved
    plan["unresolved_network_groups"] = len(unresolved_network_groups)
    if unresolved_network_groups and not skip_unresolved:
        _raise_for_unresolved_network_groups(unresolved_network_groups)
    return plan


def apply_slug_article_identity_repair(
    conn: sqlite3.Connection,
    *,
    allow_network: bool,
    network_retries: int = DEFAULT_NETWORK_RETRIES,
    network_retry_delay_seconds: float = (
        DEFAULT_NETWORK_RETRY_DELAY_SECONDS
    ),
    skip_unresolved: bool = False,
    limit: int | None = None,
) -> dict:
    summary = plan_slug_article_identity_repair(
        conn,
        allow_network=allow_network,
        network_retries=network_retries,
        network_retry_delay_seconds=network_retry_delay_seconds,
        skip_unresolved=skip_unresolved,
        limit=limit,
    )
    summary["dry_run"] = False

    cur = conn.cursor()
    cur.execute("BEGIN")
    try:
        for group in summary["groups"]:
            numeric_id = group["resolved_numeric_article_id"]
            canonical_url = group["canonical_url"]
            sources = list(group["source_article_ids"])
            title = group["title"]

            group["apply"] = {
                "status": "skipped",
                "reason": None,
                "response_transfers": [],
                "verification": [],
                "cleanup": [],
                "target": None,
            }

            if numeric_id is None:
                group["apply"]["reason"] = group["resolved_by"]
                continue

            if not _group_needs_repair(group):
                group["apply"]["reason"] = "no_identity_change"
                continue

            validate_saved_article_identity(numeric_id, "a")

            if not group.get("has_archive_rows", True):
                group["apply"]["target"] = _normalize_target_rows(
                    conn,
                    canonical_url=canonical_url,
                    source_ids=sources,
                    dest_id=numeric_id,
                )
                group["apply"]["status"] = "applied"
                continue

            _ensure_numeric_article_row(
                conn,
                numeric_id=numeric_id,
                title=title,
                canonical_url=canonical_url,
            )

            for sid in sources:
                src_count = _count_responses_for_id(conn, sid)
                inserted = _transfer_responses(
                    conn, source_id=sid, dest_id=numeric_id
                )
                missing = _missing_responses_count(
                    conn, source_id=sid, dest_id=numeric_id
                )
                group["apply"]["response_transfers"].append(
                    {
                        "source_article_id": sid,
                        "source_count": src_count,
                        "inserted": inserted,
                        "missing_after_transfer": missing,
                    }
                )
                group["apply"]["verification"].append(
                    {
                        "source_article_id": sid,
                        "ok": missing == 0,
                    }
                )

            if not all(v["ok"] for v in group["apply"]["verification"]):
                group["apply"]["status"] = "failed_verification_no_cleanup"
                raise RuntimeError(
                    "response preservation verification failed; rolling back"
                )

            for sid in sources:
                group["apply"]["cleanup"].append(
                    {
                        "source_article_id": sid,
                        **_cleanup_source_identity(conn, source_id=sid),
                    }
                )

            group["apply"]["target"] = _normalize_target_rows(
                conn,
                canonical_url=canonical_url,
                source_ids=sources,
                dest_id=numeric_id,
            )
            group["apply"]["status"] = "applied"

        conn.commit()

        applied = sum(
            1 for g in summary["groups"]
            if g.get("apply", {}).get("status") == "applied"
        )
        summary["applied_groups"] = applied
        total_inserted = 0
        total_duplicate = 0
        total_missing = 0
        for g in summary["groups"]:
            for t in g.get("apply", {}).get("response_transfers") or []:
                total_inserted += t.get("inserted", 0)
                total_duplicate += (
                    t.get("source_count", 0) - t.get("inserted", 0)
                )
                total_missing += t.get("missing_after_transfer", 0)
        summary["response_inserted"] = total_inserted
        summary["response_duplicate"] = total_duplicate
        summary["response_missing_after"] = total_missing
        return summary
    except Exception:
        conn.rollback()
        raise


def repair_slug_article_identity(
    db_path: str,
    *,
    apply: bool = False,
    allow_network: bool = False,
    network_retries: int = DEFAULT_NETWORK_RETRIES,
    network_retry_delay_seconds: float = (
        DEFAULT_NETWORK_RETRY_DELAY_SECONDS
    ),
    skip_unresolved: bool = False,
    limit: int | None = None,
) -> dict:
    if not db_path:
        raise ValueError(
            "explicit db_path is required; refusing implicit default"
        )
    if not Path(db_path).exists():
        raise FileNotFoundError(f"db_path does not exist: {db_path}")
    if limit is not None and limit < 0:
        raise ValueError(f"limit must be non-negative, got {limit}")
    network_retries = _normalize_network_retries(network_retries)
    network_retry_delay_seconds = _normalize_network_retry_delay_seconds(
        network_retry_delay_seconds
    )

    conn = sqlite3.connect(db_path)
    try:
        if apply:
            return apply_slug_article_identity_repair(
                conn,
                allow_network=allow_network,
                network_retries=network_retries,
                network_retry_delay_seconds=network_retry_delay_seconds,
                skip_unresolved=skip_unresolved,
                limit=limit,
            )
        return plan_slug_article_identity_repair(
            conn,
            allow_network=allow_network,
            network_retries=network_retries,
            network_retry_delay_seconds=network_retry_delay_seconds,
            skip_unresolved=skip_unresolved,
            limit=limit,
        )
    finally:
        conn.close()


def format_repair_summary_lines(
    db_path: str,
    summary: dict,
    *,
    summary_only: bool = False,
) -> list[str]:
    mode = "dry-run" if summary.get("dry_run") else "apply"
    counts = summary.get("legacy_counts") or {}
    groups = summary.get("groups") or []
    total_detected = summary.get("total_detected_groups", len(groups))
    processed = summary.get("processed_groups", len(groups))
    resolved = summary.get("resolved_groups", 0)
    unresolved = summary.get("unresolved_groups", 0)
    skipped = summary.get("skipped_groups", 0)
    lines = [
        "=== REPAIR SLUG ARTICLE IDENTITY ===",
        f"DB: {db_path}",
        f"Mode: {mode}",
        f"Allow network: {bool(summary.get('allow_network'))}",
        f"Network retries: {summary.get('network_retries', 0)}",
        (
            "Network retry delay seconds: "
            f"{summary.get('network_retry_delay_seconds', 0)}"
        ),
        f"Skip unresolved: {bool(summary.get('skip_unresolved'))}",
        (
            "Legacy counts: "
            f"articles={counts.get('articles', 0)} "
            f"responses={counts.get('responses', 0)} "
            f"target={counts.get('target', 0)}"
        ),
        f"Total detected groups: {total_detected}",
        f"Processed groups: {processed}",
        f"Resolved groups: {resolved}",
        f"Unresolved groups: {unresolved}",
        f"Skipped groups: {skipped}",
    ]
    if not summary.get("dry_run"):
        lines.append(f"Applied groups: {summary.get('applied_groups', 0)}")
        if "response_inserted" in summary:
            lines.append(
                "Response totals: "
                f"inserted={summary['response_inserted']} "
                f"duplicate={summary['response_duplicate']} "
                f"missing_after={summary['response_missing_after']}"
            )

    if summary_only:
        return lines

    for group in groups:
        lines.append(f"- canonical_url: {group['canonical_url']}")
        lines.append(
            "  resolved_numeric_article_id: "
            f"{group['resolved_numeric_article_id']}"
        )
        lines.append(f"  resolved_by: {group['resolved_by']}")
        if group.get("attempts_made"):
            lines.append(f"  attempts_made: {group['attempts_made']}")
        lines.append(
            f"  sources: {', '.join(group['source_article_ids']) or '-'}"
        )
        existing = group.get("existing_numeric_article_ids") or []
        lines.append(
            f"  existing_numeric_ids: {', '.join(existing) or '-'}"
        )
        if "needs_repair" in group:
            lines.append(f"  needs_repair: {bool(group['needs_repair'])}")
        if group.get("unresolved_reason"):
            lines.append(
                f"  unresolved_reason: {group['unresolved_reason']}"
            )
        if group.get("unresolved_error"):
            lines.append(
                f"  unresolved_error: {group['unresolved_error']}"
            )
        if not summary.get("dry_run"):
            apply_info = group.get("apply") or {}
            lines.append(f"  apply_status: {apply_info.get('status')}")
            reason = apply_info.get("reason")
            if reason:
                lines.append(f"  apply_reason: {reason}")
            for item in apply_info.get("response_transfers") or []:
                lines.append(
                    "  transfer: "
                    f"source={item['source_article_id']} "
                    f"inserted={item['inserted']} "
                    f"missing_after={item['missing_after_transfer']}"
                )
            target_info = apply_info.get("target")
            if target_info is not None:
                lines.append(
                    "  target_dest_is_active: "
                    f"{target_info['dest_is_active']}"
                )
                for src in target_info.get("sources") or []:
                    lines.append(
                        "  target_source: "
                        f"{src['article_id']} action={src['action']}"
                    )

    return lines


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="repair_slug_article_identity",
        description=(
            "Repair legacy slug article_id rows in a copied archive DB."
        ),
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Explicit SQLite DB path (required; no implicit runtime default).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes (writes). Without this flag, dry-run only.",
    )
    parser.add_argument(
        "--allow-network",
        action="store_true",
        help=(
            "Allow metadata fetch from canonical_url when DB lacks numeric ID."
        ),
    )
    parser.add_argument(
        "--network-retries",
        type=int,
        default=DEFAULT_NETWORK_RETRIES,
        metavar="N",
        help=(
            "Retry metadata fetch failures up to N additional times "
            f"(default: {DEFAULT_NETWORK_RETRIES})."
        ),
    )
    parser.add_argument(
        "--network-retry-delay-seconds",
        type=float,
        default=DEFAULT_NETWORK_RETRY_DELAY_SECONDS,
        metavar="SECONDS",
        help=(
            "Delay between metadata fetch retries in seconds "
            f"(default: {DEFAULT_NETWORK_RETRY_DELAY_SECONDS})."
        ),
    )
    parser.add_argument(
        "--skip-unresolved",
        action="store_true",
        help=(
            "Skip unresolved network metadata failures after retries and "
            "continue with other groups."
        ),
    )
    parser.add_argument(
        "--unresolved-report",
        help=(
            "Write a human-readable report for unresolved groups when any "
            "remain."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Limit number of canonical_url groups processed (default: all)."
        ),
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Suppress per-group output; print summary statistics only.",
    )
    args = parser.parse_args(argv)
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be non-negative")
    if args.network_retries < 0:
        parser.error("--network-retries must be non-negative")
    if args.network_retry_delay_seconds < 0:
        parser.error(
            "--network-retry-delay-seconds must be non-negative"
        )
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        summary = repair_slug_article_identity(
            args.db,
            apply=bool(args.apply),
            allow_network=bool(args.allow_network),
            network_retries=args.network_retries,
            network_retry_delay_seconds=args.network_retry_delay_seconds,
            skip_unresolved=bool(args.skip_unresolved),
            limit=args.limit,
        )
    except UnresolvedNetworkError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.unresolved_report:
        write_unresolved_report(args.unresolved_report, summary)

    for line in format_repair_summary_lines(
        args.db,
        summary,
        summary_only=bool(args.summary_only),
    ):
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
